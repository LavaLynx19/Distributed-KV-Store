package raft

import (
	"slices"

	"distributed-kv-store/internal/core"
)

// Read index (A§6.2): how a Leader answers a read from memory without
// risking a Stale read. Two things must be true before it answers.
//
//  1. Its state machine holds every write Acknowledged before the read
//     arrived. A new Leader doesn't know what its predecessor Committed until
//     it commits an Entry of its own Term, so until then reads wait. The read
//     then remembers the commit index as it stood, and is answered once that
//     much has been applied.
//
//  2. It is still the Leader. A Leader that has been replaced can't tell by
//     itself, so it asks: a round of Appends goes out after the read arrives,
//     and a Majority must echo that round. A Member that has voted in a newer
//     Term refuses the Append, so a replaced Leader can never gather the
//     echoes.
//
// Rounds are numbered. At most one is unconfirmed at a time; reads arriving
// meanwhile share the next one, so the cost per read falls as load rises.

// pendingRead is a read waiting for both conditions.
type pendingRead struct {
	ref   uint64
	round uint64 // the first round sent after the read arrived
	// index is the commit index the state machine must reach. It is set once
	// the Leader has Committed an Entry of its own Term (hasIndex).
	index    core.Index
	hasIndex bool
}

func (n *Node) ownTermCommitted() bool { return n.termAt(n.commit) == n.term }

// queueRead registers a read on the Leader and makes sure a confirmation
// round that postdates it is, or will be, on its way.
func (n *Node) queueRead(out *core.Output, r core.Read) {
	read := pendingRead{ref: r.Ref, round: n.readRound + 1}
	if n.ownTermCommitted() {
		read.index, read.hasIndex = n.commit, true
	}
	n.reads = append(n.reads, read)
	if !n.roundOpen {
		n.startRound(out)
	}
}

// startRound sends every follower an Append stamped with a new round number.
func (n *Node) startRound(out *core.Output) {
	n.readRound++
	n.roundOpen = true
	for _, m := range n.members {
		if m != n.id {
			// An empty Append placed at the last Index the follower is known
			// to hold. It carries the round number and disturbs nothing.
			// A follower that is behind the Snapshot is probed at its edge:
			// it will refuse the Append, but its echo counts all the same.
			at := max(n.match[m], n.log.base)
			n.send(out, m, Append{
				Term:       n.term,
				PrevIndex:  at,
				PrevTerm:   n.termAt(at),
				Commit:     n.commit,
				ReadRound:  n.readRound,
				LeaderLast: n.lastIndex(),
			})
		}
	}
	n.roundConfirmed(out) // a Group of one confirms itself
}

// confirmedRound is the highest round a Majority, counting the Leader, has
// echoed.
func (n *Node) confirmedRound() uint64 {
	var acked []uint64
	if n.isMember(n.id) {
		acked = append(acked, n.readRound)
	}
	for _, m := range n.members {
		if m != n.id {
			acked = append(acked, n.roundAcked[m])
		}
	}
	slices.SortFunc(acked, func(a, b uint64) int {
		switch {
		case a > b:
			return -1
		case a < b:
			return 1
		}
		return 0
	})
	if len(acked) < n.majority() {
		return 0
	}
	return acked[n.majority()-1]
}

// roundConfirmed is called when a follower echoes a round. If the latest
// round now has its Majority, reads that arrived while it was out get a
// round of their own.
func (n *Node) roundConfirmed(out *core.Output) {
	if !n.roundOpen || n.confirmedRound() < n.readRound {
		return
	}
	n.roundOpen = false
	for _, r := range n.reads {
		if r.round > n.readRound {
			n.startRound(out)
			return
		}
	}
}

// releaseReads answers every read whose two conditions now hold. It runs at
// the end of each step, after newly Committed Entries have been handed over,
// so the shell applies them before it reads.
func (n *Node) releaseReads(out *core.Output) {
	if n.role != core.LeaderRole || len(n.reads) == 0 {
		return
	}
	confirmed := n.confirmedRound()
	own := n.ownTermCommitted()
	waiting := n.reads[:0]
	for _, r := range n.reads {
		if !r.hasIndex && own {
			r.index, r.hasIndex = n.commit, true
		}
		if r.hasIndex && r.round <= confirmed && r.index <= n.applied {
			out.Reads = append(out.Reads, core.Result{Ref: r.ref, Reason: core.OK})
			continue
		}
		waiting = append(waiting, r)
	}
	n.reads = waiting
}

// failReads turns away every waiting read when the Leader steps down. A read
// changes nothing, so the client can simply ask again elsewhere.
func (n *Node) failReads(out *core.Output) {
	for _, r := range n.reads {
		out.Reads = append(out.Reads, core.Result{Ref: r.ref, Reason: core.NoMajority})
	}
	n.reads = nil
}
