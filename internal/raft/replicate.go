package raft

import (
	"fmt"

	"distributed-kv-store/internal/core"
)

// sendAppend sends follower m the Entries from next[m] on, and assumes they
// will arrive: next[m] moves past them. A heartbeat resends from match[m]+1
// if no confirmation has come back by then.
func (n *Node) sendAppend(out *core.Output, m core.NodeID) {
	prev := n.next[m] - 1
	end := min(n.lastIndex(), prev+maxBatch)
	n.send(out, m, Append{
		Term:      n.term,
		PrevIndex: prev,
		PrevTerm:  n.termAt(prev),
		Entries:   n.log.after(prev, end),
		Commit:    n.commit,
		ReadRound: n.readRound,
	})
	n.next[m] = end + 1
}

// handleAppend is the follower's side of replication. It accepts Entries only
// if its Log matches the Leader's at PrevIndex, and replaces any of its own
// Entries that conflict with the Leader's.
func (n *Node) handleAppend(out *core.Output, from core.NodeID, m Append) {
	if m.Term < n.term {
		n.send(out, from, AppendReply{Term: n.term})
		return
	}
	// An Append in the current Term comes from its one Leader.
	if n.role == core.LeaderRole {
		panic(fmt.Sprintf("raft: nodes %d and %d both lead term %d", n.id, from, n.term))
	}
	if n.role != core.Follower {
		n.becomeFollower(out, m.Term, from)
	}
	n.leader = from
	n.elapsed = 0

	if m.PrevIndex > n.lastIndex() {
		n.send(out, from, AppendReply{Term: n.term, Match: n.lastIndex(), ReadRound: m.ReadRound})
		return
	}
	if n.termAt(m.PrevIndex) != m.PrevTerm {
		n.send(out, from, AppendReply{Term: n.term, Match: m.PrevIndex - 1, ReadRound: m.ReadRound})
		return
	}

	for i, e := range m.Entries {
		index := m.PrevIndex + 1 + core.Index(i)
		if index <= n.lastIndex() {
			if n.termAt(index) == e.Term {
				continue // already have it
			}
			if index <= n.commit {
				panic(fmt.Sprintf("raft: node %d asked to replace Committed Entry %d", n.id, index))
			}
			n.log.truncateFrom(index)
			persist(out).TruncateFrom = index
		}
		n.appendEntry(out, e)
	}

	// Only Entries this Append has just confirmed can be marked Committed:
	// anything beyond them might still differ from the Leader's Log.
	confirmed := m.PrevIndex + core.Index(len(m.Entries))
	if c := min(m.Commit, confirmed); c > n.commit {
		n.commit = c
	}
	n.send(out, from, AppendReply{Term: n.term, Success: true, Match: confirmed, ReadRound: m.ReadRound})
}

func (n *Node) handleAppendReply(out *core.Output, from core.NodeID, m AppendReply) {
	if n.role != core.LeaderRole || m.Term != n.term {
		return
	}
	n.heard[from] = n.now
	// Any reply in this Term, success or not, says the follower still
	// recognised this Leader when it handled that round's Append.
	if m.ReadRound > n.roundAcked[from] {
		n.roundAcked[from] = m.ReadRound
		n.roundConfirmed(out)
	}
	if !m.Success {
		// Back up to the follower's hint, but never behind what it has
		// already confirmed.
		n.next[from] = max(m.Match, n.match[from]) + 1
		n.sendAppend(out, from)
		return
	}
	if m.Match > n.match[from] {
		n.match[from] = m.Match
	}
	if n.next[from] <= n.match[from] {
		n.next[from] = n.match[from] + 1
	}
	// A follower that is catching up gets its next batch at once, not at the
	// next heartbeat.
	if n.next[from] <= n.lastIndex() {
		n.sendAppend(out, from)
	}
	n.advanceCommit()
}

// advanceCommit marks as Committed the highest Entry of the current Term
// that a Majority holds, and with it everything before. An Entry from an
// earlier Term is never counted directly: a Majority holding it isn't enough
// to stop a later Leader from replacing it.
func (n *Node) advanceCommit() {
	for i := n.lastIndex(); i > n.commit && n.termAt(i) == n.term; i-- {
		holders := 1
		for _, m := range n.members {
			if m != n.id && n.match[m] >= i {
				holders++
			}
		}
		if holders >= n.majority() {
			n.commit = i
			return
		}
	}
}
