package raft

import (
	"fmt"
	"slices"

	"distributed-kv-store/internal/core"
)

// sendAppend sends follower m the Entries from next[m] on, and assumes they
// will arrive: next[m] moves past them. A heartbeat resends from match[m]+1
// if no confirmation has come back by then.
func (n *Node) sendAppend(out *core.Output, m core.NodeID) {
	prev := n.next[m] - 1
	if prev < n.log.base {
		// The follower needs Entries a Snapshot has replaced.
		if !n.cfg.NoSnapshotTransfer {
			n.sendSnapshot(out, m)
			return
		}
		// With transfer off there is nothing to send it but what follows
		// the Snapshot, which it will refuse.
		prev = n.log.base
	}
	end := min(n.lastIndex(), prev+core.Index(n.cfg.MaxBatch))
	n.send(out, m, Append{
		Term:       n.term,
		PrevIndex:  prev,
		PrevTerm:   n.termAt(prev),
		Entries:    n.log.after(prev, end),
		Commit:     n.commit,
		ReadRound:  n.readRound,
		LeaderLast: n.lastIndex(),
	})
	n.next[m] = end + 1
}

// sendHeartbeat is the per-tick send to follower m: whatever it is due, or
// an empty Append if that is a Snapshot already on its way, so that the
// follower keeps hearing from its Leader meanwhile.
func (n *Node) sendHeartbeat(out *core.Output, m core.NodeID) {
	if n.next[m]-1 < n.log.base && !n.cfg.NoSnapshotTransfer && !n.snapshotDue(m) {
		n.send(out, m, Append{Term: n.term, PrevIndex: n.log.base, PrevTerm: n.log.baseTerm, Commit: n.commit, ReadRound: n.readRound, LeaderLast: n.lastIndex()})
		return
	}
	n.sendAppend(out, m)
}

// snapshotDue reports whether follower m should be sent the Snapshot now. A
// Snapshot can be large, so it is sent again only after an election
// timeout's worth of ticks with no confirmation.
func (n *Node) snapshotDue(m core.NodeID) bool {
	sent, ever := n.sentSnap[m]
	return !ever || n.now-sent >= n.cfg.ElectionTicks
}

// sendSnapshot sends follower m the Leader's Snapshot, unless one went out
// recently, and expects the follower to continue from just after it.
func (n *Node) sendSnapshot(out *core.Output, m core.NodeID) {
	if !n.snapshotDue(m) {
		return
	}
	n.sentSnap[m] = n.now
	n.send(out, m, InstallSnapshot{Term: n.term, Snapshot: *n.snapshot, ReadRound: n.readRound})
	n.next[m] = n.log.base + 1
}

// handleInstallSnapshot is the follower's side of catching up by Snapshot. A
// Snapshot that reaches past what this Member has Committed replaces its
// state machine's contents and the Log the Snapshot covers.
//
// What happens to the Log after the Snapshot depends on whether this Member
// already holds the Snapshot's last Entry. If it does, its Log agrees with
// the Leader's up to there, and everything after is kept: the Member may
// have acknowledged those Entries, and the Leader may be counting on them.
// A Leader can send a Snapshot to a follower that is nearly up to date,
// because its record of the follower lags the acknowledgements in flight.
// Only a Member whose Log doesn't reach or doesn't match the Snapshot drops
// its Log entirely.
func (n *Node) handleInstallSnapshot(out *core.Output, from core.NodeID, m InstallSnapshot) {
	if m.Term < n.term {
		n.sendReply(out, from, AppendReply{Term: n.term})
		return
	}
	if n.role == core.LeaderRole {
		panic(fmt.Sprintf("raft: nodes %d and %d both lead term %d", n.id, from, n.term))
	}
	if n.role != core.Follower {
		n.becomeFollower(out, m.Term, from)
	}
	n.leader = from
	n.elapsed = 0
	n.contact = n.now

	if m.Snapshot.Index <= n.commit {
		// Nothing new: everything it covers is already Committed here, and
		// Committed Entries are the same on every Member.
		n.sendReply(out, from, AppendReply{Term: n.term, Success: true, Match: n.commit, ReadRound: m.ReadRound})
		return
	}
	snap := m.Snapshot
	n.snapshot = &snap
	p := persist(out)
	p.Snapshot = &snap
	if snap.Index <= n.lastIndex() && n.termAt(snap.Index) == snap.Term {
		n.log.compactTo(snap.Index)
		n.foldListsTo(snap.Index)
	} else {
		n.log = raftLog{base: snap.Index, baseTerm: snap.Term}
		p.ResetLog = true
		n.lists = n.lists[:1]
	}
	if snap.Members != nil {
		// The Snapshot's list is the one in force at its last Entry.
		n.lists[0] = memberList{index: snap.Index, members: slices.Clone(snap.Members)}
		n.changed = true
	}
	n.members = n.lists[len(n.lists)-1].members
	n.commit, n.applied = snap.Index, snap.Index
	out.Restore = &snap
	n.sendReply(out, from, AppendReply{Term: n.term, Success: true, Match: snap.Index, ReadRound: m.ReadRound})
}

// sendReply answers the Leader. A recovering Member flags every answer, so
// the Leader stops assuming it still holds what it confirmed before.
func (n *Node) sendReply(out *core.Output, to core.NodeID, r AppendReply) {
	r.Reset = n.recovering
	n.send(out, to, r)
}

// handleAppend is the follower's side of replication. It accepts Entries only
// if its Log matches the Leader's at PrevIndex, and replaces any of its own
// Entries that conflict with the Leader's.
func (n *Node) handleAppend(out *core.Output, from core.NodeID, m Append) {
	if m.Term < n.term {
		n.sendReply(out, from, AppendReply{Term: n.term})
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
	n.contact = n.now

	if m.PrevIndex < n.log.base {
		// The Append starts inside this Member's Snapshot. Everything a
		// Snapshot covers is Committed, so those Entries match by definition:
		// skip them and check from the Snapshot's edge.
		covered := n.log.base - m.PrevIndex
		if covered >= core.Index(len(m.Entries)) {
			n.sendReply(out, from, AppendReply{Term: n.term, Success: true, Match: n.log.base, ReadRound: m.ReadRound, Sent: m.Sent})
			return
		}
		m.Entries = m.Entries[covered:]
		m.PrevIndex, m.PrevTerm = n.log.base, n.log.baseTerm
	}
	if m.PrevIndex > n.lastIndex() {
		n.sendReply(out, from, AppendReply{Term: n.term, Match: n.lastIndex(), ReadRound: m.ReadRound, Sent: m.Sent})
		return
	}
	if n.termAt(m.PrevIndex) != m.PrevTerm {
		n.sendReply(out, from, AppendReply{Term: n.term, Match: m.PrevIndex - 1, ReadRound: m.ReadRound, Sent: m.Sent})
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
			n.dropListsFrom(index)
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
	n.sendReply(out, from, AppendReply{Term: n.term, Success: true, Match: confirmed, ReadRound: m.ReadRound, Sent: m.Sent})

	// A recovering Member that now matches the Leader's whole Log holds
	// everything that is Committed, and can be trusted to vote again (A§6.8).
	if n.recovering && confirmed >= m.LeaderLast {
		n.recovering = false
		persist(out).Recovered = true
	}
}

func (n *Node) handleAppendReply(out *core.Output, from core.NodeID, m AppendReply) {
	if n.role != core.LeaderRole || m.Term != n.term {
		return
	}
	if _, followed := n.next[from]; !followed {
		return // a Node this Leader no longer replicates to
	}
	n.heard[from] = n.now
	if at, ok := n.leaseFrom[from]; m.Sent != 0 && (!ok || m.Sent > at) {
		n.leaseFrom[from] = m.Sent
	}
	if m.Reset {
		// The follower found damage on its disk and holds less than it once
		// confirmed. What it says it has now is all that can be counted on.
		// Entries already Committed stay Committed: a Majority held them
		// when that was decided.
		n.match[from] = m.Match
		n.next[from] = m.Match + 1
	}
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
		delete(n.sentSnap, from) // progress: a later Snapshot may go at once
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
		holders := n.self()
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
