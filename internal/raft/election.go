package raft

import "distributed-kv-store/internal/core"

// startElection begins a new Term with this Member as the candidate.
func (n *Node) startElection(out *core.Output) {
	n.term++
	n.role = core.Candidate
	n.votedFor = n.id
	n.storeHardState(out)
	n.leader = 0
	n.votes = map[core.NodeID]bool{n.id: true}
	n.resetElection()
	if len(n.votes) >= n.majority() {
		n.becomeLeader(out)
		return
	}
	for _, m := range n.members {
		if m != n.id {
			n.send(out, m, RequestVote{Term: n.term, LastIndex: n.lastIndex(), LastTerm: n.termAt(n.lastIndex())})
		}
	}
}

// handleRequestVote grants at most one vote per Term, and only to a candidate
// whose Log is at least as up to date as this Member's. That second rule is
// what keeps Committed Entries from being lost: a candidate missing one
// can't gather a Majority, because a Majority holds it.
func (n *Node) handleRequestVote(out *core.Output, from core.NodeID, m RequestVote) {
	if m.Term < n.term {
		n.send(out, from, VoteReply{Term: n.term})
		return
	}
	lastTerm := n.termAt(n.lastIndex())
	upToDate := m.LastTerm > lastTerm || (m.LastTerm == lastTerm && m.LastIndex >= n.lastIndex())
	granted := upToDate && (n.votedFor == 0 || n.votedFor == from)
	if granted {
		n.votedFor = from
		n.storeHardState(out)
		n.resetElection()
	}
	n.send(out, from, VoteReply{Term: n.term, Granted: granted})
}

func (n *Node) handleVoteReply(out *core.Output, from core.NodeID, m VoteReply) {
	if n.role != core.Candidate || m.Term != n.term || !m.Granted {
		return
	}
	n.votes[from] = true
	if len(n.votes) >= n.majority() {
		n.becomeLeader(out)
	}
}

// becomeLeader takes over the Term. The new Leader appends a no-op at once:
// committing an Entry of its own Term is the only way it may conclude that
// Entries from earlier Terms are Committed.
func (n *Node) becomeLeader(out *core.Output) {
	n.role = core.LeaderRole
	n.leader = n.id
	n.heartbeat = 0
	n.next = map[core.NodeID]core.Index{}
	n.match = map[core.NodeID]core.Index{}
	n.heard = map[core.NodeID]int{}
	n.sentSnap = map[core.NodeID]int{}
	n.roundAcked = map[core.NodeID]uint64{}
	n.roundOpen = false
	for _, m := range n.members {
		n.next[m] = n.lastIndex() + 1
		n.heard[m] = n.now
	}
	n.appendEntry(out, core.Entry{Index: n.lastIndex() + 1, Term: n.term, Kind: core.EntryNoop})
	for _, m := range n.members {
		if m != n.id {
			n.sendAppend(out, m)
		}
	}
	n.advanceCommit()
}
