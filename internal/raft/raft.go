// Package raft is the consensus core (A§2.1, A§4.2): Leader election and
// Majority replication of the Log, as a pure step function. It keeps no
// clock, starts no goroutine and touches no socket or file.
//
// Rung 1 scope: a fixed set of Members, nothing durable, no Snapshots, and
// every client request (reads included) goes through the Log.
package raft

import (
	"fmt"
	"slices"

	"distributed-kv-store/internal/core"
)

// RequestVote asks a Member for its vote in Term.
type RequestVote struct {
	Term      core.Term
	LastIndex core.Index
	LastTerm  core.Term
}

// VoteReply answers a RequestVote.
type VoteReply struct {
	Term    core.Term
	Granted bool
}

// Append carries Entries from the Leader, or nothing as a heartbeat.
// PrevIndex and PrevTerm name the Entry just before them, which the follower
// must already hold.
type Append struct {
	Term      core.Term
	PrevIndex core.Index
	PrevTerm  core.Term
	Entries   []core.Entry
	Commit    core.Index
}

// AppendReply answers an Append. On success, Match is the last Index the
// follower now shares with the Leader. On failure it is a hint: the Leader
// should try again from Match+1.
type AppendReply struct {
	Term    core.Term
	Success bool
	Match   core.Index
}

// Config sets up one Member.
type Config struct {
	ID      core.NodeID
	Members []core.NodeID
	// A follower starts an election after ElectionTicks to 2×ElectionTicks−1
	// ticks without hearing from a Leader. A Leader that hasn't heard from a
	// Majority for ElectionTicks steps down.
	ElectionTicks int
	// A Leader sends a heartbeat every HeartbeatTicks.
	HeartbeatTicks int
	Rand           core.Rand
}

// maxBatch caps the Entries in one Append.
const maxBatch = 64

// Node is one Member's consensus state.
type Node struct {
	id      core.NodeID
	members []core.NodeID // ascending
	cfg     Config

	term     core.Term
	votedFor core.NodeID
	role     core.Role
	leader   core.NodeID

	log     []core.Entry // log[i] holds Index i+1
	commit  core.Index
	applied core.Index // last Index handed to the shell in Output.Committed

	now     int // ticks since start
	elapsed int // ticks since a Leader was heard from, or the election began
	timeout int

	// Candidate state.
	votes map[core.NodeID]bool

	// Leader state.
	next      map[core.NodeID]core.Index // next Index to send each follower
	match     map[core.NodeID]core.Index // highest Index known to be on each
	heard     map[core.NodeID]int        // tick of each follower's last reply
	heartbeat int                        // ticks since the last heartbeat
	pending   map[core.Index]uint64      // proposals awaiting commit, by Index
}

// New builds a Member, which starts as a follower in Term 0.
func New(cfg Config) *Node {
	members := slices.Clone(cfg.Members)
	slices.Sort(members)
	n := &Node{id: cfg.ID, members: members, cfg: cfg, pending: map[core.Index]uint64{}}
	n.resetElection()
	return n
}

func (n *Node) Status() core.Status {
	return core.Status{ID: n.id, Role: n.role, Term: n.term, Leader: n.leader, Commit: n.commit}
}

func (n *Node) majority() int         { return len(n.members)/2 + 1 }
func (n *Node) lastIndex() core.Index { return core.Index(len(n.log)) }

// termAt is the Term of the Entry at i, or 0 for the position before the Log.
func (n *Node) termAt(i core.Index) core.Term {
	if i == 0 {
		return 0
	}
	return n.log[i-1].Term
}

func (n *Node) resetElection() {
	n.elapsed = 0
	n.timeout = n.cfg.ElectionTicks + int(n.cfg.Rand.Uint64()%uint64(n.cfg.ElectionTicks))
}

// Step handles one event. Committed Entries and the Results of proposals
// they settle leave in the same Output.
func (n *Node) Step(ev core.Event) core.Output {
	var out core.Output
	switch ev := ev.(type) {
	case core.Tick:
		n.tick(&out)
	case core.Receive:
		n.receive(&out, ev.Msg)
	case core.Propose:
		n.propose(&out, ev)
	}
	n.deliverCommitted(&out)
	return out
}

func (n *Node) tick(out *core.Output) {
	n.now++
	if n.role != core.LeaderRole {
		if n.elapsed++; n.elapsed >= n.timeout {
			n.startElection(out)
		}
		return
	}

	// A Leader cut off from a Majority can't commit anything. It steps down
	// so its clients get an answer instead of waiting.
	inTouch := 1
	for _, m := range n.members {
		if m != n.id && n.now-n.heard[m] <= n.cfg.ElectionTicks {
			inTouch++
		}
	}
	if inTouch < n.majority() {
		n.becomeFollower(out, n.term, 0)
		return
	}

	if n.heartbeat++; n.heartbeat >= n.cfg.HeartbeatTicks {
		n.heartbeat = 0
		for _, m := range n.members {
			if m == n.id {
				continue
			}
			// Anything sent but not yet confirmed is sent again.
			n.next[m] = n.match[m] + 1
			n.sendAppend(out, m)
		}
	}
}

func (n *Node) send(out *core.Output, to core.NodeID, body any) {
	out.Messages = append(out.Messages, core.Message{From: n.id, To: to, Body: body})
}

// becomeFollower moves to term (if newer) under leader, which may be 0 when
// it isn't known yet. A Leader stepping down can no longer vouch for its
// uncommitted proposals.
//
// A Member that was already a follower keeps its election timer running.
// Only a granted vote or word from the Leader resets it. Otherwise a
// candidate that can never win (its Log is behind) would keep postponing the
// elections of the Members that can.
func (n *Node) becomeFollower(out *core.Output, term core.Term, leader core.NodeID) {
	if n.role == core.LeaderRole {
		n.failPending(out)
	}
	if term > n.term {
		n.term = term
		n.votedFor = 0
	}
	if n.role != core.Follower {
		n.resetElection()
	}
	n.role = core.Follower
	n.leader = leader
}

func (n *Node) failPending(out *core.Output) {
	indexes := make([]core.Index, 0, len(n.pending))
	for i := range n.pending {
		indexes = append(indexes, i)
	}
	slices.Sort(indexes)
	for _, i := range indexes {
		out.Results = append(out.Results, core.Result{Ref: n.pending[i], Reason: core.Unknown})
	}
	clear(n.pending)
}

// receive handles a message from another Member. A message from a newer Term
// always makes this Member a follower in that Term first.
func (n *Node) receive(out *core.Output, msg core.Message) {
	switch m := msg.Body.(type) {
	case RequestVote:
		if m.Term > n.term {
			n.becomeFollower(out, m.Term, 0)
		}
		n.handleRequestVote(out, msg.From, m)
	case VoteReply:
		if m.Term > n.term {
			n.becomeFollower(out, m.Term, 0)
		}
		n.handleVoteReply(out, msg.From, m)
	case Append:
		if m.Term > n.term {
			n.becomeFollower(out, m.Term, msg.From)
		}
		n.handleAppend(out, msg.From, m)
	case AppendReply:
		if m.Term > n.term {
			n.becomeFollower(out, m.Term, 0)
		}
		n.handleAppendReply(out, msg.From, m)
	}
}

// deliverCommitted hands newly Committed Entries to the shell, and tells the
// clients whose proposals they were.
func (n *Node) deliverCommitted(out *core.Output) {
	for n.applied < n.commit {
		n.applied++
		out.Committed = append(out.Committed, n.log[n.applied-1])
		if ref, ok := n.pending[n.applied]; ok {
			delete(n.pending, n.applied)
			out.Results = append(out.Results, core.Result{Ref: ref, Reason: core.OK, Index: n.applied})
		}
	}
}

// propose takes a client command if this Member leads, and says why not
// otherwise.
func (n *Node) propose(out *core.Output, p core.Propose) {
	switch {
	case n.role == core.LeaderRole:
	case n.role == core.Follower && n.leader != 0:
		out.Results = append(out.Results, core.Result{Ref: p.Ref, Reason: core.NotLeader, Leader: n.leader})
		return
	default:
		out.Results = append(out.Results, core.Result{Ref: p.Ref, Reason: core.NoMajority})
		return
	}

	index := n.lastIndex() + 1
	n.log = append(n.log, core.Entry{Index: index, Term: n.term, Kind: core.EntryCommand, Payload: slices.Clone(p.Payload)})
	n.pending[index] = p.Ref
	for _, m := range n.members {
		// A follower that has been sent everything so far gets the new Entry
		// now. One that is behind gets it with the next heartbeat.
		if m != n.id && n.next[m] == index {
			n.sendAppend(out, m)
		}
	}
	n.advanceCommit()
}

func (n *Node) String() string {
	return fmt.Sprintf("node %d: %v term %d, log %d, commit %d", n.id, n.role, n.term, n.lastIndex(), n.commit)
}
