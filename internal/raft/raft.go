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
	// ReadRound numbers the Leader's rounds of leadership confirmation
	// (read.go). The follower echoes it.
	ReadRound uint64
}

// AppendReply answers an Append. On success, Match is the last Index the
// follower now shares with the Leader. On failure it is a hint: the Leader
// should try again from Match+1.
type AppendReply struct {
	Term      core.Term
	Success   bool
	Match     core.Index
	ReadRound uint64
}

// InstallSnapshot carries the Leader's Snapshot to a follower that needs
// Entries the Leader's Log no longer holds (A§6.4). The follower answers
// with an AppendReply.
type InstallSnapshot struct {
	Term      core.Term
	Snapshot  core.Snapshot
	ReadRound uint64
}

// MessageBodies lists the types this core puts in a Message, for the
// transport to register.
func MessageBodies() []any {
	return []any{RequestVote{}, VoteReply{}, Append{}, AppendReply{}, InstallSnapshot{}}
}

// ReadMode is how a Member answers core.Read events (A§6.2).
type ReadMode uint8

const (
	// ReadsThroughLog: the shell sends reads as proposals, and never sends
	// core.Read. This is Rung 1's path.
	ReadsThroughLog ReadMode = iota
	// ReadsFromMemory: a Member that believes it leads says yes at once.
	// This is Rung 2's naive shortcut, wrong on purpose: a Leader that has
	// been replaced without knowing it hands out Stale reads.
	ReadsFromMemory
	// ReadsByIndex: Raft's read index (read.go). The Leader answers from
	// memory only once it has Committed an Entry of its own Term and a
	// Majority has confirmed, after the read arrived, that it still leads.
	ReadsByIndex
)

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
	Reads          ReadMode
	// Stored is the Member's durable state from before a restart. The zero
	// value is a Member starting for the first time.
	Stored core.Stored
	// NoSnapshotTransfer stops a Leader sending its Snapshot to a follower
	// that needs Entries the Log no longer holds. It exists so that Rung 3's
	// exposure of a Member that can't catch up stays reproducible.
	NoSnapshotTransfer bool
	// Volatile makes the Member store nothing, as before Rung 3. It exists so
	// that Rung 3's exposure of a store with no disk stays reproducible.
	Volatile bool
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

	log raftLog
	// snapshot is the latest Snapshot, kept to send to Members that need
	// Entries the Log no longer holds. Nil until one is taken or installed.
	snapshot *core.Snapshot
	commit   core.Index
	applied  core.Index // last Index handed to the shell in Output.Committed

	now     int // ticks since start
	elapsed int // ticks since a Leader was heard from, or the election began
	timeout int

	// Candidate state.
	votes map[core.NodeID]bool

	// Leader state.
	next      map[core.NodeID]core.Index // next Index to send each follower
	match     map[core.NodeID]core.Index // highest Index known to be on each
	heard     map[core.NodeID]int        // tick of each follower's last reply
	sentSnap  map[core.NodeID]int        // tick at which each was last sent the Snapshot
	heartbeat int                        // ticks since the last heartbeat
	pending   map[core.Index]uint64      // proposals awaiting commit, by Index

	// Read index state (read.go).
	readRound  uint64                 // the latest confirmation round sent
	roundOpen  bool                   // that round isn't confirmed yet
	roundAcked map[core.NodeID]uint64 // highest round each follower has echoed
	reads      []pendingRead
}

// New builds a Member as a follower, with whatever cfg.Stored says it had
// stored. What it stored is all it knows: the commit index starts again at
// its Snapshot, and Committed Entries after that are handed to the shell
// afresh as the Leader confirms them, so the shell rebuilds the state machine
// by applying them to the Snapshot.
func New(cfg Config) *Node {
	members := slices.Clone(cfg.Members)
	slices.Sort(members)
	n := &Node{id: cfg.ID, members: members, cfg: cfg, pending: map[core.Index]uint64{}}
	n.term, n.votedFor = cfg.Stored.HardState.Term, cfg.Stored.HardState.VotedFor
	if snap := cfg.Stored.Snapshot; snap != nil {
		n.snapshot = snap
		n.log.base, n.log.baseTerm = snap.Index, snap.Term
		n.commit, n.applied = snap.Index, snap.Index
	}
	n.log.entries = slices.Clone(cfg.Stored.Entries)
	n.cfg.Stored = core.Stored{} // not needed again; don't hold the Log twice
	n.resetElection()
	return n
}

// persist returns the Output's Persist, creating it on first use.
func persist(out *core.Output) *core.Persist {
	if out.Persist == nil {
		out.Persist = &core.Persist{}
	}
	return out.Persist
}

// storeHardState asks for the current Term and vote to be stored. It must
// follow every change to either.
func (n *Node) storeHardState(out *core.Output) {
	persist(out).HardState = &core.HardState{Term: n.term, VotedFor: n.votedFor}
}

// appendEntry adds an Entry to the Log and asks for it to be stored.
func (n *Node) appendEntry(out *core.Output, e core.Entry) {
	n.log.append(e)
	p := persist(out)
	p.Entries = append(p.Entries, e)
}

func (n *Node) Status() core.Status {
	return core.Status{ID: n.id, Role: n.role, Term: n.term, Leader: n.leader, Commit: n.commit}
}

func (n *Node) majority() int                 { return len(n.members)/2 + 1 }
func (n *Node) lastIndex() core.Index         { return n.log.last() }
func (n *Node) termAt(i core.Index) core.Term { return n.log.term(i) }

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
	case core.Read:
		n.read(&out, ev)
	case core.Snapshotted:
		n.snapshotted(&out, ev)
	}
	n.deliverCommitted(&out)
	n.releaseReads(&out)
	if n.cfg.Volatile {
		out.Persist = nil
	}
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
			n.sendHeartbeat(out, m)
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
		n.failReads(out)
	}
	if term > n.term {
		n.term = term
		n.votedFor = 0
		n.storeHardState(out)
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
	case InstallSnapshot:
		if m.Term > n.term {
			n.becomeFollower(out, m.Term, msg.From)
		}
		n.handleInstallSnapshot(out, msg.From, m)
	}
}

// deliverCommitted hands newly Committed Entries to the shell, and tells the
// clients whose proposals they were.
func (n *Node) deliverCommitted(out *core.Output) {
	for n.applied < n.commit {
		n.applied++
		out.Committed = append(out.Committed, n.log.entry(n.applied))
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
	n.appendEntry(out, core.Entry{Index: index, Term: n.term, Kind: core.EntryCommand, Payload: slices.Clone(p.Payload)})
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

// snapshotted takes a Snapshot from the shell and drops the Log it covers
// (A§6.4). The Snapshot is stored before the Entries are gone for good.
func (n *Node) snapshotted(out *core.Output, s core.Snapshotted) {
	if s.Index <= n.log.base || s.Index > n.applied {
		return // older than what we have, or of Entries never handed over
	}
	snap := &core.Snapshot{Index: s.Index, Term: n.termAt(s.Index), Data: s.Data}
	n.log.compactTo(s.Index)
	n.snapshot = snap
	persist(out).Snapshot = snap
}

// read rules on a read that bypasses the Log.
func (n *Node) read(out *core.Output, r core.Read) {
	switch {
	case n.cfg.Reads == ReadsThroughLog:
		panic("raft: core.Read sent to a Member configured for reads through the Log")
	case n.role == core.LeaderRole && n.cfg.Reads == ReadsByIndex:
		n.queueRead(out, r)
	case n.role == core.LeaderRole:
		out.Reads = append(out.Reads, core.Result{Ref: r.Ref, Reason: core.OK})
	case n.role == core.Follower && n.leader != 0:
		out.Reads = append(out.Reads, core.Result{Ref: r.Ref, Reason: core.NotLeader, Leader: n.leader})
	default:
		out.Reads = append(out.Reads, core.Result{Ref: r.Ref, Reason: core.NoMajority})
	}
}

func (n *Node) String() string {
	return fmt.Sprintf("node %d: %v term %d, log %d, commit %d", n.id, n.role, n.term, n.lastIndex(), n.commit)
}
