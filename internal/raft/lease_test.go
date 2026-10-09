package raft

import (
	"testing"

	"distributed-kv-store/internal/core"
)

func newLeaseNode(id core.NodeID) *Node {
	n := newNode(id, 3)
	n.cfg.Reads = ReadsByLease // newNode's ElectionTicks is 10, so a lease lasts 8
	return n
}

// lastSent is the Sent stamp of the latest Append in out addressed to to.
func lastSent(t *testing.T, out core.Output, to core.NodeID) int {
	t.Helper()
	sent := -1
	for _, m := range out.Messages {
		if a, ok := m.Body.(Append); ok && m.To == to {
			sent = a.Sent
		}
	}
	if sent < 0 {
		t.Fatalf("no Append to node %d in %+v", to, out.Messages)
	}
	return sent
}

func TestLeaseAnswersWithoutAskingAndThenLapses(t *testing.T) {
	n := newLeaseNode(1)
	elect(t, n, 2)
	// The no-op commits, but that reply echoes nothing: there is no lease.
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1})
	out := n.Step(core.Read{Ref: 1})
	if len(out.Reads) != 0 || len(rounds(out)) == 0 {
		t.Fatalf("with no lease the read must wait for a round: reads %+v, messages %+v", out.Reads, out.Messages)
	}
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1, ReadRound: 1})

	// A heartbeat goes out and node 2 answers it. The lease runs from when
	// it was sent.
	sent := lastSent(t, n.Step(core.Tick{}), 2)
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1, Sent: sent})
	out = n.Step(core.Read{Ref: 2})
	if len(out.Reads) != 1 || out.Reads[0] != (core.Result{Ref: 2, Reason: core.OK}) || len(out.Messages) != 0 {
		t.Fatalf("under a lease: reads %+v, messages %+v; want an answer and nothing sent", out.Reads, out.Messages)
	}

	// Seven ticks on, the lease has one tick left. node 3 keeps the Leader
	// from stepping down, without echoing anything.
	for range 7 {
		n.Step(core.Tick{})
		recv(n, 3, AppendReply{Term: 1, Success: true, Match: 1})
	}
	if out = n.Step(core.Read{Ref: 3}); len(out.Reads) != 1 {
		t.Fatalf("one tick before the lease ends: %+v", out.Reads)
	}
	n.Step(core.Tick{})
	recv(n, 3, AppendReply{Term: 1, Success: true, Match: 1})
	if out = n.Step(core.Read{Ref: 4}); len(out.Reads) != 0 || len(rounds(out)) == 0 {
		t.Fatalf("after the lease ended the read must wait for a round: reads %+v, messages %+v", out.Reads, out.Messages)
	}
}

// A follower that has heard from a Leader promises its vote to nobody else
// for ElectionTicks. So does a Member that has only just started.
func TestPromiseNotToVote(t *testing.T) {
	n := newLeaseNode(1)
	if v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 1})); v.Granted {
		t.Fatal("a Member that has just started voted at once")
	}
	for range 10 {
		n.elapsed = 0 // keep it from standing itself
		n.Step(core.Tick{})
	}
	if v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 2})); !v.Granted {
		t.Fatal("ElectionTicks after starting, the vote should be free")
	}

	recv(n, 2, Append{Term: 2})
	for range 9 {
		n.elapsed = 0
		n.Step(core.Tick{})
	}
	if v := reply[VoteReply](t, recv(n, 3, RequestVote{Term: 3})); v.Granted {
		t.Fatal("voted 9 ticks after hearing from a Leader")
	}
	n.elapsed = 0
	n.Step(core.Tick{})
	if v := reply[VoteReply](t, recv(n, 3, RequestVote{Term: 3})); !v.Granted {
		t.Fatal("10 ticks after hearing from a Leader the vote should be free")
	}
}

// Without the lease switch nothing changes: no promise, no stamps.
func TestNoPromiseWithoutLease(t *testing.T) {
	n := newNode(1, 3)
	if v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 1})); !v.Granted {
		t.Fatal("a read-index Member refused a vote it was free to give")
	}
	l := newReadIndexLeader(t)
	for _, m := range l.Step(core.Tick{}).Messages {
		if a, ok := m.Body.(Append); ok && a.Sent != 0 {
			t.Fatalf("a read-index Leader stamped an Append: %+v", a)
		}
	}
}
