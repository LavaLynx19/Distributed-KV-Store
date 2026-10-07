package sim

import (
	"testing"

	"distributed-kv-store/internal/core"
)

// relay is a test core with no consensus at all. It holds each proposal until
// its next tick, then commits it locally and tells every other Member.
type relay struct {
	id       core.NodeID
	members  []core.NodeID
	held     []core.Propose
	index    core.Index
	ticks    int
	received int
}

func (r *relay) Status() core.Status { return core.Status{ID: r.id, Commit: r.index} }

func (r *relay) Step(ev core.Event) core.Output {
	var out core.Output
	switch ev := ev.(type) {
	case core.Propose:
		r.held = append(r.held, ev)
	case core.Receive:
		r.received++
	case core.Tick:
		r.ticks++
		for _, p := range r.held {
			r.index++
			out.Committed = append(out.Committed, core.Entry{Index: r.index, Kind: core.EntryCommand, Payload: p.Payload})
			out.Results = append(out.Results, core.Result{Ref: p.Ref, Reason: core.OK, Index: r.index})
			for _, to := range r.members {
				if to != r.id {
					out.Messages = append(out.Messages, core.Message{From: r.id, To: to, Body: p.Payload})
				}
			}
		}
		r.held = nil
	}
	return out
}

// echo is a state machine that answers each Entry with its own payload.
type echo struct{ applied int }

func (e *echo) Apply(en core.Entry) []byte { e.applied++; return en.Payload }

func newRelaySim(seed uint64, n int) (*Sim, map[core.NodeID]*relay) {
	relays := map[core.NodeID]*relay{}
	s := New(Config{
		Seed:  seed,
		Nodes: n,
		NewNode: func(id core.NodeID, members []core.NodeID, _ core.Rand) core.Node {
			relays[id] = &relay{id: id, members: members}
			return relays[id]
		},
		NewMachine: func() Machine { return &echo{} },
	})
	return s, relays
}

// script drives a run with some of everything: proposals, a crash, a
// Partition, and their repair.
func script(s *Sim) (replies []Reply) {
	propose := func(to core.NodeID, payload string) {
		s.Propose(to, []byte(payload), func(r Reply) { replies = append(replies, r) })
	}
	s.At(5, func() { propose(1, "a") })
	s.At(30, func() { s.Partition([]core.NodeID{1}, []core.NodeID{2, 3}) })
	s.At(40, func() { propose(1, "b"); propose(2, "c") })
	s.At(70, func() { s.Heal(); s.Crash(3) })
	s.At(80, func() { propose(2, "d"); propose(3, "e") })
	s.At(120, func() { s.Restart(3) })
	s.At(130, func() { propose(3, "f") })
	s.Run(300)
	return replies
}

func TestSameSeedSameRun(t *testing.T) {
	a, _ := newRelaySim(42, 3)
	b, _ := newRelaySim(42, 3)
	ra, rb := script(a), script(b)
	if a.Digest() != b.Digest() {
		t.Fatalf("same seed, different digests: %x vs %x", a.Digest(), b.Digest())
	}
	if len(ra) != len(rb) {
		t.Fatalf("same seed, %d vs %d replies", len(ra), len(rb))
	}

	c, _ := newRelaySim(43, 3)
	script(c)
	if c.Digest() == a.Digest() {
		t.Error("different seeds gave the same digest")
	}
}

func TestProposalRepliesOnce(t *testing.T) {
	s, _ := newRelaySim(1, 3)
	var got []Reply
	s.Propose(2, []byte("hello"), func(r Reply) { got = append(got, r) })
	if len(got) != 0 {
		t.Fatal("replied before the core decided")
	}
	s.Run(50)
	if len(got) != 1 || got[0].Reason != core.OK || string(got[0].Response) != "hello" {
		t.Fatalf("got %+v, want one OK reply carrying the state machine's response", got)
	}
}

func TestPartitionCutsBothWaysUntilHealed(t *testing.T) {
	s, relays := newRelaySim(7, 3)
	s.Partition([]core.NodeID{1}, []core.NodeID{2, 3})
	s.Propose(1, []byte("x"), func(Reply) {})
	s.Propose(2, []byte("y"), func(Reply) {})
	s.Run(100)
	if relays[2].received != 0 || relays[1].received != 0 {
		t.Fatalf("messages crossed the Partition: node1 got %d, node2 got %d", relays[1].received, relays[2].received)
	}
	if relays[3].received != 1 {
		t.Fatalf("node 3 should hear node 2 inside its side, got %d", relays[3].received)
	}

	s.Heal()
	s.Propose(1, []byte("z"), func(Reply) {})
	s.Run(200)
	if relays[2].received != 1 || relays[3].received != 2 {
		t.Fatalf("after healing: node2 got %d (want 1), node3 got %d (want 2)", relays[2].received, relays[3].received)
	}
}

func TestCrashedMemberIsSilentAndKeepsState(t *testing.T) {
	s, relays := newRelaySim(9, 3)

	var pending, refused Reply
	s.Propose(3, []byte("lost"), func(r Reply) { pending = r })
	s.Crash(3)
	if pending.Reason != core.Unknown || pending.Refused {
		t.Fatalf("a proposal pending at the crash should end Unknown, got %+v", pending)
	}
	s.Propose(3, []byte("no"), func(r Reply) { refused = r })
	if !refused.Refused {
		t.Fatalf("a proposal to a down Member should be refused, got %+v", refused)
	}

	s.Propose(1, []byte("x"), func(Reply) {})
	s.Run(100)
	if relays[3].ticks != 0 || relays[3].received != 0 {
		t.Fatalf("a crashed Member got %d ticks and %d messages", relays[3].ticks, relays[3].received)
	}

	s.Restart(3)
	s.Run(200)
	if relays[3].ticks == 0 {
		t.Fatal("a restarted Member never ticked")
	}
	if got := s.Status(3).Commit; got != 1 {
		t.Fatalf("state should survive a crash in Rungs 1-2: commit = %d, want 1 (the held proposal)", got)
	}
}

func TestTimeAndOrder(t *testing.T) {
	s, _ := newRelaySim(3, 1)
	var order []int
	s.At(20, func() { order = append(order, 2) })
	s.At(10, func() { order = append(order, 1) })
	s.At(20, func() { order = append(order, 3) })
	s.Run(15)
	if s.Now() != 15 || len(order) != 1 {
		t.Fatalf("at t=15: now=%d ran=%v", s.Now(), order)
	}
	s.Run(25)
	if len(order) != 3 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("equal-time events must run in scheduling order, got %v", order)
	}
}

func TestLossDropsSomeMessages(t *testing.T) {
	s, relays := newRelaySim(11, 2)
	s.SetLoss(0.5)
	const sent = 200
	for i := range sent {
		s.At(int64(i*20), func() { s.Propose(1, []byte("m"), func(Reply) {}) })
	}
	s.Run(sent*20 + 100)
	if got := relays[2].received; got == 0 || got == sent {
		t.Fatalf("with 50%% loss, node 2 received %d of %d", got, sent)
	}

	s.SetLoss(0)
	before := relays[2].received
	s.Propose(1, []byte("m"), func(Reply) {})
	s.Run(s.Now() + 100)
	if relays[2].received != before+1 {
		t.Fatal("with loss off, a message was still lost")
	}
}
