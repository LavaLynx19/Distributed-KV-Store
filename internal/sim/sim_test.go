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
func (e *echo) Read(query []byte) []byte   { return query }
func (e *echo) Capture() func() []byte     { return func() []byte { return nil } }
func (e *echo) Restore([]byte) error       { return nil }

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

func TestCutIsOneWay(t *testing.T) {
	s, relays := newRelaySim(13, 2)
	s.Cut([]core.NodeID{1}, []core.NodeID{2}) // 1 can't reach 2; 2 still reaches 1
	s.Propose(1, []byte("x"), func(Reply) {})
	s.Propose(2, []byte("y"), func(Reply) {})
	s.Run(100)
	if relays[2].received != 0 || relays[1].received != 1 {
		t.Fatalf("node 2 got %d (want 0), node 1 got %d (want 1)", relays[2].received, relays[1].received)
	}
	if s.Reachable(1, 2) || !s.Reachable(2, 1) {
		t.Fatal("Reachable disagrees with the cut")
	}
	s.Heal()
	s.Propose(1, []byte("z"), func(Reply) {})
	s.Run(200)
	if relays[2].received != 1 {
		t.Fatalf("after healing node 2 got %d, want 1", relays[2].received)
	}
}

func TestDuplicateDeliversSomeTwice(t *testing.T) {
	s, relays := newRelaySim(17, 2)
	s.SetDuplicate(0.5)
	const sent = 200
	for i := range sent {
		s.At(int64(i*20), func() { s.Propose(1, []byte("m"), func(Reply) {}) })
	}
	s.Run(sent*20 + 100)
	if got := relays[2].received; got <= sent || got >= 2*sent {
		t.Fatalf("with 50%% duplication, node 2 received %d for %d sent", got, sent)
	}
}

// order records the payloads a Member receives, in arrival order.
type order struct {
	relay
	got []byte
}

func (o *order) Step(ev core.Event) core.Output {
	if r, ok := ev.(core.Receive); ok {
		o.got = append(o.got, r.Msg.Body.([]byte)[0])
	}
	return o.relay.Step(ev)
}

func TestWideDelayReorders(t *testing.T) {
	var receiver *order
	s := New(Config{
		Seed: 19, Nodes: 2,
		NewNode: func(id core.NodeID, members []core.NodeID, _ core.Rand) core.Node {
			o := &order{relay: relay{id: id, members: members}}
			if id == 2 {
				receiver = o
			}
			return o
		},
		NewMachine: func() Machine { return &echo{} },
	})
	s.SetDelay(1, 200)
	for i := range 50 {
		s.At(int64(i*10), func() { s.Propose(1, []byte{byte(i)}, func(Reply) {}) })
	}
	s.Run(2000)
	if len(receiver.got) != 50 {
		t.Fatalf("received %d of 50", len(receiver.got))
	}
	inOrder := true
	for i := 1; i < len(receiver.got); i++ {
		if receiver.got[i] < receiver.got[i-1] {
			inOrder = false
		}
	}
	if inOrder {
		t.Fatal("a 1..200 delay with sends 10 apart should let later messages overtake earlier ones")
	}
}

// journal is a test core that stores every proposal before acknowledging it.
type journal struct {
	id    core.NodeID
	index core.Index
	ticks int
}

func (j *journal) Status() core.Status { return core.Status{ID: j.id, Commit: j.index} }

func (j *journal) Step(ev core.Event) core.Output {
	var out core.Output
	switch ev := ev.(type) {
	case core.Tick:
		j.ticks++
	case core.Propose:
		j.index++
		e := core.Entry{Index: j.index, Kind: core.EntryCommand, Payload: ev.Payload}
		out.Persist = &core.Persist{Entries: []core.Entry{e}}
		out.Committed = []core.Entry{e}
		out.Results = []core.Result{{Ref: ev.Ref, Reason: core.OK, Index: e.Index}}
	}
	return out
}

func newJournalSim(seed uint64, delay [2]int64) (*Sim, map[core.NodeID]*journal) {
	journals := map[core.NodeID]*journal{}
	s := New(Config{
		Seed: seed, Nodes: 1, DiskDelay: delay,
		NewNode: func(id core.NodeID, _ []core.NodeID, _ core.Rand) core.Node {
			journals[id] = &journal{id: id}
			return journals[id]
		},
		Restart: func(id core.NodeID, _ []core.NodeID, _ core.Rand, stored core.Stored) core.Node {
			journals[id] = &journal{id: id, index: core.Index(len(stored.Entries))}
			return journals[id]
		},
		NewMachine: func() Machine { return &echo{} },
	})
	return s, journals
}

func TestAnswerWaitsForTheDisk(t *testing.T) {
	s, _ := newJournalSim(1, [2]int64{30, 30})
	var got []Reply
	s.At(10, func() { s.Propose(1, []byte("a"), func(r Reply) { got = append(got, r) }) })
	s.Run(39)
	if len(got) != 0 || len(s.Disk(1).Entries) != 0 {
		t.Fatalf("at t=39 the write (started t=10, 30 units) isn't done, yet: %d replies, %d Entries on disk", len(got), len(s.Disk(1).Entries))
	}
	s.Run(41)
	if len(got) != 1 || got[0].Reason != core.OK || len(s.Disk(1).Entries) != 1 {
		t.Fatalf("at t=41: %+v, %d Entries on disk", got, len(s.Disk(1).Entries))
	}
}

func TestEventsQueueBehindAWrite(t *testing.T) {
	s, journals := newJournalSim(2, [2]int64{50, 50})
	var order []string
	s.At(5, func() { s.Propose(1, []byte("a"), func(Reply) { order = append(order, "a") }) })
	s.At(6, func() { s.Propose(1, []byte("b"), func(Reply) { order = append(order, "b") }) })
	s.Run(54)
	if journals[1].index != 1 {
		t.Fatalf("the second proposal was handled while the first write was in progress (index %d)", journals[1].index)
	}
	s.Run(200)
	if len(order) != 2 || order[0] != "a" || order[1] != "b" || len(s.Disk(1).Entries) != 2 {
		t.Fatalf("order %v with %d Entries on disk; want a then b, 2 Entries", order, len(s.Disk(1).Entries))
	}
	if journals[1].ticks == 0 {
		t.Fatal("ticks that arrived during the writes were dropped")
	}
}

func TestCrashLosesTheWriteInProgress(t *testing.T) {
	s, journals := newJournalSim(3, [2]int64{30, 30})
	var first, second Reply
	s.At(10, func() { s.Propose(1, []byte("kept"), func(r Reply) { first = r }) })
	s.At(50, func() { s.Propose(1, []byte("lost"), func(r Reply) { second = r }) })
	s.At(60, func() { s.Crash(1) }) // the second write would finish at t=80
	s.At(100, func() { s.Restart(1) })
	s.Run(200)

	if first.Reason != core.OK {
		t.Fatalf("first write: %+v", first)
	}
	if second.Reason != core.Unknown {
		t.Fatalf("a proposal whose write was cut short should end Unknown, got %+v", second)
	}
	if got := s.Disk(1).Entries; len(got) != 1 || string(got[0].Payload) != "kept" {
		t.Fatalf("disk holds %+v, want only the first Entry", got)
	}
	if journals[1].index != 1 {
		t.Fatalf("the restarted core should know 1 Entry, has %d", journals[1].index)
	}
}

func TestStalledDiskHoldsWrites(t *testing.T) {
	s, _ := newJournalSim(4, [2]int64{})
	var at int64 = -1
	s.At(10, func() { s.StallDisk(1, 100) })
	s.At(20, func() { s.Propose(1, []byte("a"), func(Reply) { at = s.Now() }) })
	s.Run(300)
	if at != 110 {
		t.Fatalf("answered at t=%d, want t=110 (when the stall ends)", at)
	}
}

// Without Config.Restart a crash is still a freeze, as Rungs 1–2 rely on.
func TestNilRestartKeepsMemory(t *testing.T) {
	s, relays := newRelaySim(5, 1)
	s.Propose(1, []byte("x"), func(Reply) {})
	s.Crash(1)
	s.Restart(1)
	s.Run(100)
	if relays[1].index != 1 {
		t.Fatalf("a frozen Member lost its held proposal (index %d)", relays[1].index)
	}
}
