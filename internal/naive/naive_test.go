package naive_test

import (
	"reflect"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/naive"
	"distributed-kv-store/internal/sim"
)

func newSim(seed uint64, n int) *sim.Sim {
	return sim.New(sim.Config{
		Seed:  seed,
		Nodes: n,
		NewNode: func(id core.NodeID, members []core.NodeID, _ core.Rand) core.Node {
			return naive.New(id, members, naive.DefaultTimeout)
		},
		NewMachine: func() sim.Machine { return fsm.New() },
	})
}

func put(k, v string) []byte {
	return fsm.Command{Op: fsm.OpPut, Key: k, Value: []byte(v)}.Encode()
}

func items(s *sim.Sim, id core.NodeID) []fsm.Item { return s.Machine(id).(*fsm.Machine).Items() }

// With no Faults, the naive store works: that's what makes it tempting.
func TestWorksWithoutFaults(t *testing.T) {
	s := newSim(1, 3)
	var replies []sim.Reply
	for i, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"a", "3"}} {
		s.At(int64(20+i*20), func() {
			s.Propose(1, put(kv[0], kv[1]), func(r sim.Reply) { replies = append(replies, r) })
		})
	}
	s.Run(500)

	if len(replies) != 3 {
		t.Fatalf("got %d replies, want 3", len(replies))
	}
	for _, r := range replies {
		if r.Reason != core.OK {
			t.Fatalf("reply %+v, want OK", r)
		}
	}
	want := []fsm.Item{{Key: "a", Value: []byte("3"), Version: 3}, {Key: "b", Value: []byte("2"), Version: 2}}
	for _, id := range s.IDs() {
		if got := items(s, id); !reflect.DeepEqual(got, want) {
			t.Errorf("node %d holds %+v, want %+v", id, got, want)
		}
	}
}

func TestBackupRedirectsToPrimary(t *testing.T) {
	s := newSim(2, 3)
	var got sim.Reply
	s.At(20, func() { s.Propose(3, put("a", "1"), func(r sim.Reply) { got = r }) })
	s.Run(100)
	if got.Reason != core.NotLeader || got.Leader != 1 {
		t.Fatalf("a backup answered %+v, want NotLeader with hint 1", got)
	}
}

func TestBackupTakesOverAfterTimeout(t *testing.T) {
	s := newSim(3, 3)
	s.At(50, func() { s.Crash(1) })
	s.Run(60)
	if st := s.Status(2); st.Role == core.LeaderRole {
		t.Fatal("node 2 took over before the timeout")
	}
	s.Run(300)
	if st := s.Status(2); st.Role != core.LeaderRole {
		t.Fatalf("node 2 should be primary once node 1 has been silent, got %+v", st)
	}
	if st := s.Status(3); st.Leader != 2 {
		t.Fatalf("node 3 should follow node 2, got %+v", st)
	}
}
