package gossip

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"distributed-kv-store/internal/shard"
)

// net is a handful of Nodes and a network between them that delivers at
// once, unless told to lose a Message.
type net struct {
	nodes map[int]*Node
	down  map[int]bool
	// lose reports whether a Message from one Node to another is lost.
	lose func(from, to int) bool
	sent int
}

func addr(id int) string { return fmt.Sprintf("10.0.0.%d:7000", id) }

func (nw *net) start(id int, d Detector, seeds ...int) {
	cfg := Config{ID: id, Peer: addr(id), Client: addr(id) + "0", Rand: rand.New(rand.NewPCG(uint64(id), 7)), Detector: d}
	for _, s := range seeds {
		cfg.Seeds = append(cfg.Seeds, Member{ID: s, Peer: addr(s)})
	}
	nw.nodes[id] = New(cfg)
	delete(nw.down, id)
}

// newNet starts n Nodes that each know only Node 1.
func newNet(n int, d Detector) *net {
	nw := &net{nodes: map[int]*Node{}, down: map[int]bool{}}
	for id := 1; id <= n; id++ {
		nw.start(id, d, 1)
	}
	return nw
}

func (nw *net) deliver(msgs []Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		msgs = msgs[1:]
		nw.sent++
		if nw.down[m.To] || nw.down[m.From] || nw.nodes[m.To] == nil || (nw.lose != nil && nw.lose(m.From, m.To)) {
			continue
		}
		msgs = append(msgs, nw.nodes[m.To].Receive(m)...)
	}
}

func (nw *net) rounds(n int) {
	for range n {
		for id := 1; id <= len(nw.nodes); id++ {
			if !nw.down[id] {
				nw.deliver(nw.nodes[id].Tick())
			}
		}
	}
}

// views is what each running Node thinks of Node id.
func (nw *net) views(id int) map[int]Status {
	out := map[int]Status{}
	for at, n := range nw.nodes {
		if nw.down[at] || at == id {
			continue
		}
		for _, m := range n.Members() {
			if m.ID == id {
				out[at] = m.Status
			}
		}
	}
	return out
}

func (nw *net) all(t *testing.T, id int, want Status, when string) {
	t.Helper()
	views := nw.views(id)
	if len(views) == 0 {
		t.Fatalf("%s: nobody has heard of node %d", when, id)
	}
	for at, got := range views {
		if got != want {
			t.Fatalf("%s: node %d thinks node %d is %v, want %v (all: %v)", when, at, id, got, want, views)
		}
	}
}

var detectors = map[string]Detector{"counters": Counters, "swim": SWIM}

func TestEveryNodeLearnsOfEveryNodeFromOneSeed(t *testing.T) {
	for name, d := range detectors {
		nw := newNet(6, d)
		nw.rounds(10)
		for id, n := range nw.nodes {
			ms := n.Members()
			if len(ms) != 6 {
				t.Fatalf("%s: node %d knows %d Nodes, want 6", name, id, len(ms))
			}
			for _, m := range ms {
				if m.Peer != addr(m.ID) || m.Status != Alive {
					t.Fatalf("%s: node %d has %+v", name, id, m)
				}
			}
		}
	}
}

func TestAQuietNodeIsSuspectedThenDeadThenBack(t *testing.T) {
	for name, d := range detectors {
		nw := newNet(5, d)
		nw.rounds(10)
		nw.down[3] = true
		nw.rounds(2)
		nw.all(t, 3, Alive, name+": 2 rounds after it stopped")
		nw.rounds(40)
		nw.all(t, 3, Dead, name+": 42 rounds after it stopped")
		for id := range nw.nodes {
			if id != 3 && !nw.down[id] {
				nw.all(t, id, Alive, name+": a Node that is fine")
			}
		}
		// It restarts having forgotten everything, and is taken back.
		nw.start(3, d, 1)
		nw.rounds(20)
		nw.all(t, 3, Alive, name+": after it restarted")
	}
}

// One bad link is not a dead Node: news of it travels round the link, and
// SWIM asks others to ping across it.
func TestOneCutLinkSuspectsNobody(t *testing.T) {
	for name, d := range detectors {
		nw := newNet(5, d)
		nw.rounds(10)
		nw.lose = func(from, to int) bool { return (from == 1 && to == 2) || (from == 2 && to == 1) }
		for round := range 60 {
			nw.rounds(1)
			for _, id := range []int{1, 2} {
				for at, got := range nw.views(id) {
					if got != Alive {
						t.Fatalf("%s: round %d: node %d thinks node %d is %v", name, round, at, id, got)
					}
				}
			}
		}
	}
}

// A Partition: each side takes the other for dead, and when it heals they
// find each other again.
func TestPartitionAndHeal(t *testing.T) {
	for name, d := range detectors {
		nw := newNet(5, d)
		nw.rounds(10)
		side := func(id int) bool { return id <= 2 }
		nw.lose = func(from, to int) bool { return side(from) != side(to) }
		nw.rounds(60)
		if got := nw.nodes[1].Members()[4].Status; got != Dead {
			t.Fatalf("%s: across the Partition node 1 thinks node 5 is %v", name, got)
		}
		if got := nw.nodes[1].Members()[1].Status; got != Alive {
			t.Fatalf("%s: on its own side node 1 thinks node 2 is %v", name, got)
		}
		nw.lose = nil
		nw.rounds(40)
		for id := 1; id <= 5; id++ {
			nw.all(t, id, Alive, name+": after the Partition healed")
		}
	}
}

// SWIM: a Node wrongly suspected hears of it and says otherwise, with an
// incarnation that outranks the suspicion.
func TestASuspicionIsAnswered(t *testing.T) {
	nw := newNet(5, SWIM)
	nw.rounds(10)
	e := nw.nodes[1].members[4]
	e.Status, e.since = Suspect, nw.nodes[1].round
	nw.rounds(8)
	nw.all(t, 4, Alive, "after node 4 heard it was suspected")
	if inc := nw.nodes[4].self().Incarnation; inc == 0 {
		t.Fatal("node 4 didn't raise its incarnation")
	}
}

func TestLeavingIsNotDying(t *testing.T) {
	for name, d := range detectors {
		nw := newNet(5, d)
		nw.rounds(10)
		nw.deliver(nw.nodes[4].Leave())
		nw.down[4] = true
		nw.rounds(3)
		nw.all(t, 4, Left, name+": 3 rounds after it said it was leaving")
		nw.rounds(60)
		nw.all(t, 4, Left, name+": long after")
		nw.start(4, d, 2)
		nw.rounds(20)
		nw.all(t, 4, Alive, name+": after it came back")
	}
}

func TestTheNewestTableSpreads(t *testing.T) {
	nw := newNet(5, Counters)
	old := shard.Table{Version: 3, StoreTime: 900, Slots: []shard.Owner{{Group: 1}, {Group: 2}}}
	newer := shard.Table{Version: 8, StoreTime: 500, Slots: []shard.Owner{{Group: 2, Epoch: 1}, {Group: 2}}}
	nw.nodes[2].SetTable(old)
	nw.nodes[5].SetTable(newer)
	nw.rounds(10)
	for id, n := range nw.nodes {
		got := n.Table()
		if got.Version != 8 || got.Slots[0].Group != 2 {
			t.Fatalf("node %d has table version %d, want 8", id, got.Version)
		}
		// Store time is the latest heard, whichever table it came with.
		if got.StoreTime != 900 {
			t.Fatalf("node %d has Store time %d, want 900", id, got.StoreTime)
		}
	}
	nw.nodes[1].SetTable(old)
	if nw.nodes[1].Table().Version != 8 {
		t.Fatal("an older table replaced a newer one")
	}
}

// The same seed gives the same Messages.
func TestDeterministic(t *testing.T) {
	run := func() int {
		nw := newNet(5, SWIM)
		nw.rounds(30)
		return nw.sent
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("two runs sent %d and %d Messages", a, b)
	}
}

// A Node's note reaches every Node, the latest one wins, and a Node that
// restarts and has lost count still outranks what it said before.
func TestNotesSpreadAndSurviveARestart(t *testing.T) {
	for _, d := range []Detector{Counters, SWIM} {
		nw := newNet(5, d)
		nw.rounds(10)
		nw.nodes[3].SetNote([]byte("one"))
		nw.nodes[3].SetNote([]byte("two"))
		nw.rounds(10)
		for id, n := range nw.nodes {
			if got := string(n.Note(3)); got != "two" {
				t.Fatalf("detector %d: node %d has node 3's note as %q", d, id, got)
			}
		}
		nw.start(3, d, 1)
		nw.rounds(3)
		nw.nodes[3].SetNote([]byte("three"))
		nw.rounds(10)
		for id, n := range nw.nodes {
			if got := string(n.Note(3)); got != "three" {
				t.Fatalf("detector %d: after its restart, node %d has node 3's note as %q", d, id, got)
			}
		}
	}
}
