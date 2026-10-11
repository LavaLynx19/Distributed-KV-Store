package leaderless

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestDots(t *testing.T) {
	var ds Dots
	for _, n := range []uint64{3, 1, 5} {
		ds = ds.With(Dot{7, n})
	}
	if sp := ds[7]; sp.UpTo != 1 || !slices.Equal(sp.More, []uint64{3, 5}) {
		t.Fatalf("after 3, 1, 5: %+v", sp)
	}
	ds = ds.With(Dot{7, 2})
	if sp := ds[7]; sp.UpTo != 3 || !slices.Equal(sp.More, []uint64{5}) {
		t.Fatalf("after 2: %+v", sp)
	}
	other := Dots{}.With(Dot{7, 4}).With(Dot{8, 1})
	u := ds.Union(other)
	if sp := u[7]; sp.UpTo != 5 || len(sp.More) != 0 || !u.Has(Dot{8, 1}) || u.Has(Dot{8, 2}) || u.Has(Dot{7, 6}) {
		t.Fatalf("union: %+v", u)
	}
	if ds.Has(Dot{7, 4}) {
		t.Error("a union changed one of its parts")
	}
}

// randomState builds a State as some Node might hold it: a few additions,
// some of them since removed.
func randomState(rng *rand.Rand) State {
	s := State{Elems: map[string][]Dot{}}
	for range rng.IntN(6) {
		d := Dot{Node: NodeID(1 + rng.IntN(3)), N: uint64(1 + rng.IntN(4))}
		s.Seen = s.Seen.With(d)
		// A given Dot always marks the same element, as it does in a store.
		if e := fmt.Sprintf("e%d", (int(d.Node)*7+int(d.N))%3); rng.IntN(3) > 0 && !slices.Contains(s.Elems[e], d) {
			s.Elems[e] = append(s.Elems[e], d)
		}
	}
	return s
}

// Joining must come to the same thing in any order and any number of times:
// that is what lets Nodes meet in any order and still agree.
func TestJoinDoesNotCareAboutOrder(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		a, b, c := randomState(rng), randomState(rng), randomState(rng)
		if ab, ba := Join(AddWins, a, b), Join(AddWins, b, a); !ab.Equal(ba) {
			t.Fatalf("a⊔b = %s, b⊔a = %s", ab.Canon(), ba.Canon())
		}
		if l, r := Join(AddWins, Join(AddWins, a, b), c), Join(AddWins, a, Join(AddWins, b, c)); !l.Equal(r) {
			t.Fatalf("(a⊔b)⊔c = %s, a⊔(b⊔c) = %s", l.Canon(), r.Canon())
		}
		if aa := Join(AddWins, a, a); !aa.Equal(Join(AddWins, a, State{})) {
			t.Fatalf("a⊔a = %s, a = %s", aa.Canon(), a.Canon())
		}
	}
}

func TestAddWinsOverARemoveThatDidNotSeeIt(t *testing.T) {
	first, second := Dot{1, 1}, Dot{2, 1}
	added := State{Elems: map[string][]Dot{"x": {first}}, Seen: Dots{}.With(first)}
	// One side removes x, having seen only the first addition.
	removed := Join(AddWins, added, State{Seen: Dots{}.With(first)})
	if len(removed.Values()) != 0 {
		t.Fatalf("after the remove: %v", removed.Values())
	}
	// The other side, cut off, adds x again.
	again := Join(AddWins, added, State{Elems: map[string][]Dot{"x": {second}}, Seen: Dots{}.With(second)})
	met := Join(AddWins, removed, again)
	if !slices.Equal(met.Values(), []string{"x"}) || !slices.Equal(met.Elems["x"], []Dot{second}) {
		t.Errorf("when the sides meet: %s", met.Canon())
	}
	// Under last-write-wins one side's whole set replaces the other's.
	a := State{}.with("x", true, Stamp{1, 1})
	b := State{}.with("y", true, Stamp{1, 2})
	if got := Join(LastWriteWins, a, b); !slices.Equal(got.Values(), []string{"y"}) {
		t.Errorf("last-write-wins kept %v", got.Values())
	}
}

func TestRing(t *testing.T) {
	nodes := []NodeID{1, 2, 3, 4, 5}
	r := NewRing(nodes)
	home := map[NodeID]int{}
	for i := range 1000 {
		key := fmt.Sprintf("k%d", i)
		order := r.Order(key)
		sorted := slices.Clone(order)
		slices.Sort(sorted)
		if !slices.Equal(sorted, nodes) {
			t.Fatalf("%s: order %v doesn't list every Node once", key, order)
		}
		if !slices.Equal(r.Homes(key), order[:Homes]) {
			t.Fatalf("%s: Home Nodes %v aren't the first of %v", key, r.Homes(key), order)
		}
		for _, n := range r.Homes(key) {
			home[n]++
		}
	}
	// 3,000 places among five Nodes: 600 each if it were even.
	for _, n := range nodes {
		if home[n] < 400 || home[n] > 800 {
			t.Errorf("Node %d is a Home Node of %d keys of 1000; the ring is lopsided: %v", n, home[n], home)
		}
	}
}

// net carries Messages between Nodes at once, except to those cut off.
type net struct {
	nodes   map[NodeID]*Node
	cut     map[NodeID]bool
	replies []Reply
}

func newNet(mode Mode, w, r int, deliver bool) *net {
	ids := []NodeID{1, 2, 3, 4, 5}
	n := &net{nodes: map[NodeID]*Node{}, cut: map[NodeID]bool{}}
	for _, id := range ids {
		n.nodes[id] = New(Config{ID: id, Nodes: ids, W: w, R: r, Mode: mode, DeliverHints: deliver, Resend: true, Seed: uint64(id) * 1000})
	}
	return n
}

func (n *net) step(id NodeID, ev Event) {
	out := n.nodes[id].Step(ev)
	n.replies = append(n.replies, out.Replies...)
	for _, m := range out.Messages {
		if !n.cut[m.To] && !n.cut[m.From] {
			n.step(m.To, Receive{Msg: m})
		}
	}
}

func (n *net) ticks(count int) {
	for range count {
		for id := NodeID(1); id <= 5; id++ {
			n.step(id, Tick{})
		}
	}
}

func (n *net) reply(t *testing.T, ref uint64) Reply {
	t.Helper()
	for _, r := range n.replies {
		if r.Ref == ref {
			return r
		}
	}
	t.Fatalf("no Reply to request %d", ref)
	return Reply{}
}

func TestAWriteWaitsForWAndAHintStandsInForAHomeNode(t *testing.T) {
	n := newNet(AddWins, 3, 1, true)
	homes := n.nodes[1].Ring().Homes("k")
	order := n.nodes[1].Ring().Order("k")
	away, standIn := homes[2], order[Homes]
	n.cut[away] = true

	n.step(homes[0], Request{Ref: 1, Op: Add, Key: "k", Elem: "x"})
	if len(n.replies) != 0 {
		t.Fatal("the client was answered with only two of three Nodes holding the write")
	}
	n.ticks(hintAfter)
	if r := n.reply(t, 1); !r.OK {
		t.Fatalf("with a stand-in: %+v", r)
	}
	if n.nodes[standIn].Hints() != 1 || len(n.nodes[away].Holds("k").Values()) != 0 {
		t.Fatalf("stand-in %d holds %d Hints; the Home Node that was away holds %v", standIn, n.nodes[standIn].Hints(), n.nodes[away].Holds("k").Values())
	}

	// The Home Node is back: the Hint is handed over and dropped.
	n.cut[away] = false
	n.ticks(deliverEvery + 1)
	if got := n.nodes[away].Holds("k").Values(); !slices.Equal(got, []string{"x"}) || n.nodes[standIn].Hints() != 0 {
		t.Errorf("after delivery the Home Node holds %v and the stand-in %d Hints", got, n.nodes[standIn].Hints())
	}

	// A read and a remove through a Node that isn't a Home Node of the key.
	n.step(order[4], Request{Ref: 2, Op: Get, Key: "k"})
	if r := n.reply(t, 2); !r.OK || !slices.Equal(r.Values, []string{"x"}) {
		t.Fatalf("read: %+v", r)
	}
	n.step(order[4], Request{Ref: 3, Op: Remove, Key: "k", Elem: "x"})
	n.step(order[4], Request{Ref: 4, Op: Get, Key: "k"})
	if r := n.reply(t, 4); !n.reply(t, 3).OK || len(r.Values) != 0 {
		t.Errorf("after the remove a read gives %+v", r)
	}
}

func TestTooFewNodes(t *testing.T) {
	n := newNet(AddWins, 3, 1, true)
	order := n.nodes[1].Ring().Order("k")
	// Only the Node asked and one other can be reached: two Nodes, and W
	// is three.
	for _, id := range order[2:] {
		n.cut[id] = true
	}
	n.step(order[0], Request{Ref: 1, Op: Add, Key: "k", Elem: "x"})
	n.ticks(giveUp)
	if r := n.reply(t, 1); r.OK {
		t.Fatalf("answered with two Nodes of three: %+v", r)
	}
	// The write is on the Nodes it reached all the same.
	if got := n.nodes[order[1]].Holds("k").Values(); !slices.Equal(got, []string{"x"}) {
		t.Errorf("the other Home Node holds %v", got)
	}
}
