// Package leaderless is the core of the Leaderless variant (A§13): a store
// with no Leader, in which any Node takes any request and carries it out
// with the key's Home Nodes, every Node goes on accepting writes during a
// Partition, and what the sides did apart is joined afterwards.
//
// Like the consensus core it is a pure step function (A§4.2): the shell
// hands a Node one Event and gets back an Output saying what to store, what
// to send and what to answer. It shares nothing else with the consensus
// core.
package leaderless

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// NodeID identifies a Node. Nodes are numbered from 1.
type NodeID int

// Mode is how two writes made apart are settled when they meet.
type Mode uint8

const (
	// LastWriteWins gives each write of a key a number, and the higher
	// number replaces the lower, whole set and all. It is the store Rung 9
	// starts from.
	LastWriteWins Mode = iota
	// AddWins joins the two sets, so that nothing added on either side is
	// lost, and an element added on one side while the other removed it
	// stays (A§13.5).
	AddWins
)

// Dot marks one addition: the Node that took it, and that Node's count of
// additions to the key.
type Dot struct {
	Node NodeID
	N    uint64
}

func compareDots(a, b Dot) int {
	if a.Node != b.Node {
		return cmp.Compare(a.Node, b.Node)
	}
	return cmp.Compare(a.N, b.N)
}

// span is the Dots of one Node that a key has seen: every one up to UpTo,
// and the ones in More, which are above it and in order.
type span struct {
	UpTo uint64
	More []uint64
}

// Dots is a set of Dots. It is the record a key keeps of every addition it
// has seen, whether or not the element is still there: what tells "never
// added here" from "added and removed" (A§13.5).
type Dots map[NodeID]span

// Has reports whether d is in the set.
func (ds Dots) Has(d Dot) bool {
	s := ds[d.Node]
	if d.N <= s.UpTo {
		return true
	}
	_, found := slices.BinarySearch(s.More, d.N)
	return found
}

// With returns the set with d added.
func (ds Dots) With(d Dot) Dots {
	if ds.Has(d) {
		return ds
	}
	out := ds.clone()
	s := out[d.Node]
	at, _ := slices.BinarySearch(s.More, d.N)
	s.More = slices.Insert(slices.Clone(s.More), at, d.N)
	out[d.Node] = s.packed()
	return out
}

// packed moves into UpTo whatever of More now follows it without a gap.
func (s span) packed() span {
	for len(s.More) > 0 && s.More[0] == s.UpTo+1 {
		s.UpTo, s.More = s.More[0], s.More[1:]
	}
	if len(s.More) == 0 {
		s.More = nil
	}
	return s
}

func (ds Dots) clone() Dots {
	out := make(Dots, len(ds)+1)
	for n, s := range ds {
		out[n] = span{UpTo: s.UpTo, More: slices.Clone(s.More)}
	}
	return out
}

// Union returns every Dot in either set.
func (ds Dots) Union(other Dots) Dots {
	out := ds.clone()
	for n, o := range other {
		s := out[n]
		s.UpTo = max(s.UpTo, o.UpTo)
		more := append(slices.Clone(s.More), o.More...)
		slices.Sort(more)
		more = slices.Compact(more)
		s.More = slices.DeleteFunc(more, func(v uint64) bool { return v <= s.UpTo })
		out[n] = s.packed()
	}
	return out
}

func (ds Dots) nodes() []NodeID {
	nodes := make([]NodeID, 0, len(ds))
	for n := range ds {
		nodes = append(nodes, n)
	}
	slices.Sort(nodes)
	return nodes
}

// Stamp numbers a write under LastWriteWins: a count one above the highest
// the Node that took the write had seen for the key, and that Node to break
// ties.
type Stamp struct {
	Count uint64
	Node  NodeID
}

func (s Stamp) after(o Stamp) bool {
	if s.Count != o.Count {
		return s.Count > o.Count
	}
	return s.Node > o.Node
}

// State is what a Node holds for one key. The zero value is a key nothing
// is known of.
type State struct {
	// Elems are the elements of the set. Under AddWins each has the Dots
	// of the additions that put it there; under LastWriteWins it has none.
	Elems map[string][]Dot
	// Seen is every Dot the key has seen (AddWins).
	Seen Dots
	// Stamp is the number of the write that made this set (LastWriteWins).
	Stamp Stamp
}

// Values lists the elements, in order.
func (s State) Values() []string {
	values := make([]string, 0, len(s.Elems))
	for e := range s.Elems {
		values = append(values, e)
	}
	slices.Sort(values)
	return values
}

// Empty reports whether nothing at all is known of the key.
func (s State) Empty() bool { return len(s.Elems) == 0 && len(s.Seen) == 0 && s.Stamp == Stamp{} }

// Canon is the State as text, the same for two States that hold the same.
func (s State) Canon() string {
	var b strings.Builder
	fmt.Fprintf(&b, "stamp %d/%d;", s.Stamp.Count, s.Stamp.Node)
	for _, e := range s.Values() {
		dots := slices.Clone(s.Elems[e])
		slices.SortFunc(dots, compareDots)
		fmt.Fprintf(&b, "%q%v;", e, dots)
	}
	for _, n := range s.Seen.nodes() {
		if sp := s.Seen[n]; sp.UpTo > 0 || len(sp.More) > 0 {
			fmt.Fprintf(&b, "seen %d:%d+%v;", n, sp.UpTo, sp.More)
		}
	}
	return b.String()
}

// Equal reports whether two States hold the same.
func (s State) Equal(o State) bool { return s.Canon() == o.Canon() }

// Join is the State two copies of a key come to when they meet. It doesn't
// matter which is which, how often they meet, or in what order copies are
// joined: the result is the same.
func Join(mode Mode, a, b State) State {
	if mode == LastWriteWins {
		if b.Stamp.after(a.Stamp) {
			return b
		}
		return a
	}
	out := State{Seen: a.Seen.Union(b.Seen)}
	keep := func(e string, d Dot) {
		if out.Elems == nil {
			out.Elems = map[string][]Dot{}
		}
		if !slices.Contains(out.Elems[e], d) {
			out.Elems[e] = append(out.Elems[e], d)
		}
	}
	// A Dot stays if both have it, or if one has it and the other has
	// never seen it. A Dot one side has seen and no longer has was removed
	// there, and is removed here.
	for e, dots := range a.Elems {
		for _, d := range dots {
			if slices.Contains(b.Elems[e], d) || !b.Seen.Has(d) {
				keep(e, d)
			}
		}
	}
	for e, dots := range b.Elems {
		for _, d := range dots {
			if !a.Seen.Has(d) {
				keep(e, d)
			}
		}
	}
	for e := range out.Elems {
		slices.SortFunc(out.Elems[e], compareDots)
	}
	return out
}

// with returns the State with one element added or removed, as the whole
// set a LastWriteWins write replaces the old one with.
func (s State) with(elem string, add bool, stamp Stamp) State {
	out := State{Stamp: stamp, Elems: map[string][]Dot{}}
	for e := range s.Elems {
		out.Elems[e] = nil
	}
	if add {
		out.Elems[elem] = nil
	} else {
		delete(out.Elems, elem)
	}
	return out
}
