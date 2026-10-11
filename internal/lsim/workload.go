package lsim

import (
	"fmt"

	"distributed-kv-store/internal/leaderless"
)

// Result is how a request ended, as its client saw it.
type Result uint8

const (
	// Done: the store said it succeeded.
	Done Result = iota + 1
	// Unknown: the store said too few Nodes answered, or said nothing in
	// time. A write may have taken effect all the same.
	Unknown
	// Refused: the Node asked wasn't running. Nothing was done.
	Refused
)

// Call is one request and how it ended.
type Call struct {
	Client     int
	Ref        uint64
	Op         leaderless.Op
	Key, Elem  string
	Begin, End int64
	Result     Result
	// Values is what a read returned.
	Values []string
	// Cut is whether a Partition was in force when the request began.
	Cut bool
}

// History is every Call of a run, in the order they began.
type History struct{ Calls []*Call }

// Workload is the clients of a run.
type Workload struct {
	Clients int
	// Keys are "k0".."k<Keys-1>" and the elements added to and removed
	// from each are "e0".."e<Elems-1>": few enough that clients add and
	// remove the same ones.
	Keys, Elems int
	Timeout     int64 // how long a client waits for an answer
	Think       int64 // a client pauses 1..Think units between requests
}

// DefaultWorkload has five clients, each talking to one Node, so that a
// Partition leaves clients on every side.
var DefaultWorkload = Workload{Clients: 5, Keys: 6, Elems: 4, Timeout: 300, Think: 40}

// Start sets the clients going until time until. Client i talks to Node
// i mod Nodes + 1 throughout: a client doesn't go looking for another Node
// when its own stops answering, so what the run measures is whether the
// Node a client can reach will serve it.
func (w Workload) Start(s *Sim, h *History, until int64) {
	for i := range w.Clients {
		node := s.ids[i%len(s.ids)]
		s.After(1+s.rng.Int64N(w.Think), func() { w.next(s, h, i, node, until) })
	}
}

func (w Workload) next(s *Sim, h *History, client int, node leaderless.NodeID, until int64) {
	if s.now >= until {
		return
	}
	call := &Call{Client: client, Begin: s.now, Cut: s.Cut(s.now),
		Key: fmt.Sprintf("k%d", s.rng.IntN(w.Keys)), Elem: fmt.Sprintf("e%d", s.rng.IntN(w.Elems))}
	switch roll := s.rng.IntN(100); {
	case roll < 40:
		call.Op, call.Elem = leaderless.Get, ""
	case roll < 80:
		call.Op = leaderless.Add
	default:
		call.Op = leaderless.Remove
	}
	h.Calls = append(h.Calls, call)
	again := func() { s.After(1+s.rng.Int64N(w.Think), func() { w.next(s, h, client, node, until) }) }
	settled := false
	end := func(r Result, values []string) {
		if settled {
			return
		}
		settled = true
		call.End, call.Result, call.Values = s.now, r, values
		again()
	}
	ref, made := s.Request(node, call.Op, call.Key, call.Elem, func(r leaderless.Reply) {
		if r.OK {
			end(Done, r.Values)
		} else {
			end(Unknown, nil)
		}
	})
	if !made {
		end(Refused, nil)
		return
	}
	call.Ref = ref
	s.After(w.Timeout, func() { end(Unknown, nil) })
}
