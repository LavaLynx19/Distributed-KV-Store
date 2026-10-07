// Package rungtest runs a core through a Rung's Fault scenarios in the
// Simulation and returns the three verdicts (A§8.2). The naive core and the
// Raft core are judged by the same scenarios, so a seed that exposes a
// failure in one is a regression test for the other.
package rungtest

import (
	"fmt"
	"slices"
	"time"

	"distributed-kv-store/internal/check"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/sim"
)

// NewNode builds one Member's core.
type NewNode func(id core.NodeID, members []core.NodeID, rng core.Rand) core.Node

// Scenario injects Faults into a running Simulation between times from and
// to. It must leave repair to Run, which heals and restarts everything.
type Scenario struct {
	Name   string
	Faults func(s *sim.Sim, from, to int64)
}

// Report is the outcome of one run.
type Report struct {
	Scenario string
	Seed     uint64
	Members  int

	// History is everything the clients did and saw.
	History *check.History

	// Verdict 1: is the History Linearizable?
	Linearizable bool
	TimedOut     bool
	// Verdict 2: differences between Members' final data.
	Diverged []string
	// Verdict 3: what clients saw.
	Signals check.Signals

	// TwoLeaders is set when two running Members were Leader in the same
	// Term at the same moment.
	TwoLeaders string

	verdict check.Verdict
}

// Passed is true when the run kept every Rung 1 guarantee.
func (r Report) Passed() bool {
	return r.Linearizable && !r.TimedOut && len(r.Diverged) == 0 && r.TwoLeaders == ""
}

// Visualize writes the checked History as an HTML timeline.
func (r Report) Visualize(path string) error { return r.verdict.Visualize(path) }

func (r Report) String() string {
	state := "PASS"
	if !r.Passed() {
		state = "FAIL"
	}
	s := fmt.Sprintf("%s %s seed=%d members=%d linearizable=%v diverged=%d answered=%d rejected=%d lost=%d",
		state, r.Scenario, r.Seed, r.Members, r.Linearizable, len(r.Diverged),
		r.Signals.Answered, r.Signals.Rejected, r.Signals.Lost)
	if r.TwoLeaders != "" {
		s += " two-leaders: " + r.TwoLeaders
	}
	return s
}

const (
	warmup    = 300  // clients run fault-free first
	faultSpan = 3000 // Faults happen in this window
	cooldown  = 1500 // then everything is repaired and left to settle
)

// Run drives one Simulation: clients throughout, the scenario's Faults in the
// middle, then repair and a quiet period before the verdicts.
func Run(newNode NewNode, sc Scenario, members int, seed uint64) Report {
	s := sim.New(sim.Config{
		Seed:       seed,
		Nodes:      members,
		NewNode:    newNode,
		NewMachine: func() sim.Machine { return fsm.New() },
	})
	h := &check.History{}
	rep := Report{Scenario: sc.Name, Seed: seed, Members: members, History: h}

	faultsEnd := int64(warmup + faultSpan)
	sim.DefaultWorkload.Start(s, h, faultsEnd+cooldown/3)
	sc.Faults(s, warmup, faultsEnd)
	s.At(faultsEnd, func() {
		s.Heal()
		for _, id := range s.IDs() {
			s.Restart(id)
		}
	})
	watchLeaders(s, &rep, faultsEnd+cooldown)
	s.Run(faultsEnd + cooldown)

	rep.verdict = h.Linearizable(20 * time.Second)
	rep.Linearizable, rep.TimedOut = rep.verdict.Linearizable, rep.verdict.TimedOut
	items := map[core.NodeID][]fsm.Item{}
	for _, id := range s.IDs() {
		items[id] = s.Machine(id).(*fsm.Machine).Items()
	}
	rep.Diverged = check.Diverged(items)
	rep.Signals = h.Signals
	return rep
}

// watchLeaders samples every Member's status and records the first moment
// two running Members are Leader in the same Term.
func watchLeaders(s *sim.Sim, rep *Report, until int64) {
	var sample func()
	sample = func() {
		if s.Now() >= until || rep.TwoLeaders != "" {
			return
		}
		byTerm := map[core.Term][]core.NodeID{}
		for _, id := range s.IDs() {
			if st := s.Status(id); s.Up(id) && st.Role == core.LeaderRole {
				byTerm[st.Term] = append(byTerm[st.Term], id)
			}
		}
		terms := make([]core.Term, 0, len(byTerm))
		for t := range byTerm {
			terms = append(terms, t)
		}
		slices.Sort(terms)
		for _, t := range terms {
			if len(byTerm[t]) > 1 {
				rep.TwoLeaders = fmt.Sprintf("nodes %v in term %d at t=%d", byTerm[t], t, s.Now())
				return
			}
		}
		s.After(5, sample)
	}
	s.After(5, sample)
}

// leader is the Member that currently believes it leads, preferring the
// highest Term. It returns 0 if nobody does.
func leader(s *sim.Sim) core.NodeID {
	var best core.NodeID
	var bestTerm core.Term
	for _, id := range s.IDs() {
		if st := s.Status(id); s.Up(id) && st.Role == core.LeaderRole && (best == 0 || st.Term > bestTerm) {
			best, bestTerm = id, st.Term
		}
	}
	return best
}

func others(s *sim.Sim, id core.NodeID) []core.NodeID {
	var rest []core.NodeID
	for _, m := range s.IDs() {
		if m != id {
			rest = append(rest, m)
		}
	}
	return rest
}

// Rung1 are the Rung 1 scenarios: crashes and clean Partitions (README).
var Rung1 = []Scenario{
	{"none", func(*sim.Sim, int64, int64) {}},

	// The Leader is cut off from everyone else, then the network heals.
	{"isolate-leader", func(s *sim.Sim, from, to int64) {
		s.At(from, func() {
			if l := leader(s); l != 0 {
				s.Partition([]core.NodeID{l}, others(s, l))
			}
		})
		s.At(from+(to-from)/2, s.Heal)
	}},

	// The Leader crashes and later comes back.
	{"crash-leader", func(s *sim.Sim, from, to int64) {
		s.At(from, func() {
			if l := leader(s); l != 0 {
				s.Crash(l)
				s.At(from+(to-from)/2, func() { s.Restart(l) })
			}
		})
	}},

	// A random mix: every so often, crash or restart a Member, split the
	// Group in two at random, or heal.
	{"random", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			switch s.Rand().IntN(5) {
			case 0:
				s.Crash(ids[s.Rand().IntN(len(ids))])
			case 1:
				s.Restart(ids[s.Rand().IntN(len(ids))])
			case 2, 3:
				s.Rand().Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
				cut := 1 + s.Rand().IntN(len(ids)-1)
				s.Partition(ids[:cut], ids[cut:])
			case 4:
				s.Heal()
			}
			s.After(150+s.Rand().Int64N(300), step)
		}
		s.At(from, step)
	}},
}
