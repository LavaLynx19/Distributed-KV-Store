package lsim

import "distributed-kv-store/internal/leaderless"

// Store is what a run puts under test.
type Store struct {
	Nodes                int
	W, R                 int
	Mode                 leaderless.Mode
	DeliverHints, Resend bool
	// Exchange, Repair, Forgetful and Skew are passed on (Config).
	Exchange, Repair, Forgetful bool
	Skew                        map[leaderless.NodeID]int64
	Workload                    Workload
}

// Scenario injects Faults between times from and to. It must leave repair
// to Run.
type Scenario struct {
	Name   string
	Faults func(s *Sim, from, to int64)
}

const (
	warmup    = 400  // before any Fault
	faultSpan = 2600 // how long Faults go on
	cooldown  = 1800 // after repair, before the verdicts
)

// Run drives one Simulation: clients throughout, the scenario in the
// middle, then repair and a quiet period before the verdicts (A§13.11).
func Run(store Store, sc Scenario, seed uint64) Report {
	s := New(Config{Seed: seed, Nodes: store.Nodes, W: store.W, R: store.R, Mode: store.Mode,
		DeliverHints: store.DeliverHints, Resend: store.Resend,
		Exchange: store.Exchange, Repair: store.Repair, Forgetful: store.Forgetful, Skew: store.Skew})
	h := &History{}
	faultsEnd := int64(warmup + faultSpan)
	store.Workload.Start(s, h, faultsEnd+cooldown/3)
	sc.Faults(s, warmup, faultsEnd)
	s.At(faultsEnd, func() {
		s.Heal()
		for _, id := range s.IDs() {
			s.Restart(id)
		}
	})
	s.Run(faultsEnd + cooldown)
	rep := Judge(s, h, store.Mode)
	rep.Scenario, rep.Seed = sc.Name, seed
	for _, c := range h.Calls {
		if c.Result == Done && c.Begin >= warmup && c.Begin < faultsEnd {
			rep.WindowAnswered++
		}
	}
	return rep
}

// split divides the Nodes at random into a side of cut and the rest.
func split(s *Sim, cut int) (a, b []leaderless.NodeID) {
	ids := s.IDs()
	s.Rand().Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	return ids[:cut], ids[cut:]
}

// Scenarios are the Faults of Rung 9.
var Scenarios = []Scenario{
	{"no-faults", func(*Sim, int64, int64) {}},

	// One Partition for the whole of the Fault time: two Nodes on one
	// side, three on the other. This is the run the comparison is made
	// on.
	{"long-partition", func(s *Sim, from, to int64) {
		s.At(from, func() { s.Partition(split(s, 2)) })
	}},

	// The network keeps splitting into two sides.
	{"partitions", func(s *Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			s.Partition(split(s, 1+s.Rand().IntN(2)))
			s.After(150+s.Rand().Int64N(250), s.Heal)
			s.After(400+s.Rand().Int64N(300), step)
		}
		s.At(from+50, step)
	}},

	// Nodes stop and start again.
	{"crashes", func(s *Sim, from, to int64) { crashes(s, from, to) }},

	// One Node is down for most of the Fault time: far longer than any
	// write is sent again for.
	{"long-outage", func(s *Sim, from, to int64) {
		s.At(from+100, func() {
			ids := s.IDs()
			id := ids[s.Rand().IntN(len(ids))]
			s.Crash(id)
			s.At(to-300, func() { s.Restart(id) })
		})
	}},

	// Both.
	{"partitions-and-crashes", func(s *Sim, from, to int64) {
		crashes(s, from, to)
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			s.Partition(split(s, 1+s.Rand().IntN(2)))
			s.After(150+s.Rand().Int64N(250), s.Heal)
			s.After(500+s.Rand().Int64N(300), step)
		}
		s.At(from+80, step)
	}},
}

func crashes(s *Sim, from, to int64) {
	var step func()
	step = func() {
		if s.Now() >= to {
			return
		}
		ids := s.IDs()
		id := ids[s.Rand().IntN(len(ids))]
		s.Crash(id)
		s.After(100+s.Rand().Int64N(250), func() { s.Restart(id) })
		s.After(250+s.Rand().Int64N(300), step)
	}
	s.At(from+50, step)
}
