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
	"distributed-kv-store/internal/transport"
)

// NewNode builds one Member's core.
type NewNode func(id core.NodeID, members []core.NodeID, rng core.Rand) core.Node

// Store is what a run puts under test: a core, and how clients use it.
type Store struct {
	NewNode  NewNode
	Workload sim.Workload
	// Restart rebuilds a crashed Member's core from its disk. If nil, a
	// crash is a freeze and the Member keeps its memory (Rungs 1–2).
	Restart func(id core.NodeID, members []core.NodeID, rng core.Rand, stored core.Stored) core.Node
	// DiskDelay is how long a write takes to become durable (sim.Config).
	DiskDelay [2]int64
	// SnapshotEvery makes Members take a Snapshot every so many applied
	// Entries (sim.Config). Zero means never.
	SnapshotEvery int
	// TearWrites makes a crash leave part of a write in progress on disk,
	// possibly with zeros in it (sim.Config).
	TearWrites bool
	// UncheckedDisk stores files without checksums, as in Rung 3.
	UncheckedDisk bool
	// Stamped puts the receiving Member's clock reading into every request
	// (A§6.1), as Rung 5 and later do.
	Stamped bool
	// SessionTTL removes a Session unused for this long by Log time
	// (fsm.Machine.SessionTTL). Zero means never.
	SessionTTL int64
	// OwnClock gives each Member a state machine that judges Expiry by its
	// own clock, as Rung 5's naive store does.
	OwnClock bool
}

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
	// Recovery is how long after the Faults were repaired the first write
	// succeeded, or -1 if none did.
	Recovery int64

	// TwoLeaders is set when two running Members were Leader in the same
	// Term at the same moment.
	TwoLeaders string
	// Panic is set when a core's own safety check stopped the run.
	Panic string
	// Unreadable lists Members that could not restart because they could
	// not read their own disk, with the reason.
	Unreadable []string
	// Stalled is set when, at the end, too few Members are fit to vote for
	// a Leader to be elected: the rest found damage on their disks and are
	// recovering, or can't start at all. A stalled Group has stopped on
	// purpose (A§6.8). It has kept its safety but not its availability, so
	// its Members aren't expected to have converged.
	Stalled bool
	// StillRecovering counts Members that were Recovering at the end.
	StillRecovering int

	verdict check.Verdict
}

// Safe is true when nothing false was ever said or done: the History is
// Linearizable, no Term had two Leaders, and no core tripped its own check.
func (r Report) Safe() bool {
	return r.Linearizable && !r.TimedOut && r.TwoLeaders == "" && r.Panic == ""
}

// Passed is true when the run kept every guarantee: it was Safe, every
// Member could start, and they all ended with the same data.
func (r Report) Passed() bool {
	return r.Safe() && len(r.Diverged) == 0 && len(r.Unreadable) == 0
}

// Visualize writes the checked History as an HTML timeline.
func (r Report) Visualize(path string) error { return r.verdict.Visualize(path) }

func (r Report) String() string {
	state := "PASS"
	if !r.Passed() {
		state = "FAIL"
	}
	s := fmt.Sprintf("%s %s seed=%d members=%d linearizable=%v diverged=%d answered=%d rejected=%d lost=%d recovery=%d",
		state, r.Scenario, r.Seed, r.Members, r.Linearizable, len(r.Diverged),
		r.Signals.Answered, r.Signals.Rejected, r.Signals.Lost, r.Recovery)
	if r.TwoLeaders != "" {
		s += " two-leaders: " + r.TwoLeaders
	}
	if r.Panic != "" {
		s += " panic: " + r.Panic
	}
	if len(r.Unreadable) > 0 {
		s += fmt.Sprintf(" unreadable: %v", r.Unreadable)
	}
	if r.Stalled {
		s += " stalled"
	}
	return s
}

const (
	warmup    = 300  // clients run fault-free first
	faultSpan = 3000 // Faults happen in this window
	cooldown  = 1500 // then everything is repaired and left to settle
	settle    = 200  // and for this long at the end nothing new is started
)

// Run drives one Simulation: clients throughout, the scenario's Faults in the
// middle, then repair and a quiet period before the verdicts. Messages pass
// through the network encoding, so the core's message types must have been
// given to transport.Register.
func Run(store Store, sc Scenario, members int, seed uint64) Report {
	newMachine := func() sim.Machine {
		m := fsm.New()
		m.SessionTTL = store.SessionTTL
		return m
	}
	if store.OwnClock {
		newMachine = func() sim.Machine { return fsm.NewOwnClock() }
	}
	var s *sim.Sim
	var stamp func([]byte, int64) []byte
	var timeEntry func(sim.Machine, int64) []byte
	if store.Stamped {
		stamp = fsm.Stamp
		timeEntry = func(m sim.Machine, now int64) []byte {
			// Nothing new near the end: the Members are compared as they
			// stand when the run stops, so they must have settled.
			if s.Now() >= warmup+faultSpan+cooldown-settle || !m.(*fsm.Machine).Due(now) {
				return nil
			}
			return fsm.Command{Op: fsm.OpTick}.Encode()
		}
	}
	s = sim.New(sim.Config{
		Seed:          seed,
		Nodes:         members,
		NewNode:       store.NewNode,
		NewMachine:    newMachine,
		Stamp:         stamp,
		TimeEntry:     timeEntry,
		Copy:          transport.NewLoopback().Copy,
		Restart:       store.Restart,
		DiskDelay:     store.DiskDelay,
		SnapshotEvery: store.SnapshotEvery,
		TearWrites:    store.TearWrites,
		UncheckedDisk: store.UncheckedDisk,
	})
	h := &check.History{}
	rep := Report{Scenario: sc.Name, Seed: seed, Members: members, History: h}

	faultsEnd := int64(warmup + faultSpan)
	store.Workload.Start(s, h, faultsEnd+cooldown/3)
	sc.Faults(s, warmup, faultsEnd)
	s.At(faultsEnd, func() {
		s.Heal()
		s.SetDelay(1, 8)
		s.SetDuplicate(0)
		s.SetLoss(0)
		for _, id := range s.IDs() {
			s.Restart(id)
			// Clocks run true again, but keep whatever they have come to
			// read: nothing puts a clock right.
			s.SetClockRate(id, 100)
		}
	})
	watchLeaders(s, &rep, faultsEnd+cooldown)
	// A core panics when one of its own safety checks fails. That ends the
	// run, and is recorded as a failure with the reason.
	func() {
		defer func() {
			if p := recover(); p != nil {
				rep.Panic = fmt.Sprint(p)
			}
		}()
		s.Run(faultsEnd + cooldown)
	}()

	rep.verdict = h.Linearizable(20 * time.Second)
	rep.Linearizable, rep.TimedOut = rep.verdict.Linearizable, rep.verdict.TimedOut
	items := map[core.NodeID][]fsm.Item{}
	sessions := map[core.NodeID]int{}
	fit := 0
	for _, id := range s.IDs() {
		if err := s.StartError(id); err != nil {
			rep.Unreadable = append(rep.Unreadable, fmt.Sprintf("node %d: %v", id, err))
			continue // it holds no state to compare
		}
		machine := s.Machine(id).(*fsm.Machine)
		machine.Observe(s.Clock(id))
		items[id] = machine.Items()
		sessions[id] = machine.Sessions()
		if s.Status(id).Recovering {
			rep.StillRecovering++
		} else {
			fit++
		}
	}
	rep.Stalled = fit < members/2+1
	rep.Diverged = check.Diverged(items)
	// Members must agree on which Sessions exist too.
	for _, id := range s.IDs()[1:] {
		a, b := sessions[s.IDs()[0]], sessions[id]
		if _, ok := items[id]; ok && items[s.IDs()[0]] != nil && a != b {
			rep.Diverged = append(rep.Diverged, fmt.Sprintf("node %d has %d Sessions, node %d has %d", s.IDs()[0], a, id, b))
		}
	}
	rep.Signals = h.Signals
	rep.Recovery = h.Signals.RecoveryAfter(faultsEnd)
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

// messy makes the network slow, lossy and repetitive: delays wide enough to
// reorder messages, one message in five delivered twice, one in ten lost.
func messy(s *sim.Sim) {
	s.SetDelay(1, 60)
	s.SetDuplicate(0.2)
	s.SetLoss(0.1)
}

// Rung2 adds the Faults Rung 2 must survive (README): one-way Partitions and
// delayed, reordered and duplicated messages.
var Rung2 = []Scenario{
	// The Leader can hear everyone, but nobody hears the Leader.
	{"leader-mute", func(s *sim.Sim, from, to int64) {
		s.At(from, func() {
			if l := leader(s); l != 0 {
				s.Cut([]core.NodeID{l}, others(s, l))
			}
		})
		s.At(from+(to-from)/2, s.Heal)
	}},

	// Everyone hears the Leader, but the Leader hears nobody.
	{"leader-deaf", func(s *sim.Sim, from, to int64) {
		s.At(from, func() {
			if l := leader(s); l != 0 {
				s.Cut(others(s, l), []core.NodeID{l})
			}
		})
		s.At(from+(to-from)/2, s.Heal)
	}},

	// A messy network and nothing else.
	{"messy", func(s *sim.Sim, from, to int64) {
		s.At(from, func() { messy(s) })
	}},

	// A messy network, with crashes, restarts, Partitions and one-way cuts
	// arriving at random.
	{"messy-random", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			s.Rand().Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			cut := 1 + s.Rand().IntN(len(ids)-1)
			switch s.Rand().IntN(7) {
			case 0:
				s.Crash(ids[0])
			case 1:
				s.Restart(ids[0])
			case 2:
				s.Partition(ids[:cut], ids[cut:])
			case 3, 4:
				s.Cut(ids[:cut], ids[cut:])
			case 5, 6:
				s.Heal()
			}
			s.After(150+s.Rand().Int64N(300), step)
		}
		s.At(from, func() { messy(s); step() })
	}},
}

// Rung3 adds the Faults Rung 3 must survive (README): every Member restarting
// at once, crashes while a write is on its way to disk, and a stalled disk.
// They only bite when the Store sets Restart, so that a crash loses whatever
// wasn't durable.
var Rung3 = []Scenario{
	// Every Member crashes at the same moment and they all come back.
	{"full-restart", func(s *sim.Sim, from, to int64) {
		for _, at := range []int64{from + (to-from)/4, from + (to-from)/2} {
			s.At(at, func() {
				for _, id := range s.IDs() {
					s.Crash(id)
				}
			})
			s.At(at+200, func() {
				for _, id := range s.IDs() {
					s.Restart(id)
				}
			})
		}
	}},

	// Members crash one at a time, often with a write in progress, and
	// restart a little later.
	{"rolling-crashes", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			id := ids[s.Rand().IntN(len(ids))]
			s.Crash(id)
			s.After(50+s.Rand().Int64N(250), func() { s.Restart(id) })
			s.After(100+s.Rand().Int64N(200), step)
		}
		s.At(from, step)
	}},

	// Members blink: each crash is followed by a restart a moment later, on
	// a slow network. A Member can vote, forget and be asked again within
	// one election, which is what storing the vote is for.
	{"blink", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			id := ids[s.Rand().IntN(len(ids))]
			s.Crash(id)
			s.After(1+s.Rand().Int64N(15), func() { s.Restart(id) })
			s.After(5+s.Rand().Int64N(40), step)
		}
		s.At(from, func() { s.SetDelay(1, 60); step() })
	}},

	// Disks stall for a while, one Member at a time, the Leader included.
	{"stalled-disk", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			id := ids[s.Rand().IntN(len(ids))]
			if l := leader(s); l != 0 && s.Rand().IntN(2) == 0 {
				id = l
			}
			s.StallDisk(id, 100+s.Rand().Int64N(400))
			s.After(200+s.Rand().Int64N(400), step)
		}
		s.At(from, step)
	}},

	// Everything at once: a messy network, crashes, restarts, Partitions,
	// one-way cuts and stalled disks.
	{"everything", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			s.Rand().Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			cut := 1 + s.Rand().IntN(len(ids)-1)
			switch s.Rand().IntN(9) {
			case 0, 1:
				s.Crash(ids[0])
			case 2, 3:
				s.Restart(ids[0])
			case 4:
				s.Partition(ids[:cut], ids[cut:])
			case 5:
				s.Cut(ids[:cut], ids[cut:])
			case 6:
				s.StallDisk(ids[0], 100+s.Rand().Int64N(300))
			case 7, 8:
				s.Heal()
			}
			s.After(100+s.Rand().Int64N(250), step)
		}
		s.At(from, func() { messy(s); step() })
	}},
}

// Rung4 adds a disk that lies (README): it can flip a bit in something a
// Member stored long ago. Damage is only noticed when a Member reads its
// disk, so each flip is followed by a crash and a restart of that Member.
// Torn writes are the Store's TearWrites setting, and apply to every crash
// in every scenario.
var Rung4 = []Scenario{
	// One Member at a time has a bit flipped on its disk and restarts.
	{"bit-flips", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			id := ids[s.Rand().IntN(len(ids))]
			s.FlipBit(id)
			s.Crash(id)
			s.After(50+s.Rand().Int64N(150), func() { s.Restart(id) })
			s.After(300+s.Rand().Int64N(300), step)
		}
		s.At(from, step)
	}},

	// Bit flips on top of everything else.
	{"rot-and-everything", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			s.Rand().Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			cut := 1 + s.Rand().IntN(len(ids)-1)
			switch s.Rand().IntN(10) {
			case 0, 1:
				s.Crash(ids[0])
			case 2, 3:
				s.Restart(ids[0])
			case 4:
				s.Partition(ids[:cut], ids[cut:])
			case 5:
				s.StallDisk(ids[0], 100+s.Rand().Int64N(300))
			case 6, 7:
				s.FlipBit(ids[0])
				s.Crash(ids[0])
			case 8, 9:
				s.Heal()
			}
			s.After(100+s.Rand().Int64N(250), step)
		}
		s.At(from, func() { messy(s); step() })
	}},

	// The Leader and the followers on its side of a Partition carry on while
	// the rest fall behind. Then those followers' disks are damaged, and
	// while they are being brought back up to date the Leader is lost and
	// the Partition heals. What is left is Members that have lost Entries
	// they acknowledged and Members that never had them.
	{"half-repaired", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			s.Heal()
			for _, id := range s.IDs() {
				s.Restart(id)
			}
			s.After(250, func() {
				l := leader(s)
				if s.Now() >= to-400 {
					return // no time left to finish the sequence before the repair
				}
				if l == 0 {
					s.After(100, step)
					return
				}
				rest := others(s, l)
				s.Rand().Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
				with, behind := rest[:len(rest)/2], rest[len(rest)/2:]
				s.Partition(append([]core.NodeID{l}, with...), behind)
				s.After(200+s.Rand().Int64N(150), func() {
					for _, id := range with {
						s.FlipBit(id)
						s.Crash(id)
						s.Restart(id)
					}
					s.After(10+s.Rand().Int64N(60), func() {
						s.Crash(l)
						s.Heal()
						s.After(300, step)
					})
				})
			})
		}
		s.At(from, step)
	}},
}

// Rung5 adds clocks that disagree (README): Members' clocks run at
// different speeds, or jump. Each scenario also moves the Leader around,
// because a clock only matters while its Member is the one answering.
var Rung5 = []Scenario{
	// Every Member's clock runs at its own speed.
	{"clock-skew", func(s *sim.Sim, from, to int64) {
		s.At(from, func() {
			rates := []int64{60, 80, 100, 125, 170}
			for _, id := range s.IDs() {
				s.SetClockRate(id, rates[s.Rand().IntN(len(rates))])
			}
		})
		moveLeader(s, from, to)
	}},

	// Clocks jump, forwards and backwards, by more than any time-to-live.
	{"clock-jumps", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to {
				return
			}
			ids := s.IDs()
			jump := 300 + s.Rand().Int64N(1500)
			if s.Rand().IntN(2) == 0 {
				jump = -jump
			}
			s.JumpClock(ids[s.Rand().IntN(len(ids))], jump)
			s.After(150+s.Rand().Int64N(250), step)
		}
		s.At(from, step)
		moveLeader(s, from, to)
	}},

	// The Leader's clock slows to a fifth of true speed, and then it is cut
	// off from the others. It notices the silence five times later than it
	// should, while the others elect a Leader on time.
	{"slow-leader", func(s *sim.Sim, from, to int64) {
		var step func()
		step = func() {
			if s.Now() >= to-600 {
				return
			}
			if l := leader(s); l != 0 {
				s.SetClockRate(l, 20)
				s.Partition([]core.NodeID{l}, others(s, l))
				s.After(450, func() {
					s.Heal()
					s.SetClockRate(l, 100)
				})
			}
			s.After(700+s.Rand().Int64N(200), step)
		}
		s.At(from, step)
	}},
}

// moveLeader pauses whichever Member leads, every so often, for long enough
// that another takes over.
func moveLeader(s *sim.Sim, from, to int64) {
	var step func()
	step = func() {
		if s.Now() >= to-300 {
			return
		}
		if l := leader(s); l != 0 {
			s.Crash(l)
			s.After(250, func() { s.Restart(l) })
		}
		s.After(400+s.Rand().Int64N(300), step)
	}
	s.At(from+100, step)
}
