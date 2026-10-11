// Package lsim runs the Leaderless variant (A§13) in one process with time,
// network and disk under the test's control, so that a run can be repeated
// exactly from a seed. It is to internal/leaderless what internal/sim is to
// the consensus core, and shares nothing with it: the two cores have
// different contracts.
package lsim

import (
	"container/heap"
	"maps"
	"math/rand/v2"
	"slices"

	"distributed-kv-store/internal/leaderless"
)

// Config sets up a simulated store.
type Config struct {
	Seed  uint64
	Nodes int
	// W, R, Mode, DeliverHints and Resend are passed to every Node
	// (leaderless.Config).
	W, R                 int
	Mode                 leaderless.Mode
	DeliverHints, Resend bool
	// Exchange, Repair and Forgetful are passed on too.
	Exchange, Repair, Forgetful bool
	// Skew is how far each Node's wall clock is from the true time, by
	// Node. A Node not named keeps true time.
	Skew map[leaderless.NodeID]int64
}

const (
	tickEvery = 5 // units between a Node's ticks
	netMin    = 1 // a Message takes this long,
	netMax    = 8 // to this long
	diskMin   = 1 // and a write to disk this long,
	diskMax   = 6 // to this long
)

// member is one simulated Node.
type member struct {
	id   leaderless.NodeID
	node *leaderless.Node
	// disk is what the Node has made durable. It is what a restart begins
	// from.
	disk leaderless.Stored
	up   bool
	gone bool
	// life counts the times the Node has stopped. Whatever it was doing
	// before a stop is not finished after it.
	life int
	// busy is when the Node's disk is next free. A Node does one thing at
	// a time: what a step asks for happens after everything asked before
	// it.
	busy int64
	side int
	// waiting are the clients waiting on this Node, by request.
	waiting map[uint64]func(leaderless.Reply)
}

// Sim is one simulated store.
type Sim struct {
	cfg     Config
	now     int64
	seq     uint64
	rng     *rand.Rand
	queue   eventQueue
	members map[leaderless.NodeID]*member
	ids     []leaderless.NodeID
	nextRef uint64
	// Traces is what each write did, by request (leaderless.Trace).
	Traces map[uint64]leaderless.Trace
	// Cuts are the times during which some Node couldn't reach another:
	// each a start and an end, the last end 0 while a Partition lasts.
	Cuts [][2]int64
}

// New returns a store of cfg.Nodes Nodes, all running.
func New(cfg Config) *Sim {
	s := &Sim{cfg: cfg, rng: rand.New(rand.NewPCG(cfg.Seed, 0x1eade41e55)), members: map[leaderless.NodeID]*member{}, Traces: map[uint64]leaderless.Trace{}}
	for i := 1; i <= cfg.Nodes; i++ {
		s.ids = append(s.ids, leaderless.NodeID(i))
	}
	for _, id := range s.ids {
		m := &member{id: id, up: true, waiting: map[uint64]func(leaderless.Reply){}}
		s.members[id] = m
		s.start(m)
		s.After(1+s.rng.Int64N(tickEvery), func() { s.tick(m) })
	}
	return s
}

func cloneStored(d leaderless.Stored) leaderless.Stored {
	return leaderless.Stored{Data: maps.Clone(d.Data), Hints: maps.Clone(d.Hints), Counts: maps.Clone(d.Counts)}
}

func (s *Sim) start(m *member) {
	m.node = leaderless.New(leaderless.Config{
		ID: m.id, Nodes: s.ids, W: s.cfg.W, R: s.cfg.R, Mode: s.cfg.Mode,
		DeliverHints: s.cfg.DeliverHints, Resend: s.cfg.Resend,
		Exchange: s.cfg.Exchange, Repair: s.cfg.Repair, Forgetful: s.cfg.Forgetful,
		Stored: cloneStored(m.disk), Seed: s.rng.Uint64() >> 1,
	})
}

// Now is the time.
func (s *Sim) Now() int64 { return s.now }

// Rand is the run's source of randomness.
func (s *Sim) Rand() *rand.Rand { return s.rng }

// IDs lists the Nodes.
func (s *Sim) IDs() []leaderless.NodeID { return slices.Clone(s.ids) }

// Up reports whether a Node is running.
func (s *Sim) Up(id leaderless.NodeID) bool { return s.members[id].up }

// Node is a running Node's core, for looking at what it holds.
func (s *Sim) Node(id leaderless.NodeID) *leaderless.Node { return s.members[id].node }

// After runs fn after d units.
func (s *Sim) After(d int64, fn func()) {
	s.seq++
	heap.Push(&s.queue, event{at: s.now + max(d, 0), seq: s.seq, fn: fn})
}

// At runs fn at time t, or now if that is past.
func (s *Sim) At(t int64, fn func()) { s.After(t-s.now, fn) }

// Run carries the store on until time until.
func (s *Sim) Run(until int64) {
	for len(s.queue) > 0 && s.queue[0].at <= until {
		ev := heap.Pop(&s.queue).(event)
		s.now = ev.at
		ev.fn()
	}
	s.now = max(s.now, until)
}

func (s *Sim) tick(m *member) {
	s.After(tickEvery, func() { s.tick(m) })
	if m.up {
		s.step(m, leaderless.Tick{})
	}
}

// step hands a Node one Event and does what its Output asks, in order: what
// is to be stored first, which takes time, and only then the rest.
func (s *Sim) step(m *member, ev leaderless.Event) {
	out := m.node.Step(ev)
	for _, t := range out.Traces {
		s.Traces[t.Ref] = t
	}
	if len(out.Persist) == 0 && len(out.Messages) == 0 && len(out.Replies) == 0 {
		return
	}
	done := max(s.now, m.busy)
	if len(out.Persist) > 0 {
		done += diskMin + s.rng.Int64N(diskMax-diskMin+1)
	}
	m.busy = done
	life := m.life
	s.At(done, func() {
		if m.life != life {
			return // the Node stopped before the write reached its disk
		}
		m.disk.Apply(out.Persist)
		for _, msg := range out.Messages {
			s.send(msg)
		}
		for _, r := range out.Replies {
			if back := m.waiting[r.Ref]; back != nil {
				delete(m.waiting, r.Ref)
				back(r)
			}
		}
	})
}

// Reachable reports whether a Message from a would reach b.
func (s *Sim) Reachable(a, b leaderless.NodeID) bool {
	return s.members[a].side == s.members[b].side
}

func (s *Sim) send(msg leaderless.Message) {
	if !s.Reachable(msg.From, msg.To) {
		return
	}
	s.After(netMin+s.rng.Int64N(netMax-netMin+1), func() {
		to := s.members[msg.To]
		if to.up && s.Reachable(msg.From, msg.To) {
			s.step(to, leaderless.Receive{Msg: msg})
		}
	})
}

// Request has a client ask Node id for something. done is called with the
// Node's Reply, and never if the Node stops first. It reports false, and
// never calls done, if the Node isn't running: the request was never made.
func (s *Sim) Request(id leaderless.NodeID, op leaderless.Op, key, elem string, done func(leaderless.Reply)) (ref uint64, made bool) {
	return s.RequestTTL(id, op, key, elem, 0, done)
}

// Clock is a Node's wall clock.
func (s *Sim) Clock(id leaderless.NodeID) int64 { return s.now + s.cfg.Skew[id] }

// RequestTTL is Request for an addition that is to show for ttl units of
// the Node's clock.
func (s *Sim) RequestTTL(id leaderless.NodeID, op leaderless.Op, key, elem string, ttl int64, done func(leaderless.Reply)) (ref uint64, made bool) {
	m := s.members[id]
	if !m.up {
		return 0, false
	}
	s.nextRef++
	ref = s.nextRef
	m.waiting[ref] = done
	s.step(m, leaderless.Request{Ref: ref, Op: op, Key: key, Elem: elem, Now: s.Clock(id), TTL: ttl})
	return ref, true
}

// Crash stops a Node. It keeps what had reached its disk.
func (s *Sim) Crash(id leaderless.NodeID) {
	m := s.members[id]
	if !m.up {
		return
	}
	m.up = false
	m.life++
	m.busy = 0
	clear(m.waiting)
}

// Restart starts a stopped Node from its disk.
func (s *Sim) Restart(id leaderless.NodeID) {
	m := s.members[id]
	if m.up || m.gone {
		return
	}
	m.up = true
	s.start(m)
}

// Destroy stops a Node for good and loses its disk.
func (s *Sim) Destroy(id leaderless.NodeID) {
	s.Crash(id)
	s.members[id].gone = true
	s.members[id].disk = leaderless.Stored{}
}

// Partition splits the Nodes into sides that can't reach each other. A Node
// in none of the sets is on a side of its own with the others like it.
func (s *Sim) Partition(sets ...[]leaderless.NodeID) {
	for _, m := range s.members {
		m.side = 0
	}
	for i, set := range sets {
		for _, id := range set {
			s.members[id].side = i + 1
		}
	}
	if n := len(s.Cuts); n == 0 || s.Cuts[n-1][1] != 0 {
		s.Cuts = append(s.Cuts, [2]int64{s.now, 0})
	}
}

// Heal ends any Partition.
func (s *Sim) Heal() {
	for _, m := range s.members {
		m.side = 0
	}
	if n := len(s.Cuts); n > 0 && s.Cuts[n-1][1] == 0 {
		s.Cuts[n-1][1] = s.now
	}
}

// Cut reports whether a Partition was in force at time t.
func (s *Sim) Cut(t int64) bool {
	for _, c := range s.Cuts {
		if t >= c[0] && (c[1] == 0 || t < c[1]) {
			return true
		}
	}
	return false
}

type event struct {
	at  int64
	seq uint64
	fn  func()
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(event)) }
func (q *eventQueue) Pop() any {
	old := *q
	x := old[len(old)-1]
	*q = old[:len(old)-1]
	return x
}
