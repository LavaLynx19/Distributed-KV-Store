// Package sim is the Simulation shell (A§4.3, A§8.1): it runs every Member's
// core in one process and delivers ticks, messages and client proposals from
// a single queue ordered by virtual time. A seed fixes the whole run, so a
// failing seed can be replayed exactly.
package sim

import (
	"container/heap"
	"hash/fnv"
	"math/rand/v2"
	"slices"

	"distributed-kv-store/internal/core"
)

// Machine is the state machine a Member applies Committed Entries to. Apply
// returns the response for the client that proposed the Entry.
type Machine interface {
	Apply(core.Entry) []byte
}

// Config describes one simulated Group.
type Config struct {
	Seed  uint64
	Nodes int // Members are numbered 1..Nodes

	// NewNode builds a Member's core. members lists every Member, id included.
	NewNode func(id core.NodeID, members []core.NodeID, rng core.Rand) core.Node
	// NewMachine builds a Member's state machine.
	NewMachine func() Machine

	// Copy, if set, stands in for the network's encoding: every message is
	// passed through it on the way, so Members never share memory.
	Copy func(core.Message) core.Message

	// Virtual time is in arbitrary units. Each Member ticks every TickEvery
	// units, and a message takes MinDelay..MaxDelay units. Zero values take
	// the defaults 10, 1 and 8.
	TickEvery int64
	MinDelay  int64
	MaxDelay  int64
}

// Reply is what a client learns about one proposal.
type Reply struct {
	// Refused: the Member was down, so the proposal was never handed to it.
	Refused bool
	// Reason is the core's verdict. Unknown also covers a Member that crashed
	// while the proposal was pending.
	Reason   core.Reason
	Response []byte
	Leader   core.NodeID
}

type member struct {
	id      core.NodeID
	core    core.Node
	machine Machine
	up      bool
	pending map[uint64]func(Reply)
}

// Sim is one simulated Group. It is not safe for concurrent use: everything
// runs on the caller's goroutine.
type Sim struct {
	cfg     Config
	rng     *rand.Rand
	now     int64
	seq     uint64
	queue   eventQueue
	members map[core.NodeID]*member
	ids     []core.NodeID
	blocked map[[2]core.NodeID]bool // directed: [from, to]
	loss    float64                 // chance that any one message is lost
	dup     float64                 // chance that any one message arrives twice
	nextRef uint64
	digest  uint64
}

// New builds a Group and schedules each Member's first tick.
func New(cfg Config) *Sim {
	if cfg.TickEvery == 0 {
		cfg.TickEvery = 10
	}
	if cfg.MinDelay == 0 {
		cfg.MinDelay = 1
	}
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = 8
	}
	s := &Sim{
		cfg:     cfg,
		rng:     rand.New(rand.NewPCG(cfg.Seed, 0x5eed)),
		members: map[core.NodeID]*member{},
		blocked: map[[2]core.NodeID]bool{},
		digest:  14695981039346656037, // FNV-1a offset basis
	}
	for i := 1; i <= cfg.Nodes; i++ {
		s.ids = append(s.ids, core.NodeID(i))
	}
	for _, id := range s.ids {
		m := &member{
			id:      id,
			core:    cfg.NewNode(id, slices.Clone(s.ids), rand.New(rand.NewPCG(cfg.Seed, uint64(id)))),
			machine: cfg.NewMachine(),
			up:      true,
			pending: map[uint64]func(Reply){},
		}
		s.members[id] = m
		// Members tick out of phase with each other.
		s.schedule(s.rng.Int64N(cfg.TickEvery), func() { s.tick(m) })
	}
	return s
}

// Now is the current virtual time.
func (s *Sim) Now() int64 { return s.now }

// IDs lists the Members in order.
func (s *Sim) IDs() []core.NodeID { return slices.Clone(s.ids) }

// Status is a Member's own view of the Group.
func (s *Sim) Status(id core.NodeID) core.Status { return s.members[id].core.Status() }

// Machine is a Member's state machine, for End-state comparison.
func (s *Sim) Machine(id core.NodeID) Machine { return s.members[id].machine }

// Up reports whether a Member is running.
func (s *Sim) Up(id core.NodeID) bool { return s.members[id].up }

// Rand is the run's seeded source, for clients and Fault scripts that must be
// part of the same reproducible run.
func (s *Sim) Rand() *rand.Rand { return s.rng }

// Digest summarizes every event processed so far. Two runs with the same
// seed and the same script have equal digests.
func (s *Sim) Digest() uint64 { return s.digest }

// At runs fn when virtual time reaches t (or now, if t has passed).
func (s *Sim) At(t int64, fn func()) { s.schedule(max(t-s.now, 0), fn) }

// After runs fn after d units of virtual time.
func (s *Sim) After(d int64, fn func()) { s.schedule(d, fn) }

// Run processes events in order until virtual time would pass until.
func (s *Sim) Run(until int64) {
	for s.queue.Len() > 0 && s.queue[0].at <= until {
		e := heap.Pop(&s.queue).(event)
		s.now = e.at
		e.fn()
	}
	s.now = until
}

// Propose hands a command to one Member, as a client would. done is called
// exactly once, with the outcome.
func (s *Sim) Propose(to core.NodeID, payload []byte, done func(Reply)) {
	m := s.members[to]
	if !m.up {
		done(Reply{Refused: true})
		return
	}
	s.nextRef++
	ref := s.nextRef
	m.pending[ref] = done
	s.mix('P', uint64(to), ref)
	s.step(m, core.Propose{Ref: ref, Payload: payload})
}

// Crash stops a Member. It receives nothing while down, messages to and from
// it are lost, and its pending proposals end as Unknown. In Rungs 1–2 its
// state survives intact (PLAN §P1), so a crash behaves like a long freeze.
func (s *Sim) Crash(id core.NodeID) {
	m := s.members[id]
	if !m.up {
		return
	}
	m.up = false
	s.mix('C', uint64(id), 0)
	refs := make([]uint64, 0, len(m.pending))
	for ref := range m.pending {
		refs = append(refs, ref)
	}
	slices.Sort(refs)
	for _, ref := range refs {
		done := m.pending[ref]
		delete(m.pending, ref)
		done(Reply{Reason: core.Unknown})
	}
}

// Restart brings a crashed Member back.
func (s *Sim) Restart(id core.NodeID) {
	m := s.members[id]
	if m.up {
		return
	}
	m.up = true
	s.mix('R', uint64(id), 0)
}

// Partition cuts the network between the given sets of Members, in both
// directions. Members within a set still reach each other. It replaces any
// earlier Partition.
func (s *Sim) Partition(sets ...[]core.NodeID) {
	clear(s.blocked)
	for i, a := range sets {
		for j, b := range sets {
			if i == j {
				continue
			}
			for _, from := range a {
				for _, to := range b {
					s.blocked[[2]core.NodeID{from, to}] = true
				}
			}
		}
	}
	s.mix('X', uint64(len(s.blocked)), 0)
}

// Reachable reports whether a message from a could reach b right now: no
// Partition separates them in that direction.
func (s *Sim) Reachable(a, b core.NodeID) bool { return !s.blocked[[2]core.NodeID{a, b}] }

// Cut blocks messages from each Member in from to each Member in to, in that
// direction only: a one-way Partition. It adds to whatever is already cut.
func (s *Sim) Cut(from, to []core.NodeID) {
	for _, a := range from {
		for _, b := range to {
			if a != b {
				s.blocked[[2]core.NodeID{a, b}] = true
			}
		}
	}
	s.mix('U', uint64(len(s.blocked)), 0)
}

// SetDelay changes how long messages take from now on: min..max units each,
// chosen independently. A wide range reorders messages, since a later one
// can overtake an earlier one.
func (s *Sim) SetDelay(min, max int64) {
	s.cfg.MinDelay, s.cfg.MaxDelay = min, max
	s.mix('Y', uint64(min), uint64(max))
}

// SetDuplicate makes each message arrive a second time, after its own delay,
// with probability p.
func (s *Sim) SetDuplicate(p float64) {
	s.dup = p
	s.mix('2', uint64(p*1e6), 0)
}

// SetLoss makes each message independently lost with probability p.
func (s *Sim) SetLoss(p float64) {
	s.loss = p
	s.mix('L', uint64(p*1e6), 0)
}

// Heal removes every Partition.
func (s *Sim) Heal() {
	clear(s.blocked)
	s.mix('H', 0, 0)
}

func (s *Sim) tick(m *member) {
	s.schedule(s.cfg.TickEvery, func() { s.tick(m) })
	if m.up {
		s.mix('T', uint64(m.id), 0)
		s.step(m, core.Tick{})
	}
}

// step feeds one event to a Member's core and carries out its Output.
func (s *Sim) step(m *member, ev core.Event) {
	out := m.core.Step(ev)

	responses := make(map[core.Index][]byte, len(out.Committed))
	for _, e := range out.Committed {
		responses[e.Index] = m.machine.Apply(e)
	}
	for _, r := range out.Results {
		done, ok := m.pending[r.Ref]
		if !ok {
			continue
		}
		delete(m.pending, r.Ref)
		done(Reply{Reason: r.Reason, Response: responses[r.Index], Leader: r.Leader})
	}
	for _, msg := range out.Messages {
		s.send(msg)
	}
}

func (s *Sim) send(msg core.Message) {
	if s.loss > 0 && s.rng.Float64() < s.loss {
		s.mix('D', uint64(msg.From), uint64(msg.To))
		return
	}
	if s.cfg.Copy != nil {
		msg = s.cfg.Copy(msg)
	}
	s.schedule(s.delay(), func() { s.deliver(msg) })
	if s.dup > 0 && s.rng.Float64() < s.dup {
		s.schedule(s.delay(), func() { s.deliver(msg) })
	}
}

func (s *Sim) delay() int64 {
	return s.cfg.MinDelay + s.rng.Int64N(s.cfg.MaxDelay-s.cfg.MinDelay+1)
}

// deliver hands a message over unless the link is cut or either end is down
// at arrival time.
func (s *Sim) deliver(msg core.Message) {
	to, ok := s.members[msg.To]
	if !ok || !to.up || !s.members[msg.From].up || s.blocked[[2]core.NodeID{msg.From, msg.To}] {
		s.mix('D', uint64(msg.From), uint64(msg.To))
		return
	}
	s.mix('M', uint64(msg.From), uint64(msg.To))
	s.step(to, core.Receive{Msg: msg})
}

func (s *Sim) schedule(delay int64, fn func()) {
	s.seq++
	heap.Push(&s.queue, event{at: s.now + delay, seq: s.seq, fn: fn})
}

// mix folds one processed event into the digest.
func (s *Sim) mix(kind byte, a, b uint64) {
	h := fnv.New64a()
	var buf [33]byte
	buf[0] = kind
	for i, v := range [4]uint64{s.digest, uint64(s.now), a, b} {
		for j := range 8 {
			buf[1+i*8+j] = byte(v >> (8 * j))
		}
	}
	h.Write(buf[:])
	s.digest = h.Sum64()
}

type event struct {
	at  int64
	seq uint64 // breaks ties, so equal-time events run in scheduling order
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
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}
