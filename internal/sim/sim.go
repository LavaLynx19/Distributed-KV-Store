// Package sim is the Simulation shell (A§4.3, A§8.1): it runs every Member's
// core in one process and delivers ticks, messages and client proposals from
// a single queue ordered by virtual time. A seed fixes the whole run, so a
// failing seed can be replayed exactly.
package sim

import (
	"container/heap"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/storage"
)

// dataDir is each Member's data directory on its own simulated filesystem.
const dataDir = "data"

// must stops the run on a storage error. On the in-memory filesystem that
// can only mean the Store was asked to do something impossible.
func must(err error) {
	if err != nil {
		panic("sim: " + err.Error())
	}
}

// Machine is the state machine a Member applies Committed Entries to. Apply
// returns the response for the client that proposed the Entry. Read answers
// a query from the current state without an Entry. Capture returns a function
// that encodes the state as it was when Capture was called, and Restore
// replaces the state with an encoded one.
type Machine interface {
	Apply(core.Entry) []byte
	Read(query []byte) []byte
	Capture() func() []byte
	Restore(data []byte) error
}

// observer is a Machine that wants to know its Member's clock before it
// applies or answers anything.
type observer interface{ Observe(now int64) }

// Config describes one simulated Group.
type Config struct {
	Seed  uint64
	Nodes int // Members are numbered 1..Nodes

	// NewNode builds a Member's core. members lists every Member, id included.
	NewNode func(id core.NodeID, members []core.NodeID, rng core.Rand) core.Node
	// NewMachine builds a Member's state machine.
	NewMachine func() Machine

	// Restart, if set, makes a crash lose everything that wasn't durable: a
	// restarted Member gets a new core built from its simulated disk, and a
	// new state machine. If nil, a crashed Member resumes with its memory
	// intact, as if frozen (Rungs 1–2, PLAN §P1).
	Restart func(id core.NodeID, members []core.NodeID, rng core.Rand, stored core.Stored) core.Node
	// DiskDelay is how long a write takes to become durable: DiskDelay[0] to
	// DiskDelay[1] units. Zero means at once.
	DiskDelay [2]int64
	// TearWrites makes a crash treat a write in progress as a real disk
	// might: part of it may survive, and part of that may be zeros (Rung 4).
	// It only matters with Restart set.
	TearWrites bool
	// UncheckedDisk makes Members store their files without checksums, as
	// in Rung 3, so that Rung 4's exposure of that stays reproducible.
	UncheckedDisk bool
	// SnapshotEvery makes each Member take a Snapshot of its state machine
	// whenever it has applied this many Entries since the last one, and hand
	// it to the core (A§6.4). Zero means never.
	SnapshotEvery int

	// Stamp, if set, puts a Member's clock reading into each proposal it is
	// handed, as the real shell does (A§6.1).
	Stamp func(payload []byte, now int64) []byte

	// TimeEntry, if set, is asked on every tick of a Member that leads
	// whether it has something to propose so that time moves in a Group
	// nobody is writing to (A§6.7). It returns the proposal, or nil.
	TimeEntry func(m Machine, now int64) []byte

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
	queries map[uint64][]byte // the query of each pending read, by Ref

	// The simulated disk (A§8.1): the real storage code on a filesystem held
	// in memory, which loses whatever wasn't synced when the Member crashes
	// and can be made to tear writes and flip bits. startErr is why the last
	// restart failed, if it did: the Member then stays down.
	fs       *storage.MemFS
	store    *storage.Store
	startErr error
	// While a
	// write is on its way there the Member does nothing else: events wait in
	// inbox. A crash discards the write in progress and the inbox.
	writing      bool
	inbox        []core.Event
	life         int   // counts crashes, so a write from a past life is ignored
	stalledUntil int64 // writes don't complete before this time

	// The Member's clock (A§8.1). It read clockBase at virtual time clockAt
	// and has run at clockRate percent of true speed since. Its ticks come
	// at the same rate, so a slow clock also means slow timeouts.
	clockBase, clockAt int64
	clockRate          int64

	timePending bool // a time Entry this Member proposed is still undecided

	applied    core.Index // the last Entry applied to machine
	snapshotAt core.Index // the Entry the last Snapshot was taken at
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
			queries: map[uint64][]byte{},
			fs:      storage.NewMemFS(),

			clockRate: 100,
		}
		m.store, _, _ = s.openDisk(m.fs) // an empty MemFS can't fail to open
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
	if s.cfg.Stamp != nil {
		payload = s.cfg.Stamp(payload, m.clock(s.now))
	}
	s.step(m, core.Propose{Ref: ref, Payload: payload})
}

// Read asks one Member to answer a query from its own state, bypassing the
// Log. done is called exactly once.
func (s *Sim) Read(to core.NodeID, query []byte, done func(Reply)) {
	m := s.members[to]
	if !m.up {
		done(Reply{Refused: true})
		return
	}
	s.nextRef++
	ref := s.nextRef
	m.pending[ref] = done
	m.queries[ref] = query
	s.mix('Q', uint64(to), ref)
	s.step(m, core.Read{Ref: ref})
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
	m.life++
	m.writing = false
	m.inbox = nil
	s.mix('C', uint64(id), 0)
	if s.cfg.Restart != nil {
		// The process is gone, and with it anything not yet on disk.
		if s.cfg.TearWrites {
			m.fs.Crash(s.rng, true)
		} else {
			m.fs.Crash(nil, false)
		}
	}
	refs := make([]uint64, 0, len(m.pending))
	for ref := range m.pending {
		refs = append(refs, ref)
	}
	slices.Sort(refs)
	for _, ref := range refs {
		done := m.pending[ref]
		delete(m.pending, ref)
		delete(m.queries, ref)
		done(Reply{Reason: core.Unknown})
	}
}

// Restart brings a crashed Member back. With Config.Restart set, it comes
// back with only what its disk holds.
func (s *Sim) Restart(id core.NodeID) {
	m := s.members[id]
	if m.up {
		return
	}
	s.mix('R', uint64(id), 0)
	if s.cfg.Restart == nil {
		m.up = true
		return
	}
	store, stored, err := s.openDisk(m.fs)
	if err == nil && stored.Snapshot != nil {
		machine := s.cfg.NewMachine()
		if err = machine.Restore(stored.Snapshot.Data); err == nil {
			m.machine = machine
			m.applied, m.snapshotAt = stored.Snapshot.Index, stored.Snapshot.Index
		}
	} else if err == nil {
		m.machine = s.cfg.NewMachine()
		m.applied, m.snapshotAt = 0, 0
	}
	if err != nil {
		// What is on disk can't be read back. A real process would exit.
		m.startErr = err
		s.mix('E', uint64(id), 0)
		return
	}
	m.up, m.store, m.startErr = true, store, nil
	rng := rand.New(rand.NewPCG(s.cfg.Seed, uint64(id)+uint64(m.life)<<32))
	m.core = s.cfg.Restart(id, slices.Clone(s.ids), rng, stored)
}

func (s *Sim) openDisk(fs *storage.MemFS) (*storage.Store, core.Stored, error) {
	return storage.OpenWith(fs, dataDir, storage.Options{Unchecked: s.cfg.UncheckedDisk})
}

// StartError is why a Member's last restart failed, or nil. A Member that
// can't read its own disk stays down.
func (s *Sim) StartError(id core.NodeID) error { return s.members[id].startErr }

// Disk is what a Member would find on its disk if it crashed now and
// restarted. It panics if the disk can't be read.
func (s *Sim) Disk(id core.NodeID) core.Stored {
	_, stored, err := s.openDisk(s.members[id].fs.Durable())
	if err != nil {
		panic(fmt.Sprintf("sim: node %d's disk is unreadable: %v", id, err))
	}
	return stored
}

// FlipBit inverts one bit somewhere in what a Member has on disk, and
// reports which file, or "" if its disk is empty. Nothing notices until the
// Member next reads its disk, which is when it restarts.
func (s *Sim) FlipBit(id core.NodeID) string {
	path := s.members[id].fs.FlipBit(s.rng)
	s.mix('B', uint64(id), uint64(len(path)))
	return path
}

// StallDisk makes a Member's disk unresponsive for d units: any write it has
// in progress or starts in that time completes only afterwards.
func (s *Sim) StallDisk(id core.NodeID, d int64) {
	m := s.members[id]
	m.stalledUntil = max(m.stalledUntil, s.now+d)
	s.mix('S', uint64(id), uint64(d))
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

// Clock is what a Member's own clock reads now, in the units of virtual
// time. Clocks start at zero and agree until a Fault makes them differ.
func (s *Sim) Clock(id core.NodeID) int64 { return s.members[id].clock(s.now) }

func (m *member) clock(now int64) int64 {
	return m.clockBase + (now-m.clockAt)*m.clockRate/100
}

// SetClockRate makes a Member's clock run at percent of true speed from now
// on: 50 is half speed, 200 double. Its ticks slow down or speed up with it.
func (s *Sim) SetClockRate(id core.NodeID, percent int64) {
	if percent <= 0 {
		panic("sim: a clock must move forwards")
	}
	m := s.members[id]
	m.clockBase, m.clockAt, m.clockRate = m.clock(s.now), s.now, percent
	s.mix('C', uint64(id), uint64(percent))
}

// JumpClock moves a Member's clock by delta at once, forwards or backwards.
// Its ticks are unaffected: a jump changes what time it is, not how fast
// time passes.
func (s *Sim) JumpClock(id core.NodeID, delta int64) {
	m := s.members[id]
	m.clockBase, m.clockAt = m.clock(s.now)+delta, s.now
	s.mix('J', uint64(id), uint64(delta))
}

func (s *Sim) tick(m *member) {
	s.schedule(max(s.cfg.TickEvery*100/m.clockRate, 1), func() { s.tick(m) })
	if m.up {
		s.mix('T', uint64(m.id), 0)
		s.step(m, core.Tick{})
		s.timeEntry(m)
	}
}

// timeEntry proposes a time Entry on a Leader's behalf, one at a time.
func (s *Sim) timeEntry(m *member) {
	if s.cfg.TimeEntry == nil || m.timePending || !m.up || m.core.Status().Role != core.LeaderRole {
		return
	}
	payload := s.cfg.TimeEntry(m.machine, m.clock(s.now))
	if payload == nil {
		return
	}
	m.timePending = true
	s.Propose(m.id, payload, func(Reply) { m.timePending = false })
}

// step feeds one event to a Member's core and carries out its Output. If the
// Output has something to store, that happens first, and takes time: until
// the disk is done the Member handles nothing else (A§4.2, the order rule).
func (s *Sim) step(m *member, ev core.Event) {
	if m.writing {
		m.inbox = append(m.inbox, ev)
		return
	}
	out := m.core.Step(ev)
	if out.Persist == nil {
		s.finish(m, out)
		return
	}
	delay := s.cfg.DiskDelay[0]
	if span := s.cfg.DiskDelay[1] - s.cfg.DiskDelay[0]; span > 0 {
		delay += s.rng.Int64N(span + 1)
	}
	delay = max(delay, m.stalledUntil-s.now)
	// The write starts now: its bytes reach the filesystem, unsynced. It is
	// durable only once the sync below has run.
	must(m.store.Write(out.Persist))
	must(m.store.Flush())
	if delay <= 0 {
		must(m.store.Sync())
		s.finish(m, out)
		return
	}
	m.writing = true
	life := m.life
	s.schedule(delay, func() {
		if m.life != life {
			return // the Member crashed with this write in progress: it's lost
		}
		// A stall that began after the write started still holds it up.
		if wait := m.stalledUntil - s.now; wait > 0 {
			s.schedule(wait, func() { s.completeWrite(m, life, out) })
			return
		}
		s.completeWrite(m, life, out)
	})
}

func (s *Sim) completeWrite(m *member, life int, out core.Output) {
	if m.life != life {
		return
	}
	must(m.store.Sync())
	m.writing = false
	s.mix('W', uint64(m.id), 0)
	s.finish(m, out)
	for len(m.inbox) > 0 && !m.writing && m.up && m.life == life {
		ev := m.inbox[0]
		m.inbox = m.inbox[1:]
		s.step(m, ev)
	}
}

// finish carries out everything in an Output except its Persist.
func (s *Sim) finish(m *member, out core.Output) {
	if snap := out.Restore; snap != nil {
		if err := m.machine.Restore(snap.Data); err != nil {
			panic(fmt.Sprintf("sim: node %d can't install a Snapshot: %v", m.id, err))
		}
		m.applied, m.snapshotAt = snap.Index, snap.Index
	}
	if o, ok := m.machine.(observer); ok && len(out.Committed)+len(out.Reads) > 0 {
		o.Observe(m.clock(s.now))
	}
	responses := make(map[core.Index][]byte, len(out.Committed))
	for _, e := range out.Committed {
		responses[e.Index] = m.machine.Apply(e)
		m.applied = e.Index
	}
	for _, r := range out.Results {
		done, ok := m.pending[r.Ref]
		if !ok {
			continue
		}
		delete(m.pending, r.Ref)
		done(Reply{Reason: r.Reason, Response: responses[r.Index], Leader: r.Leader})
	}
	for _, r := range out.Reads {
		done, ok := m.pending[r.Ref]
		if !ok {
			continue
		}
		reply := Reply{Reason: r.Reason, Leader: r.Leader}
		if r.Reason == core.OK {
			reply.Response = m.machine.Read(m.queries[r.Ref])
		}
		delete(m.pending, r.Ref)
		delete(m.queries, r.Ref)
		done(reply)
	}
	for _, msg := range out.Messages {
		s.send(msg)
	}
	if n := s.cfg.SnapshotEvery; n > 0 && int(m.applied-m.snapshotAt) >= n {
		m.snapshotAt = m.applied
		s.mix('N', uint64(m.id), uint64(m.applied))
		s.step(m, core.Snapshotted{Index: m.applied, Data: m.machine.Capture()()})
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
