// Package core defines the contract between a consensus core and the shell
// that drives it (A§4.2). A core is a pure step function: the shell hands it
// one Event, and it returns an Output saying what to send, what is Committed
// and what became of each proposal. A core owns no threads, clocks, sockets
// or files, so the same core runs under the real shell and the Simulation.
//
// Durability follows one rule: the shell makes an Output's Persist durable
// before it does anything else the Output asks for. A core may therefore
// treat whatever it has put in a Persist as safely stored once Step returns.
package core

// NodeID identifies a Node. Zero means "none" (for example, no known Leader).
type NodeID uint64

// Term numbers a period in which a Group has at most one Leader.
type Term uint64

// Index is an Entry's position in the Log. The first Entry has Index 1.
type Index uint64

// EntryKind says what an Entry carries.
type EntryKind uint8

const (
	// EntryNoop carries nothing. A new Leader appends one to commit an Entry
	// of its own Term.
	EntryNoop EntryKind = iota
	// EntryCommand carries a state machine command in Payload.
	EntryCommand
)

// Entry is one change in the Log.
type Entry struct {
	Index   Index
	Term    Term
	Kind    EntryKind
	Payload []byte
}

// HardState is what a Member must remember across a restart besides its Log:
// the latest Term it has seen and who it voted for in that Term.
type HardState struct {
	Term     Term
	VotedFor NodeID
}

// Snapshot is a state machine's full contents as of the Entry at Index, which
// had Term. It stands in for the Log up to and including that Entry.
type Snapshot struct {
	Index Index
	Term  Term
	Data  []byte
}

// Persist is the change a step makes to a Member's durable state. Its parts
// apply in the order of its fields.
type Persist struct {
	// HardState replaces the stored one.
	HardState *HardState
	// Snapshot replaces the stored one, and Entries it covers are dropped.
	// With ResetLog every stored Entry is dropped, covered or not.
	Snapshot *Snapshot
	ResetLog bool
	// TruncateFrom drops stored Entries at this Index and after. Zero means
	// none.
	TruncateFrom Index
	// Entries are appended. They continue the stored Log without a gap.
	Entries []Entry
}

// Stored is a Member's durable state, as a restarted core is given it.
type Stored struct {
	HardState HardState
	Snapshot  *Snapshot
	// Entries follow the Snapshot, or start at Index 1 if there is none.
	Entries []Entry
}

// Apply makes the change p describes. Both shells keep a Member's durable
// state through this one function, so they can't disagree on what a Persist
// means.
func (s *Stored) Apply(p *Persist) {
	if p.HardState != nil {
		s.HardState = *p.HardState
	}
	if p.Snapshot != nil {
		first := s.firstIndex()
		s.Snapshot = p.Snapshot
		switch covered := p.Snapshot.Index + 1 - first; {
		case p.ResetLog || covered >= Index(len(s.Entries)):
			s.Entries = nil
		case p.Snapshot.Index >= first:
			s.Entries = append([]Entry(nil), s.Entries[covered:]...)
		}
	}
	if p.TruncateFrom != 0 {
		first := s.firstIndex()
		if p.TruncateFrom < first {
			panic("core: Persist truncates into the Snapshot")
		}
		if keep := p.TruncateFrom - first; keep < Index(len(s.Entries)) {
			s.Entries = s.Entries[:keep]
		}
	}
	for _, e := range p.Entries {
		if want := s.firstIndex() + Index(len(s.Entries)); e.Index != want {
			panic("core: Persist appends an Entry that doesn't continue the stored Log")
		}
		s.Entries = append(s.Entries, e)
	}
}

// firstIndex is the Index the first stored Entry has, or would have.
func (s *Stored) firstIndex() Index {
	if s.Snapshot != nil {
		return s.Snapshot.Index + 1
	}
	return 1
}

// Clone returns a copy that shares no memory with s, except Snapshot data and
// Entry payloads, which nothing modifies.
func (s *Stored) Clone() Stored {
	c := Stored{HardState: s.HardState, Entries: append([]Entry(nil), s.Entries...)}
	if s.Snapshot != nil {
		snap := *s.Snapshot
		c.Snapshot = &snap
	}
	return c
}

// Message is sent from one Member's core to another's. Body is specific to
// the core that produced it; the shell carries it without looking inside.
type Message struct {
	From NodeID
	To   NodeID
	Body any
}

// Event is one input to a core. The events are Tick, Receive, Propose, Read
// and Snapshotted.
type Event interface{ event() }

// Tick tells the core that one unit of time has passed. Timeouts are counted
// in Ticks; a core never reads a clock.
type Tick struct{}

// Receive delivers a Message from another Member.
type Receive struct{ Msg Message }

// Propose asks the core to add a command to the Log. Ref is the shell's
// handle for the waiting client. The core reports the proposal's fate exactly
// once, in a Result with the same Ref.
type Propose struct {
	Ref     uint64
	Payload []byte
}

// Read asks the core when the shell may answer a read from this Member's
// state machine, without putting the read in the Log (A§6.2). The core
// reports exactly once, in Output.Reads, with the same Ref.
type Read struct {
	Ref uint64
}

// Snapshotted hands the core a Snapshot the shell has taken of the state
// machine: its contents after applying the Entry at Index, which the core
// handed over as Committed earlier. The core may then drop its Log up to
// Index (A§6.4).
type Snapshotted struct {
	Index Index
	Data  []byte
}

func (Tick) event()        {}
func (Receive) event()     {}
func (Propose) event()     {}
func (Read) event()        {}
func (Snapshotted) event() {}

// Reason says why a proposal did not commit.
type Reason uint8

const (
	// OK: the proposal's Entry is Committed.
	OK Reason = iota
	// NotLeader: this Member isn't the Leader, so it did not take the
	// proposal. The Result's Leader field is a hint, or zero if unknown.
	NotLeader
	// NoMajority: this Member knows of no Leader backed by a Majority (it is
	// cut off, or an election is under way), so it did not take the proposal.
	NoMajority
	// Unknown: the Member accepted the proposal and then lost the ability to
	// say whether it committed (for example, it stopped being Leader).
	Unknown
)

// Result is the fate of one proposal. With Reason OK, Index is where its
// Entry sits in the Log, and the Entry itself is in the same Output's
// Committed, so the shell can pair the Result with what applying it returned.
type Result struct {
	Ref    uint64
	Reason Reason
	Index  Index
	Leader NodeID
}

// Output is everything one step asks the shell to do. A nil slice means
// nothing of that kind.
type Output struct {
	// Persist must be durable before the shell acts on any other field.
	Persist *Persist
	// Messages to send to other Members.
	Messages []Message
	// Committed Entries to apply to the state machine, in Log order. Each
	// Entry appears exactly once over the life of the core.
	Committed []Entry
	// Results for proposals whose fate is now known.
	Results []Result
	// Reads the core has ruled on. With Reason OK the shell may answer the
	// read now, from the state machine as it stands after applying this
	// Output's Committed Entries. Index is unused.
	Reads []Result
}

// Role is a Member's part in its Group right now.
type Role uint8

const (
	Follower Role = iota
	Candidate
	LeaderRole
)

// Status is a core's view of its Group, for the status API and for tests.
type Status struct {
	ID     NodeID
	Role   Role
	Term   Term
	Leader NodeID
	Commit Index
}

// Node is a consensus core. Step must be called from one goroutine at a
// time. Given the same Events in the same order, a Node returns the same
// Outputs.
type Node interface {
	Step(Event) Output
	Status() Status
}

// Rand is the seeded source of randomness a core is given at construction,
// for election timeout jitter. *math/rand/v2.Rand satisfies it.
type Rand interface {
	Uint64() uint64
}
