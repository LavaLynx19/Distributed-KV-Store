// Package core defines the contract between a consensus core and the shell
// that drives it (A§4.2). A core is a pure step function: the shell hands it
// one Event, and it returns an Output saying what to send, what is Committed
// and what became of each proposal. A core owns no threads, clocks, sockets
// or files, so the same core runs under the real shell and the Simulation.
//
// The contract grows with the Rungs. Read requests arrive in Rung 2, and
// durable writes and Snapshots in Rung 3 (PLAN.md).
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

// Message is sent from one Member's core to another's. Body is specific to
// the core that produced it; the shell carries it without looking inside.
type Message struct {
	From NodeID
	To   NodeID
	Body any
}

// Event is one input to a core. The events are Tick, Receive and Propose.
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

func (Tick) event()    {}
func (Receive) event() {}
func (Propose) event() {}

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
	// Messages to send to other Members.
	Messages []Message
	// Committed Entries to apply to the state machine, in Log order. Each
	// Entry appears exactly once over the life of the core.
	Committed []Entry
	// Results for proposals whose fate is now known.
	Results []Result
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
