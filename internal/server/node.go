// Package server is the real shell around a core (A§4.3): one goroutine
// drives the core and the state machine, fed by a ticker, the transport and
// the HTTP handlers. Nothing else touches them.
package server

import (
	"context"
	"log"
	"time"

	"distributed-kv-store/internal/core"
)

// Machine is the state machine Committed Entries are applied to. Read
// answers a query from its current state without an Entry. Capture returns a
// function that encodes the state as it was when Capture was called; that
// function may run on another goroutine while the Machine carries on.
type Machine interface {
	Apply(core.Entry) []byte
	Read(query []byte) []byte
	Capture() func() []byte
	Restore(data []byte) error
}

// Reply is the outcome of one proposal.
type Reply struct {
	Reason   core.Reason
	Response []byte
	Leader   core.NodeID
}

type proposal struct {
	payload []byte
	done    chan Reply
	// read marks a query that bypasses the Log (A§6.2): the core says when
	// it may be answered, and payload is then run against the state machine.
	read bool
	// members, if set, makes this a request for a Membership change to
	// that list (A§6.5).
	members []core.NodeID
}

// Storage makes a Member's durable state survive a restart. Write applies a
// change; Sync returns once everything written is on disk.
type Storage interface {
	Write(*core.Persist) error
	Sync() error
}

// Node runs one Member.
type Node struct {
	core    core.Node
	machine Machine
	send    func(core.Message)
	tick    time.Duration
	// Storage, if set before Run, receives every Persist. Without it the
	// Member keeps nothing across a restart.
	Storage Storage
	// SnapshotEvery, if set before Run, takes a Snapshot of the state machine
	// whenever this many Entries have been applied since the last one, so
	// the core can trim its Log (A§6.4). Zero means never.
	SnapshotEvery int
	// Restored is the Index of the Snapshot the state machine was restored
	// from before Run, or 0.
	Restored core.Index
	// TimeEntry, if set before Run, is asked on every tick while this
	// Member leads whether it has something to propose so that time moves
	// in a Group nobody is writing to (A§6.7). It returns the proposal, or
	// nil. It runs on the Node's goroutine.
	TimeEntry func(m Machine) []byte
	snapshots chan core.Snapshotted

	inbox     chan core.Message
	proposals chan proposal
	statusReq chan chan core.Status
	inspect   chan func(Machine)
}

// NewNode wires a core to its state machine and its way of sending Messages.
// tick is the length of one core tick.
func NewNode(c core.Node, m Machine, send func(core.Message), tick time.Duration) *Node {
	return &Node{
		core: c, machine: m, send: send, tick: tick,
		inbox:     make(chan core.Message, 4096),
		proposals: make(chan proposal, maxBatch),
		statusReq: make(chan chan core.Status),
		inspect:   make(chan func(Machine)),
		snapshots: make(chan core.Snapshotted, 1),
	}
}

// Deliver hands the Node a Message from another Member. If the Node is
// overwhelmed the Message is dropped, as the network might have done.
func (n *Node) Deliver(msg core.Message) {
	select {
	case n.inbox <- msg:
	default:
	}
}

// maxBatch is how many events one pass of the loop may take before it stores
// and acts on their Outputs.
const maxBatch = 256

// Run drives the core until ctx is cancelled. Proposals still waiting then
// are answered Unknown.
//
// Each pass takes every event that is ready, up to maxBatch, steps the core
// through them, makes all their Persists durable with one sync, and only
// then acts on the Outputs (A§4.2, the order rule). Under load many
// proposals share one disk sync.
func (n *Node) Run(ctx context.Context) {
	ticker := time.NewTicker(n.tick)
	defer ticker.Stop()
	pending := map[uint64]chan Reply{}
	queries := map[uint64][]byte{}
	var nextRef uint64
	var outs []core.Output
	applied, snapshotAt := n.Restored, n.Restored
	encoding := false // a Snapshot is being encoded on another goroutine
	// At most one time Entry of this Member's is undecided at a time.
	timePending, timeDone := false, make(chan Reply, 1)

	admit := func(p proposal) {
		nextRef++
		pending[nextRef] = p.done
		if p.members != nil {
			outs = append(outs, n.core.Step(core.Reconfigure{Ref: nextRef, Members: p.members}))
		} else if p.read {
			queries[nextRef] = p.payload
			outs = append(outs, n.core.Step(core.Read{Ref: nextRef}))
		} else {
			outs = append(outs, n.core.Step(core.Propose{Ref: nextRef, Payload: p.payload}))
		}
	}

	act := func(out core.Output) {
		if snap := out.Restore; snap != nil {
			if err := n.machine.Restore(snap.Data); err != nil {
				log.Fatalf("server: can't install the Snapshot at Entry %d: %v", snap.Index, err)
			}
			applied, snapshotAt = snap.Index, snap.Index
		}
		responses := make(map[core.Index][]byte, len(out.Committed))
		for _, e := range out.Committed {
			responses[e.Index] = n.machine.Apply(e)
			applied = e.Index
		}
		for _, r := range out.Results {
			if done, ok := pending[r.Ref]; ok {
				delete(pending, r.Ref)
				done <- Reply{Reason: r.Reason, Response: responses[r.Index], Leader: r.Leader}
			}
		}
		for _, r := range out.Reads {
			done, ok := pending[r.Ref]
			if !ok {
				continue
			}
			reply := Reply{Reason: r.Reason, Leader: r.Leader}
			if r.Reason == core.OK {
				reply.Response = n.machine.Read(queries[r.Ref])
			}
			delete(pending, r.Ref)
			delete(queries, r.Ref)
			done <- reply
		}
		for _, msg := range out.Messages {
			n.send(msg)
		}
	}

	// flush stores what the batch asked to store, then acts on it.
	flush := func() {
		if n.Storage != nil {
			wrote := false
			for i := range outs {
				if p := outs[i].Persist; p != nil {
					if err := n.Storage.Write(p); err != nil {
						log.Fatalf("server: can't store: %v", err) // continuing would break the order rule
					}
					wrote = true
				}
			}
			if wrote {
				if err := n.Storage.Sync(); err != nil {
					log.Fatalf("server: can't store: %v", err)
				}
			}
		}
		for _, out := range outs {
			act(out)
		}
		outs = outs[:0]

		// Capturing is instant: it keeps the tree's roots. Encoding is the
		// slow part, and happens off this goroutine while the core carries
		// on (A§6.4).
		if n.SnapshotEvery > 0 && !encoding && int(applied-snapshotAt) >= n.SnapshotEvery {
			encoding = true
			index, capture := applied, n.machine.Capture()
			go func() { n.snapshots <- core.Snapshotted{Index: index, Data: capture()} }()
		}
	}

	for {
		select {
		case <-ctx.Done():
			for _, done := range pending {
				done <- Reply{Reason: core.Unknown}
			}
			return
		case <-ticker.C:
			outs = append(outs, n.core.Step(core.Tick{}))
			select {
			case <-timeDone:
				timePending = false
			default:
			}
			if n.TimeEntry != nil && !timePending && n.core.Status().Role == core.LeaderRole {
				if payload := n.TimeEntry(n.machine); payload != nil {
					timePending = true
					admit(proposal{payload: payload, done: timeDone})
				}
			}
		case msg := <-n.inbox:
			outs = append(outs, n.core.Step(core.Receive{Msg: msg}))
		case p := <-n.proposals:
			admit(p)
		case reply := <-n.statusReq:
			reply <- n.core.Status()
		case fn := <-n.inspect:
			fn(n.machine)
		case snap := <-n.snapshots:
			encoding = false
			snapshotAt = snap.Index
			outs = append(outs, n.core.Step(snap))
		}
	drain:
		for len(outs) > 0 && len(outs) < maxBatch {
			select {
			case msg := <-n.inbox:
				outs = append(outs, n.core.Step(core.Receive{Msg: msg}))
			case p := <-n.proposals:
				admit(p)
			default:
				break drain
			}
		}
		flush()
	}
}

// Propose submits a command and waits for its outcome. If ctx ends first the
// outcome is Unknown: the command may still take effect.
func (n *Node) Propose(ctx context.Context, payload []byte) Reply {
	return n.submit(ctx, proposal{payload: payload})
}

// Read answers a query from this Member's state machine once the core says
// it is safe, without putting it in the Log (A§6.2). If ctx ends first the
// Reply is Unknown, which for a read just means "ask again".
func (n *Node) Read(ctx context.Context, query []byte) Reply {
	return n.submit(ctx, proposal{payload: query, read: true})
}

// Reconfigure asks for a Membership change to the given list and waits for
// its outcome (A§6.5). If ctx ends first the outcome is Unknown.
func (n *Node) Reconfigure(ctx context.Context, members []core.NodeID) Reply {
	return n.submit(ctx, proposal{members: members})
}

func (n *Node) submit(ctx context.Context, p proposal) Reply {
	done := make(chan Reply, 1) // buffered: the loop never blocks on a client that gave up
	p.done = done
	select {
	case n.proposals <- p:
	case <-ctx.Done():
		return Reply{Reason: core.Unknown}
	}
	select {
	case r := <-done:
		return r
	case <-ctx.Done():
		return Reply{Reason: core.Unknown}
	}
}

// Status asks the core for its view of the Group.
func (n *Node) Status(ctx context.Context) (core.Status, bool) {
	reply := make(chan core.Status, 1)
	select {
	case n.statusReq <- reply:
		return <-reply, true
	case <-ctx.Done():
		return core.Status{}, false
	}
}

// Inspect runs fn on the state machine between two steps, so fn sees a
// consistent state. It reports false if ctx ended first.
func (n *Node) Inspect(ctx context.Context, fn func(Machine)) bool {
	done := make(chan struct{})
	select {
	case n.inspect <- func(m Machine) { fn(m); close(done) }:
		<-done
		return true
	case <-ctx.Done():
		return false
	}
}
