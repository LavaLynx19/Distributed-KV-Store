// Package server is the real shell around a core (A§4.3): one goroutine
// drives the core and the state machine, fed by a ticker, the transport and
// the HTTP handlers. Nothing else touches them.
package server

import (
	"context"
	"time"

	"distributed-kv-store/internal/core"
)

// Machine is the state machine Committed Entries are applied to.
type Machine interface {
	Apply(core.Entry) []byte
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
}

// Node runs one Member.
type Node struct {
	core    core.Node
	machine Machine
	send    func(core.Message)
	tick    time.Duration

	inbox     chan core.Message
	proposals chan proposal
	statusReq chan chan core.Status
}

// NewNode wires a core to its state machine and its way of sending Messages.
// tick is the length of one core tick.
func NewNode(c core.Node, m Machine, send func(core.Message), tick time.Duration) *Node {
	return &Node{
		core: c, machine: m, send: send, tick: tick,
		inbox:     make(chan core.Message, 4096),
		proposals: make(chan proposal),
		statusReq: make(chan chan core.Status),
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

// Run drives the core until ctx is cancelled. Proposals still waiting then
// are answered Unknown.
func (n *Node) Run(ctx context.Context) {
	ticker := time.NewTicker(n.tick)
	defer ticker.Stop()
	pending := map[uint64]chan Reply{}
	var nextRef uint64

	step := func(ev core.Event) {
		out := n.core.Step(ev)
		responses := make(map[core.Index][]byte, len(out.Committed))
		for _, e := range out.Committed {
			responses[e.Index] = n.machine.Apply(e)
		}
		for _, r := range out.Results {
			if done, ok := pending[r.Ref]; ok {
				delete(pending, r.Ref)
				done <- Reply{Reason: r.Reason, Response: responses[r.Index], Leader: r.Leader}
			}
		}
		for _, msg := range out.Messages {
			n.send(msg)
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
			step(core.Tick{})
		case msg := <-n.inbox:
			step(core.Receive{Msg: msg})
		case p := <-n.proposals:
			nextRef++
			pending[nextRef] = p.done
			step(core.Propose{Ref: nextRef, Payload: p.payload})
		case reply := <-n.statusReq:
			reply <- n.core.Status()
		}
	}
}

// Propose submits a command and waits for its outcome. If ctx ends first the
// outcome is Unknown: the command may still take effect.
func (n *Node) Propose(ctx context.Context, payload []byte) Reply {
	done := make(chan Reply, 1) // buffered: the loop never blocks on a client that gave up
	select {
	case n.proposals <- proposal{payload: payload, done: done}:
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
