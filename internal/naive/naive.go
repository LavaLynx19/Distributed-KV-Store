// Package naive is Rung 1's primary-backup core. It is wrong on purpose
// (Decision Log: "Rung 1 ships a naive primary-backup store first"), so the
// harness can show what goes wrong without consensus:
//
//   - The primary answers a client as soon as it has applied a write itself,
//     before any backup has it.
//   - A Member decides who the primary is from who it has heard from lately.
//     Two Members that can't hear each other can both conclude it's them.
//   - A backup keeps whatever it applied first at an Index and ignores a
//     different Entry for that Index later.
//
// Don't fix these here. internal/raft replaces this package.
package naive

import (
	"slices"

	"distributed-kv-store/internal/core"
)

// Ping tells the other Members that the sender is alive.
type Ping struct{}

// Replicate carries one Entry from a primary to a backup.
type Replicate struct{ Entry core.Entry }

// DefaultTimeout is how many ticks of silence make a Member presume another
// one dead.
const DefaultTimeout = 5

// Node is one Member. The primary is the lowest-numbered Member that this
// Member believes is alive.
type Node struct {
	id      core.NodeID
	members []core.NodeID // ascending
	timeout int

	ticks     int
	lastHeard map[core.NodeID]int
	log       []core.Entry
	// early holds Entries that arrived ahead of a gap in the Log.
	early map[core.Index]core.Entry
}

// New builds a Member. Every Member is presumed alive at the start.
func New(id core.NodeID, members []core.NodeID, timeout int) *Node {
	sorted := slices.Clone(members)
	slices.Sort(sorted)
	return &Node{
		id:        id,
		members:   sorted,
		timeout:   timeout,
		lastHeard: map[core.NodeID]int{},
		early:     map[core.Index]core.Entry{},
	}
}

// primary is the lowest-numbered Member heard from within the timeout.
func (n *Node) primary() core.NodeID {
	for _, m := range n.members {
		if m == n.id || n.ticks-n.lastHeard[m] <= n.timeout {
			return m
		}
	}
	return n.id
}

func (n *Node) Status() core.Status {
	role := core.Follower
	if n.primary() == n.id {
		role = core.LeaderRole
	}
	return core.Status{ID: n.id, Role: role, Leader: n.primary(), Commit: core.Index(len(n.log))}
}

func (n *Node) Step(ev core.Event) core.Output {
	var out core.Output
	switch ev := ev.(type) {
	case core.Tick:
		n.ticks++
		n.broadcast(&out, Ping{})

	case core.Receive:
		n.lastHeard[ev.Msg.From] = n.ticks
		if r, ok := ev.Msg.Body.(Replicate); ok {
			n.accept(&out, r.Entry)
		}

	case core.Propose:
		if p := n.primary(); p != n.id {
			out.Results = append(out.Results, core.Result{Ref: ev.Ref, Reason: core.NotLeader, Leader: p})
			break
		}
		e := core.Entry{Index: core.Index(len(n.log) + 1), Kind: core.EntryCommand, Payload: ev.Payload}
		n.log = append(n.log, e)
		// The flaw: applied and acknowledged here, replicated afterwards.
		out.Committed = append(out.Committed, e)
		out.Results = append(out.Results, core.Result{Ref: ev.Ref, Reason: core.OK, Index: e.Index})
		n.broadcast(&out, Replicate{Entry: e})
	}
	return out
}

// accept applies a replicated Entry if it extends the Log, holds it if it
// arrived early, and ignores it if this Member already has that Index.
func (n *Node) accept(out *core.Output, e core.Entry) {
	next := core.Index(len(n.log) + 1)
	if e.Index < next {
		return
	}
	n.early[e.Index] = e
	for {
		e, ok := n.early[next]
		if !ok {
			return
		}
		delete(n.early, next)
		n.log = append(n.log, e)
		out.Committed = append(out.Committed, e)
		next++
	}
}

func (n *Node) broadcast(out *core.Output, body any) {
	for _, m := range n.members {
		if m != n.id {
			out.Messages = append(out.Messages, core.Message{From: n.id, To: m, Body: body})
		}
	}
}
