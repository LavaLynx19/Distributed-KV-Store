package cluster

import (
	"fmt"
	"slices"

	"distributed-kv-store/internal/check"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
	"distributed-kv-store/internal/sim"
)

// Leader is the replica that leads Group g with the highest Term among
// those running, or 0.
func (c *Cluster) Leader(g shard.GroupID) core.NodeID {
	var best core.NodeID
	var bestTerm core.Term
	for _, r := range c.Members(g) {
		if st := c.S.Status(r); c.S.Up(r) && st.Role == core.LeaderRole && (best == 0 || st.Term > bestTerm) {
			best, bestTerm = r, st.Term
		}
	}
	return best
}

// machine is a data replica's state machine.
func (c *Cluster) machine(r core.NodeID) *shardfsm.Machine { return c.S.Machine(r).(*shardfsm.Machine) }

// ownership is which Slots a Group serves as of the Entry at index.
type ownership struct {
	index  core.Index
	serves []bool
}

// applied is told of every Entry every replica applies. It keeps, for each
// data Group, what the Group serves as of the furthest Entry applied by any
// of its replicas. That is the Group's Committed state, whoever leads it and
// whichever replicas have since crashed.
//
// A Group serves a Slot from the Entry that makes it the owner to the Entry
// that freezes it. If, when one Group starts serving a Slot, another is
// still serving it by this measure, then both would have answered for it:
// the target could only have been given the Slot by something that didn't
// wait for the source to stop (A§11.12).
func (c *Cluster) applied(id core.NodeID, index core.Index, m sim.Machine) {
	g := GroupOf(id)
	if g == shard.Meta || index <= c.owned[g].index {
		return
	}
	now := ownership{index: index}
	for _, info := range m.(*shardfsm.Machine).Slots() {
		now.serves = append(now.serves, info.Serves())
	}
	before := c.owned[g]
	c.owned[g] = now
	if c.twoOwners != "" {
		return
	}
	for s, serves := range now.serves {
		if !serves || (before.serves != nil && before.serves[s]) {
			continue // only a Slot the Group has just started serving
		}
		for _, other := range c.Groups()[1:] {
			if o := c.owned[other]; other != g && o.serves != nil && o.serves[s] {
				c.twoOwners = fmt.Sprintf("Slot %d is served by Groups %d and %d at t=%d", s, min(g, other), max(g, other), c.S.Now())
			}
		}
	}
}

// TwoOwners describes the first moment two Groups served the same Slot, or
// is "" if that never happened.
func (c *Cluster) TwoOwners() string { return c.twoOwners }

// EndState compares what the store holds once it has settled (A§11.12). It
// returns one line per difference:
//   - Members of a Group that hold different data;
//   - a Slot that isn't served by exactly the Group the Meta Group's table
//     names.
//
// Replicas that are down are left out.
func (c *Cluster) EndState() []string {
	var diffs []string
	serving := map[shard.Slot][]shard.GroupID{}
	for _, g := range c.Groups()[1:] {
		items := map[core.NodeID][]fsm.Item{}
		var first *shardfsm.Machine
		for _, r := range c.Members(g) {
			if !c.S.Up(r) {
				continue
			}
			m := c.machine(r)
			items[r] = m.Items()
			if first == nil {
				first = m
			} else if !slices.Equal(m.Slots(), first.Slots()) {
				diffs = append(diffs, fmt.Sprintf("Group %d: replica %d has Slots %+v, another has %+v", g, r, m.Slots(), first.Slots()))
			}
		}
		for _, d := range check.Diverged(items) {
			diffs = append(diffs, fmt.Sprintf("Group %d: %s", g, d))
		}
		if first != nil {
			for s, info := range first.Slots() {
				if info.Serves() {
					serving[shard.Slot(s)] = append(serving[shard.Slot(s)], g)
				}
			}
		}
	}
	l := c.Leader(shard.Meta)
	if l == 0 {
		return append(diffs, "the Meta Group has no Leader")
	}
	table, err := shard.DecodeTable(c.S.Machine(l).Read(nil))
	if err != nil {
		return append(diffs, "the Meta Group's table can't be read")
	}
	for s, row := range table.Slots {
		if got := serving[shard.Slot(s)]; len(got) != 1 || got[0] != row.Group {
			diffs = append(diffs, fmt.Sprintf("Slot %d: the table says Group %d, and it is served by %v", s, row.Group, got))
		}
		if row.MovingTo != 0 {
			diffs = append(diffs, fmt.Sprintf("Slot %d: still moving to Group %d", s, row.MovingTo))
		}
	}
	return diffs
}

// MoveUnderWay finds a Slot that some Group's Leader is copying out or has
// frozen, and returns the Group it is leaving and the Group it is going to.
func (c *Cluster) MoveUnderWay() (source, target shard.GroupID, ok bool) {
	for _, g := range c.Groups()[1:] {
		l := c.Leader(g)
		if l == 0 {
			continue
		}
		for _, info := range c.machine(l).Slots() {
			if info.Status == shardfsm.Outgoing || info.Status == shardfsm.Frozen {
				return g, info.Peer, true
			}
		}
	}
	return 0, 0, false
}
