package cluster

import (
	"fmt"
	"slices"

	"distributed-kv-store/internal/check"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/gossip"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
	"distributed-kv-store/internal/sim"
)

// Leader is the replica that leads Group g with the highest Term among
// those running, or 0.
func (c *Cluster) Leader(g shard.GroupID) core.NodeID {
	var best core.NodeID
	var bestTerm core.Term
	for _, r := range c.Replicas(g) {
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
	if g == shard.Meta {
		// Note when each table version first existed anywhere.
		if v := m.(*meta.Machine).Table().Version; v != 0 {
			if _, seen := c.versionAt[v]; !seen {
				c.versionAt[v] = c.S.Now()
			}
		}
		return
	}
	if index <= c.owned[g].index {
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
		members := c.Members(g)
		if members == nil {
			members = c.Replicas(g)
		}
		for _, r := range members {
			if !c.S.Has(r) || !c.S.Up(r) {
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
	if c.cfg.Replacing.On {
		diffs = append(diffs, c.membersSettled(table)...)
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

// GossipAgrees checks what gossip should have settled on once Faults have
// stopped (A§11.10): every running Node thinks every running Node is alive,
// knows its address, and holds the Meta Group's table version. It returns
// one line per disagreement, or nil if the store doesn't gossip.
func (c *Cluster) GossipAgrees() []string {
	if c.cfg.Gossip == NoGossip {
		return nil
	}
	var diffs []string
	var version uint64
	if l := c.Leader(shard.Meta); l != 0 {
		version = c.S.Machine(l).(*meta.Machine).Table().Version
	}
	for _, n := range c.NodeIDs() {
		nd := c.nodes[n]
		if !nd.up {
			continue
		}
		seen := map[int]gossip.Member{}
		for _, m := range nd.gossip.Members() {
			seen[m.ID] = m
		}
		for _, other := range c.NodeIDs() {
			if !c.nodes[other].up {
				continue
			}
			if m, known := seen[other]; !known || m.Status != gossip.Alive || m.Peer != address(other) {
				diffs = append(diffs, fmt.Sprintf("node %d thinks node %d is %+v", n, other, m))
			}
		}
		if got := nd.gossip.Table().Version; got != version {
			diffs = append(diffs, fmt.Sprintf("node %d's gossip has table version %d, the Meta Group has %d", n, got, version))
		}
	}
	return diffs
}

// membersSettled checks what replacing dead Nodes should have settled on
// (A§11.11): no change still wanted, the table's Member lists the ones the
// Groups have, no Node lost for good still a Member, and no running Node
// holding a replica of a Group it isn't a Member of.
func (c *Cluster) membersSettled(table shard.Table) []string {
	var diffs []string
	for g, row := range table.Groups {
		g := shard.GroupID(g)
		if row.Add != 0 || row.Remove != 0 {
			diffs = append(diffs, fmt.Sprintf("Group %d: still replacing node %d with node %d", g, row.Remove, row.Add))
		}
		var members []int
		for _, r := range c.Members(g) {
			members = append(members, NodeOf(r))
		}
		if !slices.Equal(members, row.Members) {
			diffs = append(diffs, fmt.Sprintf("Group %d: has Members %v, and the table says %v", g, members, row.Members))
		}
		for _, n := range members {
			if c.nodes[n].gone {
				diffs = append(diffs, fmt.Sprintf("Group %d: node %d, lost for good, is still a Member", g, n))
			}
		}
		for _, r := range c.Replicas(g) {
			if n := NodeOf(r); c.nodes[n].up && !slices.Contains(members, n) {
				diffs = append(diffs, fmt.Sprintf("Group %d: node %d isn't a Member and still holds a replica", g, n))
			}
		}
	}
	return diffs
}
