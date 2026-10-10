package cluster

import (
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
)

// Balancing describes how a store evens out load between its Groups
// (A§11.11).
type Balancing struct {
	// On makes every Node count the load on the Slots it leads and say so
	// by gossip.
	On bool
	// Latest makes a Node report each window's count as it stands, with no
	// smoothing: the store stage 7c starts from.
	Latest bool
}

const (
	// loadWindow is how often a Node folds what it has counted into its
	// smoothed load.
	loadWindow = 100
	// loadWeight is the share of the newest window in the smoothed load.
	loadWeight = 0.25
)

// count adds one answered request to the load of its Slot on Node nd, which
// hosts the Leader that answered it.
func (c *Cluster) count(nd *node, cmd fsm.Command) {
	if !c.cfg.Balancing.On {
		return
	}
	if nd.counts == nil {
		nd.counts, nd.smooth = make([]uint32, c.cfg.Slots), make([]float64, c.cfg.Slots)
	}
	cost := uint32(shard.WriteCost)
	key := cmd.Key
	switch cmd.Op {
	case fsm.OpGet:
		cost = shard.ReadCost
	case fsm.OpTxn:
		key = txnKeys(cmd)[0] // one Group, one Entry (A§11.8)
	}
	nd.counts[shard.SlotOf(key, c.cfg.Slots)] += cost
}

// measure closes a window if one is due, and puts in Node nd's report the
// load of each Slot whose Group it leads. A Node that restarts has counted
// nothing, like a Group's new Leader: its figures build up from zero.
func (c *Cluster) measure(nd *node) {
	if nd.counts == nil {
		nd.counts, nd.smooth = make([]uint32, c.cfg.Slots), make([]float64, c.cfg.Slots)
	}
	now := c.S.Now()
	if now-nd.loadAt < loadWindow {
		return
	}
	nd.loadAt = now
	load := make([]uint32, c.cfg.Slots)
	any := false
	for s, n := range nd.counts {
		nd.smooth[s] += loadWeight * (float64(n) - nd.smooth[s])
		nd.counts[s] = 0
		r := Replica(nd.id, nd.table.Slots[s].Group)
		if !c.hosts(nd.id, nd.table.Slots[s].Group) || !c.S.Up(r) || c.S.Status(r).Role != core.LeaderRole {
			continue
		}
		load[s] = uint32(nd.smooth[s] + 0.5)
		if c.cfg.Balancing.Latest {
			load[s] = n
		}
		any = any || load[s] > 0
	}
	nd.report.Load = nil
	if any {
		nd.report.Load = load
	}
}

// Load is the load of each Slot as Node n has heard it by gossip: the most
// any Node it thinks alive reports for the Slot. Only a Group's Leader
// reports anything for a Slot, so this is the Leader's figure.
func (c *Cluster) Load(n int) []uint32 {
	nd := c.nodes[n]
	load := make([]uint32, c.cfg.Slots)
	if nd.gossip == nil {
		return load
	}
	for _, m := range nd.gossip.Members() {
		report := nd.report
		if m.ID != nd.id {
			if !nd.gossip.Alive(m.ID) {
				continue
			}
			report, _ = shard.DecodeReport(m.Note)
		}
		for s, l := range report.Load {
			if s < len(load) && l > load[s] {
				load[s] = l
			}
		}
	}
	return load
}

// GroupLoad adds a per-Slot load up by the Group the table gives each Slot
// to. The Meta Group, at index 0, has none.
func GroupLoad(table shard.Table, load []uint32, groups int) []uint64 {
	sums := make([]uint64, groups+1)
	for s, o := range table.Slots {
		if int(o.Group) < len(sums) && s < len(load) {
			sums[o.Group] += uint64(load[s])
		}
	}
	return sums
}
