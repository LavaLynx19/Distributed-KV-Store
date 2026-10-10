package cluster

import (
	"distributed-kv-store/internal/automation"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/sim"
)

// Balancing describes how a store evens out load between its Groups
// (A§11.11).
type Balancing struct {
	// On makes every Node count the load on the Slots it leads and say so
	// by gossip.
	On bool
	// Move makes the Meta Group's Leader move Slots off the busiest Group.
	Move bool
	// Naive is the store stage 7c starts from. A Node reports each window's
	// count as it stands, with no smoothing. The Meta Leader moves the
	// busiest Slot of the busiest Group to the idlest whenever the busiest
	// carries more than High times the mean, and looks at nothing else.
	Naive bool
	// High and Low are the two lines (defaults 1.5 and 1.2): rebalancing
	// starts when the busiest Group carries more than High times the mean,
	// and goes on until it carries less than Low times.
	High, Low float64
	// Rest is how long, in Store time, a Slot that has moved stays where it
	// is (default 1000). Settle is how long after any Move the Meta Leader
	// waits for the load figures to catch up with it (default 500).
	Rest, Settle int64
	// Weight is the share of the newest window in a Node's smoothed load
	// (default 0.25).
	Weight float64
}

// rules fills in the defaults.
func (b Balancing) rules() automation.Balancing {
	r := automation.Balancing{Naive: b.Naive, High: b.High, Low: b.Low, Rest: b.Rest, Settle: b.Settle, Idle: idleLoad}
	if r.High == 0 {
		r.High = 1.5
	}
	if r.Low == 0 {
		r.Low = 1.2
	}
	if r.Rest == 0 {
		r.Rest = 1000
	}
	if r.Settle == 0 {
		r.Settle = 500
	}
	return r
}

const (
	// loadWindow is how often a Node folds what it has counted into its
	// smoothed load.
	loadWindow = 100
	// loadWeight is the share of the newest window in the smoothed load.
	loadWeight = 0.25
	// idleLoad is the load per Group, per window, below which the store is
	// taken to be idle: two writes.
	idleLoad = 2 * shard.WriteCost
)

// count adds one answered request to the load of its Slot on Node nd, which
// hosts the Leader that answered it.
func (c *Cluster) count(nd *node, cmd fsm.Command) {
	if !c.cfg.Balancing.On {
		return
	}
	if nd.counts == nil {
		nd.counts = make([]uint32, c.cfg.Slots)
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
		nd.counts = make([]uint32, c.cfg.Slots)
	}
	now := c.S.Now()
	if now-nd.loadAt < loadWindow {
		return
	}
	nd.loadAt = now
	weight := c.cfg.Balancing.Weight
	if weight == 0 {
		weight = loadWeight
	}
	nd.meter.Fold(nd.counts, weight)
	clear(nd.counts)
	nd.report.Load = nd.meter.Load(func(s int) bool {
		g := nd.table.Slots[s].Group
		r := Replica(nd.id, g)
		return c.hosts(nd.id, g) && c.S.Up(r) && c.S.Status(r).Role == core.LeaderRole
	}, c.cfg.Balancing.Naive)
}

// balance is the Meta Leader's part in evening out load (A§11.11): it asks
// for the Move automation.Balancer picks, if any. The request goes through
// the Meta Group's Log like any other.
func (c *Cluster) balance(nd *node, r core.NodeID, st core.Status) {
	if !c.cfg.Balancing.Move || nd.gossip == nil {
		return
	}
	table := c.S.Machine(r).(*meta.Machine).Table()
	healthy := true
	for _, row := range table.Groups {
		for _, n := range row.Members {
			healthy = healthy && nd.gossip.Alive(n)
		}
	}
	slot, to, ok := nd.balancer.Decide(c.cfg.Balancing.rules(), table, c.Load(nd.id), c.cfg.Groups, st.Term, c.S.Now(), healthy)
	if ok {
		cmd := meta.Command{Op: meta.OpMove, Slot: slot, To: to}.Encode()
		c.S.Propose(r, cmd, func(sim.Reply) {})
	}
}

// Load is the load of each Slot as Node n has heard it by gossip
// (automation.MergeLoad). A Node it thinks is gone has no say.
func (c *Cluster) Load(n int) []uint32 {
	nd := c.nodes[n]
	if nd.gossip == nil {
		return make([]uint32, c.cfg.Slots)
	}
	var reports []shard.Report
	for _, m := range nd.gossip.Members() {
		switch {
		case m.ID == nd.id:
			reports = append(reports, nd.report)
		case nd.gossip.Alive(m.ID):
			report, _ := shard.DecodeReport(m.Note)
			reports = append(reports, report)
		}
	}
	return automation.MergeLoad(c.cfg.Slots, reports)
}

// GroupLoad adds a per-Slot load up by Group (automation.GroupLoad).
func GroupLoad(table shard.Table, load []uint32, groups int) []uint64 {
	return automation.GroupLoad(table, load, groups)
}
