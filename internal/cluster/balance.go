package cluster

import (
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

func (b Balancing) lines() (high, low float64) {
	high, low = b.High, b.Low
	if high == 0 {
		high = 1.5
	}
	if low == 0 {
		low = 1.2
	}
	return high, low
}

func (b Balancing) waits() (rest, settle int64) {
	rest, settle = b.Rest, b.Settle
	if rest == 0 {
		rest = 1000
	}
	if settle == 0 {
		settle = 500
	}
	return rest, settle
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
	weight := c.cfg.Balancing.Weight
	if weight == 0 {
		weight = loadWeight
	}
	load := make([]uint32, c.cfg.Slots)
	any := false
	for s, n := range nd.counts {
		nd.smooth[s] += weight * (float64(n) - nd.smooth[s])
		nd.counts[s] = 0
		r := Replica(nd.id, nd.table.Slots[s].Group)
		if !c.hosts(nd.id, nd.table.Slots[s].Group) || !c.S.Up(r) || c.S.Status(r).Role != core.LeaderRole {
			continue
		}
		load[s] = uint32(nd.smooth[s] + 0.5)
		if c.cfg.Balancing.Naive {
			load[s] = n
		}
		any = any || load[s] > 0
	}
	nd.report.Load = nil
	if any {
		nd.report.Load = load
	}
}

// balance is the Meta Leader's part in evening out load (A§11.11). It
// works out each Group's load from what gossip has brought, and may ask for
// one Move. The request goes through the Meta Group's Log like any other, so
// a wrong picture costs a Move that wasn't needed and nothing else.
//
// What damps it:
//   - one Move at a time across the store, and none for a while after one
//     finishes, until the figures have caught up with it;
//   - a Slot that has moved rests;
//   - the figures are smoothed, and a new Leader waits until it has heard
//     enough of them;
//   - nothing while a Node of some Group is suspected, or being replaced;
//   - two lines, so that a Group near the line doesn't start and stop;
//   - a Slot is moved only if it is no more than half the gap between the
//     two Groups, so that they can't swap places.
//
// The first two are read from the table, so a new Leader forgets neither.
func (c *Cluster) balance(nd *node, r core.NodeID, st core.Status) {
	b := c.cfg.Balancing
	if !b.Move || nd.gossip == nil {
		return
	}
	table := c.S.Machine(r).(*meta.Machine).Table()
	load := c.Load(nd.id)
	sums := GroupLoad(table, load, c.cfg.Groups)
	var total uint64
	busiest, idlest := 1, 1
	for g := 1; g <= c.cfg.Groups; g++ {
		total += sums[g]
		if sums[g] > sums[busiest] {
			busiest = g
		}
		if sums[g] < sums[idlest] {
			idlest = g
		}
	}
	mean := float64(total) / float64(c.cfg.Groups)
	high, low := b.lines()
	if total < uint64(c.cfg.Groups)*idleLoad || busiest == idlest {
		return // nothing worth moving a Slot for
	}
	move := func(slot int) {
		cmd := meta.Command{Op: meta.OpMove, Slot: shard.Slot(slot), To: shard.GroupID(idlest)}.Encode()
		c.S.Propose(r, cmd, func(sim.Reply) {})
	}
	if b.Naive {
		if float64(sums[busiest]) <= high*mean {
			return
		}
		slot := -1
		for s, o := range table.Slots {
			if int(o.Group) == busiest && o.MovingTo == 0 && (slot < 0 || load[s] > load[slot]) {
				slot = s
			}
		}
		if slot >= 0 {
			move(slot)
		}
		return
	}

	rest, settle := b.waits()
	if nd.balanceTerm != st.Term {
		// A new Leader has heard nothing it can trust yet.
		nd.balanceTerm, nd.balanceSince, nd.balancing = st.Term, c.S.Now(), false
	}
	if c.S.Now()-nd.balanceSince < settle {
		return
	}
	for _, o := range table.Slots {
		if o.MovingTo != 0 || o.MovedAt != 0 && table.StoreTime-o.MovedAt < settle {
			return
		}
	}
	for _, row := range table.Groups {
		if row.Add != 0 {
			return
		}
		for _, n := range row.Members {
			if !nd.gossip.Alive(n) {
				return
			}
		}
	}
	switch share := float64(sums[busiest]) / mean; {
	case share > high:
		nd.balancing = true
	case share < low:
		nd.balancing = false
	}
	if !nd.balancing {
		return
	}
	// The Slot to move is the biggest that is no more than half the gap
	// between the two Groups: it leaves the busier one still the busier, so
	// no Move can undo another.
	gap := sums[busiest] - sums[idlest]
	slot := -1
	for s, o := range table.Slots {
		l := uint64(load[s])
		if int(o.Group) != busiest || l == 0 || 2*l > gap || o.MovedAt != 0 && table.StoreTime-o.MovedAt < rest {
			continue
		}
		if slot < 0 || load[s] > load[slot] {
			slot = s
		}
	}
	if slot >= 0 {
		move(slot)
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
