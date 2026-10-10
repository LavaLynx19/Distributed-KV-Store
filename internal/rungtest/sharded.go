package rungtest

import (
	"fmt"
	"slices"
	"time"

	"distributed-kv-store/internal/check"
	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/transport"
)

// ShardedStore is what a run with several Groups puts under test (A§11).
type ShardedStore struct {
	NewCore  cluster.NewCore
	Workload cluster.Workload
	// Nodes, Groups, Replicas and Slots shape the store (cluster.Config).
	Nodes, Groups, Replicas, Slots int

	DiskDelay     [2]int64
	SnapshotEvery int
	TearWrites    bool
	SessionTTL    int64
	// Unchecked and FlipAtOnce are the naive stores (cluster.Config).
	Unchecked  bool
	FlipAtOnce bool
	// Gossip, Spares and GossipLoss are passed on (cluster.Config).
	Gossip     cluster.GossipMode
	Spares     int
	GossipLoss float64
	// SuspectAfter and DeadAfter override the detector's defaults.
	SuspectAfter, DeadAfter int
	// GossipDecides is the naive store of stage 7b (cluster.Config).
	GossipDecides struct{ Ownership, Members bool }
	// Replacing is how dead Nodes are replaced (cluster.Config).
	Replacing cluster.Replacing
}

// ShardedScenario injects Faults, and asks for Moves, between times from
// and to. It must leave repair to RunSharded.
type ShardedScenario struct {
	Name   string
	Faults func(c *cluster.Cluster, from, to int64)
}

// RunSharded drives one Simulation of a store with several Groups: clients
// throughout, the scenario in the middle, then repair and a quiet period
// before the verdicts (A§11.12).
func RunSharded(store ShardedStore, sc ShardedScenario, seed uint64) Report {
	c := cluster.New(cluster.Config{
		Seed: seed, Nodes: store.Nodes, Groups: store.Groups, Replicas: store.Replicas, Slots: store.Slots,
		NewCore: store.NewCore, Copy: transport.NewLoopback().Copy,
		DiskDelay: store.DiskDelay, SnapshotEvery: store.SnapshotEvery, TearWrites: store.TearWrites,
		SessionTTL: store.SessionTTL, Unchecked: store.Unchecked, FlipAtOnce: store.FlipAtOnce,
		Gossip: store.Gossip, Spares: store.Spares, GossipLoss: store.GossipLoss,
		SuspectAfter: store.SuspectAfter, DeadAfter: store.DeadAfter,
		GossipDecides: store.GossipDecides, Replacing: store.Replacing,
	})
	h := &check.History{}
	rep := Report{Scenario: sc.Name, Seed: seed, Members: store.Nodes, History: h}

	faultsEnd := int64(warmup + faultSpan)
	store.Workload.Start(c, h, faultsEnd+cooldown/3)
	sc.Faults(c, warmup, faultsEnd)
	c.S.At(faultsEnd, func() {
		c.Heal()
		for _, n := range c.NodeIDs() {
			c.RestartNode(n)
		}
	})
	watchOwners(c, &rep, faultsEnd+cooldown)
	func() {
		defer func() {
			if p := recover(); p != nil {
				rep.Panic = fmt.Sprint(p)
			}
		}()
		c.S.Run(faultsEnd + cooldown)
	}()

	rep.verdict = h.Linearizable(20 * time.Second)
	rep.Linearizable, rep.TimedOut = rep.verdict.Linearizable, rep.verdict.TimedOut
	rep.TwoOwners = c.TwoOwners()
	rep.Diverged = c.EndState()
	rep.Signals = h.Signals
	rep.Recovery = h.Signals.RecoveryAfter(faultsEnd)
	rep.Routing = Routing{Forwarded: c.Forwarded, WrongGroup: c.WrongGroup, Moving: c.Moving}
	rep.Pauses = c.Pauses
	rep.MovesAsked, rep.MovesTaken = c.MovesAsked, c.MovesTaken
	rep.Diverged = append(rep.Diverged, c.GossipAgrees()...)
	rep.TableLags, rep.GossipSent = c.TableLags, c.GossipSent
	rep.Replacements, rep.Drops = len(c.Replacements), len(c.Drops)
	for _, r := range c.Replacements {
		if r.Running {
			rep.ReplacedRunning++
		}
	}
	for _, d := range c.Drops {
		if d.Counted && rep.WrongDrop == "" {
			rep.WrongDrop = fmt.Sprintf("replica %d dropped its data at t=%d while still a Member", d.Replica, d.At)
		}
	}
	for _, row := range c.Table(1).Slots {
		rep.Moves += int(row.Epoch)
	}
	return rep
}

// watchOwners samples the store and records the first moment one Group has
// two Leaders in a Term.
func watchOwners(c *cluster.Cluster, rep *Report, until int64) {
	var sample func()
	sample = func() {
		if c.S.Now() >= until {
			return
		}
		if rep.TwoLeaders == "" {
			for _, g := range c.Groups() {
				byTerm := map[core.Term][]core.NodeID{}
				for _, r := range c.Replicas(g) {
					if st := c.S.Status(r); c.S.Up(r) && st.Role == core.LeaderRole {
						byTerm[st.Term] = append(byTerm[st.Term], r)
					}
				}
				for term, leaders := range byTerm {
					if len(leaders) > 1 {
						slices.Sort(leaders)
						rep.TwoLeaders = fmt.Sprintf("replicas %v of Group %d in term %d at t=%d", leaders, g, term, c.S.Now())
					}
				}
			}
		}
		c.S.After(5, sample)
	}
	c.S.After(5, sample)
}

// moves asks for a Move every so often: a Slot chosen at random, to a Group
// that doesn't own it, through a Node chosen at random. It stops early
// enough for the last one to finish.
func moves(c *cluster.Cluster, from, to int64, every int64) {
	var step func()
	step = func() {
		if c.S.Now() >= to-600 {
			return
		}
		nodes := c.NodeIDs()
		n := nodes[c.S.Rand().IntN(len(nodes))]
		table := c.Table(n)
		slot := shard.Slot(c.S.Rand().IntN(len(table.Slots)))
		groups := c.Groups()[1:]
		target := groups[c.S.Rand().IntN(len(groups))]
		if target != table.Slots[slot].Group {
			c.Move(n, slot, target, func(bool) {})
		}
		c.S.After(every/2+c.S.Rand().Int64N(every), step)
	}
	c.S.At(from, step)
}

// Rung7 moves Slots between Groups while clients carry on (README).
var Rung7 = []ShardedScenario{
	{"no-moves", func(*cluster.Cluster, int64, int64) {}},

	// A Slot is moved every 200 units or so, and nothing else goes wrong.
	{"moves", func(c *cluster.Cluster, from, to int64) { moves(c, from, to, 200) }},

	// Moves, while Nodes crash and come back. A Node hosts replicas of
	// several Groups, so each crash costs several Groups a Member.
	{"moves-and-crashes", func(c *cluster.Cluster, from, to int64) {
		moves(c, from, to, 250)
		var step func()
		step = func() {
			if c.S.Now() >= to {
				return
			}
			nodes := c.NodeIDs()
			n := nodes[c.S.Rand().IntN(len(nodes))]
			c.CrashNode(n)
			c.S.After(100+c.S.Rand().Int64N(250), func() { c.RestartNode(n) })
			c.S.After(250+c.S.Rand().Int64N(300), step)
		}
		c.S.At(from+50, step)
	}},

	// Moves, while the network keeps splitting into two sides.
	{"moves-and-partitions", func(c *cluster.Cluster, from, to int64) {
		moves(c, from, to, 250)
		var step func()
		step = func() {
			if c.S.Now() >= to {
				return
			}
			nodes := c.NodeIDs()
			c.S.Rand().Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
			cut := 1 + c.S.Rand().IntN(2)
			c.PartitionNodes(nodes[:cut], nodes[cut:])
			c.S.After(150+c.S.Rand().Int64N(250), c.Heal)
			c.S.After(400+c.S.Rand().Int64N(300), step)
		}
		c.S.At(from+50, step)
	}},

	// Each Move is interrupted on purpose. Once a Slot is on its way, the
	// Node leading the Group it is leaving, the Group it is going to, or
	// the Meta Group is killed, at whatever step the Move has reached.
	{"moves-interrupted", func(c *cluster.Cluster, from, to int64) {
		moves(c, from, to, 400)
		var watch func()
		watch = func() {
			if c.S.Now() >= to {
				return
			}
			next := int64(3)
			if source, target, ok := c.MoveUnderWay(); ok {
				victim := []shard.GroupID{source, target, shard.Meta}[c.S.Rand().IntN(3)]
				if l := c.Leader(victim); l != 0 {
					n := cluster.NodeOf(l)
					c.CrashNode(n)
					c.S.After(150+c.S.Rand().Int64N(150), func() { c.RestartNode(n) })
					next = 200
				}
			}
			c.S.After(next+c.S.Rand().Int64N(8), watch)
		}
		c.S.At(from, watch)
	}},

	// Moves with crashes and Partitions together.
	{"moves-and-everything", func(c *cluster.Cluster, from, to int64) {
		moves(c, from, to, 250)
		var step func()
		step = func() {
			if c.S.Now() >= to {
				return
			}
			nodes := c.NodeIDs()
			c.S.Rand().Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
			switch c.S.Rand().IntN(5) {
			case 0, 1:
				c.CrashNode(nodes[0])
			case 2:
				c.RestartNode(nodes[0])
			case 3:
				cut := 1 + c.S.Rand().IntN(2)
				c.PartitionNodes(nodes[:cut], nodes[cut:])
			case 4:
				c.Heal()
				for _, n := range nodes {
					c.RestartNode(n)
				}
			}
			c.S.After(100+c.S.Rand().Int64N(250), step)
		}
		c.S.At(from+50, step)
	}},
}

// Rung7b adds what gossip has to get through (A§11.10): Partitions long
// enough for each side to give the other up for dead, and Moves asked for
// on both sides of one.
var Rung7b = []ShardedScenario{
	// The Nodes split two against three for longer than it takes to give a
	// Node up for dead, heal, and split again another way.
	{"long-partitions", func(c *cluster.Cluster, from, to int64) {
		var step func()
		step = func() {
			if c.S.Now() >= to-700 {
				return
			}
			nodes := c.NodeIDs()
			c.S.Rand().Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
			c.PartitionNodes(nodes[:2], nodes[2:])
			c.S.After(600, c.Heal)
			c.S.After(900+c.S.Rand().Int64N(200), step)
		}
		c.S.At(from, step)
	}},

	// The same Partitions, and on each side a Node is asked to move the
	// same Slot, to different Groups.
	{"rival-moves", func(c *cluster.Cluster, from, to int64) {
		var step func()
		step = func() {
			if c.S.Now() >= to-700 {
				return
			}
			nodes := c.NodeIDs()
			c.S.Rand().Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
			c.PartitionNodes(nodes[:2], nodes[2:])
			c.S.After(50, func() {
				table := c.Table(nodes[0])
				slot := shard.Slot(c.S.Rand().IntN(len(table.Slots)))
				var others []shard.GroupID
				for _, g := range c.Groups()[1:] {
					if g != table.Slots[slot].Group {
						others = append(others, g)
					}
				}
				c.Move(nodes[0], slot, others[0], func(bool) {})
				c.Move(nodes[2], slot, others[1], func(bool) {})
			})
			c.S.After(600, c.Heal)
			c.S.After(900+c.S.Rand().Int64N(200), step)
		}
		c.S.At(from, step)
	}},
}

// lose destroys one of the founding Nodes, chosen at random, at about when.
func lose(c *cluster.Cluster, when int64) {
	c.S.At(when+c.S.Rand().Int64N(300), func() {
		for range 20 {
			if n := 1 + c.S.Rand().IntN(5); c.NodeUp(n) {
				c.DestroyNode(n)
				return
			}
		}
	})
}

// Rung7c adds what automation has to get through (A§11.11): Nodes lost for
// good, Nodes that are away long enough to be replaced and then come back,
// and Nodes that are only restarting.
var Rung7c = []ShardedScenario{
	// One Node is lost for good while Slots move.
	{"lost-node", func(c *cluster.Cluster, from, to int64) {
		moves(c, from, to, 400)
		lose(c, from+200)
	}},

	// One Node is lost for good, and the others keep crashing and coming
	// back within a few hundred units.
	{"lost-node-and-crashes", func(c *cluster.Cluster, from, to int64) {
		lose(c, from+200)
		var step func()
		step = func() {
			if c.S.Now() >= to {
				return
			}
			nodes := c.NodeIDs()
			n := nodes[c.S.Rand().IntN(len(nodes))]
			c.CrashNode(n)
			c.S.After(100+c.S.Rand().Int64N(250), func() { c.RestartNode(n) })
			c.S.After(250+c.S.Rand().Int64N(300), step)
		}
		c.S.At(from+50, step)
	}},

	// A Node is away for long enough to be given up on, and then comes
	// back to find its place taken. Then another.
	{"long-outages", func(c *cluster.Cluster, from, to int64) {
		var step func()
		step = func() {
			if c.S.Now() >= to-900 {
				return
			}
			nodes := c.NodeIDs()
			n := nodes[c.S.Rand().IntN(len(nodes))]
			c.CrashNode(n)
			c.S.After(600+c.S.Rand().Int64N(200), func() { c.RestartNode(n) })
			c.S.After(1000+c.S.Rand().Int64N(200), step)
		}
		c.S.At(from, step)
	}},

	// One Node is lost for good and replaced. After that the Nodes are
	// restarted in overlapping turns, as in an upgrade done too fast: one
	// goes down, a second is restarted while the first is away, and a third
	// goes down as the first comes back.
	{"lost-node-then-rolling-restarts", func(c *cluster.Cluster, from, to int64) {
		lose(c, from+100)
		var step func()
		step = func() {
			if c.S.Now() >= to-400 {
				return
			}
			nodes := c.NodeIDs()
			c.S.Rand().Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
			first, second, third := nodes[0], nodes[1], nodes[2]
			c.CrashNode(first)
			c.S.After(60+c.S.Rand().Int64N(60), func() {
				c.CrashNode(second)
				c.S.After(10, func() { c.RestartNode(second) })
			})
			c.S.After(100+c.S.Rand().Int64N(40), func() {
				c.RestartNode(first)
				c.CrashNode(third)
			})
			c.S.After(300, func() { c.RestartNode(third) })
			c.S.After(350+c.S.Rand().Int64N(100), step)
		}
		c.S.At(from+1000, step)
	}},
}
