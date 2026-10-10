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
	})
	h := &check.History{}
	rep := Report{Scenario: sc.Name, Seed: seed, Members: store.Nodes, History: h}

	faultsEnd := int64(warmup + faultSpan)
	store.Workload.Start(c, h, faultsEnd+cooldown/3)
	sc.Faults(c, warmup, faultsEnd)
	c.S.At(faultsEnd, func() {
		c.S.Heal()
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
				for _, r := range c.Members(g) {
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
			c.S.After(150+c.S.Rand().Int64N(250), c.S.Heal)
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
				c.S.Heal()
				for _, n := range nodes {
					c.RestartNode(n)
				}
			}
			c.S.After(100+c.S.Rand().Int64N(250), step)
		}
		c.S.At(from+50, step)
	}},
}
