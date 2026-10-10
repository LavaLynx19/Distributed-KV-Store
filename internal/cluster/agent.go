package cluster

import (
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
	"distributed-kv-store/internal/sim"
)

// agent is one Node's periodic work. It learns the table, keeps time moving,
// and for each data Group whose Leader is on this Node lets the mover take
// any Move one step on.
func (c *Cluster) agent(nd *node) {
	c.S.After(agentEvery, func() { c.agent(nd) })
	if !c.NodeUp(nd.id) {
		return
	}
	if nd.gossip == nil {
		c.refresh(nd) // with gossip the table arrives by itself
	}
	c.replicasWanted(nd)
	for _, g := range slices.Clone(nd.groups) {
		r := Replica(nd.id, g)
		st := c.S.Status(r)
		if !c.S.Up(r) || st.Role != core.LeaderRole {
			continue
		}
		c.changeMembers(nd, g, st)
		if g == shard.Meta {
			c.replaceDead(nd, r)
			// The Meta Leader's clock is the store's clock (A§11.7).
			if now := c.S.Now(); now-nd.lastTick >= tickEvery {
				nd.lastTick = now
				tick := meta.Command{Op: meta.OpTick, Stamp: c.S.Clock(r)}.Encode()
				c.S.Propose(r, tick, func(sim.Reply) {})
			}
			continue
		}
		if c.machine(r).Due(nd.table.StoreTime) {
			// Nobody is writing to this Group: say what time it is, so
			// that what is due expires (A§6.7).
			tick := fsm.Command{Op: fsm.OpTick, Stamp: nd.table.StoreTime}.Encode()
			c.S.Propose(r, tick, func(sim.Reply) {})
		}
		nd.mover.Step(env{c, nd}, g, st.Term, nd.table)
	}
}

// changeMembers is the part of a Group's Leader in a Membership change the
// Meta Group wants (A§11.11). It adds the Node to add, then removes the Node
// to remove, one change at a time (A§6.5), and tells the Meta Group whenever
// the Group's Committed Member list isn't the one in the table. It acts on a
// wish only when the table is up to date with the Group's list, so that a
// table from before some later change can't make it repeat an old one.
func (c *Cluster) changeMembers(nd *node, g shard.GroupID, st core.Status) {
	if int(g) < len(nd.table.Groups) && st.Learner != 0 && st.Learner != Replica(nd.table.Groups[g].Add, g) && !c.cfg.GossipDecides.Members {
		// The Node being brought up to date is no longer the one wanted:
		// the Meta Group has since given it up for dead too. Call it off.
		c.S.Reconfigure(Replica(nd.id, g), st.Members, func(sim.Reply) {})
		return
	}
	if int(g) >= len(nd.table.Groups) || st.Changing || c.cfg.GossipDecides.Members {
		return // in stage 7b's naive store no Log says who a Group's Members are
	}
	row := nd.table.Groups[g]
	var now []int
	for _, m := range st.Members {
		now = append(now, NodeOf(m))
	}
	if !slices.Equal(now, row.Members) || uint64(st.MembersAt) != row.At {
		if uint64(st.MembersAt) > row.At {
			report := meta.Command{Op: meta.OpMembers, To: g, Members: now, At: uint64(st.MembersAt)}.Encode()
			c.ask(nd.id, shard.Meta, false, func(int64) []byte { return report }, func(Outcome, []byte) {})
		}
		return
	}
	r := Replica(nd.id, g)
	switch {
	case row.Add != 0 && !slices.Contains(now, row.Add):
		next := append(slices.Clone(st.Members), Replica(row.Add, g))
		slices.Sort(next)
		c.S.Reconfigure(r, next, func(sim.Reply) {})
	case row.Add != 0 && slices.Contains(now, row.Remove):
		next := slices.DeleteFunc(slices.Clone(st.Members), func(m core.NodeID) bool { return m == Replica(row.Remove, g) })
		c.S.Reconfigure(r, next, func(sim.Reply) {})
	}
}

// env is what a Node's mover sees of the simulated store.
type env struct {
	c  *Cluster
	nd *node
}

func (e env) Now() int64 { return e.c.S.Now() }

func (e env) WithMachine(g shard.GroupID, fn func(*shardfsm.Machine)) {
	fn(e.c.machine(Replica(e.nd.id, g)))
}

func (e env) Local(g shard.GroupID, payload []byte, done func()) {
	e.c.S.Propose(Replica(e.nd.id, g), payload, func(sim.Reply) { done() })
}

func (e env) Ask(g shard.GroupID, payload []byte, back func(bool, []byte)) {
	e.c.ask(e.nd.id, g, false, func(int64) []byte { return payload }, func(o Outcome, raw []byte) {
		back(o == Answered, raw)
	})
}
