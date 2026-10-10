package cluster

import (
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
	"distributed-kv-store/internal/sim"
)

type simReply = sim.Reply

// agent is one Node's periodic work. It learns the table, and for each Group
// whose Leader is on this Node it moves any Move along one step.
func (c *Cluster) agent(nd *node) {
	c.S.After(agentEvery, func() { c.agent(nd) })
	if !c.NodeUp(nd.id) {
		return
	}
	c.refresh(nd)
	for _, g := range nd.groups {
		r := Replica(nd.id, g)
		st := c.S.Status(r)
		if !c.S.Up(r) || st.Role != core.LeaderRole {
			continue
		}
		if g == shard.Meta {
			// The Meta Leader's clock is the store's clock (A§11.7).
			if now := c.S.Now(); now-nd.lastTick >= tickEvery {
				nd.lastTick = now
				tick := meta.Command{Op: meta.OpTick, Stamp: c.S.Clock(r)}.Encode()
				c.S.Propose(r, tick, func(simReply) {})
			}
			continue
		}
		m := c.S.Machine(r).(*shardfsm.Machine)
		if m.Due(nd.table.StoreTime) {
			// Nobody is writing to this Group: say what time it is, so
			// that what is due expires (A§6.7).
			tick := fsm.Command{Op: fsm.OpTick, Stamp: nd.table.StoreTime}.Encode()
			c.S.Propose(r, tick, func(simReply) {})
		}
		for slot, info := range m.Slots() {
			c.moveAlong(nd, g, r, st.Term, m, shard.Slot(slot), info)
		}
	}
}

// moveAlong proposes the next step for one Slot of a Group this Node leads,
// if there is one. It decides from the Group's own state and the table, and
// every step it proposes is safe to propose again.
func (c *Cluster) moveAlong(nd *node, g shard.GroupID, r core.NodeID, term core.Term, m *shardfsm.Machine, slot shard.Slot, info shardfsm.SlotInfo) {
	key := moveKey{g, slot}
	if c.S.Now() < nd.waiting[key] {
		return
	}
	row := nd.table.Slots[slot]
	wait := func() { nd.waiting[key] = c.S.Now() + patience }
	clear := func() { delete(nd.waiting, key) }
	local := func(step shardfsm.Step) {
		wait()
		c.S.Propose(r, step.Encode(), func(simReply) { clear() })
	}
	// toGroup proposes a step to another Group. It calls then if the step
	// took, and refused if the Group answered that it couldn't take it.
	toGroup := func(to shard.GroupID, step shardfsm.Step, then, refused func()) {
		wait()
		payload := step.Encode()
		c.ask(nd.id, to, false, func(int64) []byte { return payload }, func(o Outcome, raw []byte) {
			clear()
			resp, err := fsm.DecodeResponse(raw)
			switch {
			case o != Answered || err != nil:
			case resp.Status == fsm.StatusOK:
				then()
			case refused != nil:
				refused()
			}
		})
	}

	if c.cfg.FlipAtOnce {
		// The naive store: the table is the authority. A Group that finds
		// it serves a Slot the table gives to another sends the Slot over
		// as it stands, and lets go once the other has it.
		if info.Serves() && row.Group != g {
			toGroup(row.Group, shardfsm.AcceptStep(slot, row.Epoch, g, m.Unfrozen(slot)), func() {
				c.S.Propose(r, shardfsm.FreezeStep(slot, info.Epoch).Encode(), func(simReply) {
					c.S.Propose(r, shardfsm.DropStep(slot, info.Epoch).Encode(), func(simReply) {})
				})
			}, nil)
		}
		return
	}

	switch info.Status {
	case shardfsm.Owned:
		if row.Group == g && row.Epoch == info.Epoch && row.MovingTo != 0 {
			local(shardfsm.BeginStep(slot, info.Epoch, row.MovingTo))
		}

	case shardfsm.Outgoing:
		cp := nd.copying[key]
		if !cp.covers(term, info.Epoch) {
			cp = copyState{term: term, epoch: info.Epoch} // start from the beginning
		}
		if cp.done {
			local(shardfsm.FreezeStep(slot, info.Epoch))
			return
		}
		chunk, done := m.Chunk(slot, cp.after, chunkKeys)
		if len(chunk) > 0 {
			cp.after = chunk[len(chunk)-1].Key
		}
		cp.done = done
		toGroup(info.Peer, shardfsm.IncomingStep(slot, info.Epoch+1, g, shardfsm.Transfer{Upserts: chunk}), func() {
			nd.copying[key] = cp
		}, nil)

	case shardfsm.Frozen:
		if row.Group == info.Peer && row.Epoch > info.Epoch {
			// The table says the other Group has it: let go.
			local(shardfsm.DropStep(slot, info.Epoch))
			return
		}
		// Send the rest. If this Leader made the copy it knows the target
		// has everything but what changed since. If not, it sends it all.
		cp := nd.copying[key]
		final, ok := m.Final(slot, !cp.covers(term, info.Epoch) || !cp.done)
		if !ok {
			return
		}
		toGroup(info.Peer, shardfsm.AcceptStep(slot, info.Epoch+1, g, final), func() {
			done := meta.Command{Op: meta.OpDone, Slot: slot, Epoch: info.Epoch + 1}.Encode()
			wait()
			c.ask(nd.id, shard.Meta, false, func(int64) []byte { return done }, func(Outcome, []byte) { clear() })
		}, func() {
			// The target doesn't hold the copy this agent thought it did.
			// Next time, send everything.
			delete(nd.copying, key)
		})
	}
}
