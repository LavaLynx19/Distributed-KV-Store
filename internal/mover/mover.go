// Package mover carries a Move from step to step (A§11.4). On each Node, for
// every data Group whose Leader is there, an Agent looks at that Group's own
// state and at the Slot table, and proposes whatever comes next: to its own
// Group, to the Group the Slot is going to, or to the Meta Group.
//
// An Agent holds nothing that matters. What it remembers (how far a copy
// has got, what it is waiting on) only saves work. If it is lost, or a new
// Leader's Agent takes over, the Logs say where the Move stands, and every
// step is safe to propose again.
//
// The same Agent runs under the real shell and the Simulation. It reaches
// the world only through Env.
package mover

import (
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
)

// Env is what an Agent needs from the Node it runs on.
type Env interface {
	// Now is the time, in whatever units Patience is in.
	Now() int64
	// WithMachine runs fn on the state machine of this Node's replica of
	// Group g, and returns when fn has run. fn must not keep the Machine.
	WithMachine(g shard.GroupID, fn func(*shardfsm.Machine))
	// Local proposes payload to this Node's replica of Group g. done is
	// called once the proposal's fate is known, whatever it is.
	Local(g shard.GroupID, payload []byte, done func())
	// Ask proposes payload to Group g, wherever its Leader is. back is
	// called with the state machine's answer, or with answered false if
	// there was none. It may never be called at all.
	Ask(g shard.GroupID, payload []byte, back func(answered bool, raw []byte))
}

// Agent is one Node's mover.
type Agent struct {
	// Patience is how long the Agent waits for an answer before it acts on
	// the same Slot again.
	Patience int64
	// ChunkKeys is how many keys go in one step of a copy.
	ChunkKeys int
	// FlipAtOnce is the naive store: the table is the authority, and a
	// Group hands a Slot over as it stands without freezing it first. It
	// exists so that Rung 7's exposure of that stays reproducible.
	FlipAtOnce bool
	// OnHandover, if set, is told how long a Slot was frozen: from when
	// this Agent first saw its Group had frozen it to when the other Group
	// answered that it had taken over. That is the pause clients of the
	// Slot see (A§11.4), to within how often the Agent looks.
	OnHandover func(g shard.GroupID, slot shard.Slot, frozenFor int64)

	copying map[moveKey]copyState
	waiting map[moveKey]int64
	frozen  map[moveKey]int64
}

type moveKey struct {
	group shard.GroupID
	slot  shard.Slot
}

// copyState is how far a Leader's Agent has got with copying a Slot out. It
// is only good for the Move it was made for, by the Leader that made it: a
// new Leader starts again, and so does the next Move of the same Slot.
type copyState struct {
	term  core.Term
	epoch uint32
	after string
	done  bool
}

func (cp copyState) covers(term core.Term, epoch uint32) bool {
	return cp.term == term && cp.epoch == epoch
}

// Reset forgets everything, as a Node that restarts does.
func (a *Agent) Reset() {
	// New maps, not nil: an answer to something asked before the restart
	// may still arrive and be recorded.
	a.copying, a.waiting, a.frozen = map[moveKey]copyState{}, map[moveKey]int64{}, map[moveKey]int64{}
}

// Step moves each Move of Group g along by at most one step. It must be
// called only on the Node where g's Leader is, with that Leader's Term and
// the latest table the Node has heard.
func (a *Agent) Step(env Env, g shard.GroupID, term core.Term, table shard.Table) {
	if a.copying == nil {
		a.Reset()
	}
	var slots []shardfsm.SlotInfo
	env.WithMachine(g, func(m *shardfsm.Machine) { slots = m.Slots() })
	for s, info := range slots {
		if s < len(table.Slots) {
			a.slot(env, g, term, shard.Slot(s), info, table.Slots[s])
		}
	}
}

func (a *Agent) slot(env Env, g shard.GroupID, term core.Term, slot shard.Slot, info shardfsm.SlotInfo, row shard.Owner) {
	key := moveKey{g, slot}
	if env.Now() < a.waiting[key] {
		return
	}
	wait := func() { a.waiting[key] = env.Now() + a.Patience }
	clear := func() { delete(a.waiting, key) }
	local := func(step shardfsm.Step) {
		wait()
		env.Local(g, step.Encode(), clear)
	}
	// toGroup proposes a step to another Group. It calls then if the step
	// took, and refused if the Group answered that it couldn't take it.
	toGroup := func(to shard.GroupID, step shardfsm.Step, then, refused func()) {
		wait()
		env.Ask(to, step.Encode(), func(answered bool, raw []byte) {
			clear()
			resp, err := fsm.DecodeResponse(raw)
			switch {
			case !answered || err != nil:
			case resp.Status == fsm.StatusOK:
				then()
			case refused != nil:
				refused()
			}
		})
	}

	if a.FlipAtOnce {
		// The naive store. A Group that finds it serves a Slot the table
		// gives to another sends the Slot over as it stands, and lets go
		// once the other has it.
		if info.Serves() && row.Group != g {
			var whole shardfsm.Transfer
			env.WithMachine(g, func(m *shardfsm.Machine) { whole = m.Unfrozen(slot) })
			toGroup(row.Group, shardfsm.AcceptStep(slot, row.Epoch, g, whole), func() {
				env.Local(g, shardfsm.FreezeStep(slot, info.Epoch).Encode(), func() {
					env.Local(g, shardfsm.DropStep(slot, info.Epoch).Encode(), func() {})
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
		cp := a.copying[key]
		if !cp.covers(term, info.Epoch) {
			cp = copyState{term: term, epoch: info.Epoch} // start from the beginning
		}
		if cp.done {
			// Once the freeze is Committed the Slot is refusing its
			// clients, so don't wait for the next look: send the rest now.
			wait()
			env.Local(g, shardfsm.FreezeStep(slot, info.Epoch).Encode(), func() {
				clear()
				var now []shardfsm.SlotInfo
				env.WithMachine(g, func(m *shardfsm.Machine) { now = m.Slots() })
				if int(slot) < len(now) && now[slot].Status == shardfsm.Frozen {
					a.slot(env, g, term, slot, now[slot], row)
				}
			})
			return
		}
		var chunk []fsm.Raw
		env.WithMachine(g, func(m *shardfsm.Machine) { chunk, cp.done = m.Chunk(slot, cp.after, a.ChunkKeys) })
		if len(chunk) > 0 {
			cp.after = chunk[len(chunk)-1].Key
		}
		toGroup(info.Peer, shardfsm.IncomingStep(slot, info.Epoch+1, g, shardfsm.Transfer{Upserts: chunk}), func() {
			a.copying[key] = cp
		}, nil)

	case shardfsm.Frozen:
		if row.Group == info.Peer && row.Epoch > info.Epoch {
			// The table says the other Group has it: let go.
			local(shardfsm.DropStep(slot, info.Epoch))
			return
		}
		if _, seen := a.frozen[key]; !seen {
			a.frozen[key] = env.Now()
		}
		// Send the rest. If this Leader made the copy it knows the target
		// has everything but what changed since. If not, it sends it all.
		cp := a.copying[key]
		var final shardfsm.Transfer
		var frozen bool
		env.WithMachine(g, func(m *shardfsm.Machine) {
			final, frozen = m.Final(slot, !cp.covers(term, info.Epoch) || !cp.done)
		})
		if !frozen {
			return
		}
		toGroup(info.Peer, shardfsm.AcceptStep(slot, info.Epoch+1, g, final), func() {
			if since, seen := a.frozen[key]; seen && a.OnHandover != nil {
				a.OnHandover(g, slot, env.Now()-since)
			}
			delete(a.frozen, key)
			wait()
			done := meta.Command{Op: meta.OpDone, Slot: slot, Epoch: info.Epoch + 1}.Encode()
			env.Ask(shard.Meta, done, func(bool, []byte) { clear() })
		}, func() {
			// The target doesn't hold the copy this Agent thought it did.
			// Next time, send everything.
			delete(a.copying, key)
		})
	}
}
