// Package shardfsm is a data Group's state machine in a store with several
// Groups (A§11). It wraps the key-value machine of Rungs 1–6 and adds the
// one thing a Group must know for itself: which Slots it owns. Every request
// for a key is checked against that, from this Group's own Log and nothing
// else, so no table held by a client, a forwarding Node or the Meta Group
// can make two Groups serve the same key.
//
// A Slot passes through these states in the Group it leaves:
//
//	Owned → Outgoing → Frozen → Absent
//
// and in the Group it goes to:
//
//	Absent → Incoming → Owned
//
// The source serves the Slot while Owned or Outgoing. The target serves it
// once Owned. The target becomes Owned only by an accept built from the
// source's Frozen Slot, so the two never serve at once (A§11.4).
//
// Inside a Group, keys are stored under their Slot, so a Slot's keys are one
// contiguous range that can be read out and sent.
package shardfsm

import (
	"encoding/binary"
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/tree"
)

// Status is where a Slot stands in this Group.
type Status uint8

const (
	// Absent: this Group has nothing to do with the Slot.
	Absent Status = iota
	// Owned: this Group serves the Slot.
	Owned
	// Outgoing: this Group serves the Slot and is copying it to another.
	// Keys written meanwhile are remembered, to be sent again at the end.
	Outgoing
	// Frozen: this Group has stopped serving the Slot and holds its final
	// contents until the other Group has them.
	Frozen
	// Incoming: this Group is receiving the Slot and doesn't serve it yet.
	Incoming
)

// SlotInfo is one Slot's state in this Group.
type SlotInfo struct {
	Status Status
	// Epoch is the Slot's Epoch in the table while this Group owns it, or
	// the Epoch it will have once an Incoming Slot is accepted. For an
	// Absent Slot it is the Epoch at which this Group last had it: the Slot
	// can only come back at a higher one.
	Epoch uint32
	// Peer is the Group the Slot is going to or coming from.
	Peer shard.GroupID
	// VEpoch is the Epoch this Group stamps into versions for the Slot. It
	// is at least Epoch, and higher if a Transaction has written the Slot
	// together with one whose Epoch is higher (A§11.5).
	VEpoch uint32
}

// Serves reports whether a Group in this state answers for the Slot.
func (s SlotInfo) Serves() bool { return s.Status == Owned || s.Status == Outgoing }

// Config sets up a data Group's Machine.
type Config struct {
	Group shard.GroupID
	// Slots is how many Slots the store has.
	Slots int
	// Owned are the Slots this Group starts with, at Epoch 0.
	Owned []shard.Slot
	// SessionTTL is passed to the key-value machine (fsm.Machine.SessionTTL).
	SessionTTL int64
	// Unchecked makes the Machine answer for any key, whether or not it
	// owns the Slot. It exists so that Rung 7's exposure of a Group that
	// doesn't check stays reproducible.
	Unchecked bool
}

// Machine is a data Group's state.
type Machine struct {
	cfg   Config
	inner *fsm.Machine
	slots []SlotInfo
	// dirty holds the keys written in Slots that are Outgoing, under their
	// stored names.
	dirty tree.Tree[struct{}]
	// incoming holds the keys of Slots that are Incoming, under their stored
	// names, apart from the keys this Group serves.
	incoming tree.Tree[fsm.Raw]
	// lastSlots is, per Session, the Slots its last request touched. A
	// Session moves with them (A§11.6).
	lastSlots tree.Tree[[]shard.Slot]
}

// New returns a Machine owning cfg.Owned.
func New(cfg Config) *Machine {
	m := &Machine{cfg: cfg, inner: fsm.New(), slots: make([]SlotInfo, cfg.Slots)}
	m.inner.SessionTTL = cfg.SessionTTL
	for _, s := range cfg.Owned {
		m.slots[s] = SlotInfo{Status: Owned}
	}
	return m
}

// Slots is every Slot's state in this Group.
func (m *Machine) Slots() []SlotInfo { return slices.Clone(m.slots) }

// LogTime is the time this Group's Expiry is judged against.
func (m *Machine) LogTime() int64 { return m.inner.LogTime() }

// Due reports whether a Leader that has heard Store time now should propose
// an fsm.OpTick (fsm.Machine.Due).
func (m *Machine) Due(now int64) bool { return m.inner.Due(now) }

// Sessions is how many Sessions this Group has a record of.
func (m *Machine) Sessions() int { return m.inner.Sessions() }

func (m *Machine) slotOf(key string) shard.Slot { return shard.SlotOf(key, m.cfg.Slots) }

// stored is the name a key is kept under: its Slot, then the key.
func stored(slot shard.Slot, key string) string {
	return string([]byte{byte(slot >> 8), byte(slot)}) + key
}

// slotEnd is the first stored name after every key of slot.
func slotEnd(slot shard.Slot) string { return stored(slot+1, "") }

// refusal is the answer for a key whose Slot this Group doesn't serve.
func (m *Machine) refusal(slots ...shard.Slot) (fsm.Response, bool) {
	if m.cfg.Unchecked {
		return fsm.Response{}, false
	}
	wrong, moving := false, false
	for _, s := range slots {
		switch info := m.slots[s]; {
		case info.Serves():
		case info.Status == Frozen:
			moving = true
		default:
			wrong = true
		}
	}
	switch {
	case wrong:
		return fsm.Response{Status: fsm.StatusWrongGroup}, true
	case moving:
		return fsm.Response{Status: fsm.StatusMoving}, true
	}
	return fsm.Response{}, false
}

// slotsOf lists the Slots a Command's keys are in, without repeats.
func (m *Machine) slotsOf(cmd fsm.Command) []shard.Slot {
	var slots []shard.Slot
	add := func(key string) {
		if s := m.slotOf(key); !slices.Contains(slots, s) {
			slots = append(slots, s)
		}
	}
	if cmd.Op == fsm.OpTxn {
		for _, c := range cmd.Conds {
			add(c.Key)
		}
		for _, w := range cmd.Writes {
			add(w.Key)
		}
		return slots
	}
	add(cmd.Key)
	return slots
}

// Apply executes a Committed Entry and returns its encoded fsm.Response.
func (m *Machine) Apply(e core.Entry) []byte {
	if e.Kind != core.EntryCommand {
		return nil
	}
	if len(e.Payload) > 0 && e.Payload[0] >= moveOpBase {
		return m.applyMove(e.Payload).Encode()
	}
	cmd, err := fsm.DecodeCommand(e.Payload)
	if err != nil {
		return fsm.Response{Status: fsm.StatusInvalid}.Encode()
	}
	switch cmd.Op {
	case fsm.OpTick, fsm.OpOpenSession:
		return m.inner.Apply(e)
	case fsm.OpScan:
		return m.scan(cmd, func(c fsm.Command) []byte {
			return m.inner.Apply(core.Entry{Index: e.Index, Kind: core.EntryCommand, Payload: c.Encode()})
		}).Encode()
	}

	slots := m.slotsOf(cmd)
	if resp, refused := m.refusal(slots...); refused {
		return resp.Encode()
	}
	// One version for everything the Command writes: the highest version
	// Epoch among its Slots, which they all then share.
	var vepoch uint32
	for _, s := range slots {
		vepoch = max(vepoch, m.slots[s].VEpoch)
	}
	if cmd.Op != fsm.OpGet {
		for _, s := range slots {
			m.slots[s].VEpoch = vepoch
		}
	}
	raw := m.inner.Apply(core.Entry{
		Index:   core.Index(shard.Version(vepoch, uint64(e.Index))),
		Kind:    core.EntryCommand,
		Payload: m.store(cmd).Encode(),
	})
	if cmd.Op == fsm.OpGet {
		return raw
	}
	for _, s := range slots {
		if m.slots[s].Status == Outgoing {
			m.markDirty(cmd)
			break
		}
	}
	if cmd.Session != 0 {
		m.lastSlots = m.lastSlots.Put(sessionKey(cmd.Session), slots)
	}
	return m.unstore(raw)
}

// markDirty remembers every key a write names, if its Slot is Outgoing.
func (m *Machine) markDirty(cmd fsm.Command) {
	mark := func(key string) {
		if s := m.slotOf(key); m.slots[s].Status == Outgoing {
			m.dirty = m.dirty.Put(stored(s, key), struct{}{})
		}
	}
	if cmd.Op == fsm.OpTxn {
		for _, w := range cmd.Writes {
			mark(w.Key)
		}
		return
	}
	mark(cmd.Key)
}

// Read answers an encoded get or scan from the state as it stands.
func (m *Machine) Read(query []byte) []byte {
	cmd, err := fsm.DecodeCommand(query)
	switch {
	case err != nil:
		return fsm.Response{Status: fsm.StatusInvalid}.Encode()
	case cmd.Op == fsm.OpScan:
		return m.scan(cmd, func(c fsm.Command) []byte { return m.inner.Read(c.Encode()) }).Encode()
	case cmd.Op != fsm.OpGet:
		return fsm.Response{Status: fsm.StatusInvalid}.Encode()
	}
	if resp, refused := m.refusal(m.slotOf(cmd.Key)); refused {
		return resp.Encode()
	}
	return m.inner.Read(m.store(cmd).Encode())
}

// scan answers a range scan over the Slots this Group serves. Keys are
// stored by Slot, so each Slot is scanned and the results put in key order.
// A Group answers only for what it serves: the caller merges the Groups.
func (m *Machine) scan(cmd fsm.Command, ask func(fsm.Command) []byte) fsm.Response {
	limit := cmd.Limit
	if limit == 0 || limit > fsm.MaxScan {
		limit = fsm.MaxScan
	}
	var items []fsm.Item
	for s, info := range m.slots {
		if !info.Serves() && !m.cfg.Unchecked {
			continue
		}
		part := fsm.Command{Op: fsm.OpScan, Key: stored(shard.Slot(s), cmd.Key), End: slotEnd(shard.Slot(s)), Limit: limit}
		if cmd.End != "" {
			part.End = stored(shard.Slot(s), cmd.End)
		}
		resp, err := fsm.DecodeResponse(ask(part))
		if err != nil || resp.Status != fsm.StatusOK {
			return fsm.Response{Status: fsm.StatusInvalid}
		}
		for _, it := range resp.Items {
			it.Key = it.Key[2:]
			items = append(items, it)
		}
	}
	slices.SortFunc(items, func(a, b fsm.Item) int {
		switch {
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		}
		return 0
	})
	if uint64(len(items)) > limit {
		items = items[:limit]
	}
	return fsm.Response{Status: fsm.StatusOK, Items: items}
}

// store rewrites a Command's keys to the names they are kept under.
func (m *Machine) store(cmd fsm.Command) fsm.Command {
	if cmd.Op == fsm.OpTxn {
		cmd.Conds = slices.Clone(cmd.Conds)
		for i, c := range cmd.Conds {
			cmd.Conds[i].Key = stored(m.slotOf(c.Key), c.Key)
		}
		cmd.Writes = slices.Clone(cmd.Writes)
		for i, w := range cmd.Writes {
			cmd.Writes[i].Key = stored(m.slotOf(w.Key), w.Key)
		}
		return cmd
	}
	cmd.Key = stored(m.slotOf(cmd.Key), cmd.Key)
	return cmd
}

// unstore turns stored names in a Response back into keys. Only a refused
// Transaction's Response carries any.
func (m *Machine) unstore(raw []byte) []byte {
	resp, err := fsm.DecodeResponse(raw)
	if err != nil || len(resp.Items) == 0 {
		return raw
	}
	for i := range resp.Items {
		resp.Items[i].Key = resp.Items[i].Key[2:]
	}
	return resp.Encode()
}

// Items lists every key this Group serves, in key order.
func (m *Machine) Items() []fsm.Item {
	var items []fsm.Item
	for s, info := range m.slots {
		if !info.Serves() {
			continue
		}
		m.inner.Range(stored(shard.Slot(s), ""), slotEnd(shard.Slot(s)), func(r fsm.Raw) bool {
			items = append(items, fsm.Item{Key: r.Key[2:], Value: r.Value, Version: r.Version, Expires: r.Deadline})
			return true
		})
	}
	slices.SortFunc(items, func(a, b fsm.Item) int {
		switch {
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		}
		return 0
	})
	return items
}

func sessionKey(id uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return string(b[:])
}
