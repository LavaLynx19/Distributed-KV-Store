// Package meta is the Meta Group's state machine (A§11.2): the Slot table,
// Session ids and Store time. It holds no keys. Like every state machine it
// is pure: Members that apply the same Entries hold the same table.
package meta

import (
	"encoding/binary"
	"errors"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/shard"
)

// Op is what a Command does.
type Op uint8

const (
	// OpMove records the intent to move Slot to the Group To (A§11.4, step
	// 1). The Slot's owner doesn't change yet.
	OpMove Op = iota + 1
	// OpDone records that the Move of Slot has finished: the Group it was
	// moving to owns it, at Epoch (step 5).
	OpDone
	// OpOpenSession hands out a Session id that means the same in every
	// Group (A§11.6).
	OpOpenSession
	// OpTick moves Store time on to Stamp, the Meta Leader's clock reading
	// (A§11.7).
	OpTick
)

// Command is one request to the Meta Group, carried in an Entry's payload.
type Command struct {
	Op    Op
	Slot  shard.Slot
	To    shard.GroupID
	Epoch uint32
	Stamp int64
}

// Status is the outcome of a Command.
type Status uint8

const (
	StatusOK Status = iota + 1
	// StatusBusy: the Slot is already on its way somewhere else.
	StatusBusy
	// StatusInvalid: no such Slot or Group, a Move to the Group that owns
	// the Slot already, or a Done that matches no Move.
	StatusInvalid
)

// Response is the answer to a Command. Version is the table's version
// afterwards. Session is the new id, for OpOpenSession.
type Response struct {
	Status  Status
	Version uint64
	Session uint64
}

// Machine is the Meta Group's state.
type Machine struct {
	table  shard.Table
	groups int
	// flipAtOnce is the store Rung 7 starts from: a Move changes the owner
	// in the table the moment it is asked for, and nobody confirms anything.
	flipAtOnce bool
}

// New returns the table a store starts with: slots Slots dealt out in turn
// to data Groups 1..groups, all at Epoch 0.
func New(slots, groups int) *Machine {
	m := &Machine{groups: groups}
	for i := range slots {
		m.table.Slots = append(m.table.Slots, shard.Owner{Group: shard.GroupID(1 + i%groups)})
	}
	return m
}

// NewFlipping is New for the naive store: OpMove changes a Slot's owner at
// once. It exists so that Rung 7's exposure of that stays reproducible.
func NewFlipping(slots, groups int) *Machine {
	m := New(slots, groups)
	m.flipAtOnce = true
	return m
}

// Table is the Slot table as it stands.
func (m *Machine) Table() shard.Table { return m.table.Clone() }

// Apply executes a Committed Entry and returns its encoded Response.
func (m *Machine) Apply(e core.Entry) []byte {
	if e.Kind != core.EntryCommand {
		return nil
	}
	cmd, err := DecodeCommand(e.Payload)
	if err != nil {
		return Response{Status: StatusInvalid, Version: m.table.Version}.Encode()
	}
	resp := Response{Status: StatusOK}
	switch cmd.Op {
	case OpMove:
		resp.Status = m.move(cmd, uint64(e.Index))
	case OpDone:
		resp.Status = m.done(cmd, uint64(e.Index))
	case OpOpenSession:
		// The Entry's index is unique, which makes it a ready-made id.
		resp.Session = uint64(e.Index)
	case OpTick:
		if cmd.Stamp > m.table.StoreTime {
			m.table.StoreTime = cmd.Stamp
		}
	}
	resp.Version = m.table.Version
	return resp.Encode()
}

func (m *Machine) move(cmd Command, index uint64) Status {
	if int(cmd.Slot) >= len(m.table.Slots) || cmd.To == shard.Meta || int(cmd.To) > m.groups {
		return StatusInvalid
	}
	o := &m.table.Slots[cmd.Slot]
	switch {
	case o.MovingTo == cmd.To:
		return StatusOK // asked twice
	case o.MovingTo != 0:
		return StatusBusy
	case o.Group == cmd.To:
		return StatusInvalid
	}
	if m.flipAtOnce {
		o.Group, o.Epoch = cmd.To, o.Epoch+1
	} else {
		o.MovingTo = cmd.To
	}
	m.table.Version = index
	return StatusOK
}

func (m *Machine) done(cmd Command, index uint64) Status {
	if int(cmd.Slot) >= len(m.table.Slots) {
		return StatusInvalid
	}
	o := &m.table.Slots[cmd.Slot]
	switch {
	case o.Epoch >= cmd.Epoch:
		return StatusOK // told twice
	case o.MovingTo == 0 || cmd.Epoch != o.Epoch+1:
		return StatusInvalid
	}
	o.Group, o.Epoch, o.MovingTo = o.MovingTo, cmd.Epoch, 0
	m.table.Version = index
	return StatusOK
}

// Read answers any query with the encoded table.
func (m *Machine) Read([]byte) []byte { return m.table.Encode() }

// Capture returns a function that encodes the state as it is now.
func (m *Machine) Capture() func() []byte {
	encoded := m.table.Encode()
	return func() []byte { return encoded }
}

// Restore replaces the state with a captured one.
func (m *Machine) Restore(data []byte) error {
	t, err := shard.DecodeTable(data)
	if err != nil {
		return err
	}
	m.table = t
	return nil
}

// Encode lays a Command out as its op, then Slot, To, Epoch and Stamp as
// varints.
func (c Command) Encode() []byte {
	b := []byte{byte(c.Op)}
	b = binary.AppendUvarint(b, uint64(c.Slot))
	b = binary.AppendUvarint(b, uint64(c.To))
	b = binary.AppendUvarint(b, uint64(c.Epoch))
	return binary.AppendVarint(b, c.Stamp)
}

var errMalformed = errors.New("meta: malformed payload")

func DecodeCommand(b []byte) (Command, error) {
	if len(b) < 1 || Op(b[0]) < OpMove || Op(b[0]) > OpTick {
		return Command{}, errMalformed
	}
	c := Command{Op: Op(b[0])}
	b = b[1:]
	var fields [3]uint64
	for i := range fields {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return Command{}, errMalformed
		}
		fields[i], b = v, b[n:]
	}
	stamp, n := binary.Varint(b)
	if n <= 0 || n != len(b) {
		return Command{}, errMalformed
	}
	c.Slot, c.To, c.Epoch, c.Stamp = shard.Slot(fields[0]), shard.GroupID(fields[1]), uint32(fields[2]), stamp
	return c, nil
}

// Encode lays a Response out as its status, then Version and Session.
func (r Response) Encode() []byte {
	b := []byte{byte(r.Status)}
	b = binary.AppendUvarint(b, r.Version)
	return binary.AppendUvarint(b, r.Session)
}

func DecodeResponse(b []byte) (Response, error) {
	if len(b) < 1 {
		return Response{}, errMalformed
	}
	r := Response{Status: Status(b[0])}
	v, n := binary.Uvarint(b[1:])
	if n <= 0 {
		return Response{}, errMalformed
	}
	s, k := binary.Uvarint(b[1+n:])
	if k <= 0 || 1+n+k != len(b) {
		return Response{}, errMalformed
	}
	r.Version, r.Session = v, s
	return r, nil
}
