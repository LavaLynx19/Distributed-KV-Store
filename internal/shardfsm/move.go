package shardfsm

import (
	"bytes"
	"encoding/binary"
	"errors"

	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
)

// The steps of a Move that a data Group records in its Log (A§11.4). Each
// may arrive more than once and must then change nothing.
const (
	moveOpBase = 0xF0
	// opBegin: the source starts copying the Slot out, and from here
	// remembers which of its keys are written.
	opBegin = moveOpBase + iota
	// opIncoming: the target is given some of the Slot's keys to hold.
	opIncoming
	// opFreeze: the source stops serving the Slot.
	opFreeze
	// opAccept: the target is given the rest and serves the Slot.
	opAccept
	// opDrop: the source forgets the Slot.
	opDrop
)

// Step is one step of a Move, as proposed to a data Group.
type Step struct {
	op byte
	// Slot is the Slot being moved. Epoch is the one it has in the Group
	// the Step is for: the old Epoch for the source's steps, the new one
	// for the target's.
	Slot  shard.Slot
	Epoch uint32
	// Peer is the other Group.
	Peer shard.GroupID
	// Transfer is what the target is given, for opIncoming and opAccept.
	Transfer Transfer
}

// Transfer is part of a Slot on its way from one Group to another.
type Transfer struct {
	// Full: Upserts are everything the Slot holds, and whatever the target
	// was given before is to be thrown away. Without it they are additions
	// and replacements.
	Full bool
	// Upserts are keys to hold, exactly as the source held them.
	Upserts []fsm.Raw
	// Deletes are keys the source removed after sending them.
	Deletes []string
	// Sessions are the records of Sessions whose last request was in the
	// Slot (A§11.6).
	Sessions []fsm.SessionRecord
	// LogTime is the source's Log time. The target catches up with it
	// before it serves, so the Slot's keys expire when they would have.
	LogTime int64
	// VEpoch is the Epoch the source stamped into the Slot's versions.
	VEpoch uint32
}

// BeginStep is the step that starts copying slot out of the Group that owns it
// at epoch, towards the Group to.
func BeginStep(slot shard.Slot, epoch uint32, to shard.GroupID) Step {
	return Step{op: opBegin, Slot: slot, Epoch: epoch, Peer: to}
}

// IncomingStep is the step that hands the target some of the Slot's keys. epoch
// is the Epoch the Slot will have there.
func IncomingStep(slot shard.Slot, epoch uint32, from shard.GroupID, t Transfer) Step {
	return Step{op: opIncoming, Slot: slot, Epoch: epoch, Peer: from, Transfer: t}
}

// FreezeStep is the step that stops the source serving slot.
func FreezeStep(slot shard.Slot, epoch uint32) Step {
	return Step{op: opFreeze, Slot: slot, Epoch: epoch}
}

// AcceptStep is the step that completes the Slot in the target and makes it the
// owner at epoch. t must have been read from the source after its Freeze.
func AcceptStep(slot shard.Slot, epoch uint32, from shard.GroupID, t Transfer) Step {
	return Step{op: opAccept, Slot: slot, Epoch: epoch, Peer: from, Transfer: t}
}

// DropStep is the step that makes the source forget slot, which it froze at
// epoch.
func DropStep(slot shard.Slot, epoch uint32) Step { return Step{op: opDrop, Slot: slot, Epoch: epoch} }

func (m *Machine) applyMove(payload []byte) fsm.Response {
	st, err := DecodeStep(payload)
	if err != nil || int(st.Slot) >= len(m.slots) {
		return fsm.Response{Status: fsm.StatusInvalid}
	}
	info := &m.slots[st.Slot]
	ok := fsm.Response{Status: fsm.StatusOK}
	refused := fsm.Response{Status: fsm.StatusInvalid}
	switch st.op {
	case opBegin:
		switch {
		case info.Status == Outgoing && info.Epoch == st.Epoch && info.Peer == st.Peer:
			return ok
		case info.Status != Owned || info.Epoch != st.Epoch:
			return refused
		}
		info.Status, info.Peer = Outgoing, st.Peer

	case opIncoming:
		switch {
		case info.Serves() && info.Epoch >= st.Epoch:
			return ok // this Move finished already
		case info.Status == Absent:
			*info = SlotInfo{Status: Incoming, Epoch: st.Epoch, Peer: st.Peer}
		case info.Status != Incoming || info.Epoch != st.Epoch:
			return refused
		}
		m.stage(st.Slot, st.Transfer)

	case opFreeze:
		switch {
		case info.Status == Frozen && info.Epoch == st.Epoch:
			return ok
		case !info.Serves() || info.Epoch != st.Epoch:
			return refused
		}
		info.Status = Frozen

	case opAccept:
		switch {
		case info.Serves() && info.Epoch >= st.Epoch:
			return ok
		case info.Status == Absent && st.Transfer.Full:
			// Nothing was copied ahead: the accept brings the whole Slot.
		case info.Status != Incoming || info.Epoch != st.Epoch:
			return refused
		}
		m.stage(st.Slot, st.Transfer)
		// Catch up with the source's time first, so that what it would
		// have expired by now doesn't appear here.
		m.inner.AdvanceTime(st.Transfer.LogTime)
		from, to := stored(st.Slot, ""), slotEnd(st.Slot)
		var arrived []fsm.Raw
		m.incoming.Ascend(from, to, func(_ string, r fsm.Raw) bool {
			arrived = append(arrived, r)
			return true
		})
		for _, r := range arrived {
			m.incoming = m.incoming.Delete(r.Key)
			m.inner.SetRaw(r)
		}
		m.inner.AdvanceTime(st.Transfer.LogTime) // removes any that were already due
		for _, rec := range st.Transfer.Sessions {
			if have, known := m.inner.SessionRecord(rec.ID); !known || rec.LastSeq > have.LastSeq {
				m.inner.PutSessionRecord(rec)
				m.lastSlots = m.lastSlots.Put(sessionKey(rec.ID), []shard.Slot{st.Slot})
			}
		}
		// Versions here must beat every version the source gave out.
		*info = SlotInfo{Status: Owned, Epoch: st.Epoch, VEpoch: max(st.Epoch, st.Transfer.VEpoch+1)}

	case opDrop:
		switch {
		case info.Status == Absent:
			return ok
		case info.Status != Frozen || info.Epoch != st.Epoch:
			return refused
		}
		m.clear(st.Slot)
		*info = SlotInfo{}
	}
	return ok
}

// stage adds a Transfer to what is held for an Incoming Slot.
func (m *Machine) stage(slot shard.Slot, t Transfer) {
	if t.Full {
		var held []string
		m.incoming.Ascend(stored(slot, ""), slotEnd(slot), func(k string, _ fsm.Raw) bool {
			held = append(held, k)
			return true
		})
		for _, k := range held {
			m.incoming = m.incoming.Delete(k)
		}
	}
	for _, r := range t.Upserts {
		r.Key = stored(slot, r.Key)
		m.incoming = m.incoming.Put(r.Key, r)
	}
	for _, k := range t.Deletes {
		m.incoming = m.incoming.Delete(stored(slot, k))
	}
}

// clear removes everything this Group holds of a Slot.
func (m *Machine) clear(slot shard.Slot) {
	from, to := stored(slot, ""), slotEnd(slot)
	var keys, dirty []string
	m.inner.Range(from, to, func(r fsm.Raw) bool {
		keys = append(keys, r.Key)
		return true
	})
	for _, k := range keys {
		m.inner.DeleteRaw(k)
	}
	m.dirty.Ascend(from, to, func(k string, _ struct{}) bool {
		dirty = append(dirty, k)
		return true
	})
	for _, k := range dirty {
		m.dirty = m.dirty.Delete(k)
	}
}

// Chunk reads up to n of a Slot's keys, starting after the key after ("" for
// the start), for sending to the Group the Slot is going to. done is set
// when there are no more.
func (m *Machine) Chunk(slot shard.Slot, after string, n int) (items []fsm.Raw, done bool) {
	from := stored(slot, "")
	if after != "" {
		from = stored(slot, after) + "\x00"
	}
	done = true
	m.inner.Range(from, slotEnd(slot), func(r fsm.Raw) bool {
		if len(items) == n {
			done = false
			return false
		}
		r.Key = r.Key[2:]
		items = append(items, r)
		return true
	})
	return items, done
}

// Final reads what the target still needs once the Slot is Frozen: with
// full, everything; otherwise the keys written since the copy began. It
// also carries the Sessions that go with the Slot and this Group's Log
// time. It reports false if the Slot isn't Frozen, in which case nothing it
// could return would be final.
func (m *Machine) Final(slot shard.Slot, full bool) (Transfer, bool) {
	if m.slots[slot].Status != Frozen {
		return Transfer{}, false
	}
	t := Transfer{Full: full, LogTime: m.inner.LogTime(), VEpoch: m.slots[slot].VEpoch}
	from, to := stored(slot, ""), slotEnd(slot)
	if full {
		m.inner.Range(from, to, func(r fsm.Raw) bool {
			r.Key = r.Key[2:]
			t.Upserts = append(t.Upserts, r)
			return true
		})
	} else {
		m.dirty.Ascend(from, to, func(k string, _ struct{}) bool {
			if r, exists := m.inner.GetRaw(k); exists {
				r.Key = k[2:]
				t.Upserts = append(t.Upserts, r)
			} else {
				t.Deletes = append(t.Deletes, k[2:])
			}
			return true
		})
	}
	m.lastSlots.Ascend("", "", func(id string, slots []shard.Slot) bool {
		for _, s := range slots {
			if s == slot {
				if rec, ok := m.inner.SessionRecord(binary.BigEndian.Uint64([]byte(id))); ok {
					t.Sessions = append(t.Sessions, rec)
				}
				break
			}
		}
		return true
	})
	return t, true
}

// Encode lays a Step out for an Entry's payload. Its first byte is the op,
// which no key-value Command starts with.
func (s Step) Encode() []byte {
	b := []byte{s.op}
	b = binary.AppendUvarint(b, uint64(s.Slot))
	b = binary.AppendUvarint(b, uint64(s.Epoch))
	b = binary.AppendUvarint(b, uint64(s.Peer))
	if s.op != opIncoming && s.op != opAccept {
		return b
	}
	t := s.Transfer
	full := byte(0)
	if t.Full {
		full = 1
	}
	b = append(b, full)
	b = binary.AppendVarint(b, t.LogTime)
	b = binary.AppendUvarint(b, uint64(t.VEpoch))
	b = binary.AppendUvarint(b, uint64(len(t.Upserts)))
	for _, r := range t.Upserts {
		b = appendRaw(b, r)
	}
	b = binary.AppendUvarint(b, uint64(len(t.Deletes)))
	for _, k := range t.Deletes {
		b = appendBytes(b, []byte(k))
	}
	b = binary.AppendUvarint(b, uint64(len(t.Sessions)))
	for _, rec := range t.Sessions {
		b = appendSession(b, rec)
	}
	return b
}

func appendBytes(b, field []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(field)))
	return append(b, field...)
}

func appendRaw(b []byte, r fsm.Raw) []byte {
	b = appendBytes(b, []byte(r.Key))
	b = appendBytes(b, r.Value)
	b = binary.AppendUvarint(b, r.Version)
	return binary.AppendVarint(b, r.Deadline)
}

func appendSession(b []byte, rec fsm.SessionRecord) []byte {
	b = binary.AppendUvarint(b, rec.ID)
	b = binary.AppendUvarint(b, rec.LastSeq)
	b = appendBytes(b, rec.LastResp)
	b = binary.AppendVarint(b, rec.LastUsed)
	return binary.AppendUvarint(b, rec.Floor)
}

var errMalformed = errors.New("shardfsm: malformed payload")

// reader takes fields off the front of a buffer. After a field that isn't
// there, every later one is zero and bad is set.
type reader struct {
	b   []byte
	bad bool
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.bad, r.b = true, nil
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.bad, r.b = true, nil
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) byte() byte {
	if len(r.b) == 0 {
		r.bad = true
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

// bytes reads a length and that many bytes. An empty field is nil.
func (r *reader) bytes() []byte {
	n := r.uvarint()
	if n > uint64(len(r.b)) {
		r.bad, r.b = true, nil
		return nil
	}
	if n == 0 {
		return nil
	}
	v := bytes.Clone(r.b[:n])
	r.b = r.b[n:]
	return v
}

// count reads how many items follow, refusing a number the buffer couldn't
// hold.
func (r *reader) count() int {
	n := r.uvarint()
	if n > uint64(len(r.b)) {
		r.bad, r.b = true, nil
		return 0
	}
	return int(n)
}

func (r *reader) raw() fsm.Raw {
	return fsm.Raw{Key: string(r.bytes()), Value: r.bytes(), Version: r.uvarint(), Deadline: r.varint()}
}

func (r *reader) session() fsm.SessionRecord {
	return fsm.SessionRecord{ID: r.uvarint(), LastSeq: r.uvarint(), LastResp: r.bytes(), LastUsed: r.varint(), Floor: r.uvarint()}
}

func DecodeStep(payload []byte) (Step, error) {
	if len(payload) == 0 || payload[0] < opBegin || payload[0] > opDrop {
		return Step{}, errMalformed
	}
	r := &reader{b: payload[1:]}
	s := Step{op: payload[0], Slot: shard.Slot(r.uvarint()), Epoch: uint32(r.uvarint()), Peer: shard.GroupID(r.uvarint())}
	if s.op == opIncoming || s.op == opAccept {
		t := &s.Transfer
		t.Full = r.byte() == 1
		t.LogTime = r.varint()
		t.VEpoch = uint32(r.uvarint())
		for range r.count() {
			t.Upserts = append(t.Upserts, r.raw())
		}
		for range r.count() {
			t.Deletes = append(t.Deletes, string(r.bytes()))
		}
		for range r.count() {
			t.Sessions = append(t.Sessions, r.session())
		}
	}
	if r.bad || len(r.b) != 0 {
		return Step{}, errMalformed
	}
	return s, nil
}

// Capture returns a function that encodes the state as it is right now: the
// key-value machine's own capture, then each Slot's state, the keys written
// in Outgoing Slots, the keys held for Incoming ones, and each Session's
// last Slots.
func (m *Machine) Capture() func() []byte {
	inner := m.inner.Capture()
	slots, dirty, incoming, lastSlots := m.Slots(), m.dirty, m.incoming, m.lastSlots
	return func() []byte {
		b := appendBytes(nil, inner())
		b = binary.AppendUvarint(b, uint64(len(slots)))
		for _, s := range slots {
			b = append(b, byte(s.Status))
			b = binary.AppendUvarint(b, uint64(s.Epoch))
			b = binary.AppendUvarint(b, uint64(s.Peer))
			b = binary.AppendUvarint(b, uint64(s.VEpoch))
		}
		b = binary.AppendUvarint(b, uint64(dirty.Len()))
		dirty.Ascend("", "", func(k string, _ struct{}) bool {
			b = appendBytes(b, []byte(k))
			return true
		})
		b = binary.AppendUvarint(b, uint64(incoming.Len()))
		incoming.Ascend("", "", func(_ string, r fsm.Raw) bool {
			b = appendRaw(b, r)
			return true
		})
		b = binary.AppendUvarint(b, uint64(lastSlots.Len()))
		lastSlots.Ascend("", "", func(id string, ss []shard.Slot) bool {
			b = append(b, id...) // always 8 bytes
			b = binary.AppendUvarint(b, uint64(len(ss)))
			for _, s := range ss {
				b = binary.AppendUvarint(b, uint64(s))
			}
			return true
		})
		return b
	}
}

// Restore replaces the Machine's state with a captured one.
func (m *Machine) Restore(data []byte) error {
	r := &reader{b: data}
	inner := fsm.New()
	inner.SessionTTL = m.cfg.SessionTTL
	if err := inner.Restore(r.bytes()); err != nil {
		return err
	}
	n := r.count()
	if n != len(m.slots) {
		return errMalformed
	}
	slots := make([]SlotInfo, n)
	for i := range slots {
		slots[i] = SlotInfo{Status: Status(r.byte()), Epoch: uint32(r.uvarint()), Peer: shard.GroupID(r.uvarint()), VEpoch: uint32(r.uvarint())}
	}
	next := &Machine{cfg: m.cfg, inner: inner, slots: slots}
	for range r.count() {
		next.dirty = next.dirty.Put(string(r.bytes()), struct{}{})
	}
	for range r.count() {
		raw := r.raw()
		next.incoming = next.incoming.Put(raw.Key, raw)
	}
	for range r.count() {
		if len(r.b) < 8 {
			return errMalformed
		}
		id := string(r.b[:8])
		r.b = r.b[8:]
		var ss []shard.Slot
		for range r.count() {
			ss = append(ss, shard.Slot(r.uvarint()))
		}
		next.lastSlots = next.lastSlots.Put(id, ss)
	}
	if r.bad || len(r.b) != 0 {
		return errMalformed
	}
	*m = *next
	return nil
}
