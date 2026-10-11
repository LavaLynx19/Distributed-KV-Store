package shardfsm

import (
	"encoding/binary"
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
)

// The steps of a Transaction across Groups, as Entries in a data Group's
// Log (A§12.2). Their codes sit below the Move steps' and above any
// fsm.Op.
const (
	txnOpBase = 0xE0
	// opPrepare: the Group checks its part of a Transaction and Locks the
	// keys it names.
	opPrepare = txnOpBase + iota
	// opFinish: the Group is told what was decided. It applies its writes
	// or not, and releases its Locks.
	opFinish
)

// TxnID names a Transaction: the Session and request number of the request
// that asked for it.
type TxnID struct{ Session, Seq uint64 }

func (id TxnID) key() string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], id.Session)
	binary.BigEndian.PutUint64(b[8:], id.Seq)
	return string(b[:])
}

// TxnStep is one step of a Transaction across Groups, as proposed to a data
// Group.
type TxnStep struct {
	op byte
	ID TxnID
	// Lead says the Step is for the Transaction's Coordinator Group, which
	// is also where the request's Session is checked and its outcome kept.
	Lead bool

	// For a prepare: the conditions and writes that are this Group's part,
	// the Store time the writes are stamped with, and whether the request
	// may start a record of its Session (fsm.Command.Register).
	Conds    []fsm.Cond
	Writes   []fsm.Write
	Stamp    int64
	Register bool

	// For a finish: whether the Transaction commits, and the version its
	// writes get.
	Commit  bool
	Version uint64
}

// PrepareStep asks a Group to check and Lock its part of a Transaction.
func PrepareStep(id TxnID, lead bool, conds []fsm.Cond, writes []fsm.Write, stamp int64, register bool) TxnStep {
	return TxnStep{op: opPrepare, ID: id, Lead: lead, Conds: conds, Writes: writes, Stamp: stamp, Register: register}
}

// FinishStep tells a Group what was decided.
func FinishStep(id TxnID, lead, commit bool, version uint64) TxnStep {
	return TxnStep{op: opFinish, ID: id, Lead: lead, Commit: commit, Version: version}
}

// Vote is a Group's answer to a prepare.
type Vote uint8

const (
	// VoteYes: every condition holds and the keys are Locked. The
	// Response's Version is the version this Group would give the writes.
	VoteYes Vote = iota + 1
	// VoteNo: the Group can't take part. The Response says why. If a
	// condition failed, the keys are Locked all the same, so that what
	// every Group reports is true at one moment; otherwise nothing is.
	VoteNo
	// VoteDone: the request was carried out before. The Response is the
	// answer it got then.
	VoteDone
)

// EncodeVote packs a Vote with its Response.
func EncodeVote(v Vote, resp fsm.Response) []byte {
	return append([]byte{byte(v)}, resp.Encode()...)
}

// DecodeVote unpacks the answer to a prepare.
func DecodeVote(b []byte) (Vote, fsm.Response, error) {
	if len(b) == 0 || b[0] < byte(VoteYes) || b[0] > byte(VoteDone) {
		return 0, fsm.Response{}, errMalformed
	}
	resp, err := fsm.DecodeResponse(b[1:])
	return Vote(b[0]), resp, err
}

// prepared is what a Group remembers of a Transaction between its prepare
// and its finish.
type prepared struct {
	writes []fsm.Write
	stamp  int64
	// keys are the keys Locked, under their stored names.
	keys []string
	// answer is what the prepare was answered, to answer a repeat the same
	// way.
	answer []byte
}

// Locked reports how many keys are Locked in this Group.
func (m *Machine) Locked() int { return m.locks.Len() }

// Prepared lists the Transactions this Group has prepared and not finished.
func (m *Machine) Prepared() []TxnID {
	var ids []TxnID
	m.txns.Ascend("", "", func(k string, _ prepared) bool {
		ids = append(ids, TxnID{Session: binary.BigEndian.Uint64([]byte(k[:8])), Seq: binary.BigEndian.Uint64([]byte(k[8:]))})
		return true
	})
	return ids
}

// lockedAny reports whether any of a Command's keys is Locked.
func (m *Machine) lockedAny(cmd fsm.Command) bool {
	if cmd.Op == fsm.OpTxn {
		for _, c := range cmd.Conds {
			if _, held := m.locks.Get(stored(m.slotOf(c.Key), c.Key)); held {
				return true
			}
		}
		for _, w := range cmd.Writes {
			if _, held := m.locks.Get(stored(m.slotOf(w.Key), w.Key)); held {
				return true
			}
		}
		return false
	}
	_, held := m.locks.Get(stored(m.slotOf(cmd.Key), cmd.Key))
	return held
}

// lockedIn reports whether any key of a scan's range, in a Slot this Group
// serves, is Locked.
func (m *Machine) lockedIn(from, to string) bool {
	found := false
	m.locks.Ascend("", "", func(k string, _ TxnID) bool {
		key := k[2:]
		found = key >= from && (to == "" || key < to)
		return !found
	})
	return found
}

func (m *Machine) applyTxn(index core.Index, payload []byte) []byte {
	step, err := DecodeTxnStep(payload)
	if err != nil {
		return fsm.Response{Status: fsm.StatusInvalid}.Encode()
	}
	if step.op == opPrepare {
		return m.prepare(index, step)
	}
	return m.finish(index, step).Encode()
}

func (m *Machine) prepare(index core.Index, step TxnStep) []byte {
	no := func(status fsm.Status) []byte { return EncodeVote(VoteNo, fsm.Response{Status: status}) }
	if step.Stamp > m.inner.LogTime() {
		m.inner.AdvanceTime(step.Stamp)
	}
	if step.Lead && step.ID.Session != 0 {
		rec, known := m.inner.SessionRecord(step.ID.Session)
		if !known && step.Register {
			rec, known = fsm.SessionRecord{ID: step.ID.Session, Floor: step.ID.Seq, LastUsed: m.inner.LogTime()}, true
			m.inner.PutSessionRecord(rec)
		}
		switch {
		case !known || step.ID.Seq < rec.Floor:
			return no(fsm.StatusSessionExpired)
		case step.ID.Seq == rec.LastSeq && rec.LastSeq != 0:
			resp, err := fsm.DecodeResponse(rec.LastResp)
			if err != nil {
				return no(fsm.StatusInvalid)
			}
			return EncodeVote(VoteDone, resp)
		case step.ID.Seq < rec.LastSeq:
			return no(fsm.StatusInvalid)
		}
	}
	if p, again := m.txns.Get(step.ID.key()); again {
		return p.answer
	}

	cmd := fsm.Command{Op: fsm.OpTxn, Conds: step.Conds, Writes: step.Writes}
	slots := m.slotsOf(cmd)
	if resp, refused := m.refusal(slots...); refused {
		return no(resp.Status)
	}
	if m.lockedAny(cmd) {
		return no(fsm.StatusTxnConflict)
	}
	var failed []fsm.Item
	for _, c := range step.Conds {
		if found, _ := m.inner.GetRaw(stored(m.slotOf(c.Key), c.Key)); found.Version != c.Version {
			failed = append(failed, fsm.Item{Key: c.Key, Version: found.Version})
		}
	}

	p := prepared{writes: step.Writes, stamp: step.Stamp}
	lock := func(key string) {
		name := stored(m.slotOf(key), key)
		if !slices.Contains(p.keys, name) {
			p.keys = append(p.keys, name)
			m.locks = m.locks.Put(name, step.ID)
		}
	}
	for _, c := range step.Conds {
		lock(c.Key)
	}
	for _, w := range step.Writes {
		lock(w.Key)
	}
	if len(failed) > 0 {
		p.answer = EncodeVote(VoteNo, fsm.Response{Status: fsm.StatusVersionMismatch, Items: failed})
	} else {
		p.answer = EncodeVote(VoteYes, fsm.Response{Status: fsm.StatusOK, Version: m.versionAt(index, slots)})
	}
	m.txns = m.txns.Put(step.ID.key(), p)
	return p.answer
}

func (m *Machine) finish(index core.Index, step TxnStep) fsm.Response {
	ok := fsm.Response{Status: fsm.StatusOK, Version: step.Version}
	p, found := m.txns.Get(step.ID.key())
	if !found {
		return ok
	}
	for _, name := range p.keys {
		m.locks = m.locks.Delete(name)
	}
	m.txns = m.txns.Delete(step.ID.key())
	if !step.Commit {
		return ok
	}

	cmd := fsm.Command{Op: fsm.OpTxn, Writes: p.writes, Stamp: p.stamp}
	slots := m.slotsOf(cmd)
	m.raise(index, slots, step.Version)
	m.inner.Apply(core.Entry{Index: core.Index(step.Version), Kind: core.EntryCommand, Payload: m.store(cmd).Encode()})
	m.markDirty(cmd)
	if step.Lead && step.ID.Session != 0 {
		rec, _ := m.inner.SessionRecord(step.ID.Session)
		rec.ID, rec.LastSeq, rec.LastResp, rec.LastUsed = step.ID.Session, step.ID.Seq, ok.Encode(), m.inner.LogTime()
		m.inner.PutSessionRecord(rec)
		m.lastSlots = m.lastSlots.Put(sessionKey(step.ID.Session), slots)
	}
	return ok
}

// Encode packs a TxnStep into an Entry's payload.
func (s TxnStep) Encode() []byte {
	b := []byte{s.op}
	b = binary.AppendUvarint(b, s.ID.Session)
	b = binary.AppendUvarint(b, s.ID.Seq)
	b = append(b, flag(s.Lead))
	if s.op == opFinish {
		b = append(b, flag(s.Commit))
		return binary.AppendUvarint(b, s.Version)
	}
	b = append(b, flag(s.Register))
	b = binary.AppendVarint(b, s.Stamp)
	b = binary.AppendUvarint(b, uint64(len(s.Conds)))
	for _, c := range s.Conds {
		b = appendBytes(b, []byte(c.Key))
		b = binary.AppendUvarint(b, c.Version)
	}
	b = binary.AppendUvarint(b, uint64(len(s.Writes)))
	for _, w := range s.Writes {
		b = append(b, byte(w.Op))
		b = appendBytes(b, []byte(w.Key))
		b = appendBytes(b, w.Value)
		b = binary.AppendVarint(b, w.TTL)
	}
	return b
}

func flag(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// DecodeTxnStep unpacks a TxnStep.
func DecodeTxnStep(payload []byte) (TxnStep, error) {
	if len(payload) == 0 || payload[0] < opPrepare || payload[0] > opFinish {
		return TxnStep{}, errMalformed
	}
	r := &reader{b: payload[1:]}
	s := TxnStep{op: payload[0], ID: TxnID{Session: r.uvarint(), Seq: r.uvarint()}, Lead: r.byte() == 1}
	if s.op == opFinish {
		s.Commit, s.Version = r.byte() == 1, r.uvarint()
	} else {
		s.Register, s.Stamp = r.byte() == 1, r.varint()
		for range r.count() {
			s.Conds = append(s.Conds, fsm.Cond{Key: string(r.bytes()), Version: r.uvarint()})
		}
		for range r.count() {
			w := fsm.Write{Op: fsm.Op(r.byte()), Key: string(r.bytes()), Value: r.bytes(), TTL: r.varint()}
			if w.Op != fsm.OpPut && w.Op != fsm.OpDelete {
				return TxnStep{}, errMalformed
			}
			s.Writes = append(s.Writes, w)
		}
	}
	if r.bad || len(r.b) != 0 {
		return TxnStep{}, errMalformed
	}
	return s, nil
}
