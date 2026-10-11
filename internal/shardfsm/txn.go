package shardfsm

import (
	"encoding/binary"
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
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
	// Coord is the Coordinator Group: whom to ask what was decided, and
	// whose word a finish is.
	Coord shard.GroupID

	// For a prepare: the conditions and writes that are this Group's part,
	// the Store time the writes are stamped with, and whether the request
	// may start a record of its Session (fsm.Command.Register).
	Conds    []fsm.Cond
	Writes   []fsm.Write
	Stamp    int64
	Register bool
	// Age, on a prepare, is the Transaction's age (A§12.5).
	Age int64

	// For a finish: whether the Transaction commits, the version its
	// writes get, and the version this Group offered in the prepare the
	// decision counted. A Group that has since prepared the Transaction
	// again, from a prepare that arrived late, offered another, and must
	// not commit that one (A§12.2).
	Commit  bool
	Version uint64
	Offer   uint64
}

// PrepareStep asks a Group to check and Lock its part of a Transaction.
func PrepareStep(id TxnID, coord shard.GroupID, lead bool, conds []fsm.Cond, writes []fsm.Write, stamp int64, register bool) TxnStep {
	return TxnStep{op: opPrepare, ID: id, Coord: coord, Lead: lead, Conds: conds, Writes: writes, Stamp: stamp, Register: register, Age: stamp}
}

// Aged returns the prepare with the Transaction's age set. Without it the
// age is the prepare's Stamp.
func (s TxnStep) Aged(age int64) TxnStep {
	s.Age = age
	return s
}

// FinishStep tells a Group what was decided.
func FinishStep(id TxnID, coord shard.GroupID, lead, commit bool, version, offer uint64) TxnStep {
	return TxnStep{op: opFinish, ID: id, Coord: coord, Lead: lead, Commit: commit, Version: version, Offer: offer}
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
	coord  shard.GroupID
	writes []fsm.Write
	stamp  int64
	age    int64
	// keys are the keys Locked, under their stored names.
	keys []string
	// answer is what the prepare was answered, to answer a repeat the same
	// way.
	answer []byte
}

// Finished says how a Transaction ended in a Group that had prepared it.
type Finished struct {
	ID     TxnID
	Commit bool
	// OK is false when the last Entry finished no Transaction.
	OK bool
	// Began lists the Groups of a Transaction the last Entry had this
	// Group take on as its Coordinator Group, this Group first.
	Began []shard.GroupID
}

// LastFinished is the Transaction the last Entry applied finished in this
// Group, if it finished one the Group had prepared.
func (m *Machine) LastFinished() Finished { return m.finished }

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

// Held is a Transaction a Group has prepared: its Coordinator Group and its
// age.
type Held struct {
	ID    TxnID
	Coord shard.GroupID
	Age   int64
}

// Older reports whether h is older than other: by age, then Session, then
// request number. Every Group compares the same way, so of any two
// Transactions exactly one is the older (A§12.5).
func (h Held) Older(other Held) bool {
	switch {
	case h.Age != other.Age:
		return h.Age < other.Age
	case h.ID.Session != other.ID.Session:
		return h.ID.Session < other.ID.Session
	}
	return h.ID.Seq < other.ID.Seq
}

// EncodeHeld packs a Held, for the Value of an answer that says who holds
// a key.
func EncodeHeld(h Held) []byte {
	b := binary.AppendUvarint(nil, h.ID.Session)
	b = binary.AppendUvarint(b, h.ID.Seq)
	b = binary.AppendUvarint(b, uint64(h.Coord))
	return binary.AppendVarint(b, h.Age)
}

// DecodeHeld unpacks a Held, and reports whether there was one.
func DecodeHeld(b []byte) (Held, bool) {
	r := &reader{b: b}
	h := Held{ID: TxnID{Session: r.uvarint(), Seq: r.uvarint()}, Coord: shard.GroupID(r.uvarint()), Age: r.varint()}
	return h, len(b) > 0 && !r.bad && len(r.b) == 0
}

// LockedInBy lists the Transactions that have keys of slot Locked, this
// Group's own among them.
func (m *Machine) LockedInBy(slot shard.Slot) []Held {
	var out []Held
	m.locks.Ascend(stored(slot, ""), slotEnd(slot), func(_ string, id TxnID) bool {
		if !slices.ContainsFunc(out, func(h Held) bool { return h.ID == id }) {
			p, _ := m.txns.Get(id.key())
			out = append(out, Held{ID: id, Coord: p.coord, Age: p.age})
		}
		return true
	})
	return out
}

// Holding lists the Transactions this Group has prepared and is waiting to
// be told the outcome of, in order of id. Those it coordinates itself are
// left out: it decides them.
func (m *Machine) Holding() []Held {
	var out []Held
	m.txns.Ascend("", "", func(k string, p prepared) bool {
		if p.coord != m.cfg.Group {
			out = append(out, Held{ID: idOf(k), Coord: p.coord, Age: p.age})
		}
		return true
	})
	return out
}

// LockedIn reports how many keys of slot are Locked.
func (m *Machine) LockedIn(slot shard.Slot) int {
	n := 0
	m.locks.Ascend(stored(slot, ""), slotEnd(slot), func(string, TxnID) bool {
		n++
		return true
	})
	return n
}

// holder is the Transaction that has one of a Command's keys Locked, if any
// has.
func (m *Machine) holder(cmd fsm.Command) (Held, bool) {
	var id TxnID
	held := false
	look := func(key string) {
		if !held {
			id, held = m.locks.Get(stored(m.slotOf(key), key))
		}
	}
	for _, c := range cmd.Conds {
		look(c.Key)
	}
	for _, w := range cmd.Writes {
		look(w.Key)
	}
	if !held {
		return Held{}, false
	}
	p, _ := m.txns.Get(id.key())
	return Held{ID: id, Coord: p.coord, Age: p.age}, true
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
			// The request may have been carried out as a Transaction in
			// this Group alone, before a Move split its keys.
			resp, err := fsm.DecodeResponse(m.unstore(rec.LastResp))
			if err != nil {
				return no(fsm.StatusInvalid)
			}
			return EncodeVote(VoteDone, resp)
		case step.ID.Seq < rec.LastSeq:
			return no(fsm.StatusInvalid)
		}
	}
	if !step.Lead && step.ID.Session != 0 {
		// A Group that committed a Transaction remembers it by the
		// Session's record, as it would a request of its own. A prepare
		// that arrives after the finish, sent again by a Leader that hadn't
		// heard, must not start the Transaction a second time.
		if rec, known := m.inner.SessionRecord(step.ID.Session); known && step.ID.Seq <= rec.LastSeq {
			return no(fsm.StatusInvalid)
		}
	}
	if p, again := m.txns.Get(step.ID.key()); again {
		if p.coord != step.Coord {
			// The same request, taken on by another Coordinator Group
			// after a Move, while the first hasn't finished here.
			return no(fsm.StatusTxnConflict)
		}
		return p.answer
	}

	cmd := fsm.Command{Op: fsm.OpTxn, Conds: step.Conds, Writes: step.Writes}
	slots := m.slotsOf(cmd)
	if resp, refused := m.refusal(slots...); refused {
		return no(resp.Status)
	}
	for _, s := range slots {
		// A Slot on its way out takes on no new Transaction, so that the
		// Move has only the ones already there to wait for (A§12.7).
		if m.slots[s].Status == Outgoing && !m.cfg.Unchecked {
			return no(fsm.StatusMoving)
		}
	}
	if h, held := m.holder(cmd); held {
		// The answer says who holds the key, so that whoever is older can
		// be told (A§12.5).
		return EncodeVote(VoteNo, fsm.Response{Status: fsm.StatusTxnConflict, Value: EncodeHeld(h)})
	}
	var failed []fsm.Item
	for _, c := range step.Conds {
		if found, _ := m.inner.GetRaw(stored(m.slotOf(c.Key), c.Key)); found.Version != c.Version {
			failed = append(failed, fsm.Item{Key: c.Key, Version: found.Version})
		}
	}

	p := prepared{coord: step.Coord, writes: step.Writes, stamp: step.Stamp, age: step.Age}
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
	if !found || p.coord != step.Coord {
		return ok
	}
	for _, name := range p.keys {
		m.locks = m.locks.Delete(name)
	}
	m.txns = m.txns.Delete(step.ID.key())
	// A prepare that was told no for a failed condition is no part of a
	// commit, whatever the finish says.
	// Nor is a prepare the decision didn't count.
	vote, offered, _ := DecodeVote(p.answer)
	step.Commit = step.Commit && vote == VoteYes && offered.Version == step.Offer
	m.finished = Finished{ID: step.ID, Commit: step.Commit, OK: true}
	if !step.Commit {
		return ok
	}

	cmd := fsm.Command{Op: fsm.OpTxn, Writes: p.writes, Stamp: p.stamp}
	slots := m.slotsOf(cmd)
	m.raise(index, slots, step.Version)
	m.inner.Apply(core.Entry{Index: core.Index(step.Version), Kind: core.EntryCommand, Payload: m.store(cmd).Encode()})
	m.markDirty(cmd)
	// Every Group that commits keeps the outcome with the Session: the
	// Coordinator Group to answer a retry, the others to turn away a late
	// prepare at once. The Coordinator Group has vouched for the Session
	// (A§11.6).
	m.remember(step.ID, ok.Encode(), slotsOfNames(p.keys))
	return ok
}

// Encode packs a TxnStep into an Entry's payload.
func (s TxnStep) Encode() []byte {
	b := []byte{s.op}
	b = binary.AppendUvarint(b, s.ID.Session)
	b = binary.AppendUvarint(b, s.ID.Seq)
	b = append(b, flag(s.Lead))
	b = binary.AppendUvarint(b, uint64(s.Coord))
	if s.op == opFinish {
		b = append(b, flag(s.Commit))
		b = binary.AppendUvarint(b, s.Version)
		return binary.AppendUvarint(b, s.Offer)
	}
	b = append(b, flag(s.Register))
	b = binary.AppendVarint(b, s.Age)
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
	s := TxnStep{op: payload[0], ID: TxnID{Session: r.uvarint(), Seq: r.uvarint()}, Lead: r.byte() == 1, Coord: shard.GroupID(r.uvarint())}
	if s.op == opFinish {
		s.Commit, s.Version, s.Offer = r.byte() == 1, r.uvarint(), r.uvarint()
	} else {
		s.Register, s.Age, s.Stamp = r.byte() == 1, r.varint(), r.varint()
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

// slotsOfNames lists the Slots of keys given by their stored names.
func slotsOfNames(names []string) []shard.Slot {
	var slots []shard.Slot
	for _, name := range names {
		if s := shard.Slot(name[0])<<8 | shard.Slot(name[1]); !slices.Contains(slots, s) {
			slots = append(slots, s)
		}
	}
	return slots
}

// remember puts a Transaction's answer in its Session's record, which then
// goes wherever any of slots goes. A client that gave up on the Transaction
// may have moved on: its record is never taken back to an earlier request.
func (m *Machine) remember(id TxnID, resp []byte, slots []shard.Slot) {
	if id.Session == 0 {
		return
	}
	rec, known := m.inner.SessionRecord(id.Session)
	if !known {
		rec = fsm.SessionRecord{ID: id.Session, Floor: id.Seq}
	}
	if id.Seq <= rec.LastSeq {
		return
	}
	// A Session's record holds keys under their stored names, as the
	// answers of this Group's own requests are held.
	if answer, err := fsm.DecodeResponse(resp); err == nil && len(answer.Items) > 0 {
		answer.Items = slices.Clone(answer.Items)
		for i, it := range answer.Items {
			answer.Items[i].Key = stored(m.slotOf(it.Key), it.Key)
		}
		resp = answer.Encode()
	}
	rec.LastSeq, rec.LastResp, rec.LastUsed = id.Seq, resp, m.inner.LogTime()
	m.inner.PutSessionRecord(rec)
	m.lastSlots = m.lastSlots.Put(sessionKey(id.Session), slots)
}
