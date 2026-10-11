package shardfsm

import (
	"encoding/binary"
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
)

// The steps a Coordinator Group takes for a Transaction across Groups, as
// Entries in its own Log (A§12.2). A new Leader finds in them everything it
// needs to carry the Transaction on.
const (
	// opTxnBegin: the Group takes on a Transaction. It is also the Group's
	// own prepare.
	opTxnBegin = opFinish + 1 + iota
	// opDecide: the Transaction commits or aborts. It is also the Group's
	// own finish.
	opDecide
	// opConfirm: another Group has finished the Transaction.
	opConfirm
	// opOutcome is not an Entry: it is a read, asking what was decided.
	opOutcome
)

// Part is one Group's share of a Transaction.
type Part struct {
	Group  shard.GroupID
	Conds  []fsm.Cond
	Writes []fsm.Write
}

// CoordStep is one step of a Transaction, as proposed to its Coordinator
// Group.
type CoordStep struct {
	op byte
	ID TxnID

	// For a begin: every Group's part, the Coordinator Group's own first,
	// the Store time the writes are stamped with, and whether the request
	// may start a record of its Session.
	Parts    []Part
	Stamp    int64
	Register bool

	// For a decision: whether the Transaction commits, the version its
	// writes get if it does, and the conditions the other Groups found
	// failed if it doesn't.
	Commit  bool
	Version uint64
	Failed  []fsm.Item

	// For a confirmation: the Group that has finished.
	Group shard.GroupID
}

// TxnBeginStep asks a Group to coordinate a Transaction.
func TxnBeginStep(id TxnID, parts []Part, stamp int64, register bool) CoordStep {
	return CoordStep{op: opTxnBegin, ID: id, Parts: parts, Stamp: stamp, Register: register}
}

// DecideStep records a Transaction's outcome.
func DecideStep(id TxnID, commit bool, version uint64, failed []fsm.Item) CoordStep {
	return CoordStep{op: opDecide, ID: id, Commit: commit, Version: version, Failed: failed}
}

// ConfirmStep records that a Group has finished a Transaction.
func ConfirmStep(id TxnID, g shard.GroupID) CoordStep {
	return CoordStep{op: opConfirm, ID: id, Group: g}
}

// OutcomeQuery is a read that asks a Coordinator Group what it decided.
func OutcomeQuery(id TxnID) []byte {
	return CoordStep{op: opOutcome, ID: id}.Encode()
}

// TxnState is where a Transaction stands in its Coordinator Group.
type TxnState uint8

const (
	// TxnNone: the Group has no record of the Transaction.
	TxnNone TxnState = iota + 1
	// TxnBegun: it is under way and not decided.
	TxnBegun
	// TxnDecided: the Response is the client's answer. A Transaction that
	// was aborted for a reason other than its conditions has the status
	// fsm.StatusTxnConflict.
	TxnDecided
	// TxnRefused: the Group wouldn't take the Transaction on, and holds
	// nothing for it. The Response says why.
	TxnRefused
)

// EncodeState packs a TxnState with its Response.
func EncodeState(s TxnState, resp fsm.Response) []byte {
	return append([]byte{byte(s)}, resp.Encode()...)
}

// DecodeState unpacks the answer to a begin, a decision or an OutcomeQuery.
func DecodeState(b []byte) (TxnState, fsm.Response, error) {
	if len(b) == 0 || b[0] < byte(TxnNone) || b[0] > byte(TxnRefused) {
		return 0, fsm.Response{}, errMalformed
	}
	resp, err := fsm.DecodeResponse(b[1:])
	return TxnState(b[0]), resp, err
}

// coordinated is what a Coordinator Group keeps of a Transaction.
type coordinated struct {
	// parts are the other Groups' parts.
	parts   []Part
	stamp   int64
	decided bool
	commit  bool
	version uint64
	// outcome is the client's answer, encoded, once decided.
	outcome   []byte
	confirmed []shard.GroupID
}

func (r coordinated) open() bool { return !r.decided || len(r.confirmed) < len(r.parts) }

func (r coordinated) state() []byte {
	if !r.decided {
		return EncodeState(TxnBegun, fsm.Response{})
	}
	return append([]byte{byte(TxnDecided)}, r.outcome...)
}

// Coordinated is a Transaction this Group coordinates that isn't over: not
// decided, or decided and not yet finished by every Group.
type Coordinated struct {
	ID TxnID
	// Parts are the other Groups' parts, and Stamp the Store time of the
	// begin.
	Parts []Part
	Stamp int64
	// Own is this Group's own answer to its part.
	Own     Vote
	OwnResp fsm.Response

	Decided   bool
	Commit    bool
	Version   uint64
	Confirmed []shard.GroupID
}

// Coordinating lists the Transactions this Group coordinates that aren't
// over, in order of id.
func (m *Machine) Coordinating() []Coordinated {
	var out []Coordinated
	m.openTxns.Ascend("", "", func(k string, _ struct{}) bool {
		r, _ := m.coord.Get(k)
		c := Coordinated{ID: idOf(k), Parts: r.parts, Stamp: r.stamp, Decided: r.decided, Commit: r.commit, Version: r.version, Confirmed: r.confirmed}
		if p, held := m.txns.Get(k); held {
			c.Own, c.OwnResp, _ = DecodeVote(p.answer)
		}
		out = append(out, c)
		return true
	})
	return out
}

// Records is how many Transactions this Group has a record of deciding.
func (m *Machine) Records() int { return m.coord.Len() }

func idOf(k string) TxnID {
	return TxnID{Session: binary.BigEndian.Uint64([]byte(k[:8])), Seq: binary.BigEndian.Uint64([]byte(k[8:]))}
}

func (m *Machine) applyCoord(index core.Index, payload []byte) []byte {
	st, err := DecodeCoordStep(payload)
	if err != nil || st.op == opOutcome {
		return fsm.Response{Status: fsm.StatusInvalid}.Encode()
	}
	key := st.ID.key()
	rec, known := m.coord.Get(key)
	switch st.op {
	case opTxnBegin:
		if known {
			return rec.state()
		}
		if len(st.Parts) == 0 || st.Parts[0].Group != m.cfg.Group {
			return EncodeState(TxnRefused, fsm.Response{Status: fsm.StatusWrongGroup})
		}
		own := st.Parts[0]
		vote, resp, _ := DecodeVote(m.prepare(index, PrepareStep(st.ID, true, own.Conds, own.Writes, st.Stamp, st.Register)))
		switch {
		case vote == VoteDone:
			return EncodeState(TxnDecided, resp)
		case vote == VoteNo && resp.Status != fsm.StatusVersionMismatch:
			return EncodeState(TxnRefused, resp)
		}
		rec = coordinated{parts: st.Parts[1:], stamp: st.Stamp}
		m.coord = m.coord.Put(key, rec)
		m.openTxns = m.openTxns.Put(key, struct{}{})
		return rec.state()

	case opDecide:
		if !known || rec.decided {
			if !known {
				return EncodeState(TxnNone, fsm.Response{})
			}
			return rec.state()
		}
		// The Group's own answer has the last word on its own part.
		var own fsm.Response
		vote := VoteNo
		if p, held := m.txns.Get(key); held {
			vote, own, _ = DecodeVote(p.answer)
		}
		failed := st.Failed
		if vote == VoteNo && own.Status == fsm.StatusVersionMismatch {
			failed = append(slices.Clone(own.Items), failed...)
		}
		rec.decided, rec.commit, rec.version = true, st.Commit && vote == VoteYes, st.Version
		switch {
		case rec.commit:
			rec.outcome = fsm.Response{Status: fsm.StatusOK, Version: st.Version}.Encode()
		case len(failed) > 0:
			rec.outcome = fsm.Response{Status: fsm.StatusVersionMismatch, Items: failed}.Encode()
		default:
			rec.outcome = fsm.Response{Status: fsm.StatusTxnConflict}.Encode()
		}
		m.finish(index, FinishStep(st.ID, true, rec.commit, st.Version))

	case opConfirm:
		if !known || !rec.decided || slices.Contains(rec.confirmed, st.Group) {
			return fsm.Response{Status: fsm.StatusOK}.Encode()
		}
		if !slices.ContainsFunc(rec.parts, func(p Part) bool { return p.Group == st.Group }) {
			return fsm.Response{Status: fsm.StatusInvalid}.Encode()
		}
		rec.confirmed = append(slices.Clone(rec.confirmed), st.Group)
	}
	m.coord = m.coord.Put(key, rec)
	if !rec.open() {
		m.openTxns = m.openTxns.Delete(key)
	}
	return rec.state()
}

// outcome answers an OutcomeQuery.
func (m *Machine) outcome(query []byte) []byte {
	st, err := DecodeCoordStep(query)
	if err != nil {
		return fsm.Response{Status: fsm.StatusInvalid}.Encode()
	}
	if rec, known := m.coord.Get(st.ID.key()); known {
		return rec.state()
	}
	return EncodeState(TxnNone, fsm.Response{})
}

func appendParts(b []byte, parts []Part) []byte {
	b = binary.AppendUvarint(b, uint64(len(parts)))
	for _, p := range parts {
		b = binary.AppendUvarint(b, uint64(p.Group))
		b = appendBytes(b, TxnStep{op: opPrepare, Conds: p.Conds, Writes: p.Writes}.Encode())
	}
	return b
}

func (r *reader) parts() []Part {
	var parts []Part
	for range r.count() {
		g := shard.GroupID(r.uvarint())
		step, err := DecodeTxnStep(r.bytes())
		if err != nil {
			r.bad = true
			return nil
		}
		parts = append(parts, Part{Group: g, Conds: step.Conds, Writes: step.Writes})
	}
	return parts
}

func appendItems(b []byte, items []fsm.Item) []byte {
	b = binary.AppendUvarint(b, uint64(len(items)))
	for _, it := range items {
		b = appendBytes(b, []byte(it.Key))
		b = binary.AppendUvarint(b, it.Version)
	}
	return b
}

func (r *reader) items() []fsm.Item {
	var items []fsm.Item
	for range r.count() {
		items = append(items, fsm.Item{Key: string(r.bytes()), Version: r.uvarint()})
	}
	return items
}

// Encode packs a CoordStep into an Entry's payload, or a read's query.
func (s CoordStep) Encode() []byte {
	b := []byte{s.op}
	b = binary.AppendUvarint(b, s.ID.Session)
	b = binary.AppendUvarint(b, s.ID.Seq)
	switch s.op {
	case opTxnBegin:
		b = append(b, flag(s.Register))
		b = binary.AppendVarint(b, s.Stamp)
		b = appendParts(b, s.Parts)
	case opDecide:
		b = append(b, flag(s.Commit))
		b = binary.AppendUvarint(b, s.Version)
		b = appendItems(b, s.Failed)
	case opConfirm:
		b = binary.AppendUvarint(b, uint64(s.Group))
	}
	return b
}

// DecodeCoordStep unpacks a CoordStep.
func DecodeCoordStep(payload []byte) (CoordStep, error) {
	if len(payload) == 0 || payload[0] < opTxnBegin || payload[0] > opOutcome {
		return CoordStep{}, errMalformed
	}
	r := &reader{b: payload[1:]}
	s := CoordStep{op: payload[0], ID: TxnID{Session: r.uvarint(), Seq: r.uvarint()}}
	switch s.op {
	case opTxnBegin:
		s.Register, s.Stamp = r.byte() == 1, r.varint()
		s.Parts = r.parts()
	case opDecide:
		s.Commit, s.Version = r.byte() == 1, r.uvarint()
		s.Failed = r.items()
	case opConfirm:
		s.Group = shard.GroupID(r.uvarint())
	}
	if r.bad || len(r.b) != 0 {
		return CoordStep{}, errMalformed
	}
	return s, nil
}

// appendCoord packs the Transactions a Group coordinates, for a Snapshot.
func appendCoord(b []byte, m *Machine) []byte {
	b = binary.AppendUvarint(b, uint64(m.coord.Len()))
	m.coord.Ascend("", "", func(k string, rec coordinated) bool {
		b = append(b, k...) // always 16 bytes
		b = appendParts(b, rec.parts)
		b = binary.AppendVarint(b, rec.stamp)
		b = append(b, flag(rec.decided), flag(rec.commit))
		b = binary.AppendUvarint(b, rec.version)
		b = appendBytes(b, rec.outcome)
		b = binary.AppendUvarint(b, uint64(len(rec.confirmed)))
		for _, g := range rec.confirmed {
			b = binary.AppendUvarint(b, uint64(g))
		}
		return true
	})
	return b
}

func (m *Machine) restoreCoord(r *reader) {
	for range r.count() {
		if len(r.b) < 16 {
			r.bad = true
			return
		}
		k := string(r.b[:16])
		r.b = r.b[16:]
		rec := coordinated{parts: r.parts(), stamp: r.varint(), decided: r.byte() == 1, commit: r.byte() == 1, version: r.uvarint(), outcome: r.bytes()}
		for range r.count() {
			rec.confirmed = append(rec.confirmed, shard.GroupID(r.uvarint()))
		}
		m.coord = m.coord.Put(k, rec)
		if rec.open() {
			m.openTxns = m.openTxns.Put(k, struct{}{})
		}
	}
}
