package shardfsm

import (
	"bytes"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
)

func entry(index core.Index, payload []byte) core.Entry {
	return core.Entry{Index: index, Kind: core.EntryCommand, Payload: payload}
}

func respOf(t *testing.T, raw []byte) fsm.Response {
	t.Helper()
	resp, err := fsm.DecodeResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func voteOf(t *testing.T, raw []byte) (Vote, fsm.Response) {
	t.Helper()
	v, resp, err := DecodeVote(raw)
	if err != nil {
		t.Fatal(err)
	}
	return v, resp
}

// whole is a Group that owns every Slot.
func whole(cfg Config) *Machine {
	cfg.Group, cfg.Slots = 1, 4
	for s := range cfg.Slots {
		cfg.Owned = append(cfg.Owned, shard.Slot(s))
	}
	return New(cfg)
}

func TestTxnStepRoundTrip(t *testing.T) {
	steps := []TxnStep{
		PrepareStep(TxnID{7, 3}, 9, true, []fsm.Cond{{Key: "a", Version: 9}}, []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("v"), TTL: 5}, {Op: fsm.OpDelete, Key: "c"}}, 40, true),
		FinishStep(TxnID{7, 3}, 9, false, true, 99, 98),
	}
	for _, s := range steps {
		got, err := DecodeTxnStep(s.Encode())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Encode(), s.Encode()) {
			t.Errorf("round trip changed %+v into %+v", s, got)
		}
	}
	if _, err := DecodeTxnStep([]byte{opPrepare}); err == nil {
		t.Error("a truncated step decoded")
	}
}

func TestPrepareLocksAndFinishWrites(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Session: 0, Seq: 1}
	prep := PrepareStep(id, 9, false, []fsm.Cond{{Key: "a"}}, []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}, 0, false)
	vote, resp := voteOf(t, m.Apply(entry(5, prep.Encode())))
	if vote != VoteYes || resp.Version != 5 {
		t.Fatalf("prepare: vote %d, %+v", vote, resp)
	}
	if m.Locked() != 2 || len(m.Prepared()) != 1 {
		t.Fatalf("after prepare: %d Locked, %d prepared", m.Locked(), len(m.Prepared()))
	}
	// A repeat is answered the same way and Locks nothing more.
	if again := m.Apply(entry(6, prep.Encode())); !bytes.Equal(again, EncodeVote(vote, resp)) {
		t.Error("a repeated prepare was answered differently")
	}
	// Both keys are Locked: the one in a condition and the one written.
	for _, key := range []string{"a", "b"} {
		put := fsm.Command{Op: fsm.OpPut, Key: key, Value: []byte("y")}
		if got := respOf(t, m.Apply(entry(7, put.Encode()))); got.Status != fsm.StatusLocked {
			t.Errorf("put %s while Locked: %+v", key, got)
		}
		get := fsm.Command{Op: fsm.OpGet, Key: key}
		if got := respOf(t, m.Read(get.Encode())); got.Status != fsm.StatusLocked {
			t.Errorf("get %s while Locked: %+v", key, got)
		}
	}
	if got := respOf(t, m.Read(fsm.Command{Op: fsm.OpScan}.Encode())); got.Status != fsm.StatusLocked {
		t.Errorf("scan over Locked keys: %+v", got)
	}
	// Another Transaction that wants a Locked key is told no and Locks
	// nothing.
	other := PrepareStep(TxnID{0, 2}, 9, false, nil, []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("z")}, {Op: fsm.OpPut, Key: "c", Value: []byte("z")}}, 0, false)
	if v, r := voteOf(t, m.Apply(entry(8, other.Encode()))); v != VoteNo || r.Status != fsm.StatusTxnConflict || m.Locked() != 2 {
		t.Errorf("second Transaction: vote %d, %+v, %d Locked", v, r, m.Locked())
	}

	// The version comes from elsewhere and is higher than this Group's.
	m.Apply(entry(9, FinishStep(id, 9, false, true, 500, 5).Encode()))
	if m.Locked() != 0 || len(m.Prepared()) != 0 {
		t.Fatalf("after finish: %d Locked, %d prepared", m.Locked(), len(m.Prepared()))
	}
	got := respOf(t, m.Read(fsm.Command{Op: fsm.OpGet, Key: "b"}.Encode()))
	if string(got.Value) != "x" || got.Version != 500 {
		t.Errorf("b after commit: %+v", got)
	}
	// A later write to the same Slot gets a higher version still.
	put := fsm.Command{Op: fsm.OpPut, Key: "b", Value: []byte("y")}
	if got := respOf(t, m.Apply(entry(10, put.Encode()))); got.Version <= 500 {
		t.Errorf("a write after the Transaction got version %d", got.Version)
	}
}

func TestNoOffsetLetsVersionsGoDown(t *testing.T) {
	m := whole(Config{NoOffset: true})
	id := TxnID{Seq: 1}
	m.Apply(entry(5, PrepareStep(id, 9, false, nil, []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}, 0, false).Encode()))
	m.Apply(entry(6, FinishStep(id, 9, false, true, 500, 5).Encode()))
	put := fsm.Command{Op: fsm.OpPut, Key: "b", Value: []byte("y")}
	if got := respOf(t, m.Apply(entry(7, put.Encode()))); got.Version != 7 {
		t.Errorf("expected the naive version 7, got %d", got.Version)
	}
}

func TestAbortWritesNothing(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Seq: 1}
	m.Apply(entry(5, PrepareStep(id, 9, false, nil, []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}, 0, false).Encode()))
	m.Apply(entry(6, FinishStep(id, 9, false, false, 0, 0).Encode()))
	if got := respOf(t, m.Read(fsm.Command{Op: fsm.OpGet, Key: "b"}.Encode())); got.Status != fsm.StatusNotFound || m.Locked() != 0 {
		t.Errorf("after abort: %+v, %d Locked", got, m.Locked())
	}
	// A finish for a Transaction this Group never prepared changes nothing.
	m.Apply(entry(7, FinishStep(TxnID{Seq: 9}, 9, false, true, 900, 0).Encode()))
	if got := respOf(t, m.Read(fsm.Command{Op: fsm.OpGet, Key: "b"}.Encode())); got.Status != fsm.StatusNotFound {
		t.Errorf("after a stray finish: %+v", got)
	}
}

func TestFailedConditionLocksAndReports(t *testing.T) {
	m := whole(Config{})
	m.Apply(entry(3, fsm.Command{Op: fsm.OpPut, Key: "a", Value: []byte("v")}.Encode()))
	prep := PrepareStep(TxnID{Seq: 1}, 9, false, []fsm.Cond{{Key: "a", Version: 99}}, nil, 0, false)
	vote, resp := voteOf(t, m.Apply(entry(4, prep.Encode())))
	if vote != VoteNo || resp.Status != fsm.StatusVersionMismatch || len(resp.Items) != 1 || resp.Items[0].Version != 3 {
		t.Fatalf("vote %d, %+v", vote, resp)
	}
	if m.Locked() != 1 {
		t.Errorf("%d keys Locked; the condition's key must stay as reported", m.Locked())
	}
}

func TestCoordinatorGroupRemembersTheOutcome(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Session: 77, Seq: 4}
	write := []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}
	// Without leave to start a record, an unknown Session is refused.
	if v, r := voteOf(t, m.Apply(entry(5, PrepareStep(id, 9, true, nil, write, 0, false).Encode()))); v != VoteNo || r.Status != fsm.StatusSessionExpired {
		t.Fatalf("unknown Session: vote %d, %+v", v, r)
	}
	if v, _ := voteOf(t, m.Apply(entry(6, PrepareStep(id, 9, true, nil, write, 0, true).Encode()))); v != VoteYes {
		t.Fatalf("vote %d", v)
	}
	m.Apply(entry(7, FinishStep(id, 9, true, true, 6, 6).Encode()))
	// A retry, after the request was carried out, is answered as before.
	v, r := voteOf(t, m.Apply(entry(8, PrepareStep(id, 9, true, nil, write, 0, false).Encode())))
	if v != VoteDone || r.Status != fsm.StatusOK || r.Version != 6 || m.Locked() != 0 {
		t.Errorf("retry: vote %d, %+v, %d Locked", v, r, m.Locked())
	}
}

func TestSnapshotKeepsLocks(t *testing.T) {
	m := whole(Config{})
	done, held := TxnID{Seq: 1}, TxnID{Seq: 2}
	m.Apply(entry(5, PrepareStep(done, 9, false, nil, []fsm.Write{{Op: fsm.OpPut, Key: "a", Value: []byte("x")}}, 0, false).Encode()))
	m.Apply(entry(6, FinishStep(done, 9, false, true, 500, 5).Encode()))
	_, offered := voteOf(t, m.Apply(entry(7, PrepareStep(held, 9, false, nil, []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("y")}}, 0, false).Encode())))

	next := whole(Config{})
	if err := next.Restore(m.Capture()()); err != nil {
		t.Fatal(err)
	}
	if next.Locked() != 1 || len(next.Prepared()) != 1 || next.Prepared()[0] != held {
		t.Fatalf("restored: %d Locked, prepared %v", next.Locked(), next.Prepared())
	}
	if !bytes.Equal(next.Capture()(), m.Capture()()) {
		t.Error("a restored Machine captures differently")
	}
	// The restored Machine finishes the Transaction and keeps versions rising.
	next.Apply(entry(8, FinishStep(held, 9, false, true, 600, offered.Version).Encode()))
	if got := respOf(t, next.Read(fsm.Command{Op: fsm.OpGet, Key: "b"}.Encode())); string(got.Value) != "y" || got.Version != 600 {
		t.Errorf("b after restore and commit: %+v", got)
	}
	put := fsm.Command{Op: fsm.OpPut, Key: "a", Value: []byte("z")}
	if got := respOf(t, next.Apply(entry(9, put.Encode()))); got.Version <= 500 {
		t.Errorf("a write after restore got version %d", got.Version)
	}
}

func stateOf(t *testing.T, raw []byte) (TxnState, fsm.Response) {
	t.Helper()
	s, resp, err := DecodeState(raw)
	if err != nil {
		t.Fatal(err)
	}
	return s, resp
}

func outcomeOf(t *testing.T, raw []byte) (TxnState, fsm.Response) {
	t.Helper()
	s, resp, _, err := DecodeOutcome(raw)
	if err != nil {
		t.Fatal(err)
	}
	return s, resp
}

func TestCoordStepRoundTrip(t *testing.T) {
	id := TxnID{7, 3}
	parts := []Part{
		{Group: 1, Conds: []fsm.Cond{{Key: "a", Version: 9}}},
		{Group: 2, Writes: []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("v"), TTL: 5}}},
	}
	for _, s := range []CoordStep{
		TxnBeginStep(id, parts, 40, true),
		DecideStep(id, false, 0, nil, []fsm.Item{{Key: "a", Version: 4}}),
		DecideStep(id, true, 99, []Offer{{Group: 2, Version: 41}}, nil),
		ConfirmStep(id, 2),
	} {
		got, err := DecodeCoordStep(s.Encode())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Encode(), s.Encode()) {
			t.Errorf("round trip changed %+v into %+v", s, got)
		}
	}
}

// twoGroups is a Transaction that writes a in Group 1 and b in Group 2,
// put to Group 1.
func twoGroups(id TxnID, conds []fsm.Cond) CoordStep {
	return TxnBeginStep(id, []Part{
		{Group: 1, Conds: conds, Writes: []fsm.Write{{Op: fsm.OpPut, Key: "a", Value: []byte("x")}}},
		{Group: 2, Writes: []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}},
	}, 0, true)
}

func TestCoordinatorGroupCarriesATransactionThrough(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Session: 77, Seq: 4}
	if s, _ := stateOf(t, m.Apply(entry(5, twoGroups(id, nil).Encode()))); s != TxnBegun {
		t.Fatalf("begin: state %d", s)
	}
	open := m.Coordinating()
	if len(open) != 1 || open[0].Own != VoteYes || open[0].OwnResp.Version != 5 || len(open[0].Parts) != 1 || open[0].Parts[0].Group != 2 {
		t.Fatalf("coordinating: %+v", open)
	}
	if m.Locked() != 1 {
		t.Fatalf("%d keys Locked after begin", m.Locked())
	}
	// A repeat changes nothing, and asking gets the same answer.
	if s, _ := stateOf(t, m.Apply(entry(6, twoGroups(id, nil).Encode()))); s != TxnBegun || m.Records() != 1 {
		t.Errorf("repeated begin: state %d, %d records", s, m.Records())
	}
	if s, _ := outcomeOf(t, m.Read(OutcomeQuery(id, 0))); s != TxnBegun {
		t.Errorf("outcome before the decision: state %d", s)
	}

	s, resp := stateOf(t, m.Apply(entry(7, DecideStep(id, true, 900, nil, nil).Encode())))
	if s != TxnDecided || resp.Status != fsm.StatusOK || resp.Version != 900 {
		t.Fatalf("decision: state %d, %+v", s, resp)
	}
	if got := respOf(t, m.Read(fsm.Command{Op: fsm.OpGet, Key: "a"}.Encode())); string(got.Value) != "x" || got.Version != 900 {
		t.Errorf("a after the decision: %+v", got)
	}
	// A second decision, from a Leader that hadn't heard, changes nothing.
	if s, resp := stateOf(t, m.Apply(entry(8, DecideStep(id, false, 0, nil, nil).Encode()))); s != TxnDecided || resp.Status != fsm.StatusOK {
		t.Errorf("second decision: state %d, %+v", s, resp)
	}
	if open := m.Coordinating(); len(open) != 1 || !open[0].Decided || !open[0].Commit || open[0].Version != 900 {
		t.Fatalf("after the decision: %+v", open)
	}
	m.Apply(entry(9, ConfirmStep(id, 2).Encode()))
	if len(m.Coordinating()) != 0 || m.Records() != 1 {
		t.Errorf("after every Group confirmed: %d open, %d records", len(m.Coordinating()), m.Records())
	}
	// A retry of the request is answered from the Session's record.
	retry := twoGroups(id, nil)
	retry.Register = false
	if s, resp := stateOf(t, m.Apply(entry(10, retry.Encode()))); s != TxnDecided || resp.Version != 900 {
		t.Errorf("retry: state %d, %+v", s, resp)
	}
}

func TestCoordinatorGroupRefusesWhatItCannotStart(t *testing.T) {
	m := whole(Config{})
	held := TxnID{Session: 77, Seq: 1}
	m.Apply(entry(5, twoGroups(held, nil).Encode()))
	// A second Transaction wants the key the first has Locked.
	s, resp := stateOf(t, m.Apply(entry(6, twoGroups(TxnID{Session: 78, Seq: 1}, nil).Encode())))
	if s != TxnRefused || resp.Status != fsm.StatusTxnConflict || m.Records() != 1 || m.Locked() != 1 {
		t.Errorf("state %d, %+v, %d records, %d Locked", s, resp, m.Records(), m.Locked())
	}
	// A begin put to a Group that isn't first among its parts is refused.
	wrong := TxnBeginStep(TxnID{Session: 79, Seq: 1}, []Part{{Group: 2}, {Group: 1}}, 0, true)
	if s, resp := stateOf(t, m.Apply(entry(7, wrong.Encode()))); s != TxnRefused || resp.Status != fsm.StatusWrongGroup {
		t.Errorf("wrong Group: state %d, %+v", s, resp)
	}
}

func TestFailedConditionsAreReportedTogether(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Session: 77, Seq: 1}
	// The Coordinator Group's own condition fails. It still takes the
	// Transaction on, to hear what the other Group found.
	if s, _ := stateOf(t, m.Apply(entry(5, twoGroups(id, []fsm.Cond{{Key: "a", Version: 3}}).Encode()))); s != TxnBegun {
		t.Fatalf("begin: state %d", s)
	}
	if open := m.Coordinating(); open[0].Own != VoteNo || open[0].OwnResp.Status != fsm.StatusVersionMismatch {
		t.Fatalf("own answer: %+v", open[0])
	}
	// Even told to commit, it doesn't: its own answer was no.
	s, resp := stateOf(t, m.Apply(entry(6, DecideStep(id, true, 900, nil, []fsm.Item{{Key: "b", Version: 8}}).Encode())))
	if s != TxnDecided || resp.Status != fsm.StatusVersionMismatch || len(resp.Items) != 2 || resp.Items[0].Key != "a" || resp.Items[1].Key != "b" {
		t.Errorf("decision: state %d, %+v", s, resp)
	}
	if got := respOf(t, m.Read(fsm.Command{Op: fsm.OpGet, Key: "a"}.Encode())); got.Status != fsm.StatusNotFound || m.Locked() != 0 {
		t.Errorf("after the abort: %+v, %d Locked", got, m.Locked())
	}
}

func TestLatePrepareAfterCommitIsRefused(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Session: 77, Seq: 4}
	prep := PrepareStep(id, 9, false, nil, []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}, 0, false)
	m.Apply(entry(5, prep.Encode()))
	m.Apply(entry(6, FinishStep(id, 9, false, true, 900, 5).Encode()))
	m.Apply(entry(7, fsm.Command{Op: fsm.OpPut, Key: "b", Value: []byte("newer")}.Encode()))
	// The same prepare again, sent by a Leader that hadn't heard.
	if v, r := voteOf(t, m.Apply(entry(8, prep.Encode()))); v != VoteNo || r.Status != fsm.StatusInvalid || m.Locked() != 0 {
		t.Fatalf("late prepare: vote %d, %+v, %d Locked", v, r, m.Locked())
	}
	m.Apply(entry(9, FinishStep(id, 9, false, true, 900, 5).Encode()))
	if got := respOf(t, m.Read(fsm.Command{Op: fsm.OpGet, Key: "b"}.Encode())); string(got.Value) != "newer" {
		t.Errorf("b was written a second time: %+v", got)
	}
}

func TestSnapshotKeepsCoordinatedTransactions(t *testing.T) {
	m := whole(Config{})
	done, open := TxnID{Session: 77, Seq: 1}, TxnID{Session: 78, Seq: 1}
	m.Apply(entry(5, twoGroups(done, nil).Encode()))
	m.Apply(entry(6, DecideStep(done, true, 900, nil, nil).Encode()))
	m.Apply(entry(7, ConfirmStep(done, 2).Encode()))
	m.Apply(entry(8, TxnBeginStep(open, []Part{{Group: 1, Writes: []fsm.Write{{Op: fsm.OpPut, Key: "c", Value: []byte("y")}}}, {Group: 3}}, 0, true).Encode()))

	next := whole(Config{})
	if err := next.Restore(m.Capture()()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(next.Capture()(), m.Capture()()) {
		t.Error("a restored Machine captures differently")
	}
	if got := next.Coordinating(); next.Records() != 2 || len(got) != 1 || got[0].ID != open || got[0].Own != VoteYes {
		t.Errorf("restored: %d records, coordinating %+v", next.Records(), got)
	}
	if s, resp := outcomeOf(t, next.Read(OutcomeQuery(done, 0))); s != TxnDecided || resp.Version != 900 {
		t.Errorf("restored outcome: state %d, %+v", s, resp)
	}
}

func TestAMoveWaitsForLocksAndTakesNoNewOnes(t *testing.T) {
	m := whole(Config{})
	slot := m.slotOf("b")
	held := TxnID{Session: 77, Seq: 1}
	write := []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}
	m.Apply(entry(5, PrepareStep(held, 9, false, nil, write, 0, false).Encode()))
	if got := m.Holding(); len(got) != 1 || got[0].Coord != 9 || m.LockedIn(slot) != 1 {
		t.Fatalf("holding %+v, %d Locked in the Slot", got, m.LockedIn(slot))
	}

	// The Move begins. A new Transaction is turned away from the Slot.
	m.Apply(entry(6, BeginStep(slot, 0, 2).Encode()))
	other := PrepareStep(TxnID{Session: 78, Seq: 1}, 9, false, nil, []fsm.Write{{Op: fsm.OpPut, Key: "b2", Value: []byte("y")}}, 0, false)
	for m.slotOf(other.Writes[0].Key) != slot {
		other.Writes[0].Key += "x"
	}
	if v, r := voteOf(t, m.Apply(entry(7, other.Encode()))); v != VoteNo || r.Status != fsm.StatusMoving {
		t.Errorf("prepare for a Slot on its way out: vote %d, %+v", v, r)
	}
	// The Slot isn't frozen while a key in it is Locked.
	if r := respOf(t, m.Apply(entry(8, FreezeStep(slot, 0).Encode()))); r.Status != fsm.StatusLocked || m.Slots()[slot].Status != Outgoing {
		t.Fatalf("freeze with a Lock: %+v, Slot %+v", r, m.Slots()[slot])
	}
	// The Transaction finishes, its write is sent on with the rest, and
	// the Slot can go.
	m.Apply(entry(9, FinishStep(held, 9, false, true, 500, 5).Encode()))
	if r := respOf(t, m.Apply(entry(10, FreezeStep(slot, 0).Encode()))); r.Status != fsm.StatusOK {
		t.Fatalf("freeze after the finish: %+v", r)
	}
	final, frozen := m.Final(slot, true)
	if !frozen || len(final.Upserts) != 1 || final.Upserts[0].Version != 500 {
		t.Fatalf("what the Slot takes with it: %+v", final)
	}

	// Where it arrives, the offset starts again and versions still rise.
	to := New(Config{Group: 2, Slots: 4})
	to.Apply(entry(3, AcceptStep(slot, 1, 1, final).Encode()))
	if info := to.Slots()[slot]; info.Status != Owned || info.VOffset != 0 {
		t.Fatalf("the Slot where it arrived: %+v", info)
	}
	if got := respOf(t, to.Apply(entry(4, fsm.Command{Op: fsm.OpPut, Key: "b", Value: []byte("z")}.Encode()))); got.Version <= 500 {
		t.Errorf("a write after the Move got version %d", got.Version)
	}
}

func TestAFinishFromAnotherCoordinatorGroupIsIgnored(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Session: 77, Seq: 1}
	write := []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}
	m.Apply(entry(5, PrepareStep(id, 9, false, nil, write, 0, false).Encode()))
	// The same request, taken on by another Group after a Move.
	if v, r := voteOf(t, m.Apply(entry(6, PrepareStep(id, 8, false, nil, write, 0, false).Encode()))); v != VoteNo || r.Status != fsm.StatusTxnConflict {
		t.Errorf("prepare from a second Coordinator Group: vote %d, %+v", v, r)
	}
	m.Apply(entry(7, FinishStep(id, 8, false, false, 0, 0).Encode()))
	if m.Locked() != 1 {
		t.Fatal("a finish from a Group that doesn't coordinate the Transaction released its Locks")
	}
	m.Apply(entry(8, FinishStep(id, 9, false, true, 500, 5).Encode()))
	if got := respOf(t, m.Read(fsm.Command{Op: fsm.OpGet, Key: "b"}.Encode())); string(got.Value) != "x" {
		t.Errorf("b after its own Coordinator Group's finish: %+v", got)
	}
}

func TestARetryAfterAMoveSplitTheKeysIsAnsweredAsBefore(t *testing.T) {
	m := whole(Config{})
	// The request is first carried out as a Transaction in one Group, and
	// its condition fails.
	cmd := fsm.Command{Op: fsm.OpTxn, Session: 77, Seq: 1, Register: true, Conds: []fsm.Cond{{Key: "a", Version: 3}}, Writes: []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}}
	first := respOf(t, m.Apply(entry(5, cmd.Encode())))
	// Its keys are then in two Groups, and the retry comes as a begin.
	begin := TxnBeginStep(TxnID{Session: 77, Seq: 1}, []Part{{Group: 1, Conds: cmd.Conds}, {Group: 2, Writes: cmd.Writes}}, 0, false)
	s, again := stateOf(t, m.Apply(entry(6, begin.Encode())))
	if s != TxnDecided || again.Status != first.Status || len(again.Items) != 1 || again.Items[0].Key != "a" {
		t.Errorf("retry: state %d, %+v; first answer %+v", s, again, first)
	}
}

func TestAFinishCommitsOnlyThePrepareItCounted(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Seq: 4} // no Session: the Group has nothing else to know it by
	prep := PrepareStep(id, 9, false, nil, []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}, 0, false)
	_, first := voteOf(t, m.Apply(entry(5, prep.Encode())))
	m.Apply(entry(6, FinishStep(id, 9, false, true, 900, first.Version).Encode()))
	m.Apply(entry(7, fsm.Command{Op: fsm.OpPut, Key: "b", Value: []byte("newer")}.Encode()))
	// The same prepare arrives again, long after, and Locks the key anew.
	if v, second := voteOf(t, m.Apply(entry(8, prep.Encode()))); v != VoteYes || second.Version == first.Version {
		t.Fatalf("late prepare: vote %d, offer %d, first offer %d", v, second.Version, first.Version)
	}
	// The Group asks, and is told the Transaction committed on its first
	// offer. It lets go and writes nothing.
	m.Apply(entry(9, FinishStep(id, 9, false, true, 900, first.Version).Encode()))
	if got := respOf(t, m.Read(fsm.Command{Op: fsm.OpGet, Key: "b"}.Encode())); string(got.Value) != "newer" || m.Locked() != 0 {
		t.Errorf("b after the second finish: %+v, %d Locked", got, m.Locked())
	}
	if f := m.LastFinished(); !f.OK || f.Commit {
		t.Errorf("the second finish counted as %+v", f)
	}
}

func TestTheCoordinatorGroupTellsAGroupItsOffer(t *testing.T) {
	m := whole(Config{})
	id := TxnID{Session: 77, Seq: 4}
	m.Apply(entry(5, twoGroups(id, nil).Encode()))
	m.Apply(entry(6, DecideStep(id, true, 900, []Offer{{Group: 2, Version: 41}}, nil).Encode()))
	s, resp, offer, err := DecodeOutcome(m.Read(OutcomeQuery(id, 2)))
	if err != nil || s != TxnDecided || resp.Version != 900 || offer != 41 {
		t.Errorf("outcome for Group 2: state %d, %+v, offer %d, %v", s, resp, offer, err)
	}
	if open := m.Coordinating(); open[0].Offer(2) != 41 || open[0].Offer(3) != 0 {
		t.Errorf("offers: %+v", open[0].Offers)
	}
}

func TestAConflictSaysWhoHoldsTheKey(t *testing.T) {
	m := whole(Config{})
	write := []fsm.Write{{Op: fsm.OpPut, Key: "b", Value: []byte("x")}}
	m.Apply(entry(5, PrepareStep(TxnID{Session: 77, Seq: 1}, 9, false, nil, write, 300, false).Aged(120).Encode()))
	v, r := voteOf(t, m.Apply(entry(6, PrepareStep(TxnID{Session: 78, Seq: 1}, 8, false, nil, write, 300, false).Encode())))
	h, named := DecodeHeld(r.Value)
	if v != VoteNo || r.Status != fsm.StatusTxnConflict || !named || h != (Held{ID: TxnID{Session: 77, Seq: 1}, Coord: 9, Age: 120}) {
		t.Fatalf("vote %d, %+v, holder %+v", v, r, h)
	}
	// Older is decided by age, then Session, then request number, and of
	// two Transactions exactly one is the older.
	a, b := Held{ID: TxnID{7, 1}, Age: 100}, Held{ID: TxnID{3, 9}, Age: 200}
	c := Held{ID: TxnID{3, 9}, Age: 100}
	if !a.Older(b) || b.Older(a) || !c.Older(a) || a.Older(c) || a.Older(a) {
		t.Error("ages don't order Transactions")
	}
}
