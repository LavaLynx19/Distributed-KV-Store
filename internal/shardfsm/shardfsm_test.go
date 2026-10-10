package shardfsm

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
)

const slots = 4

// group is a Machine with the index of its next Entry.
type group struct {
	*Machine
	next core.Index
}

func newGroup(id shard.GroupID, owned ...shard.Slot) *group {
	return &group{Machine: New(Config{Group: id, Slots: slots, Owned: owned}), next: 1}
}

func (g *group) apply(t *testing.T, payload []byte) fsm.Response {
	t.Helper()
	r, err := fsm.DecodeResponse(g.Apply(core.Entry{Index: g.next, Kind: core.EntryCommand, Payload: payload}))
	if err != nil {
		t.Fatal(err)
	}
	g.next++
	return r
}

func (g *group) do(t *testing.T, cmd fsm.Command) fsm.Response { return g.apply(t, cmd.Encode()) }
func (g *group) step(t *testing.T, s Step) fsm.Response        { return g.apply(t, s.Encode()) }

// keyIn returns the n-th key of the form k<i> that falls in slot.
func keyIn(slot shard.Slot, n int) string {
	for i := 0; ; i++ {
		if k := fmt.Sprintf("k%d", i); shard.SlotOf(k, slots) == slot {
			if n == 0 {
				return k
			}
			n--
		}
	}
}

func put(k, v string) fsm.Command {
	return fsm.Command{Op: fsm.OpPut, Key: k, Value: []byte(v)}
}
func get(k string) fsm.Command { return fsm.Command{Op: fsm.OpGet, Key: k} }
func del(k string) fsm.Command { return fsm.Command{Op: fsm.OpDelete, Key: k} }

func read(t *testing.T, g *group, k string) fsm.Response {
	t.Helper()
	r, err := fsm.DecodeResponse(g.Read(get(k).Encode()))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestAGroupAnswersOnlyForItsSlots(t *testing.T) {
	g := newGroup(1, 0, 2)
	mine, theirs := keyIn(0, 0), keyIn(1, 0)
	if r := g.do(t, put(mine, "1")); r.Status != fsm.StatusOK {
		t.Fatalf("a key in a Slot it owns: %+v", r)
	}
	if r := g.do(t, put(theirs, "1")); r.Status != fsm.StatusWrongGroup {
		t.Fatalf("a write to a Slot it doesn't own: %+v", r)
	}
	if r := read(t, g, theirs); r.Status != fsm.StatusWrongGroup {
		t.Fatalf("a read of a Slot it doesn't own: %+v", r)
	}
	if r := read(t, g, mine); r.Status != fsm.StatusOK || string(r.Value) != "1" {
		t.Fatalf("a read of its own key: %+v", r)
	}
	// The naive Machine answers for anything.
	naive := &group{Machine: New(Config{Group: 1, Slots: slots, Owned: []shard.Slot{0}, Unchecked: true}), next: 1}
	if r := naive.do(t, put(theirs, "1")); r.Status != fsm.StatusOK {
		t.Fatalf("the unchecked Machine refused: %+v", r)
	}
}

// move carries Slot s from a to b, with writes landing on a at every stage
// where it still serves, and returns the keys used.
func move(t *testing.T, a, b *group, s shard.Slot, epoch uint32) {
	t.Helper()
	if r := a.step(t, BeginStep(s, epoch, b.cfg.Group)); r.Status != fsm.StatusOK {
		t.Fatalf("begin: %+v", r)
	}
	for after, done := "", false; !done; {
		var chunk []fsm.Raw
		chunk, done = a.Chunk(s, after, 2)
		if len(chunk) > 0 {
			after = chunk[len(chunk)-1].Key
		}
		if r := b.step(t, IncomingStep(s, epoch+1, a.cfg.Group, Transfer{Upserts: chunk})); r.Status != fsm.StatusOK {
			t.Fatalf("incoming: %+v", r)
		}
	}
	if r := a.step(t, FreezeStep(s, epoch)); r.Status != fsm.StatusOK {
		t.Fatalf("freeze: %+v", r)
	}
	final, ok := a.Final(s, false)
	if !ok {
		t.Fatal("Final refused a Frozen Slot")
	}
	if r := b.step(t, AcceptStep(s, epoch+1, a.cfg.Group, final)); r.Status != fsm.StatusOK {
		t.Fatalf("accept: %+v", r)
	}
	if r := a.step(t, DropStep(s, epoch)); r.Status != fsm.StatusOK {
		t.Fatalf("drop: %+v", r)
	}
}

func TestMoveWithWritesDuringTheCopy(t *testing.T) {
	a, b := newGroup(1, 0, 1), newGroup(2, 2, 3)
	k := func(n int) string { return keyIn(0, n) }
	for i := range 6 {
		a.do(t, put(k(i), "old"))
	}
	other := keyIn(1, 0)
	a.do(t, put(other, "stays"))

	a.step(t, BeginStep(0, 0, 2))
	// The first chunk goes, and then the Slot changes under the copy.
	chunk, _ := a.Chunk(0, "", 3)
	b.step(t, IncomingStep(0, 1, 1, Transfer{Upserts: chunk}))
	a.do(t, put(chunk[0].Key, "changed after it was sent"))
	a.do(t, del(chunk[1].Key))
	a.do(t, put(k(9), "created during the copy"))
	rest, done := a.Chunk(0, chunk[2].Key, 100)
	if !done {
		t.Fatal("the second chunk should finish the Slot")
	}
	b.step(t, IncomingStep(0, 1, 1, Transfer{Upserts: rest}))

	// Before the freeze A serves and B doesn't.
	if r := read(t, a, chunk[2].Key); r.Status != fsm.StatusOK {
		t.Fatalf("the source during the copy: %+v", r)
	}
	if r := read(t, b, chunk[2].Key); r.Status != fsm.StatusWrongGroup {
		t.Fatalf("the target during the copy: %+v", r)
	}
	if _, ok := a.Final(0, false); ok {
		t.Fatal("Final answered for a Slot that isn't Frozen")
	}

	a.step(t, FreezeStep(0, 0))
	// Frozen: neither serves.
	if r := a.do(t, put(chunk[2].Key, "late")); r.Status != fsm.StatusMoving {
		t.Fatalf("a write to the Frozen Slot: %+v", r)
	}
	if r := read(t, a, chunk[2].Key); r.Status != fsm.StatusMoving {
		t.Fatalf("a read of the Frozen Slot: %+v", r)
	}
	if r := read(t, b, chunk[2].Key); r.Status != fsm.StatusWrongGroup {
		t.Fatalf("the target before it accepts: %+v", r)
	}
	// The Slot it isn't moving is untouched by all this.
	if r := a.do(t, put(other, "still served")); r.Status != fsm.StatusOK {
		t.Fatalf("another Slot of the source: %+v", r)
	}

	want := a.itemsOf(0)
	final, _ := a.Final(0, false)
	if len(final.Upserts) != 2 || len(final.Deletes) != 1 {
		t.Fatalf("the final part carries %d upserts and %d deletes, want the 2 keys written and the 1 deleted since the copy began", len(final.Upserts), len(final.Deletes))
	}
	b.step(t, AcceptStep(0, 1, 1, final))
	if got := b.itemsOf(0); !reflect.DeepEqual(got, want) {
		t.Fatalf("the target holds\n%+v\nwant what the source froze\n%+v", got, want)
	}
	if r := read(t, b, chunk[0].Key); string(r.Value) != "changed after it was sent" {
		t.Fatalf("the target's copy of a key changed during the copy: %+v", r)
	}
	if r := read(t, b, chunk[1].Key); r.Status != fsm.StatusNotFound {
		t.Fatalf("a key deleted during the copy is on the target: %+v", r)
	}

	a.step(t, DropStep(0, 0))
	if r := read(t, a, chunk[2].Key); r.Status != fsm.StatusWrongGroup {
		t.Fatalf("the source after the drop: %+v", r)
	}
	if got := a.Items(); len(got) != 1 || got[0].Key != other {
		t.Fatalf("the source still holds %+v", got)
	}
}

// itemsOf is what the Machine's key-value store holds of one Slot, served or
// not.
func (g *group) itemsOf(s shard.Slot) []fsm.Raw {
	items, _ := g.Chunk(s, "", 1<<30)
	return items
}

// Every step may arrive twice, or late, and must then change nothing.
func TestStepsAreSafeToRepeat(t *testing.T) {
	a, b := newGroup(1, 0), newGroup(2, 1)
	a.do(t, put(keyIn(0, 0), "1"))
	steps := func() (begin, incoming, freeze Step) {
		chunk, _ := a.Chunk(0, "", 100)
		return BeginStep(0, 0, 2), IncomingStep(0, 1, 1, Transfer{Upserts: chunk}), FreezeStep(0, 0)
	}
	begin, incoming, freeze := steps()
	for range 2 {
		if r := a.step(t, begin); r.Status != fsm.StatusOK {
			t.Fatalf("begin: %+v", r)
		}
		if r := b.step(t, incoming); r.Status != fsm.StatusOK {
			t.Fatalf("incoming: %+v", r)
		}
	}
	for range 2 {
		if r := a.step(t, freeze); r.Status != fsm.StatusOK {
			t.Fatalf("freeze: %+v", r)
		}
	}
	final, _ := a.Final(0, false)
	accept := AcceptStep(0, 1, 1, final)
	b.step(t, accept)
	// The target is serving and takes a write. A repeated accept, or a late
	// chunk, must not undo it.
	b.do(t, put(keyIn(0, 0), "written on the target"))
	for _, late := range []Step{accept, incoming} {
		if r := b.step(t, late); r.Status != fsm.StatusOK {
			t.Fatalf("a repeated step on the target: %+v", r)
		}
	}
	if r := read(t, b, keyIn(0, 0)); string(r.Value) != "written on the target" {
		t.Fatalf("a repeated step overwrote the target's data: %+v", r)
	}
	for range 2 {
		if r := a.step(t, DropStep(0, 0)); r.Status != fsm.StatusOK {
			t.Fatalf("drop: %+v", r)
		}
	}
	// Steps for the wrong Epoch are refused.
	if r := b.step(t, FreezeStep(0, 7)); r.Status != fsm.StatusInvalid {
		t.Fatalf("a freeze at the wrong Epoch: %+v", r)
	}
	if r := a.step(t, AcceptStep(1, 5, 2, Transfer{})); r.Status != fsm.StatusInvalid {
		t.Fatalf("an accept with nothing copied and nothing in it: %+v", r)
	}
}

// A Leader that lost track of the copy sends the whole Slot with the accept.
func TestAcceptCanCarryTheWholeSlot(t *testing.T) {
	a, b := newGroup(1, 0), newGroup(2, 1)
	for i := range 4 {
		a.do(t, put(keyIn(0, i), "v"))
	}
	a.step(t, BeginStep(0, 0, 2))
	stale, _ := a.Chunk(0, "", 1)
	stale[0].Value = []byte("left over from a copy that was abandoned")
	b.step(t, IncomingStep(0, 1, 1, Transfer{Upserts: append(stale, fsm.Raw{Key: keyIn(0, 50), Value: []byte("x")})}))
	a.step(t, FreezeStep(0, 0))
	final, _ := a.Final(0, true)
	if !final.Full || len(final.Upserts) != 4 {
		t.Fatalf("a full final part: %+v", final)
	}
	b.step(t, AcceptStep(0, 1, 1, final))
	if got, want := b.itemsOf(0), a.itemsOf(0); !reflect.DeepEqual(got, want) {
		t.Fatalf("the target holds %+v, want %+v", got, want)
	}
}

// A write on the new owner always has a higher version than any the old
// owner gave, though the two Logs' indexes have nothing to do with each
// other (A§11.5).
func TestVersionsKeepRisingAcrossAMove(t *testing.T) {
	a, b := newGroup(1, 0), newGroup(2, 1)
	a.next = 5000 // the source's Log is long, the target's short
	k := keyIn(0, 0)
	before := a.do(t, put(k, "1")).Version
	move(t, a, b, 0, 0)
	if r := read(t, b, k); r.Version != before {
		t.Fatalf("the Move changed the key's version from %d to %d", before, r.Version)
	}
	after := b.do(t, put(k, "2")).Version
	if after <= before {
		t.Fatalf("version %d after the Move, %d before", after, before)
	}
	// And back again.
	b.next = 9
	move(t, b, a, 0, 1)
	if back := a.do(t, put(k, "3")).Version; back <= after {
		t.Fatalf("version %d after moving back, %d before", back, after)
	}
}

func inSession(c fsm.Command, session, seq uint64, register bool) fsm.Command {
	c.Session, c.Seq, c.Register = session, seq, register
	return c
}

// A retry that reaches the new owner after a Move is recognised there
// (A§11.6).
func TestSessionMovesWithItsLastSlot(t *testing.T) {
	a, b := newGroup(1, 0, 1), newGroup(2, 2)
	moved, stayed := keyIn(0, 0), keyIn(1, 0)
	first := a.do(t, inSession(put(moved, "once"), 70, 1, true))
	a.do(t, inSession(put(stayed, "x"), 71, 1, true)) // a Session whose last request was elsewhere
	move(t, a, b, 0, 0)

	retry := b.do(t, inSession(put(moved, "once"), 70, 1, false))
	if retry.Status != fsm.StatusOK || retry.Version != first.Version {
		t.Fatalf("the retry on the new owner got %+v, want the first answer (version %d)", retry, first.Version)
	}
	if r := read(t, b, moved); r.Version != first.Version {
		t.Fatalf("the retry was applied again: version %d", r.Version)
	}
	if b.Sessions() != 1 {
		t.Fatalf("the target knows %d Sessions, want only the one that used the Slot", b.Sessions())
	}
	// The old owner refuses the retry as not its key, and keeps no record
	// of having seen it.
	if r := a.do(t, inSession(put(moved, "once"), 70, 1, false)); r.Status != fsm.StatusWrongGroup {
		t.Fatalf("the retry on the old owner: %+v", r)
	}
}

// A moved key expires when it would have, by the one Store time (A§11.7).
func TestDeadlinesSurviveAMove(t *testing.T) {
	a, b := newGroup(1, 0), newGroup(2, 1)
	at := func(stamp int64, c fsm.Command) fsm.Command { c.Stamp = stamp; return c }
	brief, lasting := keyIn(0, 0), keyIn(0, 1)
	gone := keyIn(0, 2)
	b.do(t, at(100, put(keyIn(1, 0), "x"))) // the target has heard less of the time
	short := at(1000, put(brief, "1"))
	short.TTL = 500
	a.do(t, short)
	shorter := at(1000, put(gone, "1"))
	shorter.TTL = 100
	a.do(t, shorter)
	a.do(t, at(1000, put(lasting, "1")))
	a.step(t, BeginStep(0, 0, 2))
	chunk, _ := a.Chunk(0, "", 100)
	b.step(t, IncomingStep(0, 1, 1, Transfer{Upserts: chunk}))
	a.do(t, at(1200, put(keyIn(0, 3), "moves time on"))) // past the shorter deadline
	a.step(t, FreezeStep(0, 0))
	final, _ := a.Final(0, false)
	b.step(t, AcceptStep(0, 1, 1, final))

	if b.LogTime() != 1200 {
		t.Fatalf("the target's Log time is %d, want the source's 1200", b.LogTime())
	}
	if r := read(t, b, gone); r.Status != fsm.StatusNotFound {
		t.Fatalf("a key that expired on the source during the copy is on the target: %+v", r)
	}
	if r := read(t, b, brief); r.Status != fsm.StatusOK {
		t.Fatalf("a key with time left: %+v", r)
	}
	b.do(t, at(1500, put(keyIn(1, 1), "x")))
	if r := read(t, b, brief); r.Status != fsm.StatusNotFound {
		t.Fatalf("at its deadline of 1500 the moved key is still there: %+v", r)
	}
	if r := read(t, b, lasting); r.Status != fsm.StatusOK {
		t.Fatalf("a moved key with no deadline: %+v", r)
	}
}

func TestScanAndTransactionAcrossSlots(t *testing.T) {
	g := newGroup(1, 0, 1, 2)
	var keys []string
	for s := range 3 {
		for n := range 2 {
			k := keyIn(shard.Slot(s), n)
			keys = append(keys, k)
			g.do(t, put(k, "v"))
		}
	}
	slices.Sort(keys)
	resp, err := fsm.DecodeResponse(g.Read(fsm.Command{Op: fsm.OpScan}.Encode()))
	if err != nil || resp.Status != fsm.StatusOK {
		t.Fatalf("scan: %+v, %v", resp, err)
	}
	var got []string
	for _, it := range resp.Items {
		got = append(got, it.Key)
	}
	if !slices.Equal(got, keys) {
		t.Fatalf("a scan over three Slots gave %v, want key order %v", got, keys)
	}
	limited, _ := fsm.DecodeResponse(g.Read(fsm.Command{Op: fsm.OpScan, Key: keys[1], End: keys[4], Limit: 2}.Encode()))
	if len(limited.Items) != 2 || limited.Items[0].Key != keys[1] || limited.Items[1].Key != keys[2] {
		t.Fatalf("a bounded scan gave %+v", limited.Items)
	}

	// A Transaction over two Slots gives both keys one version.
	x, y := keyIn(0, 0), keyIn(2, 0)
	txn := fsm.Command{Op: fsm.OpTxn, Writes: []fsm.Write{{Op: fsm.OpPut, Key: x, Value: []byte("a")}, {Op: fsm.OpPut, Key: y, Value: []byte("b")}}}
	v := g.do(t, txn).Version
	if rx, ry := read(t, g, x), read(t, g, y); rx.Version != v || ry.Version != v || string(ry.Value) != "b" {
		t.Fatalf("after the Transaction: %+v and %+v, want both at version %d", rx, ry, v)
	}
	// A refused one names the key, not its stored name.
	refused := g.do(t, fsm.Command{Op: fsm.OpTxn, Conds: []fsm.Cond{{Key: x, Version: 1}}, Writes: txn.Writes})
	if refused.Status != fsm.StatusVersionMismatch || len(refused.Items) != 1 || refused.Items[0].Key != x {
		t.Fatalf("a refused Transaction: %+v", refused)
	}
	// One that reaches into a Slot the Group doesn't own is refused whole.
	outside := fsm.Command{Op: fsm.OpTxn, Writes: []fsm.Write{{Op: fsm.OpPut, Key: x, Value: []byte("no")}, {Op: fsm.OpPut, Key: keyIn(3, 0), Value: []byte("no")}}}
	if r := g.do(t, outside); r.Status != fsm.StatusWrongGroup {
		t.Fatalf("a Transaction reaching outside the Group: %+v", r)
	}
	if r := read(t, g, x); string(r.Value) != "a" {
		t.Fatalf("the refused Transaction wrote something: %+v", r)
	}
}

func TestCaptureMidMove(t *testing.T) {
	a, b := newGroup(1, 0, 1), newGroup(2, 2)
	for i := range 4 {
		a.do(t, inSession(put(keyIn(0, i), "v"), 9, uint64(i+1), i == 0))
	}
	a.step(t, BeginStep(0, 0, 2))
	chunk, _ := a.Chunk(0, "", 2)
	b.step(t, IncomingStep(0, 1, 1, Transfer{Upserts: chunk}))
	a.do(t, put(chunk[0].Key, "dirty"))

	for _, g := range []*group{a, b} {
		restored := New(g.cfg)
		if err := restored.Restore(g.Capture()()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(restored.Slots(), g.Slots()) || !reflect.DeepEqual(restored.Capture()(), g.Capture()()) {
			t.Fatalf("Group %d: restored state differs", g.cfg.Group)
		}
		g.Machine = restored
	}
	// The restored pair finish the Move as the originals would have.
	rest, _ := a.Chunk(0, chunk[1].Key, 100)
	b.step(t, IncomingStep(0, 1, 1, Transfer{Upserts: rest}))
	a.step(t, FreezeStep(0, 0))
	final, _ := a.Final(0, false)
	if len(final.Upserts) != 1 || len(final.Sessions) != 1 {
		t.Fatalf("after a restore the final part has %d upserts and %d Sessions, want 1 and 1", len(final.Upserts), len(final.Sessions))
	}
	b.step(t, AcceptStep(0, 1, 1, final))
	if got, want := b.itemsOf(0), a.itemsOf(0); !reflect.DeepEqual(got, want) {
		t.Fatalf("the target holds %+v, want %+v", got, want)
	}
	if New(a.cfg).Restore([]byte{1, 2, 3}) == nil {
		t.Fatal("Restore accepted garbage")
	}
}
