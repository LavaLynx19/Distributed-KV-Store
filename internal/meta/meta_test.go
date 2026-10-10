package meta

import (
	"reflect"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/shard"
)

func run(t *testing.T, m *Machine, first core.Index, cmds ...Command) []Response {
	t.Helper()
	var out []Response
	for i, c := range cmds {
		r, err := DecodeResponse(m.Apply(core.Entry{Index: first + core.Index(i), Kind: core.EntryCommand, Payload: c.Encode()}))
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		out = append(out, r)
	}
	return out
}

func TestStartingTable(t *testing.T) {
	table := New(8, 3).Table()
	var owners []shard.GroupID
	for _, o := range table.Slots {
		owners = append(owners, o.Group)
	}
	if want := []shard.GroupID{1, 2, 3, 1, 2, 3, 1, 2}; !reflect.DeepEqual(owners, want) || table.Version != 0 {
		t.Fatalf("owners %v at version %d, want %v at 0", owners, table.Version, want)
	}
}

func TestMoveThenDone(t *testing.T) {
	m := New(8, 3)
	got := run(t, m, 10,
		Command{Op: OpMove, Slot: 0, To: 2},    // 10: intent
		Command{Op: OpMove, Slot: 0, To: 2},    // 11: asked twice
		Command{Op: OpMove, Slot: 0, To: 3},    // 12: already going elsewhere
		Command{Op: OpMove, Slot: 1, To: 2},    // 13: Group 2 owns Slot 1 already
		Command{Op: OpMove, Slot: 99, To: 2},   // 14: no such Slot
		Command{Op: OpMove, Slot: 2, To: 7},    // 15: no such Group
		Command{Op: OpDone, Slot: 3, Epoch: 1}, // 16: nothing was moving
		Command{Op: OpDone, Slot: 0, Epoch: 2}, // 17: wrong Epoch
	)
	want := []Status{StatusOK, StatusOK, StatusBusy, StatusInvalid, StatusInvalid, StatusInvalid, StatusInvalid, StatusInvalid}
	for i, r := range got {
		if r.Status != want[i] {
			t.Errorf("command %d: got %v, want %v", i, r.Status, want[i])
		}
	}
	// The intent is in the table, and the owner hasn't changed.
	if o := m.Table().Slots[0]; o != (shard.Owner{Group: 1, MovingTo: 2}) || m.Table().Version != 10 {
		t.Fatalf("after the intent: %+v at version %d", o, m.Table().Version)
	}
	got = run(t, m, 20, Command{Op: OpDone, Slot: 0, Epoch: 1}, Command{Op: OpDone, Slot: 0, Epoch: 1})
	if got[0].Status != StatusOK || got[1].Status != StatusOK {
		t.Fatalf("done, and done again: %+v", got)
	}
	if o := m.Table().Slots[0]; o != (shard.Owner{Group: 2, Epoch: 1}) || m.Table().Version != 20 {
		t.Fatalf("after done: %+v at version %d, want Group 2 at Epoch 1, version 20", o, m.Table().Version)
	}
}

func TestSessionsAndStoreTime(t *testing.T) {
	m := New(8, 2)
	got := run(t, m, 5, Command{Op: OpOpenSession}, Command{Op: OpOpenSession}, Command{Op: OpTick, Stamp: 900}, Command{Op: OpTick, Stamp: 400})
	if got[0].Session != 5 || got[1].Session != 6 {
		t.Fatalf("Session ids %d and %d, want 5 and 6", got[0].Session, got[1].Session)
	}
	if st := m.Table().StoreTime; st != 900 {
		t.Fatalf("Store time = %d, want 900: it never goes back", st)
	}
}

func TestCaptureAndRestore(t *testing.T) {
	m := New(8, 3)
	run(t, m, 1, Command{Op: OpMove, Slot: 4, To: 1}, Command{Op: OpTick, Stamp: 50})
	restored := New(8, 3)
	if err := restored.Restore(m.Capture()()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Table(), m.Table()) {
		t.Fatalf("restored %+v, want %+v", restored.Table(), m.Table())
	}
	if got, err := shard.DecodeTable(m.Read(nil)); err != nil || !reflect.DeepEqual(got, m.Table()) {
		t.Fatalf("Read gave %+v, %v", got, err)
	}
	if New(8, 3).Restore([]byte{1, 2}) == nil {
		t.Fatal("Restore accepted garbage")
	}
}

// The naive store's table: the owner changes the moment a Move is asked for.
func TestFlippingChangesTheOwnerAtOnce(t *testing.T) {
	m := NewFlipping(8, 3)
	run(t, m, 1, Command{Op: OpMove, Slot: 0, To: 2})
	if o := m.Table().Slots[0]; o != (shard.Owner{Group: 2, Epoch: 1}) {
		t.Fatalf("after a naive Move: %+v", o)
	}
}

func TestCommandRoundTrip(t *testing.T) {
	for _, c := range []Command{{Op: OpMove, Slot: 63, To: 4}, {Op: OpDone, Slot: 1, Epoch: 9}, {Op: OpTick, Stamp: -3}, {Op: OpOpenSession},
		{Op: OpReplace, Out: 2, In: 6}, {Op: OpMembers, To: 1, At: 40, Members: []int{1, 3, 6}}} {
		if got, err := DecodeCommand(c.Encode()); err != nil || !reflect.DeepEqual(got, c) {
			t.Errorf("round trip of %+v gave %+v, %v", c, got, err)
		}
	}
	for _, bad := range [][]byte{nil, {0}, {9, 0, 0, 0, 0}, {1, 0}, append(Command{Op: OpTick}.Encode(), 0)} {
		if _, err := DecodeCommand(bad); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
}

// A replacement is wanted in every Group the Node is in, and is over for a
// Group once that Group reports a list with the new Node and without the old.
func TestReplaceFollowsWhatGroupsReport(t *testing.T) {
	m := NewPlaced(8, 2, 5, 3) // Meta on 1,2,3; Group 1 on 2,3,4; Group 2 on 3,4,5
	rs := run(t, m, 1,
		Command{Op: OpReplace, Out: 4, In: 3},                             // 3 isn't a Spare
		Command{Op: OpReplace, Out: 4, In: 6},                             // Groups 1 and 2
		Command{Op: OpReplace, Out: 4, In: 7},                             // both busy
		Command{Op: OpMembers, To: 1, At: 10, Members: []int{2, 3, 4, 6}}, // half way
		Command{Op: OpMembers, To: 1, At: 12, Members: []int{2, 3, 6}},
		Command{Op: OpMembers, To: 1, At: 10, Members: []int{2, 3, 4, 6}}, // old news
		Command{Op: OpReplace, Out: 6, In: 7},                             // 6 died before Group 2 took it
	)
	var got []Status
	for _, r := range rs {
		got = append(got, r.Status)
	}
	if want := []Status{StatusInvalid, StatusOK, StatusBusy, StatusOK, StatusOK, StatusOK, StatusOK}; !reflect.DeepEqual(got, want) {
		t.Fatalf("statuses %v, want %v", got, want)
	}
	table := m.Table()
	if g := table.Groups[0]; !reflect.DeepEqual(g, shard.Group{Members: []int{1, 2, 3}}) {
		t.Errorf("Meta Group: %+v", g)
	}
	if g := table.Groups[1]; !reflect.DeepEqual(g, shard.Group{Members: []int{2, 3, 6}, At: 12, Add: 7, Remove: 6}) {
		t.Errorf("Group 1: %+v", g)
	}
	if g := table.Groups[2]; !reflect.DeepEqual(g, shard.Group{Members: []int{3, 4, 5}, Add: 7, Remove: 4}) {
		t.Errorf("Group 2: %+v", g)
	}
	// The table survives a Snapshot.
	again := New(8, 2)
	if err := again.Restore(m.Capture()()); err != nil || !reflect.DeepEqual(again.Table(), table) {
		t.Errorf("restored %+v, %v", again.Table(), err)
	}
}
