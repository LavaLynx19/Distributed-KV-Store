package shard

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSlotOfIsStableAndSpread(t *testing.T) {
	if SlotOf("k1", 8) != SlotOf("k1", 8) {
		t.Fatal("a key's Slot changed between calls")
	}
	// These are relied on by tests elsewhere: if the hash changes, they must.
	counts := make([]int, 8)
	for i := range 800 {
		counts[SlotOf(fmt.Sprintf("k%d", i), 8)]++
	}
	for slot, n := range counts {
		if n < 60 || n > 140 {
			t.Errorf("Slot %d got %d of 800 keys", slot, n)
		}
	}
}

func TestTableRoundTrip(t *testing.T) {
	table := Table{Version: 41, StoreTime: -7, Slots: []Owner{{Group: 1}, {Group: 2, Epoch: 3, MovingTo: 1}, {Group: 3, Epoch: 65000}}}
	got, err := DecodeTable(table.Encode())
	if err != nil || !reflect.DeepEqual(got, table) {
		t.Fatalf("round trip gave %+v, %v", got, err)
	}
	for _, bad := range [][]byte{nil, {1}, table.Encode()[:5], append(table.Encode(), 0)} {
		if _, err := DecodeTable(bad); err == nil {
			t.Errorf("accepted %d malformed bytes", len(bad))
		}
	}
	c := table.Clone()
	c.Slots[0].Group = 9
	if table.Slots[0].Group != 1 {
		t.Fatal("Clone shares its Slots")
	}
}

// A write after a Move always has a higher version than any before it,
// whatever the two Groups' Log indexes.
func TestVersionOrdersByEpochFirst(t *testing.T) {
	before := Version(2, 900_000)
	after := Version(3, 4)
	if after <= before {
		t.Fatalf("Epoch 3 index 4 (%d) should beat Epoch 2 index 900000 (%d)", after, before)
	}
	if Version(0, 77) != 77 {
		t.Fatal("Epoch 0 must be the bare index")
	}
	if e, i := SplitVersion(after); e != 3 || i != 4 {
		t.Fatalf("split gave %d, %d", e, i)
	}
}
