// Package shard holds what every part of a store with several Groups must
// agree on (A§11): how a key maps to a Slot, what the Slot table says, and
// how a version is made from a Slot's Epoch and a Log index. It is pure.
package shard

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
)

// Slot is one of a fixed number of shares of the keys.
type Slot uint16

// GroupID names a Group. The Meta Group is 0; data Groups count from 1.
type GroupID uint32

// Meta is the Meta Group's id.
const Meta GroupID = 0

// DefaultSlots is how many Slots a store has unless it is created with
// another number. The number never changes afterwards.
const DefaultSlots = 64

// SlotOf is the Slot a key belongs to, out of slots.
func SlotOf(key string, slots int) Slot {
	h := fnv.New64a()
	h.Write([]byte(key)) // never fails
	return Slot(h.Sum64() % uint64(slots))
}

// Owner is one Slot's row of the table.
type Owner struct {
	// Group owns the Slot.
	Group GroupID
	// Epoch counts the times the Slot has changed owner.
	Epoch uint32
	// MovingTo is the Group the Slot is on its way to, or 0. While it is
	// set, Group still owns the Slot.
	MovingTo GroupID
}

// Table is the Slot table, as the Meta Group holds it.
type Table struct {
	// Version is the index of the Meta Group Entry that last changed the
	// table. A higher Version is newer.
	Version uint64
	// Slots has one Owner per Slot.
	Slots []Owner
	// StoreTime is the time the whole store goes by (A§11.7).
	StoreTime int64
}

// OwnerOf is the Group the table says owns key's Slot.
func (t Table) OwnerOf(key string) GroupID { return t.Slots[SlotOf(key, len(t.Slots))].Group }

// Clone returns a copy that shares nothing with t.
func (t Table) Clone() Table {
	t.Slots = append([]Owner(nil), t.Slots...)
	return t
}

// Encode lays the table out as Version, StoreTime, the number of Slots and
// then each Owner, all as varints.
func (t Table) Encode() []byte {
	b := binary.AppendUvarint(nil, t.Version)
	b = binary.AppendVarint(b, t.StoreTime)
	b = binary.AppendUvarint(b, uint64(len(t.Slots)))
	for _, o := range t.Slots {
		b = binary.AppendUvarint(b, uint64(o.Group))
		b = binary.AppendUvarint(b, uint64(o.Epoch))
		b = binary.AppendUvarint(b, uint64(o.MovingTo))
	}
	return b
}

var errMalformed = errors.New("shard: malformed table")

func DecodeTable(b []byte) (Table, error) {
	var t Table
	next := func() uint64 {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			b = nil
			return 0
		}
		b = b[n:]
		return v
	}
	if len(b) == 0 {
		return t, errMalformed
	}
	t.Version = next()
	st, n := binary.Varint(b)
	if n <= 0 {
		return Table{}, errMalformed
	}
	t.StoreTime, b = st, b[n:]
	count := next()
	if count > uint64(len(b)) {
		return Table{}, errMalformed
	}
	for range count {
		if len(b) == 0 {
			return Table{}, errMalformed
		}
		t.Slots = append(t.Slots, Owner{Group: GroupID(next()), Epoch: uint32(next()), MovingTo: GroupID(next())})
	}
	if b == nil || len(b) != 0 {
		return Table{}, errMalformed
	}
	return t, nil
}

// epochShift leaves 48 bits for the Log index and 16 for the Epoch.
const epochShift = 48

// Version is a key's version: the Epoch of its Slot when it was written,
// and the index of the Entry that wrote it in the owning Group's Log
// (A§11.5). Compared as a number, a later write always has a higher one,
// whichever Group made it. With Epoch 0 it is the bare index, as in Rungs
// 1–6.
func Version(epoch uint32, index uint64) uint64 { return uint64(epoch)<<epochShift | index }

// SplitVersion is the inverse of Version.
func SplitVersion(v uint64) (epoch uint32, index uint64) {
	return uint32(v >> epochShift), v & (1<<epochShift - 1)
}

// A store's Groups are placed on its Nodes by one rule, so that every Node
// can work out where everything is from four numbers (A§11.9). Nodes are
// numbered from 1. Group g has its replicas on consecutive Nodes starting
// after Node g, so Groups overlap and a Node's failure hits some of them
// and not others.

// Hosts lists the Nodes that hold a replica of Group g, ascending, in a
// store of nodes Nodes with replicas Members per Group.
func Hosts(g GroupID, nodes, replicas int) []int {
	hosts := make([]int, 0, replicas)
	for i := range replicas {
		hosts = append(hosts, 1+(int(g)+i)%nodes)
	}
	// Three or five numbers: insertion sort.
	for i := 1; i < len(hosts); i++ {
		for j := i; j > 0 && hosts[j] < hosts[j-1]; j-- {
			hosts[j], hosts[j-1] = hosts[j-1], hosts[j]
		}
	}
	return hosts
}

// ReplicaID is the id Node n's replica of Group g goes by among the Members
// of its Group. It names both, so one network can carry every Group's
// messages. A store has fewer than 100 Groups.
func ReplicaID(n int, g GroupID) uint64 { return uint64(n*100 + int(g)) }

// SplitReplicaID is the inverse of ReplicaID.
func SplitReplicaID(id uint64) (n int, g GroupID) { return int(id) / 100, GroupID(id % 100) }
