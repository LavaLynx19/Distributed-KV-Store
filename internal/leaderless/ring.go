package leaderless

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"slices"
)

// Homes is how many Home Nodes a key has.
const Homes = 3

// ringPoints is how many places each Node has on the ring. With one each,
// five Nodes would share the keys unevenly, and the Node after a dead one
// would take all its Hints.
const ringPoints = 16

type point struct {
	at   uint64
	node NodeID
}

// Ring places keys on Nodes (A§13.2). Every Node of a store builds the same
// Ring from the same list of Nodes.
type Ring struct {
	points []point
	nodes  int
}

func hash64(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	// FNV alone leaves keys that differ in one digit close together.
	x := h.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return x
}

// NewRing builds the Ring of nodes.
func NewRing(nodes []NodeID) Ring {
	r := Ring{nodes: len(nodes)}
	for _, n := range nodes {
		for i := range ringPoints {
			var b [16]byte
			binary.BigEndian.PutUint64(b[:8], uint64(n))
			binary.BigEndian.PutUint64(b[8:], uint64(i))
			r.points = append(r.points, point{at: hash64(b[:]), node: n})
		}
	}
	slices.SortFunc(r.points, func(a, b point) int {
		if a.at != b.at {
			return cmp.Compare(a.at, b.at)
		}
		return cmp.Compare(a.node, b.node)
	})
	return r
}

// Order lists every Node in the order the ring gives for key. The first
// Homes of them are the key's Home Nodes. The rest, in order, are the Nodes
// that stand in for a Home Node that can't be reached.
func (r Ring) Order(key string) []NodeID {
	at := hash64([]byte(key))
	start, _ := slices.BinarySearchFunc(r.points, at, func(p point, at uint64) int { return cmp.Compare(p.at, at) })
	order := make([]NodeID, 0, r.nodes)
	for i := 0; len(order) < r.nodes && i < len(r.points); i++ {
		if n := r.points[(start+i)%len(r.points)].node; !slices.Contains(order, n) {
			order = append(order, n)
		}
	}
	return order
}

// Homes lists the key's Home Nodes.
func (r Ring) Homes(key string) []NodeID {
	order := r.Order(key)
	return order[:min(Homes, len(order))]
}
