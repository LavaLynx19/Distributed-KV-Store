// Package tree is a copy-on-write ordered map from string keys to values
// (A§2.11). A Tree is a value: Put and Delete return a new Tree and leave the
// old one exactly as it was, sharing every part the change didn't touch. So
// keeping a Snapshot of the whole map costs nothing but holding on to an old
// Tree, and a range scan walks keys in order.
//
// It is a treap: a binary search tree on the keys that is also a heap on a
// priority derived from each key's hash. The hash makes the shape depend only
// on which keys are present, never on the order they arrived or on any
// source of randomness, so two Members with the same keys hold identical
// trees.
package tree

type node[V any] struct {
	key         string
	value       V
	priority    uint64
	left, right *node[V]
}

// Tree is an immutable ordered map. The zero value is an empty Tree.
type Tree[V any] struct {
	root *node[V]
	size int
}

// Len is the number of keys.
func (t Tree[V]) Len() int { return t.size }

// Get returns the value stored under key.
func (t Tree[V]) Get(key string) (V, bool) {
	for n := t.root; n != nil; {
		switch {
		case key < n.key:
			n = n.left
		case key > n.key:
			n = n.right
		default:
			return n.value, true
		}
	}
	var zero V
	return zero, false
}

// Put returns a Tree with key set to value.
func (t Tree[V]) Put(key string, value V) Tree[V] {
	root, added := put(t.root, key, value, priority(key))
	if added {
		return Tree[V]{root: root, size: t.size + 1}
	}
	return Tree[V]{root: root, size: t.size}
}

// Delete returns a Tree without key. If key isn't present it returns t.
func (t Tree[V]) Delete(key string) Tree[V] {
	if _, ok := t.Get(key); !ok {
		return t
	}
	return Tree[V]{root: remove(t.root, key), size: t.size - 1}
}

// Ascend calls fn for each key in [from, to), in order, until fn returns
// false. An empty to means no upper bound.
func (t Tree[V]) Ascend(from, to string, fn func(key string, value V) bool) {
	ascend(t.root, from, to, fn)
}

func ascend[V any](n *node[V], from, to string, fn func(string, V) bool) bool {
	if n == nil {
		return true
	}
	if n.key >= from {
		if !ascend(n.left, from, to, fn) {
			return false
		}
		if to != "" && n.key >= to {
			return false
		}
		if !fn(n.key, n.value) {
			return false
		}
	}
	if to != "" && n.key >= to {
		return false
	}
	return ascend(n.right, from, to, fn)
}

// put returns a copy of the subtree at n with key set, sharing every node
// off the path to key. Rotations only ever touch nodes this call created.
func put[V any](n *node[V], key string, value V, prio uint64) (*node[V], bool) {
	if n == nil {
		return &node[V]{key: key, value: value, priority: prio}, true
	}
	c := *n
	var added bool
	switch {
	case key < n.key:
		c.left, added = put(n.left, key, value, prio)
		if c.left.priority > c.priority {
			l := c.left
			c.left, l.right = l.right, &c
			return l, added
		}
	case key > n.key:
		c.right, added = put(n.right, key, value, prio)
		if c.right.priority > c.priority {
			r := c.right
			c.right, r.left = r.left, &c
			return r, added
		}
	default:
		c.value = value
	}
	return &c, added
}

// remove returns a copy of the subtree at n without key, which must be
// present.
func remove[V any](n *node[V], key string) *node[V] {
	switch {
	case key < n.key:
		c := *n
		c.left = remove(n.left, key)
		return &c
	case key > n.key:
		c := *n
		c.right = remove(n.right, key)
		return &c
	}
	return merge(n.left, n.right)
}

// merge joins two subtrees where every key in a sorts before every key in b.
func merge[V any](a, b *node[V]) *node[V] {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case a.priority > b.priority:
		c := *a
		c.right = merge(a.right, b)
		return &c
	}
	c := *b
	c.left = merge(a, b.left)
	return &c
}

// priority is the 64-bit FNV-1a hash of key, mixed once more so that keys
// differing only in their last bytes still get unrelated priorities.
func priority(key string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return h
}
