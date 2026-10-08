package tree

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

func keys[V any](t Tree[V], from, to string) []string {
	out := []string{}
	t.Ascend(from, to, func(k string, _ V) bool { out = append(out, k); return true })
	return out
}

func depth[V any](n *node[V]) int {
	if n == nil {
		return 0
	}
	return 1 + max(depth(n.left), depth(n.right))
}

// checkShape verifies the search-tree order and the heap order everywhere.
func checkShape[V any](t *testing.T, n *node[V], low, high string) {
	t.Helper()
	if n == nil {
		return
	}
	if (low != "" && n.key <= low) || (high != "" && n.key >= high) {
		t.Fatalf("key %q out of order (between %q and %q)", n.key, low, high)
	}
	for _, child := range []*node[V]{n.left, n.right} {
		if child != nil && child.priority > n.priority {
			t.Fatalf("heap order broken under %q", n.key)
		}
	}
	checkShape(t, n.left, low, n.key)
	checkShape(t, n.right, n.key, high)
}

// Random puts and deletes, checked against a plain map after every step.
func TestMatchesAMap(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var tr Tree[int]
	ref := map[string]int{}
	for step := range 20000 {
		key := fmt.Sprintf("k%03d", rng.IntN(300))
		if rng.IntN(3) == 0 {
			tr = tr.Delete(key)
			delete(ref, key)
		} else {
			tr = tr.Put(key, step)
			ref[key] = step
		}
		if tr.Len() != len(ref) {
			t.Fatalf("step %d: Len %d, want %d", step, tr.Len(), len(ref))
		}
		if got, ok := tr.Get(key); ok != (ref[key] != 0 || containsKey(ref, key)) || (ok && got != ref[key]) {
			t.Fatalf("step %d: Get(%q) = %d, %v; map has %d", step, key, got, ok, ref[key])
		}
		if step%500 == 0 {
			want := make([]string, 0, len(ref))
			for k := range ref {
				want = append(want, k)
			}
			slices.Sort(want)
			if got := keys(tr, "", ""); !reflect.DeepEqual(got, want) {
				t.Fatalf("step %d: keys out of step with the map", step)
			}
			checkShape(t, tr.root, "", "")
		}
	}
}

func containsKey(m map[string]int, k string) bool { _, ok := m[k]; return ok }

// An old Tree is untouched by anything done to Trees made from it.
func TestOldVersionsNeverChange(t *testing.T) {
	var tr Tree[string]
	for i := range 100 {
		tr = tr.Put(fmt.Sprintf("k%03d", i), "old")
	}
	before := tr
	wantKeys := keys(before, "", "")

	for i := range 100 {
		key := fmt.Sprintf("k%03d", i)
		if i%2 == 0 {
			tr = tr.Delete(key)
		} else {
			tr = tr.Put(key, "new")
		}
	}
	tr = tr.Put("zzz", "new")

	if before.Len() != 100 || !reflect.DeepEqual(keys(before, "", ""), wantKeys) {
		t.Fatal("the old Tree's keys changed")
	}
	before.Ascend("", "", func(k, v string) bool {
		if v != "old" {
			t.Fatalf("the old Tree now holds %q under %q", v, k)
		}
		return true
	})
	if tr.Len() != 51 {
		t.Fatalf("the new Tree has %d keys, want 51", tr.Len())
	}
}

func TestAscendRange(t *testing.T) {
	var tr Tree[int]
	for i, k := range []string{"d", "b", "f", "a", "c", "e", "g"} {
		tr = tr.Put(k, i)
	}
	tests := []struct {
		from, to string
		want     []string
	}{
		{"", "", []string{"a", "b", "c", "d", "e", "f", "g"}},
		{"c", "", []string{"c", "d", "e", "f", "g"}},
		{"", "c", []string{"a", "b"}},
		{"b", "f", []string{"b", "c", "d", "e"}},
		{"bb", "e", []string{"c", "d"}},
		{"x", "", []string{}},
		{"c", "c", []string{}},
	}
	for _, tt := range tests {
		if got := keys(tr, tt.from, tt.to); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Ascend(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
	// fn can stop the walk.
	var seen []string
	tr.Ascend("", "", func(k string, _ int) bool { seen = append(seen, k); return k != "c" })
	if !reflect.DeepEqual(seen, []string{"a", "b", "c"}) {
		t.Errorf("stopping at c visited %v", seen)
	}
}

// The shape depends only on the keys, not on the order they were added, and
// stays shallow even when keys arrive sorted.
func TestShapeIsDeterministicAndShallow(t *testing.T) {
	const n = 20000
	var forward, backward Tree[int]
	for i := range n {
		forward = forward.Put(fmt.Sprintf("key-%06d", i), i)
		backward = backward.Put(fmt.Sprintf("key-%06d", n-1-i), n-1-i)
	}
	if d := depth(forward.root); d > 60 {
		t.Fatalf("%d sorted keys gave a tree %d deep", n, d)
	}
	var same func(a, b *node[int]) bool
	same = func(a, b *node[int]) bool {
		if a == nil || b == nil {
			return a == b
		}
		return a.key == b.key && a.value == b.value && same(a.left, b.left) && same(a.right, b.right)
	}
	if !same(forward.root, backward.root) {
		t.Fatal("the same keys added in a different order gave a different tree")
	}
}

func TestDeleteMissingKeyReturnsSameTree(t *testing.T) {
	tr := Tree[int]{}.Put("a", 1)
	if got := tr.Delete("nope"); got.root != tr.root || got.Len() != 1 {
		t.Fatal("deleting a missing key copied or changed the Tree")
	}
}
