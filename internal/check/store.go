package check

import (
	"fmt"
	"slices"
	"strings"

	"github.com/anishathalye/porcupine"

	"distributed-kv-store/internal/fsm"
)

// storeState is what the whole-store model knows: one keyState per key that
// has ever been written. A storeState is never modified, so states can share
// what they have in common. canon is its content as text, which is what two
// states are compared by.
type storeState struct {
	keys  map[string]keyState
	canon string
}

func newStoreState(keys map[string]keyState) storeState {
	names := make([]string, 0, len(keys))
	for k := range keys {
		names = append(names, k)
	}
	slices.Sort(names)
	var b strings.Builder
	for _, k := range names {
		fmt.Fprintf(&b, "%q=%+v;", k, keys[k])
	}
	return storeState{keys: keys, canon: b.String()}
}

func (s storeState) key(k string) keyState {
	if ks, ok := s.keys[k]; ok {
		return ks
	}
	return keyState{known: true}
}

// with returns the state with some keys replaced.
func (s storeState) with(changed map[string]keyState) storeState {
	keys := make(map[string]keyState, len(s.keys)+len(changed))
	for k, v := range s.keys {
		keys[k] = v
	}
	for k, v := range changed {
		keys[k] = v
	}
	return newStoreState(keys)
}

// holding returns the key's state given that its version was found to be v,
// and whether that is possible. Finding version 0 where a mortal key stood
// means the key expired.
func holding(ks keyState, v uint64) (keyState, bool) {
	if v == 0 && ks.exists && ks.mortal {
		return ks.expired(), true
	}
	return ks.versionIs(v)
}

// mayNotHold reports whether the key's version could be something other
// than v.
func mayNotHold(ks keyState, v uint64) bool {
	return ks.versionMayDiffer(v) || (ks.exists && ks.mortal && v != 0)
}

// stepStore returns every state the store could be in after cmd produced
// out, or nil if it couldn't have.
func stepStore(s storeState, cmd fsm.Command, out Outcome) []storeState {
	switch cmd.Op {
	case fsm.OpScan:
		return stepScan(s, cmd, out)
	case fsm.OpTxn:
		return stepTxn(s, cmd, out)
	}
	var next []storeState
	for _, ks := range step(s.key(cmd.Key), cmd, out) {
		next = append(next, s.with(map[string]keyState{cmd.Key: ks}))
	}
	return next
}

// stepScan checks a scan's answer against the state: it must list exactly
// the keys that exist in the range, in order, up to the limit. A mortal key
// that is missing from the answer has expired.
func stepScan(s storeState, cmd fsm.Command, out Outcome) []storeState {
	if out.Unknown {
		return []storeState{s}
	}
	if out.Resp.Status != fsm.StatusOK {
		return nil
	}
	limit := cmd.Limit
	if limit == 0 || limit > fsm.MaxScan {
		limit = fsm.MaxScan
	}
	items := out.Resp.Items
	if uint64(len(items)) > limit {
		return nil
	}
	inRange := func(k string) bool { return k >= cmd.Key && (cmd.End == "" || k < cmd.End) }
	found := map[string]fsm.Item{}
	for i, it := range items {
		if !inRange(it.Key) || (i > 0 && it.Key <= items[i-1].Key) {
			return nil
		}
		found[it.Key] = it
	}
	// Once the limit is reached the scan says nothing about later keys.
	full := uint64(len(items)) == limit
	changed := map[string]keyState{}
	for k, ks := range s.keys {
		if !ks.exists || !inRange(k) {
			continue
		}
		it, listed := found[k]
		switch {
		case listed:
			narrowed, ok := ks.versionIs(it.Version)
			if !ok || ks.value != string(it.Value) {
				return nil
			}
			changed[k] = narrowed
			delete(found, k)
		case full && k > items[len(items)-1].Key:
		case ks.mortal:
			changed[k] = ks.expired()
		default:
			return nil
		}
	}
	if len(found) > 0 {
		return nil // the scan listed a key that doesn't exist
	}
	return []storeState{s.with(changed)}
}

// stepTxn checks a Transaction's answer. Applied, every condition held and
// every write happened. Refused, exactly the conditions it names failed.
func stepTxn(s storeState, cmd fsm.Command, out Outcome) []storeState {
	if out.Unknown {
		var next []storeState
		if applied, ok := txnApplied(s, cmd, 0, false); ok {
			next = append(next, applied)
		}
		for _, c := range cmd.Conds {
			if mayNotHold(s.key(c.Key), c.Version) {
				return append(next, s)
			}
		}
		return next
	}
	switch out.Resp.Status {
	case fsm.StatusOK:
		if applied, ok := txnApplied(s, cmd, out.Resp.Version, true); ok {
			return []storeState{applied}
		}
	case fsm.StatusVersionMismatch:
		failed := map[string]uint64{}
		for _, it := range out.Resp.Items {
			failed[it.Key] = it.Version
		}
		if len(failed) == 0 {
			return nil
		}
		changed := map[string]keyState{}
		for _, c := range cmd.Conds {
			ks, seen := changed[c.Key]
			if !seen {
				ks = s.key(c.Key)
			}
			want := c.Version
			if f, isFailed := failed[c.Key]; isFailed {
				if f == c.Version {
					return nil
				}
				want = f
			}
			narrowed, ok := holding(ks, want)
			if !ok {
				return nil
			}
			changed[c.Key] = narrowed
		}
		for k := range failed {
			if _, isCond := changed[k]; !isCond {
				return nil // it named a key the Transaction never asked about
			}
		}
		return []storeState{s.with(changed)}
	}
	return nil
}

// txnApplied is the state after the Transaction took effect, and whether it
// could have. With seen, its writes got version v; otherwise nobody saw the
// answer.
func txnApplied(s storeState, cmd fsm.Command, v uint64, seen bool) (storeState, bool) {
	changed := map[string]keyState{}
	for _, c := range cmd.Conds {
		ks, already := changed[c.Key]
		if !already {
			ks = s.key(c.Key)
		}
		narrowed, ok := holding(ks, c.Version)
		if !ok {
			return s, false
		}
		changed[c.Key] = narrowed
	}
	// Only the last write to a key shows.
	last := map[string]fsm.Write{}
	for _, w := range cmd.Writes {
		last[w.Key] = w
	}
	for k, w := range last {
		ks, already := changed[k]
		if !already {
			ks = s.key(k)
		}
		as := fsm.Command{Value: w.Value, TTL: w.TTL}
		switch {
		case w.Op == fsm.OpDelete:
			ks = ks.removed()
		case seen:
			var ok bool
			if ks, ok = ks.written(as, v); !ok {
				return s, false
			}
		default:
			ks = ks.writtenUnseen(as)
		}
		changed[k] = ks
	}
	return s.with(changed), true
}

// StoreModel is the store's sequential specification over all keys at once,
// for Histories with scans or Transactions (A§8.2). It can't be checked one
// key at a time, so such Histories must be kept short.
func StoreModel() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Init: func() []any { return []any{newStoreState(nil)} },
		Step: func(state, input, output any) []any {
			var next []any
			for _, s := range stepStore(state.(storeState), input.(fsm.Command), output.(Outcome)) {
				next = append(next, s)
			}
			return next
		},
		Equal: func(a, b any) bool { return a.(storeState).canon == b.(storeState).canon },
		DescribeOperation: func(input, output any) string {
			return describe(input.(fsm.Command), output.(Outcome))
		},
		DescribeState: func(state any) string { return state.(storeState).canon },
	}
	return nm.ToModel()
}
