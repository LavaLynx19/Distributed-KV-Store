package check

import (
	"fmt"

	"github.com/anishathalye/porcupine"

	"distributed-kv-store/internal/fsm"
)

// Outcome is what a client saw for one request. Unknown means it never got a
// definite answer, so the request may or may not have taken effect.
type Outcome struct {
	Unknown bool
	Resp    fsm.Response
}

// keyState is what the model knows about one key. A write whose outcome is
// unknown leaves the key with a version nobody has observed yet (known is
// false). The next response that reports the version pins it.
type keyState struct {
	exists  bool
	value   string
	version uint64
	known   bool
	// floor is the highest version ever observed for the key. A version is
	// the Index of the Entry that wrote it, so later writes must exceed it.
	floor uint64
	// mortal: the key was written with a time-to-live. The model doesn't
	// know the store's time, so it lets such a key stop existing at any
	// moment. What it does insist on is that the key then stays gone until
	// something writes it again (A§6.7).
	mortal bool
}

// expired is the state after a mortal key's time ran out.
func (s keyState) expired() keyState { return s.removed() }

// versionIs returns the state narrowed by learning that the key's version is
// v, and whether that is possible.
func (s keyState) versionIs(v uint64) (keyState, bool) {
	switch {
	case !s.exists:
		return s, v == 0
	case s.known:
		return s, v == s.version
	case v > s.floor:
		s.version, s.known, s.floor = v, true, v
		return s, true
	}
	return s, false
}

// versionMayDiffer reports whether the key's version could be something
// other than v.
func (s keyState) versionMayDiffer(v uint64) bool {
	switch {
	case !s.exists:
		return v != 0
	case s.known:
		return v != s.version
	}
	return true
}

// written is the state after a write that reported version v.
func (s keyState) written(cmd fsm.Command, v uint64) (keyState, bool) {
	if v <= s.floor {
		return s, false
	}
	return keyState{exists: true, value: string(cmd.Value), version: v, known: true, floor: v, mortal: cmd.TTL > 0}, true
}

// writtenUnseen is the state after a write nobody saw the response to.
func (s keyState) writtenUnseen(cmd fsm.Command) keyState {
	return keyState{exists: true, value: string(cmd.Value), floor: s.floor, mortal: cmd.TTL > 0}
}

func (s keyState) removed() keyState {
	return keyState{known: true, floor: s.floor}
}

// step returns every state the key could be in after cmd produced out, or
// nil if cmd could not have produced out from s. A mortal key may have
// expired just before cmd, so both possibilities are followed.
func step(s keyState, cmd fsm.Command, out Outcome) []keyState {
	next := stepFrom(s, cmd, out)
	if s.exists && s.mortal {
		next = append(next, stepFrom(s.expired(), cmd, out)...)
	}
	return next
}

func stepFrom(s keyState, cmd fsm.Command, out Outcome) []keyState {
	if out.Unknown {
		return stepUnknown(s, cmd)
	}
	r := out.Resp
	one := func(next keyState, ok bool) []keyState {
		if !ok {
			return nil
		}
		return []keyState{next}
	}
	// mismatch: a compare-and-set that failed and reported the version found.
	mismatch := func() []keyState {
		if r.Version == cmd.IfVersion || !s.versionMayDiffer(cmd.IfVersion) {
			return nil
		}
		return one(s.versionIs(r.Version))
	}

	switch cmd.Op {
	case fsm.OpGet:
		switch r.Status {
		case fsm.StatusNotFound:
			return one(s, !s.exists)
		case fsm.StatusOK:
			if !s.exists || s.value != string(r.Value) {
				return nil
			}
			return one(s.versionIs(r.Version))
		}
	case fsm.OpPut:
		switch r.Status {
		case fsm.StatusOK:
			if cmd.Conditional {
				var ok bool
				if s, ok = s.versionIs(cmd.IfVersion); !ok {
					return nil
				}
			}
			return one(s.written(cmd, r.Version))
		case fsm.StatusVersionMismatch:
			if cmd.Conditional {
				return mismatch()
			}
		}
	case fsm.OpDelete:
		switch r.Status {
		case fsm.StatusOK:
			if !s.exists {
				return nil
			}
			if cmd.Conditional {
				var ok bool
				if s, ok = s.versionIs(cmd.IfVersion); !ok {
					return nil
				}
			}
			return []keyState{s.removed()}
		case fsm.StatusNotFound:
			return one(s, !s.exists && (!cmd.Conditional || cmd.IfVersion == 0))
		case fsm.StatusVersionMismatch:
			if cmd.Conditional {
				return mismatch()
			}
		}
	}
	return nil
}

// stepUnknown covers a write whose client never heard back. It either took
// effect or, for a compare-and-set, found a different version and didn't.
func stepUnknown(s keyState, cmd fsm.Command) []keyState {
	var applied keyState
	switch cmd.Op {
	case fsm.OpPut:
		applied = s.writtenUnseen(cmd)
	case fsm.OpDelete:
		applied = s.removed()
	default:
		return []keyState{s}
	}
	if !cmd.Conditional {
		return []keyState{applied}
	}
	var next []keyState
	if matched, ok := s.versionIs(cmd.IfVersion); ok {
		if cmd.Op == fsm.OpPut {
			applied = matched.writtenUnseen(cmd)
		} else if !matched.exists {
			applied = matched
		}
		next = append(next, applied)
	}
	if s.versionMayDiffer(cmd.IfVersion) {
		next = append(next, s)
	}
	return next
}

// Model is the store's sequential specification for single-key operations,
// checked one key at a time (A§8.2).
func Model() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := map[string][]porcupine.Operation{}
			var keys []string
			for _, op := range history {
				k := op.Input.(fsm.Command).Key
				if _, seen := byKey[k]; !seen {
					keys = append(keys, k)
				}
				byKey[k] = append(byKey[k], op)
			}
			parts := make([][]porcupine.Operation, 0, len(keys))
			for _, k := range keys {
				parts = append(parts, byKey[k])
			}
			return parts
		},
		Init: func() []any { return []any{keyState{known: true}} },
		Step: func(state, input, output any) []any {
			var next []any
			for _, s := range step(state.(keyState), input.(fsm.Command), output.(Outcome)) {
				next = append(next, s)
			}
			return next
		},
		DescribeOperation: func(input, output any) string {
			return describe(input.(fsm.Command), output.(Outcome))
		},
		DescribeState: func(state any) string {
			s := state.(keyState)
			switch {
			case !s.exists:
				return "absent"
			case !s.known:
				return fmt.Sprintf("%q v?%s", s.value, mortal(s))
			}
			return fmt.Sprintf("%q v%d%s", s.value, s.version, mortal(s))
		},
	}
	return nm.ToModel()
}

func mortal(s keyState) string {
	if s.mortal {
		return " (may expire)"
	}
	return ""
}

func describe(cmd fsm.Command, out Outcome) string {
	var call string
	cond := ""
	if cmd.Conditional {
		cond = fmt.Sprintf(" if v%d", cmd.IfVersion)
	}
	switch cmd.Op {
	case fsm.OpGet:
		call = fmt.Sprintf("get(%s)", cmd.Key)
	case fsm.OpPut:
		if cmd.TTL > 0 {
			cond += fmt.Sprintf(" ttl %d", cmd.TTL)
		}
		call = fmt.Sprintf("put(%s, %q%s)", cmd.Key, cmd.Value, cond)
	case fsm.OpDelete:
		call = fmt.Sprintf("delete(%s%s)", cmd.Key, cond)
	}
	if out.Unknown {
		return call + " → ?"
	}
	switch out.Resp.Status {
	case fsm.StatusOK:
		if cmd.Op == fsm.OpGet {
			return fmt.Sprintf("%s → %q v%d", call, out.Resp.Value, out.Resp.Version)
		}
		return fmt.Sprintf("%s → ok v%d", call, out.Resp.Version)
	case fsm.StatusNotFound:
		return call + " → not found"
	case fsm.StatusVersionMismatch:
		return fmt.Sprintf("%s → mismatch, found v%d", call, out.Resp.Version)
	}
	return call + " → invalid"
}
