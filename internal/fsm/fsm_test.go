package fsm

import (
	"reflect"
	"testing"

	"distributed-kv-store/internal/core"
)

// run applies cmds as Entries 1, 2, 3… and returns the decoded responses.
func run(t *testing.T, m *Machine, cmds ...Command) []Response {
	t.Helper()
	var out []Response
	for i, c := range cmds {
		raw := m.Apply(core.Entry{Index: core.Index(i + 1), Kind: core.EntryCommand, Payload: c.Encode()})
		r, err := DecodeResponse(raw)
		if err != nil {
			t.Fatalf("command %d: %v", i+1, err)
		}
		out = append(out, r)
	}
	return out
}

// put builds a put. An empty value is nil, which is how it decodes.
func put(k, v string) Command {
	c := Command{Op: OpPut, Key: k}
	if v != "" {
		c.Value = []byte(v)
	}
	return c
}
func get(k string) Command { return Command{Op: OpGet, Key: k} }
func del(k string) Command { return Command{Op: OpDelete, Key: k} }
func cas(k, v string, ifVersion uint64) Command {
	return Command{Op: OpPut, Key: k, Value: []byte(v), Conditional: true, IfVersion: ifVersion}
}

func TestApply(t *testing.T) {
	ok := func(v string, version uint64) Response {
		r := Response{Status: StatusOK, Version: version}
		if v != "" {
			r.Value = []byte(v)
		}
		return r
	}
	tests := []struct {
		name string
		cmds []Command
		want []Response
	}{
		{"get missing", []Command{get("a")}, []Response{{Status: StatusNotFound}}},
		{"put then get", []Command{put("a", "1"), get("a")}, []Response{ok("", 1), ok("1", 1)}},
		{"overwrite bumps version to the Entry's Index",
			[]Command{put("a", "1"), put("a", "2"), get("a")},
			[]Response{ok("", 1), ok("", 2), ok("2", 2)}},
		{"delete then get",
			[]Command{put("a", "1"), del("a"), get("a")},
			[]Response{ok("", 1), {Status: StatusOK}, {Status: StatusNotFound}}},
		{"delete missing", []Command{del("a")}, []Response{{Status: StatusNotFound}}},
		{"compare-and-set matches",
			[]Command{put("a", "1"), cas("a", "2", 1), get("a")},
			[]Response{ok("", 1), ok("", 2), ok("2", 2)}},
		{"compare-and-set on a stale version changes nothing",
			[]Command{put("a", "1"), put("a", "2"), cas("a", "3", 1), get("a")},
			[]Response{ok("", 1), ok("", 2), {Status: StatusVersionMismatch, Version: 2}, ok("2", 2)}},
		{"create only if absent",
			[]Command{cas("a", "1", 0), cas("a", "2", 0)},
			[]Response{ok("", 1), {Status: StatusVersionMismatch, Version: 1}}},
		{"a recreated key never reuses a version",
			[]Command{put("a", "1"), del("a"), put("a", "1"), cas("a", "x", 1)},
			[]Response{ok("", 1), {Status: StatusOK}, ok("", 3), {Status: StatusVersionMismatch, Version: 3}}},
		{"conditional delete",
			[]Command{put("a", "1"), {Op: OpDelete, Key: "a", Conditional: true, IfVersion: 9}, {Op: OpDelete, Key: "a", Conditional: true, IfVersion: 1}},
			[]Response{ok("", 1), {Status: StatusVersionMismatch, Version: 1}, {Status: StatusOK}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := run(t, New(), tt.cmds...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestNoopAndGarbage(t *testing.T) {
	m := New()
	if got := m.Apply(core.Entry{Index: 1, Kind: core.EntryNoop}); got != nil {
		t.Errorf("a no-op returned %v", got)
	}
	for _, payload := range [][]byte{nil, {0}, {99, 0, 0, 0, 0}, put("a", "1").Encode()[:4], append(put("a", "1").Encode(), 0)} {
		r, err := DecodeResponse(m.Apply(core.Entry{Index: 2, Kind: core.EntryCommand, Payload: payload}))
		if err != nil || r.Status != StatusInvalid {
			t.Errorf("payload %v: got %+v, %v; want StatusInvalid", payload, r, err)
		}
	}
	if len(m.Items()) != 0 {
		t.Error("invalid Entries changed the state")
	}
}

func TestCommandRoundTrip(t *testing.T) {
	for _, c := range []Command{
		get("k"), put("k", "v"), put("", ""), del("k"), cas("key with spaces", "v", 1<<40),
		{Op: OpDelete, Key: "k", Conditional: true},
	} {
		got, err := DecodeCommand(c.Encode())
		if err != nil || !reflect.DeepEqual(got, c) {
			t.Errorf("round trip of %+v gave %+v, %v", c, got, err)
		}
	}
}

// Two Machines fed the same Entries end up identical, whatever order the keys
// were written in.
func TestItemsAreSortedAndDeterministic(t *testing.T) {
	cmds := []Command{put("c", "3"), put("a", "1"), put("b", "2"), del("c"), cas("a", "9", 2)}
	a, b := New(), New()
	run(t, a, cmds...)
	run(t, b, cmds...)
	want := []Item{{Key: "a", Value: []byte("9"), Version: 5}, {Key: "b", Value: []byte("2"), Version: 3}}
	if !reflect.DeepEqual(a.Items(), want) || !reflect.DeepEqual(a.Items(), b.Items()) {
		t.Errorf("a = %+v\nb = %+v\nwant %+v", a.Items(), b.Items(), want)
	}
}
