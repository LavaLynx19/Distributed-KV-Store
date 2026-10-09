package fsm

import (
	"reflect"
	"testing"
)

func scan(start, end string, limit uint64) Command {
	return Command{Op: OpScan, Key: start, End: end, Limit: limit}
}

func txn(conds []Cond, writes ...Write) Command {
	return Command{Op: OpTxn, Conds: conds, Writes: writes}
}

func keysOf(items []Item) []string {
	var ks []string
	for _, it := range items {
		ks = append(ks, it.Key)
	}
	return ks
}

func TestScan(t *testing.T) {
	m := New()
	run(t, m, put("b", "2"), put("a", "1"), put("d", "4"), put("c", "3"), del("c"), at(10, ttl(put("e", "5"), 5)))
	read := func(c Command) Response {
		t.Helper()
		r, err := DecodeResponse(m.Read(c.Encode()))
		if err != nil || r.Status != StatusOK {
			t.Fatalf("scan answered %+v, %v", r, err)
		}
		return r
	}
	for _, tt := range []struct {
		name string
		cmd  Command
		want []string
	}{
		{"everything", scan("", "", 0), []string{"a", "b", "d", "e"}},
		{"the end is left out", scan("a", "d", 0), []string{"a", "b"}},
		{"from the middle", scan("b", "", 0), []string{"b", "d", "e"}},
		{"between keys", scan("bb", "dd", 0), []string{"d"}},
		{"limit", scan("", "", 2), []string{"a", "b"}},
		{"nothing there", scan("x", "z", 0), nil},
	} {
		if got := keysOf(read(tt.cmd).Items); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.want)
		}
	}
	if it := read(scan("b", "c", 0)).Items[0]; string(it.Value) != "2" || it.Version != 1 {
		t.Errorf("scan gave %+v for b, want value 2 at version 1", it)
	}
	// A scan sees Expiry like any other read, and works as an Entry too.
	got := run(t, m, at(15, scan("", "", 0)))
	if ks := keysOf(got[0].Items); !reflect.DeepEqual(ks, []string{"a", "b", "d"}) {
		t.Errorf("after e's deadline a scan through the Log found %v", ks)
	}
}

func TestTransactionAppliesAllOrNothing(t *testing.T) {
	m := New()
	got := run(t, m,
		put("a", "1"), // version 1
		put("b", "1"), // version 2
		// Move 1 from a to b, and drop c, given both are as last seen.
		txn([]Cond{{"a", 1}, {"b", 2}, {"c", 0}},
			Write{Op: OpPut, Key: "a", Value: []byte("0")},
			Write{Op: OpPut, Key: "b", Value: []byte("2")},
			Write{Op: OpDelete, Key: "c"}),
		// The same again must fail: both versions have moved on.
		txn([]Cond{{"a", 1}, {"b", 2}, {"c", 0}},
			Write{Op: OpPut, Key: "a", Value: []byte("-1")},
			Write{Op: OpPut, Key: "z", Value: []byte("never")}),
	)
	if got[2].Status != StatusOK || got[2].Version != 3 {
		t.Fatalf("transaction answered %+v, want ok at version 3", got[2])
	}
	if want := []Item{{Key: "a", Version: 3}, {Key: "b", Version: 3}}; got[3].Status != StatusVersionMismatch || !reflect.DeepEqual(got[3].Items, want) {
		t.Fatalf("refused transaction answered %+v, want a mismatch naming %+v", got[3], want)
	}
	want := []Item{{Key: "a", Value: []byte("0"), Version: 3}, {Key: "b", Value: []byte("2"), Version: 3}}
	if !reflect.DeepEqual(m.Items(), want) {
		t.Fatalf("items = %+v\nwant   %+v", m.Items(), want)
	}
}

func TestTransactionWithTimeToLiveAndSession(t *testing.T) {
	m := New()
	move := txn(nil, Write{Op: OpPut, Key: "lease", Value: []byte("me"), TTL: 50}, Write{Op: OpPut, Key: "count", Value: []byte("1")})
	got := run(t, m,
		at(100, Command{Op: OpOpenSession}),
		at(100, inSession(move, 1, 1)),
		at(120, inSession(move, 1, 1)), // a retry: applied once
		at(150, get("lease")),
		at(150, get("count")),
	)
	if got[2].Version != got[1].Version || got[1].Version != 2 {
		t.Fatalf("retry answered %+v, first answer %+v", got[2], got[1])
	}
	if got[3].Status != StatusNotFound || got[4].Status != StatusOK {
		t.Fatalf("at the deadline: lease %+v, count %+v", got[3], got[4])
	}
}

func TestMultiKeyRoundTrip(t *testing.T) {
	for _, c := range []Command{
		scan("a", "m", 10), scan("", "", 0), at(5, scan("x", "", 1)),
		txn(nil), txn([]Cond{{"a", 0}, {"b", 9}}),
		at(7, inSession(txn([]Cond{{"a", 1}}, Write{Op: OpPut, Key: "a", Value: []byte("v"), TTL: 3}, Write{Op: OpDelete, Key: "b"}), 4, 2)),
	} {
		got, err := DecodeCommand(c.Encode())
		if err != nil || !reflect.DeepEqual(got, c) {
			t.Errorf("round trip of %+v gave %+v, %v", c, got, err)
		}
	}
	for _, r := range []Response{
		{Status: StatusOK, Items: []Item{{Key: "a", Value: []byte("1"), Version: 3}, {Key: "b", Version: 4}}},
		{Status: StatusVersionMismatch, Items: []Item{{Key: "a", Version: 0}}},
	} {
		got, err := DecodeResponse(r.Encode())
		if err != nil || !reflect.DeepEqual(got, r) {
			t.Errorf("round trip of %+v gave %+v, %v", r, got, err)
		}
	}
	// A Transaction may write nothing but a put or a delete.
	bad := txn(nil, Write{Op: OpGet, Key: "a"}).Encode()
	if _, err := DecodeCommand(bad); err == nil {
		t.Error("a Transaction containing a get was accepted")
	}
}
