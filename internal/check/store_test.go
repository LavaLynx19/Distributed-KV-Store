package check

import (
	"testing"

	"distributed-kv-store/internal/fsm"
)

func scan(start, end string, limit uint64) fsm.Command {
	return fsm.Command{Op: fsm.OpScan, Key: start, End: end, Limit: limit}
}

func found(items ...fsm.Item) fsm.Response { return fsm.Response{Status: fsm.StatusOK, Items: items} }

func item(k, v string, version uint64) fsm.Item {
	return fsm.Item{Key: k, Value: []byte(v), Version: version}
}

func txn(conds []fsm.Cond, writes ...fsm.Write) fsm.Command {
	return fsm.Command{Op: fsm.OpTxn, Conds: conds, Writes: writes}
}

func setTo(k, v string) fsm.Write { return fsm.Write{Op: fsm.OpPut, Key: k, Value: []byte(v)} }
func drop(k string) fsm.Write     { return fsm.Write{Op: fsm.OpDelete, Key: k} }

func refused(failed ...fsm.Item) fsm.Response {
	return fsm.Response{Status: fsm.StatusVersionMismatch, Items: failed}
}

func TestStoreModel(t *testing.T) {
	tests := []struct {
		name string
		ops  []op
		want bool
	}{
		{"a scan lists what was written", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("b", "2"), 20, 30, Answered, wrote(2)},
			{scan("", "", 0), 40, 50, Answered, found(item("a", "1", 1), item("b", "2", 2))},
		}, true},
		{"a scan that misses a key", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("b", "2"), 20, 30, Answered, wrote(2)},
			{scan("", "", 0), 40, 50, Answered, found(item("a", "1", 1))},
		}, false},
		{"a scan that lists a deleted key", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{del("a"), 20, 30, Answered, deleted},
			{scan("", "", 0), 40, 50, Answered, found(item("a", "1", 1))},
		}, false},
		{"a scan mixing two moments: a's new value with b's old one", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("b", "1"), 20, 30, Answered, wrote(2)},
			{put("a", "2"), 40, 50, Answered, wrote(3)},
			{put("b", "2"), 60, 70, Answered, wrote(4)},
			{scan("", "", 0), 80, 90, Answered, found(item("a", "2", 3), item("b", "1", 2))},
		}, false},
		{"a scan overlapping a write may see either side", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("b", "2"), 20, 60, Answered, wrote(2)},
			{scan("", "", 0), 30, 40, Answered, found(item("a", "1", 1))},
		}, true},
		{"a scan stops at its end and its limit", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("b", "2"), 20, 30, Answered, wrote(2)},
			{put("c", "3"), 40, 50, Answered, wrote(3)},
			{scan("a", "c", 0), 60, 70, Answered, found(item("a", "1", 1), item("b", "2", 2))},
			{scan("", "", 1), 80, 90, Answered, found(item("a", "1", 1))},
		}, true},
		{"a scan under its limit may not skip a key", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("b", "2"), 20, 30, Answered, wrote(2)},
			{scan("", "", 2), 40, 50, Answered, found(item("b", "2", 2))},
		}, false},
		{"a scan may find a mortal key gone, and then it stays gone", []op{
			{putTTL("a", "1"), 0, 10, Answered, wrote(1)},
			{scan("", "", 0), 20, 30, Answered, found()},
			{get("a"), 40, 50, Answered, read("1", 1)},
		}, false},
		{"a transaction's writes appear together", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{txn([]fsm.Cond{{Key: "a", Version: 1}, {Key: "b", Version: 0}}, setTo("a", "0"), setTo("b", "1")), 20, 30, Answered, wrote(2)},
			{scan("", "", 0), 40, 50, Answered, found(item("a", "0", 2), item("b", "1", 2))},
		}, true},
		{"half a transaction", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{txn(nil, setTo("a", "0"), setTo("b", "1")), 20, 30, Answered, wrote(2)},
			{scan("", "", 0), 40, 50, Answered, found(item("a", "0", 2))},
		}, false},
		{"a transaction applied although a condition was false", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{put("a", "2"), 20, 30, Answered, wrote(2)},
			{txn([]fsm.Cond{{Key: "a", Version: 1}}, setTo("b", "1")), 40, 50, Answered, wrote(3)},
		}, false},
		{"a refused transaction changes nothing and names what failed", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{txn([]fsm.Cond{{Key: "a", Version: 7}, {Key: "b", Version: 0}}, setTo("b", "1")), 20, 30, Answered, refused(fsm.Item{Key: "a", Version: 1})},
			{scan("", "", 0), 40, 50, Answered, found(item("a", "1", 1))},
		}, true},
		{"a transaction refused although every condition held", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{txn([]fsm.Cond{{Key: "a", Version: 1}}, setTo("b", "1")), 20, 30, Answered, refused(fsm.Item{Key: "a", Version: 5})},
		}, false},
		{"a transaction can delete, and its last write to a key wins", []op{
			{put("a", "1"), 0, 10, Answered, wrote(1)},
			{txn(nil, setTo("b", "x"), drop("a"), setTo("b", "y")), 20, 30, Answered, wrote(2)},
			{scan("", "", 0), 40, 50, Answered, found(item("b", "y", 2))},
		}, true},
		{"a lost transaction may have happened", []op{
			{txn(nil, setTo("a", "1"), setTo("b", "1")), 0, 10, Lost, fsm.Response{}},
			{get("a"), 20, 30, Answered, read("1", 1)},
			{get("b"), 40, 50, Answered, read("1", 1)},
		}, true},
		{"a lost transaction may not have happened", []op{
			{txn(nil, setTo("a", "1"), setTo("b", "1")), 0, 10, Lost, fsm.Response{}},
			{scan("", "", 0), 20, 30, Answered, found()},
		}, true},
		{"a lost transaction can't half happen", []op{
			{txn(nil, setTo("a", "1"), setTo("b", "1")), 0, 10, Lost, fsm.Response{}},
			{get("a"), 20, 30, Answered, read("1", 1)},
			{get("b"), 40, 50, Answered, notFound},
			{get("b"), 60, 70, Answered, notFound},
			{get("a"), 80, 90, Answered, read("1", 1)},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := history(tt.ops...).Linearizable(0)
			if v.Linearizable != tt.want || v.TimedOut {
				t.Errorf("Linearizable = %v (timed out %v), want %v", v.Linearizable, v.TimedOut, tt.want)
			}
		})
	}
}
