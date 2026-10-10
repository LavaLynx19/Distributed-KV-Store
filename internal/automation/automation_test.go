package automation

import (
	"reflect"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
)

func TestANodeIsHeldDeadOnlyAfterTheWait(t *testing.T) {
	var w Watch
	if got := w.Held(100, 50, []int{4}); got != nil {
		t.Fatalf("just given up: %v", got)
	}
	if got := w.Held(149, 50, []int{4, 5}); got != nil {
		t.Fatalf("before the wait is over: %v", got)
	}
	if got := w.Held(150, 50, []int{5, 4}); !reflect.DeepEqual(got, []int{4}) {
		t.Fatalf("after the wait: %v", got)
	}
	// A Node heard from again starts over.
	w.Held(160, 50, []int{5})
	if got := w.Held(170, 50, []int{4, 5}); got != nil {
		t.Fatalf("node 4 came back and went again: %v", got)
	}
}

func TestDeadNeedsAMajorityOfThoseWithASay(t *testing.T) {
	reports := map[int]shard.Report{1: {Dead: []int{4, 5}}, 2: {Dead: []int{4}}, 3: {Dead: []int{5, 4}}}
	all := func(n int) (shard.Report, bool) { return reports[n], true }
	if got := Dead([]int{1, 2, 3}, all); !reflect.DeepEqual(got, []int{4, 5}) {
		t.Fatalf("three voters: %v", got)
	}
	// With voter 3 gone, node 5 has one vote of three.
	without3 := func(n int) (shard.Report, bool) { return reports[n], n != 3 }
	if got := Dead([]int{1, 2, 3}, without3); !reflect.DeepEqual(got, []int{4}) {
		t.Fatalf("voter 3 has no say: %v", got)
	}
	// One voter alone is never a Majority of three.
	alone := func(n int) (shard.Report, bool) { return reports[n], n == 1 }
	if got := Dead([]int{1, 2, 3}, alone); got != nil {
		t.Fatalf("one voter of three: %v", got)
	}
}

func TestReplacementPicksADeadMemberAndAFreeSpare(t *testing.T) {
	table := meta.NewPlaced(8, 2, 5, 3).Table() // Meta on 1,2,3; Group 1 on 2,3,4; Group 2 on 3,4,5
	if out, in, ok := Replacement(table, []int{4}, []int{1, 2, 3, 5, 6, 7}); !ok || out != 4 || in != 6 {
		t.Fatalf("node 4 dead, nodes 6 and 7 free: %d %d %v", out, in, ok)
	}
	if _, _, ok := Replacement(table, []int{4}, []int{1, 2, 3, 5}); ok {
		t.Fatal("no Spare, and a replacement was picked")
	}
	if _, _, ok := Replacement(table, []int{7}, []int{1, 2, 3, 4, 5, 6}); ok {
		t.Fatal("a dead Spare needs no replacing")
	}
	// Node 6 was on its way into Group 1 and died: node 7 takes over.
	table.Groups[1].Add, table.Groups[1].Remove = 6, 4
	table.Groups[2].Add, table.Groups[2].Remove = 6, 4
	if out, in, ok := Replacement(table, []int{4, 6}, []int{1, 2, 3, 5, 7}); !ok || out != 6 || in != 7 {
		t.Fatalf("the Spare died too: %d %d %v", out, in, ok)
	}
}

func TestALeadersStepsThroughAChange(t *testing.T) {
	ids := func(ns ...int) []core.NodeID {
		var out []core.NodeID
		for _, n := range ns {
			out = append(out, core.NodeID(shard.ReplicaID(n, 1)))
		}
		return out
	}
	leader := func(at core.Index, members ...int) core.Status {
		return core.Status{Role: core.LeaderRole, Members: ids(members...), MembersAt: at}
	}
	row := shard.Group{Members: []int{2, 3, 4}, Add: 6, Remove: 4}
	// Add first.
	if s := Members(1, row, leader(0, 2, 3, 4)); !reflect.DeepEqual(s.Reconfigure, ids(2, 3, 4, 6)) || s.Report != nil {
		t.Fatalf("first step: %+v", s)
	}
	// Nothing while that is under way.
	busy := leader(0, 2, 3, 4)
	busy.Changing, busy.Learner = true, ids(6)[0]
	if s := Members(1, row, busy); s.Reconfigure != nil || s.Report != nil {
		t.Fatalf("while node 6 catches up: %+v", s)
	}
	// Then say so, and wait for the table to catch up.
	if s := Members(1, row, leader(9, 2, 3, 4, 6)); s.Report == nil || s.Report.At != 9 || !reflect.DeepEqual(s.Report.Members, []int{2, 3, 4, 6}) || s.Reconfigure != nil {
		t.Fatalf("after adding: %+v", s)
	}
	row.Members, row.At = []int{2, 3, 4, 6}, 9
	if s := Members(1, row, leader(9, 2, 3, 4, 6)); !reflect.DeepEqual(s.Reconfigure, ids(2, 3, 6)) {
		t.Fatalf("then remove: %+v", s)
	}
	// A table from before a later change makes the Leader do nothing.
	if s := Members(1, shard.Group{Members: []int{2, 3, 4}, Add: 6, Remove: 4}, leader(30, 2, 3, 4)); s.Reconfigure != nil || s.Report == nil {
		t.Fatalf("an old table: %+v", s)
	}
	// The Node being added is no longer the one wanted: call it off.
	row = shard.Group{Members: []int{2, 3, 4}, Add: 7, Remove: 4}
	if s := Members(1, row, busy); !reflect.DeepEqual(s.Reconfigure, ids(2, 3, 4)) {
		t.Fatalf("the wish changed: %+v", s)
	}
}

func TestGoneNeedsACommittedListNoOlderThanItsOwn(t *testing.T) {
	me := core.NodeID(401)
	leader := core.Status{Role: core.LeaderRole, Members: []core.NodeID{201, 301, 601}, MembersAt: 12}
	if !Gone(me, core.Status{MembersAt: 0}, leader) {
		t.Fatal("left out of a Committed list newer than its own")
	}
	if Gone(me, core.Status{MembersAt: 20}, leader) {
		t.Fatal("it holds a newer list than the Leader showed")
	}
	changing := leader
	changing.Changing = true
	if Gone(me, core.Status{}, changing) {
		t.Fatal("the Leader's list isn't Committed")
	}
	follower := leader
	follower.Role = core.Follower
	if Gone(me, core.Status{}, follower) {
		t.Fatal("the answer didn't come from a Leader")
	}
	in := leader
	in.Members = []core.NodeID{201, 301, 401}
	if Gone(me, core.Status{}, in) {
		t.Fatal("it is in the list")
	}
}

func TestBalancer(t *testing.T) {
	cfg := Balancing{High: 1.5, Low: 1.2, Rest: 1000, Settle: 500, Idle: 10}
	table := meta.New(6, 3).Table() // Slots 0,3 to Group 1; 1,4 to Group 2; 2,5 to Group 3
	table.StoreTime = 5000
	load := []uint32{300, 20, 20, 200, 20, 20} // Group 1: 500, Groups 2 and 3: 40 each
	var b Balancer
	if _, _, ok := b.Decide(cfg, table, load, 3, 1, 1000, true); ok {
		t.Fatal("a new Leader decided at once")
	}
	if _, _, ok := b.Decide(cfg, table, load, 3, 1, 1600, false); ok {
		t.Fatal("decided while a Node seemed not to be alive")
	}
	// The gap is 460: Slot 3 at 200 is the biggest no more than half of it.
	if slot, to, ok := b.Decide(cfg, table, load, 3, 1, 1600, true); !ok || slot != 3 || to != 2 {
		t.Fatalf("picked Slot %d for Group %d, %v", slot, to, ok)
	}
	// One Move at a time, and none just after one.
	moving := table.Clone()
	moving.Slots[4].MovingTo = 3
	if _, _, ok := b.Decide(cfg, moving, load, 3, 1, 1700, true); ok {
		t.Fatal("decided while a Slot was moving")
	}
	settling := table.Clone()
	settling.Slots[4].MovedAt = 4800
	if _, _, ok := b.Decide(cfg, settling, load, 3, 1, 1700, true); ok {
		t.Fatal("decided just after a Move")
	}
	// A Slot that moved lately rests: the next best goes.
	rested := table.Clone()
	rested.Slots[3].MovedAt = 4400
	if _, _, ok := b.Decide(cfg, rested, load, 3, 1, 1700, true); ok {
		t.Fatal("Slot 0 is more than half the gap and Slot 3 is resting: nothing should move")
	}
	// One Slot busier than everything else: no Move helps, so none is made.
	if _, _, ok := b.Decide(cfg, table, []uint32{900, 20, 20, 0, 20, 20}, 3, 1, 1700, true); ok {
		t.Fatal("moved a Slot that is the whole problem")
	}
	// Between the lines it carries on only if it had started.
	between := []uint32{40, 50, 40, 120, 50, 40} // Group 1 at 1.41 of the mean
	if _, _, ok := (&Balancer{}).Decide(cfg, table, between, 3, 0, 1700, true); ok {
		t.Fatal("started below the high line")
	}
	if _, _, ok := b.Decide(cfg, table, between, 3, 1, 1700, true); !ok {
		t.Fatal("stopped above the low line")
	}
	// The naive one moves the busiest Slot, whatever it is.
	naive := cfg
	naive.Naive = true
	if slot, to, ok := (&Balancer{}).Decide(naive, table, []uint32{900, 20, 20, 0, 20, 20}, 3, 1, 0, false); !ok || slot != 0 || to != 2 {
		t.Fatalf("naive picked Slot %d for Group %d, %v", slot, to, ok)
	}
	// A store that is nearly idle is left alone.
	if _, _, ok := b.Decide(cfg, table, []uint32{9, 1, 1, 9, 1, 1}, 3, 1, 1700, true); ok {
		t.Fatal("moved a Slot in an idle store")
	}
}

func TestMeterSmoothsAndReportsOnlyWhatItLeads(t *testing.T) {
	var m Meter
	m.Fold([]uint32{100, 100, 0}, 0.25)
	m.Fold([]uint32{100, 0, 0}, 0.25)
	leads := func(s int) bool { return s != 1 }
	if got := m.Load(leads, false); !reflect.DeepEqual(got, []uint32{44, 0, 0}) {
		t.Fatalf("smoothed: %v", got)
	}
	if got := m.Load(leads, true); !reflect.DeepEqual(got, []uint32{100, 0, 0}) {
		t.Fatalf("latest: %v", got)
	}
	if got := m.Load(func(int) bool { return false }, false); got != nil {
		t.Fatalf("leading nothing: %v", got)
	}
	if got := MergeLoad(3, []shard.Report{{Load: []uint32{44, 0, 0}}, {}, {Load: []uint32{0, 7, 0}}}); !reflect.DeepEqual(got, []uint32{44, 7, 0}) {
		t.Fatalf("merged: %v", got)
	}
}
