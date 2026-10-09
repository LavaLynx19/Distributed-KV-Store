package raft

import (
	"slices"
	"testing"

	"distributed-kv-store/internal/core"
)

// settledLeader is node 1 leading {1,2,3} with its first Entry Committed.
func settledLeader(t *testing.T) *Node {
	t.Helper()
	n := newNode(1, 3)
	elect(t, n, 2)
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1})
	if !n.ownTermCommitted() {
		t.Fatal("the Leader's first Entry should be Committed")
	}
	return n
}

func ids(list ...core.NodeID) []core.NodeID { return list }

func resultOf(t *testing.T, out core.Output, ref uint64) core.Result {
	t.Helper()
	for _, r := range out.Results {
		if r.Ref == ref {
			return r
		}
	}
	t.Fatalf("no result for request %d in %+v", ref, out.Results)
	return core.Result{}
}

// appendsTo lists who an Output sends Appends to.
func appendsTo(out core.Output) []core.NodeID {
	var to []core.NodeID
	for _, m := range out.Messages {
		if _, ok := m.Body.(Append); ok && !slices.Contains(to, m.To) {
			to = append(to, m.To)
		}
	}
	slices.Sort(to)
	return to
}

func TestOnlySingleChangesAreTaken(t *testing.T) {
	for _, tt := range []struct {
		name string
		want []core.NodeID
	}{
		{"two added", ids(1, 2, 3, 4, 5)},
		{"two removed", ids(1)},
		{"one in, one out", ids(1, 2, 4)},
		{"nothing changed", ids(1, 2, 3)},
		{"nobody left", nil},
	} {
		n := settledLeader(t)
		out := n.Step(core.Reconfigure{Ref: 9, Members: tt.want})
		if r := resultOf(t, out, 9); r.Reason != core.Invalid {
			t.Errorf("%s: got %v, want Invalid", tt.name, r.Reason)
		}
		if !slices.Equal(n.members, ids(1, 2, 3)) || out.Persist != nil {
			t.Errorf("%s: a refused change left a mark: members %v, persist %+v", tt.name, n.members, out.Persist)
		}
	}
}

func TestOneChangeAtATime(t *testing.T) {
	n := settledLeader(t)
	out := n.Step(core.Reconfigure{Ref: 1, Members: ids(1, 2)})
	if len(out.Results) != 0 || !slices.Equal(n.members, ids(1, 2)) {
		t.Fatalf("a removal should be in force as soon as it is in the Log: members %v, results %+v", n.members, out.Results)
	}
	if e := out.Persist.Entries[0]; e.Kind != core.EntryMembers || e.Index != 2 {
		t.Fatalf("stored %+v, want a Membership change at Index 2", e)
	}
	// Not Committed yet: a second change must wait.
	if r := resultOf(t, n.Step(core.Reconfigure{Ref: 2, Members: ids(1)}), 2); r.Reason != core.Busy {
		t.Fatalf("a second change while the first is uncommitted got %v, want Busy", r.Reason)
	}
	out = recv(n, 2, AppendReply{Term: 1, Success: true, Match: 2})
	if r := resultOf(t, out, 1); r.Reason != core.OK || r.Index != 2 {
		t.Fatalf("after commit: %+v, want OK at Index 2", r)
	}
	// Node 3 is no longer sent anything.
	if to := appendsTo(n.Step(core.Tick{})); !slices.Equal(to, ids(2)) {
		t.Fatalf("heartbeats went to %v, want only node 2", to)
	}
	// With the first change Committed the next is taken. A Group of one
	// commits it by itself.
	if r := resultOf(t, n.Step(core.Reconfigure{Ref: 3, Members: ids(1)}), 3); r.Reason != core.OK {
		t.Fatalf("the next change: %+v, want OK", r)
	}
}

// A new Leader may not change the list until it has Committed an Entry of
// its own Term.
func TestNoChangeBeforeOwnTermCommit(t *testing.T) {
	n := newNode(1, 3)
	elect(t, n, 2)
	if r := resultOf(t, n.Step(core.Reconfigure{Ref: 1, Members: ids(1, 2)}), 1); r.Reason != core.Busy {
		t.Fatalf("got %v, want Busy", r.Reason)
	}
	if !slices.Equal(n.members, ids(1, 2, 3)) {
		t.Fatalf("members = %v", n.members)
	}
}

func TestNewMemberCatchesUpBeforeItCounts(t *testing.T) {
	n := settledLeader(t)
	n.Step(core.Propose{Ref: 50, Payload: []byte("x")})
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 2}) // Entry 2 Committed

	out := n.Step(core.Reconfigure{Ref: 1, Members: ids(1, 2, 3, 4)})
	if out.Persist != nil || !slices.Equal(n.members, ids(1, 2, 3)) {
		t.Fatalf("nothing should change until node 4 has caught up: members %v, persist %+v", n.members, out.Persist)
	}
	if to := appendsTo(out); !slices.Equal(to, ids(4)) {
		t.Fatalf("the Leader should start sending node 4 the Log, sent to %v", to)
	}
	// It is behind, and says so. It is sent what it lacks and still doesn't count.
	out = recv(n, 4, AppendReply{Term: 1, Match: 0})
	if out.Persist != nil || n.majority() != 2 {
		t.Fatalf("a learner that is behind changed something: %+v", out.Persist)
	}
	// It has everything Committed: now the change is appended.
	out = recv(n, 4, AppendReply{Term: 1, Success: true, Match: 2})
	if out.Persist == nil || out.Persist.Entries[0].Kind != core.EntryMembers || !slices.Equal(n.members, ids(1, 2, 3, 4)) {
		t.Fatalf("once caught up the change should be appended: members %v, persist %+v", n.members, out.Persist)
	}
	// Four Members need three: node 2 alone no longer commits it.
	if out = recv(n, 2, AppendReply{Term: 1, Success: true, Match: 3}); len(out.Results) != 0 {
		t.Fatalf("Committed with 2 of 4: %+v", out.Results)
	}
	if r := resultOf(t, recv(n, 4, AppendReply{Term: 1, Success: true, Match: 3}), 1); r.Reason != core.OK {
		t.Fatalf("with 3 of 4: %+v", r)
	}
}

func TestNewMemberThatNeverCatchesUpIsGivenUpOn(t *testing.T) {
	n := settledLeader(t)
	n.Step(core.Reconfigure{Ref: 1, Members: ids(1, 2, 3, 4)})
	var got *core.Result
	for i := 0; i < learnerPatience*10+5 && got == nil; i++ {
		out := n.Step(core.Tick{})
		for _, r := range append(out.Results, recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1}).Results...) {
			got = &r
		}
	}
	if got == nil || got.Ref != 1 || got.Reason != core.NoCatchUp {
		t.Fatalf("got %+v, want NoCatchUp for request 1", got)
	}
	if to := appendsTo(n.Step(core.Tick{})); slices.Contains(to, 4) || !slices.Equal(n.members, ids(1, 2, 3)) {
		t.Fatalf("after giving up: still sending to %v, members %v", to, n.members)
	}
	// And the way is clear for another change.
	if out := n.Step(core.Reconfigure{Ref: 2, Members: ids(1, 2)}); len(out.Results) != 0 {
		t.Fatalf("a change after giving up was refused: %+v", out.Results)
	}
}

// A Leader that removes itself leads until the change is Committed, without
// counting itself, and then steps down for good.
func TestLeaderRemovesItself(t *testing.T) {
	n := settledLeader(t)
	n.Step(core.Reconfigure{Ref: 1, Members: ids(2, 3)})
	if n.role != core.LeaderRole {
		t.Fatal("it must go on leading until the change is Committed")
	}
	// One of the two remaining Members isn't a Majority of them.
	if out := recv(n, 2, AppendReply{Term: 1, Success: true, Match: 2}); len(out.Results) != 0 {
		t.Fatalf("Committed with 1 of 2: %+v", out.Results)
	}
	out := recv(n, 3, AppendReply{Term: 1, Success: true, Match: 2})
	if r := resultOf(t, out, 1); r.Reason != core.OK {
		t.Fatalf("after both acknowledged: %+v", r)
	}
	if n.role != core.Follower {
		t.Fatalf("role = %v after its own removal was Committed", n.role)
	}
	for range 100 {
		if out := n.Step(core.Tick{}); len(out.Messages) != 0 || n.role != core.Follower {
			t.Fatalf("a Node outside the Group stood for election: %+v", out.Messages)
		}
	}
}

func TestNodeOutsideTheGroup(t *testing.T) {
	// Node 4 starts as a spare: the Group is {1,2,3}.
	spare := New(Config{ID: 4, Members: ids(1, 2, 3), ElectionTicks: 10, HeartbeatTicks: 1, Rand: new(counter)})
	for range 100 {
		if out := spare.Step(core.Tick{}); len(out.Messages) != 0 {
			t.Fatalf("a spare stood for election: %+v", out.Messages)
		}
	}
	// A Member takes nothing from a Node that isn't in its list: no vote,
	// and no Term either.
	n := newNode(1, 3)
	if out := recv(n, 4, RequestVote{Term: 99, LastIndex: 50, LastTerm: 98}); len(out.Messages) != 0 || n.term != 0 {
		t.Fatalf("a request from outside the Group was acted on: term %d, %+v", n.term, out.Messages)
	}
	// The spare learns it is a Member from the Log, and then it may stand.
	change := core.Entry{Index: 1, Term: 1, Kind: core.EntryMembers, Payload: encodeMembers(ids(1, 2, 3, 4))}
	recv(spare, 1, Append{Term: 1, Entries: []core.Entry{change}})
	if !slices.Equal(spare.Status().Members, ids(1, 2, 3, 4)) {
		t.Fatalf("members = %v", spare.Status().Members)
	}
	stood := false
	for range 100 {
		for _, m := range spare.Step(core.Tick{}).Messages {
			if _, ok := m.Body.(RequestVote); ok {
				stood = true
			}
		}
	}
	if !stood {
		t.Fatal("a Node that was added never stood for election")
	}
}

// A change that is replaced in the Log before it Commits is forgotten, and
// the list before it is in force again.
func TestReplacedChangeIsForgotten(t *testing.T) {
	n := newNode(3, 3)
	change := core.Entry{Index: 2, Term: 1, Kind: core.EntryMembers, Payload: encodeMembers(ids(1, 2, 3, 4))}
	recv(n, 1, Append{Term: 1, Entries: []core.Entry{{Index: 1, Term: 1}, change}})
	if !slices.Equal(n.members, ids(1, 2, 3, 4)) {
		t.Fatalf("members = %v, want the new list as soon as it is in the Log", n.members)
	}
	// A Leader of a later Term has something else at Index 2.
	recv(n, 2, Append{Term: 2, PrevIndex: 1, PrevTerm: 1, Entries: []core.Entry{{Index: 2, Term: 2, Kind: core.EntryCommand}}})
	if !slices.Equal(n.members, ids(1, 2, 3)) {
		t.Fatalf("members = %v after the change was replaced, want the founders", n.members)
	}
}

func TestSnapshotCarriesTheMemberList(t *testing.T) {
	n := settledLeader(t)
	// Unchanged: a Snapshot says nothing about Members.
	if out := n.Step(core.Snapshotted{Index: 1, Data: []byte("s1")}); out.Persist.Snapshot.Members != nil {
		t.Fatalf("a Group that never changed stored a list: %v", out.Persist.Snapshot.Members)
	}
	n.Step(core.Reconfigure{Ref: 1, Members: ids(1, 2)})
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 2})
	n.Step(core.Propose{Ref: 2, Payload: []byte("x")})
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 3})

	out := n.Step(core.Snapshotted{Index: 3, Data: []byte("s3")})
	snap := out.Persist.Snapshot
	if !slices.Equal(snap.Members, ids(1, 2)) {
		t.Fatalf("snapshot members = %v, want [1 2]", snap.Members)
	}
	// A Member restarted from that Snapshot, with stale flags, knows the list.
	restarted := New(Config{ID: 1, Members: ids(1, 2, 3), ElectionTicks: 10, HeartbeatTicks: 1, Rand: new(counter),
		Stored: core.Stored{HardState: core.HardState{Term: 1}, Snapshot: snap}})
	if !slices.Equal(restarted.Status().Members, ids(1, 2)) {
		t.Fatalf("after restart members = %v, want [1 2]", restarted.Status().Members)
	}
	// And one restarted from the Log alone reads it from the change Entry.
	change := core.Entry{Index: 2, Term: 1, Kind: core.EntryMembers, Payload: encodeMembers(ids(1, 2))}
	fromLog := New(Config{ID: 1, Members: ids(1, 2, 3), ElectionTicks: 10, HeartbeatTicks: 1, Rand: new(counter),
		Stored: core.Stored{HardState: core.HardState{Term: 1}, Entries: []core.Entry{{Index: 1, Term: 1}, change}}})
	if !slices.Equal(fromLog.Status().Members, ids(1, 2)) {
		t.Fatalf("from the Log members = %v, want [1 2]", fromLog.Status().Members)
	}
	// A follower sent the Snapshot takes its list with it.
	follower := newNode(3, 3)
	recv(follower, 1, InstallSnapshot{Term: 1, Snapshot: *snap})
	if !slices.Equal(follower.Status().Members, ids(1, 2)) {
		t.Fatalf("after installing the Snapshot members = %v, want [1 2]", follower.Status().Members)
	}
}

// Unsafe recovery (A§6.6): a forced list overrides every change the Member
// held, and the first survivor to lead writes it into the Log.
func TestForcedMembers(t *testing.T) {
	old := core.Entry{Index: 2, Term: 1, Kind: core.EntryMembers, Payload: encodeMembers(ids(1, 2, 3, 4, 5))}
	stored := core.Stored{
		HardState: core.HardState{Term: 3},
		Entries:   []core.Entry{{Index: 1, Term: 1}, old, {Index: 3, Term: 2}},
		Forced:    &core.ForcedMembers{Members: ids(2, 1), At: 3},
	}
	if list, ok := StoredMembers(core.Stored{Entries: stored.Entries}); !ok || !slices.Equal(list, ids(1, 2, 3, 4, 5)) {
		t.Fatalf("before forcing the disk says %v %v, want the list of five", list, ok)
	}
	if _, ok := StoredMembers(core.Stored{Entries: stored.Entries[:1]}); ok {
		t.Fatal("a disk with no change on it claimed to know the Member list")
	}

	n := New(Config{ID: 1, Members: ids(1, 2, 3), ElectionTicks: 10, HeartbeatTicks: 1, Rand: new(counter), Stored: stored})
	if !slices.Equal(n.Status().Members, ids(1, 2)) {
		t.Fatalf("members = %v, want the forced [1 2]", n.Status().Members)
	}
	// Two Members need both. With node 2's vote it leads, and its first
	// act is to put the list in the Log.
	out := elect(t, n, 2)
	var kinds []core.EntryKind
	for _, e := range out.Persist.Entries {
		kinds = append(kinds, e.Kind)
	}
	if !slices.Equal(kinds, []core.EntryKind{core.EntryNoop, core.EntryMembers}) {
		t.Fatalf("a recovered Leader appended %v, want a no-op and then the Member list", kinds)
	}
	if list, _ := decodeMembers(out.Persist.Entries[1].Payload); !slices.Equal(list, ids(1, 2)) {
		t.Fatalf("the list it appended is %v", list)
	}

	// Restarted with that Entry in its Log, it reads the list from there.
	stored.Entries = append(stored.Entries, out.Persist.Entries...)
	again := New(Config{ID: 1, Members: ids(1, 2, 3), ElectionTicks: 10, HeartbeatTicks: 1, Rand: new(counter), Stored: stored})
	if !slices.Equal(again.Status().Members, ids(1, 2)) || again.forced {
		t.Fatalf("after restart: members %v, still marked forced %v", again.Status().Members, again.forced)
	}
	// A change made after the recovery stands over the forced list.
	later := core.Entry{Index: 6, Term: 4, Kind: core.EntryMembers, Payload: encodeMembers(ids(1, 2, 6))}
	stored.Entries = append(stored.Entries, later)
	grown := New(Config{ID: 1, Members: ids(1, 2, 3), ElectionTicks: 10, HeartbeatTicks: 1, Rand: new(counter), Stored: stored})
	if !slices.Equal(grown.Status().Members, ids(1, 2, 6)) {
		t.Fatalf("members = %v, want the later change [1 2 6]", grown.Status().Members)
	}
}
