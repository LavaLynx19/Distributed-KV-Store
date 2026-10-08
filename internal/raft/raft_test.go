package raft

import (
	"reflect"
	"testing"

	"distributed-kv-store/internal/core"
)

// counter is a predictable core.Rand.
type counter uint64

func (c *counter) Uint64() uint64 { *c++; return uint64(*c) }

func newNode(id core.NodeID, members int) *Node {
	ids := make([]core.NodeID, members)
	for i := range ids {
		ids[i] = core.NodeID(i + 1)
	}
	return New(Config{ID: id, Members: ids, ElectionTicks: 10, HeartbeatTicks: 1, Rand: new(counter)})
}

func recv(n *Node, from core.NodeID, body any) core.Output {
	return n.Step(core.Receive{Msg: core.Message{From: from, To: n.id, Body: body}})
}

// reply returns the single message a step produced.
func reply[T any](t *testing.T, out core.Output) T {
	t.Helper()
	if len(out.Messages) != 1 {
		t.Fatalf("want 1 message, got %d: %+v", len(out.Messages), out.Messages)
	}
	body, ok := out.Messages[0].Body.(T)
	if !ok {
		t.Fatalf("unexpected reply %T", out.Messages[0].Body)
	}
	return body
}

// elect ticks n until it stands for election, then hands it the votes.
func elect(t *testing.T, n *Node, voters ...core.NodeID) core.Output {
	t.Helper()
	for i := 0; n.role != core.Candidate; i++ {
		if i > 100 {
			t.Fatal("never started an election")
		}
		n.Step(core.Tick{})
	}
	var out core.Output
	for _, v := range voters {
		out = recv(n, v, VoteReply{Term: n.term, Granted: true})
	}
	if n.role != core.LeaderRole {
		t.Fatalf("not elected: %v", n)
	}
	return out
}

func entries(terms ...core.Term) []core.Entry {
	es := make([]core.Entry, len(terms))
	for i, term := range terms {
		es[i] = core.Entry{Index: core.Index(i + 1), Term: term, Kind: core.EntryCommand}
	}
	return es
}

func logTerms(n *Node) []core.Term {
	terms := make([]core.Term, len(n.log.entries))
	for i, e := range n.log.entries {
		terms[i] = e.Term
	}
	return terms
}

func TestOneVotePerTerm(t *testing.T) {
	n := newNode(1, 3)
	if v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 1})); !v.Granted {
		t.Fatal("first candidate of the Term should get the vote")
	}
	if v := reply[VoteReply](t, recv(n, 3, RequestVote{Term: 1})); v.Granted {
		t.Fatal("a second candidate in the same Term got a vote too")
	}
	if v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 1})); !v.Granted {
		t.Fatal("the same candidate asking again should be told yes again")
	}
	if v := reply[VoteReply](t, recv(n, 3, RequestVote{Term: 2})); !v.Granted || v.Term != 2 {
		t.Fatalf("a new Term means a new vote, got %+v", v)
	}
	if v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 1})); v.Granted || v.Term != 2 {
		t.Fatalf("a stale candidate should be refused and told the Term, got %+v", v)
	}
}

func TestVoteOnlyForUpToDateLog(t *testing.T) {
	n := newNode(1, 3)
	recv(n, 2, Append{Term: 2, Entries: entries(1, 2)}) // n's Log ends at Index 2, Term 2

	tests := []struct {
		name      string
		lastIndex core.Index
		lastTerm  core.Term
		want      bool
	}{
		{"shorter Log, same last Term", 1, 2, false},
		{"longer Log, older last Term", 9, 1, false},
		{"same Log", 2, 2, true},
		{"longer Log, same last Term", 3, 2, true},
		{"shorter Log, newer last Term", 1, 3, true},
	}
	term := core.Term(2)
	for _, tt := range tests {
		term++ // a fresh Term each time, so the one-vote rule doesn't interfere
		v := reply[VoteReply](t, recv(n, 3, RequestVote{Term: term, LastIndex: tt.lastIndex, LastTerm: tt.lastTerm}))
		if v.Granted != tt.want {
			t.Errorf("%s: granted = %v, want %v", tt.name, v.Granted, tt.want)
		}
	}
}

func TestFollowerChecksAndRepairsItsLog(t *testing.T) {
	n := newNode(1, 3)
	recv(n, 2, Append{Term: 1, Entries: entries(1, 1, 1)})

	// A gap: the Leader assumes Entries this follower doesn't have.
	if r := reply[AppendReply](t, recv(n, 2, Append{Term: 2, PrevIndex: 5, PrevTerm: 1})); r.Success || r.Match != 3 {
		t.Fatalf("gap: got %+v, want failure with hint 3", r)
	}
	// A mismatch at PrevIndex.
	if r := reply[AppendReply](t, recv(n, 2, Append{Term: 2, PrevIndex: 3, PrevTerm: 2})); r.Success || r.Match != 2 {
		t.Fatalf("mismatch: got %+v, want failure with hint 2", r)
	}
	// A conflicting Entry replaces the follower's own, and everything after.
	r := reply[AppendReply](t, recv(n, 2, Append{Term: 2, PrevIndex: 1, PrevTerm: 1, Entries: []core.Entry{{Index: 2, Term: 2}}}))
	if !r.Success || r.Match != 2 {
		t.Fatalf("repair: got %+v, want success at 2", r)
	}
	if got := logTerms(n); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("Log terms = %v, want [1 2]", got)
	}
	// A duplicate of an old Append changes nothing.
	recv(n, 2, Append{Term: 2, PrevIndex: 0, Entries: []core.Entry{{Index: 1, Term: 1}}})
	if len(n.log.entries) != 2 {
		t.Fatalf("a duplicate Append truncated the Log to %d", len(n.log.entries))
	}
}

func TestFollowerCommitsOnlyWhatWasConfirmed(t *testing.T) {
	n := newNode(1, 3)
	recv(n, 2, Append{Term: 1, Entries: entries(1, 1, 1)})
	// The Leader has Committed up to 9, but this Append only confirms Index 2.
	out := recv(n, 2, Append{Term: 1, PrevIndex: 2, PrevTerm: 1, Commit: 9})
	if n.commit != 2 || len(out.Committed) != 2 {
		t.Fatalf("commit = %d with %d Entries delivered, want 2 and 2", n.commit, len(out.Committed))
	}
	// Each Committed Entry is delivered exactly once.
	out = recv(n, 2, Append{Term: 1, PrevIndex: 3, PrevTerm: 1, Commit: 9})
	if len(out.Committed) != 1 || out.Committed[0].Index != 3 {
		t.Fatalf("second delivery = %+v, want only Index 3", out.Committed)
	}
}

// Raft's Figure 8: an Entry from an earlier Term held by a Majority is not
// Committed until an Entry of the Leader's own Term is.
func TestLeaderCommitsOnlyThroughItsOwnTerm(t *testing.T) {
	n := newNode(1, 5)
	elect(t, n, 2, 3) // Leader of Term 1; Log: no-op@1
	n.Step(core.Propose{Ref: 1, Payload: []byte("x")})
	if got := logTerms(n); len(got) != 2 {
		t.Fatalf("Log = %v, want two Term 1 Entries", got)
	}

	// Deposed before anything commits, then elected again in a later Term.
	out := recv(n, 4, RequestVote{Term: 2, LastIndex: 9, LastTerm: 1})
	if len(out.Results) != 1 || out.Results[0].Reason != core.Unknown {
		t.Fatalf("stepping down should end the pending proposal as Unknown, got %+v", out.Results)
	}
	elect(t, n, 2, 3)
	if n.term != 3 || len(n.log.entries) != 3 {
		t.Fatalf("want Leader of Term 3 with a new no-op, got %v", n)
	}

	// A Majority (1, 2, 3) now holds the Term 1 Entries. Not enough.
	recv(n, 2, AppendReply{Term: 3, Success: true, Match: 2})
	recv(n, 3, AppendReply{Term: 3, Success: true, Match: 2})
	if n.commit != 0 {
		t.Fatalf("Committed Index %d on the strength of old-Term Entries", n.commit)
	}
	// Once the Term 3 no-op is on a Majority, everything before commits too.
	recv(n, 2, AppendReply{Term: 3, Success: true, Match: 3})
	out = recv(n, 3, AppendReply{Term: 3, Success: true, Match: 3})
	if n.commit != 3 || len(out.Committed) != 3 {
		t.Fatalf("commit = %d, delivered %d; want 3 and 3", n.commit, len(out.Committed))
	}
}

func TestLeaderBacksUpForALaggingFollower(t *testing.T) {
	n := newNode(1, 3)
	elect(t, n, 2)
	for i := range 5 {
		n.Step(core.Propose{Ref: uint64(i), Payload: []byte("x")})
	}
	// Follower 3 says it has nothing.
	a := reply[Append](t, recv(n, 3, AppendReply{Term: 1, Match: 0}))
	if a.PrevIndex != 0 || len(a.Entries) != 6 {
		t.Fatalf("resend starts after %d with %d Entries, want 0 and 6", a.PrevIndex, len(a.Entries))
	}
	// A late failure must not drag the Leader behind what's confirmed.
	recv(n, 3, AppendReply{Term: 1, Success: true, Match: 6})
	a = reply[Append](t, recv(n, 3, AppendReply{Term: 1, Match: 2}))
	if a.PrevIndex != 6 {
		t.Fatalf("after a stale failure the Leader resends from %d, want 6", a.PrevIndex)
	}
}

func TestProposalAnswers(t *testing.T) {
	n := newNode(1, 3)

	// Nobody leads yet.
	out := n.Step(core.Propose{Ref: 1})
	if len(out.Results) != 1 || out.Results[0].Reason != core.NoMajority {
		t.Fatalf("with no known Leader: %+v, want NoMajority", out.Results)
	}
	// A follower points at its Leader.
	recv(n, 2, Append{Term: 1})
	out = n.Step(core.Propose{Ref: 2})
	if len(out.Results) != 1 || out.Results[0].Reason != core.NotLeader || out.Results[0].Leader != 2 {
		t.Fatalf("as a follower: %+v, want NotLeader with hint 2", out.Results)
	}

	// A Leader answers when the Entry commits, in the same Output.
	elect(t, n, 3)
	out = n.Step(core.Propose{Ref: 3, Payload: []byte("x")})
	if len(out.Results) != 0 {
		t.Fatalf("answered before a Majority held the Entry: %+v", out.Results)
	}
	index := n.lastIndex()
	out = recv(n, 3, AppendReply{Term: n.term, Success: true, Match: index})
	if len(out.Results) != 1 || out.Results[0] != (core.Result{Ref: 3, Reason: core.OK, Index: index}) {
		t.Fatalf("on commit: %+v, want OK at %d", out.Results, index)
	}
	if last := out.Committed[len(out.Committed)-1]; last.Index != index || string(last.Payload) != "x" {
		t.Fatalf("the Entry must be Committed in the same Output, got %+v", out.Committed)
	}
}

func TestLeaderWithoutAMajorityStepsDown(t *testing.T) {
	n := newNode(1, 3)
	elect(t, n, 2)
	n.Step(core.Propose{Ref: 7, Payload: []byte("x")})

	var results []core.Result
	for range 30 { // silence from everyone
		results = append(results, n.Step(core.Tick{}).Results...)
		if n.role != core.LeaderRole {
			break
		}
	}
	if n.role == core.LeaderRole {
		t.Fatal("still Leader after a long silence")
	}
	if len(results) != 1 || results[0].Ref != 7 || results[0].Reason != core.Unknown {
		t.Fatalf("pending proposal ended as %+v, want Unknown", results)
	}
	out := n.Step(core.Propose{Ref: 8})
	if out.Results[0].Reason != core.NoMajority {
		t.Fatalf("after stepping down: %+v, want NoMajority", out.Results)
	}
}

func TestSingleMemberGroup(t *testing.T) {
	n := newNode(1, 1)
	for n.role != core.LeaderRole {
		n.Step(core.Tick{})
	}
	out := n.Step(core.Propose{Ref: 1, Payload: []byte("x")})
	if len(out.Results) != 1 || out.Results[0].Reason != core.OK {
		t.Fatalf("a Group of one commits by itself, got %+v", out.Results)
	}
}

// A candidate whose Log is behind can't win, and must not keep the Members
// that can win from standing: a refused vote leaves the timer running.
func TestRefusedVoteDoesNotResetElectionTimer(t *testing.T) {
	n := newNode(1, 3)
	recv(n, 2, Append{Term: 1, Entries: entries(1, 1)})
	for range 5 {
		n.Step(core.Tick{})
	}
	before := n.elapsed
	v := reply[VoteReply](t, recv(n, 3, RequestVote{Term: 5})) // an empty Log
	if v.Granted || n.term != 5 {
		t.Fatalf("got %+v in term %d; want a refusal that still adopts Term 5", v, n.term)
	}
	if n.elapsed != before {
		t.Fatalf("a refused vote moved the election timer from %d to %d", before, n.elapsed)
	}
	if recv(n, 3, RequestVote{Term: 6, LastIndex: 2, LastTerm: 1}); n.elapsed != 0 {
		t.Fatal("a granted vote should reset the election timer")
	}
}

// A follower far behind is sent batch after batch as fast as it confirms
// them, without waiting for heartbeats.
func TestCatchUpDoesNotWaitForHeartbeats(t *testing.T) {
	n := newNode(1, 3)
	elect(t, n, 2)
	for i := range 3 * maxBatch {
		n.Step(core.Propose{Ref: uint64(i), Payload: []byte("x")})
	}
	last := n.lastIndex()

	a := reply[Append](t, recv(n, 3, AppendReply{Term: 1, Match: 0})) // "I have nothing"
	sent := core.Index(len(a.Entries))
	for rounds := 0; sent < last; rounds++ {
		if rounds > 10 {
			t.Fatalf("stuck after sending %d of %d", sent, last)
		}
		a = reply[Append](t, recv(n, 3, AppendReply{Term: 1, Success: true, Match: sent}))
		if a.PrevIndex != sent || len(a.Entries) == 0 {
			t.Fatalf("after confirming %d the Leader sent %d Entries from %d", sent, len(a.Entries), a.PrevIndex)
		}
		sent += core.Index(len(a.Entries))
	}
	// Fully caught up: nothing more to send.
	if out := recv(n, 3, AppendReply{Term: 1, Success: true, Match: last}); len(out.Messages) != 0 {
		t.Fatalf("a caught-up follower was sent %d more messages", len(out.Messages))
	}
}

// Rung 2's naive read path: whoever believes it leads says yes.
func TestReadsFromMemory(t *testing.T) {
	n := newNode(1, 3)
	n.cfg.Reads = ReadsFromMemory

	if out := n.Step(core.Read{Ref: 1}); len(out.Reads) != 1 || out.Reads[0].Reason != core.NoMajority {
		t.Fatalf("with no known Leader: %+v, want NoMajority", out.Reads)
	}
	recv(n, 2, Append{Term: 1})
	if out := n.Step(core.Read{Ref: 2}); out.Reads[0].Reason != core.NotLeader || out.Reads[0].Leader != 2 {
		t.Fatalf("as a follower: %+v, want NotLeader with hint 2", out.Reads)
	}
	elect(t, n, 3)
	if out := n.Step(core.Read{Ref: 3}); out.Reads[0] != (core.Result{Ref: 3, Reason: core.OK}) {
		t.Fatalf("as Leader: %+v, want OK", out.Reads)
	}
}

func newReadIndexLeader(t *testing.T) *Node {
	t.Helper()
	n := newNode(1, 3)
	n.cfg.Reads = ReadsByIndex
	elect(t, n, 2)
	return n
}

// rounds lists the ReadRound of each Append in out, per destination.
func rounds(out core.Output) map[core.NodeID]uint64 {
	got := map[core.NodeID]uint64{}
	for _, m := range out.Messages {
		if a, ok := m.Body.(Append); ok {
			got[m.To] = a.ReadRound
		}
	}
	return got
}

// A new Leader may hold Committed Entries it hasn't applied. It must not
// answer a read until an Entry of its own Term is Committed.
func TestReadIndexWaitsForOwnTermCommit(t *testing.T) {
	n := newReadIndexLeader(t) // Log: no-op@1, not yet Committed

	out := n.Step(core.Read{Ref: 1})
	if len(out.Reads) != 0 {
		t.Fatalf("answered before anything of this Term was Committed: %+v", out.Reads)
	}
	// Leadership is confirmed, but the no-op still isn't Committed.
	out = recv(n, 2, AppendReply{Term: 1, Match: 0, ReadRound: 1})
	if len(out.Reads) != 0 {
		t.Fatalf("answered on a confirmed round alone: %+v", out.Reads)
	}
	// The no-op commits: now both conditions hold.
	out = recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1, ReadRound: 1})
	if len(out.Reads) != 1 || out.Reads[0] != (core.Result{Ref: 1, Reason: core.OK}) {
		t.Fatalf("after the own-Term commit: %+v, want OK", out.Reads)
	}
	if len(out.Committed) != 1 {
		t.Fatalf("the Entries the read depends on must be handed over in the same Output, got %+v", out.Committed)
	}
}

// A Leader answers only after a Majority echoes a round sent after the read
// arrived. Echoes of earlier rounds prove nothing.
func TestReadIndexNeedsAFreshRound(t *testing.T) {
	n := newReadIndexLeader(t)
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1}) // no-op Committed

	out := n.Step(core.Read{Ref: 1})
	if got := rounds(out); got[2] != 1 || got[3] != 1 {
		t.Fatalf("a read should start round 1 to both followers, got %v", got)
	}
	if len(out.Reads) != 0 {
		t.Fatal("answered before any echo")
	}
	if out = recv(n, 3, AppendReply{Term: 1, Success: true, Match: 0, ReadRound: 0}); len(out.Reads) != 0 {
		t.Fatal("an echo of an earlier round released the read")
	}
	out = recv(n, 3, AppendReply{Term: 1, Success: true, Match: 0, ReadRound: 1})
	if len(out.Reads) != 1 || out.Reads[0].Reason != core.OK {
		t.Fatalf("after a Majority echoed round 1: %+v, want OK", out.Reads)
	}
}

// Reads that arrive while a round is out share the next one.
func TestReadIndexBatchesReads(t *testing.T) {
	n := newReadIndexLeader(t)
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1})

	n.Step(core.Read{Ref: 1}) // starts round 1
	for ref := uint64(2); ref <= 5; ref++ {
		if out := n.Step(core.Read{Ref: ref}); len(out.Messages) != 0 {
			t.Fatalf("read %d started another round while one was out", ref)
		}
	}
	out := recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1, ReadRound: 1})
	if len(out.Reads) != 1 || out.Reads[0].Ref != 1 {
		t.Fatalf("round 1 should release only the read that preceded it, got %+v", out.Reads)
	}
	if got := rounds(out); got[2] != 2 || got[3] != 2 {
		t.Fatalf("the waiting reads should start round 2 at once, got %v", got)
	}
	out = recv(n, 3, AppendReply{Term: 1, Success: true, Match: 1, ReadRound: 2})
	if len(out.Reads) != 4 {
		t.Fatalf("round 2 should release the other four reads, got %+v", out.Reads)
	}
}

// A Leader that has been replaced gathers no echoes and never answers. When
// it finds out, it turns its waiting reads away.
func TestReadIndexReplacedLeaderNeverAnswers(t *testing.T) {
	n := newReadIndexLeader(t)
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 1})

	if out := n.Step(core.Read{Ref: 1}); len(out.Reads) != 0 {
		t.Fatal("answered with no echo")
	}
	// The others have moved on: a follower answers from a newer Term.
	out := recv(n, 2, AppendReply{Term: 2})
	if n.role == core.LeaderRole {
		t.Fatal("still Leader after seeing a newer Term")
	}
	if len(out.Reads) != 1 || out.Reads[0].Reason != core.NoMajority {
		t.Fatalf("the waiting read should be turned away, got %+v", out.Reads)
	}
}

func TestReadIndexSingleMember(t *testing.T) {
	n := newNode(1, 1)
	n.cfg.Reads = ReadsByIndex
	for n.role != core.LeaderRole {
		n.Step(core.Tick{})
	}
	if out := n.Step(core.Read{Ref: 1}); len(out.Reads) != 1 || out.Reads[0].Reason != core.OK {
		t.Fatalf("a Group of one confirms itself, got %+v", out.Reads)
	}
}

// restarted builds a new core from what the given Outputs asked to store, as
// a shell does after a crash.
func restarted(n *Node, outs ...core.Output) *Node {
	var stored core.Stored
	for _, out := range outs {
		if out.Persist != nil {
			stored.Apply(out.Persist)
		}
	}
	cfg := n.cfg
	cfg.Rand = new(counter)
	cfg.Stored = stored
	return New(cfg)
}

// A vote must be stored before it is sent. A Member that forgot its vote
// could give a second one in the same Term, and two candidates could each
// count a Majority.
func TestVoteIsStoredAndSurvivesRestart(t *testing.T) {
	n := newNode(1, 3)
	out := recv(n, 2, RequestVote{Term: 5})
	if v := reply[VoteReply](t, out); !v.Granted {
		t.Fatal("vote not granted")
	}
	if out.Persist == nil || out.Persist.HardState == nil || *out.Persist.HardState != (core.HardState{Term: 5, VotedFor: 2}) {
		t.Fatalf("the Output that grants a vote must store it, got %+v", out.Persist)
	}

	again := restarted(n, out)
	if v := reply[VoteReply](t, recv(again, 3, RequestVote{Term: 5})); v.Granted {
		t.Fatal("after a restart the Member voted for a second candidate in the same Term")
	}
	if v := reply[VoteReply](t, recv(again, 2, RequestVote{Term: 5})); !v.Granted {
		t.Fatal("after a restart the Member should still confirm the vote it gave")
	}
}

// Term and Log survive a restart; the commit index doesn't, and is learned
// again from the Leader.
func TestLogAndTermSurviveRestart(t *testing.T) {
	n := newNode(1, 3)
	out1 := recv(n, 2, Append{Term: 3, Entries: entries(1, 3, 3), Commit: 2})
	if out1.Persist == nil || len(out1.Persist.Entries) != 3 || out1.Persist.HardState.Term != 3 {
		t.Fatalf("an Append must store its Entries and the new Term, got %+v", out1.Persist)
	}
	// A conflicting suffix: Entry 3 is replaced.
	out2 := recv(n, 2, Append{Term: 4, PrevIndex: 2, PrevTerm: 3, Entries: []core.Entry{{Index: 3, Term: 4}}})
	if out2.Persist.TruncateFrom != 3 || len(out2.Persist.Entries) != 1 {
		t.Fatalf("a conflict must store the truncation and the replacement, got %+v", out2.Persist)
	}

	again := restarted(n, out1, out2)
	if again.term != 4 || !reflect.DeepEqual(logTerms(again), []core.Term{1, 3, 4}) {
		t.Fatalf("after restart: term %d, Log terms %v; want 4 and [1 3 4]", again.term, logTerms(again))
	}
	if again.commit != 0 {
		t.Fatalf("the commit index isn't stored, yet it is %d after restart", again.commit)
	}
	// The Leader's next heartbeat tells it what is Committed, and the shell
	// gets those Entries again to rebuild the state machine.
	out := recv(again, 2, Append{Term: 4, PrevIndex: 3, PrevTerm: 4, Commit: 3})
	if len(out.Committed) != 3 {
		t.Fatalf("want all 3 Entries handed over again after restart, got %d", len(out.Committed))
	}
}

// A candidate stores its own vote, and a Leader stores what it appends,
// before any message about either goes out.
func TestCandidateAndLeaderStoreBeforeSending(t *testing.T) {
	n := newNode(1, 3)
	var out core.Output
	for n.role != core.Candidate {
		out = n.Step(core.Tick{})
	}
	if out.Persist == nil || *out.Persist.HardState != (core.HardState{Term: 1, VotedFor: 1}) || len(out.Messages) != 2 {
		t.Fatalf("standing for election must store the Term and self-vote with the requests, got %+v", out.Persist)
	}
	out = recv(n, 2, VoteReply{Term: 1, Granted: true})
	if out.Persist == nil || len(out.Persist.Entries) != 1 || out.Persist.Entries[0].Kind != core.EntryNoop {
		t.Fatalf("a new Leader must store its no-op, got %+v", out.Persist)
	}
	out = n.Step(core.Propose{Ref: 1, Payload: []byte("x")})
	if out.Persist == nil || len(out.Persist.Entries) != 1 || string(out.Persist.Entries[0].Payload) != "x" {
		t.Fatalf("a proposal must be stored with the Append that carries it, got %+v", out.Persist)
	}
}

// A Snapshot lets the core drop the Log it covers. The Snapshot is stored in
// the same Output, and everything after it still works.
func TestSnapshotTrimsTheLog(t *testing.T) {
	n := newNode(1, 3)
	recv(n, 2, Append{Term: 1, Entries: entries(1, 1, 1, 1, 1), Commit: 4})

	// A Snapshot of Entries the shell was never given is ignored.
	if out := n.Step(core.Snapshotted{Index: 5, Data: []byte("x")}); out.Persist != nil || n.log.base != 0 {
		t.Fatal("accepted a Snapshot beyond what was handed over as Committed")
	}
	out := n.Step(core.Snapshotted{Index: 3, Data: []byte("state at 3")})
	if p := out.Persist; p == nil || p.Snapshot == nil || p.Snapshot.Index != 3 || p.Snapshot.Term != 1 || string(p.Snapshot.Data) != "state at 3" {
		t.Fatalf("the Snapshot must be stored, got %+v", out.Persist)
	}
	if n.log.base != 3 || len(n.log.entries) != 2 || n.lastIndex() != 5 {
		t.Fatalf("Log after the Snapshot: base %d, %d Entries, last %d; want 3, 2, 5", n.log.base, len(n.log.entries), n.lastIndex())
	}
	// An older Snapshot changes nothing.
	if out := n.Step(core.Snapshotted{Index: 2}); out.Persist != nil {
		t.Fatal("accepted a Snapshot older than the one held")
	}

	// Appends continue normally past the Snapshot.
	r := reply[AppendReply](t, recv(n, 2, Append{Term: 1, PrevIndex: 5, PrevTerm: 1, Entries: []core.Entry{{Index: 6, Term: 1}}, Commit: 6}))
	if !r.Success || r.Match != 6 {
		t.Fatalf("append after the Snapshot: %+v", r)
	}
	// A late Append that starts inside the Snapshot is accepted for the part
	// that isn't covered.
	r = reply[AppendReply](t, recv(n, 2, Append{Term: 1, PrevIndex: 1, PrevTerm: 1, Entries: []core.Entry{{Index: 2, Term: 1}, {Index: 3, Term: 1}}}))
	if !r.Success || r.Match != 3 {
		t.Fatalf("an Append wholly inside the Snapshot: %+v, want success at the Snapshot's edge", r)
	}
	if n.lastIndex() != 6 {
		t.Fatalf("a stale Append changed the Log: last %d", n.lastIndex())
	}

	// And the Member restarts from the Snapshot plus what follows it.
	again := restarted(n, core.Output{Persist: &core.Persist{HardState: &core.HardState{Term: 1}, Entries: entries(1, 1, 1, 1, 1)}}, out,
		core.Output{Persist: &core.Persist{Entries: []core.Entry{{Index: 6, Term: 1}}}})
	if again.log.base != 3 || again.lastIndex() != 6 || again.commit != 3 || again.applied != 3 {
		t.Fatalf("after restart: base %d, last %d, commit %d, applied %d; want 3, 6, 3, 3", again.log.base, again.lastIndex(), again.commit, again.applied)
	}
}

// leaderWithSnapshot is a Leader of three whose Log starts after Entry 4:
// no-op, then five commands, all Committed with follower 2, and a Snapshot
// taken at Entry 4.
func leaderWithSnapshot(t *testing.T) *Node {
	t.Helper()
	n := newNode(1, 3)
	elect(t, n, 2)
	for i := range 5 {
		n.Step(core.Propose{Ref: uint64(i), Payload: []byte("x")})
	}
	recv(n, 2, AppendReply{Term: 1, Success: true, Match: 6})
	if n.commit != 6 {
		t.Fatalf("setup: commit %d, want 6", n.commit)
	}
	n.Step(core.Snapshotted{Index: 4, Data: []byte("state at 4")})
	return n
}

// A follower that needs Entries the Leader has trimmed is sent the Snapshot
// instead, then the Log after it.
func TestLeaderSendsSnapshotToAMemberBehindIt(t *testing.T) {
	n := leaderWithSnapshot(t)

	// Follower 3 reports that it has nothing.
	out := recv(n, 3, AppendReply{Term: 1, Match: 0})
	snap := reply[InstallSnapshot](t, out)
	if snap.Snapshot.Index != 4 || string(snap.Snapshot.Data) != "state at 4" || snap.Term != 1 {
		t.Fatalf("sent %+v, want the Snapshot at Entry 4", snap)
	}
	// Asking again straight away doesn't send it twice.
	if out := recv(n, 3, AppendReply{Term: 1, Match: 0}); len(out.Messages) != 0 {
		t.Fatalf("the Snapshot was sent again at once: %+v", out.Messages)
	}
	// Meanwhile heartbeats still reach the follower.
	hb := n.Step(core.Tick{})
	var toThree []any
	for _, m := range hb.Messages {
		if m.To == 3 {
			toThree = append(toThree, m.Body)
		}
	}
	if len(toThree) != 1 {
		t.Fatalf("want one heartbeat to follower 3 while the Snapshot is on its way, got %+v", toThree)
	}
	if _, isAppend := toThree[0].(Append); !isAppend {
		t.Fatalf("the heartbeat should be an Append, not a second %T", toThree[0])
	}

	// Once the follower confirms the Snapshot, the rest of the Log follows.
	a := reply[Append](t, recv(n, 3, AppendReply{Term: 1, Success: true, Match: 4}))
	if a.PrevIndex != 4 || len(a.Entries) != 2 {
		t.Fatalf("after the Snapshot the Leader sent %d Entries from %d, want 2 from 4", len(a.Entries), a.PrevIndex)
	}
}

// If no confirmation comes, the Snapshot is sent again after an election
// timeout's worth of ticks.
func TestSnapshotIsResentAfterSilence(t *testing.T) {
	n := leaderWithSnapshot(t)
	reply[InstallSnapshot](t, recv(n, 3, AppendReply{Term: 1, Match: 0}))
	resent := false
	for range n.cfg.ElectionTicks + 1 {
		recv(n, 2, AppendReply{Term: 1, Success: true, Match: 6}) // follower 2 keeps the Leader in touch with a Majority
		for _, m := range n.Step(core.Tick{}).Messages {
			if _, ok := m.Body.(InstallSnapshot); ok && m.To == 3 {
				resent = true
			}
		}
	}
	if !resent {
		t.Fatal("the Snapshot was never sent again")
	}
}

func TestFollowerInstallsSnapshot(t *testing.T) {
	n := newNode(3, 3)
	recv(n, 1, Append{Term: 1, Entries: entries(1, 1), Commit: 1}) // an old, short Log

	snap := core.Snapshot{Index: 10, Term: 2, Data: []byte("state at 10")}
	out := recv(n, 1, InstallSnapshot{Term: 2, Snapshot: snap, ReadRound: 7})
	r := reply[AppendReply](t, out)
	if !r.Success || r.Match != 10 || r.ReadRound != 7 {
		t.Fatalf("reply %+v, want success at 10 echoing round 7", r)
	}
	if out.Restore == nil || string(out.Restore.Data) != "state at 10" {
		t.Fatalf("the shell must be told to replace the state machine, got %+v", out.Restore)
	}
	if p := out.Persist; p == nil || p.Snapshot == nil || p.Snapshot.Index != 10 || !p.ResetLog {
		t.Fatalf("the Snapshot must be stored and the old Log dropped, got %+v", out.Persist)
	}
	if n.log.base != 10 || n.lastIndex() != 10 || n.commit != 10 || n.applied != 10 || len(out.Committed) != 0 {
		t.Fatalf("after install: base %d, last %d, commit %d, applied %d", n.log.base, n.lastIndex(), n.commit, n.applied)
	}

	// The Log carries on from the Snapshot.
	out = recv(n, 1, Append{Term: 2, PrevIndex: 10, PrevTerm: 2, Entries: []core.Entry{{Index: 11, Term: 2}}, Commit: 11})
	if r := reply[AppendReply](t, out); !r.Success || r.Match != 11 || len(out.Committed) != 1 {
		t.Fatalf("append after install: %+v with %d Committed", r, len(out.Committed))
	}
	// A Snapshot of what is already Committed here changes nothing.
	out = recv(n, 1, InstallSnapshot{Term: 2, Snapshot: core.Snapshot{Index: 5, Term: 1}})
	if r := reply[AppendReply](t, out); !r.Success || r.Match != 11 || out.Restore != nil || out.Persist != nil {
		t.Fatalf("an old Snapshot: reply %+v, restore %v, persist %v", r, out.Restore, out.Persist)
	}
	// One from a stale Leader is refused.
	if r := reply[AppendReply](t, recv(n, 2, InstallSnapshot{Term: 1, Snapshot: core.Snapshot{Index: 99, Term: 1}})); r.Success || r.Term != 2 {
		t.Fatalf("a Snapshot from an old Term: %+v", r)
	}
}

// A follower that already holds the Snapshot's last Entry keeps the Entries
// after it. It may have acknowledged them, and dropping them would leave the
// Leader counting a copy that no longer exists.
func TestInstallKeepsAcknowledgedEntriesAfterTheSnapshot(t *testing.T) {
	n := newNode(3, 3)
	r := reply[AppendReply](t, recv(n, 1, Append{Term: 1, Entries: entries(1, 1, 1, 1, 1), Commit: 2}))
	if !r.Success || r.Match != 5 {
		t.Fatalf("setup: %+v", r)
	}

	// The Leader's Snapshot ends at 4. This Member holds 4 and has
	// acknowledged 5.
	out := recv(n, 1, InstallSnapshot{Term: 1, Snapshot: core.Snapshot{Index: 4, Term: 1, Data: []byte("state at 4")}})
	if r := reply[AppendReply](t, out); !r.Success || r.Match != 4 {
		t.Fatalf("reply %+v", r)
	}
	if n.lastIndex() != 5 || n.log.base != 4 {
		t.Fatalf("after install: base %d, last %d; Entry 5 must survive", n.log.base, n.lastIndex())
	}
	if out.Persist.ResetLog {
		t.Fatal("the stored Log was reset, which drops the acknowledged Entry 5 from disk too")
	}
	if out.Restore == nil || n.commit != 4 || n.applied != 4 {
		t.Fatalf("the state machine should be restored to Entry 4: restore %v, commit %d, applied %d", out.Restore != nil, n.commit, n.applied)
	}
	// Entry 5 is still there for the Leader to commit.
	out = recv(n, 1, Append{Term: 1, PrevIndex: 5, PrevTerm: 1, Commit: 5})
	if len(out.Committed) != 1 || out.Committed[0].Index != 5 {
		t.Fatalf("Entry 5 should now be handed over as Committed, got %+v", out.Committed)
	}

	// A Member whose Log disagrees with the Snapshot at its last Entry does
	// drop its Log.
	other := newNode(3, 3)
	recv(other, 1, Append{Term: 1, Entries: entries(1, 1, 1, 1, 1)})
	out = recv(other, 2, InstallSnapshot{Term: 2, Snapshot: core.Snapshot{Index: 4, Term: 2}})
	if other.lastIndex() != 4 || !out.Persist.ResetLog {
		t.Fatalf("a Log that doesn't match the Snapshot must go: last %d, reset %v", other.lastIndex(), out.Persist.ResetLog)
	}
}

// damagedNode is a Member of three that restarted after its storage found
// damage: it is in Term 2 and holds only the first three Entries.
func damagedNode() *Node {
	ids := []core.NodeID{1, 2, 3}
	return New(Config{ID: 3, Members: ids, ElectionTicks: 10, HeartbeatTicks: 1, Rand: new(counter),
		Stored: core.Stored{HardState: core.HardState{Term: 2}, Entries: entries(1, 1, 2), Damaged: true}})
}

// A Member that found damage on its disk may have acknowledged Entries it no
// longer holds. Until it has caught up it neither votes nor stands.
func TestRecoveringMemberStaysOutOfElections(t *testing.T) {
	n := damagedNode()
	for range 100 {
		if out := n.Step(core.Tick{}); n.role != core.Follower || len(out.Messages) != 0 {
			t.Fatal("a recovering Member stood for election")
		}
	}
	v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 3, LastIndex: 99, LastTerm: 2}))
	if v.Granted {
		t.Fatal("a recovering Member granted a vote")
	}
	if n.term != 3 {
		t.Fatalf("it should still learn the new Term, has %d", n.term)
	}
}

// It tells the Leader its Log went backwards, and the Leader believes it.
func TestLeaderAcceptsThatARecoveringFollowerLostEntries(t *testing.T) {
	follower := damagedNode()
	r := reply[AppendReply](t, recv(follower, 1, Append{Term: 2, PrevIndex: 9, PrevTerm: 2, LeaderLast: 9}))
	if r.Success || r.Match != 3 || !r.Reset {
		t.Fatalf("reply %+v, want failure at 3 with Reset", r)
	}

	leader := newNode(1, 3)
	elect(t, leader, 2)
	for i := range 8 {
		leader.Step(core.Propose{Ref: uint64(i), Payload: []byte("x")})
	}
	recv(leader, 3, AppendReply{Term: 1, Success: true, Match: 9}) // follower 3 once confirmed everything
	if leader.match[3] != 9 {
		t.Fatalf("setup: match %d", leader.match[3])
	}
	a := reply[Append](t, recv(leader, 3, AppendReply{Term: 1, Match: 3, Reset: true}))
	if leader.match[3] != 3 || a.PrevIndex != 3 || len(a.Entries) != 6 {
		t.Fatalf("after Reset: match %d, resend from %d with %d Entries; want 3, 3, 6", leader.match[3], a.PrevIndex, len(a.Entries))
	}
	if leader.commit != 9 {
		t.Fatalf("what was Committed stays Committed, but commit is now %d", leader.commit)
	}
	// Without Reset the same reply is taken for a stale one and ignored.
	recv(leader, 3, AppendReply{Term: 1, Success: true, Match: 9})
	a = reply[Append](t, recv(leader, 3, AppendReply{Term: 1, Match: 3}))
	if leader.match[3] != 9 || a.PrevIndex != 9 {
		t.Fatalf("a failure without Reset lowered match to %d", leader.match[3])
	}
}

// Once it matches the Leader's whole Log it holds everything Committed, and
// takes part again. The storage is told, so the mark doesn't outlive it.
func TestRecoveringMemberResumesWhenCaughtUp(t *testing.T) {
	n := damagedNode()
	// Some of what it lacks, but the Leader has more.
	out := recv(n, 1, Append{Term: 2, PrevIndex: 3, PrevTerm: 2, Entries: []core.Entry{{Index: 4, Term: 2}}, LeaderLast: 6})
	if r := reply[AppendReply](t, out); !r.Success || !r.Reset || !n.recovering {
		t.Fatalf("part way: reply %+v, recovering %v", r, n.recovering)
	}
	if out.Persist.Recovered {
		t.Fatal("declared recovered before reaching the end of the Leader's Log")
	}
	// The rest.
	out = recv(n, 1, Append{Term: 2, PrevIndex: 4, PrevTerm: 2, Entries: []core.Entry{{Index: 5, Term: 2}, {Index: 6, Term: 2}}, LeaderLast: 6})
	if n.recovering || out.Persist == nil || !out.Persist.Recovered {
		t.Fatalf("caught up: recovering %v, persist %+v", n.recovering, out.Persist)
	}
	// It votes and stands again.
	if v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 3, LastIndex: 6, LastTerm: 2})); !v.Granted {
		t.Fatal("a recovered Member should vote")
	}
	if r := reply[AppendReply](t, recv(n, 2, Append{Term: 3, PrevIndex: 6, PrevTerm: 2, LeaderLast: 6})); r.Reset {
		t.Fatal("a recovered Member still flags its replies")
	}
}

// With the exposure-only switch a damaged Member votes at once.
func TestRepairWithoutAbstainingVotes(t *testing.T) {
	n := damagedNode()
	n.cfg.RepairWithoutAbstaining = true
	if v := reply[VoteReply](t, recv(n, 2, RequestVote{Term: 3, LastIndex: 3, LastTerm: 2})); !v.Granted {
		t.Fatal("expected the unsafe vote")
	}
}
