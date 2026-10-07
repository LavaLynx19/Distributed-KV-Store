package raft

import (
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
	terms := make([]core.Term, len(n.log))
	for i, e := range n.log {
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
	if len(n.log) != 2 {
		t.Fatalf("a duplicate Append truncated the Log to %d", len(n.log))
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
	if n.term != 3 || len(n.log) != 3 {
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
