package raft

import (
	"encoding/binary"
	"errors"
	"slices"

	"distributed-kv-store/internal/core"
)

// Membership change (A§6.5). The Group's Member list is part of the Log: a
// change is an Entry that carries the new list, and a Member uses a list
// from the moment the Entry is in its Log, Committed or not.
//
// Changing more than one Member at a time is unsafe. While some Members hold
// the new list and some the old, a Majority of each can exist with nobody in
// common, and each can elect a Leader in the same Term. If the lists differ
// by one Member, any Majority of one shares a Member with any Majority of
// the other. So a change adds or removes exactly one Member, and two more
// rules keep consecutive changes from adding up to a bigger one:
//
//  1. Only one change may be uncommitted at a time.
//  2. A Leader may not append a change until it has Committed an Entry of
//     its own Term. Without this, a Leader could build on a list that an
//     earlier Leader appended but never Committed, and that some other
//     Member may yet be elected under (a flaw found in the original scheme).
//
// A Member to be added first receives the Log as a learner: the Leader
// replicates to it, but it isn't in the list and counts for nothing. Only
// once it holds everything Committed does the Leader append the change. A
// new Member that counted from the start would be a vote the Group needs
// and can't yet get.

// memberList is the Member list in force from the Entry at index on.
type memberList struct {
	index   core.Index
	members []core.NodeID // ascending
}

// learnerPatience is how many election timeouts a Leader waits for a Member
// being added to catch up before it gives up on the change.
const learnerPatience = 20

func encodeMembers(members []core.NodeID) []byte {
	b := binary.AppendUvarint(nil, uint64(len(members)))
	for _, m := range members {
		b = binary.AppendUvarint(b, uint64(m))
	}
	return b
}

func decodeMembers(b []byte) ([]core.NodeID, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 || count == 0 || count > uint64(len(b)) {
		return nil, errors.New("raft: unreadable Member list")
	}
	b = b[n:]
	members := make([]core.NodeID, 0, count)
	for range count {
		id, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, errors.New("raft: unreadable Member list")
		}
		members = append(members, core.NodeID(id))
		b = b[n:]
	}
	if len(b) != 0 {
		return nil, errors.New("raft: unreadable Member list")
	}
	return members, nil
}

func (n *Node) isMember(id core.NodeID) bool { return slices.Contains(n.members, id) }

// self is 1 if this Node is a Member, for counts that start with itself.
func (n *Node) self() int {
	if n.isMember(n.id) {
		return 1
	}
	return 0
}

// peers are the Nodes a Leader replicates to: every other Member, and the
// learner if there is one.
func (n *Node) peers() []core.NodeID {
	peers := make([]core.NodeID, 0, len(n.members))
	for _, m := range n.members {
		if m != n.id {
			peers = append(peers, m)
		}
	}
	if n.learner != 0 && !n.isMember(n.learner) {
		peers = append(peers, n.learner)
	}
	return peers
}

// useMembers records that the Entry e, now in the Log, changes the list.
func (n *Node) useMembers(e core.Entry) {
	members, err := decodeMembers(e.Payload)
	if err != nil {
		panic(err.Error()) // an Entry that passed its checksum and isn't what a Leader wrote
	}
	n.lists = append(n.lists, memberList{index: e.Index, members: members})
	n.members = members
	n.forced = false // the Log says who the Members are again
	if n.role == core.LeaderRole {
		for _, m := range members {
			n.follow(m)
		}
	}
}

// follow starts a Leader's record of a Node it will replicate to.
func (n *Node) follow(m core.NodeID) {
	if _, known := n.next[m]; !known && m != n.id {
		n.next[m] = n.lastIndex() + 1
		n.heard[m] = n.now
	}
}

// dropListsFrom forgets the lists of Entries at index and after, which are
// being removed from the Log.
func (n *Node) dropListsFrom(index core.Index) {
	for len(n.lists) > 1 && n.lists[len(n.lists)-1].index >= index {
		n.lists = n.lists[:len(n.lists)-1]
	}
	n.members = n.lists[len(n.lists)-1].members
}

// listAt is the Member list in force at index.
func (n *Node) listAt(index core.Index) memberList {
	at := n.lists[0]
	for _, l := range n.lists[1:] {
		if l.index <= index {
			at = l
		}
	}
	return at
}

// foldListsTo makes the list in force at index the oldest one remembered,
// when a Snapshot replaces the Log up to there.
func (n *Node) foldListsTo(index core.Index) {
	base := n.listAt(index)
	kept := []memberList{base}
	for _, l := range n.lists[1:] {
		if l.index > index {
			kept = append(kept, l)
		}
	}
	n.lists = kept
}

// snapshotMembers is the list to store with a Snapshot at index: nil while
// the Group still has the list it started with.
func (n *Node) snapshotMembers(index core.Index) []core.NodeID {
	if at := n.listAt(index); at.index != 0 || n.changed {
		return slices.Clone(at.members)
	}
	return nil
}

// changing reports whether a Membership change is under way: an Entry not
// yet Committed, or a learner still catching up.
func (n *Node) changing() bool {
	return n.lists[len(n.lists)-1].index > n.commit || n.learner != 0
}

// reconfigure takes a request for a Membership change.
func (n *Node) reconfigure(out *core.Output, ev core.Reconfigure) {
	refuse := func(reason core.Reason) {
		out.Results = append(out.Results, core.Result{Ref: ev.Ref, Reason: reason, Leader: n.leader})
	}
	switch {
	case n.role == core.LeaderRole:
	case n.role == core.Follower && n.leader != 0:
		refuse(core.NotLeader)
		return
	default:
		refuse(core.NoMajority)
		return
	}
	want := slices.Clone(ev.Members)
	slices.Sort(want)
	want = slices.Compact(want)

	if n.cfg.SwapMembersAtOnce {
		// The naive store: whatever list is asked for, at once.
		if len(want) == 0 {
			refuse(core.Invalid)
			return
		}
		n.appendMembers(out, want, ev.Ref)
		return
	}

	var added, removed []core.NodeID
	for _, m := range want {
		if !n.isMember(m) {
			added = append(added, m)
		}
	}
	for _, m := range n.members {
		if !slices.Contains(want, m) {
			removed = append(removed, m)
		}
	}
	switch {
	case len(want) == 0 || len(added)+len(removed) != 1:
		refuse(core.Invalid)
	case n.changing() || !n.ownTermCommitted():
		refuse(core.Busy)
	case len(removed) == 1:
		n.appendMembers(out, want, ev.Ref)
	case n.cfg.AddWithoutCatchUp:
		n.appendMembers(out, want, ev.Ref)
	default:
		// The new Member catches up first.
		n.learner, n.learnerRef, n.learnerSince = added[0], ev.Ref, n.now
		n.follow(n.learner)
		n.sendAppend(out, n.learner)
	}
}

// appendMembers appends the Entry that makes list the Group's Member list,
// and sends it on.
func (n *Node) appendMembers(out *core.Output, list []core.NodeID, ref uint64) {
	index := n.lastIndex() + 1
	n.appendEntry(out, core.Entry{Index: index, Term: n.term, Kind: core.EntryMembers, Payload: encodeMembers(list)})
	n.pending[index] = ref
	for _, m := range n.peers() {
		if n.next[m] == index {
			n.sendAppend(out, m)
		}
	}
	n.advanceCommit()
}

// promoteLearner appends the change that adds the learner, once it holds
// everything that is Committed. It gives up if the learner has taken too
// long.
func (n *Node) promoteLearner(out *core.Output) {
	if n.learner == 0 {
		return
	}
	switch {
	case n.match[n.learner] >= n.commit && n.ownTermCommitted():
		list := append(slices.Clone(n.members), n.learner)
		slices.Sort(list)
		ref := n.learnerRef
		n.learner, n.learnerRef = 0, 0
		n.appendMembers(out, list, ref)
	case n.now-n.learnerSince > learnerPatience*n.cfg.ElectionTicks:
		out.Results = append(out.Results, core.Result{Ref: n.learnerRef, Reason: core.NoCatchUp})
		n.forget(n.learner)
		n.learner, n.learnerRef = 0, 0
	}
}

// forget drops a Leader's record of a Node it no longer replicates to.
func (n *Node) forget(m core.NodeID) {
	delete(n.next, m)
	delete(n.match, m)
	delete(n.heard, m)
	delete(n.sentSnap, m)
	delete(n.roundAcked, m)
	delete(n.leaseFrom, m)
}

// leaveIfRemoved makes a Leader step down once a change that removes it is
// Committed. Until then it leads a Group it is no longer counted in.
func (n *Node) leaveIfRemoved(out *core.Output) {
	if n.role == core.LeaderRole && !n.isMember(n.id) && !n.changing() {
		n.becomeFollower(out, n.term, 0)
	}
}

// StoredMembers is the Member list a stopped Member's disk gives it, and
// whether the disk says at all: a Member that has seen no Membership change
// holds only the list it was started with, which isn't on its disk. An
// operator's tool uses this to say what Unsafe recovery replaces (A§6.6).
func StoredMembers(stored core.Stored) ([]core.NodeID, bool) {
	n := New(Config{ElectionTicks: 1, HeartbeatTicks: 1, Rand: noRand{}, Stored: stored})
	if !n.changed {
		return nil, false
	}
	return slices.Clone(n.members), true
}

// noRand is the randomness of a Node that is built only to be looked at.
type noRand struct{}

func (noRand) Uint64() uint64 { return 0 }
