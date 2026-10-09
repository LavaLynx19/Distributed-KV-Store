package raft

import "slices"

// Lease reads (A§6.2, variant): a Leader answers a read from memory without
// asking anyone, on the strength of a promise. The argument is about time:
//
//   - A follower that hears from a Leader promises not to vote for anyone
//     else for ElectionTicks. A Member that has just started makes the same
//     promise, since it can't know what it promised before it stopped.
//   - So once a Majority has answered a message the Leader sent at tick S,
//     no other Leader can be elected until those followers have counted
//     ElectionTicks from when they received it, which was after S.
//   - The Leader therefore answers reads until its own count reaches
//     S + LeaseTicks, with LeaseTicks a little under ElectionTicks.
//
// The last step compares the Leader's count of ticks with the followers'.
// If the Leader's ticks are slower than theirs, its lease outlasts their
// promise, and it answers reads after another Leader has taken writes: a
// Stale read. Nothing in the protocol can detect that, which is why this is
// a variant and read index is the default (Decision Log).

// promised reports whether this Member has promised not to vote yet.
func (n *Node) promised() bool {
	return n.cfg.Reads == ReadsByLease && n.now-n.contact < n.cfg.ElectionTicks
}

// leaseHolds reports whether the Leader may answer a read without asking:
// it has Committed an Entry of its own Term, so its state is current, and a
// Majority has answered something it sent less than LeaseTicks ago.
func (n *Node) leaseHolds() bool {
	if !n.ownTermCommitted() {
		return false
	}
	var sent []int
	if n.isMember(n.id) {
		sent = append(sent, n.now)
	}
	for _, m := range n.members {
		if at, ok := n.leaseFrom[m]; ok && m != n.id {
			sent = append(sent, at)
		}
	}
	if len(sent) < n.majority() {
		return false
	}
	slices.SortFunc(sent, func(a, b int) int { return b - a })
	return n.now-sent[n.majority()-1] < n.cfg.LeaseTicks
}
