package raft

import (
	"fmt"
	"slices"

	"distributed-kv-store/internal/core"
)

// raftLog is the part of the Log a Member still holds: the Entries after its
// Snapshot. base is the Index the Snapshot covers up to (0 with no Snapshot)
// and baseTerm is that Entry's Term, which is all that is remembered of it.
type raftLog struct {
	base     core.Index
	baseTerm core.Term
	entries  []core.Entry // entries[i] has Index base+1+i
}

func (l *raftLog) last() core.Index { return l.base + core.Index(len(l.entries)) }

// term is the Term of the Entry at i. It is known for base and everything
// after it; asking about an Entry the Snapshot has replaced is a bug.
func (l *raftLog) term(i core.Index) core.Term {
	switch {
	case i == l.base:
		return l.baseTerm
	case i < l.base || i > l.last():
		panic(fmt.Sprintf("raft: no Term known for Index %d (Log holds %d..%d)", i, l.base+1, l.last()))
	}
	return l.entries[i-l.base-1].Term
}

func (l *raftLog) entry(i core.Index) core.Entry { return l.entries[i-l.base-1] }

// after returns a copy of the Entries with Index in (from, to].
func (l *raftLog) after(from, to core.Index) []core.Entry {
	return slices.Clone(l.entries[from-l.base : to-l.base])
}

func (l *raftLog) append(e core.Entry) {
	if e.Index != l.last()+1 {
		panic(fmt.Sprintf("raft: appending Entry %d after %d", e.Index, l.last()))
	}
	l.entries = append(l.entries, e)
}

// compactTo drops the Entries up to and including i, which a Snapshot now
// stands in for.
func (l *raftLog) compactTo(i core.Index) {
	term := l.term(i)
	l.entries = slices.Clone(l.entries[i-l.base:])
	l.base, l.baseTerm = i, term
}

// truncateFrom drops the Entry at i and everything after it.
func (l *raftLog) truncateFrom(i core.Index) { l.entries = l.entries[:i-l.base-1] }
