// Package automation holds the decisions a store takes by itself (A§11.11):
// when a Node is to be replaced and by which Spare, what a Group's Leader
// does about a change of Members the Meta Group wants, when a Node may drop
// a replica, and when a Slot is moved to even out load.
//
// It decides and does nothing. Every function here is given what a Node
// knows and returns what it should ask for; the asking goes through a Raft
// Log like any other request, so a decision made on a wrong picture can
// cost work and can't break a guarantee. The package is pure: the
// Simulation and the real shell run the same code.
package automation

import (
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
)

// Watch remembers since when each Node has been thought dead, so that a
// Node says so to the others only after a wait.
type Watch struct {
	since map[int]int64
}

// Held takes the Nodes this Node's detector gives up for dead at time now,
// and returns those it has given up for at least wait, ascending.
func (w *Watch) Held(now, wait int64, dead []int) []int {
	if w.since == nil {
		w.since = map[int]int64{}
	}
	for n := range w.since {
		if !slices.Contains(dead, n) {
			delete(w.since, n)
		}
	}
	var held []int
	for _, n := range dead {
		since, known := w.since[n]
		switch {
		case !known:
			w.since[n] = now
		case now-since >= wait:
			held = append(held, n)
		}
	}
	slices.Sort(held)
	return held
}

// Dead lists the Nodes that more than half of voters report dead,
// ascending. voters are the Nodes that host the Meta Group's Members, and
// report gives what each last said, or false for one that has no say: a
// voter that itself seems to be gone.
func Dead(voters []int, report func(n int) (shard.Report, bool)) []int {
	said := map[int]int{}
	for _, v := range voters {
		if r, ok := report(v); ok {
			for _, n := range r.Dead {
				said[n]++
			}
		}
	}
	var dead []int
	for n, votes := range said {
		if votes > len(voters)/2 {
			dead = append(dead, n)
		}
	}
	slices.Sort(dead)
	return dead
}

// Replacement picks the next replacement to ask the Meta Group for: a dead
// Node that some Group still needs replacing, and the Spare to take its
// place. alive lists the Nodes that seem alive, ascending. A Spare is one
// of them that the table has in no Group and on its way into none. ok is
// false if nothing needs replacing or there is no Spare.
func Replacement(table shard.Table, dead, alive []int) (out, in int, ok bool) {
	for _, n := range dead {
		needed := false
		for _, row := range table.Groups {
			member := slices.Contains(row.Members, n)
			needed = needed || member && row.Add == 0 || !member && row.Add == n
		}
		if !needed {
			continue
		}
		for _, s := range alive {
			free := true
			for _, row := range table.Groups {
				free = free && !slices.Contains(row.Members, s) && row.Add != s
			}
			if free {
				return n, s, true
			}
		}
		return 0, 0, false
	}
	return 0, 0, false
}

// Step is what a Group's Leader should do next about its Members: at most
// one of the two.
type Step struct {
	// Report, if set, is to be put to the Meta Group.
	Report *meta.Command
	// Reconfigure, if set, is the Member list to ask the Group's own core
	// for.
	Reconfigure []core.NodeID
}

// Members is the part of Group g's Leader in a change of Members the Meta
// Group wants. row is the Group's row of the table the Leader's Node holds,
// and st the Leader's own status.
//
// The Leader adds the Node to add, then removes the Node to remove, one
// change at a time (A§6.5), and tells the Meta Group whenever the Group's
// Committed list isn't the one in the table. It acts on a wish only when
// the table is up to date with the Group's list, so that a table from
// before some later change can't make it repeat an old one. If the Node it
// is bringing up to date is no longer the one wanted, it calls that off.
func Members(g shard.GroupID, row shard.Group, st core.Status) Step {
	replica := func(n int) core.NodeID { return core.NodeID(shard.ReplicaID(n, g)) }
	if st.Learner != 0 && st.Learner != replica(row.Add) {
		return Step{Reconfigure: slices.Clone(st.Members)}
	}
	if st.Changing {
		return Step{}
	}
	var now []int
	for _, m := range st.Members {
		n, _ := shard.SplitReplicaID(uint64(m))
		now = append(now, n)
	}
	if !slices.Equal(now, row.Members) || uint64(st.MembersAt) != row.At {
		if uint64(st.MembersAt) > row.At {
			return Step{Report: &meta.Command{Op: meta.OpMembers, To: g, Members: now, At: uint64(st.MembersAt)}}
		}
		return Step{}
	}
	switch {
	case row.Add != 0 && !slices.Contains(now, row.Add):
		next := append(slices.Clone(st.Members), replica(row.Add))
		slices.Sort(next)
		return Step{Reconfigure: next}
	case row.Add != 0 && slices.Contains(now, row.Remove):
		return Step{Reconfigure: slices.DeleteFunc(slices.Clone(st.Members), func(m core.NodeID) bool { return m == replica(row.Remove) })}
	}
	return Step{}
}

// Gone reports whether the Node holding replica me may drop it. mine is the
// replica's own status. leader is the status of its Group's Leader, taken
// after that Leader confirmed it still leads. The replica is gone if the
// Leader's list is Committed, leaves it out, and is no older than the list
// the replica itself holds.
func Gone(me core.NodeID, mine, leader core.Status) bool {
	return leader.Role == core.LeaderRole && !leader.Changing &&
		!slices.Contains(leader.Members, me) && mine.MembersAt <= leader.MembersAt
}

// Meter turns counts of the load on each Slot into a smoothed figure.
type Meter struct {
	smooth []float64
	last   []uint32
}

// Fold closes a window: counts is the load each Slot took in it, and weight
// the share the newest window has in the smoothed figure.
func (m *Meter) Fold(counts []uint32, weight float64) {
	if len(m.smooth) != len(counts) {
		m.smooth, m.last = make([]float64, len(counts)), make([]uint32, len(counts))
	}
	for s, n := range counts {
		m.smooth[s] += weight * (float64(n) - m.smooth[s])
		m.last[s] = n
	}
}

// Load is what a Node reports: the smoothed load of each Slot for which
// leads is true, and 0 for the rest; or nil if that is all zeroes. With
// latest it is the last window's count as it stands, with no smoothing.
func (m *Meter) Load(leads func(slot int) bool, latest bool) []uint32 {
	load := make([]uint32, len(m.smooth))
	any := false
	for s := range m.smooth {
		if !leads(s) {
			continue
		}
		load[s] = uint32(m.smooth[s] + 0.5)
		if latest {
			load[s] = m.last[s]
		}
		any = any || load[s] > 0
	}
	if !any {
		return nil
	}
	return load
}

// MergeLoad is the load of each of slots Slots, from what Nodes report: the
// most any of them says. Only the Node that leads a Slot's Group reports
// anything for it.
func MergeLoad(slots int, reports []shard.Report) []uint32 {
	load := make([]uint32, slots)
	for _, r := range reports {
		for s, l := range r.Load {
			if s < slots && l > load[s] {
				load[s] = l
			}
		}
	}
	return load
}

// GroupLoad adds a per-Slot load up by the Group the table gives each Slot
// to. The Meta Group, at index 0, has none.
func GroupLoad(table shard.Table, load []uint32, groups int) []uint64 {
	sums := make([]uint64, groups+1)
	for s, o := range table.Slots {
		if int(o.Group) < len(sums) && s < len(load) {
			sums[o.Group] += uint64(load[s])
		}
	}
	return sums
}

// Balancing sets how load is evened out.
type Balancing struct {
	// Naive is the store stage 7c starts from: the busiest Slot of the
	// busiest Group goes to the idlest whenever the busiest carries more
	// than High times the mean, and nothing else is looked at.
	Naive bool
	// High and Low are the two lines: rebalancing starts when the busiest
	// Group carries more than High times the mean, and goes on until it
	// carries less than Low times.
	High, Low float64
	// Rest is how long, in Store time, a Slot that has moved stays where it
	// is. Settle is how long after any Move, and after becoming Leader, the
	// Meta Leader waits for the load figures to catch up.
	Rest, Settle int64
	// Idle is the load per Group below which the store is taken to be idle
	// and nothing is moved.
	Idle uint64
}

// Balancer is what a Meta Leader remembers between looks at the load. None
// of it matters to anyone else: a new Leader starts with a new one.
type Balancer struct {
	term      core.Term
	since     int64
	balancing bool
}

// Decide is the Meta Leader's part in evening out load: it may pick one
// Slot to move and the Group to move it to. load is each Slot's load as
// gossip has brought it, term the Leader's Term, now its clock in the units
// of Store time, and healthy whether every Member of every Group seems
// alive.
//
// What damps it:
//   - one Move at a time across the store, and none for a while after one
//     finishes, until the figures have caught up with it;
//   - a Slot that has moved rests;
//   - a new Leader waits until it has heard enough;
//   - nothing while a Node is not healthy, or being replaced, or while the
//     store is close to idle;
//   - two lines, so that a Group near the line doesn't start and stop;
//   - a Slot is moved only if it is no more than half the gap between the
//     two Groups, so that the busier stays the busier and no Move can undo
//     another.
//
// The Move under way and when each Slot last moved are read from the table,
// so a new Leader forgets neither.
func (b *Balancer) Decide(cfg Balancing, table shard.Table, load []uint32, groups int, term core.Term, now int64, healthy bool) (shard.Slot, shard.GroupID, bool) {
	sums := GroupLoad(table, load, groups)
	var total uint64
	busiest, idlest := 1, 1
	for g := 1; g <= groups; g++ {
		total += sums[g]
		if sums[g] > sums[busiest] {
			busiest = g
		}
		if sums[g] < sums[idlest] {
			idlest = g
		}
	}
	mean := float64(total) / float64(groups)
	if total < uint64(groups)*cfg.Idle || total == 0 || busiest == idlest {
		return 0, 0, false
	}
	if cfg.Naive {
		if float64(sums[busiest]) <= cfg.High*mean {
			return 0, 0, false
		}
		slot := -1
		for s, o := range table.Slots {
			if int(o.Group) == busiest && o.MovingTo == 0 && (slot < 0 || load[s] > load[slot]) {
				slot = s
			}
		}
		return shard.Slot(max(slot, 0)), shard.GroupID(idlest), slot >= 0
	}

	if b.term != term {
		b.term, b.since, b.balancing = term, now, false
	}
	if now-b.since < cfg.Settle || !healthy {
		return 0, 0, false
	}
	for _, o := range table.Slots {
		if o.MovingTo != 0 || o.MovedAt != 0 && table.StoreTime-o.MovedAt < cfg.Settle {
			return 0, 0, false
		}
	}
	for _, row := range table.Groups {
		if row.Add != 0 {
			return 0, 0, false
		}
	}
	switch share := float64(sums[busiest]) / mean; {
	case share > cfg.High:
		b.balancing = true
	case share < cfg.Low:
		b.balancing = false
	}
	if !b.balancing {
		return 0, 0, false
	}
	gap := sums[busiest] - sums[idlest]
	slot := -1
	for s, o := range table.Slots {
		l := uint64(load[s])
		if int(o.Group) != busiest || l == 0 || 2*l > gap || o.MovedAt != 0 && table.StoreTime-o.MovedAt < cfg.Rest {
			continue
		}
		if slot < 0 || load[s] > load[slot] {
			slot = s
		}
	}
	return shard.Slot(max(slot, 0)), shard.GroupID(idlest), slot >= 0
}
