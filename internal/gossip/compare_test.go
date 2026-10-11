package gossip

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// lab is a set of Nodes on a network that delivers within the round, loses a
// share of Messages at random, and can be told to lose particular ones. It
// is for measuring the two detectors against each other (A§11.10).
type lab struct {
	*net
	rng   *rand.Rand
	loss  float64
	slow  int // a Node that takes a round only every third round
	round int
	// alarms counts the times some Node came to suspect a Node that was
	// running and reachable, and verdicts the times it took one for dead.
	alarms, verdicts int
	views            map[[2]int]Status
	wrong            func(at, about int) bool // is it wrong for at to think ill of about?
}

func newLab(n int, d Detector, suspect, dead int, seed uint64) *lab {
	l := &lab{net: &net{nodes: map[int]*Node{}, down: map[int]bool{}}, rng: rand.New(rand.NewPCG(seed, 99)), views: map[[2]int]Status{}}
	for id := 1; id <= n; id++ {
		cfg := Config{ID: id, Peer: addr(id), Rand: rand.New(rand.NewPCG(seed, uint64(id))), Detector: d, SuspectAfter: suspect, DeadAfter: dead}
		for s := 1; s <= n; s++ {
			cfg.Seeds = append(cfg.Seeds, Member{ID: s, Peer: addr(s)})
		}
		l.nodes[id] = New(cfg)
	}
	l.wrong = func(int, int) bool { return true }
	base := l.net.lose
	l.net.lose = func(from, to int) bool {
		return (base != nil && base(from, to)) || (l.loss > 0 && l.rng.Float64() < l.loss)
	}
	return l
}

// run takes rounds rounds, counting false alarms as it goes.
func (l *lab) run(rounds int) {
	for range rounds {
		l.round++
		for id := 1; id <= len(l.nodes); id++ {
			if l.down[id] || (id == l.slow && l.round%3 != 0) {
				continue
			}
			l.deliver(l.nodes[id].Tick())
		}
		for at, n := range l.nodes {
			if l.down[at] {
				continue
			}
			for _, m := range n.Members() {
				key := [2]int{at, m.ID}
				if m.Status > l.views[key] && !l.down[m.ID] && l.wrong(at, m.ID) {
					if m.Status == Dead {
						l.verdicts++
					} else if l.views[key] == Alive {
						l.alarms++
					}
				}
				l.views[key] = m.Status
			}
		}
	}
}

// until runs until every running Node thinks about of status, and returns
// how many rounds that took, or -1 after limit.
func (l *lab) until(about int, status Status, limit int) int {
	for r := 0; r <= limit; r++ {
		agreed := true
		for at, n := range l.nodes {
			if l.down[at] || at == about {
				continue
			}
			for _, m := range n.Members() {
				if m.ID == about && m.Status != status {
					agreed = false
				}
			}
		}
		if agreed {
			return r
		}
		l.run(1)
	}
	return -1
}

type setting struct {
	name          string
	d             Detector
	suspect, dead int
}

var settings = []setting{
	{"counters 5/15", Counters, 5, 15},
	{"counters 10/25", Counters, 10, 25},
	{"counters 15/35", Counters, 15, 35},
	{"swim 3/9", SWIM, 3, 9},
	{"swim 5/15", SWIM, 5, 15},
	{"swim 8/24", SWIM, 8, 24},
}

// TestCompareDetectors measures both detectors, at three settings each, on
// the same questions. Run it with -v for the table. It fails only if a
// detector misses a dead Node altogether.
func TestCompareDetectors(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement, and a slow one")
	}
	const trials = 20
	for _, nodes := range []int{5, 12} {
		t.Logf("%d Nodes, %d trials each. Times are in rounds.", nodes, trials)
		t.Logf("%-16s | %-16s | %-44s | %-44s | %-10s | %s", "detector", "dead Node noticed", "wrongly suspected, per 1,000 rounds", "wrongly taken for dead, per 1,000 rounds", "after heal", "Messages")
		t.Logf("%-16s | %-16s | %-44s | %-44s | %-10s | %s", "", "suspected / dead", "quiet / 10% lost / 30% lost / slow / cut", "quiet / 10% lost / 30% lost / slow / cut", "all alive", "per Node per round")
		for _, s := range settings {
			var suspected, dead, healed, sent int
			alarms, verdicts := map[string]int{}, map[string]int{}
			for trial := range trials {
				seed := uint64(1000*nodes + trial)

				// A Node dies.
				l := newLab(nodes, s.d, s.suspect, s.dead, seed)
				l.run(30)
				before := l.sent
				l.run(100)
				sent += l.sent - before
				l.down[2] = true
				a := l.until(2, Suspect, 400)
				if s.d == Counters {
					// Counters go from suspect to dead without stopping.
					a = min(a, l.until(2, Dead, 0)+a)
				}
				b := l.until(2, Dead, 400)
				if b < 0 {
					t.Fatalf("%s, %d Nodes, trial %d: a dead Node was never noticed", s.name, nodes, trial)
				}
				suspected += max(a, 0)
				dead += max(a, 0) + b

				// Nothing is wrong, under more and more trouble.
				for _, c := range []struct {
					name  string
					setup func(*lab)
				}{
					{"quiet", func(*lab) {}},
					{"lost10", func(l *lab) { l.loss = 0.10 }},
					{"lost30", func(l *lab) { l.loss = 0.30 }},
					{"slow", func(l *lab) { l.slow = 3 }},
					{"cut", func(l *lab) {
						l.net.lose = func(from, to int) bool { return (from == 1 && to == 2) || (from == 2 && to == 1) }
					}},
				} {
					l := newLab(nodes, s.d, s.suspect, s.dead, seed)
					l.run(30)
					c.setup(l)
					l.alarms, l.verdicts = 0, 0
					l.run(1000)
					alarms[c.name] += l.alarms
					verdicts[c.name] += l.verdicts
				}

				// A Partition, held until each side has given up the other,
				// then healed.
				l = newLab(nodes, s.d, s.suspect, s.dead, seed)
				l.run(30)
				side := func(id int) bool { return id <= nodes/2 }
				l.net.lose = func(from, to int) bool { return side(from) != side(to) }
				l.run(150)
				l.net.lose = nil
				worst := 0
				for id := 1; id <= nodes; id++ {
					worst = max(worst, l.until(id, Alive, 400))
				}
				healed += worst
			}
			per := func(n int) string { return fmt.Sprintf("%.1f", float64(n)/trials) }
			row := func(m map[string]int) string {
				return fmt.Sprintf("%6s / %7s / %8s / %6s / %-6s", per(m["quiet"]), per(m["lost10"]), per(m["lost30"]), per(m["slow"]), per(m["cut"]))
			}
			t.Logf("%-16s | %5s / %-8s | %s | %s | %-10s | %.1f", s.name,
				per(suspected), per(dead), row(alarms), row(verdicts),
				per(healed), float64(sent)/trials/100/float64(nodes))
		}
	}
}
