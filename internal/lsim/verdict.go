package lsim

import (
	"fmt"
	"slices"

	"distributed-kv-store/internal/leaderless"
)

// Report is what a run is judged on (A§13.11).
type Report struct {
	Scenario string
	Seed     uint64
	// Apart lists what keeps the store from being Converged: Home Nodes
	// that hold different things for a key, and Hints not handed over.
	Apart []string
	// Lost lists elements a client was told were added and that are gone
	// though nothing removed them.
	Lost []string
	// Back lists elements a client was told were removed and that are
	// there though nothing added them again.
	Back []string
	// Requests and Answered count every request that reached a running
	// Node and those the store said succeeded. CutRequests and CutAnswered
	// count those that began during a Partition.
	Requests, Answered       int
	CutRequests, CutAnswered int
	// WindowAnswered counts the requests answered that began in the time
	// set aside for Faults, whether or not this run had any. Set beside
	// the same count from a run with no Faults, it says how much of the
	// store's work went on through them.
	WindowAnswered int
	// Reads counts reads that were answered, and Stale those that missed
	// an element whose addition a client had been told of before the read
	// began and which nothing could have removed.
	Reads, Stale int
	// Adds and Removes count the writes the store said succeeded.
	Adds, Removes int
	// HintsLeft is how many Hints were still held at the end.
	HintsLeft int
}

// Passed reports whether the run ended Converged with nothing lost and
// nothing back.
func (r Report) Passed() bool { return len(r.Apart) == 0 && len(r.Lost) == 0 && len(r.Back) == 0 }

func (r Report) String() string {
	state := "PASS"
	if !r.Passed() {
		state = "FAIL"
	}
	s := fmt.Sprintf("%s %s seed=%d apart=%d lost=%d back=%d answered=%d/%d during-cut=%d/%d stale=%d/%d", state, r.Scenario, r.Seed,
		len(r.Apart), len(r.Lost), len(r.Back), r.Answered, r.Requests, r.CutAnswered, r.CutRequests, r.Stale, r.Reads)
	for _, list := range [][]string{r.Apart, r.Lost, r.Back} {
		if len(list) > 0 {
			s += " | " + list[0]
		}
	}
	return s
}

// Judge gives the verdicts on a run that has been healed and left to
// settle.
func Judge(s *Sim, h *History, mode leaderless.Mode) Report {
	var rep Report
	ring := leaderless.NewRing(s.ids)

	// Converged, and what the store finally holds of each key: the join of
	// what its Home Nodes hold.
	keys := map[string]bool{}
	for _, id := range s.ids {
		if !s.Up(id) {
			continue
		}
		rep.HintsLeft += s.Node(id).Hints()
		if n := s.Node(id).Hints(); n > 0 {
			rep.Apart = append(rep.Apart, fmt.Sprintf("Node %d still holds %d Hints", id, n))
		}
		for _, k := range s.Node(id).Keys() {
			keys[k] = true
		}
	}
	for _, c := range h.Calls {
		keys[c.Key] = true
	}
	final := map[string]leaderless.State{}
	names := make([]string, 0, len(keys))
	for k := range keys {
		names = append(names, k)
	}
	slices.Sort(names)
	for _, k := range names {
		var first *leaderless.State
		var joined leaderless.State
		for _, home := range ring.Homes(k) {
			if !s.Up(home) {
				continue
			}
			held := s.Node(home).Holds(k)
			joined = leaderless.Join(mode, joined, held)
			if first == nil {
				first = &held
			} else if !first.Equal(held) {
				rep.Apart = append(rep.Apart, fmt.Sprintf("key %s: Home Node %d holds %v and another holds %v", k, home, held.Values(), first.Values()))
			}
		}
		final[k] = joined
	}

	type target struct{ key, elem string }
	adds, removes := map[target][]*Call{}, map[target][]*Call{}
	for _, c := range h.Calls {
		if c.Result == Refused {
			continue
		}
		rep.Requests++
		if c.Cut {
			rep.CutRequests++
		}
		if c.Result == Done {
			rep.Answered++
			if c.Cut {
				rep.CutAnswered++
			}
		}
		if c.End == 0 {
			c.End = s.now // never answered: it may yet have taken effect
		}
		switch c.Op {
		case leaderless.Add:
			adds[target{c.Key, c.Elem}] = append(adds[target{c.Key, c.Elem}], c)
			if c.Result == Done {
				rep.Adds++
			}
		case leaderless.Remove:
			removes[target{c.Key, c.Elem}] = append(removes[target{c.Key, c.Elem}], c)
			if c.Result == Done {
				rep.Removes++
			}
		}
	}
	// mayHaveRemoved: some remove could have seen the addition a began.
	// A remove sees only what was added before it ended.
	mayHaveRemoved := func(t target, a *Call) bool {
		return slices.ContainsFunc(removes[t], func(r *Call) bool { return r.End > a.Begin })
	}

	if mode == leaderless.AddWins {
		// Exact, by the Dots: an addition is lost if its Dot is gone and no
		// remove took it out; an element is back if a Dot a remove took out
		// is there.
		removed := map[string][]leaderless.Dot{}
		for _, c := range h.Calls {
			if c.Op == leaderless.Remove && c.Result != Refused {
				removed[c.Key] = append(removed[c.Key], s.Traces[c.Ref].Observed...)
			}
		}
		for _, c := range h.Calls {
			tr := s.Traces[c.Ref]
			switch {
			case c.Op == leaderless.Add && c.Result == Done && tr.Added != nil:
				if !slices.Contains(final[c.Key].Elems[c.Elem], *tr.Added) && !slices.Contains(removed[c.Key], *tr.Added) {
					rep.Lost = append(rep.Lost, fmt.Sprintf("%s in %s, added by client %d at %d", c.Elem, c.Key, c.Client, c.Begin))
				}
			case c.Op == leaderless.Remove && c.Result == Done:
				for _, d := range tr.Observed {
					if slices.Contains(final[c.Key].Elems[c.Elem], d) {
						rep.Back = append(rep.Back, fmt.Sprintf("%s in %s, removed by client %d at %d", c.Elem, c.Key, c.Client, c.Begin))
						break
					}
				}
			}
		}
	} else {
		// By the times of the requests, which is all there is to go by.
		for t, list := range adds {
			_, there := final[t.key].Elems[t.elem]
			for _, a := range list {
				if a.Result == Done && !there && !mayHaveRemoved(t, a) {
					rep.Lost = append(rep.Lost, fmt.Sprintf("%s in %s, added by client %d at %d", t.elem, t.key, a.Client, a.Begin))
				}
			}
		}
		for t, list := range removes {
			if _, there := final[t.key].Elems[t.elem]; !there {
				continue
			}
			for _, r := range list {
				addedSince := slices.ContainsFunc(adds[t], func(a *Call) bool { return a.End > r.Begin })
				if r.Result == Done && !addedSince {
					rep.Back = append(rep.Back, fmt.Sprintf("%s in %s, removed by client %d at %d", t.elem, t.key, r.Client, r.Begin))
				}
			}
		}
		slices.Sort(rep.Lost)
		slices.Sort(rep.Back)
	}

	// Stale reads.
	for _, c := range h.Calls {
		if c.Op != leaderless.Get || c.Result != Done {
			continue
		}
		rep.Reads++
		stale := false
		for t, list := range adds {
			if t.key != c.Key || slices.Contains(c.Values, t.elem) {
				continue
			}
			for _, a := range list {
				if a.Result == Done && a.End < c.Begin && !mayHaveRemoved(t, a) {
					stale = true
				}
			}
		}
		if stale {
			rep.Stale++
		}
	}
	return rep
}
