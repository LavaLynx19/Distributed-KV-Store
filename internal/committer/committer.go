// Package committer carries Transactions across Groups to their end
// (A§12). It is the part a Node plays for the Groups it leads: for a Group
// that coordinates a Transaction, sending the prepares, proposing the
// decision and sending the finishes; for a Group that has keys Locked for
// one, asking what was decided. It keeps nothing that matters. Everything a
// new Leader needs is in the Groups' Logs (shardfsm), and an Agent that
// starts empty finds it there.
//
// The Simulation and a real Node run the same Agent, each through its own
// Env.
package committer

import (
	"slices"

	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
)

// Env is what an Agent needs from the Node it runs on.
type Env interface {
	// Now is the time, in whatever units the Agent's waits are in.
	Now() int64
	// WithMachine runs fn on the state machine of this Node's replica of
	// Group g, and returns when fn has run. fn must not keep the Machine.
	WithMachine(g shard.GroupID, fn func(*shardfsm.Machine))
	// Local proposes payload to this Node's replica of Group g, and
	// doesn't wait to hear what became of it.
	Local(g shard.GroupID, payload []byte)
	// Ask sends payload to Group g, wherever its Leader is: as a read
	// answered from the Leader's state, or as a proposal. back is called
	// with the state machine's answer, or with answered false if there
	// was none. It may never be called at all. It must be called where
	// the Agent's other methods are: an Agent is not safe for use from
	// several goroutines.
	Ask(g shard.GroupID, read bool, payload []byte, back func(answered bool, raw []byte))
}

// ConflictMode is what a Transaction does about a key another has Locked.
type ConflictMode uint8

const (
	// RefuseAtOnce aborts it, as in stage 8a. Nothing ever waits, so
	// nothing can wait for ever.
	RefuseAtOnce ConflictMode = iota
	// WaitInTurn has it wait for the key, however long that takes, with
	// no order between Transactions. It is the store stage 8b starts from.
	WaitInTurn
	// OlderWins has the older of the two abort the younger, and the
	// younger wait a while for the older (A§12.5).
	OlderWins
)

// Agent is one Node's committer.
type Agent struct {
	Conflicts ConflictMode
	// NoAsking leaves a Group that has Locked keys waiting to be told the
	// outcome, never asking for it. It exists so that the Locks this
	// leaves behind stay reproducible.
	NoAsking bool

	// AskAgain is how long the Agent waits for an answer before it sends
	// a step again, and GiveUp how long a Leader waits for every Group's
	// answer before it aborts.
	AskAgain, GiveUp int64
	// LockAskAfter is how old a Lock is before its Group asks what was
	// decided, and LockAskAgain how soon it asks again while a request is
	// waiting.
	LockAskAfter, LockAskAgain int64
	// Retry is how soon a Transaction that found a key Locked tries for it
	// again, Wait how long a younger one goes on trying for an older one's
	// key, and WoundAgain how long before the Agent asks a second time for
	// the same Transaction to be aborted.
	Retry, Wait, WoundAgain int64

	// OnWound and OnAsked, if set, are told each time the Agent asks for a
	// Transaction to be aborted, and each time a Group finishes one on the
	// answer to its own question.
	OnWound, OnAsked func()

	open    map[txnKey]*state
	wounded map[shardfsm.TxnID]int64
	// settled is when the Agent last asked what became of each
	// Transaction that was in another's way.
	settled map[txnKey]int64
	nudged  bool
}

// txnKey names a Transaction the Agent is carrying on for a Group.
type txnKey struct {
	group shard.GroupID
	id    shardfsm.TxnID
}

// state is what the Agent remembers of one of them.
type state struct {
	since int64
	// votes are the other Groups' answers to their prepares, and asked
	// when each was last sent a step.
	votes map[shard.GroupID]fsm.Response
	yes   map[shard.GroupID]bool
	asked map[shard.GroupID]int64
	// proposed is when the Agent last proposed a decision, or last asked
	// for one.
	proposed int64
	// blocked is when each Group first answered that a key was Locked by
	// another Transaction, while this one is still trying for it.
	blocked map[shard.GroupID]int64
	// tries is how often each Group has given that answer in a row.
	tries map[shard.GroupID]int
	// finishing is set once the Agent has seen the Transaction decided.
	finishing bool
}

// Reset forgets everything, as a Node that restarts does.
func (a *Agent) Reset() {
	// New maps, not nil: an answer to something asked before the restart
	// may still arrive and be recorded.
	a.open, a.wounded, a.nudged = map[txnKey]*state{}, map[shardfsm.TxnID]int64{}, false
	a.settled = map[txnKey]int64{}
}

// Nudge has the Agent ask about Locks at its next Step without waiting for
// them to grow old: a request has found a key Locked.
func (a *Agent) Nudge() { a.nudged = true }

// Step takes the next step for every Transaction that the Groups in leads
// coordinate or have Locked keys for. leads must be the data Groups whose
// Leader is on this Node.
func (a *Agent) Step(env Env, leads []shard.GroupID) {
	if a.open == nil {
		a.Reset()
	}
	seen := map[txnKey]bool{}
	for _, g := range leads {
		var coordinating []shardfsm.Coordinated
		var holding []shardfsm.Held
		env.WithMachine(g, func(m *shardfsm.Machine) { coordinating, holding = m.Coordinating(), m.Holding() })
		for _, t := range coordinating {
			key := txnKey{g, t.ID}
			seen[key] = true
			a.coordinate(env, g, t, key)
		}
		for _, h := range holding {
			if a.NoAsking {
				break
			}
			key := txnKey{g, h.ID}
			seen[key] = true
			a.askOutcome(env, g, h, key)
		}
	}
	for key := range a.open {
		if !seen[key] {
			delete(a.open, key)
		}
	}
	a.nudged = false
}

// askOutcome has Group g ask the Coordinator Group of a Transaction it has
// Locked keys for what was decided, once the Locks are old enough or a
// request is waiting for them, and finish the Transaction here if there is
// an answer (A§12.3).
func (a *Agent) askOutcome(env Env, g shard.GroupID, h shardfsm.Held, key txnKey) {
	now := env.Now()
	st := a.open[key]
	if st == nil {
		st = &state{since: now, proposed: now - a.AskAgain}
		a.open[key] = st
	}
	switch {
	case a.nudged && now-st.proposed >= a.LockAskAgain:
	case now-st.since >= a.LockAskAfter && now-st.proposed >= a.AskAgain:
	default:
		return
	}
	st.proposed = now
	env.Ask(h.Coord, true, shardfsm.OutcomeQuery(h.ID, g), func(answered bool, raw []byte) {
		state, resp, offer, err := shardfsm.DecodeOutcome(raw)
		if !answered || err != nil {
			return
		}
		var finish shardfsm.TxnStep
		switch state {
		case shardfsm.TxnDecided:
			finish = shardfsm.FinishStep(h.ID, h.Coord, false, resp.Status == fsm.StatusOK, resp.Version, offer)
		case shardfsm.TxnNone:
			// No record means abort: a begin is Committed before any
			// prepare is sent.
			finish = shardfsm.FinishStep(h.ID, h.Coord, false, false, 0, 0)
		default:
			return // not decided yet
		}
		if a.OnAsked != nil {
			a.OnAsked()
		}
		env.Local(g, finish.Encode())
	})
}

// coordinate takes the next step for one Transaction that Group g
// coordinates.
func (a *Agent) coordinate(env Env, g shard.GroupID, t shardfsm.Coordinated, key txnKey) {
	now := env.Now()
	st := a.open[key]
	if st == nil {
		st = &state{since: now, votes: map[shard.GroupID]fsm.Response{}, yes: map[shard.GroupID]bool{}, asked: map[shard.GroupID]int64{}, blocked: map[shard.GroupID]int64{}, tries: map[shard.GroupID]int{}, proposed: now - a.AskAgain}
		a.open[key] = st
	}
	due := func(p shardfsm.Part) bool {
		at, asked := st.asked[p.Group]
		return !asked || now-at >= a.AskAgain
	}

	if t.Decided {
		if !st.finishing {
			// The finishes are a new step: when the prepares were sent has
			// no bearing on when these are due.
			st.finishing = true
			clear(st.asked)
		}
		for _, p := range t.Parts {
			if slices.Contains(t.Confirmed, p.Group) || !due(p) {
				continue
			}
			st.asked[p.Group] = now
			finish := shardfsm.FinishStep(t.ID, g, false, t.Commit, t.Version, t.Offer(p.Group)).Encode()
			env.Ask(p.Group, false, finish, func(answered bool, _ []byte) {
				if answered {
					env.Local(g, shardfsm.ConfirmStep(t.ID, p.Group).Encode())
				}
			})
		}
		return
	}

	for _, p := range t.Parts {
		if _, voted := st.votes[p.Group]; voted || !due(p) {
			continue
		}
		st.asked[p.Group] = now
		prepare := shardfsm.PrepareStep(t.ID, g, false, p.Conds, p.Writes, t.Stamp, false).Aged(t.Age).Encode()
		env.Ask(p.Group, false, prepare, func(answered bool, raw []byte) {
			vote, resp, err := shardfsm.DecodeVote(raw)
			if !answered || err != nil {
				return // asked again in a while
			}
			if vote == shardfsm.VoteNo && resp.Status == fsm.StatusTxnConflict {
				if _, was := st.blocked[p.Group]; !was {
					st.blocked[p.Group] = env.Now()
				}
				a.settle(env, p.Group, resp)
				if a.Yield(env, shardfsm.Held{ID: t.ID, Coord: g, Age: t.Age}, resp, st.blocked[p.Group], true) {
					// Nothing is held there: try for the key again shortly,
					// and less often each time. Every try is an Entry in
					// that Group's Log, which the Transaction in the way
					// needs room in to finish.
					st.asked[p.Group] = env.Now() - a.AskAgain + Backoff(a.Retry, st.tries[p.Group])
					st.tries[p.Group]++
					return
				}
			}
			st.votes[p.Group], st.yes[p.Group] = resp, vote == shardfsm.VoteYes
		})
	}
	if now-st.proposed < a.AskAgain {
		return
	}
	giveUp := a.GiveUp
	if a.Conflicts == WaitInTurn {
		giveUp = 1 << 60 // the naive store waits for as long as it takes
	}
	commit := t.Own == shardfsm.VoteYes
	version := t.OwnResp.Version
	var failed []fsm.Item
	var offers []shardfsm.Offer
	conflict := false
	for _, p := range t.Parts {
		resp, voted := st.votes[p.Group]
		switch {
		case !voted:
			if now-st.since < giveUp {
				return // not every Group has answered, and there is time yet
			}
			commit, conflict = false, true
		case st.yes[p.Group]:
			version = max(version, resp.Version)
			offers = append(offers, shardfsm.Offer{Group: p.Group, Version: resp.Version})
		case resp.Status == fsm.StatusVersionMismatch:
			commit = false
			failed = append(failed, resp.Items...)
		default:
			commit, conflict = false, true
		}
	}
	if conflict {
		// A decision to abort over a conflict must not be reported as
		// failed conditions: they weren't all looked at.
		failed = nil
	}
	st.proposed = now
	env.Local(g, shardfsm.DecideStep(t.ID, commit, version, offers, failed).Encode())
}

// Backoff is how long to wait before the try after tries failed ones: the
// first wait, doubled each time up to eight times over.
func Backoff(first int64, tries int) int64 { return first << min(tries, 3) }

// settle is what a prepare does about the Transaction that has its key
// Locked in Group g, before it thinks of waiting: it asks that
// Transaction's Coordinator Group what was decided, and if it is over,
// tells g so. A Lock can outlive its Transaction where a prepare arrived
// after the finish, and g would otherwise find out only when the Lock had
// grown old (A§12.3).
func (a *Agent) settle(env Env, g shard.GroupID, resp fsm.Response) {
	holder, named := shardfsm.DecodeHeld(resp.Value)
	now, key := env.Now(), txnKey{g, holder.ID}
	if at, asked := a.settled[key]; !named || holder.Coord == g || a.NoAsking || (asked && now-at < a.LockAskAgain) {
		return
	}
	for k, at := range a.settled {
		if now-at >= a.AskAgain {
			delete(a.settled, k)
		}
	}
	a.settled[key] = now
	env.Ask(holder.Coord, true, shardfsm.OutcomeQuery(holder.ID, g), func(answered bool, raw []byte) {
		state, was, offer, err := shardfsm.DecodeOutcome(raw)
		if !answered || err != nil || state == shardfsm.TxnBegun {
			return
		}
		commit := state == shardfsm.TxnDecided && was.Status == fsm.StatusOK
		finish := shardfsm.FinishStep(holder.ID, holder.Coord, false, commit, was.Version, offer)
		env.Ask(g, false, finish.Encode(), func(bool, []byte) {})
	})
}

// Yield decides what a Transaction does on finding a key Locked by
// another, and reports whether it should try for the key again. resp is
// the answer that said the key was Locked, and since is when this
// Transaction first found it so. With OlderWins, the older of the two has
// the younger aborted and tries again; the younger tries again for a while
// and then gives way.
//
// holding says whether this Transaction has keys Locked anywhere yet. One
// that has none can't be part of a ring of Transactions waiting for each
// other, so it has no need to abort anybody at once: it would only throw
// away the work of one that may be a moment from done. It waits as long as
// a younger one would, and has the other aborted only then, so that it
// isn't kept out for ever.
func (a *Agent) Yield(env Env, me shardfsm.Held, resp fsm.Response, since int64, holding bool) bool {
	holder, named := shardfsm.DecodeHeld(resp.Value)
	now := env.Now()
	switch a.Conflicts {
	case WaitInTurn:
		return true
	case OlderWins:
		if !named {
			return false
		}
		if !me.Older(holder) {
			return now-since < a.Wait
		}
		if !holding && now-since < a.Wait {
			return true
		}
		// The younger one is aborted by its own Coordinator Group, if it
		// isn't decided yet. Either way its Locks go. It is asked once,
		// and again only after a good while: each asking is an Entry in
		// that Group's Log, and a crowd of Transactions asking every time
		// they look would leave it room for little else.
		if a.wounded == nil {
			a.Reset()
		}
		if at, asked := a.wounded[holder.ID]; !asked || now-at >= a.WoundAgain {
			for id, at := range a.wounded {
				if now-at >= a.WoundAgain {
					delete(a.wounded, id)
				}
			}
			a.wounded[holder.ID] = now
			if a.OnWound != nil {
				a.OnWound()
			}
			env.Ask(holder.Coord, false, shardfsm.DecideStep(holder.ID, false, 0, nil, nil).Encode(), func(bool, []byte) {})
		}
		return now-since < a.GiveUp
	}
	return false
}
