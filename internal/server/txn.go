package server

import (
	"context"
	"slices"
	"time"

	"distributed-kv-store/internal/committer"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
)

// The waits of a Transaction across Groups between real Nodes (A§12), in
// milliseconds. They stand in the same order as the Simulation's, and are
// sized for a step that takes about 100 ms, which is what one takes here
// with 64 clients and twelve replicas on one disk. With the waits first
// chosen, for a step of 16 ms, a younger Transaction gave way after 400 ms
// to an older one that needed that long just to finish, and a step was sent
// a second time while the first was still on its way.
const (
	txnAskAgain   = 1000 // before a step is sent again
	txnGiveUp     = 4000 // before a Leader aborts for want of an answer
	lockAskAfter  = 400  // how old a Lock is before its Group asks about it
	lockAskAgain  = 20   // how soon it asks again while a request waits
	lockWait      = 60   // how long a request for a Locked key waits
	txnRetry      = 20   // before a Transaction tries again for a Locked key
	txnWait       = 1500 // how long a younger one keeps trying
	txnWoundAgain = 1000 // before the same Transaction is asked to abort again
)

// txnEnv is what the committer sees of a real Node. Answers to what it
// asked come back through calls, so that the Agent is touched only on the
// committer's goroutine.
type txnEnv struct {
	s   *Store
	ctx context.Context
}

func (e txnEnv) Now() int64 { return time.Now().UnixMilli() }

func (e txnEnv) WithMachine(g shard.GroupID, fn func(*shardfsm.Machine)) {
	ctx, cancel := context.WithTimeout(e.ctx, e.s.Timeout)
	defer cancel()
	if n, hosted := e.s.replica(g); hosted {
		n.Inspect(ctx, func(m Machine) { fn(m.(*shardfsm.Machine)) })
	}
}

func (e txnEnv) Local(g shard.GroupID, payload []byte) {
	go func() {
		ctx, cancel := context.WithTimeout(e.ctx, e.s.Timeout)
		defer cancel()
		if n, hosted := e.s.replica(g); hosted {
			n.Propose(ctx, payload)
		}
		e.s.kick()
	}()
}

func (e txnEnv) Ask(g shard.GroupID, read bool, payload []byte, back func(bool, []byte)) {
	go func() {
		ctx, cancel := context.WithTimeout(e.ctx, e.s.Timeout)
		defer cancel()
		o, raw := e.s.ask(ctx, g, read, false, payload)
		select {
		case e.s.txnCalls <- func() { back(o == answered, raw) }:
		case <-e.ctx.Done():
		}
	}()
}

// kick has the committer look for work now.
func (s *Store) kick() {
	select {
	case s.txnKick <- struct{}{}:
	default:
	}
}

// nudge has the committer ask about this Node's Locks without waiting for
// them to grow old: a request has found a key Locked.
func (s *Store) nudge() {
	select {
	case s.txnCalls <- func() { s.committer.Nudge() }:
	default:
	}
}

// commit runs this Node's committer until ctx ends: it carries on the
// Transactions of every data Group whose Leader is here (A§12.2).
func (s *Store) commit(ctx context.Context) {
	every := max(s.Tick/4, 5*time.Millisecond)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	e := txnEnv{s: s, ctx: ctx}
	for {
		select {
		case <-ctx.Done():
			return
		case fn := <-s.txnCalls:
			fn()
		case <-s.txnKick:
		case <-ticker.C:
		}
		// Take in everything that has come back before looking again.
		for more := true; more; {
			select {
			case fn := <-s.txnCalls:
				fn()
			default:
				more = false
			}
		}
		step, cancel := context.WithTimeout(ctx, s.Timeout)
		var leads []shard.GroupID
		for g, n := range s.replicas() {
			if st, ok := n.Status(step); ok && g != shard.Meta && st.Role == core.LeaderRole {
				leads = append(leads, g)
			}
		}
		cancel()
		slices.Sort(leads)
		s.committer.Step(e, leads)
	}
}

// yield asks the committer what a Transaction that found a key Locked
// should do (committer.Agent.Yield), from a request's goroutine.
func (s *Store) yield(ctx context.Context, me shardfsm.Held, resp fsm.Response, since int64) bool {
	answer := make(chan bool, 1)
	ask := func() { answer <- s.committer.Yield(txnEnv{s: s, ctx: s.txnCtx}, me, resp, since, false) }
	select {
	case s.txnCalls <- ask:
	case <-ctx.Done():
		return false
	}
	select {
	case again := <-answer:
		return again
	case <-ctx.Done():
		return false
	}
}

// txnName is a number no other request on any Node has, that rises with
// the time: the clock, made unique on this Node, then this Node's number.
func (s *Store) txnName() uint64 {
	for {
		last := s.txnLast.Load()
		next := max(uint64(time.Now().UnixNano()), last+1)
		if s.txnLast.CompareAndSwap(last, next) {
			return next<<8 | uint64(s.Node&0xff)
		}
	}
}

// partsOf divides a Transaction between the Groups the table gives for its
// keys. The Coordinator Group, which owns the lowest key, comes first.
func partsOf(t shard.Table, cmd fsm.Command) []shardfsm.Part {
	var parts []shardfsm.Part
	of := func(key string) *shardfsm.Part {
		g := t.OwnerOf(key)
		for i := range parts {
			if parts[i].Group == g {
				return &parts[i]
			}
		}
		parts = append(parts, shardfsm.Part{Group: g})
		return &parts[len(parts)-1]
	}
	lowest := ""
	note := func(key string) {
		if lowest == "" || key < lowest {
			lowest = key
		}
	}
	for _, c := range cmd.Conds {
		p := of(c.Key)
		p.Conds = append(p.Conds, c)
		note(c.Key)
	}
	for _, w := range cmd.Writes {
		p := of(w.Key)
		p.Writes = append(p.Writes, w)
		note(w.Key)
	}
	lead := t.OwnerOf(lowest)
	slices.SortFunc(parts, func(a, b shardfsm.Part) int {
		switch {
		case a.Group == lead:
			return -1
		case b.Group == lead:
			return 1
		}
		return int(a.Group) - int(b.Group)
	})
	return parts
}

// transact puts a Transaction whose keys are in more than one Group to its
// Coordinator Group and waits for the decision (A§12.2). This Node keeps
// nothing that matters: if it stops, the Transaction goes on without it.
// It returns the Transaction's age, to send with the next attempt if this
// one is aborted to make way for another (A§12.5).
func (s *Store) transact(ctx context.Context, cmd fsm.Command, parts []shardfsm.Part, age int64) (outcome, fsm.Response, int64) {
	now := s.Table().StoreTime
	if age <= 0 || age > now {
		age = max(now, 1)
	}
	id := shardfsm.TxnID{Session: cmd.Session, Seq: cmd.Seq}
	if id.Session == 0 {
		// A request outside any Session still needs a name of its own, and
		// one that orders it: of two with the same age, the one that
		// arrived first is the older. A name drawn at random would make a
		// newcomer the older half the time, and it would have the
		// Transaction in front of it aborted for nothing.
		id.Seq = s.txnName()
	}
	lead := parts[0].Group
	begin := shardfsm.TxnBeginStep(id, parts, now, cmd.Register)
	begin.Age = age
	started, tries := time.Now().UnixMilli(), 0
	pause := func(d time.Duration) bool {
		select {
		case <-time.After(d):
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		o, raw := s.ask(ctx, lead, false, false, begin.Encode())
		if o != answered {
			return o, fsm.Response{}, age
		}
		state, resp, err := shardfsm.DecodeState(raw)
		switch {
		case err != nil || state == shardfsm.TxnNone:
			return unknown, fsm.Response{}, age
		case state == shardfsm.TxnRefused && resp.Status == fsm.StatusTxnConflict &&
			s.yield(ctx, shardfsm.Held{ID: id, Coord: lead, Age: age}, resp, started):
			// The Coordinator Group's own keys are Locked, and nothing is
			// held for this Transaction yet: try again shortly.
			if !pause(time.Duration(committer.Backoff(txnRetry, tries)) * time.Millisecond) {
				return answered, resp, age
			}
			tries++
			continue
		case state != shardfsm.TxnBegun:
			return answered, resp, age
		}
		s.kick()
		// It is under way. Ask until it is decided.
		for wait := 2 * time.Millisecond; ; wait = min(2*wait, 20*time.Millisecond) {
			if !pause(wait) {
				return unknown, fsm.Response{}, age
			}
			o, raw := s.ask(ctx, lead, true, false, shardfsm.OutcomeQuery(id, 0))
			state, resp, _, err := shardfsm.DecodeOutcome(raw)
			switch {
			case o != answered || err != nil || state == shardfsm.TxnNone:
				// Whoever asks next must take the outcome as unknown.
				return unknown, fsm.Response{}, age
			case state == shardfsm.TxnDecided:
				return answered, resp, age
			}
		}
	}
}
