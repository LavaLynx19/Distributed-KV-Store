package cluster

import (
	"slices"

	"distributed-kv-store/internal/committer"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
	"distributed-kv-store/internal/sim"
)

// Transactions is how a Transaction whose keys are in more than one Group
// is carried out (A§12).
type Transactions struct {
	// On carries them out. Without it they are refused, as in Rung 7.
	On bool
	// Memory has the Node that was asked carry a Transaction out and keep
	// what it decided in memory (transact). Without it the Coordinator
	// Group's Log holds every step (begin, coordinate).
	Memory bool
	// ReadLocked lets a Locked key be read (shardfsm.Config.ReadLocked).
	// It is one of the stores Rung 8 starts from.
	ReadLocked bool
	// NoOffset is another (shardfsm.Config.NoOffset).
	NoOffset bool
	// Conflicts is what a Transaction does when it finds a key Locked by
	// another.
	Conflicts ConflictMode
	// MovesAbort has a Move abort the undecided Transactions that hold
	// keys in its Slot, and not wait for them (A§12.7). Forget has a
	// Coordinator Group drop its record of a Transaction once every Group
	// has confirmed (A§12.2).
	MovesAbort, Forget bool
	// NoAsking leaves a Group that has Locked keys waiting to be told the
	// outcome, never asking for it. It exists so that the Locks this
	// leaves behind stay reproducible.
	NoAsking bool
}

// ConflictMode is what a Transaction does about a key another has Locked
// (committer.ConflictMode).
type ConflictMode = committer.ConflictMode

const (
	RefuseAtOnce = committer.RefuseAtOnce
	WaitInTurn   = committer.WaitInTurn
	OlderWins    = committer.OlderWins
)

// part is one Group's share of a Transaction.
type part struct {
	group  shard.GroupID
	conds  []fsm.Cond
	writes []fsm.Write
}

const (
	txnEvery      = 2   // how often a Node looks at the Transactions its Groups coordinate
	txnAskAgain   = 40  // how long it waits for an answer before sending a step again
	txnGiveUp     = 400 // how long a Leader waits for every Group's answer before it aborts
	lockAskAfter  = 30  // how old a Lock is before its Group asks what was decided
	lockAskAgain  = 6   // how soon it asks again while a request is waiting
	lockWait      = 12  // how long a request for a Locked key waits before it is refused
	txnRetry      = 10  // how soon a Transaction that found a key Locked tries for it again
	txnWait       = 80  // how long a younger Transaction waits for an older one's keys
	txnWoundAgain = 100 // how long before a Node asks again for the same Transaction to be aborted
)

// split divides a Transaction between the Groups the table gives for its
// keys. The Coordinator Group, which owns the lowest key, comes first.
func split(t shard.Table, cmd fsm.Command) []*part {
	var parts []*part
	of := func(key string) *part {
		g := t.OwnerOf(key)
		for _, p := range parts {
			if p.group == g {
				return p
			}
		}
		parts = append(parts, &part{group: g})
		return parts[len(parts)-1]
	}
	for _, c := range cmd.Conds {
		p := of(c.Key)
		p.conds = append(p.conds, c)
	}
	for _, w := range cmd.Writes {
		p := of(w.Key)
		p.writes = append(p.writes, w)
	}
	lead := t.OwnerOf(slices.Min(txnKeys(cmd)))
	slices.SortFunc(parts, func(a, b *part) int {
		switch {
		case a.group == lead:
			return -1
		case b.group == lead:
			return 1
		}
		return int(a.group) - int(b.group)
	})
	return parts
}

// transact carries out a Transaction across Groups from Node n, which
// keeps everything it knows of it in memory: it asks every Group to
// prepare, decides, and tells every Group. If the Node stops, nobody else
// knows what was decided, or that anything was. This is the store Rung 8
// starts from.
func (c *Cluster) transact(n int, cmd fsm.Command, done func(Outcome, fsm.Response)) {
	nd := c.nodes[n]
	boot := nd.boots
	alive := func() bool { return nd.up && nd.boots == boot }
	id := shardfsm.TxnID{Session: cmd.Session, Seq: cmd.Seq}
	parts := split(nd.table, cmd)
	if c.txnGroups == nil {
		c.txnGroups = map[shardfsm.TxnID][]shard.GroupID{}
	}
	for _, p := range parts {
		if !slices.Contains(c.txnGroups[id], p.group) {
			c.txnGroups[id] = append(c.txnGroups[id], p.group)
		}
	}

	// tell sends every Group the decision, each until it answers, and
	// calls then when all have.
	type answer struct {
		vote shardfsm.Vote
		resp fsm.Response
	}
	answers := make([]*answer, len(parts))
	tell := func(commit bool, version uint64, then func()) {
		left := len(parts)
		for i, p := range parts {
			offer := uint64(0)
			if a := answers[i]; a != nil && a.vote == shardfsm.VoteYes {
				offer = a.resp.Version
			}
			payload := shardfsm.FinishStep(id, parts[0].group, i == 0, commit, version, offer).Encode()
			var send func()
			send = func() {
				if !alive() {
					return
				}
				c.ask(n, p.group, false, func(int64) []byte { return payload }, func(o Outcome, _ []byte) {
					if o != Answered {
						c.S.After(10+c.S.Rand().Int64N(20), send)
						return
					}
					if left--; left == 0 && alive() {
						then()
					}
				})
			}
			send()
		}
	}

	decided := false
	decide := func() {
		if decided || !alive() {
			return
		}
		if slices.Contains(answers, nil) {
			return // not every Group has answered
		}
		version, commit := uint64(0), true
		var failed []fsm.Item
		conflict := false
		for _, a := range answers {
			switch {
			case a.vote == shardfsm.VoteDone:
				// A retry of a request that was carried out. Whatever this
				// attempt Locked is let go.
				decided = true
				tell(false, 0, func() {})
				done(Answered, a.resp)
				return
			case a.vote == shardfsm.VoteYes:
				version = max(version, a.resp.Version)
			case a.resp.Status == fsm.StatusVersionMismatch:
				commit = false
				failed = append(failed, a.resp.Items...)
			default:
				commit, conflict = false, true
				if a.resp.Status == fsm.StatusWrongGroup {
					c.WrongGroup++
					c.refresh(nd)
				}
				if a.resp.Status == fsm.StatusSessionExpired {
					decided = true
					tell(false, 0, func() {})
					done(Answered, a.resp)
					return
				}
			}
		}
		decided = true
		switch {
		case commit:
			c.TxnCommitted++
			nd.finishing++
			tell(true, version, func() {
				nd.finishing--
				done(Answered, fsm.Response{Status: fsm.StatusOK, Version: version})
			})
		case conflict:
			c.TxnConflict++
			tell(false, 0, func() {})
			done(Refused, fsm.Response{Status: fsm.StatusTxnConflict})
		default:
			// The conditions are reported only once every Group has let go
			// of its Locks: until then they are still true.
			tell(false, 0, func() { done(Answered, fsm.Response{Status: fsm.StatusVersionMismatch, Items: failed}) })
		}
	}
	for i, p := range parts {
		build := func(storeTime int64) []byte {
			return shardfsm.PrepareStep(id, parts[0].group, i == 0, p.conds, p.writes, storeTime, cmd.Register).Encode()
		}
		c.ask(n, p.group, false, build, func(o Outcome, raw []byte) {
			vote, resp, err := shardfsm.DecodeVote(raw)
			if o != Answered || err != nil {
				// The Group may or may not have prepared. The Node gives up
				// on the Transaction and says so to every Group.
				vote, resp = shardfsm.VoteNo, fsm.Response{Status: fsm.StatusTxnConflict}
			}
			answers[i] = &answer{vote: vote, resp: resp}
			decide()
		})
	}
}

// begin puts a Transaction across Groups to its Coordinator Group, from
// Node n, and waits there for the decision (A§12.2). The Node keeps
// nothing that matters: if it stops, the Transaction goes on without it.
func (c *Cluster) begin(n int, cmd fsm.Command, age int64, answer func(Outcome, fsm.Response, int64)) {
	nd := c.nodes[n]
	boot := nd.boots
	if age == 0 || age > nd.table.StoreTime {
		// A Transaction is as old as the Store time its first attempt
		// found, and no older than a client can honestly say (A§12.5).
		age = max(nd.table.StoreTime, 1)
	}
	started, tries := c.S.Now(), 0
	done := func(o Outcome, resp fsm.Response) { answer(o, resp, age) }
	id := shardfsm.TxnID{Session: cmd.Session, Seq: cmd.Seq}
	split := split(nd.table, cmd)
	parts := make([]shardfsm.Part, len(split))
	for i, p := range split {
		parts[i] = shardfsm.Part{Group: p.group, Conds: p.conds, Writes: p.writes}
	}
	lead := parts[0].Group

	var settle func(o Outcome, raw []byte)
	wait := func() {
		c.S.After(3, func() {
			if !nd.up || nd.boots != boot {
				return
			}
			query := shardfsm.OutcomeQuery(id, 0)
			c.ask(n, lead, true, func(int64) []byte { return query }, func(o Outcome, raw []byte) {
				state, resp, _, err := shardfsm.DecodeOutcome(raw)
				if o != Answered || err != nil {
					// The Transaction is under way: whoever asks next must
					// take its outcome as unknown.
					done(Unknown, fsm.Response{})
					return
				}
				settle(o, shardfsm.EncodeState(state, resp))
			})
		})
	}
	build := func(storeTime int64) []byte {
		step := shardfsm.TxnBeginStep(id, parts, storeTime, cmd.Register)
		step.Age = age
		return step.Encode()
	}
	settle = func(o Outcome, raw []byte) {
		if o != Answered {
			done(o, fsm.Response{})
			return
		}
		state, resp, err := shardfsm.DecodeState(raw)
		switch {
		case err != nil || state == shardfsm.TxnNone:
			done(Unknown, fsm.Response{})
		case state == shardfsm.TxnBegun:
			wait()
		case state == shardfsm.TxnRefused && resp.Status == fsm.StatusSessionExpired:
			done(Answered, resp)
		case state == shardfsm.TxnRefused && resp.Status == fsm.StatusTxnConflict && nd.committer.Yield(txnEnv{c, nd}, shardfsm.Held{ID: id, Coord: lead, Age: age}, resp, started, false):
			// The Coordinator Group's own keys are Locked, and nothing is
			// held for this Transaction yet: try again shortly.
			c.S.After(committer.Backoff(txnRetry, tries), func() {
				if nd.up && nd.boots == boot {
					c.ask(n, lead, false, build, settle)
				}
			})
			tries++
		case state == shardfsm.TxnRefused:
			if resp.Status == fsm.StatusWrongGroup {
				c.WrongGroup++
				c.refresh(nd)
			}
			if resp.Status == fsm.StatusTxnConflict {
				c.TxnConflict++
			}
			done(Refused, resp)
		case resp.Status == fsm.StatusTxnConflict:
			c.TxnConflict++
			done(Answered, resp)
		default:
			if resp.Status == fsm.StatusOK {
				c.TxnCommitted++
			}
			done(Answered, resp)
		}
	}
	c.ask(n, lead, false, build, settle)
}

// txnEnv is what a Node's committer sees of the simulated store. What it
// asked before the Node stopped is not answered after.
type txnEnv struct {
	c  *Cluster
	nd *node
}

func (e txnEnv) Now() int64 { return e.c.S.Now() }

func (e txnEnv) WithMachine(g shard.GroupID, fn func(*shardfsm.Machine)) {
	fn(e.c.machine(Replica(e.nd.id, g)))
}

func (e txnEnv) Local(g shard.GroupID, payload []byte) {
	e.c.S.Propose(Replica(e.nd.id, g), payload, func(sim.Reply) {})
}

func (e txnEnv) Ask(g shard.GroupID, read bool, payload []byte, back func(bool, []byte)) {
	boot := e.nd.boots
	e.c.ask(e.nd.id, g, read, func(int64) []byte { return payload }, func(o Outcome, raw []byte) {
		if e.nd.up && e.nd.boots == boot {
			back(o == Answered, raw)
		}
	})
}

// newCommitter is a Node's committer, with the waits of the Simulation.
func (c *Cluster) newCommitter() *committer.Agent {
	return &committer.Agent{
		Conflicts: c.cfg.Transactions.Conflicts, NoAsking: c.cfg.Transactions.NoAsking,
		AskAgain: txnAskAgain, GiveUp: txnGiveUp, LockAskAfter: lockAskAfter, LockAskAgain: lockAskAgain,
		Retry: txnRetry, Wait: txnWait, WoundAgain: txnWoundAgain,
		OnWound: func() { c.TxnWounded++ }, OnAsked: func() { c.LocksAsked++ },
	}
}

// txnLoop has Node nd carry on the Transactions of every Group it leads.
func (c *Cluster) txnLoop(nd *node) {
	c.S.After(txnEvery, func() { c.txnLoop(nd) })
	if !c.NodeUp(nd.id) || !c.cfg.Transactions.On || c.cfg.Transactions.Memory {
		return
	}
	var leads []shard.GroupID
	for _, g := range nd.groups {
		if r := Replica(nd.id, g); g != shard.Meta && c.S.Up(r) && c.S.Status(r).Role == core.LeaderRole {
			leads = append(leads, g)
		}
	}
	nd.committer.Step(txnEnv{c, nd}, leads)
}

// MidTransaction lists the Nodes that are in the middle of a Transaction
// across Groups: carrying one out from memory, leading a Group that
// coordinates one, or leading a Group that has keys Locked for one.
func (c *Cluster) MidTransaction() []int {
	var out []int
	for _, id := range c.NodeIDs() {
		nd := c.nodes[id]
		if !nd.up {
			continue
		}
		mid := nd.finishing > 0
		for _, g := range nd.groups {
			r := Replica(id, g)
			if g == shard.Meta || !c.S.Up(r) || c.S.Status(r).Role != core.LeaderRole {
				continue
			}
			m := c.machine(r)
			mid = mid || m.Locked() > 0 || len(m.Coordinating()) > 0
		}
		if mid {
			out = append(out, id)
		}
	}
	return out
}
