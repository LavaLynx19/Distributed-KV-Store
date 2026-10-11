package cluster

import (
	"slices"

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
	started := c.S.Now()
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
		case state == shardfsm.TxnRefused && resp.Status == fsm.StatusTxnConflict && c.yield(nd, shardfsm.Held{ID: id, Coord: lead, Age: age}, resp, started):
			// The Coordinator Group's own keys are Locked, and nothing is
			// held for this Transaction yet: try again shortly.
			c.S.After(txnRetry, func() {
				if nd.up && nd.boots == boot {
					c.ask(n, lead, false, build, settle)
				}
			})
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

// yield decides what a Transaction does on finding a key Locked by
// another, and reports whether it should try for the key again. With
// OlderWins, the older of the two has the younger aborted and tries again;
// the younger tries again for a while and then gives way. since is when
// the Transaction first found the key Locked.
func (c *Cluster) yield(nd *node, me shardfsm.Held, resp fsm.Response, since int64) bool {
	holder, named := shardfsm.DecodeHeld(resp.Value)
	switch c.cfg.Transactions.Conflicts {
	case WaitInTurn:
		return true
	case OlderWins:
		if !named {
			return false
		}
		if !me.Older(holder) {
			return c.S.Now()-since < txnWait
		}
		// The younger one is aborted by its own Coordinator Group, if it
		// isn't decided yet. Either way its Locks go.
		// It is asked once, and again only after a good while: each asking
		// is an Entry in that Group's Log, and a crowd of Transactions
		// asking every time they look would leave it room for little else.
		if at, asked := nd.wounded[holder.ID]; !asked || c.S.Now()-at >= txnWoundAgain {
			if nd.wounded == nil {
				nd.wounded = map[shardfsm.TxnID]int64{}
			}
			for id, at := range nd.wounded {
				if c.S.Now()-at >= txnWoundAgain {
					delete(nd.wounded, id)
				}
			}
			nd.wounded[holder.ID] = c.S.Now()
			c.TxnWounded++
			abort := shardfsm.DecideStep(holder.ID, false, 0, nil, nil).Encode()
			c.ask(nd.id, holder.Coord, false, func(int64) []byte { return abort }, func(Outcome, []byte) {})
		}
		return c.S.Now()-since < txnGiveUp
	}
	return false
}

// coordKey names a Transaction a Node is carrying on for a Group it leads.
type coordKey struct {
	group shard.GroupID
	id    shardfsm.TxnID
}

// coordState is what a Leader remembers of a Transaction it is carrying on.
// None of it matters: another Leader starts with none and finds out again.
type coordState struct {
	since int64
	// votes are the other Groups' answers to their prepares, and asked
	// when each was last sent a step.
	votes map[shard.GroupID]fsm.Response
	yes   map[shard.GroupID]bool
	asked map[shard.GroupID]int64
	// proposed is when this Leader last proposed a decision.
	proposed int64
	// blocked is when each Group first answered that a key was Locked by
	// another Transaction, while this one is still trying for it.
	blocked map[shard.GroupID]int64
}

// txnLoop has Node nd carry on the Transactions of every Group it leads.
func (c *Cluster) txnLoop(nd *node) {
	c.S.After(txnEvery, func() { c.txnLoop(nd) })
	if !c.NodeUp(nd.id) || !c.cfg.Transactions.On || c.cfg.Transactions.Memory {
		return
	}
	if nd.coord == nil {
		nd.coord = map[coordKey]*coordState{}
	}
	seen := map[coordKey]bool{}
	for _, g := range nd.groups {
		r := Replica(nd.id, g)
		if g == shard.Meta || !c.S.Up(r) || c.S.Status(r).Role != core.LeaderRole {
			continue
		}
		for _, t := range c.machine(r).Coordinating() {
			key := coordKey{g, t.ID}
			seen[key] = true
			c.coordinate(nd, g, t, key)
		}
		for _, h := range c.machine(r).Holding() {
			if c.cfg.Transactions.NoAsking {
				break
			}
			key := coordKey{g, h.ID}
			seen[key] = true
			c.askOutcome(nd, g, h, key)
		}
	}
	for key := range nd.coord {
		if !seen[key] {
			delete(nd.coord, key)
		}
	}
	nd.nudged = false
}

// askOutcome has Group g, led from Node nd, ask the Coordinator Group of a
// Transaction it has Locked keys for what was decided, once the Locks are
// old enough or a request is waiting for them, and finish the Transaction
// here if there is an answer (A§12.3).
func (c *Cluster) askOutcome(nd *node, g shard.GroupID, h shardfsm.Held, key coordKey) {
	now := c.S.Now()
	st := nd.coord[key]
	if st == nil {
		st = &coordState{since: now, proposed: -txnAskAgain}
		nd.coord[key] = st
	}
	switch {
	case nd.nudged && now-st.proposed >= lockAskAgain:
	case now-st.since >= lockAskAfter && now-st.proposed >= txnAskAgain:
	default:
		return
	}
	st.proposed = now
	boot := nd.boots
	query := shardfsm.OutcomeQuery(h.ID, g)
	c.ask(nd.id, h.Coord, true, func(int64) []byte { return query }, func(o Outcome, raw []byte) {
		state, resp, offer, err := shardfsm.DecodeOutcome(raw)
		if o != Answered || err != nil || !nd.up || nd.boots != boot {
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
		c.LocksAsked++
		c.S.Propose(Replica(nd.id, g), finish.Encode(), func(sim.Reply) {})
	})
}

// coordinate takes the next step for one Transaction that Group g, led from
// Node nd, coordinates.
func (c *Cluster) coordinate(nd *node, g shard.GroupID, t shardfsm.Coordinated, key coordKey) {
	now := c.S.Now()
	st := nd.coord[key]
	if st == nil {
		st = &coordState{since: now, votes: map[shard.GroupID]fsm.Response{}, yes: map[shard.GroupID]bool{}, asked: map[shard.GroupID]int64{}, blocked: map[shard.GroupID]int64{}, proposed: -txnAskAgain}
		nd.coord[key] = st
	}
	boot := nd.boots
	due := func(p shardfsm.Part) bool {
		at, asked := st.asked[p.Group]
		return !asked || now-at >= txnAskAgain
	}
	propose := func(step shardfsm.CoordStep) {
		c.S.Propose(Replica(nd.id, g), step.Encode(), func(sim.Reply) {})
	}

	if t.Decided {
		for _, p := range t.Parts {
			if slices.Contains(t.Confirmed, p.Group) || !due(p) {
				continue
			}
			st.asked[p.Group] = now
			finish := shardfsm.FinishStep(t.ID, g, false, t.Commit, t.Version, t.Offer(p.Group)).Encode()
			c.ask(nd.id, p.Group, false, func(int64) []byte { return finish }, func(o Outcome, _ []byte) {
				if o == Answered && nd.up && nd.boots == boot {
					propose(shardfsm.ConfirmStep(t.ID, p.Group))
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
		c.ask(nd.id, p.Group, false, func(int64) []byte { return prepare }, func(o Outcome, raw []byte) {
			vote, resp, err := shardfsm.DecodeVote(raw)
			if o != Answered || err != nil || nd.boots != boot {
				return // asked again in a while
			}
			if vote == shardfsm.VoteNo && resp.Status == fsm.StatusTxnConflict {
				if _, was := st.blocked[p.Group]; !was {
					st.blocked[p.Group] = c.S.Now()
				}
				if c.yield(nd, shardfsm.Held{ID: t.ID, Coord: g, Age: t.Age}, resp, st.blocked[p.Group]) {
					// Nothing is held there: try for the key again shortly.
					st.asked[p.Group] = c.S.Now() - txnAskAgain + txnRetry
					return
				}
			}
			st.votes[p.Group], st.yes[p.Group] = resp, vote == shardfsm.VoteYes
		})
	}
	if now-st.proposed < txnAskAgain {
		return
	}
	giveUp := int64(txnGiveUp)
	if c.cfg.Transactions.Conflicts == WaitInTurn {
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
		failed = nil // the conditions weren't all looked at
	}
	st.proposed = now
	// A decision to abort over a conflict must not be reported as failed
	// conditions, whatever this Group's own were.
	propose(shardfsm.DecideStep(t.ID, commit, version, offers, failed))
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
