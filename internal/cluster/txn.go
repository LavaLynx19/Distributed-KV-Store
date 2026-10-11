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
}

// part is one Group's share of a Transaction.
type part struct {
	group  shard.GroupID
	conds  []fsm.Cond
	writes []fsm.Write
}

const (
	txnEvery    = 2   // how often a Node looks at the Transactions its Groups coordinate
	txnAskAgain = 40  // how long it waits for an answer before sending a step again
	txnGiveUp   = 400 // how long a Leader waits for every Group's answer before it aborts
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
	tell := func(commit bool, version uint64, then func()) {
		left := len(parts)
		for i, p := range parts {
			payload := shardfsm.FinishStep(id, i == 0, commit, version).Encode()
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

	type answer struct {
		vote shardfsm.Vote
		resp fsm.Response
	}
	answers := make([]*answer, len(parts))
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
			return shardfsm.PrepareStep(id, i == 0, p.conds, p.writes, storeTime, cmd.Register).Encode()
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
func (c *Cluster) begin(n int, cmd fsm.Command, done func(Outcome, fsm.Response)) {
	nd := c.nodes[n]
	boot := nd.boots
	id := shardfsm.TxnID{Session: cmd.Session, Seq: cmd.Seq}
	split := split(nd.table, cmd)
	parts := make([]shardfsm.Part, len(split))
	if c.txnGroups == nil {
		c.txnGroups = map[shardfsm.TxnID][]shard.GroupID{}
	}
	for i, p := range split {
		parts[i] = shardfsm.Part{Group: p.group, Conds: p.conds, Writes: p.writes}
		if !slices.Contains(c.txnGroups[id], p.group) {
			c.txnGroups[id] = append(c.txnGroups[id], p.group)
		}
	}
	lead := parts[0].Group

	var settle func(o Outcome, raw []byte)
	wait := func() {
		c.S.After(3, func() {
			if !nd.up || nd.boots != boot {
				return
			}
			query := shardfsm.OutcomeQuery(id)
			c.ask(n, lead, true, func(int64) []byte { return query }, func(o Outcome, raw []byte) {
				if o != Answered {
					// The Transaction is under way: whoever asks next must
					// take its outcome as unknown.
					done(Unknown, fsm.Response{})
					return
				}
				settle(o, raw)
			})
		})
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
		case state == shardfsm.TxnRefused:
			if resp.Status == fsm.StatusWrongGroup {
				c.WrongGroup++
				c.refresh(nd)
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
	build := func(storeTime int64) []byte {
		return shardfsm.TxnBeginStep(id, parts, storeTime, cmd.Register).Encode()
	}
	c.ask(n, lead, false, build, settle)
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
}

// txnLoop has Node nd carry on the Transactions of every Group it leads.
func (c *Cluster) txnLoop(nd *node) {
	c.S.After(txnEvery, func() { c.txnLoop(nd) })
	if !c.NodeUp(nd.id) || !c.cfg.Transactions.On || c.cfg.Transactions.Memory {
		return
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
	}
	for key := range nd.coord {
		if !seen[key] {
			delete(nd.coord, key)
		}
	}
}

// coordinate takes the next step for one Transaction that Group g, led from
// Node nd, coordinates.
func (c *Cluster) coordinate(nd *node, g shard.GroupID, t shardfsm.Coordinated, key coordKey) {
	now := c.S.Now()
	st := nd.coord[key]
	if st == nil {
		st = &coordState{since: now, votes: map[shard.GroupID]fsm.Response{}, yes: map[shard.GroupID]bool{}, asked: map[shard.GroupID]int64{}, proposed: -txnAskAgain}
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
			finish := shardfsm.FinishStep(t.ID, false, t.Commit, t.Version).Encode()
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
		prepare := shardfsm.PrepareStep(t.ID, false, p.Conds, p.Writes, t.Stamp, false).Encode()
		c.ask(nd.id, p.Group, false, func(int64) []byte { return prepare }, func(o Outcome, raw []byte) {
			vote, resp, err := shardfsm.DecodeVote(raw)
			if o != Answered || err != nil || nd.boots != boot {
				return // asked again in a while
			}
			st.votes[p.Group], st.yes[p.Group] = resp, vote == shardfsm.VoteYes
		})
	}
	if now-st.proposed < txnAskAgain {
		return
	}
	commit := t.Own == shardfsm.VoteYes
	version := t.OwnResp.Version
	var failed []fsm.Item
	conflict := false
	for _, p := range t.Parts {
		resp, voted := st.votes[p.Group]
		switch {
		case !voted:
			if now-st.since < txnGiveUp {
				return // not every Group has answered, and there is time yet
			}
			commit, conflict = false, true
		case st.yes[p.Group]:
			version = max(version, resp.Version)
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
	propose(shardfsm.DecideStep(t.ID, commit, version, failed))
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
