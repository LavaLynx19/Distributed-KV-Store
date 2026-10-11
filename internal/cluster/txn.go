package cluster

import (
	"slices"

	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
)

// Transactions is how a Transaction whose keys are in more than one Group
// is carried out (A§12).
type Transactions struct {
	// On carries them out. Without it they are refused, as in Rung 7.
	On bool
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

// Finishing is a Node that has decided to commit a Transaction across
// Groups and hasn't yet heard every Group finish it, if there is one.
func (c *Cluster) Finishing() (n int, ok bool) {
	for _, id := range c.NodeIDs() {
		if nd := c.nodes[id]; nd.up && nd.finishing > 0 {
			return id, true
		}
	}
	return 0, false
}
