// Package cluster simulates a store with several Groups (A§11): Nodes that
// each host a replica of some of the Groups, a Meta Group holding the Slot
// table, and on every Node the two pieces of shell that tie them together.
//
//   - Routing (A§11.3): a Node asked about a key works out the Group from
//     the table it has cached, and passes the request to a Node it believes
//     hosts that Group's Leader. It forwards once and never retries.
//   - The agent: on each Node, for each Group whose Leader is there, it
//     reads that Group's own state and the cached table and proposes the
//     next step of any Move under way (A§11.4). It holds nothing that
//     matters: a new Leader's agent picks up from what the Logs say.
//
// It is built on sim.Sim. Every replica is one simulated Node there, with an
// id that gives its machine and its Group, so the scheduler, the network,
// the clocks and the disks are the ones every earlier Rung was tested on.
package cluster

import (
	"fmt"
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/gossip"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/mover"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
	"distributed-kv-store/internal/sim"
)

// NewCore builds one replica's consensus core. members are the replicas of
// its Group. stored is what its disk held, or the zero value at first start.
type NewCore func(replica core.NodeID, members []core.NodeID, rng core.Rand, stored core.Stored) core.Node

// Config describes a simulated store.
type Config struct {
	Seed uint64
	// Nodes is the number of machines, numbered from 1. Groups is the
	// number of data Groups, numbered from 1; the Meta Group is extra.
	// Each Group has Replicas Members, on consecutive Nodes, so Groups
	// overlap and a Node's failure hits some and not others (A§11.9).
	Nodes, Groups, Replicas int
	// Slots is how many Slots the store has.
	Slots   int
	NewCore NewCore
	// Copy stands in for the network's encoding (sim.Config.Copy).
	Copy func(core.Message) core.Message

	DiskDelay     [2]int64
	SnapshotEvery int
	TearWrites    bool
	SessionTTL    int64

	// Gossip chooses how Nodes learn the table and of each other: by asking
	// the Meta Group on a timer, as in stage 7a, or by gossip with one of
	// its two detectors (A§11.10). Spares is how many extra Nodes run from
	// the start hosting no Group, each knowing only Node 1. GossipLoss is
	// the chance that any one gossip Message is lost.
	Gossip     GossipMode
	Spares     int
	GossipLoss float64
	// SuspectAfter and DeadAfter override the detector's defaults
	// (gossip.Config), in rounds.
	SuspectAfter, DeadAfter int

	// GossipDecides is the naive store of stage 7b, in two parts. With
	// Ownership, a Move is not put to the Meta Group: the Node asked changes
	// its own table and gossips it, and each Group does what the table it
	// has heard says. With Members, each replica takes for its Group's
	// Members whichever of them its Node's gossip thinks are alive.
	GossipDecides struct{ Ownership, Members bool }

	// Replacing is how Nodes that die are replaced (replace.go).
	Replacing Replacing
	// Balancing is how load is measured and evened out (balance.go).
	Balancing Balancing

	// Unchecked makes data Groups answer for keys in Slots they don't own
	// (shardfsm.Config.Unchecked), and FlipAtOnce makes a Move change the
	// table at once with nobody confirming, after which the Groups follow
	// the table (meta.NewFlipping). They are the stores Rung 7 starts from.
	Unchecked  bool
	FlipAtOnce bool
}

const (
	agentEvery = 20  // how often a Node's agent looks for work
	tickEvery  = 100 // how often the Meta Leader moves Store time on
	chunkKeys  = 4   // keys sent per step while copying a Slot
	rpcMin     = 1   // a request between Nodes takes this long, one way,
	rpcMax     = 8   // to this long
	patience   = 200 // how long the agent waits for an answer before asking again
)

// GossipMode is how a store's Nodes keep each other informed.
type GossipMode uint8

const (
	// NoGossip: every Node asks the Meta Group for the table on a timer.
	NoGossip GossipMode = iota
	// GossipCounters and GossipSWIM: Nodes gossip, noticing quiet Nodes by
	// heartbeat counters or by SWIM's pings.
	GossipCounters
	GossipSWIM
)

// Replica is the id of Node n's replica of Group g.
func Replica(n int, g shard.GroupID) core.NodeID { return core.NodeID(shard.ReplicaID(n, g)) }

// NodeOf and GroupOf take a replica's id apart.
func NodeOf(r core.NodeID) int {
	n, _ := shard.SplitReplicaID(uint64(r))
	return n
}

func GroupOf(r core.NodeID) shard.GroupID {
	_, g := shard.SplitReplicaID(uint64(r))
	return g
}

// node is one machine's shell state. None of it is durable, and none of it
// is trusted by any Group.
type node struct {
	id     int
	groups []shard.GroupID
	// table is the latest Slot table this Node has heard.
	table shard.Table
	// leader is where this Node last heard each Group's Leader was.
	leader map[shard.GroupID]core.NodeID
	// mover carries out the Moves of the Groups this Node leads.
	mover    *mover.Agent
	lastTick int64
	// up is whether the machine is running, and side which side of a
	// Partition it is on. A Spare has no replica to ask. gone is set when
	// the machine is lost for good.
	up, gone bool
	side     int
	// gossip is this Node's gossip, if the store uses it.
	gossip *gossip.Node
	// deadSince is when this Node's gossip gave each Node up for dead, and
	// report what it tells the others (replace.go). asking is when it last
	// asked a Group whether a replica it holds is still wanted.
	deadSince map[int]int64
	report    shard.Report
	asking    map[shard.GroupID]int64
	// counts is the load each Slot has put on this Node since loadAt, and
	// smooth its smoothed load (balance.go).
	counts []uint32
	smooth []float64
	loadAt int64
	// What the Meta Leader on this Node remembers between looks at the
	// load: the Term it began in, when, and whether it is between the two
	// lines on the way down.
	balanceTerm  core.Term
	balanceSince int64
	balancing    bool
}

// Cluster is one simulated store.
type Cluster struct {
	S     *sim.Sim
	cfg   Config
	nodes map[int]*node
	// Signals count what the routing layer did.
	Forwarded, WrongGroup, Moving int
	// MovesAsked counts the Moves requested, and MovesTaken those the Meta
	// Group recorded the intent of.
	MovesAsked, MovesTaken int
	// GossipSent counts gossip Messages sent.
	GossipSent int
	// TableLags is, for each time a Node learned of a new table version,
	// how long that was after the Meta Group first held it.
	TableLags []int64
	versionAt map[uint64]int64
	// Pauses is how long each Slot that was handed over stayed frozen
	// (mover.Agent.OnHandover).
	Pauses []int64

	// owned is, per data Group, which Slots it serves as of the latest
	// Entry any of its replicas has applied, and twoOwners the first moment
	// two Groups served the same Slot by that measure (verdict.go).
	owned     map[shard.GroupID]ownership
	twoOwners string

	// Replacements are the replacements the Meta Group took, and Drops the
	// times a Node dropped a replica's data.
	Replacements []Replaced
	Drops        []Dropped
	wanted       map[int][2]int // per Group, the change the newest table wants
	// joiner marks the replicas that didn't found their Group. It stands
	// for a mark on the replica's disk: it outlives a crash.
	joiner map[core.NodeID]bool

	// MovedAt is when each Move that finished did so.
	MovedAt []int64
	moved   int

	// sets is the Partition in force, by machine, or nil.
	sets [][]int
}

// Founders are the replicas Group g starts with (A§11.9).
func (c *Cluster) Founders(g shard.GroupID) []core.NodeID {
	return replicasOn(g, shard.Hosts(g, c.cfg.Nodes, c.cfg.Replicas))
}

func replicasOn(g shard.GroupID, nodes []int) []core.NodeID {
	var rs []core.NodeID
	for _, n := range nodes {
		rs = append(rs, Replica(n, g))
	}
	return rs
}

// Replicas lists every replica of Group g there is, on whichever Nodes,
// whether or not the Group counts it as a Member.
func (c *Cluster) Replicas(g shard.GroupID) []core.NodeID {
	var rs []core.NodeID
	for _, n := range c.NodeIDs() {
		if c.hosts(n, g) {
			rs = append(rs, Replica(n, g))
		}
	}
	return rs
}

// Members are the replicas Group g's Leader has as its Members, or nil if
// it has no Leader.
func (c *Cluster) Members(g shard.GroupID) []core.NodeID {
	if l := c.Leader(g); l != 0 {
		return c.S.Status(l).Members
	}
	return nil
}

// startMembers is the Member list a replica's core is started with: the one
// in the table its Node holds. What the replica has in its own Log, once it
// has anything, overrides it.
//
// A replica that was started to join a Group, or whose data was dropped, is
// never in the list it starts with, whatever the table says. Its Node may
// hold an old table that names it, and a replica with an empty Log that
// took itself for a Member would stand for election and refuse the Leader
// that is trying to bring it up to date.
func (c *Cluster) startMembers(id core.NodeID) []core.NodeID {
	g := GroupOf(id)
	members := replicasOn(g, c.nodes[NodeOf(id)].table.Hosts(g))
	if c.joiner[id] {
		members = slices.DeleteFunc(members, func(m core.NodeID) bool { return m == id })
	}
	return members
}

// Groups lists every Group, the Meta Group first.
func (c *Cluster) Groups() []shard.GroupID {
	gs := []shard.GroupID{shard.Meta}
	for g := 1; g <= c.cfg.Groups; g++ {
		gs = append(gs, shard.GroupID(g))
	}
	return gs
}

// NodeIDs lists the machines.
func (c *Cluster) NodeIDs() []int {
	ids := make([]int, c.cfg.Nodes+c.cfg.Spares)
	for i := range ids {
		ids[i] = i + 1
	}
	return ids
}

// New builds the store and starts every Node's agent.
func New(cfg Config) *Cluster {
	c := &Cluster{cfg: cfg, nodes: map[int]*node{}, owned: map[shard.GroupID]ownership{}, versionAt: map[uint64]int64{}, wanted: map[int][2]int{}, joiner: map[core.NodeID]bool{}}
	start := meta.NewPlaced(cfg.Slots, cfg.Groups, cfg.Nodes, cfg.Replicas).Table()
	var ids []core.NodeID
	for n := 1; n <= cfg.Nodes+cfg.Spares; n++ {
		c.nodes[n] = &node{id: n, up: true, table: start.Clone(), leader: map[shard.GroupID]core.NodeID{},
			mover: &mover.Agent{Patience: patience, ChunkKeys: chunkKeys,
				FlipAtOnce:            cfg.FlipAtOnce || cfg.GossipDecides.Ownership,
				TakeWhatTheTableGives: cfg.GossipDecides.Ownership,
				OnHandover:            func(_ shard.GroupID, _ shard.Slot, frozenFor int64) { c.Pauses = append(c.Pauses, frozenFor) }}}
	}
	for _, g := range c.Groups() {
		for _, r := range c.Founders(g) {
			ids = append(ids, r)
			c.nodes[NodeOf(r)].groups = append(c.nodes[NodeOf(r)].groups, g)
		}
	}
	slices.Sort(ids)
	c.S = sim.New(sim.Config{
		Seed: cfg.Seed,
		IDs:  ids,
		NewNode: func(id core.NodeID, _ []core.NodeID, rng core.Rand) core.Node {
			return cfg.NewCore(id, c.startMembers(id), rng, core.Stored{})
		},
		Restart: func(id core.NodeID, _ []core.NodeID, rng core.Rand, stored core.Stored) core.Node {
			return cfg.NewCore(id, c.startMembers(id), rng, stored)
		},
		NewMachineFor: c.newMachine,
		OnApply:       c.applied,
		Copy:          cfg.Copy,
		DiskDelay:     cfg.DiskDelay,
		SnapshotEvery: cfg.SnapshotEvery,
		TearWrites:    cfg.TearWrites,
	})
	for _, n := range c.NodeIDs() {
		c.S.After(1+c.S.Rand().Int64N(agentEvery), func() { c.agent(c.nodes[n]) })
	}
	c.startGossip()
	return c
}

func (c *Cluster) newMachine(id core.NodeID) sim.Machine {
	g := GroupOf(id)
	if g == shard.Meta {
		m := meta.NewPlaced(c.cfg.Slots, c.cfg.Groups, c.cfg.Nodes, c.cfg.Replicas)
		if c.cfg.FlipAtOnce {
			return m.Flipping()
		}
		return m
	}
	var owned []shard.Slot
	for s, o := range meta.New(c.cfg.Slots, c.cfg.Groups).Table().Slots {
		if o.Group == g {
			owned = append(owned, shard.Slot(s))
		}
	}
	return shardfsm.New(shardfsm.Config{Group: g, Slots: c.cfg.Slots, Owned: owned, SessionTTL: c.cfg.SessionTTL, Unchecked: c.cfg.Unchecked})
}

// Table is the Slot table Node n has cached.
func (c *Cluster) Table(n int) shard.Table { return c.nodes[n].table.Clone() }

// hosts reports whether Node n has a replica of Group g.
func (c *Cluster) hosts(n int, g shard.GroupID) bool { return slices.Contains(c.nodes[n].groups, g) }

// NodeUp reports whether Node n is running. Its replicas are crashed and
// restarted together.
func (c *Cluster) NodeUp(n int) bool { return c.nodes[n].up }

// replicasOf lists Node n's replicas.
func (c *Cluster) replicasOf(n int) []core.NodeID {
	var rs []core.NodeID
	for _, g := range c.nodes[n].groups {
		rs = append(rs, Replica(n, g))
	}
	return rs
}

// CrashNode stops every replica on Node n, and RestartNode starts them
// again from their disks. The Node's shell state is lost with it.
func (c *Cluster) CrashNode(n int) {
	for _, r := range c.replicasOf(n) {
		c.S.Crash(r)
	}
	c.nodes[n].up = false
	c.nodes[n].mover.Reset()
}

func (c *Cluster) RestartNode(n int) {
	if c.nodes[n].gone {
		return
	}
	for _, r := range c.replicasOf(n) {
		c.S.Restart(r)
	}
	if nd := c.nodes[n]; !nd.up {
		// It comes back knowing only what a Node is started with: the table
		// the store was founded with, and nothing of where the Leaders are.
		nd.up = true
		nd.table = meta.NewPlaced(c.cfg.Slots, c.cfg.Groups, c.cfg.Nodes, c.cfg.Replicas).Table()
		clear(nd.leader)
		c.newGossip(nd)
	}
}

// DestroyNode stops Node n for good, as if the machine had been lost.
func (c *Cluster) DestroyNode(n int) {
	c.CrashNode(n)
	c.nodes[n].gone = true
}

// host starts a replica of Group g on Node nd, which had none: empty, and a
// Member of nothing until the Group's Leader adds it (A§11.11).
func (c *Cluster) host(nd *node, g shard.GroupID) {
	nd.groups = append(nd.groups, g)
	slices.Sort(nd.groups)
	c.joiner[Replica(nd.id, g)] = true
	c.S.Add(Replica(nd.id, g))
	if c.sets != nil {
		c.PartitionNodes(c.sets...) // the new replica is on its Node's side
	}
}

// Heal ends every Partition.
func (c *Cluster) Heal() {
	c.sets = nil
	c.S.Heal()
	for _, nd := range c.nodes {
		nd.side = 0
	}
}

// PartitionNodes cuts the network between the given sets of machines.
func (c *Cluster) PartitionNodes(sets ...[]int) {
	c.sets = sets
	replicas := make([][]core.NodeID, len(sets))
	for _, nd := range c.nodes {
		nd.side = 0
	}
	for i, set := range sets {
		for _, n := range set {
			replicas[i] = append(replicas[i], c.replicasOf(n)...)
			c.nodes[n].side = i + 1
		}
	}
	c.S.Partition(replicas...)
}

// reachable reports whether a request from Node a can reach Node b.
func (c *Cluster) reachable(a, b int) bool {
	sa, sb := c.nodes[a].side, c.nodes[b].side
	// A Node that no Partition names is on every side.
	return a == b || sa == 0 || sb == 0 || sa == sb
}

// Outcome is how a request to a Group ended, as the asking Node saw it.
type Outcome uint8

const (
	// Answered: the Group's state machine gave a response.
	Answered Outcome = iota
	// Refused: the request had no effect. It never arrived, or the Member
	// it reached wasn't the Leader.
	Refused
	// Unknown: the request may or may not have taken effect.
	Unknown
)

// ask sends a request from Node n to Group g. If n was only guessing where
// the Leader is, and the Node it tried says it is elsewhere and that nothing
// happened, n tries there, once (A§11.3).
func (c *Cluster) ask(n int, g shard.GroupID, read bool, payload func(storeTime int64) []byte, back func(Outcome, []byte)) {
	nd := c.nodes[n]
	_, known := nd.leader[g]
	c.askOnce(n, g, read, payload, func(o Outcome, raw []byte) {
		if _, told := nd.leader[g]; o == Refused && !known && told && c.NodeUp(n) {
			c.askOnce(n, g, read, payload, back)
			return
		}
		back(o, raw)
	})
}

// askOnce sends a request from Node n to Group g: to the replica n believes
// leads it, on whichever machine that is. With read set it is answered
// from the Leader's state without an Entry (A§6.2). payload is built on the
// machine that handles it, which is given that machine's Store time. back is
// called at most once: a reply lost on the way never arrives.
func (c *Cluster) askOnce(n int, g shard.GroupID, read bool, payload func(storeTime int64) []byte, back func(Outcome, []byte)) {
	nd := c.nodes[n]
	target, known := nd.leader[g]
	if !known {
		members := replicasOn(g, nd.table.Hosts(g))
		if nd.gossip != nil {
			// With gossip, a Node thought dead isn't worth a guess, unless
			// they all are.
			alive := slices.DeleteFunc(slices.Clone(members), func(m core.NodeID) bool { return !nd.gossip.Alive(NodeOf(m)) })
			if len(alive) > 0 {
				members = alive
			}
		}
		target = members[c.S.Rand().IntN(len(members))]
		// A Node that hosts the Group asks its own replica first.
		if c.hosts(n, g) {
			target = Replica(n, g)
		}
	}
	to := NodeOf(target)
	handle := func(reply func(sim.Reply)) {
		p := payload(c.nodes[to].table.StoreTime)
		if read {
			c.S.Read(target, p, reply)
		} else {
			c.S.Propose(target, p, reply)
		}
	}
	settle := func(r sim.Reply) {
		switch {
		case r.Refused:
			delete(nd.leader, g)
			back(Refused, nil)
		case r.Reason == core.OK:
			nd.leader[g] = target
			back(Answered, r.Response)
		case r.Reason == core.NotLeader || r.Reason == core.NoMajority:
			delete(nd.leader, g)
			if r.Leader != 0 {
				nd.leader[g] = r.Leader
			}
			back(Refused, nil)
		default:
			delete(nd.leader, g)
			back(Unknown, nil)
		}
	}
	if to == n {
		handle(settle)
		return
	}
	c.Forwarded++
	if !c.reachable(n, to) {
		// The connection can't be made: definitely no effect.
		c.S.After(rpcMax, func() {
			delete(nd.leader, g)
			back(Refused, nil)
		})
		return
	}
	c.S.After(c.rpcDelay(), func() {
		handle(func(r sim.Reply) {
			c.S.After(c.rpcDelay(), func() {
				if c.reachable(to, n) && c.NodeUp(n) {
					settle(r)
				}
			})
		})
	})
}

func (c *Cluster) rpcDelay() int64 { return rpcMin + c.S.Rand().Int64N(rpcMax-rpcMin+1) }

// Request is a client asking Node n to carry out cmd (A§11.3). done is
// called at most once, with what the client may conclude: an answer, a
// definite refusal that changed nothing, or nothing known. If the reply is
// lost, done is never called, and the client must time out.
func (c *Cluster) Request(n int, cmd fsm.Command, done func(Outcome, fsm.Response)) {
	if !c.NodeUp(n) {
		done(Refused, fsm.Response{})
		return
	}
	nd := c.nodes[n]
	if cmd.Op == fsm.OpOpenSession {
		open := func(int64) []byte { return meta.Command{Op: meta.OpOpenSession}.Encode() }
		c.ask(n, shard.Meta, false, open, func(o Outcome, raw []byte) {
			resp, err := meta.DecodeResponse(raw)
			if o != Answered || err != nil {
				done(o, fsm.Response{})
				return
			}
			done(Answered, fsm.Response{Status: fsm.StatusOK, Session: resp.Session})
		})
		return
	}
	g, same := c.groupOf(nd.table, cmd)
	if !same {
		// The keys are owned by more than one Group (A§11.8).
		done(Refused, fsm.Response{Status: fsm.StatusCrossGroup})
		return
	}
	read := cmd.Op == fsm.OpGet
	build := func(storeTime int64) []byte {
		if !read {
			cmd.Stamp = storeTime
		}
		return cmd.Encode()
	}
	c.ask(n, g, read, build, func(o Outcome, raw []byte) {
		if o != Answered {
			done(o, fsm.Response{})
			return
		}
		resp, err := fsm.DecodeResponse(raw)
		if err != nil {
			panic(fmt.Sprintf("cluster: undecodable response: %v", err))
		}
		switch resp.Status {
		case fsm.StatusWrongGroup:
			// This Node's table is out of date. The Group refused, so
			// nothing changed.
			c.WrongGroup++
			c.refresh(nd)
			done(Refused, resp)
		case fsm.StatusMoving:
			c.Moving++
			done(Refused, resp)
		default:
			if l, known := nd.leader[g]; known {
				c.count(c.nodes[NodeOf(l)], cmd)
			}
			done(Answered, resp)
		}
	})
}

// Part is one Group's answer to a scan.
type Part struct {
	Group shard.GroupID
	Resp  fsm.Response
}

// Scan is a client asking Node n for a range scan (A§11.8). The Node asks
// every data Group and merges what they say. Each Group's Part is
// Linearizable; the merged answer is not one moment, because the Groups are
// read one after another while Slots may be moving between them. done gets
// the merged answer and the Parts it was made from, or ok false if any
// Group couldn't be reached, in which case there is no answer.
func (c *Cluster) Scan(n int, cmd fsm.Command, done func(merged fsm.Response, parts []Part, ok bool)) {
	if !c.NodeUp(n) {
		done(fsm.Response{}, nil, false)
		return
	}
	groups := c.Groups()[1:]
	parts := make([]Part, 0, len(groups))
	failed := false
	payload := cmd.Encode()
	for _, g := range groups {
		c.ask(n, g, true, func(int64) []byte { return payload }, func(o Outcome, raw []byte) {
			if failed {
				return
			}
			resp, err := fsm.DecodeResponse(raw)
			if o != Answered || err != nil || resp.Status != fsm.StatusOK {
				failed = true
				done(fsm.Response{}, nil, false)
				return
			}
			parts = append(parts, Part{Group: g, Resp: resp})
			if len(parts) < len(groups) {
				return
			}
			merged := fsm.Response{Status: fsm.StatusOK}
			for _, p := range parts {
				merged.Items = append(merged.Items, p.Resp.Items...)
			}
			slices.SortFunc(merged.Items, func(a, b fsm.Item) int {
				switch {
				case a.Key < b.Key:
					return -1
				case a.Key > b.Key:
					return 1
				}
				return 0
			})
			if limit := cmd.Limit; limit > 0 && uint64(len(merged.Items)) > limit {
				merged.Items = merged.Items[:limit]
			}
			done(merged, parts, true)
		})
	}
}

// groupOf is the Group the table gives for a Command's keys, and whether it
// is the same for all of them.
func (c *Cluster) groupOf(t shard.Table, cmd fsm.Command) (shard.GroupID, bool) {
	if cmd.Op != fsm.OpTxn {
		return t.OwnerOf(cmd.Key), true
	}
	var g shard.GroupID
	for i, key := range txnKeys(cmd) {
		if owner := t.OwnerOf(key); i == 0 {
			g = owner
		} else if owner != g {
			return 0, false
		}
	}
	return g, true
}

func txnKeys(cmd fsm.Command) []string {
	var keys []string
	for _, cond := range cmd.Conds {
		keys = append(keys, cond.Key)
	}
	for _, w := range cmd.Writes {
		keys = append(keys, w.Key)
	}
	return keys
}

// Move asks the Meta Group, through Node n, to move slot to Group to.
func (c *Cluster) Move(n int, slot shard.Slot, to shard.GroupID, done func(ok bool)) {
	c.MovesAsked++
	if nd := c.nodes[n]; c.cfg.GossipDecides.Ownership {
		// The naive store: say so, and let the news spread.
		if nd.up && nd.table.Slots[slot].Group != to {
			t := nd.table.Clone()
			t.Slots[slot] = shard.Owner{Group: to, Epoch: t.Slots[slot].Epoch + 1}
			t.Version++
			nd.gossip.SetTable(t)
			c.gossipLearn(nd)
			c.MovesTaken++
		}
		done(nd.up)
		return
	}
	cmd := func(int64) []byte { return meta.Command{Op: meta.OpMove, Slot: slot, To: to}.Encode() }
	c.ask(n, shard.Meta, false, cmd, func(o Outcome, raw []byte) {
		resp, err := meta.DecodeResponse(raw)
		ok := o == Answered && err == nil && resp.Status == meta.StatusOK
		if ok {
			c.MovesTaken++
		}
		done(ok)
	})
}

// Replace asks the Meta Group, through Node n, to have Node in take Node
// out's place in every Group out is a Member of (A§11.11).
func (c *Cluster) Replace(n, out, in int, done func(ok bool)) {
	cmd := func(int64) []byte { return meta.Command{Op: meta.OpReplace, Out: out, In: in}.Encode() }
	c.ask(n, shard.Meta, false, cmd, func(o Outcome, raw []byte) {
		resp, err := meta.DecodeResponse(raw)
		done(o == Answered && err == nil && resp.Status == meta.StatusOK)
	})
}

// refresh asks the Meta Group for the table and keeps it if it is newer.
func (c *Cluster) refresh(nd *node) {
	c.ask(nd.id, shard.Meta, true, func(int64) []byte { return nil }, func(o Outcome, raw []byte) {
		if o != Answered {
			return
		}
		t, err := shard.DecodeTable(raw)
		if err != nil {
			panic(fmt.Sprintf("cluster: undecodable table: %v", err))
		}
		c.learn(nd, t)
	})
}

// learn makes t the table Node nd routes by, if it is newer, and notes how
// long the news took to arrive.
func (c *Cluster) learn(nd *node, t shard.Table) {
	if t.Version > nd.table.Version {
		if at, known := c.versionAt[t.Version]; known {
			c.TableLags = append(c.TableLags, c.S.Now()-at)
		}
	}
	if t.Version > nd.table.Version || t.StoreTime > nd.table.StoreTime {
		nd.table = t
	}
}
