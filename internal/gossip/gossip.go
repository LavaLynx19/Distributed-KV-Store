// Package gossip is how the Nodes of a store learn about each other and
// about the Slot table without asking anyone in particular (A§11.10). Each
// Node keeps what it has heard of every Node, the newest table it has
// heard, and tells a few others. News spreads from Node to Node.
//
// Gossip informs and never decides. Nothing here changes who is a Member of
// a Group or which Group owns a Slot: those change only through a Raft Log.
// A Node uses what it learns to route requests and to report.
//
// Like the consensus core, it is a pure step function. A tick or a message
// goes in and messages come out. It reads no clock and its randomness comes
// from a seeded source, so the same code runs in the Simulation and the real
// shell.
//
// There are two ways to notice that a Node has gone quiet, built to be
// measured against each other:
//
//   - Counters. Every Node bumps a counter each round and passes on every
//     Node's counter. A Node whose counter hasn't risen for some rounds is
//     suspected, and later taken for dead. Each Node judges for itself.
//   - SWIM. Each round a Node pings one other. With no answer it asks a few
//     others to ping for it. Still nothing: it tells everyone the Node is
//     suspected. The Node, hearing this, answers by raising its incarnation
//     number, which outranks the suspicion. Unanswered, a suspicion becomes
//     a verdict.
package gossip

import (
	"slices"

	"distributed-kv-store/internal/shard"
)

// Status is what a Node thinks of another.
type Status uint8

const (
	// Alive: nothing says otherwise.
	Alive Status = iota
	// Suspect: it has gone quiet, and may yet speak up.
	Suspect
	// Dead: it stayed quiet too long.
	Dead
	// Left: it said it was shutting down.
	Left
)

func (s Status) String() string {
	return [...]string{"alive", "suspect", "dead", "left"}[s]
}

// Member is what is passed around about one Node.
type Member struct {
	ID int
	// Peer and Client are the Node's addresses for other Nodes and for
	// clients.
	Peer, Client string
	// Incarnation settles which report of Status is newer: a higher one
	// wins, and only the Node itself raises it.
	Incarnation uint64
	Status      Status
	// Counter is the Node's heartbeat, for the counters detector.
	Counter uint64
}

// Detector is how a Node notices that another has gone quiet.
type Detector uint8

const (
	Counters Detector = iota
	SWIM
)

// Kind is what a Message is for.
type Kind uint8

const (
	// Sync carries what the sender knows. With WantReply the receiver
	// answers with a Sync of its own.
	Sync Kind = iota
	// Ping asks "are you there", and Ack answers it.
	Ping
	Ack
	// PingReq asks the receiver to ping Target on the sender's behalf.
	PingReq
)

// Message is sent from one Node's gossip to another's. Every Message carries
// everything its sender knows: the stores this is for have a dozen Nodes.
type Message struct {
	From, To  int
	Kind      Kind
	WantReply bool
	// Seq ties an Ack to its Ping. Target is who a PingReq is about, and
	// who an Ack relayed for someone else is about.
	Seq    uint64
	Target int
	// ToAddr is where to send it if To is a Node the network hasn't been
	// told about yet.
	ToAddr  string
	Members []Member
	Table   shard.Table
}

// Rand is the seeded source of randomness. *math/rand/v2.Rand satisfies it.
type Rand interface {
	Uint64() uint64
}

// Config sets up one Node's gossip.
type Config struct {
	ID           int
	Peer, Client string
	// Seeds are Nodes to start from. One is enough.
	Seeds []Member
	Rand  Rand
	// Detector chooses how quiet Nodes are noticed.
	Detector Detector
	// Fanout is how many Nodes are told each round (default 2).
	Fanout int
	// SuspectAfter and DeadAfter are how many rounds a Node may be quiet
	// before it is suspected, and then taken for dead.
	//
	// For the counters detector both count from the last rise of its
	// counter that this Node heard of (defaults 10 and 25). News of a rise
	// takes a few rounds to get round, so SuspectAfter must be well above
	// that or healthy Nodes are suspected.
	//
	// For SWIM, SuspectAfter is how long a ping may go unanswered, with the
	// last third of it for pings through others, and DeadAfter counts from
	// the suspicion (defaults 5 and 15).
	SuspectAfter, DeadAfter int
}

// entry is what this Node holds about one Node.
type entry struct {
	Member
	// heard is the round its counter last rose, or it was first heard of.
	heard int
	// since is the round its Status last changed.
	since int
}

// probe is a ping this Node is waiting on.
type probe struct {
	target  int
	seq     uint64
	started int
	asked   bool // others have been asked to ping it
	// replyTo and replySeq are set when this ping is on another's behalf.
	replyTo  int
	replySeq uint64
}

// Node is one Node's gossip.
type Node struct {
	cfg     Config
	round   int
	members map[int]*entry
	table   shard.Table
	left    bool

	seq    uint64
	probes []probe
	order  []int // SWIM: Nodes still to be pinged this lap
}

// New starts a Node that knows itself and its seeds.
func New(cfg Config) *Node {
	if cfg.Fanout == 0 {
		cfg.Fanout = 2
	}
	suspect, dead := 5, 15
	if cfg.Detector == Counters {
		suspect, dead = 10, 25
	}
	if cfg.SuspectAfter == 0 {
		cfg.SuspectAfter = suspect
	}
	if cfg.DeadAfter == 0 {
		cfg.DeadAfter = dead
	}
	n := &Node{cfg: cfg, members: map[int]*entry{}}
	n.members[cfg.ID] = &entry{Member: Member{ID: cfg.ID, Peer: cfg.Peer, Client: cfg.Client}}
	for _, s := range cfg.Seeds {
		if s.ID != cfg.ID {
			n.members[s.ID] = &entry{Member: Member{ID: s.ID, Peer: s.Peer, Client: s.Client}}
		}
	}
	return n
}

func (n *Node) self() *entry { return n.members[n.cfg.ID] }

// ids lists the Nodes known, ascending.
func (n *Node) ids() []int {
	ids := make([]int, 0, len(n.members))
	for id := range n.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// status is what this Node thinks of e right now.
func (n *Node) status(e *entry) Status {
	if e.ID == n.cfg.ID {
		if n.left {
			return Left
		}
		return Alive
	}
	if n.cfg.Detector == SWIM || e.Status == Left {
		return e.Status
	}
	switch quiet := n.round - e.heard; {
	case quiet >= n.cfg.DeadAfter:
		return Dead
	case quiet >= n.cfg.SuspectAfter:
		return Suspect
	}
	return Alive
}

// Members lists every Node this Node has heard of, with what it thinks of
// each, ascending by id.
func (n *Node) Members() []Member {
	out := make([]Member, 0, len(n.members))
	for _, id := range n.ids() {
		m := n.members[id].Member
		m.Status = n.status(n.members[id])
		out = append(out, m)
	}
	return out
}

// Alive reports whether this Node thinks Node id is there to be talked to.
// A Node it has never heard of is given the benefit of the doubt.
func (n *Node) Alive(id int) bool {
	e, known := n.members[id]
	return !known || n.status(e) == Alive
}

// Table is the newest Slot table this Node has heard.
func (n *Node) Table() shard.Table { return n.table.Clone() }

// SetTable gives the Node a table from somewhere other than gossip: its own
// Meta Group replica, if it hosts one. It is kept if it is newer.
func (n *Node) SetTable(t shard.Table) { n.takeTable(t) }

func (n *Node) takeTable(t shard.Table) {
	if len(t.Slots) == 0 {
		return
	}
	storeTime := max(n.table.StoreTime, t.StoreTime)
	if t.Version > n.table.Version || len(n.table.Slots) == 0 {
		n.table = t.Clone()
	}
	n.table.StoreTime = storeTime
}

// message starts a Message to Node to carrying everything this Node knows.
func (n *Node) message(to int, kind Kind) Message {
	m := Message{From: n.cfg.ID, To: to, Kind: kind, Members: n.Members(), Table: n.Table()}
	if e, known := n.members[to]; known {
		m.ToAddr = e.Peer
	}
	// In gossip a Node's own claims about itself are the only ones that
	// carry its counter and incarnation forward.
	return m
}

// others lists the Nodes other than this one whose Status passes keep,
// shuffled.
func (n *Node) others(keep func(Status) bool) []int {
	var ids []int
	for _, id := range n.ids() {
		if id != n.cfg.ID && keep(n.status(n.members[id])) {
			ids = append(ids, id)
		}
	}
	for i := len(ids) - 1; i > 0; i-- {
		j := int(n.cfg.Rand.Uint64() % uint64(i+1))
		ids[i], ids[j] = ids[j], ids[i]
	}
	return ids
}

func reachable(s Status) bool { return s == Alive || s == Suspect }

// Tick is one round. It returns the Messages to send.
func (n *Node) Tick() []Message {
	if n.left {
		return nil
	}
	n.round++
	n.self().Counter++
	var out []Message

	// Tell a few Nodes what this one knows. Until it knows of anyone but
	// its seeds it asks for an answer, to learn the rest at once.
	targets := n.others(reachable)
	if len(targets) > n.cfg.Fanout {
		targets = targets[:n.cfg.Fanout]
	}
	// Now and then try a Node thought dead or gone. If a Partition has
	// healed, or it has restarted, this is how the two sides find out.
	if gone := n.others(func(s Status) bool { return s == Dead || s == Left }); len(gone) > 0 && n.round%4 == 0 {
		targets = append(targets, gone[0])
	}
	for _, to := range targets {
		m := n.message(to, Sync)
		m.WantReply = n.round <= 3
		out = append(out, m)
	}
	if n.cfg.Detector == SWIM {
		out = append(out, n.probe()...)
	}
	return out
}

// Leave marks this Node as shutting down and returns the Messages that say
// so. After it, the Node sends nothing more.
func (n *Node) Leave() []Message {
	if n.left {
		return nil
	}
	n.self().Incarnation++
	n.left = true
	var out []Message
	for _, to := range n.others(reachable) {
		out = append(out, n.message(to, Sync))
	}
	return out
}

// Receive handles a Message from another Node and returns the Messages to
// send in answer.
func (n *Node) Receive(m Message) []Message {
	if n.left {
		return nil
	}
	n.takeTable(m.Table)
	for _, heard := range m.Members {
		n.merge(heard, heard.ID == m.From)
	}
	var out []Message
	switch m.Kind {
	case Sync:
		if m.WantReply {
			out = append(out, n.message(m.From, Sync))
		}
	case Ping:
		ack := n.message(m.From, Ack)
		ack.Seq = m.Seq
		out = append(out, ack)
	case Ack:
		out = append(out, n.acked(m)...)
	case PingReq:
		// Ping the target for the asker, and pass the answer back.
		n.seq++
		n.probes = append(n.probes, probe{target: m.Target, seq: n.seq, started: n.round, asked: true, replyTo: m.From, replySeq: m.Seq})
		ping := n.message(m.Target, Ping)
		ping.Seq = n.seq
		out = append(out, ping)
	}
	return out
}

// rank orders reports with the same incarnation: the worse news wins.
func rank(s Status) int { return int(s) }

// merge takes in one report about one Node. direct is set when the report
// comes from that Node itself.
func (n *Node) merge(heard Member, direct bool) {
	if heard.ID == n.cfg.ID {
		// What others say about this Node. It may have restarted and lost
		// count: catch up with the counter they hold. And if they think
		// badly of it, outrank them.
		me := n.self()
		me.Counter = max(me.Counter, heard.Counter)
		if heard.Status != Alive && heard.Incarnation >= me.Incarnation {
			me.Incarnation = heard.Incarnation + 1
		} else if heard.Incarnation > me.Incarnation {
			me.Incarnation = heard.Incarnation
		}
		return
	}
	e, known := n.members[heard.ID]
	if !known {
		e = &entry{Member: Member{ID: heard.ID}, heard: n.round, since: n.round}
		n.members[heard.ID] = e
	}
	if heard.Peer != "" {
		e.Peer, e.Client = heard.Peer, heard.Client
	}
	if heard.Counter > e.Counter {
		e.Counter, e.heard = heard.Counter, n.round
	}
	before := e.Status
	switch {
	case n.cfg.Detector == Counters && heard.Status != Left:
		// Each Node judges quiet for itself. Only leaving is passed on. A
		// Node that left and is heard from again, with a higher
		// incarnation, is back.
		if e.Status == Left && heard.Incarnation > e.Incarnation {
			e.Status, e.Incarnation = Alive, heard.Incarnation
		}
		e.Incarnation = max(e.Incarnation, heard.Incarnation)
	case heard.Incarnation > e.Incarnation:
		e.Incarnation, e.Status = heard.Incarnation, heard.Status
	case heard.Incarnation == e.Incarnation && rank(heard.Status) > rank(e.Status):
		e.Status = heard.Status
	}
	if !known || e.Status != before {
		e.since = n.round
	}
}

// probe is SWIM's part of a round: time out the pings that weren't
// answered, and send the next one.
func (n *Node) probe() []Message {
	var out []Message
	waiting := n.probes[:0]
	for _, p := range n.probes {
		quiet := n.round - p.started
		switch {
		case quiet >= n.cfg.SuspectAfter:
			// Nobody got an answer. Unless this was for someone else, say
			// that the Node is suspected.
			if e := n.members[p.target]; p.replyTo == 0 && e != nil && e.Status == Alive {
				e.Status, e.since = Suspect, n.round
			}
			continue
		case !p.asked && quiet >= n.cfg.SuspectAfter-n.cfg.SuspectAfter/3:
			// No answer directly. Ask others to try: the trouble may be
			// between this Node and that one only.
			p.asked = true
			helpers := n.others(func(s Status) bool { return s == Alive })
			helpers = slices.DeleteFunc(helpers, func(id int) bool { return id == p.target })
			if len(helpers) > n.cfg.Fanout {
				helpers = helpers[:n.cfg.Fanout]
			}
			for _, h := range helpers {
				req := n.message(h, PingReq)
				req.Seq, req.Target = p.seq, p.target
				out = append(out, req)
			}
		}
		waiting = append(waiting, p)
	}
	n.probes = waiting

	// A suspicion nobody answered becomes a verdict.
	for _, id := range n.ids() {
		if e := n.members[id]; id != n.cfg.ID && e.Status == Suspect && n.round-e.since >= n.cfg.DeadAfter {
			e.Status, e.since = Dead, n.round
		}
	}

	// Ping the next Node. Each lap goes through all of them once, in a new
	// order, so none waits long for its turn.
	if len(n.order) == 0 {
		n.order = n.others(reachable)
	}
	for len(n.order) > 0 {
		target := n.order[0]
		n.order = n.order[1:]
		if e := n.members[target]; e == nil || !reachable(e.Status) {
			continue
		}
		n.seq++
		n.probes = append(n.probes, probe{target: target, seq: n.seq, started: n.round})
		ping := n.message(target, Ping)
		ping.Seq = n.seq
		out = append(out, ping)
		break
	}
	return out
}

// acked handles an answer to a ping: this Node's own, one it sent for
// someone else, or one relayed to it.
func (n *Node) acked(m Message) []Message {
	var out []Message
	about := m.From
	if m.Target != 0 {
		about = m.Target // relayed: someone else reached it for us
	}
	for i, p := range n.probes {
		if p.seq != m.Seq || p.target != about {
			continue
		}
		n.probes = slices.Delete(n.probes, i, i+1)
		if p.replyTo != 0 {
			relay := n.message(p.replyTo, Ack)
			relay.Seq, relay.Target = p.replySeq, p.target
			out = append(out, relay)
		}
		break
	}
	return out
}
