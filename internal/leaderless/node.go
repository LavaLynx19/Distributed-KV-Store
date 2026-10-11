package leaderless

import (
	"cmp"
	"slices"
)

// Op is what a client asks for.
type Op uint8

const (
	// Get reads a key's set.
	Get Op = iota + 1
	// Add puts one element in a key's set.
	Add
	// Remove takes one element out.
	Remove
)

// Config sets up a Node.
type Config struct {
	ID NodeID
	// Nodes is every Node of the store, the same list on each.
	Nodes []NodeID
	// W is how many Nodes must have a write on disk before the client is
	// told it succeeded, and R how many Home Nodes must answer a read
	// (A§13.2).
	W, R int
	Mode Mode
	// DeliverHints has a Node that holds a Hint keep trying to hand it to
	// the Home Node it is for. Resend has the Node that took a write go on
	// sending it to Home Nodes that haven't answered, after the client has
	// its answer. Without them a Hint stays where it is and a write is
	// sent once: the store stage 9a starts from (A§13.4).
	DeliverHints, Resend bool
	// Stored is what the Node had on disk when it last stopped.
	Stored Stored
	// Seed sets where the Node's numbering of its own requests starts, so
	// that an answer meant for a request made before a restart isn't taken
	// for one made after.
	Seed uint64
}

// The waits of a Node, in ticks.
const (
	hintAfter    = 8  // before a write is given to a stand-in for a Home Node that hasn't answered
	giveUp       = 24 // before a client is told too few Nodes answered
	resendEvery  = 6  // between sendings of a write to a Home Node that hasn't answered
	resendFor    = 90 // how long that goes on
	deliverEvery = 8  // between attempts to hand over a Hint
)

// HintKey names a Hint: the Home Node it is for, and the key.
type HintKey struct {
	For NodeID
	Key string
}

// Stored is a Node's durable state.
type Stored struct {
	// Data is what the Node holds of each key.
	Data map[string]State
	// Hints are writes kept for Home Nodes that couldn't be reached.
	Hints map[HintKey]State
	// Counts is, per key, how many additions this Node has taken.
	Counts map[string]uint64
}

// RecordKind says what a Record changes.
type RecordKind uint8

const (
	// PutData replaces what is held of Key with State.
	PutData RecordKind = iota + 1
	// PutHint replaces the Hint for (For, Key) with State.
	PutHint
	// DropHint removes the Hint for (For, Key).
	DropHint
	// PutCount sets the count of additions taken for Key.
	PutCount
)

// Record is one change to a Node's durable state.
type Record struct {
	Kind  RecordKind
	Key   string
	For   NodeID
	State State
	Count uint64
}

// Apply makes the changes recs describe. Both shells keep a Node's durable
// state through this one function.
func (s *Stored) Apply(recs []Record) {
	if s.Data == nil {
		s.Data, s.Hints, s.Counts = map[string]State{}, map[HintKey]State{}, map[string]uint64{}
	}
	for _, r := range recs {
		switch r.Kind {
		case PutData:
			s.Data[r.Key] = r.State
		case PutHint:
			s.Hints[HintKey{r.For, r.Key}] = r.State
		case DropHint:
			delete(s.Hints, HintKey{r.For, r.Key})
		case PutCount:
			s.Counts[r.Key] = r.Count
		}
	}
}

// Message is what one Node sends another.
type Message struct {
	From, To NodeID
	Body     any
}

// The bodies of Messages.
type (
	// Write gives a Home Node a State to join into what it holds of Key.
	Write struct {
		ID    uint64
		Key   string
		State State
	}
	// WriteAck says the Write is on disk.
	WriteAck struct{ ID uint64 }
	// HintMsg gives a Node a write to keep for the Home Node For.
	HintMsg struct {
		ID    uint64
		For   NodeID
		Key   string
		State State
	}
	// HintAck says the Hint is on disk.
	HintAck struct{ ID uint64 }
	// ReadReq asks a Home Node what it holds of Key.
	ReadReq struct {
		ID  uint64
		Key string
	}
	// ReadResp is the answer.
	ReadResp struct {
		ID    uint64
		State State
	}
	// Deliver hands a Hint to the Home Node it is for.
	Deliver struct {
		Key   string
		State State
	}
	// DeliverAck says what was delivered is on disk. It carries the State
	// back, so that the holder drops the Hint only if it hasn't grown
	// since.
	DeliverAck struct {
		Key   string
		State State
	}
)

// Event is something that happens to a Node.
type Event interface{ event() }

// Tick is the passing of one unit of the Node's time.
type Tick struct{}

// Receive is a Message arriving.
type Receive struct{ Msg Message }

// Request is a client asking for something. Ref comes back in the Reply.
type Request struct {
	Ref  uint64
	Op   Op
	Key  string
	Elem string
}

func (Tick) event()    {}
func (Receive) event() {}
func (Request) event() {}

// Reply answers a Request.
type Reply struct {
	Ref uint64
	// OK is false when too few Nodes answered in time (A§13.4). A write may
	// then be on some of them all the same.
	OK     bool
	Values []string
}

// Trace says, for whoever checks a run, exactly what a write did: the Dot
// an addition was given, or the Dots a remove had seen and took out.
type Trace struct {
	Ref      uint64
	Added    *Dot
	Observed []Dot
}

// Output is what a step asks of the shell. The shell makes Persist durable
// before it does anything else here, so a Node may take whatever it has put
// in a Persist as stored once Step returns.
type Output struct {
	Persist  []Record
	Messages []Message
	Replies  []Reply
	Traces   []Trace
}

// pending is a request this Node is carrying out.
type pending struct {
	req     Request
	started int64
	order   []NodeID // the ring's order for the key: Home Nodes first
	// reading is set while the Node is asking the Home Nodes what they
	// hold, and read is what has come back.
	reading bool
	read    map[NodeID]State
	// state is what the write gives every Home Node. acked are the Home
	// Nodes that have it, stood are the Nodes standing in for those that
	// don't (by Home Node), and held the stand-ins that have it.
	state    State
	acked    map[NodeID]bool
	stood    map[NodeID]NodeID
	held     map[NodeID]bool
	answered bool
	lastSent int64
}

// Node is one Node of the Leaderless variant.
type Node struct {
	cfg    Config
	ring   Ring
	now    int64
	nextID uint64
	stored Stored
	open   map[uint64]*pending
	// offerAt is when each Hint was last offered to its Home Node.
	offerAt map[HintKey]int64
}

// New returns a Node.
func New(cfg Config) *Node {
	n := &Node{cfg: cfg, ring: NewRing(cfg.Nodes), nextID: cfg.Seed, stored: cfg.Stored,
		open: map[uint64]*pending{}, offerAt: map[HintKey]int64{}}
	n.stored.Apply(nil)
	return n
}

// Holds is what the Node holds of key.
func (n *Node) Holds(key string) State { return n.stored.Data[key] }

// Keys lists the keys the Node holds anything of, in order.
func (n *Node) Keys() []string {
	keys := make([]string, 0, len(n.stored.Data))
	for k := range n.stored.Data {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Hints is how many Hints the Node is keeping.
func (n *Node) Hints() int { return len(n.stored.Hints) }

// Ring is the Node's Ring.
func (n *Node) Ring() Ring { return n.ring }

// Step takes one Event.
func (n *Node) Step(ev Event) Output {
	var out Output
	switch ev := ev.(type) {
	case Tick:
		n.now++
		n.tick(&out)
	case Request:
		n.request(ev, &out)
	case Receive:
		n.receive(ev.Msg, &out)
	}
	n.stored.Apply(out.Persist)
	return out
}

func (n *Node) send(out *Output, to NodeID, body any) {
	out.Messages = append(out.Messages, Message{From: n.cfg.ID, To: to, Body: body})
}

// store joins state into what the Node holds of key. A write that brings
// nothing new, as one sent a second time does, costs no write to disk: a
// Node that has been away is sent the same things again and again, and
// would otherwise spend its disk on repeats while new writes queued.
func (n *Node) store(out *Output, key string, state State) {
	held, known := n.stored.Data[key]
	joined := Join(n.cfg.Mode, held, state)
	if known && joined.Equal(held) {
		return
	}
	out.Persist = append(out.Persist, Record{Kind: PutData, Key: key, State: joined})
	n.stored.Data[key] = joined
}

// keep joins state into the Hint the Node holds for a Home Node.
func (n *Node) keep(out *Output, hk HintKey, state State) {
	held, known := n.stored.Hints[hk]
	joined := Join(n.cfg.Mode, held, state)
	if known && joined.Equal(held) {
		return
	}
	out.Persist = append(out.Persist, Record{Kind: PutHint, Key: hk.Key, For: hk.For, State: joined})
	n.stored.Hints[hk] = joined
}

func (n *Node) request(req Request, out *Output) {
	n.nextID++
	id := n.nextID
	p := &pending{req: req, started: n.now, order: n.ring.Order(req.Key),
		read: map[NodeID]State{}, acked: map[NodeID]bool{}, stood: map[NodeID]NodeID{}, held: map[NodeID]bool{}}
	n.open[id] = p
	if req.Op == Add && n.cfg.Mode == AddWins {
		// An addition needs to know nothing of what is there.
		count := n.stored.Counts[req.Key] + 1
		out.Persist = append(out.Persist, Record{Kind: PutCount, Key: req.Key, Count: count})
		n.stored.Counts[req.Key] = count
		dot := Dot{Node: n.cfg.ID, N: count}
		out.Traces = append(out.Traces, Trace{Ref: req.Ref, Added: &dot})
		n.write(id, p, State{Elems: map[string][]Dot{req.Elem: {dot}}, Seen: Dots{}.With(dot)}, out)
		return
	}
	p.reading = true
	for _, h := range p.homes() {
		if h == n.cfg.ID {
			p.read[h] = n.stored.Data[req.Key]
		} else {
			n.send(out, h, ReadReq{ID: id, Key: req.Key})
		}
	}
	n.afterRead(id, p, out)
}

func (p *pending) homes() []NodeID { return p.order[:min(Homes, len(p.order))] }

// afterRead goes on once R Home Nodes have said what they hold.
func (n *Node) afterRead(id uint64, p *pending, out *Output) {
	if !p.reading || len(p.read) < n.cfg.R {
		return
	}
	p.reading = false
	var joined State
	nodes := make([]NodeID, 0, len(p.read))
	for h := range p.read {
		nodes = append(nodes, h)
	}
	slices.Sort(nodes)
	for _, h := range nodes {
		joined = Join(n.cfg.Mode, joined, p.read[h])
	}
	switch {
	case p.req.Op == Get:
		out.Replies = append(out.Replies, Reply{Ref: p.req.Ref, OK: true, Values: joined.Values()})
		delete(n.open, id)
	case n.cfg.Mode == LastWriteWins:
		// The whole set, changed, with a number above any seen.
		n.write(id, p, joined.with(p.req.Elem, p.req.Op == Add, Stamp{Count: joined.Stamp.Count + 1, Node: n.cfg.ID}), out)
	default:
		// A remove takes out the additions it has seen, and no others.
		observed := slices.Clone(joined.Elems[p.req.Elem])
		out.Traces = append(out.Traces, Trace{Ref: p.req.Ref, Observed: observed})
		seen := Dots{}
		for _, d := range observed {
			seen = seen.With(d)
		}
		n.write(id, p, State{Seen: seen}, out)
	}
}

// write gives state to the key's Home Nodes.
func (n *Node) write(id uint64, p *pending, state State, out *Output) {
	p.state, p.lastSent = state, n.now
	for _, h := range p.homes() {
		if h == n.cfg.ID {
			n.store(out, p.req.Key, state)
			p.acked[h] = true
		} else {
			n.send(out, h, Write{ID: id, Key: p.req.Key, State: state})
		}
	}
	n.afterWrite(id, p, out)
}

// afterWrite answers the client once W Nodes have the write.
func (n *Node) afterWrite(id uint64, p *pending, out *Output) {
	if !p.answered && len(p.acked)+len(p.held) >= n.cfg.W {
		p.answered = true
		out.Replies = append(out.Replies, Reply{Ref: p.req.Ref, OK: true})
	}
	if p.answered && (!n.cfg.Resend || len(p.acked) == len(p.homes())) {
		delete(n.open, id)
	}
}

func (n *Node) receive(m Message, out *Output) {
	switch b := m.Body.(type) {
	case ReadReq:
		n.send(out, m.From, ReadResp{ID: b.ID, State: n.stored.Data[b.Key]})
	case ReadResp:
		if p := n.open[b.ID]; p != nil && p.reading {
			p.read[m.From] = b.State
			n.afterRead(b.ID, p, out)
		}
	case Write:
		n.store(out, b.Key, b.State)
		n.send(out, m.From, WriteAck{ID: b.ID})
	case WriteAck:
		if p := n.open[b.ID]; p != nil && !p.reading {
			p.acked[m.From] = true
			n.afterWrite(b.ID, p, out)
		}
	case HintMsg:
		n.keep(out, HintKey{b.For, b.Key}, b.State)
		n.send(out, m.From, HintAck{ID: b.ID})
	case HintAck:
		if p := n.open[b.ID]; p != nil && !p.reading {
			p.held[m.From] = true
			n.afterWrite(b.ID, p, out)
		}
	case Deliver:
		n.store(out, b.Key, b.State)
		n.send(out, m.From, DeliverAck{Key: b.Key, State: b.State})
	case DeliverAck:
		// The Hint is dropped only if it is still what was delivered: a
		// later write for the same Home Node may have joined it since.
		hk := HintKey{m.From, b.Key}
		if held, ok := n.stored.Hints[hk]; ok && held.Equal(b.State) {
			out.Persist = append(out.Persist, Record{Kind: DropHint, Key: b.Key, For: m.From})
			delete(n.stored.Hints, hk)
			delete(n.offerAt, hk)
		}
	}
}

func (n *Node) tick(out *Output) {
	ids := make([]uint64, 0, len(n.open))
	for id := range n.open {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		p := n.open[id]
		age := n.now - p.started
		switch {
		case !p.answered && age >= giveUp:
			// Too few Nodes answered. A write may be on some of them.
			p.answered = true
			out.Replies = append(out.Replies, Reply{Ref: p.req.Ref})
			if p.reading || !n.cfg.Resend {
				delete(n.open, id)
			}
		case p.reading:
		case age >= hintAfter && (!p.answered || n.cfg.Resend):
			// A Home Node that hasn't answered by now gets a stand-in,
			// whether or not the client has its answer: W says when to
			// answer, not how many Nodes the write is meant for.
			n.standIn(id, p, out)
		}
		if p := n.open[id]; p != nil && !p.reading && n.cfg.Resend {
			switch {
			case age >= resendFor:
				delete(n.open, id)
			case n.now-p.lastSent >= resendEvery:
				p.lastSent = n.now
				for _, h := range p.homes() {
					if !p.acked[h] {
						n.send(out, h, Write{ID: id, Key: p.req.Key, State: p.state})
					}
				}
			}
		}
	}
	if n.cfg.DeliverHints {
		n.deliver(out)
	}
}

// standIn gives the write to a Node that isn't a Home Node of the key, for
// each Home Node that hasn't answered and has no stand-in yet (A§13.4).
func (n *Node) standIn(id uint64, p *pending, out *Output) {
	spare := p.order[min(Homes, len(p.order)):]
	for _, h := range p.homes() {
		if p.acked[h] {
			continue
		}
		if _, has := p.stood[h]; has {
			continue
		}
		// The next Node on the ring that isn't standing in already.
		for _, s := range spare {
			taken := false
			for _, other := range p.stood {
				taken = taken || other == s
			}
			if taken {
				continue
			}
			p.stood[h] = s
			if s == n.cfg.ID {
				n.keep(out, HintKey{h, p.req.Key}, p.state)
				p.held[s] = true
			} else {
				n.send(out, s, HintMsg{ID: id, For: h, Key: p.req.Key, State: p.state})
			}
			break
		}
	}
	n.afterWrite(id, p, out)
}

// deliver offers each Hint to the Home Node it is for, every so often.
func (n *Node) deliver(out *Output) {
	hints := make([]HintKey, 0, len(n.stored.Hints))
	for hk := range n.stored.Hints {
		hints = append(hints, hk)
	}
	slices.SortFunc(hints, func(a, b HintKey) int {
		if a.For != b.For {
			return cmp.Compare(a.For, b.For)
		}
		return cmp.Compare(a.Key, b.Key)
	})
	for _, hk := range hints {
		if at, tried := n.offerAt[hk]; tried && n.now-at < deliverEvery {
			continue
		}
		n.offerAt[hk] = n.now
		n.send(out, hk.For, Deliver{Key: hk.Key, State: n.stored.Hints[hk]})
	}
}
