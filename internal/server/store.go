package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"distributed-kv-store/internal/automation"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/gossip"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/mover"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/shardfsm"
)

// Store is one Node of a store with several Groups (A§11.9): the replicas
// this Node hosts, and the shell that ties the Groups together. It routes a
// request to the Group that owns its key, forwarding it once to another Node
// if the Leader isn't here (A§11.3), and its agent carries out the Moves of
// the Groups this Node leads (A§11.4).
//
// None of what a Store remembers is trusted by any Group. Its table and its
// idea of who leads are hints; each Group decides what it owns by its own
// Log.
type Store struct {
	// Node is this Node's number. Nodes, Groups and Replicas say where every
	// Group's replicas were when the store was founded (shard.Hosts), and
	// Slots how many Slots it has.
	Node, Nodes, Groups, Replicas, Slots int
	// Local are the replicas this Node hosts, by Group. Read it with
	// replica: with Auto set, replicas come and go (A§11.11).
	Local map[shard.GroupID]*Node
	// Peers is the address, for other Nodes, of each Node this one knows
	// of. Like Clients it grows as gossip brings more.
	Peers map[int]string
	// Auto makes this Node take its part in replacing dead Nodes and in
	// moving Slots off busy Groups (auto.go). DeadWait is how long it must
	// have thought a Node dead before it says so.
	Auto     bool
	DeadWait time.Duration
	// Clients is the client API address of each Node this one knows of, as
	// a URL. It starts with what the Node was told and grows as gossip
	// brings more (A§11.10). Read it with clientURL.
	Clients map[int]string
	// Gossip is this Node's gossip. GossipEvery is the length of a round.
	Gossip      *gossip.Node
	GossipEvery time.Duration
	// Timeout bounds one request to a Group.
	Timeout time.Duration
	// Tick is how often the agent looks for work, and TimeEvery how often a
	// Meta Leader here moves Store time on (A§11.7).
	Tick, TimeEvery time.Duration

	mu     sync.Mutex // guards table, leader, Clients, Peers, Gossip, watch, meter and report
	table  shard.Table
	leader map[shard.GroupID]int // the Node last heard to host each Group's Leader
	httpc  *http.Client

	lmu     sync.RWMutex // guards Local and handles
	handles map[shard.GroupID]*handle
	// open starts a replica of a Group here and drop stops one and removes
	// its data. role and setRole read and record whether a replica joined
	// its Group late or was dropped (OpenStore).
	open    func(g shard.GroupID, members []core.NodeID) error
	drop    func(g shard.GroupID) error
	role    func(g shard.GroupID) string
	setRole func(g shard.GroupID, role string) error
	roles   map[shard.GroupID]string

	// counts is the load each Slot has put on this Node in the window now
	// open. The rest is this Node's part in automation (auto.go).
	counts   []atomic.Uint32
	meter    automation.Meter
	watch    automation.Watch
	report   shard.Report
	loadAt   time.Time
	balancer automation.Balancer
}

// replica is this Node's replica of Group g, if it has one.
func (s *Store) replica(g shard.GroupID) (*Node, bool) {
	s.lmu.RLock()
	defer s.lmu.RUnlock()
	n, hosted := s.Local[g]
	return n, hosted
}

// replicas lists the replicas this Node has, by Group.
func (s *Store) replicas() map[shard.GroupID]*Node {
	s.lmu.RLock()
	defer s.lmu.RUnlock()
	out := make(map[shard.GroupID]*Node, len(s.Local))
	for g, n := range s.Local {
		out[g] = n
	}
	return out
}

// Start gives the Store the table a store begins with and runs its agent
// until ctx ends. The replicas in Local must be running.
func (s *Store) Start(ctx context.Context, slots int) {
	s.table = meta.NewPlaced(slots, s.Groups, s.Nodes, s.Replicas).Table()
	s.leader = map[shard.GroupID]int{}
	s.counts = make([]atomic.Uint32, slots)
	s.loadAt = time.Now()
	// Requests to other Nodes come as fast as clients send them, many at
	// once. The default of two idle connections kept per Node would open
	// and close a connection for nearly every one.
	s.httpc = &http.Client{Timeout: s.Timeout, Transport: &http.Transport{
		MaxIdleConns: 4096, MaxIdleConnsPerHost: 512, IdleConnTimeout: 30 * time.Second,
	}}
	go s.agent(ctx)
	go s.gossip(ctx)
}

// clientURL is where Node n's client API is, or "" if this Node hasn't
// heard.
func (s *Store) clientURL(n int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Clients[n]
}

// gossip runs this Node's gossip rounds until ctx ends, and then says it is
// leaving. A Node that hosts a Meta Group replica puts that replica's table
// into its gossip each round: that is how the table gets there (A§11.10).
func (s *Store) gossip(ctx context.Context) {
	ticker := time.NewTicker(s.GossipEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			bye := s.Gossip.Leave()
			s.mu.Unlock()
			farewell, cancel := context.WithTimeout(context.Background(), s.GossipEvery)
			defer cancel()
			var wg sync.WaitGroup
			for _, m := range bye {
				wg.Add(1)
				go func() {
					defer wg.Done()
					s.gossipPost(farewell, m)
				}()
			}
			wg.Wait()
			return
		case <-ticker.C:
		}
		var held *shard.Table
		if n, hosted := s.replica(shard.Meta); hosted {
			look, cancel := context.WithTimeout(ctx, s.GossipEvery)
			n.Inspect(look, func(m Machine) {
				t := m.(*meta.Machine).Table()
				held = &t
			})
			cancel()
		}
		leads := s.leads(ctx)
		s.mu.Lock()
		if held != nil {
			s.Gossip.SetTable(*held)
		}
		s.tell(leads)
		out := s.Gossip.Tick()
		s.gossipLearn()
		s.mu.Unlock()
		s.gossipSend(ctx, out)
	}
}

// gossipLearn takes what gossip now holds as what this Node routes by: the
// table, and where every Node is. The caller holds mu.
func (s *Store) gossipLearn() {
	if t := s.Gossip.Table(); t.Version > s.table.Version || t.StoreTime > s.table.StoreTime {
		s.table = t
	}
	for _, m := range s.Gossip.Members() {
		if m.Client != "" {
			s.Clients[m.ID] = "http://" + m.Client
		}
		if m.Peer != "" {
			s.Peers[m.ID] = m.Peer
		}
	}
}

// gossipSend sends gossip Messages, each on its own, and forgets them: a
// lost one is made up for by the next round.
func (s *Store) gossipSend(ctx context.Context, msgs []gossip.Message) {
	for _, m := range msgs {
		go func() {
			send, cancel := context.WithTimeout(ctx, 2*s.GossipEvery)
			defer cancel()
			s.gossipPost(send, m)
		}()
	}
}

func (s *Store) gossipPost(ctx context.Context, m gossip.Message) {
	url := s.clientURL(m.To)
	if url == "" {
		return
	}
	body, err := json.Marshal(m)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/internal/gossip", bytes.NewReader(body))
	if err != nil {
		return
	}
	if resp, err := s.httpc.Do(req); err == nil {
		resp.Body.Close()
	}
}

// gossipReceive takes a gossip Message from another Node.
func (s *Store) gossipReceive(w http.ResponseWriter, r *http.Request) {
	var m gossip.Message
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	if err == nil {
		err = json.Unmarshal(body, &m)
	}
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	out := s.Gossip.Receive(m)
	s.gossipLearn()
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
	// The answers go out on their own, not on this connection: some are
	// for Nodes other than the sender. They outlive this request.
	s.gossipSend(context.WithoutCancel(r.Context()), out)
}

// NodeJSON is one Node as this Node's gossip has it.
type NodeJSON struct {
	ID          int    `json:"id"`
	Peer        string `json:"peer"`
	Client      string `json:"client"`
	Status      string `json:"status"`
	Incarnation uint64 `json:"incarnation"`
}

// nodes lists every Node this Node has heard of and what it thinks of each.
func (s *Store) nodes(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	members := s.Gossip.Members()
	s.mu.Unlock()
	out := make([]NodeJSON, 0, len(members))
	for _, m := range members {
		out = append(out, NodeJSON{ID: m.ID, Peer: m.Peer, Client: m.Client, Status: m.Status.String(), Incarnation: m.Incarnation})
	}
	writeJSON(w, http.StatusOK, out)
}

// Table is the latest Slot table this Node has heard.
func (s *Store) Table() shard.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.table.Clone()
}

// outcome is how a request to a Group ended, as this Node saw it.
type outcome uint8

const (
	answered outcome = iota // the Group's state machine gave a response
	refused                 // no effect: it never arrived, or didn't reach a Leader
	unknown                 // it may or may not have taken effect
)

// load says which Slot a request falls on and what it costs (A§11.11), so
// that the Node whose replica answers it can count it. The zero value is a
// request that isn't counted.
type load struct {
	slot int
	cost uint32
}

func loadOf(slot shard.Slot, read bool) load {
	if read {
		return load{slot: int(slot) + 1, cost: shard.ReadCost}
	}
	return load{slot: int(slot) + 1, cost: shard.WriteCost}
}

// local hands a request to this Node's replica of Group g.
func (s *Store) local(ctx context.Context, g shard.GroupID, read bool, payload []byte, l load) (outcome, []byte, int) {
	n, hosted := s.replica(g)
	if !hosted {
		return refused, nil, 0
	}
	var reply Reply
	if read {
		reply = n.Read(ctx, payload)
	} else {
		reply = n.Propose(ctx, payload)
	}
	hint, _ := shard.SplitReplicaID(uint64(reply.Leader))
	switch reply.Reason {
	case core.OK:
		if l.slot > 0 && l.slot <= len(s.counts) {
			s.counts[l.slot-1].Add(l.cost)
		}
		return answered, reply.Response, s.Node
	case core.NotLeader, core.NoMajority:
		return refused, nil, hint
	}
	return unknown, nil, 0
}

// internalReply is the body of an answer between Nodes.
type internalReply struct {
	Outcome  outcome `json:"outcome"`
	Response []byte  `json:"response,omitempty"`
	Leader   int     `json:"leader,omitempty"`
}

// ask sends a request to Group g. If the Node it tries says the Leader is
// on another, and so that nothing happened, it tries that one, once.
func (s *Store) ask(ctx context.Context, g shard.GroupID, read, stamp bool, payload []byte) (outcome, []byte) {
	return s.askCounted(ctx, g, read, stamp, payload, load{})
}

// askCounted is ask for a client's request, which adds to its Slot's load.
func (s *Store) askCounted(ctx context.Context, g shard.GroupID, read, stamp bool, payload []byte, l load) (outcome, []byte) {
	s.mu.Lock()
	_, known := s.leader[g]
	s.mu.Unlock()
	o, raw := s.askOnce(ctx, g, read, stamp, payload, l)
	if o != refused || known {
		return o, raw
	}
	// The first try was a guess. If it was told who leads, that is worth
	// one more.
	s.mu.Lock()
	_, told := s.leader[g]
	s.mu.Unlock()
	if !told {
		return o, raw
	}
	return s.askOnce(ctx, g, read, stamp, payload, l)
}

// askOnce sends a request to Group g: to this Node's replica if it may
// lead, otherwise to the Node believed to host the Leader, which handles it
// or refuses and never passes it on (A§11.3). With stamp, the Node that
// proposes it puts its Store time in (A§11.7).
func (s *Store) askOnce(ctx context.Context, g shard.GroupID, read, stamp bool, payload []byte, l load) (outcome, []byte) {
	s.mu.Lock()
	target, known := s.leader[g]
	hosts := slices.Clone(s.table.Hosts(g))
	s.mu.Unlock()
	if len(hosts) == 0 {
		return refused, nil
	}
	if !known {
		// A guess. A Node gossip thinks is dead isn't worth one, unless
		// they all are.
		s.mu.Lock()
		alive := slices.DeleteFunc(slices.Clone(hosts), func(n int) bool { return !s.Gossip.Alive(n) })
		s.mu.Unlock()
		if len(alive) > 0 {
			hosts = alive
		}
		target = hosts[int(time.Now().UnixNano())%len(hosts)]
		if slices.Contains(hosts, s.Node) {
			target = s.Node
		}
	}
	learn := func(o outcome, hint int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case o == answered:
			s.leader[g] = target
		case hint != 0:
			s.leader[g] = hint
		default:
			delete(s.leader, g)
		}
	}
	if target == s.Node {
		if stamp {
			payload = fsm.Stamp(payload, s.Table().StoreTime)
		}
		o, raw, hint := s.local(ctx, g, read, payload, l)
		learn(o, hint)
		return o, raw
	}
	url := fmt.Sprintf("%s/v1/internal/group/%d?read=%t&stamp=%t&slot=%d&cost=%d", s.clientURL(target), g, read, stamp, l.slot, l.cost)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return refused, nil
	}
	resp, err := s.httpc.Do(req)
	if err != nil {
		learn(refused, 0)
		if errors.Is(err, syscall.ECONNREFUSED) {
			return refused, nil // never reached the other Node
		}
		return unknown, nil
	}
	defer resp.Body.Close()
	var reply internalReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		learn(refused, 0)
		return unknown, nil
	}
	learn(reply.Outcome, reply.Leader)
	return reply.Outcome, reply.Response
}

// internal serves a request another Node has forwarded. It is handled here
// or refused: a request is forwarded once (A§11.3).
func (s *Store) internal(w http.ResponseWriter, r *http.Request) {
	g, err := strconv.ParseUint(r.PathValue("group"), 10, 32)
	payload, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil || readErr != nil {
		writeJSON(w, http.StatusBadRequest, internalReply{Outcome: refused})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	q := r.URL.Query()
	if q.Get("stamp") == "true" {
		payload = fsm.Stamp(payload, s.Table().StoreTime)
	}
	slot, _ := strconv.Atoi(q.Get("slot"))
	cost, _ := strconv.ParseUint(q.Get("cost"), 10, 32)
	o, raw, hint := s.local(ctx, shard.GroupID(g), q.Get("read") == "true", payload, load{slot: slot, cost: uint32(cost)})
	writeJSON(w, http.StatusOK, internalReply{Outcome: o, Response: raw, Leader: hint})
}

// serve carries out a client's Command and writes the answer for anything
// that isn't the state machine's own response. It reports false if it has
// written one.
func (s *Store) serve(w http.ResponseWriter, r *http.Request, cmd fsm.Command) (fsm.Response, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	fail := func(o outcome) (fsm.Response, bool) {
		if o == refused {
			writeError(w, http.StatusServiceUnavailable, errorResponse{Reason: "no_majority", Message: "no Leader of the owning Group was reached; nothing changed"})
		} else {
			writeError(w, http.StatusGatewayTimeout, errorResponse{Reason: "timeout", Message: "outcome unknown"})
		}
		return fsm.Response{}, false
	}
	table := s.Table()
	w.Header().Set("Table-Version", strconv.FormatUint(table.Version, 10))

	switch cmd.Op {
	case fsm.OpOpenSession:
		o, raw := s.ask(ctx, shard.Meta, false, false, meta.Command{Op: meta.OpOpenSession}.Encode())
		resp, err := meta.DecodeResponse(raw)
		if o != answered || err != nil {
			return fail(o)
		}
		return fsm.Response{Status: fsm.StatusOK, Session: resp.Session}, true
	case fsm.OpScan:
		return s.scan(ctx, cmd, fail)
	}

	g := table.OwnerOf(cmd.Key)
	slot := shard.SlotOf(cmd.Key, len(table.Slots))
	if cmd.Op == fsm.OpTxn {
		keys := make([]string, 0, len(cmd.Conds)+len(cmd.Writes))
		for _, c := range cmd.Conds {
			keys = append(keys, c.Key)
		}
		for _, wr := range cmd.Writes {
			keys = append(keys, wr.Key)
		}
		if len(keys) == 0 {
			return fsm.Response{Status: fsm.StatusInvalid}, true
		}
		g, slot = table.OwnerOf(keys[0]), shard.SlotOf(keys[0], len(table.Slots))
		for _, k := range keys {
			if table.OwnerOf(k) != g {
				return fsm.Response{Status: fsm.StatusCrossGroup}, true
			}
		}
	}
	read := cmd.Op == fsm.OpGet
	o, raw := s.askCounted(ctx, g, read, !read, cmd.Encode(), loadOf(slot, read))
	if o != answered {
		return fail(o)
	}
	resp, err := fsm.DecodeResponse(raw)
	if err != nil {
		writeError(w, http.StatusInternalServerError, errorResponse{Reason: "internal", Message: err.Error()})
		return fsm.Response{}, false
	}
	if resp.Status == fsm.StatusWrongGroup {
		s.refresh(ctx) // this Node's table is out of date
	}
	return resp, true
}

// scan asks every data Group and merges what they say (A§11.8).
func (s *Store) scan(ctx context.Context, cmd fsm.Command, fail func(outcome) (fsm.Response, bool)) (fsm.Response, bool) {
	merged := fsm.Response{Status: fsm.StatusOK}
	payload := cmd.Encode()
	for g := 1; g <= s.Groups; g++ {
		o, raw := s.ask(ctx, shard.GroupID(g), true, false, payload)
		resp, err := fsm.DecodeResponse(raw)
		if o != answered || err != nil || resp.Status != fsm.StatusOK {
			return fail(refused) // a scan changes nothing
		}
		merged.Items = append(merged.Items, resp.Items...)
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
	if cmd.Limit > 0 && uint64(len(merged.Items)) > cmd.Limit {
		merged.Items = merged.Items[:cmd.Limit]
	}
	return merged, true
}

// refresh asks the Meta Group for the table and keeps it if it is newer.
func (s *Store) refresh(ctx context.Context) {
	o, raw := s.ask(ctx, shard.Meta, true, false, nil)
	if o != answered {
		return
	}
	t, err := shard.DecodeTable(raw)
	if err != nil {
		log.Printf("store: node %d: unreadable table from the Meta Group: %v", s.Node, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.Version > s.table.Version || t.StoreTime > s.table.StoreTime {
		s.table = t
	}
}

// agent is this Node's periodic work: learn the table, keep time moving, and
// let the mover take each Move of a Group led here one step on. Everything
// the mover remembers is touched only on this goroutine: answers to what it
// asked come back through calls.
func (s *Store) agent(ctx context.Context) {
	ticker := time.NewTicker(s.Tick)
	defer ticker.Stop()
	calls := make(chan func(), 256)
	e := storeEnv{s: s, ctx: ctx, calls: calls}
	agent := &mover.Agent{Patience: (20 * s.Tick).Milliseconds(), ChunkKeys: 256,
		OnHandover: func(g shard.GroupID, slot shard.Slot, frozenFor int64) {
			log.Printf("store: node %d: Slot %d handed over by Group %d after being frozen for %d ms", s.Node, slot, g, frozenFor)
		}}
	var lastTime time.Time
	auto := &automaton{s: s, e: e, asking: map[shard.GroupID]bool{}}
	for round := 0; ; round++ {
		select {
		case <-ctx.Done():
			return
		case fn := <-calls:
			fn()
			continue
		case <-ticker.C:
		}
		// The table arrives by gossip. Nobody asks the Meta Group on a timer.
		step, cancel := context.WithTimeout(ctx, s.Timeout)
		table := s.Table()
		slow := s.Auto && round%autoEvery == 0
		if slow {
			auto.replicasWanted(ctx, table)
		}
		for g, n := range s.replicas() {
			st, ok := n.Status(step)
			if !ok || st.Role != core.LeaderRole {
				continue
			}
			if slow {
				auto.changeMembers(ctx, g, n, st, table)
			}
			if g == shard.Meta {
				if slow {
					auto.steer(ctx, n, st)
				}
				// The Meta Leader's clock is the store's clock (A§11.7).
				if time.Since(lastTime) >= s.TimeEvery {
					lastTime = time.Now()
					tick := meta.Command{Op: meta.OpTick, Stamp: time.Now().UnixMilli()}.Encode()
					go n.Propose(ctx, tick)
				}
				continue
			}
			due := false
			e.WithMachine(g, func(m *shardfsm.Machine) { due = m.Due(table.StoreTime) })
			if due {
				// Nobody is writing to this Group: say what time it is.
				go n.Propose(ctx, fsm.Command{Op: fsm.OpTick, Stamp: table.StoreTime}.Encode())
			}
			agent.Step(e, g, st.Term, table)
		}
		cancel()
	}
}

// storeEnv is what the mover sees of a real Node.
type storeEnv struct {
	s     *Store
	ctx   context.Context
	calls chan func()
}

func (e storeEnv) Now() int64 { return time.Now().UnixMilli() }

func (e storeEnv) WithMachine(g shard.GroupID, fn func(*shardfsm.Machine)) {
	ctx, cancel := context.WithTimeout(e.ctx, e.s.Timeout)
	defer cancel()
	if n, hosted := e.s.replica(g); hosted {
		n.Inspect(ctx, func(m Machine) { fn(m.(*shardfsm.Machine)) })
	}
}

// back runs fn on the agent's goroutine, unless the Node is stopping.
func (e storeEnv) back(fn func()) {
	select {
	case e.calls <- fn:
	case <-e.ctx.Done():
	}
}

func (e storeEnv) Local(g shard.GroupID, payload []byte, done func()) {
	go func() {
		ctx, cancel := context.WithTimeout(e.ctx, e.s.Timeout)
		defer cancel()
		if n, hosted := e.s.replica(g); hosted {
			n.Propose(ctx, payload)
		}
		e.back(done)
	}()
}

func (e storeEnv) Ask(g shard.GroupID, payload []byte, back func(bool, []byte)) {
	go func() {
		ctx, cancel := context.WithTimeout(e.ctx, e.s.Timeout)
		defer cancel()
		o, raw := e.s.ask(ctx, g, false, false, payload)
		e.back(func() { back(o == answered, raw) })
	}()
}

// groupStatus is one hosted replica's view of its Group.
type groupStatus struct {
	Group  shard.GroupID `json:"group"`
	Role   string        `json:"role"`
	Term   core.Term     `json:"term"`
	Commit core.Index    `json:"commit"`
}

type storeStatus struct {
	ID           int           `json:"id"`
	TableVersion uint64        `json:"table_version"`
	StoreTime    int64         `json:"store_time"`
	Groups       []groupStatus `json:"groups"`
}

// status reports what this Node hosts and the table version it holds.
func (s *Store) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	table := s.Table()
	out := storeStatus{ID: s.Node, TableVersion: table.Version, StoreTime: table.StoreTime}
	roles := map[core.Role]string{core.Follower: "follower", core.Candidate: "candidate", core.LeaderRole: "leader"}
	for g := 0; g <= s.Groups; g++ {
		n, hosted := s.replica(shard.GroupID(g))
		if !hosted {
			continue
		}
		if st, ok := n.Status(ctx); ok {
			out.Groups = append(out.Groups, groupStatus{Group: shard.GroupID(g), Role: roles[st.Role], Term: st.Term, Commit: st.Commit})
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// tableJSON is the Slot table as a client sees it.
type tableJSON struct {
	Version   uint64      `json:"version"`
	StoreTime int64       `json:"store_time"`
	Slots     []ownerJSON `json:"slots"`
	Groups    []groupJSON `json:"groups"`
}

type ownerJSON struct {
	Slot     int           `json:"slot"`
	Group    shard.GroupID `json:"group"`
	Epoch    uint32        `json:"epoch"`
	MovingTo shard.GroupID `json:"moving_to,omitempty"`
	// Load is the Slot's smoothed load as this Node has heard it (A§11.11).
	Load uint32 `json:"load"`
}

// groupJSON is one Group's Members as the table has them, and the change
// the Meta Group wants made, if any.
type groupJSON struct {
	Group   shard.GroupID `json:"group"`
	Members []int         `json:"members"`
	Add     int           `json:"add,omitempty"`
	Remove  int           `json:"remove,omitempty"`
	Load    uint64        `json:"load"`
}

// tableHandler returns the table this Node holds, refreshed first.
func (s *Store) tableHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	s.refresh(ctx)
	t := s.Table()
	out := tableJSON{Version: t.Version, StoreTime: t.StoreTime}
	heard := s.heardLoad()
	sums := automation.GroupLoad(t, heard, s.Groups)
	for i, o := range t.Slots {
		out.Slots = append(out.Slots, ownerJSON{Slot: i, Group: o.Group, Epoch: o.Epoch, MovingTo: o.MovingTo, Load: heard[i]})
	}
	for g, row := range t.Groups {
		out.Groups = append(out.Groups, groupJSON{Group: shard.GroupID(g), Members: row.Members, Add: row.Add, Remove: row.Remove, Load: sums[g]})
	}
	writeJSON(w, http.StatusOK, out)
}

// move asks the Meta Group to move a Slot to a Group (A§11.4). It answers
// once the intent is Committed. The Move itself then proceeds by itself.
func (s *Store) move(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Slot shard.Slot    `json:"slot"`
		To   shard.GroupID `json:"to"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err == nil {
		err = json.Unmarshal(body, &req)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "body must be JSON like {\"slot\": 3, \"to\": 2}"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	o, raw := s.ask(ctx, shard.Meta, false, false, meta.Command{Op: meta.OpMove, Slot: req.Slot, To: req.To}.Encode())
	resp, err := meta.DecodeResponse(raw)
	switch {
	case o == refused:
		writeError(w, http.StatusServiceUnavailable, errorResponse{Reason: "no_majority", Message: "the Meta Group's Leader wasn't reached"})
	case o != answered || err != nil:
		writeError(w, http.StatusGatewayTimeout, errorResponse{Reason: "timeout", Message: "outcome unknown"})
	case resp.Status == meta.StatusBusy:
		writeError(w, http.StatusConflict, errorResponse{Reason: "change_in_progress", Message: "that Slot is already being moved"})
	case resp.Status != meta.StatusOK:
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "no such Slot or Group, or the Group owns the Slot already"})
	default:
		writeJSON(w, http.StatusOK, map[string]uint64{"table_version": resp.Version})
	}
}

// GroupItems is one hosted data replica's own state, for the harness.
type GroupItems struct {
	Group shard.GroupID `json:"group"`
	// Serves lists the Slots the replica would answer for.
	Serves []int      `json:"serves"`
	Items  []ItemJSON `json:"items"`
}

// debugItems dumps what each data replica on this Node holds and serves, as
// applied so far (A§7.4).
func (s *Store) debugItems(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	out := []GroupItems{}
	for g := 1; g <= s.Groups; g++ {
		n, hosted := s.replica(shard.GroupID(g))
		if !hosted {
			continue
		}
		dump := GroupItems{Group: shard.GroupID(g), Serves: []int{}, Items: []ItemJSON{}}
		ok := n.Inspect(ctx, func(m Machine) {
			sm := m.(*shardfsm.Machine)
			for slot, info := range sm.Slots() {
				if info.Serves() {
					dump.Serves = append(dump.Serves, slot)
				}
			}
			for _, it := range sm.Items() {
				dump.Items = append(dump.Items, ItemJSON{Key: it.Key, Value: string(it.Value), Version: it.Version, Expires: it.Expires})
			}
		})
		if !ok {
			writeError(w, http.StatusGatewayTimeout, errorResponse{Reason: "timeout"})
			return
		}
		out = append(out, dump)
	}
	writeJSON(w, http.StatusOK, out)
}
