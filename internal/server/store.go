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
	"syscall"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
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
	// Node is this Node's number. Nodes, Groups and Replicas place every
	// Group's replicas (shard.Hosts).
	Node, Nodes, Groups, Replicas int
	// Local are the replicas this Node hosts, by Group.
	Local map[shard.GroupID]*Node
	// Clients is every Node's client API address, as a URL.
	Clients map[int]string
	// Timeout bounds one request to a Group.
	Timeout time.Duration
	// Tick is how often the agent looks for work, and TimeEvery how often a
	// Meta Leader here moves Store time on (A§11.7).
	Tick, TimeEvery time.Duration

	mu     sync.Mutex
	table  shard.Table
	leader map[shard.GroupID]int // the Node last heard to host each Group's Leader
	httpc  *http.Client
}

// Start gives the Store the table a store begins with and runs its agent
// until ctx ends. The replicas in Local must be running.
func (s *Store) Start(ctx context.Context, slots int) {
	s.table = meta.New(slots, s.Groups).Table()
	s.leader = map[shard.GroupID]int{}
	s.httpc = &http.Client{Timeout: s.Timeout}
	go s.agent(ctx)
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

// local hands a request to this Node's replica of Group g.
func (s *Store) local(ctx context.Context, g shard.GroupID, read bool, payload []byte) (outcome, []byte, int) {
	n, hosted := s.Local[g]
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
	s.mu.Lock()
	_, known := s.leader[g]
	s.mu.Unlock()
	o, raw := s.askOnce(ctx, g, read, stamp, payload)
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
	return s.askOnce(ctx, g, read, stamp, payload)
}

// askOnce sends a request to Group g: to this Node's replica if it may
// lead, otherwise to the Node believed to host the Leader, which handles it
// or refuses and never passes it on (A§11.3). With stamp, the Node that
// proposes it puts its Store time in (A§11.7).
func (s *Store) askOnce(ctx context.Context, g shard.GroupID, read, stamp bool, payload []byte) (outcome, []byte) {
	s.mu.Lock()
	target, known := s.leader[g]
	s.mu.Unlock()
	hosts := shard.Hosts(g, s.Nodes, s.Replicas)
	if !known {
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
		o, raw, hint := s.local(ctx, g, read, payload)
		learn(o, hint)
		return o, raw
	}
	url := fmt.Sprintf("%s/v1/internal/group/%d?read=%t&stamp=%t", s.Clients[target], g, read, stamp)
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
	o, raw, hint := s.local(ctx, shard.GroupID(g), q.Get("read") == "true", payload)
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
		g = table.OwnerOf(keys[0])
		for _, k := range keys {
			if table.OwnerOf(k) != g {
				return fsm.Response{Status: fsm.StatusCrossGroup}, true
			}
		}
	}
	read := cmd.Op == fsm.OpGet
	o, raw := s.ask(ctx, g, read, !read, cmd.Encode())
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
	agent := &mover.Agent{Patience: (20 * s.Tick).Milliseconds(), ChunkKeys: 256}
	var lastTime time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case fn := <-calls:
			fn()
			continue
		case <-ticker.C:
		}
		step, cancel := context.WithTimeout(ctx, s.Timeout)
		s.refresh(step)
		table := s.Table()
		for g, n := range s.Local {
			st, ok := n.Status(step)
			if !ok || st.Role != core.LeaderRole {
				continue
			}
			if g == shard.Meta {
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
	e.s.Local[g].Inspect(ctx, func(m Machine) { fn(m.(*shardfsm.Machine)) })
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
		e.s.Local[g].Propose(ctx, payload)
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
		n, hosted := s.Local[shard.GroupID(g)]
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
}

type ownerJSON struct {
	Slot     int           `json:"slot"`
	Group    shard.GroupID `json:"group"`
	Epoch    uint32        `json:"epoch"`
	MovingTo shard.GroupID `json:"moving_to,omitempty"`
}

// tableHandler returns the table this Node holds, refreshed first.
func (s *Store) tableHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	s.refresh(ctx)
	t := s.Table()
	out := tableJSON{Version: t.Version, StoreTime: t.StoreTime}
	for i, o := range t.Slots {
		out.Slots = append(out.Slots, ownerJSON{Slot: i, Group: o.Group, Epoch: o.Epoch, MovingTo: o.MovingTo})
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
		n, hosted := s.Local[shard.GroupID(g)]
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
