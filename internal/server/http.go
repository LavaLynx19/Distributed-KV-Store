package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
)

// API serves the client contract of A§7.1: single keys, Sessions and status
// so far. Scans, transactions and admin arrive with their Rungs.
type API struct {
	Node *Node
	// Clients maps each Member to its client-facing address, for the hint in
	// a not_leader answer.
	Clients map[core.NodeID]string
	// Timeout bounds how long a request waits for its command to commit.
	Timeout time.Duration
	// ReadsBypassLog answers gets through the core's read path (A§6.2). The
	// core must be configured to match.
	ReadsBypassLog bool
	// Now is this Member's clock, in milliseconds. Nil means the system
	// clock.
	Now func() int64
	// AdminTimeout bounds a Membership change, which can take far longer
	// than a write: a Node being added must first be sent the whole Log.
	AdminTimeout time.Duration
	// Store, if set, makes this the API of a Node in a store with several
	// Groups (A§11): requests are routed by key to the Group that owns it,
	// and Node is unused.
	Store *Store
}

func (a *API) now() int64 {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now().UnixMilli()
}

type putRequest struct {
	Value     string  `json:"value"`
	IfVersion *uint64 `json:"if_version"`
	// TTL is the key's time-to-live in milliseconds. Zero or absent means
	// the key lives until it is deleted.
	TTL int64 `json:"ttl"`
}

type kvResponse struct {
	Value   *string `json:"value,omitempty"`
	Version uint64  `json:"version"`
}

type scanResponse struct {
	Items []ItemJSON `json:"items"`
}

// txnRequest is a Transaction (A§5.2): if every key under "if" has the
// version given (0 for a key that must not exist), every write is applied.
type txnRequest struct {
	If []struct {
		Key     string `json:"key"`
		Version uint64 `json:"version"`
	} `json:"if"`
	Writes []struct {
		Op    string `json:"op"` // "put" or "delete"
		Key   string `json:"key"`
		Value string `json:"value"`
		TTL   int64  `json:"ttl"`
	} `json:"writes"`
}

// errorResponse is the body of every non-2xx answer (A§7.2).
type errorResponse struct {
	Reason  string  `json:"reason"`
	Message string  `json:"message,omitempty"`
	Leader  string  `json:"leader,omitempty"`
	Version *uint64 `json:"version,omitempty"`
	// Failed lists the conditions a refused Transaction failed, each with
	// the version found.
	Failed []ItemJSON `json:"failed,omitempty"`
}

type sessionResponse struct {
	Session uint64 `json:"session"`
}

type statusResponse struct {
	ID     core.NodeID `json:"id"`
	Role   string      `json:"role"`
	Term   core.Term   `json:"term"`
	Leader core.NodeID `json:"leader"`
	Commit core.Index  `json:"commit"`
	// Recovering is set while the Member stays out of elections after
	// finding damage on its disk (A§6.8).
	Recovering bool `json:"recovering,omitempty"`
	// Members is the Group's Member list as this Node has it (A§6.5).
	Members []core.NodeID `json:"members"`
}

const maxBody = 1 << 20

// Handler returns the routes.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	if a.Store != nil {
		mux.HandleFunc("GET /v1/kv/{key}", a.get)
		mux.HandleFunc("PUT /v1/kv/{key}", a.put)
		mux.HandleFunc("DELETE /v1/kv/{key}", a.delete)
		mux.HandleFunc("GET /v1/kv", a.scan)
		mux.HandleFunc("POST /v1/txn", a.txn)
		mux.HandleFunc("POST /v1/sessions", a.openSession)
		mux.HandleFunc("POST /v1/internal/group/{group}", a.Store.internal)
		mux.HandleFunc("POST /v1/internal/gossip", a.Store.gossipReceive)
		mux.HandleFunc("GET /v1/internal/members/{group}", a.Store.members)
		mux.HandleFunc("GET /v1/nodes", a.Store.nodes)
		mux.HandleFunc("GET /v1/status", a.Store.status)
		mux.HandleFunc("GET /v1/table", a.Store.tableHandler)
		mux.HandleFunc("POST /v1/admin/moves", a.Store.move)
		mux.HandleFunc("GET /v1/debug/items", a.Store.debugItems)
		return mux
	}
	mux.HandleFunc("GET /v1/kv/{key}", a.get)
	mux.HandleFunc("PUT /v1/kv/{key}", a.put)
	mux.HandleFunc("DELETE /v1/kv/{key}", a.delete)
	mux.HandleFunc("GET /v1/kv", a.scan)
	mux.HandleFunc("POST /v1/txn", a.txn)
	mux.HandleFunc("POST /v1/sessions", a.openSession)
	mux.HandleFunc("POST /v1/admin/members", a.addMember)
	mux.HandleFunc("DELETE /v1/admin/members/{id}", a.removeMember)
	mux.HandleFunc("GET /v1/status", a.status)
	mux.HandleFunc("GET /v1/debug/items", a.debugItems)
	return mux
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	a.run(w, r, fsm.Command{Op: fsm.OpGet, Key: r.PathValue("key")})
}

func (a *API) put(w http.ResponseWriter, r *http.Request) {
	var req putRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err == nil {
		err = json.Unmarshal(body, &req)
	}
	if err != nil || req.TTL < 0 {
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "body must be JSON like {\"value\": \"…\"}, with ttl in milliseconds if given"})
		return
	}
	cmd := fsm.Command{Op: fsm.OpPut, Key: r.PathValue("key"), Value: []byte(req.Value), TTL: req.TTL}
	if req.IfVersion != nil {
		cmd.Conditional, cmd.IfVersion = true, *req.IfVersion
	}
	a.run(w, r, cmd)
}

func (a *API) delete(w http.ResponseWriter, r *http.Request) {
	cmd := fsm.Command{Op: fsm.OpDelete, Key: r.PathValue("key")}
	if v := r.URL.Query().Get("if_version"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "if_version must be a non-negative integer"})
			return
		}
		cmd.Conditional, cmd.IfVersion = true, n
	}
	a.run(w, r, cmd)
}

// scan answers a range scan: the keys from start up to but not including
// end, in key order (A§7.1).
func (a *API) scan(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	cmd := fsm.Command{Op: fsm.OpScan, Key: q.Get("start"), End: q.Get("end")}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "limit must be a non-negative integer"})
			return
		}
		cmd.Limit = n
	}
	resp, ok := a.propose(w, r, cmd)
	if !ok {
		return
	}
	out := scanResponse{Items: []ItemJSON{}}
	for _, it := range resp.Items {
		out.Items = append(out.Items, ItemJSON{Key: it.Key, Value: string(it.Value), Version: it.Version})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) txn(w http.ResponseWriter, r *http.Request) {
	var req txnRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err == nil {
		err = json.Unmarshal(body, &req)
	}
	cmd := fsm.Command{Op: fsm.OpTxn}
	for _, c := range req.If {
		cmd.Conds = append(cmd.Conds, fsm.Cond{Key: c.Key, Version: c.Version})
	}
	for _, wr := range req.Writes {
		switch {
		case wr.Op == "put" && wr.TTL >= 0:
			cmd.Writes = append(cmd.Writes, fsm.Write{Op: fsm.OpPut, Key: wr.Key, Value: []byte(wr.Value), TTL: wr.TTL})
		case wr.Op == "delete":
			cmd.Writes = append(cmd.Writes, fsm.Write{Op: fsm.OpDelete, Key: wr.Key})
		default:
			err = errors.New("bad write")
		}
	}
	if err != nil || !identify(r, &cmd) {
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "body must be JSON like {\"if\": [{\"key\": …, \"version\": …}], \"writes\": [{\"op\": \"put\"|\"delete\", \"key\": …}]}"})
		return
	}
	resp, ok := a.propose(w, r, cmd)
	if !ok {
		return
	}
	switch resp.Status {
	case fsm.StatusOK:
		writeJSON(w, http.StatusOK, kvResponse{Version: resp.Version})
	case fsm.StatusVersionMismatch:
		failed := []ItemJSON{}
		for _, it := range resp.Items {
			failed = append(failed, ItemJSON{Key: it.Key, Version: it.Version})
		}
		writeError(w, http.StatusConflict, errorResponse{Reason: "version_mismatch", Failed: failed})
	case fsm.StatusSessionExpired:
		writeError(w, http.StatusGone, errorResponse{Reason: "session_expired"})
	case fsm.StatusWrongGroup:
		writeError(w, http.StatusMisdirectedRequest, errorResponse{Reason: "wrong_group", Message: "this Node's table was out of date; nothing changed"})
	case fsm.StatusMoving:
		writeError(w, http.StatusServiceUnavailable, errorResponse{Reason: "moving", Message: "the key's Slot is being moved; nothing changed"})
	case fsm.StatusCrossGroup:
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "cross_group", Message: "the keys are owned by more than one Group"})
	default:
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid"})
	}
}

func (a *API) openSession(w http.ResponseWriter, r *http.Request) {
	if resp, ok := a.propose(w, r, fsm.Command{Op: fsm.OpOpenSession}); ok {
		writeJSON(w, http.StatusOK, sessionResponse{Session: resp.Session})
	}
}

// identify reads the Session-Id and Request-Seq headers into cmd (A§6.3).
// Both or neither must be present. Without them the request is applied each
// time it arrives.
func identify(r *http.Request, cmd *fsm.Command) bool {
	id, seq := r.Header.Get("Session-Id"), r.Header.Get("Request-Seq")
	if id == "" && seq == "" {
		return true
	}
	var err1, err2 error
	cmd.Session, err1 = strconv.ParseUint(id, 10, 64)
	cmd.Seq, err2 = strconv.ParseUint(seq, 10, 64)
	// A client sets this on a request no earlier attempt of which can have
	// taken effect, to let a Group that doesn't know the Session start a
	// record of it (A§11.6).
	cmd.Register = r.Header.Get("Session-Register") != ""
	return err1 == nil && err2 == nil && cmd.Session != 0 && cmd.Seq != 0
}

// propose submits cmd and handles every outcome that isn't the state
// machine's own answer. It reports false if it has already written one.
func (a *API) propose(w http.ResponseWriter, r *http.Request, cmd fsm.Command) (fsm.Response, bool) {
	if a.Store != nil {
		return a.Store.serve(w, r, cmd)
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()
	var reply Reply
	if a.ReadsBypassLog && (cmd.Op == fsm.OpGet || cmd.Op == fsm.OpScan) {
		reply = a.Node.Read(ctx, cmd.Encode())
	} else {
		// This Member's clock reading goes in with the request. It counts
		// only if this Member is the Leader: nobody else's proposal reaches
		// the Log (A§6.1).
		cmd.Stamp = a.now()
		reply = a.Node.Propose(ctx, cmd.Encode())
	}

	switch reply.Reason {
	case core.NotLeader:
		writeError(w, http.StatusMisdirectedRequest, errorResponse{Reason: "not_leader", Leader: a.Clients[reply.Leader]})
		return fsm.Response{}, false
	case core.NoMajority:
		writeError(w, http.StatusServiceUnavailable, errorResponse{Reason: "no_majority"})
		return fsm.Response{}, false
	case core.Unknown:
		writeError(w, http.StatusGatewayTimeout, errorResponse{Reason: "timeout", Message: "outcome unknown"})
		return fsm.Response{}, false
	}
	resp, err := fsm.DecodeResponse(reply.Response)
	if err != nil {
		writeError(w, http.StatusInternalServerError, errorResponse{Reason: "internal", Message: err.Error()})
		return fsm.Response{}, false
	}
	return resp, true
}

// run proposes cmd and turns the outcome into the answer A§7.2 prescribes.
func (a *API) run(w http.ResponseWriter, r *http.Request, cmd fsm.Command) {
	if !identify(r, &cmd) {
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "Session-Id and Request-Seq must both be positive integers"})
		return
	}
	resp, ok := a.propose(w, r, cmd)
	if !ok {
		return
	}
	switch resp.Status {
	case fsm.StatusOK:
		out := kvResponse{Version: resp.Version}
		if cmd.Op == fsm.OpGet {
			v := string(resp.Value)
			out.Value = &v
		}
		writeJSON(w, http.StatusOK, out)
	case fsm.StatusNotFound:
		writeError(w, http.StatusNotFound, errorResponse{Reason: "not_found"})
	case fsm.StatusVersionMismatch:
		writeError(w, http.StatusConflict, errorResponse{Reason: "version_mismatch", Version: &resp.Version})
	case fsm.StatusSessionExpired:
		writeError(w, http.StatusGone, errorResponse{Reason: "session_expired"})
	case fsm.StatusWrongGroup:
		writeError(w, http.StatusMisdirectedRequest, errorResponse{Reason: "wrong_group", Message: "this Node's table was out of date; nothing changed"})
	case fsm.StatusMoving:
		writeError(w, http.StatusServiceUnavailable, errorResponse{Reason: "moving", Message: "the key's Slot is being moved; nothing changed"})
	case fsm.StatusCrossGroup:
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "cross_group", Message: "the keys are owned by more than one Group"})
	default:
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid"})
	}
}

// addMember adds one Node to the Group (A§7.3). The Node must already be
// running as a Spare: the Leader brings it up to date before it is added.
func (a *API) addMember(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID core.NodeID `json:"id"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err == nil {
		err = json.Unmarshal(body, &req)
	}
	if err != nil || req.ID == 0 {
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "body must be JSON like {\"id\": 4}"})
		return
	}
	a.changeMembers(w, r, func(members []core.NodeID) []core.NodeID { return append(members, req.ID) })
}

// removeMember removes one Member from the Group (A§7.3).
func (a *API) removeMember(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "the Member's id must be a positive integer"})
		return
	}
	a.changeMembers(w, r, func(members []core.NodeID) []core.NodeID {
		return slices.DeleteFunc(members, func(m core.NodeID) bool { return m == core.NodeID(id) })
	})
}

// changeMembers asks for the Member list change(current), and answers with
// the list as it then stands.
func (a *API) changeMembers(w http.ResponseWriter, r *http.Request, change func([]core.NodeID) []core.NodeID) {
	ctx, cancel := context.WithTimeout(r.Context(), a.AdminTimeout)
	defer cancel()
	st, ok := a.Node.Status(ctx)
	if !ok {
		writeError(w, http.StatusGatewayTimeout, errorResponse{Reason: "timeout"})
		return
	}
	want := change(slices.Clone(st.Members))
	switch reply := a.Node.Reconfigure(ctx, want); reply.Reason {
	case core.OK:
		slices.Sort(want)
		writeJSON(w, http.StatusOK, membersResponse{Members: want})
	case core.NotLeader:
		writeError(w, http.StatusMisdirectedRequest, errorResponse{Reason: "not_leader", Leader: a.Clients[reply.Leader]})
	case core.NoMajority:
		writeError(w, http.StatusServiceUnavailable, errorResponse{Reason: "no_majority"})
	case core.Busy:
		writeError(w, http.StatusConflict, errorResponse{Reason: "change_in_progress", Message: "another Membership change is under way, or the Leader is new; try again"})
	case core.NoCatchUp:
		writeError(w, http.StatusServiceUnavailable, errorResponse{Reason: "member_unreachable", Message: "the Node didn't catch up with the Log, so it wasn't added"})
	case core.Invalid:
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "that Node is already a Member, or isn't one, or is the last one"})
	default:
		writeError(w, http.StatusGatewayTimeout, errorResponse{Reason: "timeout", Message: "outcome unknown"})
	}
}

type membersResponse struct {
	Members []core.NodeID `json:"members"`
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()
	st, ok := a.Node.Status(ctx)
	if !ok {
		writeError(w, http.StatusGatewayTimeout, errorResponse{Reason: "timeout"})
		return
	}
	role := map[core.Role]string{core.Follower: "follower", core.Candidate: "candidate", core.LeaderRole: "leader"}[st.Role]
	writeJSON(w, http.StatusOK, statusResponse{ID: st.ID, Role: role, Term: st.Term, Leader: st.Leader, Commit: st.Commit, Recovering: st.Recovering, Members: st.Members})
}

// ItemJSON is one key in the debug dump (A§7.4).
type ItemJSON struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version uint64 `json:"version"`
	Expires int64  `json:"expires,omitempty"`
}

// debugItems dumps this Member's own applied data for the harness.
func (a *API) debugItems(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()
	items := []ItemJSON{}
	ok := a.Node.Inspect(ctx, func(m Machine) {
		if lister, ok := m.(interface{ Items() []fsm.Item }); ok {
			for _, it := range lister.Items() {
				items = append(items, ItemJSON{Key: it.Key, Value: string(it.Value), Version: it.Version, Expires: it.Expires})
			}
		}
	})
	if !ok {
		writeError(w, http.StatusGatewayTimeout, errorResponse{Reason: "timeout"})
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body) // the client has gone if this fails
}

func writeError(w http.ResponseWriter, code int, body errorResponse) {
	writeJSON(w, code, body)
}
