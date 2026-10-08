package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
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
}

type putRequest struct {
	Value     string  `json:"value"`
	IfVersion *uint64 `json:"if_version"`
}

type kvResponse struct {
	Value   *string `json:"value,omitempty"`
	Version uint64  `json:"version"`
}

// errorResponse is the body of every non-2xx answer (A§7.2).
type errorResponse struct {
	Reason  string  `json:"reason"`
	Message string  `json:"message,omitempty"`
	Leader  string  `json:"leader,omitempty"`
	Version *uint64 `json:"version,omitempty"`
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
}

const maxBody = 1 << 20

// Handler returns the routes.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key}", a.get)
	mux.HandleFunc("PUT /v1/kv/{key}", a.put)
	mux.HandleFunc("DELETE /v1/kv/{key}", a.delete)
	mux.HandleFunc("POST /v1/sessions", a.openSession)
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
	if err != nil {
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid", Message: "body must be JSON like {\"value\": \"…\"}"})
		return
	}
	cmd := fsm.Command{Op: fsm.OpPut, Key: r.PathValue("key"), Value: []byte(req.Value)}
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
	return err1 == nil && err2 == nil && cmd.Session != 0 && cmd.Seq != 0
}

// propose submits cmd and handles every outcome that isn't the state
// machine's own answer. It reports false if it has already written one.
func (a *API) propose(w http.ResponseWriter, r *http.Request, cmd fsm.Command) (fsm.Response, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()
	var reply Reply
	if a.ReadsBypassLog && cmd.Op == fsm.OpGet {
		reply = a.Node.Read(ctx, cmd.Encode())
	} else {
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
	default:
		writeError(w, http.StatusBadRequest, errorResponse{Reason: "invalid"})
	}
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
	writeJSON(w, http.StatusOK, statusResponse{ID: st.ID, Role: role, Term: st.Term, Leader: st.Leader, Commit: st.Commit})
}

// ItemJSON is one key in the debug dump (A§7.4).
type ItemJSON struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version uint64 `json:"version"`
}

// debugItems dumps this Member's own applied data for the harness.
func (a *API) debugItems(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()
	items := []ItemJSON{}
	ok := a.Node.Inspect(ctx, func(m Machine) {
		if lister, ok := m.(interface{ Items() []fsm.Item }); ok {
			for _, it := range lister.Items() {
				items = append(items, ItemJSON{Key: it.Key, Value: string(it.Value), Version: it.Version})
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
