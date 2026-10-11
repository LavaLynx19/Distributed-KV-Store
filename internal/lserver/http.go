package lserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"distributed-kv-store/internal/leaderless"
)

// API serves the Leaderless variant's client routes (A§13.3). They are the
// routes of the consistent store, and they mean something else.
type API struct {
	Server *Server
	// Timeout bounds one request.
	Timeout time.Duration
}

type reason struct {
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body) // a body of strings and lists always encodes
}

func (a *API) do(w http.ResponseWriter, r *http.Request, op leaderless.Op, key, elem string, ttl time.Duration) (leaderless.Reply, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()
	reply, err := a.Server.Do(ctx, op, key, elem, ttl)
	if err != nil || !reply.OK {
		writeJSON(w, http.StatusServiceUnavailable, reason{Reason: "too_few_nodes", Message: "too few Nodes answered in time; a write may be on some of them, and sending it again is safe"})
		return reply, false
	}
	return reply, true
}

// Handler returns the routes.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Value     string  `json:"value"`
			TTL       int64   `json:"ttl"`
			IfVersion *uint64 `json:"if_version"`
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err == nil {
			err = json.Unmarshal(raw, &body)
		}
		switch {
		case err != nil || body.TTL < 0:
			writeJSON(w, http.StatusBadRequest, reason{Reason: "invalid", Message: `body must be JSON like {"value": …, "ttl": …}`})
			return
		case body.IfVersion != nil:
			writeJSON(w, http.StatusBadRequest, reason{Reason: "not_offered", Message: "the Leaderless variant has no compare-and-set"})
			return
		}
		if _, ok := a.do(w, r, leaderless.Add, r.PathValue("key"), body.Value, time.Duration(body.TTL)*time.Millisecond); ok {
			writeJSON(w, http.StatusOK, struct{}{})
		}
	})
	mux.HandleFunc("DELETE /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("value") {
			writeJSON(w, http.StatusBadRequest, reason{Reason: "invalid", Message: "say which element to remove: ?value=…"})
			return
		}
		if _, ok := a.do(w, r, leaderless.Remove, r.PathValue("key"), r.URL.Query().Get("value"), 0); ok {
			writeJSON(w, http.StatusOK, struct{}{})
		}
	})
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if reply, ok := a.do(w, r, leaderless.Get, r.PathValue("key"), "", 0); ok {
			values := reply.Values
			if values == nil {
				values = []string{}
			}
			writeJSON(w, http.StatusOK, map[string][]string{"values": values})
		}
	})
	mux.HandleFunc("GET /v1/kv", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if reply, ok := a.do(w, r, leaderless.Scan, q.Get("start"), q.Get("end"), 0); ok {
			type item struct {
				Key    string   `json:"key"`
				Values []string `json:"values"`
			}
			items := []item{}
			for _, it := range reply.Items {
				items = append(items, item{Key: it.Key, Values: it.Values})
			}
			writeJSON(w, http.StatusOK, map[string][]item{"items": items})
		}
	})
	notOffered := func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusBadRequest, reason{Reason: "not_offered", Message: "the Leaderless variant has no Transactions and no Sessions"})
	}
	mux.HandleFunc("POST /v1/txn", notOffered)
	mux.HandleFunc("POST /v1/sessions", notOffered)
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		v := a.Server.State()
		writeJSON(w, http.StatusOK, map[string]any{"id": a.Server.cfg.ID, "leaderless": true, "keys": len(v.Keys), "hints": v.Hints})
	})
	// What the Node holds, for whoever checks a run.
	mux.HandleFunc("GET /v1/debug/state", func(w http.ResponseWriter, _ *http.Request) {
		v := a.Server.State()
		type key struct {
			Values []string `json:"values"`
			Canon  string   `json:"canon"`
			Home   bool     `json:"home"`
		}
		out := struct {
			Hints int            `json:"hints"`
			Keys  map[string]key `json:"keys"`
		}{Hints: v.Hints, Keys: map[string]key{}}
		for k, kv := range v.Keys {
			out.Keys[k] = key{Values: kv.Values, Canon: kv.Canon, Home: kv.Home}
		}
		writeJSON(w, http.StatusOK, out)
	})
	return mux
}
