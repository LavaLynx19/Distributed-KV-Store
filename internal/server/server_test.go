package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/transport"
)

type member struct {
	api    *httptest.Server
	cancel context.CancelFunc
	tr     *transport.Transport
}

// cluster starts n real Members in this process: TCP between them, HTTP in
// front, a 5ms tick.
func cluster(t *testing.T, n int) map[core.NodeID]*member {
	t.Helper()
	return clusterReading(t, n, raft.ReadsByIndex)
}

func clusterReading(t *testing.T, n int, reads raft.ReadMode) map[core.NodeID]*member {
	t.Helper()
	transport.Register(raft.MessageBodies()...)

	listeners := map[core.NodeID]net.Listener{}
	peers := map[core.NodeID]string{}
	var ids []core.NodeID
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id := core.NodeID(i)
		ids = append(ids, id)
		listeners[id] = ln
		peers[id] = ln.Addr().String()
	}

	members := map[core.NodeID]*member{}
	clients := map[core.NodeID]string{}
	for _, id := range ids {
		var node *server.Node
		tr := transport.New(id, listeners[id], peers, func(m core.Message) { node.Deliver(m) })
		c := raft.New(raft.Config{ID: id, Members: ids, ElectionTicks: 10, HeartbeatTicks: 1,
			Rand: rand.New(rand.NewPCG(uint64(id), 99)), Reads: reads})
		node = server.NewNode(c, fsm.New(), tr.Send, 5*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		go node.Run(ctx)
		api := &server.API{Node: node, Clients: clients, Timeout: 2 * time.Second, ReadsBypassLog: reads != raft.ReadsThroughLog}
		m := &member{api: httptest.NewServer(api.Handler()), cancel: cancel, tr: tr}
		members[id] = m
		clients[id] = m.api.URL
		t.Cleanup(func() { m.api.Close(); cancel(); tr.Close() })
	}
	return members
}

type answer struct {
	code int
	body map[string]any
}

func call(t *testing.T, method, url, body string, headers ...string) answer {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	a := answer{code: resp.StatusCode, body: map[string]any{}}
	if err := json.NewDecoder(resp.Body).Decode(&a.body); err != nil {
		t.Fatalf("%s %s: undecodable body: %v", method, url, err)
	}
	return a
}

// leaderURL waits for a Leader and returns its client address.
func leaderURL(t *testing.T, members map[core.NodeID]*member) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if a := call(t, "GET", m.api.URL+"/v1/status", ""); a.body["role"] == "leader" {
				return m.api.URL
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no Leader within 5s")
	return ""
}

func TestClientAPI(t *testing.T) {
	members := cluster(t, 3)
	leader := leaderURL(t, members)

	if a := call(t, "GET", leader+"/v1/kv/a", ""); a.code != 404 || a.body["reason"] != "not_found" {
		t.Fatalf("get missing: %+v", a)
	}
	put := call(t, "PUT", leader+"/v1/kv/a", `{"value":"1"}`)
	if put.code != 200 || put.body["version"] == nil {
		t.Fatalf("put: %+v", put)
	}
	version := put.body["version"].(float64)
	if a := call(t, "GET", leader+"/v1/kv/a", ""); a.code != 200 || a.body["value"] != "1" || a.body["version"] != version {
		t.Fatalf("get: %+v", a)
	}

	// Compare-and-set: a stale version is refused and told the real one.
	if a := call(t, "PUT", leader+"/v1/kv/a", `{"value":"x","if_version":999}`); a.code != 409 || a.body["reason"] != "version_mismatch" || a.body["version"] != version {
		t.Fatalf("stale compare-and-set: %+v", a)
	}
	if a := call(t, "PUT", leader+"/v1/kv/a", fmt.Sprintf(`{"value":"2","if_version":%d}`, int(version))); a.code != 200 {
		t.Fatalf("compare-and-set: %+v", a)
	}
	if a := call(t, "PUT", leader+"/v1/kv/a", `{"value":"again","if_version":0}`); a.code != 409 {
		t.Fatalf("create-if-absent on an existing key: %+v", a)
	}

	if a := call(t, "DELETE", leader+"/v1/kv/a?if_version=1", ""); a.code != 409 {
		t.Fatalf("conditional delete on a stale version: %+v", a)
	}
	if a := call(t, "DELETE", leader+"/v1/kv/a", ""); a.code != 200 {
		t.Fatalf("delete: %+v", a)
	}
	if a := call(t, "DELETE", leader+"/v1/kv/a", ""); a.code != 404 {
		t.Fatalf("delete missing: %+v", a)
	}

	if a := call(t, "PUT", leader+"/v1/kv/a", `not json`); a.code != 400 || a.body["reason"] != "invalid" {
		t.Fatalf("bad body: %+v", a)
	}
	if a := call(t, "DELETE", leader+"/v1/kv/a?if_version=-1", ""); a.code != 400 {
		t.Fatalf("bad if_version: %+v", a)
	}
}

func TestFollowerPointsAtLeader(t *testing.T) {
	members := cluster(t, 3)
	leader := leaderURL(t, members)
	for _, m := range members {
		if m.api.URL == leader {
			continue
		}
		// A follower learns who leads from the first heartbeat.
		var a answer
		for range 100 {
			if a = call(t, "PUT", m.api.URL+"/v1/kv/a", `{"value":"1"}`); a.code == 421 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if a.code != 421 || a.body["reason"] != "not_leader" || a.body["leader"] != leader {
			t.Fatalf("follower answered %+v, want 421 not_leader with hint %s", a, leader)
		}
	}
}

func TestSurvivesLeaderLoss(t *testing.T) {
	members := cluster(t, 3)
	leader := leaderURL(t, members)
	if a := call(t, "PUT", leader+"/v1/kv/a", `{"value":"kept"}`); a.code != 200 {
		t.Fatalf("put: %+v", a)
	}
	for id, m := range members {
		if m.api.URL == leader {
			m.cancel() // stop the Leader's core
			m.tr.Close()
			m.api.Close()
			delete(members, id)
		}
	}
	next := leaderURL(t, members)
	if a := call(t, "GET", next+"/v1/kv/a", ""); a.code != 200 || a.body["value"] != "kept" {
		t.Fatalf("after losing the Leader: %+v, want the Acknowledged write", a)
	}
}

func TestSessionMakesARetrySafe(t *testing.T) {
	members := cluster(t, 3)
	leader := leaderURL(t, members)

	open := call(t, "POST", leader+"/v1/sessions", "")
	if open.code != 200 || open.body["session"] == nil {
		t.Fatalf("open session: %+v", open)
	}
	sid := fmt.Sprint(int(open.body["session"].(float64)))

	// The same request twice: create "a" only if it doesn't exist.
	body := `{"value":"once","if_version":0}`
	first := call(t, "PUT", leader+"/v1/kv/a", body, "Session-Id", sid, "Request-Seq", "1")
	retry := call(t, "PUT", leader+"/v1/kv/a", body, "Session-Id", sid, "Request-Seq", "1")
	if first.code != 200 || retry.code != 200 || first.body["version"] != retry.body["version"] {
		t.Fatalf("first %+v, retry %+v: the retry should get the first answer", first, retry)
	}
	// Without a Session the repeat is a new request, and fails.
	if a := call(t, "PUT", leader+"/v1/kv/a", body); a.code != 409 {
		t.Fatalf("repeat without a Session: %+v, want 409", a)
	}

	if a := call(t, "PUT", leader+"/v1/kv/b", `{"value":"x"}`, "Session-Id", "424242", "Request-Seq", "1"); a.code != 410 || a.body["reason"] != "session_expired" {
		t.Fatalf("unknown Session: %+v", a)
	}
	if a := call(t, "PUT", leader+"/v1/kv/b", `{"value":"x"}`, "Session-Id", sid); a.code != 400 {
		t.Fatalf("Session-Id without Request-Seq: %+v", a)
	}
}

// The client API behaves the same whether gets go through the Log (Rung 1)
// or the read index.
func TestClientAPIWithReadsThroughLog(t *testing.T) {
	members := clusterReading(t, 3, raft.ReadsThroughLog)
	leader := leaderURL(t, members)
	if a := call(t, "PUT", leader+"/v1/kv/a", `{"value":"1"}`); a.code != 200 {
		t.Fatalf("put: %+v", a)
	}
	if a := call(t, "GET", leader+"/v1/kv/a", ""); a.code != 200 || a.body["value"] != "1" {
		t.Fatalf("get: %+v", a)
	}
}

func TestReadIndexFollowerRedirectsGets(t *testing.T) {
	members := cluster(t, 3)
	leader := leaderURL(t, members)
	if a := call(t, "PUT", leader+"/v1/kv/a", `{"value":"1"}`); a.code != 200 {
		t.Fatalf("put: %+v", a)
	}
	for _, m := range members {
		if m.api.URL == leader {
			continue
		}
		if a := call(t, "GET", m.api.URL+"/v1/kv/a", ""); a.code != 421 || a.body["leader"] != leader {
			t.Fatalf("a follower answered a get with %+v, want 421 and a hint", a)
		}
	}
}

func decode(t *testing.T, resp *http.Response, into any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatal(err)
	}
}
