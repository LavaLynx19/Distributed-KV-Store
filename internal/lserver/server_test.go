package lserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"distributed-kv-store/internal/leaderless"
	"distributed-kv-store/internal/lserver"
)

type node struct {
	url    string
	server *lserver.Server
	stop   func()
}

type cluster struct {
	t     *testing.T
	peers map[int]string
	dirs  map[int]string
	nodes map[int]*node
}

func (c *cluster) start(id int) {
	c.t.Helper()
	ln, err := net.Listen("tcp", c.peers[id])
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s, err := lserver.Open(ctx, lserver.Config{
		ID: id, Peers: c.peers, Listener: ln, W: 2, R: 2, Mode: leaderless.AddWins,
		DeliverHints: true, Resend: true, Exchange: true, Repair: true,
		Data: c.dirs[id], Tick: 10 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	api := httptest.NewServer((&lserver.API{Server: s, Timeout: 2 * time.Second}).Handler())
	c.nodes[id] = &node{url: api.URL, server: s, stop: func() { api.Close(); cancel(); s.Close() }}
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	c := &cluster{t: t, peers: map[int]string{}, dirs: map[int]string{}, nodes: map[int]*node{}}
	for id := 1; id <= 5; id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.peers[id] = ln.Addr().String()
		ln.Close()
		c.dirs[id] = t.TempDir()
	}
	for id := 1; id <= 5; id++ {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.stop()
		}
	})
	return c
}

func call(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func values(t *testing.T, url, key string) []string {
	t.Helper()
	code, body := call(t, "GET", url+"/v1/kv/"+key, "")
	if code != 200 {
		t.Fatalf("get %s: %d %v", key, code, body)
	}
	var out []string
	for _, v := range body["values"].([]any) {
		out = append(out, v.(string))
	}
	return out
}

func TestRealNodesAddRemoveScanAndConverge(t *testing.T) {
	c := startCluster(t)
	put := func(id int, key, value string) {
		t.Helper()
		if code, body := call(t, "PUT", c.nodes[id].url+"/v1/kv/"+key, fmt.Sprintf(`{"value":%q}`, value)); code != 200 {
			t.Fatalf("add %s to %s through Node %d: %d %v", value, key, id, code, body)
		}
	}
	for i := range 10 {
		put(1+i%5, fmt.Sprintf("k%d", i%3), fmt.Sprintf("v%d", i))
	}
	if got := values(t, c.nodes[4].url, "k0"); !slices.Equal(got, []string{"v0", "v3", "v6", "v9"}) {
		t.Fatalf("k0 = %v", got)
	}
	if code, _ := call(t, "DELETE", c.nodes[2].url+"/v1/kv/k0?value=v3", ""); code != 200 {
		t.Fatalf("remove: %d", code)
	}
	if got := values(t, c.nodes[5].url, "k0"); !slices.Equal(got, []string{"v0", "v6", "v9"}) {
		t.Fatalf("k0 after the remove = %v", got)
	}
	// An empty key is an empty list, not an error; the things this store
	// doesn't have say so.
	if got := values(t, c.nodes[1].url, "nothing"); len(got) != 0 {
		t.Errorf("a key never written = %v", got)
	}
	for _, path := range []string{"/v1/txn", "/v1/sessions"} {
		if code, body := call(t, "POST", c.nodes[1].url+path, "{}"); code != 400 || body["reason"] != "not_offered" {
			t.Errorf("%s: %d %v", path, code, body)
		}
	}
	if code, body := call(t, "PUT", c.nodes[1].url+"/v1/kv/k0", `{"value":"x","if_version":3}`); code != 400 || body["reason"] != "not_offered" {
		t.Errorf("compare-and-set: %d %v", code, body)
	}
	code, body := call(t, "GET", c.nodes[3].url+"/v1/kv?start=k1", "")
	if items, _ := body["items"].([]any); code != 200 || len(items) != 2 {
		t.Fatalf("scan from k1: %d %v", code, body)
	}

	// A Node stops. Writes go on, with another Node standing in for it.
	c.nodes[3].stop()
	for i := range 6 {
		put(1+i%2, fmt.Sprintf("k%d", i%3), fmt.Sprintf("w%d", i))
	}
	// It comes back from its disk and is brought up to date.
	c.start(3)
	deadline := time.Now().Add(10 * time.Second)
	for {
		same, hints := true, 0
		canon := map[string]string{}
		for _, n := range c.nodes {
			v := n.server.State()
			hints += v.Hints
			for key, kv := range v.Keys {
				if !kv.Home {
					continue
				}
				if seen, ok := canon[key]; ok && seen != kv.Canon {
					same = false
				}
				canon[key] = kv.Canon
			}
		}
		if same && hints == 0 && len(c.nodes[3].server.State().Keys) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not Converged after 10 s: same=%v, %d Hints left", same, hints)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := values(t, c.nodes[3].url, "k0"); !slices.Equal(got, []string{"v0", "v6", "v9", "w0", "w3"}) {
		t.Errorf("k0 through the Node that was away = %v", got)
	}
}
