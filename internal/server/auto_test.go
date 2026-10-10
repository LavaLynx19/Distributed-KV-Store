package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"distributed-kv-store/internal/server"
)

// autoNode is one Node of a store that looks after itself, and how to stop
// it and start it again where it was.
type autoNode struct {
	url   string
	stop  func()
	start func()
}

// startAuto runs a store founded with four Nodes, with two data Groups and
// the Meta Group on 3 Nodes each and 8 Slots, plus spares Spares that join
// through Node 2. Dead Nodes are replaced after a wait of 300 ms.
func startAuto(t *testing.T, spares int) map[int]*autoNode {
	t.Helper()
	const founders = 4
	peers, clients := map[int]string{}, map[int]string{}
	for n := 1; n <= founders+spares; n++ {
		for _, addrs := range []map[int]string{peers, clients} {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addrs[n] = ln.Addr().String()
			ln.Close() // taken again when the Node starts
		}
	}
	nodes := map[int]*autoNode{}
	for n := 1; n <= founders+spares; n++ {
		node := &autoNode{url: "http://" + clients[n]}
		data := t.TempDir()
		told := struct{ peers, clients map[int]string }{peers, clients}
		if n > founders {
			told.peers = map[int]string{n: peers[n]}
			told.clients = map[int]string{n: clients[n], 2: clients[2]}
		}
		node.start = func() {
			listen := func(addr string) net.Listener {
				for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
					ln, err := net.Listen("tcp", addr)
					if err == nil {
						return ln
					}
					if time.Now().After(deadline) {
						t.Fatalf("node %d: %v", n, err)
					}
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			store, wait, err := server.OpenStore(ctx, server.StoreConfig{
				Node: n, Nodes: founders, Groups: 2, Replicas: 3, Slots: 8,
				Peers: told.peers, Clients: told.clients, Listener: listen(peers[n]), Data: data,
				Tick: 5 * time.Millisecond, ElectionTicks: 40, HeartbeatTicks: 1,
				SnapshotEvery: 50, Timeout: 2 * time.Second, GossipEvery: 20 * time.Millisecond,
				Auto: true, DeadWait: 300 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			api := httptest.NewUnstartedServer((&server.API{Store: store, Timeout: 2 * time.Second}).Handler())
			api.Listener.Close()
			api.Listener = listen(clients[n])
			api.Start()
			stopped := false
			node.stop = func() {
				if !stopped {
					stopped = true
					api.Close()
					cancel()
					wait()
				}
			}
		}
		node.start()
		nodes[n] = node
		t.Cleanup(func() { node.stop() })
	}
	return nodes
}

// tableAt is the table one Node holds.
func tableAt(t *testing.T, url string) (table struct {
	Groups []struct {
		Group, Add, Remove int
		Members            []int
	}
}) {
	t.Helper()
	resp, err := http.Get(url + "/v1/table")
	if err != nil {
		return table
	}
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(&table)
	return table
}

// hostedAt lists the Groups one Node has replicas of.
func hostedAt(t *testing.T, url string) []int {
	t.Helper()
	var status struct {
		Groups []struct{ Group int }
	}
	resp, err := http.Get(url + "/v1/status")
	if err != nil {
		return []int{-1}
	}
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(&status)
	out := []int{}
	for _, g := range status.Groups {
		out = append(out, g.Group)
	}
	return out
}

// until waits for ok to hold.
func until(t *testing.T, limit time.Duration, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(limit); !ok(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("still not so after %v: %s", limit, what)
		}
	}
}

// On real Nodes: a Node that stops and stays stopped is replaced by the
// Spare in every Group it was in, with nobody asking, and the store keeps
// every key. When the Node comes back it finds its place taken, drops its
// replicas once the Groups confirm it, and hosts nothing (A§11.11).
func TestADeadNodeIsReplacedAndReturnsAsASpare(t *testing.T) {
	nodes := startAuto(t, 1)
	for i := range 16 {
		eventually(t, 200, "PUT", fmt.Sprintf("%s/v1/kv/k%d", nodes[1].url, i), fmt.Sprintf(`{"value":"v%d"}`, i))
	}
	// Node 4 is in both data Groups.
	if in := hostedAt(t, nodes[4].url); !slices.Equal(in, []int{1, 2}) {
		t.Fatalf("node 4 hosts Groups %v, want 1 and 2", in)
	}
	nodes[4].stop()
	settled := func(url string) func() bool {
		return func() bool {
			table := tableAt(t, url)
			if len(table.Groups) != 3 {
				return false
			}
			for _, g := range table.Groups[1:] {
				if g.Add != 0 || slices.Contains(g.Members, 4) || !slices.Contains(g.Members, 5) || len(g.Members) != 3 {
					return false
				}
			}
			return true
		}
	}
	until(t, 30*time.Second, "node 5 in node 4's place in Groups 1 and 2", settled(nodes[1].url))
	if in := hostedAt(t, nodes[5].url); !slices.Equal(in, []int{1, 2}) {
		t.Fatalf("node 5 hosts Groups %v, want 1 and 2", in)
	}
	for i := range 16 {
		if r := eventually(t, 200, "GET", fmt.Sprintf("%s/v1/kv/k%d", nodes[5].url, i), ""); r.body["value"] != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%d read through node 5: %+v", i, r)
		}
	}
	// Node 5 holds the data itself, not just a route to it.
	var dumps []server.GroupItems
	resp, err := http.Get(nodes[5].url + "/v1/debug/items")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(resp.Body).Decode(&dumps); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	held := 0
	for _, d := range dumps {
		held += len(d.Items)
	}
	if held != 16 {
		t.Fatalf("node 5 holds %d keys in its replicas, want 16", held)
	}

	nodes[4].start()
	until(t, 30*time.Second, "node 4 holding no replica after its return", func() bool {
		return len(hostedAt(t, nodes[4].url)) == 0
	})
	if !settled(nodes[4].url)() {
		t.Fatalf("node 4's table after its return: %+v", tableAt(t, nodes[4].url))
	}
	// It is restarted once more, and starts nothing it dropped.
	nodes[4].stop()
	nodes[4].start()
	time.Sleep(500 * time.Millisecond)
	if in := hostedAt(t, nodes[4].url); len(in) != 0 {
		t.Fatalf("after another restart node 4 hosts Groups %v", in)
	}
	if r := eventually(t, 200, "GET", nodes[4].url+"/v1/kv/k3", ""); r.body["value"] != "v3" || !strings.HasPrefix(nodes[4].url, "http://") {
		t.Fatalf("k3 read through node 4: %+v", r)
	}
}
