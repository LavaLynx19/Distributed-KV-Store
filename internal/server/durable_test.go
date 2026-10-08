package server_test

import (
	"context"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/internal/transport"
)

// durable is a Group of real Members that keep their state in directories
// under dir, and can be stopped and started again.
type durable struct {
	t       *testing.T
	dir     string
	ids     []core.NodeID
	peers   map[core.NodeID]string
	clients map[core.NodeID]string
	stops   map[core.NodeID]func()
	// snapshotEvery is passed to each Member started from now on.
	snapshotEvery int
}

func newDurable(t *testing.T, n int) *durable {
	t.Helper()
	return newDurableSnapshotting(t, n, 0)
}

func newDurableSnapshotting(t *testing.T, n, snapshotEvery int) *durable {
	t.Helper()
	transport.Register(raft.MessageBodies()...)
	d := &durable{t: t, dir: t.TempDir(), snapshotEvery: snapshotEvery, peers: map[core.NodeID]string{}, clients: map[core.NodeID]string{}, stops: map[core.NodeID]func(){}}
	for i := 1; i <= n; i++ {
		id := core.NodeID(i)
		d.ids = append(d.ids, id)
		// Reserve two addresses per Member that it keeps across restarts.
		for _, addrs := range []map[core.NodeID]string{d.peers, d.clients} {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addrs[id] = ln.Addr().String()
			ln.Close()
		}
	}
	for _, id := range d.ids {
		d.start(id)
	}
	t.Cleanup(func() {
		for _, id := range d.ids {
			d.stop(id)
		}
	})
	return d
}

func (d *durable) start(id core.NodeID) {
	d.t.Helper()
	store, stored, err := storage.Open(filepath.Join(d.dir, "node"+strconv.Itoa(int(id))), 0)
	if err != nil {
		d.t.Fatal(err)
	}
	ln, err := net.Listen("tcp", d.peers[id])
	if err != nil {
		d.t.Fatal(err)
	}
	var node *server.Node
	tr := transport.New(id, ln, d.peers, func(m core.Message) { node.Deliver(m) })
	c := raft.New(raft.Config{ID: id, Members: d.ids, ElectionTicks: 10, HeartbeatTicks: 1,
		Rand: rand.New(rand.NewPCG(uint64(id), uint64(time.Now().UnixNano()))), Reads: raft.ReadsByIndex, Stored: stored})
	machine := fsm.New()
	node = server.NewNode(c, machine, tr.Send, 5*time.Millisecond)
	node.Storage = store
	node.SnapshotEvery = d.snapshotEvery
	if snap := stored.Snapshot; snap != nil {
		if err := machine.Restore(snap.Data); err != nil {
			d.t.Fatal(err)
		}
		node.Restored = snap.Index
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { node.Run(ctx); close(done) }()

	hints := map[core.NodeID]string{}
	for m, addr := range d.clients {
		hints[m] = "http://" + addr
	}
	api := &server.API{Node: node, Clients: hints, Timeout: 2 * time.Second, ReadsBypassLog: true}
	cl, err := net.Listen("tcp", d.clients[id])
	if err != nil {
		d.t.Fatal(err)
	}
	srv := &httptest.Server{Listener: cl, Config: &http.Server{Handler: api.Handler()}}
	srv.Start()
	d.stops[id] = func() {
		srv.Close()
		cancel()
		<-done
		tr.Close()
		store.Close()
	}
}

func (d *durable) stop(id core.NodeID) {
	if stop := d.stops[id]; stop != nil {
		stop()
		delete(d.stops, id)
	}
}

// leader waits for a running Member to say it leads.
func (d *durable) leader() string {
	d.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id := range d.stops {
			url := "http://" + d.clients[id]
			resp, err := http.Get(url + "/v1/status")
			if err != nil {
				continue
			}
			resp.Body.Close()
			if a := call(d.t, "GET", url+"/v1/status", ""); a.body["role"] == "leader" {
				return url
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	d.t.Fatal("no Leader within 10s")
	return ""
}

// Every Acknowledged write survives all Members stopping and starting again.
func TestFullRestartKeepsAcknowledgedWrites(t *testing.T) {
	d := newDurable(t, 3)
	leader := d.leader()
	for i := range 20 {
		key := "k" + strconv.Itoa(i)
		if a := call(t, "PUT", leader+"/v1/kv/"+key, `{"value":"v`+strconv.Itoa(i)+`"}`); a.code != 200 {
			t.Fatalf("put %s: %+v", key, a)
		}
	}
	before := call(t, "GET", leader+"/v1/kv/k7", "")

	for _, id := range d.ids {
		d.stop(id)
	}
	for _, id := range d.ids {
		d.start(id)
	}
	leader = d.leader()
	for i := range 20 {
		key := "k" + strconv.Itoa(i)
		if a := call(t, "GET", leader+"/v1/kv/"+key, ""); a.code != 200 || a.body["value"] != "v"+strconv.Itoa(i) {
			t.Fatalf("after a full restart, %s: %+v", key, a)
		}
	}
	// Versions are Log positions, and the Log is the same Log.
	if after := call(t, "GET", leader+"/v1/kv/k7", ""); after.body["version"] != before.body["version"] {
		t.Fatalf("k7 was version %v before the restart and %v after", before.body["version"], after.body["version"])
	}
	// And the Group still takes writes.
	if a := call(t, "PUT", leader+"/v1/kv/new", `{"value":"x"}`); a.code != 200 {
		t.Fatalf("put after restart: %+v", a)
	}
}

// One Member stops, misses writes, and catches up from the Log when it
// returns.
func TestRestartedMemberCatchesUp(t *testing.T) {
	d := newDurable(t, 3)
	leader := d.leader()
	var down core.NodeID
	for _, id := range d.ids {
		if "http://"+d.clients[id] != leader {
			down = id
			break
		}
	}
	d.stop(down)
	for i := range 10 {
		if a := call(t, "PUT", leader+"/v1/kv/k"+strconv.Itoa(i), `{"value":"x"}`); a.code != 200 {
			t.Fatalf("put with one Member down: %+v", a)
		}
	}
	d.start(down)

	url := "http://" + d.clients[down] + "/v1/debug/items"
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			var items []server.ItemJSON
			decode(t, resp, &items)
			if len(items) == 10 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the restarted Member didn't catch up within 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// With Snapshots trimming the Log, a full restart rebuilds each Member from
// its Snapshot plus the Entries after it.
func TestFullRestartFromSnapshots(t *testing.T) {
	d := newDurableSnapshotting(t, 3, 25)
	leader := d.leader()
	for i := range 120 {
		key := "k" + strconv.Itoa(i%30)
		if a := call(t, "PUT", leader+"/v1/kv/"+key, `{"value":"v`+strconv.Itoa(i)+`"}`); a.code != 200 {
			t.Fatalf("put %d: %+v", i, a)
		}
	}
	for _, id := range d.ids {
		d.stop(id)
	}
	// Every Member took Snapshots, and its Log no longer starts at Entry 1.
	for _, id := range d.ids {
		store, stored, err := storage.Open(filepath.Join(d.dir, "node"+strconv.Itoa(int(id))), 0)
		if err != nil {
			t.Fatal(err)
		}
		store.Close()
		if stored.Snapshot == nil || stored.Snapshot.Index < 100 || len(stored.Entries) > 40 {
			t.Fatalf("node %d: Snapshot %+v with %d Entries after it; want a Snapshot past Entry 100 and a short Log", id, stored.Snapshot != nil, len(stored.Entries))
		}
	}
	for _, id := range d.ids {
		d.start(id)
	}
	leader = d.leader()
	for i := 90; i < 120; i++ {
		key := "k" + strconv.Itoa(i%30)
		if a := call(t, "GET", leader+"/v1/kv/"+key, ""); a.code != 200 || a.body["value"] != "v"+strconv.Itoa(i) {
			t.Fatalf("after restarting from Snapshots, %s: %+v", key, a)
		}
	}
}

// A Member is away while the others write enough to trim their Logs past
// where it stopped. When it returns it is caught up by Snapshot, then Log.
func TestReturningMemberIsCaughtUpBySnapshot(t *testing.T) {
	d := newDurableSnapshotting(t, 3, 25)
	leader := d.leader()
	var away core.NodeID
	for _, id := range d.ids {
		if "http://"+d.clients[id] != leader {
			away = id
			break
		}
	}
	if a := call(t, "PUT", leader+"/v1/kv/before", `{"value":"x"}`); a.code != 200 {
		t.Fatalf("put: %+v", a)
	}
	d.stop(away)
	for i := range 100 {
		if a := call(t, "PUT", leader+"/v1/kv/k"+strconv.Itoa(i%10), `{"value":"v`+strconv.Itoa(i)+`"}`); a.code != 200 {
			t.Fatalf("put %d with one Member away: %+v", i, a)
		}
	}
	d.start(away)

	want := map[string]string{"before": "x"}
	for i := 90; i < 100; i++ {
		want["k"+strconv.Itoa(i%10)] = "v" + strconv.Itoa(i)
	}
	url := "http://" + d.clients[away] + "/v1/debug/items"
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := map[string]string{}
		if resp, err := http.Get(url); err == nil {
			var items []server.ItemJSON
			decode(t, resp, &items)
			for _, it := range items {
				got[it.Key] = it.Value
			}
		}
		if len(got) == len(want) {
			for k, v := range want {
				if got[k] != v {
					t.Fatalf("after catching up, %s = %q, want %q", k, got[k], v)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the returning Member holds %d keys after 10s, want %d", len(got), len(want))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// It was sent a Snapshot: its own Log no longer starts at Entry 1.
	d.stop(away)
	store, stored, err := storage.Open(filepath.Join(d.dir, "node"+strconv.Itoa(int(away))), 0)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	if stored.Snapshot == nil || stored.Snapshot.Index < 50 {
		t.Fatalf("expected the returning Member to hold a Snapshot well past where it stopped, got %+v", stored.Snapshot != nil)
	}
}
