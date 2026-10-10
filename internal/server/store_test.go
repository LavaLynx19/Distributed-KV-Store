package server_test

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"
	"time"

	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/shard"
)

// startStore runs a store of nodes Nodes with two data Groups and the Meta
// Group, each on 3 Nodes, with 8 Slots, and returns each Node's URL.
func startStore(t *testing.T, nodes int) map[int]string {
	t.Helper()
	listeners, peers, clients := map[int]net.Listener{}, map[int]string{}, map[int]string{}
	apis := map[int]*httptest.Server{}
	for n := 1; n <= nodes; n++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[n], peers[n] = ln, ln.Addr().String()
		// The client address must be known before the Nodes start, so the
		// server is made first and given its handler afterwards.
		apis[n] = httptest.NewUnstartedServer(nil)
		clients[n] = apis[n].Listener.Addr().String()
	}
	urls := map[int]string{}
	for n := 1; n <= nodes; n++ {
		ctx, cancel := context.WithCancel(context.Background())
		store, stop, err := server.OpenStore(ctx, server.StoreConfig{
			Node: n, Nodes: nodes, Groups: 2, Replicas: 3, Slots: 8,
			Peers: peers, Clients: clients, Listener: listeners[n], Data: t.TempDir(),
			Tick: 5 * time.Millisecond, ElectionTicks: 40, HeartbeatTicks: 1,
			SnapshotEvery: 50, Timeout: 2 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		api := &server.API{Store: store, Timeout: 2 * time.Second}
		apis[n].Config.Handler = api.Handler()
		apis[n].Start()
		urls[n] = apis[n].URL
		t.Cleanup(func() { apis[n].Close(); cancel(); stop() })
	}
	return urls
}

// eventually repeats a request until it gets the wanted status code.
func eventually(t *testing.T, want int, method, url, body string, headers ...string) answer {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		a := call(t, method, url, body, headers...)
		if a.code == want {
			return a
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s: still %+v after 5s, want %d", method, url, a, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// keyInSlot returns the n-th key k<i> in slot, of 8.
func keyInSlot(slot shard.Slot, n int) string {
	for i := 0; ; i++ {
		if k := fmt.Sprintf("k%d", i); shard.SlotOf(k, 8) == slot {
			if n == 0 {
				return k
			}
			n--
		}
	}
}

func TestStoreRoutesMovesAndRefuses(t *testing.T) {
	urls := startStore(t, 4)
	// Any Node takes any key: some of these are forwarded.
	var keys []string
	for i := range 16 {
		key := "k" + strconv.Itoa(i)
		keys = append(keys, key)
		eventually(t, 200, "PUT", urls[1+i%4]+"/v1/kv/"+key, `{"value":"`+key+`"}`)
	}
	for i, key := range keys {
		if a := eventually(t, 200, "GET", urls[1+(i+1)%4]+"/v1/kv/"+key, ""); a.body["value"] != key {
			t.Fatalf("get %s: %+v", key, a)
		}
	}
	// A scan is merged from both Groups, in key order.
	scan := eventually(t, 200, "GET", urls[2]+"/v1/kv", "")
	var got []string
	for _, it := range scan.body["items"].([]any) {
		got = append(got, it.(map[string]any)["key"].(string))
	}
	slices.Sort(keys)
	if !slices.Equal(got, keys) {
		t.Fatalf("scan gave %v, want %v", got, keys)
	}

	// Slot 0 starts in Group 1 and Slot 1 in Group 2. A Transaction over
	// both is refused; one inside a Group works.
	a, b := keyInSlot(0, 0), keyInSlot(1, 0)
	across := fmt.Sprintf(`{"writes":[{"op":"put","key":%q,"value":"x"},{"op":"put","key":%q,"value":"x"}]}`, a, b)
	if r := eventually(t, 400, "POST", urls[1]+"/v1/txn", across); r.body["reason"] != "cross_group" {
		t.Fatalf("a Transaction across Groups: %+v", r)
	}
	within := fmt.Sprintf(`{"writes":[{"op":"put","key":%q,"value":"x"},{"op":"put","key":%q,"value":"x"}]}`, a, keyInSlot(2, 0))
	eventually(t, 200, "POST", urls[1]+"/v1/txn", within)

	// Move Slot 0 to Group 2. Its keys stay readable, and afterwards the
	// two keys are in one Group, so the Transaction is taken.
	before := eventually(t, 200, "GET", urls[3]+"/v1/kv/"+a, "")
	eventually(t, 200, "POST", urls[4]+"/v1/admin/moves", `{"slot":0,"to":2}`)
	deadline := time.Now().Add(10 * time.Second)
	for {
		table := eventually(t, 200, "GET", urls[1]+"/v1/table", "")
		row := table.body["slots"].([]any)[0].(map[string]any)
		if row["group"] == float64(2) && row["epoch"] == float64(1) && row["moving_to"] == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the Move didn't finish: Slot 0 is %+v", row)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r := eventually(t, 200, "GET", urls[2]+"/v1/kv/"+a, ""); r.body["value"] != "x" {
		t.Fatalf("after the Move, get %s: %+v", a, r)
	}
	after := eventually(t, 200, "PUT", urls[3]+"/v1/kv/"+a, `{"value":"y"}`)
	if after.body["version"].(float64) <= before.body["version"].(float64) {
		t.Fatalf("version %v after the Move, %v before", after.body["version"], before.body["version"])
	}
	eventually(t, 200, "POST", urls[1]+"/v1/txn", across)

	// Refusals of the Move request itself.
	if r := call(t, "POST", urls[1]+"/v1/admin/moves", `{"slot":0,"to":2}`); r.code != 400 {
		t.Fatalf("moving a Slot to the Group that owns it: %+v", r)
	}
	if r := call(t, "POST", urls[1]+"/v1/admin/moves", `{"slot":99,"to":1}`); r.code != 400 {
		t.Fatalf("moving a Slot that doesn't exist: %+v", r)
	}
}

// A Session's id comes from the Meta Group, and a Group that hasn't seen the
// Session takes it from the first request that asks (A§11.6).
func TestStoreSessions(t *testing.T) {
	urls := startStore(t, 3)
	open := eventually(t, 200, "POST", urls[1]+"/v1/sessions", "")
	id := strconv.Itoa(int(open.body["session"].(float64)))
	key := keyInSlot(0, 0)
	session := []string{"Session-Id", id, "Request-Seq", "1"}

	// Not registered, and not asking to be: the Group doesn't know it.
	eventually(t, 410, "PUT", urls[2]+"/v1/kv/"+key, `{"value":"1"}`, session...)
	first := eventually(t, 200, "PUT", urls[2]+"/v1/kv/"+key, `{"value":"1"}`, append(session, "Session-Register", "1")...)
	// A retry, through another Node, is answered as before and not applied.
	retry := eventually(t, 200, "PUT", urls[3]+"/v1/kv/"+key, `{"value":"1"}`, session...)
	if retry.body["version"] != first.body["version"] {
		t.Fatalf("the retry got version %v, the first attempt %v", retry.body["version"], first.body["version"])
	}
}
