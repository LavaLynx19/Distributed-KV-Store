// Command kvbench drives a running Group over HTTP, as the Simulation's
// Workload does in-process: clients read and write a set of keys, record a
// History, and the run ends with the three verdicts of A§8.2.
//
//	kvbench -nodes http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003 \
//	  -clients 8 -duration 10s
//
// It exits 1 if the History isn't Linearizable or Members end with different
// data, and 0 otherwise.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"distributed-kv-store/internal/check"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/server"
)

type config struct {
	nodes    []string
	clients  int
	keys     int
	duration time.Duration
	timeout  time.Duration
	mark     time.Duration
	settle   time.Duration
	checkFor time.Duration
	seed     uint64
	retry    bool
	readPct  int
	ttlPct   int
	ttl      time.Duration
}

func main() {
	var cfg config
	nodes := flag.String("nodes", "", "client API URLs of every Member, comma-separated")
	flag.IntVar(&cfg.clients, "clients", 8, "concurrent clients, each sending one request at a time")
	flag.IntVar(&cfg.keys, "keys", 50, "number of distinct keys")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "how long clients run")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Second, "how long a client waits for one answer")
	flag.DurationVar(&cfg.mark, "mark", 0, "report the longest pause in successful writes after this point (set it to just before a Fault is injected)")
	flag.DurationVar(&cfg.settle, "settle", 15*time.Second, "how long to wait after the load for Members to converge")
	flag.DurationVar(&cfg.checkFor, "check", time.Minute, "time limit for the linearizability check (0 skips it)")
	flag.Uint64Var(&cfg.seed, "seed", 1, "seed for the clients' choices")
	flag.IntVar(&cfg.readPct, "read-pct", 35, "percentage of requests that are gets; the rest keep the default write mix")
	flag.IntVar(&cfg.ttlPct, "ttl-pct", 0, "percentage of puts that carry a time-to-live (A§6.7)")
	flag.DurationVar(&cfg.ttl, "ttl", 200*time.Millisecond, "the longest time-to-live given; each is between a quarter of this and all of it")
	flag.BoolVar(&cfg.retry, "retry", false, "open a Session per client and retry requests that get no definite answer (A§6.3)")
	flag.Parse()
	for _, n := range strings.Split(*nodes, ",") {
		if n = strings.TrimSpace(n); n != "" {
			cfg.nodes = append(cfg.nodes, strings.TrimRight(n, "/"))
		}
	}
	if len(cfg.nodes) == 0 {
		log.Fatal("kvbench: -nodes is required")
	}
	if !run(cfg) {
		os.Exit(1)
	}
}

// recorder guards a History shared by the client goroutines.
type recorder struct {
	mu        sync.Mutex
	start     time.Time
	history   check.History
	latencies []time.Duration // of answered requests
	reads     []time.Duration // of answered gets
	writes    []time.Duration // of answered puts and deletes
}

func (r *recorder) begin(client int, cmd fsm.Command) (int, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	return r.history.Begin(client, cmd, int64(now.Sub(r.start))), now
}

func (r *recorder) end(id int, began time.Time, isRead bool, result check.Result, resp fsm.Response) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.history.End(id, result, resp, int64(now.Sub(r.start)))
	if result == check.Answered {
		r.latencies = append(r.latencies, now.Sub(began))
		if isRead {
			r.reads = append(r.reads, now.Sub(began))
		} else {
			r.writes = append(r.writes, now.Sub(began))
		}
	}
}

func run(cfg config) bool {
	rec := &recorder{start: time.Now()}
	httpc := &http.Client{Timeout: cfg.timeout, Transport: &http.Transport{MaxIdleConnsPerHost: cfg.clients}}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := range cfg.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &client{
				id: i, cfg: cfg, http: httpc, rec: rec,
				rng:    rand.New(rand.NewPCG(cfg.seed, uint64(i))),
				target: cfg.nodes[i%len(cfg.nodes)],
				seen:   map[string]uint64{},
			}
			for ctx.Err() == nil {
				// A client opens a Session first, and again whenever the
				// store has removed the one it had.
				if cfg.retry && c.session == 0 && !c.openSession(ctx) {
					return
				}
				c.request()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(rec.start)

	sig := rec.history.Signals
	total := sig.Answered + sig.Rejected + sig.Lost
	fmt.Printf("requests:    %d in %.1fs (answered %d, rejected %d, lost %d)\n", total, elapsed.Seconds(), sig.Answered, sig.Rejected, sig.Lost)
	fmt.Printf("throughput:  %.0f answered/s with %d clients\n", float64(sig.Answered)/elapsed.Seconds(), cfg.clients)
	report := func(label string, ds []time.Duration) {
		slices.Sort(ds)
		n := len(ds)
		if n == 0 {
			return
		}
		pct := func(p float64) time.Duration { return ds[min(n-1, int(p*float64(n)))] }
		fmt.Printf("%-12s p50 %s  p95 %s  p99 %s  max %s  (%.0f/s)\n", label, round(pct(0.50)), round(pct(0.95)), round(pct(0.99)), round(ds[n-1]), float64(n)/elapsed.Seconds())
	}
	report("latency:", rec.latencies)
	report("  reads:", rec.reads)
	report("  writes:", rec.writes)
	if cfg.mark > 0 {
		if pause := sig.LongestPauseAfter(int64(cfg.mark)); pause >= 0 {
			fmt.Printf("recovery:    longest pause in successful writes after the mark at %s: %s\n", cfg.mark, round(time.Duration(pause)))
		} else {
			fmt.Printf("recovery:    no write succeeded after the mark at %s\n", cfg.mark)
		}
	}

	ok := true
	if cfg.checkFor > 0 {
		v := rec.history.Linearizable(cfg.checkFor)
		switch {
		case !v.Linearizable:
			ok = false
			fmt.Println("linearizable: NO")
			if err := v.Visualize("kvbench-history.html"); err == nil {
				fmt.Println("             timeline written to kvbench-history.html")
			}
		case v.TimedOut:
			fmt.Printf("linearizable: no violation found in %s (check not finished)\n", cfg.checkFor)
		default:
			fmt.Println("linearizable: yes")
		}
	}

	// Members that fell behind need a moment to catch up. Divergence only
	// counts if it is still there when the settle time runs out.
	settleBy := time.Now().Add(cfg.settle)
	diffs, unreachable := compare(httpc, cfg.nodes)
	for (len(diffs) > 0 || unreachable > 0) && time.Now().Before(settleBy) {
		time.Sleep(100 * time.Millisecond)
		diffs, unreachable = compare(httpc, cfg.nodes)
	}
	if waited := cfg.settle - time.Until(settleBy); waited > 200*time.Millisecond {
		fmt.Printf("settle:      Members took %s to converge\n", waited.Round(100*time.Millisecond))
	}
	current := members(httpc, cfg.nodes)
	switch {
	case len(diffs) > 0:
		ok = false
		fmt.Printf("end state:   %d differences between Members\n", len(diffs))
		for _, d := range diffs[:min(len(diffs), 5)] {
			fmt.Println("             " + d)
		}
	case unreachable > 0:
		fmt.Printf("end state:   identical on the %d Members that answered (%d unreachable)\n", len(current)-unreachable, unreachable)
	default:
		fmt.Printf("end state:   identical on all %d Members %v\n", len(current), current)
	}
	return ok
}

func round(d time.Duration) time.Duration {
	if d > 10*time.Millisecond {
		return d.Round(100 * time.Microsecond)
	}
	return d.Round(10 * time.Microsecond)
}

type client struct {
	id     int
	cfg    config
	http   *http.Client
	rec    *recorder
	rng    *rand.Rand
	target string            // the Member this client currently talks to
	seen   map[string]uint64 // last version observed per key
	count  int
	// session and seq identify this client's requests when -retry is on.
	session uint64
	seq     uint64
}

// openSession keeps asking until a Leader registers a Session, or ctx ends.
func (c *client) openSession(ctx context.Context) bool {
	for ctx.Err() == nil {
		resp, err := c.http.Post(c.target+"/v1/sessions", "application/json", nil)
		if err == nil {
			var a answer
			raw, _ := io.ReadAll(resp.Body) // a failed read leaves a.Session 0, which retries
			resp.Body.Close()
			_ = json.Unmarshal(raw, &a)
			if resp.StatusCode == http.StatusOK && a.Session != 0 {
				c.session = a.Session
				return true
			}
			if a.Leader != "" && slices.Contains(c.cfg.nodes, a.Leader) {
				c.target = a.Leader
				continue
			}
		}
		c.elsewhere()
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// request sends one command and records how it ended. Without -retry it is
// sent once, like Rung 1's clients. With -retry it is sent again, under the
// same request number, until it gets a definite answer or four timeouts
// have passed; the History records one request either way.
func (c *client) request() {
	cmd := c.pick()
	if c.session != 0 {
		c.seq++
		cmd.Session, cmd.Seq = c.session, c.seq
	}
	id, began := c.rec.begin(c.id, cmd)
	result, resp := c.send(cmd)
	for giveUp := began.Add(4 * c.cfg.timeout); c.cfg.retry && c.session != 0 && result != check.Answered && time.Now().Before(giveUp); {
		time.Sleep(2 * time.Millisecond)
		result, resp = c.send(cmd)
	}
	if c.cfg.retry && result == check.Rejected {
		result = check.Lost // an earlier attempt may have gone through
	}
	c.rec.end(id, began, cmd.Op == fsm.OpGet, result, resp)
	if result == check.Answered {
		switch resp.Status {
		case fsm.StatusOK, fsm.StatusVersionMismatch:
			c.seen[cmd.Key] = resp.Version
		case fsm.StatusNotFound:
			c.seen[cmd.Key] = 0
		}
	}
}

// pick mirrors sim.Workload at the default -read-pct of 35: 35% gets, 30%
// puts, 25% compare-and-sets, 10% deletes. Other values keep the writes in
// the same 30:25:10 proportion.
func (c *client) pick() fsm.Command {
	key := fmt.Sprintf("k%d", c.rng.IntN(c.cfg.keys))
	c.count++
	value := []byte(fmt.Sprintf("c%d-%d", c.id, c.count))
	if c.rng.IntN(100) < c.cfg.readPct {
		return fsm.Command{Op: fsm.OpGet, Key: key}
	}
	var cmd fsm.Command
	switch roll := c.rng.IntN(65); {
	case roll < 30:
		cmd = fsm.Command{Op: fsm.OpPut, Key: key, Value: value}
	case roll < 55:
		cmd = fsm.Command{Op: fsm.OpPut, Key: key, Value: value, Conditional: true, IfVersion: c.seen[key]}
	default:
		return fsm.Command{Op: fsm.OpDelete, Key: key}
	}
	if c.cfg.ttlPct > 0 && c.rng.IntN(100) < c.cfg.ttlPct {
		longest := max(c.cfg.ttl.Milliseconds(), 4)
		cmd.TTL = longest/4 + c.rng.Int64N(longest-longest/4+1)
	}
	return cmd
}

type answer struct {
	Value   string `json:"value"`
	Version uint64 `json:"version"`
	Session uint64 `json:"session"`
	Reason  string `json:"reason"`
	Leader  string `json:"leader"`
}

func (c *client) elsewhere() { c.target = c.cfg.nodes[c.rng.IntN(len(c.cfg.nodes))] }

// send performs cmd against the client's current Member and classifies the
// answer by A§7.2.
func (c *client) send(cmd fsm.Command) (check.Result, fsm.Response) {
	endpoint := c.target + "/v1/kv/" + url.PathEscape(cmd.Key)
	var req *http.Request
	var err error
	switch cmd.Op {
	case fsm.OpGet:
		req, err = http.NewRequest(http.MethodGet, endpoint, nil)
	case fsm.OpPut:
		body := map[string]any{"value": string(cmd.Value)}
		if cmd.Conditional {
			body["if_version"] = cmd.IfVersion
		}
		if cmd.TTL > 0 {
			body["ttl"] = cmd.TTL
		}
		raw, _ := json.Marshal(body) // a map of strings and numbers always encodes
		req, err = http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(raw))
	case fsm.OpDelete:
		req, err = http.NewRequest(http.MethodDelete, endpoint, nil)
	}
	if err != nil {
		log.Fatalf("kvbench: building request: %v", err)
	}

	if cmd.Session != 0 {
		req.Header.Set("Session-Id", strconv.FormatUint(cmd.Session, 10))
		req.Header.Set("Request-Seq", strconv.FormatUint(cmd.Seq, 10))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		c.elsewhere()
		if errors.Is(err, syscall.ECONNREFUSED) {
			return check.Rejected, fsm.Response{} // never reached the store
		}
		return check.Lost, fsm.Response{}
	}
	defer resp.Body.Close()
	var a answer
	raw, err := io.ReadAll(resp.Body)
	if err == nil {
		err = json.Unmarshal(raw, &a)
	}
	if err != nil {
		c.elsewhere()
		return check.Lost, fsm.Response{}
	}

	switch resp.StatusCode {
	case http.StatusOK:
		r := fsm.Response{Status: fsm.StatusOK, Version: a.Version}
		if cmd.Op == fsm.OpGet && a.Value != "" {
			r.Value = []byte(a.Value)
		}
		return check.Answered, r
	case http.StatusNotFound:
		return check.Answered, fsm.Response{Status: fsm.StatusNotFound}
	case http.StatusConflict:
		return check.Answered, fsm.Response{Status: fsm.StatusVersionMismatch, Version: a.Version}
	case http.StatusGone:
		// The store removed this client's Session. An earlier attempt may
		// have gone through before it did.
		c.session, c.seq = 0, 0
		return check.Lost, fsm.Response{}
	case http.StatusMisdirectedRequest, http.StatusServiceUnavailable:
		if a.Leader != "" && slices.Contains(c.cfg.nodes, a.Leader) {
			c.target = a.Leader
		} else {
			c.elsewhere()
			time.Sleep(5 * time.Millisecond) // no Leader known yet; don't spin
		}
		return check.Rejected, fsm.Response{}
	}
	c.elsewhere()
	return check.Lost, fsm.Response{} // 504 and anything unexpected: outcome unknown
}

// members asks the Nodes who is in the Group now, preferring the Leader's
// answer. Node i of nodes must have id i+1. If nobody says, every Node is
// taken to be a Member.
func members(httpc *http.Client, nodes []string) []core.NodeID {
	var list []core.NodeID
	for _, n := range nodes {
		resp, err := httpc.Get(n + "/v1/status")
		if err != nil {
			continue
		}
		var st struct {
			Role    string        `json:"role"`
			Members []core.NodeID `json:"members"`
		}
		err = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if err != nil || len(st.Members) == 0 {
			continue
		}
		if st.Role == "leader" {
			return st.Members
		}
		list = st.Members
	}
	if list == nil {
		for i := range nodes {
			list = append(list, core.NodeID(i+1))
		}
	}
	return list
}

// compare fetches every Member's own data and lists the differences. Nodes
// that aren't Members now, Spares and the removed, are left out.
func compare(httpc *http.Client, nodes []string) (diffs []string, unreachable int) {
	items := map[core.NodeID][]fsm.Item{}
	current := members(httpc, nodes)
	for i, n := range nodes {
		if !slices.Contains(current, core.NodeID(i+1)) {
			continue
		}
		resp, err := httpc.Get(n + "/v1/debug/items")
		if err != nil {
			unreachable++
			continue
		}
		var dump []server.ItemJSON
		err = json.NewDecoder(resp.Body).Decode(&dump)
		resp.Body.Close()
		if err != nil {
			unreachable++
			continue
		}
		list := make([]fsm.Item, len(dump))
		for j, it := range dump {
			list[j] = fsm.Item{Key: it.Key, Value: []byte(it.Value), Version: it.Version}
		}
		items[core.NodeID(i+1)] = list
	}
	return check.Diverged(items), unreachable
}
