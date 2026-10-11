package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"
)

// The Leaderless variant (A§13) has a workload and verdicts of its own: its
// values are sets, and nothing about it is Linearizable.

type lcall struct {
	client     int
	op         string // "get", "add" or "remove"
	key, elem  string
	begin, end time.Duration
	// ok: the store said it succeeded. made: the request reached a Node,
	// so that a write may have taken effect whatever was said.
	ok, made bool
	values   []string
}

// runLeaderless loads a store of the Leaderless variant and reports what
// A§13.11 asks for. Each client talks to one Node throughout: what is
// measured is whether the Node a client can reach will serve it.
func runLeaderless(cfg config, elems int, mark, markFor time.Duration) bool {
	httpc := &http.Client{Timeout: cfg.timeout, Transport: &http.Transport{MaxIdleConnsPerHost: cfg.clients}}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()
	start := time.Now()
	var mu sync.Mutex
	var calls []*lcall
	var wg sync.WaitGroup
	for i := range cfg.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(cfg.seed, uint64(i)))
			node := cfg.nodes[i%len(cfg.nodes)]
			for ctx.Err() == nil {
				c := &lcall{client: i, key: fmt.Sprintf("k%d", rng.IntN(cfg.keys)), elem: fmt.Sprintf("e%d", rng.IntN(elems)), begin: time.Since(start)}
				at := node + "/v1/kv/" + url.PathEscape(c.key)
				var req *http.Request
				switch roll := rng.IntN(100); {
				case roll < 40:
					c.op = "get"
					req, _ = http.NewRequest(http.MethodGet, at, nil)
				case roll < 80:
					c.op = "add"
					body, _ := json.Marshal(map[string]string{"value": c.elem})
					req, _ = http.NewRequest(http.MethodPut, at, bytes.NewReader(body))
				default:
					c.op = "remove"
					req, _ = http.NewRequest(http.MethodDelete, at+"?value="+url.QueryEscape(c.elem), nil)
				}
				resp, err := httpc.Do(req)
				if err == nil {
					c.made = true
					var a struct {
						Values []string `json:"values"`
					}
					_ = json.NewDecoder(resp.Body).Decode(&a) // an unreadable body leaves the values empty
					resp.Body.Close()
					c.ok, c.values = resp.StatusCode == http.StatusOK, a.Values
				} else {
					// No answer: the Node is stopped, or slow. A write may
					// have reached it.
					c.made = true
					time.Sleep(5 * time.Millisecond)
				}
				c.end = time.Since(start)
				mu.Lock()
				calls = append(calls, c)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	var answered int
	var lat, reads, writes []time.Duration
	for _, c := range calls {
		if !c.ok {
			continue
		}
		answered++
		lat = append(lat, c.end-c.begin)
		if c.op == "get" {
			reads = append(reads, c.end-c.begin)
		} else {
			writes = append(writes, c.end-c.begin)
		}
	}
	fmt.Printf("requests:    %d in %.1fs (answered %d, not answered %d)\n", len(calls), elapsed.Seconds(), answered, len(calls)-answered)
	fmt.Printf("throughput:  %.0f answered/s with %d clients\n", float64(answered)/elapsed.Seconds(), cfg.clients)
	report := func(label string, ds []time.Duration) {
		slices.Sort(ds)
		if n := len(ds); n > 0 {
			pct := func(p float64) time.Duration { return ds[min(n-1, int(p*float64(n)))] }
			fmt.Printf("%-12s p50 %s  p95 %s  p99 %s  max %s  (%.0f/s)\n", label, round(pct(0.50)), round(pct(0.95)), round(pct(0.99)), round(ds[n-1]), float64(n)/elapsed.Seconds())
		}
	}
	report("latency:", lat)
	report("  reads:", reads)
	report("  writes:", writes)
	if mark > 0 && markFor > 0 {
		// How much of the store's work went on through the Fault: answers
		// per second while it lasted, against before it.
		var before, during int
		for _, c := range calls {
			switch {
			case !c.ok:
			case c.begin < mark:
				before++
			case c.begin < mark+markFor:
				during++
			}
		}
		was, is := float64(before)/mark.Seconds(), float64(during)/markFor.Seconds()
		fmt.Printf("through the Fault: %.0f answered/s for the %s from %s, against %.0f/s before: %.0f%%\n", is, markFor, mark, was, 100*is/was)
	}

	// Converged: every Home Node of every key holds the same, and no Hint
	// is left. Nodes need a moment after the load, and after a Fault.
	type nodeState struct {
		Hints int `json:"hints"`
		Keys  map[string]struct {
			Values []string `json:"values"`
			Canon  string   `json:"canon"`
			Home   bool     `json:"home"`
		} `json:"keys"`
	}
	look := func() (apart []string, final map[string][]string, reached int) {
		final = map[string][]string{}
		canon := map[string]string{}
		for i, n := range cfg.nodes {
			resp, err := httpc.Get(n + "/v1/debug/state")
			if err != nil {
				continue
			}
			var st nodeState
			err = json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if err != nil {
				continue
			}
			reached++
			if st.Hints > 0 {
				apart = append(apart, fmt.Sprintf("Node %d still holds %d Hints", i+1, st.Hints))
			}
			for key, k := range st.Keys {
				if !k.Home {
					continue
				}
				if seen, ok := canon[key]; ok && seen != k.Canon {
					apart = append(apart, fmt.Sprintf("key %s: Home Nodes hold %v and %v", key, final[key], k.Values))
				}
				canon[key] = k.Canon
				for _, v := range k.Values {
					if !slices.Contains(final[key], v) {
						final[key] = append(final[key], v)
					}
				}
			}
		}
		return apart, final, reached
	}
	settleBy := time.Now().Add(cfg.settle)
	apart, final, reached := look()
	for (len(apart) > 0 || reached < len(cfg.nodes)) && time.Now().Before(settleBy) {
		time.Sleep(100 * time.Millisecond)
		apart, final, reached = look()
	}
	if waited := cfg.settle - time.Until(settleBy); waited > 200*time.Millisecond {
		fmt.Printf("settle:      Nodes took %s\n", waited.Round(100*time.Millisecond))
	}
	ok := true
	switch {
	case reached < len(cfg.nodes):
		fmt.Printf("converged:   unknown: %d of %d Nodes answered\n", reached, len(cfg.nodes))
	case len(apart) > 0:
		ok = false
		fmt.Printf("converged:   NO, %d differences\n", len(apart))
		for _, a := range apart[:min(len(apart), 5)] {
			fmt.Println("             " + a)
		}
	default:
		fmt.Printf("converged:   yes: every Home Node of every key holds the same, and no Hint is left\n")
	}

	// Lost and back, by the times of the requests: all a client outside
	// the store has to go by. An add counts as lost if its element is gone
	// and no remove of it ended after the add began; an element as back if
	// it is there, a remove of it was acknowledged, and no add of it ended
	// after that remove began.
	type target struct{ key, elem string }
	adds, removes := map[target][]*lcall{}, map[target][]*lcall{}
	for _, c := range calls {
		switch c.op {
		case "add":
			adds[target{c.key, c.elem}] = append(adds[target{c.key, c.elem}], c)
		case "remove":
			removes[target{c.key, c.elem}] = append(removes[target{c.key, c.elem}], c)
		}
	}
	lost, back, acked := 0, 0, 0
	for t, list := range adds {
		there := slices.Contains(final[t.key], t.elem)
		for _, a := range list {
			if !a.ok {
				continue
			}
			acked++
			if !there && !slices.ContainsFunc(removes[t], func(r *lcall) bool { return r.end > a.begin }) {
				lost++
			}
		}
	}
	for t, list := range removes {
		if !slices.Contains(final[t.key], t.elem) {
			continue
		}
		for _, r := range list {
			if r.ok && !slices.ContainsFunc(adds[t], func(a *lcall) bool { return a.end > r.begin }) {
				back++
			}
		}
	}
	if reached == len(cfg.nodes) {
		fmt.Printf("adds lost:   %d of %d acknowledged\n", lost, acked)
		fmt.Printf("back:        %d removed elements are there with nothing to have added them again\n", back)
		ok = ok && lost == 0 && back == 0
	}
	return ok
}
