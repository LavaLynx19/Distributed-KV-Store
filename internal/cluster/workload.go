package cluster

import (
	"fmt"

	"distributed-kv-store/internal/check"
	"distributed-kv-store/internal/fsm"
)

// Workload is a set of simulated clients of a store with several Groups.
// Each sits next to one Node and asks it, or another it can reach if that
// one is down. Every client works in a Session and retries a request until
// it has a definite answer, as from Rung 2 on (A§6.3).
type Workload struct {
	Clients int
	Keys    int   // keys are "k0".."k<Keys-1>", spread over the Slots by their hash
	Timeout int64 // how long a client waits for one answer
	Think   int64 // a client pauses 1..Think units between requests
	// TTLPercent of puts carry a time-to-live of TTL[0]..TTL[1] units.
	TTLPercent int
	TTL        [2]int64
	// TxnPercent of requests are Transactions over two keys. Many of them
	// span Groups and are refused (A§11.8).
	TxnPercent int
	// ScanPercent of requests are range scans, merged from every Group.
	// The History records each Group's part as a request of its own, since
	// that is the unit the store promises is Linearizable.
	ScanPercent int
	// Skew sends Percent of requests to the keys of one of Sets. With
	// ShiftEvery set, the next set takes over every so many units, so the
	// load shifts from Slot to Slot.
	Skew struct {
		Percent    int
		Sets       [][]string
		ShiftEvery int64
	}
}

// DefaultWorkload spreads a dozen keys over the Slots, few enough that
// clients still collide on them.
var DefaultWorkload = Workload{Clients: 5, Keys: 12, Timeout: 300, Think: 40}

type client struct {
	id      int
	home    int
	seen    map[string]uint64
	count   int
	session uint64
	seq     uint64
}

// Start launches the clients. They stop issuing requests at time until.
func (w Workload) Start(c *Cluster, h *check.History, until int64) {
	for i := range w.Clients {
		cl := &client{id: i, home: 1 + i%c.cfg.Nodes, seen: map[string]uint64{}}
		c.S.After(1+c.S.Rand().Int64N(w.Think), func() { w.next(c, h, cl, until) })
	}
}

// node picks the Node a client asks: its own if that is running, otherwise
// one its own could reach.
func (w Workload) node(c *Cluster, cl *client) int {
	if c.NodeUp(cl.home) {
		return cl.home
	}
	ids := c.NodeIDs()
	for range ids {
		if n := ids[c.S.Rand().IntN(len(ids))]; c.NodeUp(n) && c.reachable(cl.home, n) {
			return n
		}
	}
	return cl.home
}

// openSession asks the Meta Group for a Session id until it gets one.
func (w Workload) openSession(c *Cluster, cl *client, then func()) {
	var attempt func()
	attempt = func() {
		over := false
		again := func() {
			if !over {
				over = true
				c.S.After(10+c.S.Rand().Int64N(20), attempt)
			}
		}
		c.S.After(w.Timeout, again)
		c.Request(w.node(c, cl), fsm.Command{Op: fsm.OpOpenSession}, func(o Outcome, resp fsm.Response) {
			if over {
				return
			}
			if o != Answered || resp.Session == 0 {
				again()
				return
			}
			over = true
			cl.session, cl.seq = resp.Session, 0
			then()
		})
	}
	attempt()
}

func (w Workload) next(c *Cluster, h *check.History, cl *client, until int64) {
	if c.S.Now() >= until {
		return
	}
	if cl.session == 0 {
		w.openSession(c, cl, func() { w.next(c, h, cl, until) })
		return
	}
	if w.ScanPercent > 0 && c.S.Rand().IntN(100) < w.ScanPercent {
		w.scan(c, h, cl, until)
		return
	}
	cmd := w.choose(c, cl)
	cl.seq++
	cmd.Session, cmd.Seq = cl.session, cl.seq
	id := h.Begin(cl.id, cmd, c.S.Now())
	giveUp := c.S.Now() + 4*w.Timeout
	settled := false
	// unknown is set once some attempt may have taken effect. Until then the
	// request may still register the Session with a Group that doesn't know
	// it (A§11.6); after that it must not.
	unknown := false
	finish := func(result check.Result, resp fsm.Response) {
		if settled {
			return
		}
		settled = true
		h.End(id, result, resp, c.S.Now())
		c.S.After(1+c.S.Rand().Int64N(w.Think), func() { w.next(c, h, cl, until) })
	}
	var attempt func()
	again := func() { c.S.After(10+c.S.Rand().Int64N(20), attempt) }
	attempt = func() {
		if settled {
			return
		}
		if c.S.Now() >= giveUp {
			if unknown {
				finish(check.Lost, fsm.Response{})
			} else {
				finish(check.Rejected, fsm.Response{})
			}
			return
		}
		over := false
		c.S.After(w.Timeout, func() {
			if !over && !settled {
				over, unknown = true, true
				attempt()
			}
		})
		try := cmd
		try.Register = !unknown
		c.Request(w.node(c, cl), try, func(o Outcome, resp fsm.Response) {
			if over || settled {
				return
			}
			over = true
			switch {
			case o == Unknown:
				unknown = true
				again()
			case o == Refused && resp.Status == fsm.StatusCrossGroup && unknown:
				// This attempt was refused, but an earlier one may have got
				// through before a Move split the keys between Groups.
				finish(check.Lost, fsm.Response{})
			case o == Refused && resp.Status == fsm.StatusCrossGroup:
				finish(check.Rejected, resp)
			case o == Refused:
				again()
			case resp.Status == fsm.StatusSessionExpired:
				// The Group has no record of the Session, and this attempt
				// wasn't allowed to start one. An earlier attempt may have
				// gone through.
				cl.session = 0
				h.Signals.SessionExpired++
				finish(check.Lost, fsm.Response{})
			default:
				cl.observe(cmd, resp)
				finish(check.Answered, resp)
			}
		})
	}
	attempt()
}

// scan sends one range scan, once. A scan changes nothing, so a failed one
// is simply dropped.
func (w Workload) scan(c *Cluster, h *check.History, cl *client, until int64) {
	rng := c.S.Rand()
	cmd := fsm.Command{Op: fsm.OpScan}
	if rng.IntN(2) == 0 {
		a, b := rng.IntN(w.Keys), rng.IntN(w.Keys+1)
		cmd.Key, cmd.End = fmt.Sprintf("k%d", min(a, b)), fmt.Sprintf("k%d", max(a, b))
	}
	if rng.IntN(3) == 0 {
		cmd.Limit = uint64(1 + rng.IntN(w.Keys))
	}
	start := c.S.Now()
	settled := false
	next := func() {
		if !settled {
			settled = true
			c.S.After(1+rng.Int64N(w.Think), func() { w.next(c, h, cl, until) })
		}
	}
	c.S.After(w.Timeout, next)
	c.Scan(w.node(c, cl), cmd, func(merged fsm.Response, parts []Part, ok bool) {
		if settled {
			return
		}
		if ok {
			for _, p := range parts {
				// Each part is its own request, by a client of its own:
				// the parts overlap in time.
				id := h.Begin(1000*(cl.id+1)+int(p.Group), cmd, start)
				h.End(id, check.Answered, p.Resp, c.S.Now())
			}
			for _, it := range merged.Items {
				cl.seen[it.Key] = it.Version
			}
		}
		next()
	})
}

func (w Workload) choose(c *Cluster, cl *client) fsm.Command {
	rng := c.S.Rand()
	key := fmt.Sprintf("k%d", rng.IntN(w.Keys))
	if w.Skew.Percent > 0 && rng.IntN(100) < w.Skew.Percent {
		set := w.Skew.Sets[0]
		if w.Skew.ShiftEvery > 0 {
			set = w.Skew.Sets[int(c.S.Now()/w.Skew.ShiftEvery)%len(w.Skew.Sets)]
		}
		key = set[rng.IntN(len(set))]
	}
	cl.count++
	value := []byte(fmt.Sprintf("c%d-%d", cl.id, cl.count))
	if w.TxnPercent > 0 && rng.IntN(100) < w.TxnPercent {
		other := fmt.Sprintf("k%d", rng.IntN(w.Keys))
		cmd := fsm.Command{Op: fsm.OpTxn, Writes: []fsm.Write{{Op: fsm.OpPut, Key: key, Value: value}, {Op: fsm.OpPut, Key: other, Value: value}}}
		if rng.IntN(3) != 0 {
			cmd.Conds = []fsm.Cond{{Key: key, Version: cl.seen[key]}}
		}
		return cmd
	}
	var cmd fsm.Command
	switch roll := rng.IntN(100); {
	case roll < 35:
		return fsm.Command{Op: fsm.OpGet, Key: key}
	case roll < 65:
		cmd = fsm.Command{Op: fsm.OpPut, Key: key, Value: value}
	case roll < 90:
		cmd = fsm.Command{Op: fsm.OpPut, Key: key, Value: value, Conditional: true, IfVersion: cl.seen[key]}
	default:
		return fsm.Command{Op: fsm.OpDelete, Key: key}
	}
	if w.TTLPercent > 0 && rng.IntN(100) < w.TTLPercent {
		cmd.TTL = w.TTL[0] + rng.Int64N(w.TTL[1]-w.TTL[0]+1)
	}
	return cmd
}

func (cl *client) observe(cmd fsm.Command, resp fsm.Response) {
	if cmd.Op == fsm.OpTxn {
		switch resp.Status {
		case fsm.StatusOK:
			for _, w := range cmd.Writes {
				cl.seen[w.Key] = resp.Version
			}
		case fsm.StatusVersionMismatch:
			for _, it := range resp.Items {
				cl.seen[it.Key] = it.Version
			}
		}
		return
	}
	switch resp.Status {
	case fsm.StatusOK, fsm.StatusVersionMismatch:
		cl.seen[cmd.Key] = resp.Version
	case fsm.StatusNotFound:
		cl.seen[cmd.Key] = 0
	}
}
