package sim

import (
	"fmt"

	"distributed-kv-store/internal/check"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
)

// Workload is a set of simulated clients that read and write a few keys and
// record everything they see in a History. In Rung 1 a client never retries:
// a request with no definite answer is recorded as lost and the client moves
// on (A§6.3).
//
// Each client sits next to one Member, its home, and shares that Member's
// view of the network: during a Partition it reaches only the Members its
// home can reach.
type Workload struct {
	Clients int
	Keys    int   // keys are "k0".."k<Keys-1>"; a small number forces contention
	Timeout int64 // how long a client waits for a reply before giving up
	Think   int64 // a client pauses 1..Think units between requests
}

// DefaultWorkload is enough contention to make ordering mistakes visible.
var DefaultWorkload = Workload{Clients: 4, Keys: 3, Timeout: 300, Think: 40}

type client struct {
	id     int
	home   core.NodeID
	leader core.NodeID       // where this client currently sends requests
	seen   map[string]uint64 // last version this client observed per key
	count  int
}

// Start launches the clients. They stop issuing requests at time until.
func (w Workload) Start(s *Sim, h *check.History, until int64) {
	ids := s.IDs()
	for i := range w.Clients {
		c := &client{id: i, home: ids[i%len(ids)], leader: ids[s.Rand().IntN(len(ids))], seen: map[string]uint64{}}
		s.After(1+s.Rand().Int64N(w.Think), func() { w.next(s, h, c, until) })
	}
}

func (w Workload) next(s *Sim, h *check.History, c *client, until int64) {
	if s.Now() >= until {
		return
	}
	cmd := w.pick(s, c)
	id := h.Begin(c.id, cmd, s.Now())
	settled := false
	// finish records the request's ending once, and schedules the next one.
	finish := func(result check.Result, resp fsm.Response) {
		if settled {
			return
		}
		settled = true
		h.End(id, result, resp, s.Now())
		s.After(1+s.Rand().Int64N(w.Think), func() { w.next(s, h, c, until) })
	}
	elsewhere := func() core.NodeID {
		ids := s.IDs()
		return ids[s.Rand().IntN(len(ids))]
	}

	target := c.leader
	if !s.Reachable(c.home, target) {
		// The connection can't be made, so the request definitely had no
		// effect.
		c.leader = elsewhere()
		s.After(5, func() { finish(check.Rejected, fsm.Response{}) })
		return
	}
	s.After(w.Timeout, func() {
		if !settled {
			c.leader = elsewhere()
			finish(check.Lost, fsm.Response{})
		}
	})
	s.Propose(target, cmd.Encode(), func(r Reply) {
		if settled || !s.Reachable(target, c.home) {
			return // the reply can't get back; the client will time out
		}
		switch {
		case r.Refused:
			c.leader = elsewhere()
			finish(check.Rejected, fsm.Response{})
		case r.Reason == core.OK:
			resp, err := fsm.DecodeResponse(r.Response)
			if err != nil {
				panic(fmt.Sprintf("sim: undecodable response: %v", err))
			}
			c.observe(cmd, resp)
			finish(check.Answered, resp)
		case r.Reason == core.NotLeader || r.Reason == core.NoMajority:
			if c.leader = r.Leader; c.leader == 0 {
				c.leader = elsewhere()
			}
			finish(check.Rejected, fsm.Response{})
		default: // core.Unknown
			c.leader = elsewhere()
			finish(check.Lost, fsm.Response{})
		}
	})
}

// pick chooses the client's next request: mostly reads and writes, with
// compare-and-sets against the version the client last saw.
func (w Workload) pick(s *Sim, c *client) fsm.Command {
	key := fmt.Sprintf("k%d", s.Rand().IntN(w.Keys))
	c.count++
	value := []byte(fmt.Sprintf("c%d-%d", c.id, c.count))
	switch roll := s.Rand().IntN(100); {
	case roll < 35:
		return fsm.Command{Op: fsm.OpGet, Key: key}
	case roll < 65:
		return fsm.Command{Op: fsm.OpPut, Key: key, Value: value}
	case roll < 90:
		return fsm.Command{Op: fsm.OpPut, Key: key, Value: value, Conditional: true, IfVersion: c.seen[key]}
	default:
		return fsm.Command{Op: fsm.OpDelete, Key: key}
	}
}

func (c *client) observe(cmd fsm.Command, resp fsm.Response) {
	switch resp.Status {
	case fsm.StatusOK, fsm.StatusVersionMismatch:
		c.seen[cmd.Key] = resp.Version
	case fsm.StatusNotFound:
		c.seen[cmd.Key] = 0
	}
}
