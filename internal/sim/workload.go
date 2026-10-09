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

	// ReadsBypassLog sends gets as core.Read events instead of proposals, so
	// the core's read path answers them (A§6.2).
	ReadsBypassLog bool
	// Retry makes a client send a request again, to another Member, when it
	// gets no definite answer or is turned away, until it is answered or
	// four Timeouts have passed. The History records one request, from the
	// first attempt to the final answer.
	Retry bool
	// Sessions makes each client open a Session first and number its
	// requests within it, so the store can recognise a retry (A§6.3).
	Sessions bool
	// TTLPercent of puts carry a time-to-live of TTL[0]..TTL[1] units
	// (A§6.7).
	TTLPercent int
	TTL        [2]int64
}

// DefaultWorkload is enough contention to make ordering mistakes visible.
var DefaultWorkload = Workload{Clients: 4, Keys: 3, Timeout: 300, Think: 40}

type client struct {
	id     int
	home   core.NodeID
	leader core.NodeID       // where this client currently sends requests
	seen   map[string]uint64 // last version this client observed per key
	count  int
	// session and seq identify this client's requests when Sessions is on.
	session uint64
	seq     uint64
}

// Start launches the clients. They stop issuing requests at time until.
func (w Workload) Start(s *Sim, h *check.History, until int64) {
	ids := s.IDs()
	for i := range w.Clients {
		c := &client{id: i, home: ids[i%len(ids)], leader: ids[s.Rand().IntN(len(ids))], seen: map[string]uint64{}}
		start := func() { w.next(s, h, c, until) }
		if w.Sessions {
			start = func() { w.openSession(s, c, func() { w.next(s, h, c, until) }) }
		}
		s.After(1+s.Rand().Int64N(w.Think), start)
	}
}

// openSession keeps trying until some Leader has registered a Session for
// the client, then calls then.
func (w Workload) openSession(s *Sim, c *client, then func()) {
	ids := s.IDs()
	done := false
	var attempt func()
	attempt = func() {
		if done {
			return
		}
		target := c.leader
		over := false
		move := func(to core.NodeID) {
			if over {
				return
			}
			over = true
			if c.leader = to; to == 0 {
				c.leader = ids[s.Rand().IntN(len(ids))]
			}
			s.After(10+s.Rand().Int64N(20), attempt)
		}
		if !s.Reachable(c.home, target) {
			move(0)
			return
		}
		s.After(w.Timeout, func() { move(0) })
		s.Propose(target, fsm.Command{Op: fsm.OpOpenSession}.Encode(), func(r Reply) {
			if over || !s.Reachable(target, c.home) {
				return
			}
			if r.Refused || r.Reason != core.OK {
				move(r.Leader)
				return
			}
			resp, err := fsm.DecodeResponse(r.Response)
			if err != nil || resp.Session == 0 {
				panic(fmt.Sprintf("sim: bad answer to OpOpenSession: %+v, %v", resp, err))
			}
			over, done = true, true
			c.session = resp.Session
			then()
		})
	}
	attempt()
}

// dispatch sends cmd to a Member by the path the Workload is configured for.
func (w Workload) dispatch(s *Sim, to core.NodeID, cmd fsm.Command, done func(Reply)) {
	if w.ReadsBypassLog && cmd.Op == fsm.OpGet {
		s.Read(to, cmd.Encode(), done)
		return
	}
	s.Propose(to, cmd.Encode(), done)
}

func (w Workload) next(s *Sim, h *check.History, c *client, until int64) {
	if s.Now() >= until {
		return
	}
	if w.Sessions && c.session == 0 {
		// The store no longer knows this client's Session: start a new one.
		w.openSession(s, c, func() { w.next(s, h, c, until) })
		return
	}
	if w.Retry {
		w.nextRetrying(s, h, c, until)
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
	w.dispatch(s, target, cmd, func(r Reply) {
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

// pick chooses the client's next request and, if the client has a Session,
// gives it the next request number.
func (w Workload) pick(s *Sim, c *client) fsm.Command {
	cmd := w.choose(s, c)
	if c.session != 0 {
		c.seq++
		cmd.Session, cmd.Seq = c.session, c.seq
	}
	return cmd
}

// choose picks mostly reads and writes, with compare-and-sets against the
// version the client last saw.
func (w Workload) choose(s *Sim, c *client) fsm.Command {
	key := fmt.Sprintf("k%d", s.Rand().IntN(w.Keys))
	c.count++
	value := []byte(fmt.Sprintf("c%d-%d", c.id, c.count))
	var cmd fsm.Command
	switch roll := s.Rand().IntN(100); {
	case roll < 35:
		return fsm.Command{Op: fsm.OpGet, Key: key}
	case roll < 65:
		cmd = fsm.Command{Op: fsm.OpPut, Key: key, Value: value}
	case roll < 90:
		cmd = fsm.Command{Op: fsm.OpPut, Key: key, Value: value, Conditional: true, IfVersion: c.seen[key]}
	default:
		return fsm.Command{Op: fsm.OpDelete, Key: key}
	}
	if w.TTLPercent > 0 && s.Rand().IntN(100) < w.TTLPercent {
		cmd.TTL = w.TTL[0] + s.Rand().Int64N(w.TTL[1]-w.TTL[0]+1)
	}
	return cmd
}

func (c *client) observe(cmd fsm.Command, resp fsm.Response) {
	switch resp.Status {
	case fsm.StatusOK, fsm.StatusVersionMismatch:
		c.seen[cmd.Key] = resp.Version
	case fsm.StatusNotFound:
		c.seen[cmd.Key] = 0
	}
}

// nextRetrying is next for a client that retries. Each attempt gets Timeout
// to produce an answer; the client gives up, recording the request as lost,
// four Timeouts after the first attempt.
func (w Workload) nextRetrying(s *Sim, h *check.History, c *client, until int64) {
	cmd := w.pick(s, c)
	id := h.Begin(c.id, cmd, s.Now())
	giveUp := s.Now() + 4*w.Timeout
	settled := false
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

	var attempt func()
	again := func() { s.After(10+s.Rand().Int64N(20), attempt) }
	attempt = func() {
		if settled {
			return
		}
		if s.Now() >= giveUp {
			finish(check.Lost, fsm.Response{})
			return
		}
		target := c.leader
		if !s.Reachable(c.home, target) {
			c.leader = elsewhere()
			again()
			return
		}
		over := false // this attempt has ended, one way or another
		s.After(w.Timeout, func() {
			if !over && !settled {
				over = true
				c.leader = elsewhere()
				attempt()
			}
		})
		w.dispatch(s, target, cmd, func(r Reply) {
			if over || settled || !s.Reachable(target, c.home) {
				return // too late, or the reply can't get back
			}
			over = true
			switch {
			case !r.Refused && r.Reason == core.OK:
				resp, err := fsm.DecodeResponse(r.Response)
				if err != nil {
					panic(fmt.Sprintf("sim: undecodable response: %v", err))
				}
				if resp.Status == fsm.StatusSessionExpired {
					// This attempt changed nothing, but an earlier one may
					// have, before the store lost the Session.
					c.session, c.seq = 0, 0
					finish(check.Lost, fsm.Response{})
					return
				}
				c.observe(cmd, resp)
				finish(check.Answered, resp)
			case !r.Refused && r.Reason == core.NotLeader && r.Leader != 0:
				c.leader = r.Leader
				again()
			default: // refused, no Leader known, or outcome unknown
				c.leader = elsewhere()
				again()
			}
		})
	}
	attempt()
}
