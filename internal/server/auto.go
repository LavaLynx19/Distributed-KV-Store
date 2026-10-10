package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strconv"
	"time"

	"distributed-kv-store/internal/automation"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/gossip"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
)

// This file is a real Node's part in what the store does by itself
// (A§11.11). The decisions are internal/automation's, the same ones the
// Simulation runs; here they are given what this Node knows and their
// answers are sent on their way.

const (
	// autoEvery is how many of the agent's ticks pass between its looks at
	// Members and load.
	autoEvery = 10
	// loadWindow is how often a Node folds what it has counted into its
	// smoothed load, and loadWeight the share of the newest window in it.
	loadWindow = 500 * time.Millisecond
	loadWeight = 0.25
)

// balancing is how a real store evens out load: the lines the Simulation
// settled on, and its waits in the same proportion to the window, in
// milliseconds of Store time.
var balancing = automation.Balancing{High: 1.5, Low: 1.2, Rest: 5_000, Settle: 2_500, Idle: 2 * shard.WriteCost}

// leads reports which Groups this Node's replicas lead.
func (s *Store) leads(ctx context.Context) map[shard.GroupID]bool {
	if !s.Auto {
		return nil
	}
	look, cancel := context.WithTimeout(ctx, s.GossipEvery)
	defer cancel()
	leads := map[shard.GroupID]bool{}
	for g, n := range s.replicas() {
		if st, ok := n.Status(look); ok && st.Role == core.LeaderRole {
			leads[g] = true
		}
	}
	return leads
}

// detectorDead lists the Nodes this Node's gossip gives up for dead, or was
// told have left: either way they are gone. The caller holds mu.
func (s *Store) detectorDead() []int {
	var dead []int
	for _, m := range s.Gossip.Members() {
		if m.Status == gossip.Dead || m.Status == gossip.Left {
			dead = append(dead, m.ID)
		}
	}
	return dead
}

// tell works out what this Node tells the others about itself and puts it
// in its gossip: the Nodes it has thought dead for longer than the wait,
// and the load on the Slots it leads. The caller holds mu.
func (s *Store) tell(leads map[shard.GroupID]bool) {
	if !s.Auto {
		return
	}
	now := time.Now()
	s.report.Dead = s.watch.Held(now.UnixMilli(), s.DeadWait.Milliseconds(), s.detectorDead())
	if now.Sub(s.loadAt) >= loadWindow {
		s.loadAt = now
		counts := make([]uint32, len(s.counts))
		for i := range s.counts {
			counts[i] = s.counts[i].Swap(0)
		}
		s.meter.Fold(counts, loadWeight)
		s.report.Load = s.meter.Load(func(slot int) bool { return leads[s.table.Slots[slot].Group] }, false)
	}
	s.Gossip.SetNote(s.report.Encode())
}

// reports gathers what every Node this one thinks alive last said about
// itself, by Node. The caller holds mu.
func (s *Store) reports() map[int]shard.Report {
	out := map[int]shard.Report{s.Node: s.report}
	for _, m := range s.Gossip.Members() {
		if m.ID != s.Node && m.Status == gossip.Alive {
			out[m.ID], _ = shard.DecodeReport(m.Note)
		}
	}
	return out
}

// heardLoad is the load of each Slot as this Node has heard it.
func (s *Store) heardLoad() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []shard.Report
	for _, r := range s.reports() {
		all = append(all, r)
	}
	return automation.MergeLoad(len(s.table.Slots), all)
}

// automaton is the agent's memory for automation. It is touched only on the
// agent's goroutine.
type automaton struct {
	s *Store
	e storeEnv
	// asking marks the Groups this Node is in the middle of asking whether
	// it may drop its replica of.
	asking map[shard.GroupID]bool
}

// replicasWanted starts the replicas the table wants this Node to have, and
// asks about the ones it has and the table doesn't name it for.
func (a *automaton) replicasWanted(ctx context.Context, table shard.Table) {
	s := a.s
	for g, row := range table.Groups {
		g := shard.GroupID(g)
		_, hosted := s.replica(g)
		member := slices.Contains(row.Members, s.Node)
		switch {
		case hosted && !member && row.Add != s.Node && !a.asking[g]:
			a.asking[g] = true
			go a.askIfGone(ctx, g, row)
		case !hosted && row.Add == s.Node:
			// It joins as a Member of nothing: the Group's Leader adds it.
			var members []core.NodeID
			for _, n := range row.Members {
				if n != s.Node {
					members = append(members, core.NodeID(shard.ReplicaID(n, g)))
				}
			}
			if err := s.setRole(g, "joined"); err == nil {
				err = s.open(g, members)
				log.Printf("store: node %d: started a replica of Group %d to take node %d's place (%v)", s.Node, g, row.Remove, errOrOK(err))
			}
		}
	}
}

func errOrOK(err error) string {
	if err != nil {
		return err.Error()
	}
	return "ok"
}

// changeMembers does what automation.Members says the Leader of Group g,
// here, should do next about its Members.
func (a *automaton) changeMembers(ctx context.Context, g shard.GroupID, n *Node, st core.Status, table shard.Table) {
	if int(g) >= len(table.Groups) {
		return
	}
	step := automation.Members(g, table.Groups[g], st)
	if step.Report != nil {
		go a.s.ask(ctx, shard.Meta, false, false, step.Report.Encode())
	}
	if step.Reconfigure != nil {
		go func() {
			change, cancel := context.WithTimeout(ctx, 30*a.s.Timeout)
			defer cancel()
			n.Reconfigure(change, step.Reconfigure)
		}()
	}
}

// steer is the Meta Leader's part: have a Spare take a dead Node's place,
// and move a Slot off the busiest Group. Both go into the Meta Group's Log
// through its own replica n.
func (a *automaton) steer(ctx context.Context, n *Node, st core.Status) {
	s := a.s
	look, cancel := context.WithTimeout(ctx, s.Timeout)
	var table shard.Table
	ok := n.Inspect(look, func(m Machine) { table = m.(*meta.Machine).Table() })
	cancel()
	if !ok {
		return
	}
	s.mu.Lock()
	reports := s.reports()
	var alive []int
	for _, m := range s.Gossip.Members() {
		if m.Status == gossip.Alive {
			alive = append(alive, m.ID)
		}
	}
	s.mu.Unlock()
	dead := automation.Dead(table.Hosts(shard.Meta), func(n int) (shard.Report, bool) {
		r, has := reports[n]
		return r, has
	})
	if out, in, ok := automation.Replacement(table, dead, alive); ok {
		log.Printf("store: node %d: node %d is dead by a Majority of the Meta Group; asking node %d to take its place", s.Node, out, in)
		go n.Propose(ctx, meta.Command{Op: meta.OpReplace, Out: out, In: in}.Encode())
		return
	}
	healthy := true
	for _, row := range table.Groups {
		for _, m := range row.Members {
			healthy = healthy && slices.Contains(alive, m)
		}
	}
	var all []shard.Report
	for _, r := range reports {
		all = append(all, r)
	}
	load := automation.MergeLoad(len(table.Slots), all)
	if slot, to, ok := s.balancer.Decide(balancing, table, load, s.Groups, st.Term, time.Now().UnixMilli(), healthy); ok {
		sums := automation.GroupLoad(table, load, s.Groups)
		log.Printf("store: node %d: load by Group %v; moving Slot %d (load %d) to Group %d", s.Node, sums[1:], slot, load[slot], to)
		go n.Propose(ctx, meta.Command{Op: meta.OpMove, Slot: slot, To: to}.Encode())
	}
}

// membersReply is what a Node answers when asked for its replica's view of
// a Group's Members.
type membersReply struct {
	// Confirmed is set if the replica leads the Group and confirmed it with
	// a Majority before answering. Otherwise Leader is the Node it thinks
	// leads, or 0.
	Confirmed bool          `json:"confirmed"`
	Leader    int           `json:"leader,omitempty"`
	Members   []core.NodeID `json:"members,omitempty"`
	MembersAt core.Index    `json:"members_at,omitempty"`
	Changing  bool          `json:"changing,omitempty"`
}

// members answers another Node asking who Group g's Members are. Only a
// Leader that has just confirmed it still leads answers with a list.
func (s *Store) members(w http.ResponseWriter, r *http.Request) {
	g, err := strconv.ParseUint(r.PathValue("group"), 10, 32)
	n, hosted := s.replica(shard.GroupID(g))
	if err != nil || !hosted {
		writeJSON(w, http.StatusOK, membersReply{})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	// Any read will do: what matters is that it is answered only by a
	// Leader a Majority still follows (A§6.2).
	reply := n.Read(ctx, fsm.Command{Op: fsm.OpGet, Key: "members"}.Encode())
	hint, _ := shard.SplitReplicaID(uint64(reply.Leader))
	st, ok := n.Status(ctx)
	if reply.Reason != core.OK || !ok || st.Role != core.LeaderRole {
		writeJSON(w, http.StatusOK, membersReply{Leader: hint})
		return
	}
	writeJSON(w, http.StatusOK, membersReply{Confirmed: true, Members: st.Members, MembersAt: st.MembersAt, Changing: st.Changing})
}

// askIfGone is how this Node finds out that a replica it holds is no longer
// wanted. The table only prompts the question: it may be old, or ahead of
// the Group. The Node asks the Group's Leader, and drops the replica if
// automation.Gone says the answer allows it.
func (a *automaton) askIfGone(ctx context.Context, g shard.GroupID, row shard.Group) {
	s := a.s
	defer a.e.back(func() { delete(a.asking, g) })
	ask := func(node int) (membersReply, bool) {
		url := s.clientURL(node)
		if url == "" || node == s.Node {
			return membersReply{}, false
		}
		one, cancel := context.WithTimeout(ctx, s.Timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(one, http.MethodGet, fmt.Sprintf("%s/v1/internal/members/%d", url, g), nil)
		if err != nil {
			return membersReply{}, false
		}
		resp, err := s.httpc.Do(req)
		if err != nil {
			return membersReply{}, false
		}
		defer resp.Body.Close()
		var reply membersReply
		return reply, json.NewDecoder(resp.Body).Decode(&reply) == nil
	}
	var reply membersReply
	for _, node := range row.Members {
		var ok bool
		if reply, ok = ask(node); !ok {
			continue
		}
		if !reply.Confirmed && reply.Leader != 0 {
			reply, _ = ask(reply.Leader)
		}
		if reply.Confirmed {
			break
		}
	}
	if !reply.Confirmed {
		return
	}
	n, hosted := s.replica(g)
	if !hosted {
		return
	}
	mine, ok := n.Status(ctx)
	leader := core.Status{Role: core.LeaderRole, Members: reply.Members, MembersAt: reply.MembersAt, Changing: reply.Changing}
	if !ok || !automation.Gone(core.NodeID(shard.ReplicaID(s.Node, g)), mine, leader) {
		return
	}
	a.e.back(func() {
		err := s.drop(g)
		log.Printf("store: node %d: Group %d no longer has this Node as a Member; dropped its replica (%v)", s.Node, g, errOrOK(err))
	})
}
