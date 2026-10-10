package cluster_test

import (
	"fmt"
	"slices"
	"testing"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
	"distributed-kv-store/internal/gossip"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/transport"
)

func newCore(id core.NodeID, members []core.NodeID, rng core.Rand, stored core.Stored) core.Node {
	return raft.New(raft.Config{ID: id, Members: members, ElectionTicks: 10, HeartbeatTicks: 1, Rand: rng, Reads: raft.ReadsByIndex, Stored: stored})
}

func newCluster(seed uint64) *cluster.Cluster {
	transport.Register(raft.MessageBodies()...)
	return cluster.New(cluster.Config{
		Seed: seed, Nodes: 5, Groups: 3, Replicas: 3, Slots: 8,
		NewCore: newCore, Copy: transport.NewLoopback().Copy,
	})
}

// do sends cmd through Node n, trying again until it is answered.
func do(t *testing.T, c *cluster.Cluster, n int, cmd fsm.Command) fsm.Response {
	t.Helper()
	var got *fsm.Response
	deadline := c.S.Now() + 3000
	for got == nil && c.S.Now() < deadline {
		c.Request(n, cmd, func(o cluster.Outcome, r fsm.Response) {
			if o == cluster.Answered {
				got = &r
			}
		})
		c.S.Run(c.S.Now() + 60)
	}
	if got == nil {
		t.Fatalf("%+v was never answered through node %d", cmd, n)
	}
	return *got
}

func TestRequestsReachTheOwningGroup(t *testing.T) {
	c := newCluster(1)
	c.S.Run(300)
	for i := range 24 {
		key := fmt.Sprintf("k%d", i)
		n := 1 + i%5
		if r := do(t, c, n, fsm.Command{Op: fsm.OpPut, Key: key, Value: []byte(key)}); r.Status != fsm.StatusOK {
			t.Fatalf("put %s through node %d: %+v", key, n, r)
		}
	}
	// Any Node can be asked about any key.
	for i := range 24 {
		key := fmt.Sprintf("k%d", i)
		if r := do(t, c, 1+(i+2)%5, fsm.Command{Op: fsm.OpGet, Key: key}); r.Status != fsm.StatusOK || string(r.Value) != key {
			t.Fatalf("get %s: %+v", key, r)
		}
	}
	if c.Forwarded == 0 {
		t.Fatal("no request was forwarded, though no Node hosts every Group")
	}
}

func TestMoveASlot(t *testing.T) {
	c := newCluster(2)
	c.S.Run(300)
	var inSlot []string
	for i := 0; len(inSlot) < 10; i++ {
		if key := fmt.Sprintf("k%d", i); shard.SlotOf(key, 8) == 0 {
			inSlot = append(inSlot, key)
			do(t, c, 1, fsm.Command{Op: fsm.OpPut, Key: key, Value: []byte("before")})
		}
	}
	before := do(t, c, 2, fsm.Command{Op: fsm.OpGet, Key: inSlot[0]})

	asked := false
	c.Move(3, 0, 2, func(ok bool) { asked = ok })
	c.S.Run(c.S.Now() + 2000)
	if !asked {
		t.Fatal("the Meta Group didn't take the Move")
	}
	for _, n := range c.NodeIDs() {
		if row := c.Table(n).Slots[0]; row.Group != 2 || row.Epoch != 1 || row.MovingTo != 0 || row.MovedAt == 0 {
			t.Fatalf("node %d's table has Slot 0 as %+v, want Group 2 at Epoch 1", n, row)
		}
	}
	for _, key := range inSlot {
		if r := do(t, c, 4, fsm.Command{Op: fsm.OpGet, Key: key}); r.Status != fsm.StatusOK || string(r.Value) != "before" {
			t.Fatalf("after the Move, get %s: %+v", key, r)
		}
	}
	after := do(t, c, 5, fsm.Command{Op: fsm.OpPut, Key: inSlot[0], Value: []byte("after")})
	if after.Version <= before.Version {
		t.Fatalf("version %d after the Move, %d before", after.Version, before.Version)
	}
	if diffs := c.EndState(); len(diffs) > 0 {
		t.Fatalf("end state: %v", diffs)
	}
}

func newGossipCluster(seed uint64, mode cluster.GossipMode, spares int) *cluster.Cluster {
	transport.Register(raft.MessageBodies()...)
	return cluster.New(cluster.Config{
		Seed: seed, Nodes: 5, Groups: 3, Replicas: 3, Slots: 8, Spares: spares,
		NewCore: newCore, Copy: transport.NewLoopback().Copy, Gossip: mode,
	})
}

// A Node started knowing one other becomes known to all, address and all,
// and can route requests though it hosts no Group (A§11.10).
func TestASpareJoinsWithOneAddress(t *testing.T) {
	for _, mode := range []cluster.GossipMode{cluster.GossipCounters, cluster.GossipSWIM} {
		c := newGossipCluster(3, mode, 2)
		c.S.Run(400)
		if diffs := c.GossipAgrees(); len(diffs) > 0 {
			t.Fatalf("mode %d: after 400 units: %v", mode, diffs)
		}
		if n := len(c.Gossip(7)); n != 7 {
			t.Fatalf("mode %d: the second Spare knows %d Nodes, want 7", mode, n)
		}
		// Ask the Spare for a key: it knows the table and where to send it.
		if r := do(t, c, 6, fsm.Command{Op: fsm.OpPut, Key: "k1", Value: []byte("via a Spare")}); r.Status != fsm.StatusOK {
			t.Fatalf("mode %d: a put through a Spare: %+v", mode, r)
		}
		if r := do(t, c, 7, fsm.Command{Op: fsm.OpGet, Key: "k1"}); string(r.Value) != "via a Spare" {
			t.Fatalf("mode %d: a get through the other Spare: %+v", mode, r)
		}
	}
}

// A Node that says it is leaving is marked as gone at once and never
// suspected; one that just stops is suspected, then taken for dead.
func TestLeavingAndDyingLookDifferent(t *testing.T) {
	status := func(c *cluster.Cluster, at, about int) gossip.Status {
		for _, m := range c.Gossip(at) {
			if m.ID == about {
				return m.Status
			}
		}
		return 99
	}
	for _, mode := range []cluster.GossipMode{cluster.GossipCounters, cluster.GossipSWIM} {
		c := newGossipCluster(4, mode, 0)
		c.S.Run(300)
		c.LeaveNode(4)
		c.CrashNode(5)
		c.S.Run(360)
		if got := status(c, 1, 4); got != gossip.Left {
			t.Fatalf("mode %d: 60 units after node 4 said it was leaving, node 1 thinks it is %v", mode, got)
		}
		if got := status(c, 1, 5); got == gossip.Dead || got == gossip.Left {
			t.Fatalf("mode %d: 60 units after node 5 stopped, node 1 already thinks it is %v", mode, got)
		}
		c.S.Run(1200)
		if a, b := status(c, 2, 4), status(c, 2, 5); a != gossip.Left || b != gossip.Dead {
			t.Fatalf("mode %d: long after, node 2 thinks node 4 is %v and node 5 is %v; want left and dead", mode, a, b)
		}
		// Both come back and are taken back.
		c.RestartNode(4)
		c.RestartNode(5)
		c.S.Run(2000)
		if diffs := c.GossipAgrees(); len(diffs) > 0 {
			t.Fatalf("mode %d: after both returned: %v", mode, diffs)
		}
	}
}

// A Node lost for good is replaced by a Spare in every Group it was in,
// the Meta Group included, once the Meta Group is asked (A§11.11). The
// Spare starts a replica of each because the table says so, each Group's
// Leader adds it and then removes the lost Node, and the table ends up
// with what the Groups report.
func TestASpareTakesALostNodesPlace(t *testing.T) {
	for _, lost := range []int{2, 4} { // Node 2 is in the Meta Group; Node 4 is not
		c := newGossipCluster(5, cluster.GossipCounters, 1)
		c.S.Run(300)
		for i := range 24 {
			key := fmt.Sprintf("k%d", i)
			do(t, c, 1, fsm.Command{Op: fsm.OpPut, Key: key, Value: []byte(key)})
		}
		var in []shard.GroupID
		for g, row := range c.Table(1).Groups {
			if slices.Contains(row.Members, lost) {
				in = append(in, shard.GroupID(g))
			}
		}
		if len(in) < 2 {
			t.Fatalf("node %d is in Groups %v: the test wants it in several", lost, in)
		}
		c.DestroyNode(lost)
		asked := false
		for !asked && c.S.Now() < 3000 {
			c.Replace(1, lost, 6, func(ok bool) { asked = asked || ok })
			c.S.Run(c.S.Now() + 100)
		}
		if !asked {
			t.Fatalf("the Meta Group didn't take the replacement of node %d", lost)
		}
		c.S.Run(c.S.Now() + 3000)
		for g, row := range c.Table(1).Groups {
			g := shard.GroupID(g)
			var members []int
			for _, r := range c.Members(g) {
				members = append(members, cluster.NodeOf(r))
			}
			if !slices.Equal(members, row.Members) || row.Add != 0 || row.Remove != 0 {
				t.Errorf("node %d lost: Group %d has Members %v, and the table says %+v", lost, g, members, row)
			}
			if slices.Contains(members, lost) || slices.Contains(members, 6) != slices.Contains(in, g) || len(members) != 3 {
				t.Errorf("node %d lost: Group %d ended with Members %v", lost, g, members)
			}
		}
		for i := range 24 {
			key := fmt.Sprintf("k%d", i)
			if r := do(t, c, 6, fsm.Command{Op: fsm.OpGet, Key: key}); string(r.Value) != key {
				t.Fatalf("node %d lost: get %s: %+v", lost, key, r)
			}
		}
		if diffs := c.EndState(); len(diffs) > 0 {
			t.Fatalf("node %d lost: end state: %v", lost, diffs)
		}
		if diffs := c.GossipAgrees(); len(diffs) > 0 {
			t.Fatalf("node %d lost: gossip: %v", lost, diffs)
		}
	}
}
