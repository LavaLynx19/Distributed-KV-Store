package cluster_test

import (
	"fmt"
	"slices"
	"testing"

	"distributed-kv-store/internal/check"
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

func newReplacingCluster(seed uint64) *cluster.Cluster {
	transport.Register(raft.MessageBodies()...)
	return cluster.New(cluster.Config{
		Seed: seed, Nodes: 5, Groups: 3, Replicas: 3, Slots: 8, Spares: 2,
		NewCore: newCore, Copy: transport.NewLoopback().Copy, Gossip: cluster.GossipCounters,
		Replacing: cluster.Replacing{On: true},
	})
}

// replicasHeld counts the replicas Node n holds.
func replicasHeld(c *cluster.Cluster, n int) int {
	held := 0
	for _, g := range c.Groups() {
		for _, r := range c.Replicas(g) {
			if cluster.NodeOf(r) == n {
				held++
			}
		}
	}
	return held
}

// Two Nodes are cut off for a long time, one of them a Member of the Meta
// Group. Their side can replace nobody: it has no Majority of the Meta
// Group. The other side replaces them both, in every Group that still has a
// Majority there. When the network heals they find their places taken and
// drop what they hold, once each Group confirms it. A Node left in no Group
// is a Spare again (A§11.11).
//
// Group 3 is on Nodes 1, 4 and 5, so its Majority is on the cut-off side.
// It can't be changed until the network heals, and by then only one of the
// two changes wanted for it is still wanted.
func TestTheMinoritySideIsReplacedAndReturnsAsSpares(t *testing.T) {
	c := newReplacingCluster(7)
	c.S.Run(300)
	for i := range 24 {
		key := fmt.Sprintf("k%d", i)
		do(t, c, 2, fsm.Command{Op: fsm.OpPut, Key: key, Value: []byte(key)})
	}
	before := [2]int{replicasHeld(c, 1), replicasHeld(c, 4)}
	c.PartitionNodes([]int{1, 4}, []int{2, 3, 5, 6, 7})
	c.S.Run(c.S.Now() + 2500)
	for _, r := range c.Replacements {
		if r.Out != 1 && r.Out != 4 {
			t.Fatalf("node %d was replaced, and it was on the side with the Majority", r.Out)
		}
	}
	if len(c.Replacements) != 2 {
		t.Fatalf("replacements while cut off: %+v, want nodes 1 and 4", c.Replacements)
	}
	if got := [2]int{replicasHeld(c, 1), replicasHeld(c, 4)}; got != before || len(c.Drops) != 0 {
		t.Fatalf("while cut off, nodes 1 and 4 hold %v replicas, had %v; drops %+v", got, before, c.Drops)
	}
	for _, g := range c.Table(1).Groups {
		if g.Add != 0 || slices.Contains(g.Members, 6) || slices.Contains(g.Members, 7) {
			t.Fatalf("the cut-off side's table changed: %+v", g)
		}
	}
	c.Heal()
	c.S.Run(c.S.Now() + 1500)
	for _, n := range []int{1, 4} {
		in := 0
		for _, g := range c.Table(2).Groups {
			if slices.Contains(g.Members, n) {
				in++
			}
		}
		if held := replicasHeld(c, n); held != in || in > 1 {
			t.Fatalf("after healing, node %d holds %d replicas and is a Member of %d Groups", n, held, in)
		}
	}
	if len(c.Drops) == 0 {
		t.Fatal("after healing, nothing was dropped")
	}
	for _, d := range c.Drops {
		if d.Counted {
			t.Fatalf("a replica was dropped while still a Member: %+v", d)
		}
	}
	for i := range 24 {
		key := fmt.Sprintf("k%d", i)
		if r := do(t, c, 1, fsm.Command{Op: fsm.OpGet, Key: key}); string(r.Value) != key {
			t.Fatalf("get %s through a returned Node: %+v", key, r)
		}
	}
	if diffs := append(c.EndState(), c.GossipAgrees()...); len(diffs) > 0 {
		t.Fatalf("end state: %v", diffs)
	}
	// Lose another Node: whichever of them is a Spare again takes over.
	c.DestroyNode(5)
	c.S.Run(c.S.Now() + 2500)
	if last := c.Replacements[len(c.Replacements)-1]; last.Out != 5 || (last.In != 1 && last.In != 4) {
		t.Fatalf("node 5 was lost, and the last replacement is %+v", last)
	}
	if diffs := c.EndState(); len(diffs) > 0 {
		t.Fatalf("end state after losing node 5: %v", diffs)
	}
}

// A Move is asked for when the Group it is going to has lost its Majority:
// one Member for good, another down. Nothing can be done for that Group, by
// the Move or by replacement, because a Group with no Majority can't change
// its own Members. The Move waits. When the Member that was down returns
// the Group has its Majority back, finishes the Move, and has its lost
// Member replaced (A§11.11).
func TestAMoveWaitsOutALostMajority(t *testing.T) {
	c := newReplacingCluster(8)
	c.S.Run(300)
	target := shard.GroupID(2)
	if c.Table(1).Slots[0].Group == target {
		target = 3
	}
	hosts := c.Table(1).Hosts(target)
	c.DestroyNode(hosts[0])
	c.CrashNode(hosts[1])
	asked := false
	for !asked && c.S.Now() < 2000 {
		c.Move(hosts[2], 0, target, func(ok bool) { asked = asked || ok })
		c.S.Run(c.S.Now() + 100)
	}
	if !asked {
		t.Fatal("the Meta Group didn't take the Move")
	}
	c.S.Run(c.S.Now() + 2000)
	if row := c.Table(hosts[2]).Slots[0]; row.MovingTo != target {
		t.Fatalf("with the target Group short of a Majority the Move should be waiting: %+v", row)
	}
	if got := c.Table(hosts[2]).Hosts(target); !slices.Equal(got, hosts) {
		t.Fatalf("a Group with no Majority changed its Members from %v to %v", hosts, got)
	}
	c.RestartNode(hosts[1])
	c.S.Run(c.S.Now() + 4000)
	if row := c.Table(hosts[2]).Slots[0]; row.Group != target || row.MovingTo != 0 {
		t.Fatalf("with its Majority back the Group should have finished the Move: %+v", row)
	}
	if diffs := c.EndState(); len(diffs) > 0 {
		t.Fatalf("end state: %v", diffs)
	}
}

// Each Group's Leader counts the load on its Slots and says so by gossip,
// and every Node ends up with the same picture (A§11.11). Most requests
// here go to two keys, so their Slots stand out.
func TestLoadIsCountedAndGossiped(t *testing.T) {
	transport.Register(raft.MessageBodies()...)
	c := cluster.New(cluster.Config{
		Seed: 9, Nodes: 5, Groups: 3, Replicas: 3, Slots: 8, Spares: 1,
		NewCore: newCore, Copy: transport.NewLoopback().Copy, Gossip: cluster.GossipCounters,
		Balancing: cluster.Balancing{On: true},
	})
	w := cluster.DefaultWorkload
	w.Skew.Percent, w.Skew.Sets = 80, [][]string{{"k0", "k1"}}
	w.Start(c, &check.History{}, 3000)
	c.S.Run(3000)

	hot := map[shard.Slot]bool{shard.SlotOf("k0", 8): true, shard.SlotOf("k1", 8): true}
	keyed := map[shard.Slot]bool{}
	for i := range w.Keys {
		keyed[shard.SlotOf(fmt.Sprintf("k%d", i), 8)] = true
	}
	load := c.Load(6) // a Spare leads nothing: all it knows is hearsay
	var hottest, coolest uint32
	for s, l := range load {
		switch s := shard.Slot(s); {
		case hot[s]:
			if hottest == 0 || l < hottest {
				hottest = l
			}
		case !keyed[s] && l != 0:
			t.Errorf("Slot %d has no keys and load %d", s, l)
		case l > coolest:
			coolest = l
		}
	}
	if hottest < 3*coolest || coolest == 0 {
		t.Fatalf("load by Slot %v: the Slots of k0 and k1 should carry several times any other's", load)
	}
	for _, n := range c.NodeIDs() {
		for s, l := range c.Load(n) {
			if diff := int64(l) - int64(load[s]); diff > int64(load[s])/2+shard.WriteCost || -diff > int64(load[s])/2+shard.WriteCost {
				t.Errorf("node %d has Slot %d at load %d, node 6 has it at %d", n, s, l, load[s])
			}
		}
	}
	sums := cluster.GroupLoad(c.Table(6), load, 3)
	t.Logf("load by Slot %v, by Group %v", load, sums[1:])
}
