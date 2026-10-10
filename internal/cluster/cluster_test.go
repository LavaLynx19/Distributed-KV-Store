package cluster_test

import (
	"fmt"
	"testing"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
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
		if row := c.Table(n).Slots[0]; row != (shard.Owner{Group: 2, Epoch: 1}) {
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
