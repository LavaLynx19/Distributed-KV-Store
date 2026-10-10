package cluster

import (
	"fmt"
	"math/rand/v2"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/gossip"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
)

// gossipEvery is the length of one gossip round, in units: a tick.
const gossipEvery = 10

func address(n int) string { return fmt.Sprintf("node%d", n) }

// newGossip gives a Node a fresh gossip, knowing what a Node is started
// with: the founders if it is one of them, and otherwise only Node 1.
func (c *Cluster) newGossip(nd *node) {
	if c.cfg.Gossip == NoGossip {
		return
	}
	cfg := gossip.Config{
		ID: nd.id, Peer: address(nd.id), Client: address(nd.id),
		Rand:         rand.New(rand.NewPCG(c.cfg.Seed, uint64(1000+nd.id))),
		Detector:     gossip.Counters,
		SuspectAfter: c.cfg.SuspectAfter, DeadAfter: c.cfg.DeadAfter,
	}
	if c.cfg.Gossip == GossipSWIM {
		cfg.Detector = gossip.SWIM
	}
	if nd.id <= c.cfg.Nodes {
		for n := 1; n <= c.cfg.Nodes; n++ {
			cfg.Seeds = append(cfg.Seeds, gossip.Member{ID: n, Peer: address(n), Client: address(n)})
		}
	} else {
		cfg.Seeds = []gossip.Member{{ID: 1, Peer: address(1), Client: address(1)}}
	}
	nd.gossip = gossip.New(cfg)
	nd.deadSince, nd.asking, nd.report = map[int]int64{}, map[shard.GroupID]int64{}, shard.Report{}
}

// startGossip starts every Node's gossip rounds.
func (c *Cluster) startGossip() {
	if c.cfg.Gossip == NoGossip {
		return
	}
	for _, n := range c.NodeIDs() {
		nd := c.nodes[n]
		c.newGossip(nd)
		c.S.After(1+c.S.Rand().Int64N(gossipEvery), func() { c.gossipRound(nd) })
	}
}

// gossipRound is one Node's gossip round. A Node that hosts a Meta Group
// replica first puts that replica's table in: that is how the table gets
// into gossip at all (A§11.10).
func (c *Cluster) gossipRound(nd *node) {
	c.S.After(gossipEvery, func() { c.gossipRound(nd) })
	if !nd.up {
		return
	}
	if r := Replica(nd.id, shard.Meta); c.hosts(nd.id, shard.Meta) && c.S.Up(r) {
		nd.gossip.SetTable(c.S.Machine(r).(*meta.Machine).Table())
	}
	c.report(nd)
	c.gossipSend(nd.gossip.Tick())
	c.gossipLearn(nd)
	if c.cfg.GossipDecides.Members {
		// The naive store: a Group's Members are whichever of its founders
		// this Node's gossip thinks are alive.
		for _, g := range nd.groups {
			var alive []core.NodeID
			for _, r := range c.Founders(g) {
				if NodeOf(r) == nd.id || nd.gossip.Alive(NodeOf(r)) {
					alive = append(alive, r)
				}
			}
			c.S.Inject(Replica(nd.id, g), core.Decree{Members: alive})
		}
	}
}

// gossipLearn takes the table a Node's gossip holds as the one it routes by.
func (c *Cluster) gossipLearn(nd *node) {
	c.learn(nd, nd.gossip.Table())
}

// gossipSend carries gossip Messages between Nodes, subject to Partitions,
// crashes, delay and loss, and delivers the answers the same way.
func (c *Cluster) gossipSend(msgs []gossip.Message) {
	for _, m := range msgs {
		c.GossipSent++
		if _, exists := c.nodes[m.To]; !exists || !c.reachable(m.From, m.To) {
			continue
		}
		if c.cfg.GossipLoss > 0 && c.S.Rand().Float64() < c.cfg.GossipLoss {
			continue
		}
		c.S.After(c.rpcDelay(), func() {
			to := c.nodes[m.To]
			if !to.up || !c.reachable(m.From, m.To) {
				return
			}
			c.gossipSend(to.gossip.Receive(m))
			c.gossipLearn(to)
		})
	}
}

// Gossip is what Node n's gossip thinks of every Node, or nil if the store
// doesn't gossip or the Node is down.
func (c *Cluster) Gossip(n int) []gossip.Member {
	if nd := c.nodes[n]; nd.gossip != nil && nd.up {
		return nd.gossip.Members()
	}
	return nil
}

// LeaveNode shuts Node n down in an orderly way: it says it is leaving, and
// then stops.
func (c *Cluster) LeaveNode(n int) {
	if nd := c.nodes[n]; nd.up && nd.gossip != nil {
		c.gossipSend(nd.gossip.Leave())
	}
	c.CrashNode(n)
}
