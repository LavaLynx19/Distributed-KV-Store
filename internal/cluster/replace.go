package cluster

import (
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/gossip"
	"distributed-kv-store/internal/meta"
	"distributed-kv-store/internal/shard"
	"distributed-kv-store/internal/sim"
)

// Replacing describes how a store replaces Nodes that have died (A§11.11).
type Replacing struct {
	// On makes the Meta Group's Leader have a Spare take the place of a
	// Node found dead.
	On bool
	// OneWord and FollowTable are the store stage 7c starts from. With
	// OneWord the Meta Leader acts the moment its own Node's gossip gives a
	// Node up for dead. With FollowTable a Node starts a replica of every
	// Group the table it holds names it a Member of, and drops the data of
	// every Group it doesn't.
	OneWord     bool
	FollowTable bool
}

// Replaced is one replacement the Meta Group recorded the wish for.
type Replaced struct {
	At      int64
	Out, In int
	// Running is set if the Node replaced was running at that moment.
	Running bool
}

// Dropped is one replica's data being dropped by its Node.
type Dropped struct {
	At      int64
	Replica core.NodeID
	// Counted is set if the Group still had the replica as a Member at that
	// moment: the Group was relying on data that was thrown away.
	Counted bool
}

// replicasWanted starts and drops Node nd's replicas, as the table it holds
// says.
func (c *Cluster) replicasWanted(nd *node) {
	for g, row := range nd.table.Groups {
		g := shard.GroupID(g)
		member := slices.Contains(row.Members, nd.id)
		switch {
		case c.hosts(nd.id, g):
			if c.cfg.Replacing.FollowTable && !member && row.Add != nd.id {
				c.drop(nd, g)
			}
		case row.Add == nd.id || c.cfg.Replacing.FollowTable && member:
			c.host(nd, g)
		}
	}
}

// drop stops Node nd's replica of Group g and removes its data.
func (c *Cluster) drop(nd *node, g shard.GroupID) {
	r := Replica(nd.id, g)
	c.Drops = append(c.Drops, Dropped{At: c.S.Now(), Replica: r, Counted: slices.Contains(c.truly(g), r)})
	nd.groups = slices.DeleteFunc(nd.groups, func(have shard.GroupID) bool { return have == g })
	delete(nd.leader, g)
	c.S.DropData(r)
}

// truly is Group g's Member list as the Group itself has it: its Leader's,
// or with no Leader the newest list any running replica holds.
func (c *Cluster) truly(g shard.GroupID) []core.NodeID {
	if ms := c.Members(g); ms != nil {
		return ms
	}
	var newest core.Status
	for _, r := range c.Replicas(g) {
		if st := c.S.Status(r); c.S.Up(r) && (newest.Members == nil || st.MembersAt > newest.MembersAt) {
			newest = st
		}
	}
	return newest.Members
}

// replaceDead is the Meta Leader's part in replacing a dead Node: it picks
// the Node and a Spare and puts the wish in the Meta Group's Log. Each
// Group's Leader does the rest (changeMembers). One Node at a time.
func (c *Cluster) replaceDead(nd *node, r core.NodeID) {
	if !c.cfg.Replacing.On || nd.gossip == nil {
		return
	}
	table := c.S.Machine(r).(*meta.Machine).Table()
	for _, out := range c.dead(nd) {
		wanted := false
		for _, row := range table.Groups {
			in := slices.Contains(row.Members, out)
			wanted = wanted || in && row.Add == 0 || !in && row.Add == out
		}
		if !wanted {
			continue
		}
		in := spare(nd, table)
		if in == 0 {
			return
		}
		cmd := meta.Command{Op: meta.OpReplace, Out: out, In: in}.Encode()
		running := c.nodes[out].up
		c.S.Propose(r, cmd, func(reply sim.Reply) {
			if resp, err := meta.DecodeResponse(reply.Response); reply.Reason == core.OK && err == nil && resp.Status == meta.StatusOK {
				c.Replacements = append(c.Replacements, Replaced{At: c.S.Now(), Out: out, In: in, Running: running})
			}
		})
		return
	}
}

// dead lists the Nodes the Meta Leader on Node nd may replace, ascending.
func (c *Cluster) dead(nd *node) []int {
	var out []int
	for _, m := range nd.gossip.Members() {
		if m.Status == gossip.Dead {
			out = append(out, m.ID)
		}
	}
	return out
}

// spare picks a Node to give replicas to: the lowest-numbered one that
// seems alive and that the table has in no Group and on its way into none.
func spare(nd *node, table shard.Table) int {
	for _, m := range nd.gossip.Members() {
		if m.Status != gossip.Alive {
			continue
		}
		free := true
		for _, row := range table.Groups {
			free = free && !slices.Contains(row.Members, m.ID) && row.Add != m.ID
		}
		if free {
			return m.ID
		}
	}
	return 0
}
