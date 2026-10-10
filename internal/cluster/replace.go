package cluster

import (
	"slices"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
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
	// Wait is how long a Meta Member must have thought a Node dead before
	// it says so to the others (default 200 units).
	Wait int64
}

// deadWait is the default for Replacing.Wait. A Node is given up for dead 250
// units after it was last heard from, so this replaces it after 450: longer
// than any restart in the scenarios takes.
const deadWait = 200

// askEvery is how often a Node asks a Group whether a replica it holds is
// still wanted, while the table says it isn't.
const askEvery = 60

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
			if member || row.Add == nd.id {
				continue
			}
			if c.cfg.Replacing.FollowTable {
				c.drop(nd, g)
			} else if c.cfg.Replacing.On && nd.gossip != nil {
				c.askIfGone(nd, g)
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
	c.joiner[r] = true
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
	for _, out := range c.dead(nd, table) {
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
		c.S.Propose(r, cmd, func(sim.Reply) {})
		return
	}
}

// noteReplacements records the replacements that table t is the first
// version of the table to want.
func (c *Cluster) noteReplacements(t shard.Table) {
	seen := map[[2]int]bool{}
	for g, row := range t.Groups {
		pair := [2]int{row.Remove, row.Add}
		if row.Add != 0 && c.wanted[g] != pair && !seen[pair] {
			seen[pair] = true
			c.Replacements = append(c.Replacements, Replaced{At: c.S.Now(), Out: row.Remove, In: row.Add, Running: c.nodes[row.Remove].up})
		}
		c.wanted[g] = pair
	}
}

// dead lists the Nodes the Meta Leader on Node nd may replace, ascending:
// those that a Majority of the Meta Group's Members report having thought
// dead for longer than the wait (A§11.11). A Meta Member this Node thinks is
// gone has no say: what it last said is old.
func (c *Cluster) dead(nd *node, table shard.Table) []int {
	var out []int
	if c.cfg.Replacing.OneWord {
		for _, m := range nd.gossip.Members() {
			if m.Status == gossip.Dead {
				out = append(out, m.ID)
			}
		}
		return out
	}
	said := map[int]int{}
	voters := table.Hosts(shard.Meta)
	for _, m := range voters {
		report := nd.report
		if m != nd.id {
			if !nd.gossip.Alive(m) {
				continue
			}
			report, _ = shard.DecodeReport(nd.gossip.Note(m))
		}
		for _, n := range report.Dead {
			said[n]++
		}
	}
	for n, votes := range said {
		if votes > len(voters)/2 {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}

// report works out what Node nd tells the others about itself, and puts it
// in its gossip: the Nodes it has thought dead for longer than the wait,
// and the load on the Slots it leads.
func (c *Cluster) report(nd *node) {
	told := false
	if c.cfg.Replacing.On && !c.cfg.Replacing.OneWord {
		told = true
		wait := c.cfg.Replacing.Wait
		if wait == 0 {
			wait = deadWait
		}
		now := c.S.Now()
		nd.report.Dead = nil
		for _, m := range nd.gossip.Members() {
			since, held := nd.deadSince[m.ID]
			switch {
			case m.Status != gossip.Dead:
				delete(nd.deadSince, m.ID)
			case !held:
				nd.deadSince[m.ID] = now
			case now-since >= wait:
				nd.report.Dead = append(nd.report.Dead, m.ID)
			}
		}
	}
	if c.cfg.Balancing.On {
		told = true
		c.measure(nd)
	}
	if told {
		nd.gossip.SetNote(nd.report.Encode())
	}
}

// askIfGone is how a Node finds out that a replica it holds is no longer
// wanted. The table only prompts the question: it may be old, or ahead of
// the Group. The Node asks the Group's Leader, which answers once it has
// confirmed it still leads, with its Member list if that list is Committed.
// The Node drops the replica if the list leaves it out and is no older than
// the list the replica itself holds.
func (c *Cluster) askIfGone(nd *node, g shard.GroupID) {
	now := c.S.Now()
	if at, asking := nd.asking[g]; asking && now-at < askEvery {
		return
	}
	nd.asking[g] = now
	me := Replica(nd.id, g)
	answer := func(st core.Status) {
		delete(nd.asking, g)
		if !nd.up || !c.hosts(nd.id, g) || !c.S.Up(me) {
			return
		}
		if mine := c.S.Status(me); !slices.Contains(st.Members, me) && mine.MembersAt <= st.MembersAt {
			c.drop(nd, g)
		}
	}
	// A Node that doesn't answer isn't asked again next time.
	var try func(target core.NodeID, again bool)
	try = func(target core.NodeID, again bool) {
		to := NodeOf(target)
		delete(nd.leader, g)
		if to == nd.id || !c.reachable(nd.id, to) {
			return
		}
		c.S.After(c.rpcDelay(), func() {
			if !c.nodes[to].up || !c.hosts(to, g) || !c.S.Up(target) {
				return
			}
			probe := fsm.Command{Op: fsm.OpGet, Key: "members"}.Encode()
			c.S.Read(target, probe, func(r sim.Reply) {
				st := c.S.Status(target)
				c.S.After(c.rpcDelay(), func() {
					switch {
					case !c.reachable(to, nd.id) || !nd.up:
					case r.Reason == core.OK && !r.Refused && st.Role == core.LeaderRole && !st.Changing:
						nd.leader[g] = target
						answer(st)
					case again && r.Leader != 0 && r.Leader != target:
						try(r.Leader, false)
					}
				})
			})
		})
	}
	target, known := nd.leader[g]
	if !known || target == me {
		var others []core.NodeID
		for _, n := range nd.table.Hosts(g) {
			if n != nd.id && nd.gossip.Alive(n) {
				others = append(others, Replica(n, g))
			}
		}
		if len(others) == 0 {
			return
		}
		target = others[c.S.Rand().IntN(len(others))]
	}
	try(target, true)
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
