# Rung 7 retro: several Groups

Status: **all three stages done; 7c waiting for approval**. Rung 7 is built in three stages (A§11.1). This file has a part per stage, in order, and the verdict for the whole Rung at the end.

Stage 7a in one paragraph: several Groups share the keys by Slot, a Meta Group holds the table, and a Slot can be moved between Groups while clients carry on. 2,400 simulated runs with 10,224 Moves have no run with two owners and none that isn't Linearizable, and 13 real runs are clean. A moved Slot refuses its clients for about 80 ms. **Three Groups on one machine are slower than one Group, at 0.52× with the disk on**, which misses the 0.9× target; the reason is below.

# Stage 7a: Slots, the table, and moving a Slot

Environment: the Simulation, with 5 Nodes, three data Groups and the Meta Group, each Group on 3 of the Nodes, and 8 Slots. A Node hosts a replica of two or three Groups, so one Node crashing costs several Groups a Member. Clients ask any Node about any of 12 keys. A scenario asks the Meta Group to move a Slot to another Group every 200 units or so.

A run now gets a fourth verdict: **one owner**. At the moment a Group's Log makes it start serving a Slot, no other Group's Log may still have it serving that Slot.

## Exposed: a Group that answers for whatever it is asked (P7a.4)

The Move itself is done properly here. What is missing is the check: a Group asked about a key answers from whatever it holds, trusting the sender to have sent it to the right place.

50 seeds per cell.

| Faults | Not Linearizable |
|---|---|
| No Moves | 0 |
| Moves | 44 |
| Moves and crashes | 43 |
| Moves and Partitions | 37 |
| Moves interrupted at each step | 38 |
| Moves, crashes and Partitions | 36 |

Every Node caches the table and refreshes it every 20 units. For that long after a Move, some Node still sends the Slot's keys to the Group that used to own them. That Group has given the Slot away, and answers anyway: a read finds nothing, or a write lands where nobody will look for it.

A stale table is normal. It can't be prevented, only made harmless.

## Exposed, part two: a table flipped with nobody confirming (P7a.5)

Now every Group checks, by its own Log, that it owns what it serves. The Move is the naive one: the Meta Group changes the owner in the table at once, and the Groups follow the table. The old owner, seeing the table name someone else, sends the Slot over as it stands and lets go when the other has it.

| Faults (50 seeds) | Two Groups serving one Slot | Not Linearizable |
|---|---|---|
| No Moves | 0 | 0 |
| Moves | **50** | 15 |
| Moves and crashes | **50** | 11 |
| Moves and Partitions | **50** | 12 |
| Moves interrupted at each step | **50** | 3 |
| Moves, crashes and Partitions | **49** | 12 |

Nearly every run with a Move has two owners. Seed 1: Slot 0 is served by two Groups within 400 units.

The new owner starts when it is given the Slot. The old one stops when its own "let go" is Committed, a little later. In between both answer. Most runs get away with it, because the window is short and a client has to write to the old owner inside it. The ones that don't lose that write.

(These two tables were measured again after stage 7b changed how a Node finds a Leader, which changes every run. The seed numbers quoted for the bugs below are from before that; the bugs have unit tests of their own.)

The History catches this in a fifth of the runs. The one-owner verdict catches it in all of them, which is why it was worth adding.

### Reproduce
```
go test -run 'TestUncheckedGroup|TestFlippedTable' -v ./internal/rungtest/
```

## Fix: the Move is confirmed by both Groups (P7a.7)

The Meta Group records the intent and nothing else. The Group that owns the Slot copies it to the other while still serving, then commits a freeze and stops. Only then does it send what changed during the copy. The other Group commits an accept and starts. The Meta Group records that it is done, and the first Group drops the Slot.

A Group serves a Slot from its accept to its freeze, by its own Log. The accept can only be built from the frozen Slot. So the two never overlap, whatever any table says.

### Same scenarios, same harness

`TestRung7a`: 6 scenarios × 200 seeds × two workloads = **2,400 runs, none unsafe, 10,224 Moves finished**, every run ending with each Slot served by exactly the Group the table names. The second workload adds time-to-lives, Transactions over two keys, range scans merged from every Group, and Sessions that are cleaned up.

### Two bugs the suite found in the fix

**A late step brought a Slot back** (`moves-and-everything`, seed 39). Slot 3 went from Group 1 to Group 3, and later back to Group 1. Then a duplicate of the first Move's accept, sent by a Leader that had lost touch, finally got through to Group 3. Group 3 no longer had the Slot, the accept carried a whole copy of it, and Group 3 took it. Two owners.

A Group that dropped a Slot forgot everything about it, including the Epoch. It now keeps the Epoch at which it last had each Slot and takes the Slot back only at a higher one.

Nothing in the History showed this. All 200 answers in that run were consistent. Only the one-owner verdict saw it, and only after I rewrote that verdict (next section).

**A later Move reused the copy progress of an earlier one** (`moves`, seed 7). The agent remembers how far it has copied a Slot. It kept that after the Move finished. The next time the same Slot left the same Group, under the same Leader, the agent thought the copy was already done, froze at once, and sent only "what changed". The target had nothing, refused, and the Slot stayed frozen for good. Safe, and stuck. The progress is now tied to the Move's Epoch, and a refused accept makes the agent send everything next time.

### A verdict that was wrong first

My first one-owner check sampled each Group's Leader every 5 units and asked whether it would serve the Slot. It reported two owners in 13 of 300 runs of the fixed store.

They were false. A Leader that has just restarted hasn't applied its Log yet. Its state machine still shows the Slot as served, although the freeze is Committed and it will apply it before answering anything. The check was reading a state the Group was no longer in.

The check now runs at the moment each Entry is applied. For each Group it keeps what the Group serves as of the furthest Entry any replica has applied, which is the Group's Committed state whoever leads it. With that, the 13 went away, and one real case remained: seed 39 above.

### A bug in the test client

Three runs in 200 weren't Linearizable with Transactions and crashes together (seed 79). The store was right. A Transaction's first attempt had timed out and then succeeded. A Move then put its two keys in different Groups, so the retry was refused as `cross_group`, and my client recorded "refused, nothing happened". A refusal on a retry says nothing about the earlier attempt. The rule is now in A§7.2.

### Testing the tests

Eight deliberate breaks of the Move, each run against the 2,400.

| Break | Runs that fail | How |
|---|---|---|
| A Frozen Slot is still served | 2,000 | Two owners in every run with a Move; 1,519 not Linearizable |
| Versions don't rise across a Move | 1,412 | Not Linearizable |
| Writes during the copy aren't remembered | 1,077 | Not Linearizable |
| The final part leaves out deletes | 239 | Not Linearizable |
| Sessions don't move with the Slot | 53 | Not Linearizable: a retry applied twice |
| A dropped Slot forgets its Epoch | 5 | Two owners; 1 not Linearizable |
| Copy progress outlives its Move | **0** | Nothing wrong any more: the agent now recovers by sending everything. It costs a longer freeze |
| The target doesn't catch up with the source's time first | **0** | No verdict can see it: a key living too long is allowed. `TestDeadlinesSurviveAMove` covers it |

## The pause of a Move

A Slot refuses its clients from its freeze being Committed in the Group it is leaving to its accept being Committed in the Group it goes to. In between is one request between Nodes and one commit.

| | Slots handed over | Frozen for |
|---|---|---|
| Simulation, Moves only | 1,229 | median 21 units, 95% within 30, at most 43 (a tick is 10) |
| Simulation, Moves interrupted at each step | 1,133 | median 20, 95% within 158, at most 415 |
| Real, 8 clients | 3 | median 80 ms, at most 139 ms |
| Real, 64 clients | 1 | 332 ms |

The copy itself doesn't pause anything, which was the reason for copying before freezing. With the runs' handful of keys per Slot the copy is one step anyway, so this measures the floor: the freeze costs one commit however big the Slot is.

The real figures are few. A Move under 64 clients took over a second from request to done, so only one fitted in the three seconds given. Each of its five or six steps is a commit, and a commit took 90 ms under that load.

An interrupted Move can leave a Slot frozen for as long as the interruption lasts: up to 415 units above. Nothing times a Move out. If the Group a Slot is going to is lost for good, the Slot stays frozen. That is a gap.

## Real runs (P7a.13)

M4 Pro, local processes, 10 s each, default mix. Five Nodes host three data Groups and the Meta Group, each on three of them.

### Throughput (64 clients, no Faults)
| | Disk on | Disk off |
|---|---|---|
| One Group, 3 Nodes (Rung 6: 1,387) | 1,344 | 68,766 |
| Three Groups, 5 Nodes | **704** | 46,996 |
| Ratio | 0.52× | 0.68× |

One Group is unchanged from Rung 6 (0.97×). Three Groups are slower than one, with or without the disk.

- **Disk off, 0.68×:** most requests land on a Node that doesn't host the owning Group's Leader, and cost a second HTTP hop.
- **Disk on, 0.52×:** twelve replicas now sync to one SSD where three did. A sync here takes about 30 ms and they queue behind each other, so each Group commits half as often: p50 went from 47 ms to 88 ms. Splitting the keys bought no parallel disks, because there is one disk.

Several Groups are for machines that each have their own disk. On one laptop they can only cost. I can't show the gain here, and I'd rather report the loss than leave the number out.

### The 0.52× looked into (after stage 7a was approved)

A throwaway program measured this disk directly: writers that each append to their own file and sync it.

| Writers | Each syncs its own file | They share one flush of the drive |
|---|---|---|
| 1 | 245 syncs/s | 112 |
| 3 | 254 in total (85 each) | 318 in total (106 each) |
| 12 | 377 in total (31 each) | 1,010 in total (84 each) |

A sync on macOS flushes the whole drive, and the drive does one at a time. Three writers get no more syncs between them than one does. Twelve replicas therefore get 31 syncs a second each where three got 85, and a commit needs two of them, one on the Leader and one on a follower. That is the 88 ms.

The right-hand column is the same writers handing their data to the drive and then all waiting on one shared flush. Twelve of them get nearly three times as much done.

So `kvnode -shared-sync` does that for the replicas inside one Node: each hands its file to the drive, and they share the flush.

| Three Groups, 5 Nodes, 64 clients | Requests/s | p50 | Against one Group |
|---|---|---|---|
| Each replica syncs for itself | 700 | 88 ms | 0.52× |
| A Node's replicas share a flush | **899** | 68 ms | 0.67× |

It helps by 28% and doesn't close the gap. Five Node processes still flush separately and still queue behind each other. Sharing across processes isn't something a real deployment would have, since there each Node has its own disk and none of this applies.

It is off by default. It depends on one flush covering every file on the drive, which holds on macOS. Elsewhere it is still correct and gains nothing.

### A bug the first no-disk run found
Three Groups with the disk off first ran at 5,268 requests/s and **lost 11,406 requests**: clients were told "outcome unknown". Nodes forward to each other over HTTP, and Go's client keeps two idle connections per host by default. With 64 clients nearly every forwarded request opened a connection and closed it, and the machine ran out. Keeping the connections brought it to 46,996 with none lost. With the disk on the bug was invisible, because the disk was slower than the connection churn.

### Under Faults (clients retrying in Sessions)
| Clients | Fault | Requests/s | Rejected / lost | Longest pause in any write | Verdicts |
|---|---|---|---|---|---|
| 8 | A Slot moved every half second | 185 | 0 / 0 | 57 ms | Linearizable; one owner per Slot |
| 64 | The same | 720 | 0 / 0 | 40 ms | Linearizable; one owner per Slot |
| 8 | A Node hosting three replicas killed and restarted | 192 | 0 / 0 | 203 ms | Linearizable; one owner per Slot |
| 8 | Both at once | 184 | 0 / 0 | 265 ms | Linearizable; one owner per Slot |

### Separate Nodes for every Group
You asked for this to be tried once. Twelve Nodes, each Group on three of its own: it ran, 625 requests/s at 64 clients with Slots moving, Linearizable, one owner per Slot. It was no heavier on the machine than five Nodes, since there are the same twelve replicas either way. It is a little slower, because no Node now hosts two Groups and every request to another Group is a hop. The placement rule gives a Group Nodes to itself whenever there are enough.

**Not covered by real runs:** scans and Transactions (`kvbench` doesn't send them; the API tests do), and anything in Docker (the Compose file has no multi-Group mode and I didn't change it).

### Reproduce
```
go test -run 'TestRung7a|TestUncheckedGroup|TestFlippedTable' ./internal/rungtest/
go test ./internal/shardfsm/ ./internal/meta/ ./internal/shard/ ./internal/cluster/
CLIENTS=64 DATA_GROUPS=3 harness/run.sh local 5
RETRY=1 DATA_GROUPS=3 harness/run.sh local 5 move-slots
RETRY=1 DATA_GROUPS=3 harness/run.sh local 5 move-and-kill
DATA_GROUPS=3 harness/local.sh start 5 && harness/local.sh table && harness/local.sh move 0 2
```

## Things decided while building, not at the design step
- **Versions carry a "version Epoch" per Slot, not the table's Epoch alone.** A Transaction gives all its keys one version, and its keys can be in Slots with different Epochs. Each Group keeps a per-Slot number at least the Slot's Epoch; a Transaction raises its Slots to the highest among them, and a Move adds one. A§11.5.
- **The client, not the Node, says when a Session may be registered.** The design had the Node register a Session on first use. A Node can't tell first use from a retry after cleanup. The request now carries a flag the client sets only while no earlier attempt can have taken effect. A§11.6.
- **A Group remembers the Epoch of a Slot it gave up** (the seed 39 bug). A§11.4.
- **A Node follows one Leader hint after a wrong first guess.** The design said "forwards once". The forwarded-to Node still never passes a request on. A§11.3.
- **Store time reaches Nodes by each asking the Meta Group** every agent tick, until gossip carries it in 7b.

## Verdict for stage 7a
| Check | Result |
|---|---|
| Exposed | **Pass**: a Group that doesn't check (`TestUncheckedGroupIsExposed`), and a table flipped with nobody confirming (`TestFlippedTableIsExposed`) |
| Each key owned by exactly one Group at any moment | **Pass**: 2,400 simulated runs, 10,224 Moves, with crashes and Partitions at every step |
| Single-key operations stay Linearizable while keys move | **Pass**: the same runs, and 13 real ones |
| A retry that crosses a Move takes effect once | **Pass**: in the suite; breaking it fails 53 runs |
| The pause of a Move reported | **Done**: about 2 ticks simulated; 80 ms real at 8 clients |
| Numbers: at least 0.9× Rung 6 | **One Group: pass (0.97×). Three Groups on one machine: miss (0.52×, or 0.67× with `-shared-sync`)**. The cause is one disk shared by twelve replicas, measured above |
| Scans across Groups; Transactions across Groups refused | **Pass** in simulation and API tests |

## Lessons from stage 7a
1. **The History isn't enough once ownership can move.** Two Groups serving one Slot produced a wrong answer in a fifth of the runs where it happened, and the late-accept bug in none. A verdict about the store's state caught both every time.
2. **A verdict is code too.** The first one-owner check read a state the Group had already left, and cried wolf 13 times. I only trusted the one real failure after rewriting it to look at each Entry as it is applied.
3. **Forgetting is a decision.** Dropping a Slot and wiping everything about it looked like tidiness. The Epoch was the one thing that had to outlive the data.
4. **Helpful memory is still state.** The agent's copy progress was "only an optimisation" and froze a Slot for good. Anything remembered across two Moves needs to say which Move it is about.
5. **A slow part hides the bug next to it.** The connection churn between Nodes was invisible behind a 30 ms disk sync. The run with the disk off was meant to explain a number and found a bug.
6. **Sharding one disk is a loss.** I expected several Groups to at least hold their own. They halve throughput here, and the design is still right for the case it is for.

# Stage 7b: gossip

Status: **done and approved**. Nodes now learn the Slot table, and of each other, by gossip, and no Node asks the Meta Group for the table on a timer. Gossip decides nothing. Two stores where it does decide are shown failing. 2,400 simulated runs with gossip only informing are clean, and so are 6 real ones. Two ways of noticing a dead Node were built and measured; neither ever took a running Node for dead.

Environment: the 7a Simulation with gossip between the Nodes. A gossip round is one tick. Each round a Node tells two others everything it knows: every Node's address and how it seems, and the whole Slot table. Nodes that host a Meta Group replica put that replica's table into their gossip, which is the only way it gets there.

## Exposed: gossip decides who owns a Slot (P7b.3)

No Meta Group is asked. The Node asked for a Move changes its own table, raises the version, and gossips it. Each Group does what the table it has heard says, as in 7a's second naive store. A Group named as owner that isn't sent the Slot takes it empty after a wait.

50 seeds per cell.

| Faults | Two Groups serving one Slot | Not Linearizable |
|---|---|---|
| No Moves | 0 | 0 |
| Moves | 50 | 16 |
| Long Partitions, no Moves | 0 | 0 |
| Long Partitions, and a Move of the same Slot asked for on each side | **50** | **33** |

`rival-moves`, seed 1: the Nodes split two against three. A Node on each side is asked to move the same Slot, to different Groups. Both raise the table to the same version with different contents. Each side believes its own, and neither ever replaces it, since "the higher version wins" has no answer for a tie. Two Groups serve the Slot, one of them from nothing.

A version number orders things only if one party hands the numbers out.

## Exposed, part two: gossip decides who is in a Group (P7b.4)

Each replica takes for its Group's Members whichever of them its Node's gossip thinks are alive. There is no Entry and no agreement. It needed a switch in the consensus core that lets the shell set the Member list, which exists for this and nothing else.

| Faults (50 seeds) | Unsafe | Not Linearizable | Two Leaders in one Term | Core's safety check tripped |
|---|---|---|---|---|
| No Moves | 0 | 0 | 0 | 0 |
| Moves, nothing else | 0 | 0 | 0 | 0 |
| Long Partitions | **50** | 43 | 16 | 50 |

`long-partitions`, seed 1: replicas 201 and 301 of Group 1 both lead Term 2. A Partition lasts longer than it takes to give a Node up for dead. Each side drops the other. A lone replica is then a Majority of the one Member it has left, and elects itself.

This is Rung 6's failure, two Majorities, arrived at by a different road. It is also what stage 7c must not do, which is why it is here first.

### Reproduce
```
go test -run 'TestOwnershipByGossip|TestMembersByGossip' -v ./internal/rungtest/
```

## Fix: gossip informs (P7b.1–P7b.2)

Nothing was fixed so much as not done. The table in gossip is a hint for routing. Each Group still serves a Slot by its own Log, and a Move is still decided by the Meta Group and confirmed by both Groups. The Member lists don't look at gossip at all.

`TestRung7b`: 8 scenarios × 100 seeds × three stores (counters, SWIM, and SWIM with the busier workload) = **2,400 runs, none unsafe, 7,080 Moves finished**. The End-state verdict gained a part: once Faults stop, every running Node must think every running Node alive, know its address, and hold the Meta Group's table version. The two seeds above pass.

### How fast news of a Move travels
How long after the Meta Group first holds a new table version each Node has it, in units (a tick is 10):

| | Median | 95% within | At most |
|---|---|---|---|
| 7a: every Node asks the Meta Group every 20 units | 12 | 26 | 41 |
| 7b: gossip, two Nodes told per round | 11 | 21 | 41 |

The same. Gossip didn't make the table arrive sooner. What it removed is every Node's standing dependence on reaching the Meta Group's Leader: a Node cut off from it still hears the table from whoever it can reach.

### What a Move's pause is, measured properly
While adding this I found the 7a harness counted a handover again each time the agent re-sent its last step. The count is now once per Move, and the figures barely moved: median 21 units simulated, and in the real runs a median of 52 ms over 3 Moves (at most 113 ms).

## The two detectors (P7b.5–P7b.6)

- **Counters.** Every Node bumps a counter each round and passes on everyone's. A Node whose counter hasn't risen for a while is suspected, then dead. Each Node judges alone.
- **SWIM.** Each round a Node pings one other, and if that fails asks two more to ping for it. Failing that it tells everyone the Node is suspected. The Node, hearing so, raises its incarnation number, which outranks the suspicion. Unanswered, the suspicion becomes a verdict.

Twelve Nodes, 20 trials, times in rounds. The two numbers after each name are the rounds before suspicion and before the verdict.

| Detector | Dead Node suspected / taken for dead | Running Node wrongly suspected, per 1,000 rounds: quiet / 10% lost / 30% lost / slow Node / cut link | Wrongly taken for dead | All alive again after a Partition heals | Messages per Node per round |
|---|---|---|---|---|---|
| Counters 5/15 | 5.6 / 14.6 | 47 / 139 / 954 / 215 / 52 | 0 | 4.8 | 2.0 |
| Counters 10/25 | 10.6 / 24.6 | 0 / 0 / 0.5 / 0 / 0 | 0 | 4.9 | 2.0 |
| Counters 15/35 | 15.6 / 34.6 | 0 / 0 / 0 / 0 / 0 | 0 | 4.7 | 2.0 |
| SWIM 3/9 | 5.7 / 13.9 | 0 / 1,940 / 17,916 / 0 / 0 | 0 | 8.3 | 4.0 |
| SWIM 5/15 | 7.8 / 21.9 | 0 / 1,930 / 17,740 / 0 / 0 | 0 | 8.2 | 4.0 |
| SWIM 8/24 | 10.8 / 34.1 | 0 / 1,905 / 17,339 / 0 / 0 | 0 | 8.3 | 4.0 |

With five Nodes the pattern is the same.

- **Neither ever took a running Node for dead**, at any setting, with 30% of Messages lost, a Node running at a third of the speed, or a link cut between two Nodes. That is the number 7c needs, and it is zero for both.
- **Counters need a threshold above the time news takes to get round.** At 5 rounds they suspect healthy Nodes even when nothing is wrong, because a Node often goes 5 rounds without hearing a fresh count for some other. At 10 they don't.
- **SWIM suspects easily when Messages are lost** and is always put right. One lost ping and a lost relay are enough to raise a suspicion, and no setting changes that: the patience is in the wait after the suspicion. With 30% loss every Node is under suspicion by someone most of the time.
- **SWIM is unmoved by a slow Node or a cut link,** which is what asking others to ping is for. Counters at 10 or more are too.
- **They notice a dead Node equally fast** when set to. SWIM sends twice the Messages.

At this size counters at 10/25 are as good as SWIM on everything measured and cheaper. SWIM's known advantage, that its cost and speed don't grow with the number of Nodes, doesn't show at twelve. `kvnode` defaults to counters, and `-detector swim` selects the other.

`go test -v -run TestCompareDetectors ./internal/gossip` prints the table.

## Joining and leaving (P7b.7)

A Node started with one other Node's address exchanges everything with it and is then known to all. It hosts no Group: it is a Spare. It learns the table by gossip and routes requests like any Node. On an orderly shutdown a Node says it is leaving and is marked so at once, without being suspected.

On real processes: five Nodes and a sixth started with `local.sh join 6`, told only where Node 1 is. A second and a half later all six agreed on all six. A key written through the sixth was read through the third. Then one Node was killed and the sixth stopped with a signal: four seconds later the others had the first as dead and the sixth as left.

**What this doesn't do yet.** Addresses learned by gossip are used for requests between Nodes. The Groups' own replication still uses the addresses given at start, because the Groups' Members are still the founding Nodes. Stage 7c, which gives a Spare something to host, has to finish that.

## Real runs (P7b.8)

M4 Pro, local processes, 10 s, five Nodes hosting three data Groups and the Meta Group, gossip carrying the table.

| Clients | Fault | Requests/s | Rejected / lost | Longest pause in any write | Verdicts |
|---|---|---|---|---|---|
| 64 | None | 697 | 0 / 0 | — | Linearizable; one owner per Slot |
| 8 | A Slot moved every half second | 206 | 0 / 0 | 68 ms | Linearizable; one owner per Slot |
| 8 | A Node hosting three replicas killed and restarted | 204 | 0 / 0 | 139 ms | Linearizable; one owner per Slot |
| 8 | Both at once | 204 | 0 / 0 | 117 ms | Linearizable; one owner per Slot |

Throughput is what it was in 7a (700). Gossip costs nothing visible.

**Not covered:** `kvbench` can't check that Nodes' gossip agrees at the end; the Simulation and the server tests do. Nothing ran in Docker.

### Reproduce
```
go test -run 'TestRung7b|TestOwnershipByGossip|TestMembersByGossip' ./internal/rungtest/
go test ./internal/gossip/ ./internal/cluster/
RETRY=1 DATA_GROUPS=3 harness/run.sh local 5 move-and-kill
DATA_GROUPS=3 harness/local.sh start 5 && harness/local.sh join 6 && harness/local.sh nodes
```

## Things decided while building stage 7b
- **Gossip rides on the Nodes' HTTP network** in the real shell, not on a network of its own.
- **The counters detector defaults to 10 and 25 rounds**, after 5 and 15 suspected healthy Nodes.
- **A Node's answer to a suspicion, and its catching up with its own counter after a restart, use the same rule**: what others say about a Node tells it where it had got to.
- **`kvnode -join` needs `-founders`**, the number of Nodes the store began with. Where the Groups' replicas live is worked out from it.
- **The simulated Node now follows one Leader hint** after a wrong guess, as the real one does since 7a. That changed every run, so two exposure seeds were pinned again.

## Verdict for stage 7b
| Check | Result |
|---|---|
| Exposed | **Pass**: ownership decided by gossip (`TestOwnershipByGossipIsExposed`); Membership decided by gossip (`TestMembersByGossipIsExposed`) |
| Those seeds can't be reproduced with gossip only informing | **Pass** |
| The 7a guarantees with the table carried by gossip | **Pass**: 2,400 simulated runs, 6 real |
| Every live Node agrees on who is alive and on the table once Faults stop | **Pass**: part of the End-state verdict in those runs |
| A Node started with one address is known to all | **Pass**: in simulation, in the server tests and on real processes |
| The two detectors side by side | **Done**: the table above |

## Lessons from stage 7b
1. **The naive version of this stage was to do more.** Both exposures are gossip being given a say. The correct store is the one where gossip is ignored for every decision, and the work was in making sure nothing quietly depended on it.
2. **A version number means something only if one party issues it.** Two sides of a Partition each raised the table to version 2. Nothing was wrong with either side's arithmetic.
3. **Suspicion and verdict are different measurements.** My first comparison counted any bad opinion as a false alarm and made SWIM look hopeless. What 7c will act on is the verdict, and both detectors' count of wrong ones is zero.
4. **A tight threshold fails in good weather.** Counters at 5 rounds suspected healthy Nodes with nothing wrong at all. The cause was the time gossip itself takes, which a threshold has to clear before it measures anything.
5. **A hint that sticks is worse than no hint.** Preferring Nodes thought alive, I first always picked the first of them, and a Node that wasn't the Leader got every retry. Picking at random among them fixed it.

# Stage 7c: the store looks after itself

Status: **done, waiting for approval**. A Node that dies is replaced by a Spare in every Group it was in, the Meta Group included, with nobody asking. A Node that was replaced and comes back drops its replicas and is a Spare again. Slots are moved off a Group that carries far more than its share. Three naive stores are shown failing first. 3,200 simulated runs with all of it on are clean, and so are 7 real ones. **Evening out load makes this machine slower**, for the reason 7a found: one disk.

Environment: the 7b Simulation with two Spares, counters detector unless said. Two things changed in the Simulation itself:
- **A Node that restarts now forgets its table** and holds the one the store was founded with until gossip tells it more. A real Node always did. The Simulation had kept the table across a crash, and that kept the naive store looking correct.
- **A scenario can lose a Node for good.**

## Exposed: a Node keeps the replicas its table names it for (P7c.2)
The naive rule is the obvious one: start a replica of every Group the table says you are in, drop the data of every Group it says you aren't. A Node that took a lost Node's place isn't in the founding table. So when it restarts it drops data its Group is counting on, and when gossip catches up a moment later it starts again, empty, as a Member in good standing.

30 seeds per scenario, naive store:

| Scenario | Runs with a replica dropped while still a Member |
|---|---|
| A Node lost for good, nothing else | 0 |
| A Node lost, the others crashing and returning | 8 |
| Slots moving, Nodes crashing and returning | 7 |
| A Node lost, then Nodes restarted in overlapping turns | 12 |

A wrong drop usually costs nothing visible: the Leader sends the empty Member everything again. It costs an Acknowledged write when one other Member is behind and the third is away at that moment, because the empty Member and the one behind then elect a Leader that never saw what was Committed. I searched 4,000 seeds of the last scenario for that:
- **Seed 3480: the History isn't Linearizable.**
- **Seed 1585: a core trips its own check**, asked for an Entry it no longer has.

## Exposed, part two: one Meta Member's word, no wait
The Meta Leader replaces a Node the moment its own gossip gives the Node up. In "Slots moving, Nodes crashing and returning" every Node is back within 350 units, and gossip gives a Node up after 250.

| | Nodes replaced in 30 runs | Replicas dropped |
|---|---|---|
| Naive | 73 | 184 |
| A Majority and a wait | 0 | 0 |

Each replacement is a full copy of every Group the Node was in, for a Node that was only restarting.

## Exposed, part three: rebalancing with no damping (P7c.5)
The Meta Leader moves the busiest Slot off the busiest Group whenever that Group is over the line, going by the last count it heard. The table is under "Load" below: about 90 Moves a run where a handful would do, a third of the clients' requests lost to frozen Slots, and the load no more even at the end.

### Reproduce
```
go test -run 'TestReplicasThatFollowTheTable|TestOneWordReplacement|TestNaiveRebalancing' ./internal/rungtest/
```

## Fix: replacing a dead Node (P7c.1, P7c.3)
- **The Slot table holds each Group's Member list.** The Meta Group records the change it wants, the Group's Leader makes it in its own Log, one Member at a time, and reports. The table only follows: the Group's Log decides.
- **A Node is replaced when a Majority of the Meta Group's Members have each thought it dead for longer than a wait.** They say so by gossip. The decision is an Entry in the Meta Group's Log.
- **A Node drops a replica only on the Group's say-so.** The table prompts it to ask. The Group's Leader confirms it still leads, then shows its Committed list. The Node keeps its Term and vote.

Same scenarios, 30 seeds each:

| Scenario | Naive: runs failing | Fixed: runs failing | Fixed: Nodes replaced |
|---|---|---|---|
| A Node lost for good | 0 | 0 | 30 |
| A Node lost, others crashing | 9 | 0 | 30 |
| Nodes away for 600–800 units, then back | 0 | 0 | 47 |
| Partitions of 600 units | 0 | 0 | 94 |
| Slots moving, Nodes crashing | 7 | 0 | 0 |
| A Node lost, then rolling restarts | 12 | 0 | 30 |

`TestRung7cReplacing`: 2,400 runs over all twelve scenarios of 7a, 7b and 7c, with counters, with SWIM and with the busy workload. None unsafe, none with a wrong drop. 2,423 Nodes replaced, 3,973 replicas dropped. In the 200 runs where every crashed Node is back within 350 units, 2 Nodes were replaced: a Node that crashes again just after restarting looks away for both spells.

### A Partition does what it should, and one thing I didn't expect
Two Nodes cut off for a long time are replaced by the other side. Their side replaces nobody: it has no Majority of the Meta Group. That is `TestTheMinoritySideIsReplacedAndReturnsAsSpares`.

What I didn't expect: **a Group whose Majority is on the cut-off side can't be changed until the network heals**, because only its Leader can change its Members and its Leader is over there. A change still wanted for it at that point is carried out even though the Node to be removed is back.

### Bugs found on the way
1. **Members disagreed on which Entry set their Member list.** After a Snapshot a Member knew its list "as of the Snapshot", so the index differed from Member to Member, and a Leader waiting for the table to catch up with its list waited for ever. A Snapshot now carries the index. Found by seed 28 of the long Partitions.
2. **A Spare that died while being added blocked its Group for 2,000 units.** The Meta Group had already named another Spare, and the Leader was still waiting on the first. A Leader can now call an addition off (A§6.5). Found by one run in 3,600.
3. **My first explanation of bug 2 was wrong.** I thought a joining replica was standing for election on an old table, fixed that, and the run failed exactly as before. The fix stays, because a replica with an empty Log that takes itself for a Member is the naive store's failure. I then traced the run and found the real cause.
4. **On real processes a Spare had no address for the founders** and never answered them, **dropping a replica failed** because the Log is a directory there, and **a Leader held up by its disk reported no load**, so its Group looked idle and was given another Slot. The server test found the first two, a real run the third.

### One design statement that was wrong
At the design step I offered "never cancel a Move; replacement gives the Group its Majority back", and it was chosen. **Replacement can't do that.** A Group that has lost its Majority can't change its own Members, so nothing can be added to it. The Move waits until enough Members return or an operator runs Unsafe recovery. A Group that has only lost one Member carries on with the Move while the Member is replaced. `TestAMoveWaitsOutALostMajority` shows both. This needs confirming, since the choice was made on my wrong description.

## Load, and moving Slots by it (P7c.4–P7c.6)
**What a read costs.** One Group, 64 clients, 10 s, on real processes:

| Requests | Answered a second |
|---|---|
| Writes only | 1,371 |
| Reads only | 90,712 |

A read costs a sixty-sixth of a write. Load counts a write as 64 and a read as 1.

**What rebalancing does.** Twenty clients, no Faults, 30 seeds each. "One busy Group" sends four requests in five to the keys of the Slots Group 1 starts with. "One busy Slot" sends them to a single key. "Shifting" changes the busy Group every 1,200 units.

| Load | Rebalancing | Moves per run | Units frozen | Requests answered | Busiest Group, × the mean |
|---|---|---|---|---|---|
| Even | Off | 0 | 0 | 1,657 | 1.51 |
| Even | Naive | 87.4 | 2,220 | 1,061 | 2.14 |
| Even | Damped | 1.1 | 25 | 1,677 | 1.09 |
| One busy Group | Off | 0 | 0 | 1,370 | 2.71 |
| One busy Group | Naive | 91.0 | 2,400 | 898 | 2.41 |
| One busy Group | Damped | 2.2 | 45 | 1,572 | 1.13 |
| One busy Slot | Off | 0 | 0 | 1,370 | 2.71 |
| One busy Slot | Naive | 75.6 | 2,121 | 753 | 2.61 |
| One busy Slot | Damped | 2.0 | 42 | 1,405 | 2.52 |
| Shifting | Off | 0 | 0 | 1,402 | 2.04 |
| Shifting | Naive | 91.5 | 2,410 | 909 | 2.22 |
| Shifting | Damped | 6.0 | 124 | 1,432 | 1.52 |

- **Even load isn't even.** Twenty-four keys hash unevenly over eight Slots, so one Group starts at 1.5× the mean. One Move fixes it, and after that nothing moves.
- **One busy Slot can't be helped**, and the damped store stops after two Moves of the other Slots. The naive one passes the busy Slot from Group to Group for the whole run.
- **Shifting load is followed, a step behind.** The busy Group changes every 1,200 units and a Move is followed by 500 units with no other.

**The lines.** 1.3 and 1.1 were where the design started.

| Lines | Even: Moves | One busy Group: Moves | One busy Group: busiest × mean |
|---|---|---|---|
| 1.3 and 1.1 | 1.8 | 3.4 | 1.11 |
| 1.5 and 1.2 | 1.1 | 2.2 | 1.13 |

As even, with a third fewer Moves. The store uses 1.5 and 1.2.

**With five clients the figures are noise.** A Group then sees about four requests in each window. The damped store made about 3 Moves a run under even load and went on making them. The balancing tests use twenty clients for that reason.

**Which Slot to move changed after a real run.** The first rule was the biggest Slot no more than half the gap between the two Groups. A real run left two busy Slots together in one Group at 1.6× the mean, when moving one would have levelled all three. The rule is now the Slot nearest half the gap, and none over three quarters of it.

## Real runs (P7c.8)
M4 Pro, local processes, 10 s, 64 clients retrying in Sessions, five Nodes hosting three data Groups and the Meta Group.

| Run | Requests/s | Lost | Longest pause in any write | What the store did | Verdicts |
|---|---|---|---|---|---|
| Busy Group, automation off, 8 Slots | 885 | 0 | — | Nothing | Linearizable; one owner per Slot |
| Busy Group, automation on, 8 Slots | 729 | 0 | — | Two Moves. Group 1 went from 89% of the load to 33%, with 38% and 30% for the others | Same |
| Node 2 killed for good, a Spare running | 746 | 0 | 37 ms | Node 2 judged dead 5 s after the kill (`-dead-wait 2s`); Node 6 took its place in the Meta Group and Group 1 | Same, with Node 6 as a Member |
| A Slot moved every half second | 715 | 0 | 43 ms | No Slot moved by itself | Same |
| Node 1 killed for 3 s and restarted | 720 | 0 | 39 ms | Node 1 not replaced | Same |
| Both at once | 683 | 0 | 60 ms | Nothing by itself | Same |

- **Evening out the load lowered throughput, 885 to 729.** With the load on one Group, one Log carries nearly every write and each flush of the disk covers many of them. Spread over three Groups, three Logs queue for the same disk. It is 7a's finding again: several Groups pay off where Nodes have disks of their own.
- **Automation costs nothing when it has nothing to do.** 715 and 720 here against 697 to 704 in 7a and 7b.
- **A moved Slot was frozen for 76 to 380 ms** in these runs, against a median of 52 ms in 7b. These have 64 clients where those had 8, and the freeze spans three Entries that each wait their turn at the disk.
- The automation-on run was made twice. The first showed the vanishing-load bug above. The table is the second.

The same on Nodes in one process (`TestADeadNodeIsReplacedAndReturnsAsASpare`): a stopped Node is replaced in both its Groups, the Spare holds all 16 keys itself, and the Node, started again, drops both replicas and starts neither after another restart.

**Not covered:**
- **No plain run with no Fault at 64 clients** was repeated, so "automation costs nothing" rests on the Fault runs.
- **A returning Node on separate processes** was not run. The in-process test covers it.
- **No real run of the naive stores.** They exist in the Simulation only.
- **With automation off a Node reports no load**, so the first row has no load shares.
- Nothing ran in Docker, as in 7a and 7b.
- `kvbench` still sends no scans or Transactions.

### Reproduce
```
go test -run 'TestRung7c' ./internal/rungtest/
go test ./internal/automation/ ./internal/cluster/ ./internal/server/
KV_MEASURE=1 go test -run 'TestMeasure' -v ./internal/rungtest/
RETRY=1 DATA_GROUPS=3 CLIENTS=64 harness/run.sh local 5 kill-for-good
RETRY=1 DATA_GROUPS=3 SLOTS=8 CLIENTS=64 harness/run.sh local 5 skewed-load      # AUTO=0 to compare
```

## Things decided while building stage 7c
- **One wait before replacing, for every size of Group**, where the design step chose a faster path for a Group one failure from stopping. With three Members that is every Group, always. Left to me at the design step.
- **A Node that says it is leaving is replaced like a dead one**, after the wait.
- **The decisions live in one pure package**, `internal/automation`, that the Simulation and the real Node both call. The Simulation tests the code the real Node runs.
- **A Leader can call off an addition** (A§6.5). New behaviour in the Rung 6 core.
- **A Snapshot carries the index of the Entry that set its Member list.** The file format changed; data directories from before don't open.
- **`kvnode -auto` is on by default.** `-dead-wait` defaults to 5 s.
- **Real load windows are half a second**, with 2.5 s after a Move and 5 s of rest for a moved Slot.
- **The two measurement tests run only with `KV_MEASURE=1`.** The suite was within 20 s of Go's ten-minute limit with them.
- **Plan changes:** real-shell work planned under P7c.1 and P7c.4 was done in P7c.7, and P7c.5 and P7c.6 went in as one commit.

## Verdict for stage 7c
| Check | Result |
|---|---|
| Exposed | **Pass**: table-following replicas (`TestReplicasThatFollowTheTableAreExposed`, seeds 4, 1585, 3480), one word and no wait (`TestOneWordReplacementIsExposed`), no damping (`TestNaiveRebalancingIsExposed`) |
| Those seeds can't be reproduced after | **Pass** |
| A Node lost for good is replaced everywhere, the Meta Group included, with no hand on it | **Pass**: every such run of the suite; one real run |
| A minority side replaces nobody | **Pass** |
| A returned Node never drops data its Group counts on | **Pass**: no wrong drop in 3,200 runs, 3,973 drops in the replacing suite alone |
| One busy Group is relieved | **Pass**: 2.71× the mean to 1.13× in 2.2 Moves; 89% to 33% on real processes |
| Load that is even, or can't be evened, is left alone | **Pass**: 0.1 Moves a run or fewer in the last 1,500 units |
| Everything on at once | **Pass**: 600 runs |
| A Move stuck on a lost Majority finishes after replacement | **Not as designed**: it finishes when Members return. See above |

## Verdict for Rung 7
The four checks of the README:

| Check | Result |
|---|---|
| Exposed | **Pass**: seven naive stores across the three stages, each with seeds kept as tests |
| Guarantee: each key owned by exactly one Group at any moment; single-key operations Linearizable while keys move | **Pass**: 8,000 simulated runs over the three stages and 26 real ones, none with two owners, none not Linearizable |
| Numbers: at least 0.9× Rung 6 | **One Group: pass (0.97×). Three Groups on one machine: miss (0.52×)**, and evening out load on one machine costs a further 18% |
| Retro | This file |

## Lessons from stage 7c
1. **The Simulation was kinder than a real Node.** It kept a Node's table across a crash. The naive store passed every scenario until the Simulation forgot what a real Node forgets.
2. **Following the table is gossip deciding, by another door.** Stage 7b's rule was that gossip informs. A Node acting on the table it happens to hold breaks that rule just as surely as one taking Members from gossip did.
3. **A fix that changes nothing wasn't the fix.** The run failed identically after my first repair, down to the totals. Identical totals across 3,600 runs said the code path I had changed was never on the failing path.
4. **An identifier must mean the same on every Member.** "The index of my Member list" quietly meant "of my Snapshot" on some. Nothing was unsafe; one Group just never finished.
5. **Check a claim against the mechanism before offering it as a choice.** "Replacement heals a Group without a Majority" sounded right and can't happen. The user chose on it.
6. **A measurement can vanish as well as be wrong.** A Leader too busy to answer "do you lead?" in 100 ms reported nothing, and the busiest Group looked idle.
7. **Balance is a cost here.** The store moved load exactly as designed and the machine got slower. Whether a decision helps depends on what the bottleneck is, and this one has one disk.
