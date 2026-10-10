# Rung 7 retro: several Groups

Status: **stage 7a done and waiting for approval; 7b and 7c not started**. Rung 7 is built in three stages (A§11.1). This file gets a part per stage.

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
| Moves | 47 |
| Moves and crashes | 42 |
| Moves and Partitions | 35 |
| Moves interrupted at each step | 42 |
| Moves, crashes and Partitions | 35 |

Every Node caches the table and refreshes it every 20 units. For that long after a Move, some Node still sends the Slot's keys to the Group that used to own them. That Group has given the Slot away, and answers anyway: a read finds nothing, or a write lands where nobody will look for it.

A stale table is normal. It can't be prevented, only made harmless.

## Exposed, part two: a table flipped with nobody confirming (P7a.5)

Now every Group checks, by its own Log, that it owns what it serves. The Move is the naive one: the Meta Group changes the owner in the table at once, and the Groups follow the table. The old owner, seeing the table name someone else, sends the Slot over as it stands and lets go when the other has it.

| Faults (50 seeds) | Two Groups serving one Slot | Not Linearizable |
|---|---|---|
| No Moves | 0 | 0 |
| Moves | **50** | 11 |
| Moves and crashes | **50** | 12 |
| Moves and Partitions | **50** | 7 |
| Moves interrupted at each step | **50** | 3 |
| Moves, crashes and Partitions | **50** | 8 |

Every run with a Move has two owners. Seed 1: Slot 0 is served by Groups 1 and 3 at t=380.

The new owner starts when it is given the Slot. The old one stops when its own "let go" is Committed, a little later. In between both answer. Most runs get away with it, because the window is short and a client has to write to the old owner inside it. The ones that don't lose that write.

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
