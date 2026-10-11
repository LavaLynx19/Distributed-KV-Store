# Rung 9 retro: the Leaderless variant

Status: **all three stages done and approved.** Rung 9 is built in three stages (A§13.1). This file has a part per stage, and the verdict for the whole Rung at the end.

Stage 9a in one paragraph: a second store with no Leader, in which a key is a set of strings kept on three Home Nodes of five, a write is acknowledged when W Nodes have it, and a Node that isn't a Home Node stands in for one that can't be reached. With two Nodes cut off from three, the consistent store of Rung 8 gets 19% of its usual work done and the Leaderless variant at W=1 R=1 gets 92% done. With sets where add wins, 4,500 simulated runs lose no Acknowledged add. It does not yet always end Converged: that is stage 9b.

# Stage 9a: Home Nodes, W and R, Hints, and a set where add wins

Environment: a Simulation of its own (`internal/lsim`), since the Leaderless core has a different contract from the consensus core and can't run in the other one. Five Nodes, five clients each talking to one Node, 6 keys with 4 possible elements each; 40% reads, 40% adds, 20% removes. A write to disk takes 1 to 6 units and a Node does one thing at a time.

A run gets these verdicts (A§13.11). **Converged:** every Home Node of every key holds the same, and no Hint is left. **No Acknowledged add lost** and **nothing comes back:** checked exactly, from the mark each addition was given and the marks each remove took out. **Availability** and **stale reads** are numbers.

## Exposed: two shortcuts

300 seeds per cell, W=2 R=2. Each store has one shortcut and is otherwise where stage 9a ends.

| Runs of 300 in which… | No Faults | One long Partition | Partitions | Crashes | Both |
|---|---|---|---|---|---|
| **Last-write-wins:** an Acknowledged add was lost | 62 | 82 | 85 | 80 | 80 |
| …a removed element came back | 83 | 66 | 91 | 100 | 90 |
| **Hints never delivered:** Hints left at the end | 166 | 300 | 300 | 300 | 300 |
| …an Acknowledged add no Home Node has | 0 | 103 | 1 | 0 | 6 |

**Last-write-wins loses adds with no Fault at all, in 62 runs of 300.** The design expected loss only between writes made on two sides of a Partition. But a write here has to read the set, change it and write it back whole, so two clients adding to one key at the same moment overwrite each other on a healthy store. A Partition adds to it (82) and is not needed for it.

**Hints never delivered.** A write made while a Home Node was cut off succeeds on the strength of a stand-in. If the stand-in never hands it over, the Home Nodes never have it: in 103 runs of 300 under a long Partition, an add a client was told had succeeded is on no Home Node at the end. Even with no Fault, 166 runs end with Hints, because a Home Node that is merely slow to answer is given a stand-in too.

## The fix

- **A set where add wins (P9a.5).** Each addition is marked with the Node that took it and that Node's count for the key. A key keeps the marks it has seen. Two copies are joined mark by mark; a remove takes out the marks it has seen and no others. An addition needs to read nothing first.
- **Hints delivered, writes sent again (P9a.6).** A stand-in keeps offering its Hint to the Home Node and drops it when that Node has it. The Node that took a write goes on sending it to Home Nodes that haven't answered, for 90 ticks, and gives every one of them a stand-in, not only as many as W needs.

## A bug the runs found

**A Home Node that had been away was buried in repeats.** Everything it had missed was sent to it again and again, and it wrote each copy to disk although the copy changed nothing. Its disk fell further and further behind, its answers came later, and so more was sent again. Hints for it were then never taken. A write that brings nothing new now costs no write to disk.

## Results

300 seeds per cell, 4,500 runs. No run lost an Acknowledged add, brought a removed element back, or left a Hint.

| Runs of 300 not Converged | No Faults | One long Partition | Partitions | Crashes | Both |
|---|---|---|---|---|---|
| W=1 R=1 | 0 | 300 | 11 | 8 | 199 |
| W=2 R=2 | 0 | 300 | 3 | 9 | 134 |
| W=3 R=1 | 0 | 300 | 3 | 9 | 160 |

**After one long Partition the store never ends Converged.** A write is sent again for 90 ticks and the Partition lasts far longer. If the stand-in for a Home Node is on the far side too, nothing ever carries the write across. Most of what differs at the end is bookkeeping and not what a read shows: of 424 keys that differed in 50 such runs, 309 showed the same elements on every Home Node. Stage 9b's Exchange is for this.

## The comparison

Two Nodes cut off from three for the whole of the Fault time, 300 seeds. "Work done" is the requests answered that began during the Partition, as a share of those answered over the same time from the same seeds with no Fault.

| Store | Work done through the Partition | Acknowledged adds lost |
|---|---|---|
| Consistent (Rung 8: 5 Nodes, 3 Groups) | 18.8% | none; every run Linearizable |
| Leaderless, W=1 R=1 | 91.6% | none in 300 runs |
| Leaderless, W=2 R=2 | 43.5% | none |
| Leaderless, W=3 R=1 | 33.1% | none |
| Leaderless, last-write-wins, W=2 R=2 | — | lost in 82 runs of 300 |

- **The consistent store does less than a fifth of its work,** not the three fifths one might expect from three Nodes of five. Each Group has its Majority on one side or the other, so each side can reach some keys and not the rest, and a client waits 1,200 units on the first key it can't reach.
- **The measure matters.** By share of requests answered, the consistent store scores 91%, because a client kept waiting asks for little and so fails little. That number is in no table here.
- **W=1 R=1 pays in stale reads:** 3.0% of reads during the long Partition run missed an element whose addition had been acknowledged, against 0.1% with no Fault.
- **W=2 R=2 and W=3 R=1 are not much more available than they are safe.** A write finds stand-ins on either side, but a read or a remove needs R Home Nodes of the key, and the small side often has one.

## What the design got wrong

- **Last-write-wins "orders writes on one side correctly"** (A§13.5 as first written). It doesn't, for writes at the same moment. Corrected.
- **Availability as a share of requests answered.** See above. Now work done against a run with no Fault.
- **"Lost" by the times of requests.** A remove can rightly take out an addition that was made before the remove ended, which times alone can't tell from a loss. The check uses the marks. Times are used only for last-write-wins, which has none.

## Not covered

- **Converged under Faults,** time-to-live, scans, repair on read: stage 9b.
- **A remove can miss an addition** when R and W don't overlap: the client is told the element is removed and it is still there, because the Home Nodes the remove read from hadn't got the addition yet. This isn't counted yet.
- The comparison's two stores run in two Simulations with the same shape of Fault, not the same code. Their clients differ in one way that matters: the consistent store's wait four times as long before giving up.
- A Node lost for good, with its disk, in no scenario yet.
- Real processes and Docker: stage 9c.

## Reproduce

```
go test ./internal/leaderless ./internal/lsim
go test ./internal/shardtest -run Leaderless
KV_MEASURE=1 go test ./internal/lsim -run TestMeasureRung9a -v
KV_MEASURE=1 go test ./internal/shardtest -run TestMeasureRung9Availability -v
```

Seeds kept as tests: last-write-wins (long Partition 2, 4, 5; no Faults 10, 13, 20), Hints never delivered (long Partition 3, 12, 13).

## Verdict (9a)

No Acknowledged add lost and nothing back, at all three settings: yes, 4,500 runs. Converged: only with no Faults. Approved by the user.

# Stage 9b: the Exchange, repair on read, removes, time-to-live, scans

Environment: as stage 9a, with one more scenario: a Node down for most of the Fault time, far longer than any write is sent again for. The busy store gives 30% of additions a time-to-live, makes one request in ten a scan, and sets two Nodes' clocks 80 units slow and 120 fast.

## Exposed

300 seeds per cell, runs of 300 that failed a verdict.

| Shortcut | No Faults | Long Partition | Partitions | Crashes | Long outage | Partitions and crashes |
|---|---|---|---|---|---|---|
| No Exchange, no repair, W=2 R=2: not Converged | 0 | 300 | 3 | 9 | 0 | 134 |
| A remove forgotten at once: an element back | 276 | 295 | 287 | 278 | 285 | 282 |

**No Exchange.** After a Partition that outlasts the sending again, Home Nodes stay apart for good, in every run.

- **A long outage of one Node doesn't show it** (0 of 300). The Node's stand-ins hold everything it missed and hand it over when it returns. It takes a Partition, where the stand-in can be on the far side as well.

**A remove forgotten at once.** Nearly every run, with or without Faults, ends with an element back that a client was told was removed. A Node that drops its record of a remove can't tell that element from one it has never heard of, and Home Nodes hear of things in different orders all the time.

## The fix

- **The Exchange (P9b.2).** Every 10 ticks a Node offers the next Node in turn a fingerprint of each key the two are both Home Nodes of. The other sends back what it holds of the keys that differ and asks for the rest. Three messages; a key is compared whole, by one fingerprint, not by ranges.
- **The record of a remove is kept (P9b.3),** for good.
- **Repair on read (P9b.4).** A read is answered at R, waits for the other Home Nodes, and sends what it found to those that had less.
- **Time-to-live (P9b.5).** An addition carries a deadline from the clock of the Node that took it. Each Node shows the element until its own clock passes that. Nothing is removed from what is stored.
- **Scans (P9b.5).** Every Node is asked for the keys in the range it is a Home Node of. The scan answers when all have, or after 10 ticks with what it has.

## Results

7,200 runs, 300 seeds per cell: the three settings and the busy store under the six scenarios. Every run ended Converged, with no Acknowledged add lost and nothing back.

**What each way of converging is worth,** at W=1 R=1, runs of 300 not Converged:

| | Long Partition | Partitions | Crashes | Partitions and crashes |
|---|---|---|---|---|
| Sending again and Hints only | 300 | 11 | 8 | 199 |
| With repair on read | 0 | 0 | 0 | 0 |
| With the Exchange | 0 | 0 | 0 | 0 |

- **Repair on read alone converged every run.** That is the test, not the store: there are 6 keys and clients read all of them all the time. A key nobody reads would never be repaired, and no run here has one.
- **Neither repair nor the Exchange lowers the stale-read rate.** At W=1 R=1 under the long Partition it is 2.96% with neither, 2.76% with repair, 2.87% with the Exchange, 2.69% with both. A read is stale while its side of the Partition hasn't got the write, and nothing can carry it across until the Partition ends.

**Stale reads and removes that missed,** stage 9b, 300 seeds:

| Setting | Stale reads, no Faults | Stale reads, long Partition | Removes that missed, long Partition |
|---|---|---|---|
| W=1 R=1 | 0.10% | 2.69% | 2.85% |
| W=2 R=2 | 0.00% | 2.36% | 3.43% |
| W=3 R=1 | 0.00% | 1.57% | 0.07% |

- **A remove that missed** is one the client was told succeeded and that took nothing out, though an addition of that element had been acknowledged before it began and is there at the end. The Home Nodes it read from hadn't got the addition. About 3% of removes during a long Partition at the first two settings. It is the cost of add wins that a client can see: "removed" means "what this side had seen of it is removed".

**Expiry.** With one Node's clock 100 units slow and another's 150 fast, an element given 400 units to live stopped showing 250 units apart on those two Nodes: exactly as far as their clocks are. What is stored stayed the same on every Home Node.

## What the design got wrong

- **"Comparing by hashes of ranges"** was left to settle and wasn't done. Every shared key's fingerprint is sent in every Exchange. With 6 keys that is nothing; with a million it is the whole cost of the Exchange.
- **Repair on read was expected to lower stale reads.** It doesn't, measurably.

## Not covered

- **A Node that loses its disk.** It would come back counting its additions from one again, and its new additions would carry marks the others have already seen, and be taken for old news. A Node that loses its disk can't return under the same number. Not built, and no scenario loses a disk.
- **A key nobody reads** and repair on read without the Exchange, as above.
- **The cost of the Exchange** with many keys.
- **Time-to-live under last-write-wins** isn't offered: the deadline belongs to an addition's mark, and last-write-wins has none.
- An expired element is hidden and never deleted. What is stored only grows.
- Real processes and Docker: stage 9c.

## Reproduce

```
go test ./internal/leaderless ./internal/lsim
KV_MEASURE=1 go test ./internal/lsim -run TestMeasureRung9b -v -timeout 30m
```

Seeds kept as tests: no Exchange (long Partition 1, 2, 3), a remove forgotten (no Faults 2, 4, 5).

## Verdict (9b)

Replicas converge after healing: yes, 7,200 runs. Approved by the user.

# Stage 9c: real processes, and a real Partition

## What was built

- **`kvnode -leaderless`** (`internal/lserver`): one loop owns the core, takes whatever has arrived, steps through it, writes what that asks to be stored with one sync, and only then sends and answers. The Node's file is a run of checksummed frames, replayed at start; it is never trimmed. Nodes talk over the TCP transport the consistent store uses. The HTTP routes are those of A§13.3.
- **`kvbench -leaderless`**: clients add to, remove from and read sets, each keeping to one Node. It reports whether the Nodes end Converged, and adds lost and elements back by the times of the requests, which is all a client outside the store has to go by.
- **`harness/leaderless.sh`** for local processes, and **`harness/partition.sh`** for Docker with the network cut.
- **Docker for both stores.** The compose file gives every Node extra flags from `KVNODE_ARGS`: three lines, agreed by the user beforehand. The Leaderless variant needed nothing more. The store of Rung 8 also forwards requests and gossips between Nodes over HTTP, at addresses that mean nothing between containers and pass through no proxy. Each link got a second proxy, and a Node is told to reach the others through it (`kvnode -forward-via`).

An in-process test runs five real Nodes with disks and sockets: adds, a remove and a scan through different Nodes, a Node stopped while writes go on, and the Node brought back from its disk and up to date, Converged, within 10 s.

## Real runs

M4 Pro, 10 s each. Run by the user's leave: seven runs.

**Local processes, five Nodes, 64 clients, no Partition possible.**

| Run | Answered/s | p50 | Converged | Acknowledged adds lost |
|---|---|---|---|---|
| Consistent store, Rung 8 (from `retros/rung-8.md`) | 710 | 88 ms | — | — |
| Leaderless, W=2 R=2 | 647 | 94 ms | yes | 0 of 2,644 |
| Leaderless, W=1 R=1 | 1,280 | 37 ms | yes | 0 of 5,180 |
| Leaderless, W=2 R=2, Node 1 killed for 3 s and restarted | 644; 87% of its rate while the Node was down | 87 ms | yes | 0 of 2,604 |

- **At W=2 R=2 the Leaderless variant is no faster than the consistent store** here: 647 against 710. Both wait for two disks, and the disks are one disk. Having no Leader saves nothing when the disk is the wait.
- **W=1 R=1 is twice as fast,** because it waits for one disk.
- A read takes as long as a write (88 ms against 98 ms at W=2 R=2), though it writes nothing. A Node answers everything it took in one batch after that batch's one sync, reads included.

**Docker, five Nodes, 16 clients each keeping to one Node; Nodes 1 and 2 cut off from 3, 4 and 5 for 4 s.** "Through the Partition" is answers per second while it lasted, against before it.

| Run | Through the Partition | Before it | Verdicts |
|---|---|---|---|
| Consistent store, Rung 8 | 73% (2,306/s) | 3,153/s | Linearizable; Groups identical |
| Leaderless, W=1 R=1 | 77% (2,976/s) | 3,878/s | Converged; 0 of 13,598 adds lost |
| Leaderless, W=2 R=2 | **2%** (48/s) | 2,889/s | Converged; 0 of 5,540 lost |
| Leaderless, W=2 R=2, last-write-wins | 1% (21/s) | 2,666/s | Converged; **5 of 4,231 adds lost, 5 elements back** |

- **This is not what the Simulation said, and the difference is the finding.** There the consistent store did 19% of its work through a Partition and the Leaderless variant at W=2 R=2 did 44%. Here it is 73% and 2%.
- **Why the consistent store did well.** Where a Group's Members are is fixed, and with Nodes 1 and 2 cut off, every data Group had its Majority on the large side. The nine clients there lost nothing. And the seven on the small side were told "no Majority" at once, 5,530 times, where the Simulation's clients wait and try again for 1,200 units.
- **Why W=2 R=2 collapsed.** About three keys in ten have two of their three Home Nodes on the small side. A read or a remove of one of those, from the large side, can't find R=2 Home Nodes, and the Node waits 1.2 s before it says so. Clients here send the next request the moment the last is answered, so within a few requests every client is waiting on such a key. Few requests failed (49 in the whole run). They were each a second long.
- **A Leaderless Node doesn't know a Node is unreachable; it only knows nobody has answered yet.** The consistent store's Node knows it has no Leader and says so in a millisecond. Nothing here remembers that a Node didn't answer a moment ago. How fast a store can say no decided this comparison more than whether it could say yes.
- **W=1 R=1 came through at 77%,** and that is two Nodes of five answering for themselves: the seven clients on the small side were served too.
- **Last-write-wins lost Acknowledged adds on real processes too:** 5 of 4,231.
- **Throughput here can't be set beside the local runs.** A write takes 3 to 6 ms in Docker and 85 ms outside it: Docker Desktop's disk doesn't wait for the drive as macOS does when asked to.

## What the design got wrong

- **"Availability" has no one number.** Three were used in this Rung and they rank the stores differently: share of requests answered (flatters a store whose clients wait), work done against a run with no Fault in the Simulation (clients pause between requests, and wait long on the consistent store), and the same on real processes (clients don't pause, and a slow no costs everything).
- **The Simulation's Partition was kinder to the Leaderless variant and harder on the consistent store than the real one.** It splits the Nodes at random, so some Groups keep their Majority on the small side, and its clients behave differently in the two stores.
- **The cost of a slow no was in nobody's design.** W and R were chosen for what they promise, not for how long a Node waits to find they can't be met.

## Not covered

- **One Partition, one shape, once per store.** Nodes 1 and 2 against the rest. A cut that leaves a data Group's Majority on the small side would treat the consistent store very differently.
- **Failing fast.** A Node that remembered who hadn't answered lately could say `too_few_nodes` at once. Not built.
- **Lost and back on real processes are judged by times,** not by marks: the exact check is in the Simulation only.
- The Leaderless Node's file is never trimmed, and nothing of the Exchange's cost with many keys was measured: 50 keys here.
- Time-to-live and scans were not part of the real load.
- `kvctl` knows nothing of the Leaderless variant.

## Reproduce

```
go test ./internal/lserver
CLIENTS=64 W=2 R=2 harness/leaderless.sh            # or kill-node, pause-nodes
harness/partition.sh consistent
W=1 R=1 harness/partition.sh leaderless              # LWW=1 for last-write-wins
```

## Verdict (9c)

Done and approved by the user.

# Rung 9 verdict

| Check (README) | Result |
|---|---|
| Exposed: the Leaderless variant accepts writes on both sides of a Partition and Acknowledged writes are overwritten | **Done.** Last-write-wins lost an Acknowledged add in 82 runs of 300 under a Partition in the Simulation, and 5 of 4,231 adds under a real one. It also loses them with no Fault at all (62 runs of 300) |
| Replicas converge after healing | **Done:** 7,200 simulated runs, and every real run, including under a real Partition |
| Reported next to the consistent store as availability during the Partition against writes lost | **Done, with a caution.** Simulation: consistent 19% of its work, Leaderless 92% at W=1 R=1 and 44% at W=2 R=2, nothing lost with sets where add wins. Real Partition: consistent 73%, Leaderless 77% at W=1 R=1 and 2% at W=2 R=2. The two don't agree, for the reasons above |
| Numbers | There is no Rung before it to hold it to. Beside the consistent store on the same machine: 647 against 710 requests/s at W=2 R=2, 1,280 at W=1 R=1 |
| Retro | This file |

## Lessons

- **The price of availability was not where the Rung went looking for it.** With sets where add wins, no acknowledged write was lost in any run. What was paid instead: reads that miss a write (3% during a Partition), removes that take nothing out (3%), a value that isn't a string any more, and a record of every remove kept for ever.
- **A store is as available as its no is fast.** The store built to answer during a Partition did a fiftieth of its work through one at W=2 R=2, because it couldn't tell "not yet" from "never".
- **Last-write-wins is wrong before it is unlucky.** It was meant to show loss under a Partition. It lost writes on a healthy store, because replacing a whole value needs a read first, and two reads can see the same thing.
- **A comparison between two stores is also a comparison between two clients.** Whether a client waits, how long, and whether it pauses changed the answer more than the stores did.
- **Sending again must cost nothing when it brings nothing.** Twice in this project, in Rung 8's Transactions and here, a Node behind was kept behind by being told the same things over and over.
