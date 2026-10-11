# Rung 9 retro: the Leaderless variant

Status: **stage 9a done and approved. Stage 9b done, waiting for approval. Stage 9c not started.** Rung 9 is built in three stages (A§13.1). This file has a part per stage, and the verdict for the whole Rung at the end.

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

Replicas converge after healing: yes, 7,200 runs. Waiting for approval before stage 9c.
