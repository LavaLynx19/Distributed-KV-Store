# Rung 9 retro: the Leaderless variant

Status: **stage 9a done, waiting for approval. Stages 9b and 9c not started.** Rung 9 is built in three stages (A§13.1). This file has a part per stage, and the verdict for the whole Rung at the end.

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

No Acknowledged add lost and nothing back, at all three settings: yes, 4,500 runs. Converged: only with no Faults. Waiting for approval before stage 9b.
