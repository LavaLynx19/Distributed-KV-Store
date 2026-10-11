# Rung 8 retro: Transactions across Groups

Status: **stage 8a done, waiting for approval. Stage 8b not started.** Rung 8 is built in two stages (A§12.1). This file has a part per stage, and the verdict for the whole Rung at the end.

Stage 8a in one paragraph: a Transaction whose keys are in several Groups commits in all of them or none, with every step in the Log of its Coordinator Group. 5,400 simulated runs under crashes, Partitions and Moves committed 112,948 such Transactions with none half-committed, no History that isn't Linearizable, and nothing left Locked. Four shortcuts were shipped first and seen failing. Two bugs that none of the planned exposures would have shown were found by the suite and are described below. Nothing here has run as real processes yet: that is stage 8b.

# Stage 8a: the steps, Locks, and one version

Environment: the Simulation of Rung 7, with 5 Nodes, three data Groups and the Meta Group, 8 Slots, 5 clients and 12 keys. 30% of requests are Transactions over two keys, and about two in three of those have keys in two Groups. The busy store adds time-to-live, scans, and Sessions removed after 800 units unused.

A run gets two more verdicts (A§12.9). **Same outcome everywhere:** a Transaction that committed anywhere committed exactly once in every Group its begin named. **Nothing left:** after Faults stop, no key is Locked.

## Exposed: four shortcuts

300 seeds per cell. Each store has one shortcut and is otherwise the finished store of stage 8a, except the first, which is where the Rung started.

| Shortcut | No Faults | Crashes | Partitions | Stopped mid-commit |
|---|---|---|---|---|
| Decision in a Node's memory (P8a.2) | 0 | 23 | 54 | 93 |
| Locked keys can be read (P8a.3) | 33 | 28 | 22 | 50 |
| Version with no offset (P8a.3) | 300 | 300 | 300 | 300 |
| Groups never ask about their Locks | 0 | 1 | 0 | 3 |

"Stopped mid-commit" stops a Node while it is in the middle of a Transaction: carrying one out from memory, leading its Coordinator Group, or leading a Group with keys Locked for it.

**Decision in memory.** The Node that was asked sends the prepares, decides, and sends the finishes. Stopped between two finishes, it leaves one Group with the writes and the other with Locked keys. The client asks again through another Node, which finds the Coordinator Group saying "carried out", tells the client so, and releases the other Group's Locks without writing. Of the 93 failures under "stopped mid-commit", 47 had a Transaction committed in one Group and not the other, and 50 ended with keys still Locked. It doesn't take a crash: under a Partition a client that stops waiting asks again, and two Nodes carry out the same Transaction without knowing of each other (54 of 300).

**Locked keys readable.** The client is answered when the Coordinator Group has decided. Another Group may not have heard yet, and a read of its key gets the old value: a Stale read. It happens with no Fault at all, in 33 runs of 300.

This could not be shown against the first store. There the client is answered only after every Group has finished, so a read in between is not stale. The exposure was made after the decision moved into the Log.

**No offset.** Every run fails. A Group with a short Log is given a version from a Group with a long one, and its next write to that key gets a lower version. The offset had to be built before anything else could be seen, and is switched off here to show it.

**No asking.** Rare, and not one of the planned exposures: see the first bug below.

## The fix

- **The steps are Entries (P8a.4).** Begin, decision and each Group's confirmation are in the Coordinator Group's Log; prepare and finish are in the other Groups'. The Coordinator Group's Leader carries a Transaction on from its state, and a new Leader does the same. The client's Node only waits.
- **Locked keys are refused (P8a.5),** after the Group has asked the Coordinator Group what was decided and waited 12 units. A Group also asks about any Lock older than 30 units.
- **A Move waits (P8a.5).** A Slot on its way out takes no new prepare and isn't frozen while a key in it is Locked.
- **One version (P8a.6):** the highest any Group offered, with a per-Slot offset so later versions stay above it.

## Bugs the suite found

1. **A prepare that arrives after the finish.** A prepare is sent again when no answer comes. If the second copy lands after the Group has finished the Transaction, the Group Locks the keys again.
   - After an abort, nobody ever tells it to let go, since the Coordinator Group has already heard that Group finish. This is what "no asking" shows, and asking ends it.
   - After a commit it is worse: the next finish writes the Transaction's values a second time, over newer writes. First fix: the Group remembers the Transaction in the Session's record. That held for 4,500 runs and then failed once in the busy store (moves-and-everything, seed 56): a Leader that had been replaced without knowing sent a prepare 2,400 units late, after the Session's record had been removed. Final fix: the decision records the version each Group offered, and a finish commits only the prepare that made that offer.
2. **A retry after a Move split the keys.** A request first carried out inside one Group, whose reply was lost, was retried after a Move had put its keys in two Groups. The Coordinator Group answered from the Session's record with the keys' internal names in it. Seeds: moves-interrupted 1, moves-and-crashes 54, moves-and-partitions 20.

Two more were found by reading, not by a failing run, and are fixed and unit-tested: a commit could move a Session's record back to an earlier request, and the same request could be taken on by two Coordinator Groups after a Move.

## What the design got wrong

- **"A no holds nothing" (A§12.2 as first written).** If one Group reports a failed condition at one moment and another reports its conditions held at a later one, the client's answer describes a store that never existed. A no for a failed condition now keeps its Locks until the finish. Approved by the user after P8a.1.
- **The order of the tasks.** The offset (P8a.6) had to come first, and the Stale read (P8a.3) could only be shown after the fix of P8a.4.
- **"Every step may be applied twice without harm"** was true of each step alone and false of a prepare applied again after the finish.

## Results

300 seeds per cell, 5,400 runs. No run failed any verdict.

| Faults | Plain: committed | aborted over a conflict | Busy: committed | aborted over a conflict |
|---|---|---|---|---|
| Crashes | 7,592 | 899 | 7,458 | 865 |
| Partitions | 5,237 | 735 | 5,091 | 676 |
| Stopped mid-commit | 5,965 | 788 | 5,826 | 825 |
| No Faults, no Moves | 9,097 | 1,208 | 8,810 | 1,084 |
| Moves | 8,071 | 1,439 | 7,964 | 1,384 |
| Moves and crashes | 6,792 | 1,152 | 6,707 | 1,073 |
| Moves and Partitions | 4,638 | 861 | 4,676 | 828 |
| Moves interrupted | 5,159 | 966 | 5,051 | 957 |
| Moves, crashes and Partitions | 4,409 | 922 | 4,405 | 900 |

- **About one Transaction in nine is aborted over a conflict** with no Faults (1,208 against 9,097 committed). Until stage 8b a Transaction that meets another's Lock is refused at once, and with 5 clients on 12 keys they meet often.
- **Requests refused `locked`:** 1,744 in the 300 runs with no Faults, about 6 a run, after the 12-unit wait.
- **Moves still finish:** 2,473 Moves in 300 runs with Transactions running, and the longest pause of a Slot with no other Fault was 43 units.

## Not covered

- Nothing has run as real processes. The real shell still refuses a Transaction across Groups with `cross_group`.
- No numbers check yet (stage 8b).
- Automation (replacement and balancing, stage 7c) was off in every run here.
- Records in the Coordinator Group are never removed, so its Snapshots grow with every Transaction (stage 8b).
- A Coordinator Group without a Majority leaves the other Groups' keys Locked (A§12.4). No scenario here loses a Majority for good.
- The longest pause of a moving Slot under crashes and Partitions together reached 2,698 units. Whether Locks made that worse than in Rung 7 was not compared.
- The whole suite now takes 524 s, against Go's limit of 600 s for one package.

## Reproduce

```
go test ./internal/rungtest -run 'Rung8a|Txn|Decision|ReadableLock|NoOffset|Locks'
KV_MEASURE=1 go test ./internal/rungtest -run TestMeasureRung8a -v -timeout 30m
```

Seeds kept as tests: decision in memory (stopped-mid-commit 66, 72, 89; crashes 24, 25; partitions 67, 94), readable Lock (no Faults 35, 39, 42), no offset (1–3), no asking (crashes 82; stopped-mid-commit 76, 80, 150), and the regressions above.

## Verdict (8a)

Atomic under Coordinator and participant crashes, in the Simulation: yes, 5,400 runs. Nothing stays undecided: yes, while every Group has a Majority. Waiting for approval before stage 8b.
