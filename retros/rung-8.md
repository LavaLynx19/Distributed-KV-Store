# Rung 8 retro: Transactions across Groups

Status: **both stages done and approved.** Rung 8 is built in two stages (A§12.1). This file has a part per stage, and the verdict for the whole Rung at the end.

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
- The Rung tests ran for 524 s in one package, against Go's limit of 600 s. The tests of Rungs 7 and 8 now have a package of their own, `internal/shardtest`.

## Reproduce

```
go test ./internal/shardtest -run 'Rung8a|Txn|Decision|ReadableLock|NoOffset|Locks'
KV_MEASURE=1 go test ./internal/shardtest -run TestMeasureRung8a -v -timeout 30m
```

Seeds kept as tests: decision in memory (stopped-mid-commit 66, 72, 89; crashes 24, 25; partitions 67, 94), readable Lock (no Faults 35, 39, 42), no offset (1–3), no asking (crashes 82; stopped-mid-commit 76, 80, 150), and the regressions above.

## Verdict (8a)

Atomic under Coordinator and participant crashes, in the Simulation: yes, 5,400 runs. Nothing stays undecided: yes, while every Group has a Majority. Approved by the user.

# Stage 8b: conflicts, Moves, forgetting, real Nodes

Environment: as stage 8a, with Transactions over three keys so that they can span all three Groups, and clients that send an aborted Transaction again, with its age, up to five more times. The contention runs use 8 clients with half their requests Transactions.

## Exposed: waiting with no order (P8b.1)

100 seeds per cell, no Faults.

| A Transaction that finds a key Locked | Keys | Runs failed | Committed | Aborted over a conflict | Clients that gave up | Requests never answered |
|---|---|---|---|---|---|---|
| Is refused at once (stage 8a) | 2 | 0 | 5,122 | 5,989 | 62 | 0 |
| Waits, in no order | 2 | 0 | 5,302 | 0 | 0 | 0 |
| Older wins | 2 | 0 | 5,053 | 638 | 0 | 3 |
| Is refused at once | 3 | 0 | 4,084 | 14,592 | 597 | 0 |
| Waits, in no order | 3 | **82** | 2,494 | 152 | 0 | 923 |
| Older wins | 3 | 0 | 4,270 | 3,296 | 18 | 23 |

- **Three keys, waiting in no order: 82 runs of 100 end with Transactions that are never decided.** One holds its keys in Group 2 and waits for Group 3, another the reverse. Their keys stay Locked and every client that touches them waits too.
- **Two keys can't do it.** A Transaction's Coordinator Group owns its lowest key and Locks its own keys first, so each waits only for a key higher than one it holds, and no ring can close. Waiting with no order then beats everything: no aborts at all, and the most commits. The exposure the plan asked for ("two Transactions that never decide") doesn't exist with two keys; it took three Groups.
- **Refusing at once, which stage 8a did, is the worst when keys are fought over:** more aborts than commits with two keys, and 597 Transactions given up after six attempts with three.

## The fix (P8b.2–P8b.4)

- **Older wins.** Age is the Store time the first attempt found, then the Session, then the request number. A prepare that finds a key Locked by a younger Transaction has it aborted by its own Coordinator Group; one that finds an older holder tries again for 80 units and then gives way. A Transaction that is aborted is told its age and sends it back.
- **Moves abort.** A Slot ready to be frozen asks for the undecided Transactions holding its keys to be aborted.
- **Forgetting.** A Coordinator Group drops its record once every Group has confirmed. The answer, abort or commit, is by then in the Session's record.

"None loses for ever" is nearly true, not exactly: with three keys 18 Transactions of about 4,300 were still given up after six attempts. A retry keeps its age but can still lose to one that is older still.

## How long a Move waits for Locks (P8b.3)

300 seeds per row. A Move waits only if a key in its Slot is Locked at the moment its copy is done.

| Faults | Moves that waited | Longest wait, Moves waiting | Longest wait, Moves aborting |
|---|---|---|---|
| Moves | about 150 of 2,430 | 360 | 60 |
| Moves and crashes | about 133 of 1,845 | 400 | 240 |
| Moves and Partitions | about 135 of 1,304 | 780 | 520 |
| Moves interrupted | about 70 of 1,011 | 340 | 240 |
| Moves, crashes and Partitions | about 110 of 1,265 | 2,080 | 1,980 |

With nothing else wrong, aborting cuts the longest wait from 360 units to 60. Under Faults it helps little: the wait is then for a Coordinator Group that can't be reached, and asking it to abort waits just as long.

## Results (P8b.6, Simulation)

6,100 runs, none failing any verdict, 158,788 Transactions across Groups committed.

| Store | Scenarios | Seeds each | Runs | Committed | Moves | Nodes replaced |
|---|---|---|---|---|---|---|
| Plain | the 9 of stage 8a | 300 | 2,700 | 68,363 | 7,854 | — |
| Busy (time-to-live, scans, Sessions that expire) | the same | 300 | 2,700 | 66,915 | 7,852 | — |
| Replacement on (stage 7c) | its 4 | 100 | 400 | 12,760 | 328 | 452 |
| Balancer on, load skewed onto Group 1 | no Faults | 300 | 300 | 10,750 | 76 | — |

## Real Nodes (P8b.5)

The part a Leader plays, sending steps and proposing decisions, is one package (`internal/committer`) used by the Simulation and by a real Node. Two tests run five real Nodes in one process, with real disks and sockets:

- A Transaction across two Groups commits with one version on both keys, a later write in the Group with the shorter Log gets a higher version, a failed condition in one Group stops the write in the other, and a retry in a Session is answered as before and carried out once.
- Six clients send 90 Transactions over the same three keys in three Groups. All 90 commit, the three keys end with the same value and version, and nothing is left Locked.

Measured in those tests, not under `kvbench`: a Transaction across three Groups with nobody else on its keys takes 88 ms, where a single write takes 16 ms. With six clients on the same three keys a commit takes about 200 ms.

## Real runs (P8b.6)

M4 Pro, local processes, 10 s each, five Nodes hosting three data Groups and the Meta Group, 64 clients, twelve replicas syncing to one disk. Run by the user's leave: eight runs in two batches. `kvbench` sends Transactions that write two keys in different Groups, on keys of their own, and sends one again with its age when it is aborted over a conflict.

**The numbers check.** With no Transactions and no Fault: **710 requests/s, against Rung 7's 704 (1.01×).** Pass. The checks Rung 8 adds to every read and write cost nothing that shows.

**What a Transaction costs.** Single-key requests are in the same runs; their latency didn't change (p50 80 to 88 ms throughout).

| Run | Keys | Single-key requests/s | Transactions/s | Transaction p50 / p95 | Attempts aborted over a conflict | Not committed | Verdicts |
|---|---|---|---|---|---|---|---|
| 10% Transactions | 5,000 | 406 | 43 | 581 ms / 899 ms | 0 | 0 | Linearizable; Groups identical |
| 10%, Node 1 killed for 3 s | 5,000 | 415 | 44 | 516 ms / 901 ms | 0 | 16 | Same; longest pause in writes 370 ms |
| 10%, a Slot moved every half second | 5,000 | 452 | 48 | 498 ms / 780 ms | 8 | 1 | Same; 2 Slots moved, frozen 174 ms median, 240 ms at most |
| 10% Transactions | 50 | 222 | 20 | 988 ms / 3.5 s | 142 | 48 | Same |
| 100% Transactions | 50 | — | 17 | 1.17 s / 5.0 s | 316 | 97 | Same |
| 10%, before the waits were changed | 50 | 158 | 14 | 1.48 s / 5.7 s | 513 | 40 | Same |
| 100%, before | 50 | — | 8 | 3.6 s / 7.7 s | 1,004 | 71 | Same |

- **A Transaction across two Groups takes about 0.55 s here, where a single write takes 0.08 s: about seven times.** Three of its steps are disk writes one after the other (begin, prepare, decision), about 85 ms each under this load. The rest is the Node that took the request asking the Coordinator Group whether it is decided yet: each asking is a Linearizable read, which takes about 100 ms here, and it takes two or three.
- **Single-key throughput falls from 710 to about 410, and none of that is the store being slower.** A client sends one request at a time, and one request in ten now takes seven times as long.
- **On 50 keys, 64 clients fight.** A Transaction's median is 1 s, and one in six isn't committed within the client's 2 s. "Not committed" here means the client stopped waiting: `kvbench` sends its Transactions outside a Session and can't ask again.
- **The first two runs were worse, and showed a mistake.** The waits had been chosen in-process, where a step takes 16 ms. Under load a step takes 85 ms, so a younger Transaction gave way after 400 ms to an older one that needed about that long to finish, and steps were sent a second time while the first was still on its way. With waits sized for the step time measured, aborted attempts on 50 keys fell from 513 to 142. The waits are still fixed numbers, right for one machine and one load.
- **Moves were few and slow in the Move run:** two Slots in 3 s, taking 1.4 s and 2.0 s each, where Rung 7 moved one every half second. That run had 10,000 keys where Rung 7's had 50, so each Slot had about 150 keys to copy. Whether Locks added to it was not separated out.

## Bugs found

1. **A forgotten abort came back with the wrong key names.** Once records were forgotten, a retry was answered from the Session's record, which held the failed conditions under the plain names of the keys where the reading code expected internal ones. 7 runs of 100 with no Faults, 69 with crashes.
2. **Finishes were sent late.** The Leader counted "when did I last send this Group a step" from the prepare, so a finish waited out the rest of that interval: 250 ms on real Nodes. The Simulation had the same delay and no test that could see it. Found by timing one Transaction on real Nodes: 267 ms before, 93 ms after.
3. **Retries crowded out the Transaction they were waiting for.** Every try for a Locked key is an Entry in that Group's Log. Six real clients on the same keys took 350 ms a commit. Three changes: each try waits twice as long as the last; a Node asks only once in a while for the same Transaction to be aborted; and a Transaction that holds nothing yet waits before it has anybody aborted (it can't be part of a ring). 200 ms a commit after.
4. **Requests outside a Session were named at random,** so of two Transactions of the same age a newcomer was the older half the time and had the one in front aborted for nothing. They are now named from the clock.
5. **A Lock that had outlived its Transaction blocked others for 200 ms,** until its Group thought to ask. A blocked prepare now asks on its behalf.
6. **Rung 7's Move, not Rung 8:** a Group that had given a Slot away, and was cut off before it dropped its copy, never dropped it if the Slot had since moved on again, and could then never take the Slot back. One run in 600. It drops the copy now whoever owns the Slot.

## What the design got wrong

- **"Two Transactions that never decide"** needs three Groups, as above.
- **"An older Transaction makes a younger one abort" was too eager** for one that hasn't started. See bug 3.
- **Who asks for the abort.** The design had the Group that holds the key ask. It answers the prepare with who holds it, and the Leader that sent the prepare asks: that Leader is the one waiting, and the Group holds nothing for the Transaction that was refused.

## Not covered

- **A crash or a Move during Transactions, on real processes, was run once each.** Both ended Linearizable with the Groups identical. `kvbench` doesn't check that its Transactions were atomic: that rests on the Simulation and the in-process tests.
- **How a Transaction is answered.** The Node that took the request asks again and again whether it is decided, by Linearizable reads. That is two or three reads of 100 ms on top of three writes. A decision once made can't be unmade, so it could be read from any Member's state without the round trip to a Majority. Not done.
- **The waits don't follow the load.** They are constants, sized once for the Simulation and once for this machine.
- **A crowd.** Twenty clients on 24 keys with Transactions over three of them commit 23 to 50 Transactions in a run where five clients commit about 37 each run on average, and leave some undecided when the run ends. Their steps and retries fill the Groups' Logs. Nothing in the design sheds that load. The linearizability check can't be run on a History that size either: it is over the whole store at once, and stops being feasible well before 20 clients. The balancer was therefore tested with 6 clients, not the 20 its load figures were measured with.
- **`kvbench` doesn't check its Transactions.** They use keys of their own and are counted and timed. The History it checks has only the single-key requests.
- A Coordinator Group without a Majority, and Docker, as in stage 8a.
- Old data directories: a Snapshot from before Rung 8 opens; one from the middle of this stage's work doesn't.

## Reproduce

```
go test ./internal/shardtest -run 'Rung8|Waiting|TwoKeys|OlderWins|AMove'
go test ./internal/server -run 'Transaction'
KV_MEASURE=1 go test ./internal/shardtest -run TestMeasureRung8b -v -timeout 40m
```

## Verdict (8b)

Done and approved by the user.

# Rung 8 verdict

| Check (README) | Result |
|---|---|
| Exposed: a transaction across Groups half-committed when its coordinator crashes | **Done.** 93 runs of 300 with the Node stopped mid-commit, 54 under Partitions with no crash at all. Three more shortcuts shown failing on the way: readable Locks (Stale reads with no Fault), versions with no offset (every run), waiting with no order (82 runs of 100, with three keys) |
| Atomic under coordinator and participant crashes; nothing stays undecided | **Done in the Simulation:** 5,400 runs in stage 8a and 6,100 in stage 8b, none failing, with crashes, Partitions, Moves, replacement and the balancer. On real processes: in-process tests, and one run each with a Node killed and with Slots moving. "Nothing stays undecided" holds while every Group involved has a Majority |
| Numbers: at least 0.9× Rung 7 | **Pass: 710 against 704 requests/s (1.01×)** on the same load. Transactions across Groups are a known cost on top: about seven single writes each |
| Retro | This file |

## Lessons

- **The step that is safe alone is not safe out of order.** Every step of the commit could be applied twice. A prepare applied after its own finish was the hole, and it took three fixes: asking about old Locks, remembering by Session, and at last naming in the decision the prepare it counted. Only the last doesn't depend on something being remembered.
- **The lowest key gave an order for free.** Because the Coordinator Group owns the lowest key and Locks first, two-key Transactions can't wait in a ring, and for them the naive waiting was the best policy measured. "Older wins" is needed from three Groups up, and costs aborts below that.
- **A wait is a guess about how long a step takes.** Every wait here was chosen on a quiet machine and was wrong under load. Stage 7c found the same with the load figures.
- **What a test can't see, it doesn't test.** Finishes sent 250 ms late passed 11,000 simulated runs. One timing on real Nodes found it.
- **The checker sets the size of the test.** A History with Transactions is checked over the whole store, which rules out 20 clients. The crowd that broke the store's throughput was also the one case its correctness couldn't be checked in.
