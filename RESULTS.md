# Results: a replicated key-value store, built in nine Rungs

A key-value store that stays consistent under faults, written in Go with the standard library only (plus Porcupine for checking). It was built as a ladder of nine Rungs. Each Rung first shipped a naive version, recorded the seeds on which it failed, then fixed it, measured it, and wrote a retro. Rungs 1–6 are a hand-written Raft Group with durable storage, corruption repair, time, scans, Transactions and Membership changes. Rung 7 splits the keys over several Groups with a Meta Group, Moves, gossip, automatic replacement and balancing. Rung 8 adds Transactions across Groups. Rung 9 is a second store with no Leader, built to measure what availability costs.

The consensus core is a pure step function, so a whole store runs in one process under a Simulation that controls time, network and disk and repeats exactly from a seed. About 102,600 simulated runs across the nine Rungs passed every verdict, and each naive version's failing seeds are kept as tests.

Everything here is taken from `retros/rung-1.md` to `retros/rung-9.md`. No number was measured for this document. Design is in `ARCHITECTURE.md`; terms in bold are defined in `CONTEXT.md`.

## Design comparison

### What each Rung shipped first, and what replaced it

| Rung | Naive version | How it failed | What replaced it |
|---|---|---|---|
| 1 | Primary-backup that acknowledges early and fails over on a timeout | Every run under Faults: split brain, lost **Acknowledged writes** | Raft: **Leader** election, **Majority** replication |
| 2 | Reads from the Leader's memory; clients that retry | **Stale reads** in 0.7–9% of runs with a Leader change; a retried write applied twice | Read index; **Sessions** |
| 3 | Nothing on disk | 200 of 200 runs when every Member restarts at once | Durable **Log** and vote; **Snapshots**; catch-up |
| 4 | Files with no checksums; then a repair that carries on voting | A flipped bit believed, or a Member that can't restart; then **Committed** Entries lost in up to 86 of 300 runs | Checksums, repair from other Members, and a damaged Member that abstains |
| 5 | **Expiry** by each Member's own clock | More than half the runs with a clock Fault; also after a plain restart | Time from the Log only |
| 6 | Member list swapped in one step; a new Member that counts at once | Two Leaders in one Term, 100 of 100 runs; a Group stopped by its own new Member | One Member at a time, the current-Term rule, **Learners** |
| 7 | A Group that doesn't check it owns a key; a table flipped with nobody confirming; ownership and Members by gossip; replacement on one Node's word; rebalancing with no damping | Two Groups serving one **Slot** in 50 of 50 runs; two Leaders; about 90 **Moves** a run where a handful would do | Ownership by each Group's own Log; gossip informs and never decides; a Majority and a wait; damping |
| 8 | Decision in a Node's memory; readable **Locks**; no version offset; waiting with no order | Half-committed Transactions in 93 of 300 runs; Stale reads with no Fault; undecided Transactions in 82 of 100 | Every step an Entry in the **Coordinator Group**'s Log; Locks; older wins |
| 9 | Last-write-wins; **Hints** never delivered; no **Exchange**; a remove forgotten at once | An Acknowledged add lost in 82 of 300 runs, and 62 of 300 with no Fault | Sets where add wins; Hints delivered; the Exchange |

### Variants that were measured against each other

| Choice | Options | Result | Best fit |
|---|---|---|---|
| How reads are answered (Rungs 2, 3, 5) | Through the Log / read index / Leader lease | With no disk the read index is 0.90× the Log. With a disk it is 1.37×. The lease is 1.51× the read index and gives Stale reads when a Leader's clock runs slow (56 of 100 runs) | Read index by default. The lease only where clocks can be trusted |
| A Transaction meets another's Lock (Rung 8) | Refused at once / waits with no order / older wins | Two keys: waiting is best (0 aborts, most commits). Three keys: waiting deadlocks in 82 of 100 runs; older wins commits 4,270 and aborts 3,296 | Older wins from three Groups up. For two keys the lowest-key order makes plain waiting safe |
| Two writes made apart (Rung 9) | Last-write-wins / a set where add wins | Last-write-wins loses Acknowledged adds even on a healthy store. Add wins lost none in 11,700 runs | Add wins, at the price of a value that is a set |
| W and R in the **Leaderless variant** | 1/1, 2/2, 3/1 | See Throughput and Observations | 1/1 for availability; 2/2 buys little here |
| Consistent store against the Leaderless variant | Rung 8's store / Rung 9's | See Observations 8 and 9 | Depends on the Partition and on the clients |

## Performance results

**Test environment.** Apple M4 Pro, 24 GB, macOS 27.0.1, Go 1.27.1. Load from `kvbench` on the same machine. Local runs are separate processes on one SSD; Docker runs use Docker Desktop with toxiproxy on every link between Nodes. Real runs are 10 s each. Simulation times are in units (a tick is 10). Results gathered over the build, the last on 2026-10-11.

The full suite (`go test ./...`) last passed on the code as merged, with exit status 0.

### Correctness tests

A simulated run fails if any verdict fails. The verdicts grew with the Rungs: History **Linearizable**, Members identical, never two Leaders in a Term (Rung 1); one owner per Slot (Rung 7); the same outcome in every Group and nothing left Locked (Rung 8). Rung 9 has its own: **Converged**, no Acknowledged add lost, nothing back.

| Rung | What must hold | Simulated runs | Failed | Real runs | Result |
|---|---|---|---|---|---|
| 1 | One Leader per Term; no Acknowledged write lost; Linearizable on 3 and 5 Members | 16,000 | 0 | 5 with Faults | Pass |
| 2 | No Stale read; a retry takes effect once | 3,200 | 0 | 4 | Pass |
| 3 | Every Acknowledged write survives a full restart, a crash mid-write, a stalled disk | 26,000 | 0 | 5 | Pass |
| 4 | Corruption always detected, never applied | 6,400, and 3,464 single-bit flips checked one by one | 0 | 3 | Pass. A Group with a Majority damaged at once stops |
| 5 | Members identical under clock skew and jumps; scans and Transactions Linearizable | 14,500 | 0 | 8 | Pass. Real runs don't cover scans or Transactions |
| 6 | Never two Leaders during a Membership change; a dead Member replaced under load | 5,300, with 12,612 changes | 0 | 3 replacements | Pass |
| 7 | One owner per Slot at any moment; Linearizable while Slots move | 8,000, with over 10,000 Moves | 0 | 26 | Pass |
| 8 | A Transaction across Groups commits everywhere or nowhere; nothing stays undecided | 11,500 | 0 | 8, and in-process tests | Pass, while every Group involved has a Majority |
| 9 | Replicas converge after healing; no Acknowledged add lost | 11,700 | 0 | 7 | Pass. Converged needs the Exchange (stage 9b) |

**Atomicity and concurrency, specifically.**

| Test | Input | Expected | Actual |
|---|---|---|---|
| Transactions across Groups, Simulation | 11,500 runs with crashes, Partitions, Moves, replacement, balancing; 271,736 Transactions committed | Every Transaction ends the same way in every Group; History Linearizable over the whole store | As expected in every run |
| Six real clients on the same three keys in three Groups | 90 Transactions, each writing one value to all three keys | All 90 commit; the three keys end with the same value and version; nothing Locked | As expected |
| A retried request in a Session | A Transaction sent again through another Node after it committed | Answered as before, carried out once | As expected |
| Version after a Transaction | A write in the Group with the shorter Log | A higher version than the Transaction gave | As expected |
| The naive versions | The seeds recorded for each | Still fail | They do: each is a test |

### Latency

Percentiles are those `kvbench` printed. p2.5, p97.5 and the mean were not recorded; "—" means that figure isn't in the retro.

| Scenario | Clients | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Rung 1, 3 Members, nothing on disk | 8 | 180 µs | — | 330 µs | — |
| Rung 3, 3 Members, durable (writes) | 64 | 44.9 ms | — | 62.1 ms | — |
| Rung 6, 3 Members | 64 | 45.7 ms | — | — | — |
| Rung 8, 3 Groups on 5 Nodes, single-key requests | 64 | 87.9 ms | 130.1 ms | 159.8 ms | 204.1 ms |
| Rung 8, a Transaction across two Groups, 5,000 keys | 64 | 581 ms | 899 ms | 1.03 s | 1.23 s |
| Rung 8, a Transaction across two Groups, 50 keys | 64 | 988 ms | 3.53 s | 4.70 s | 5.91 s |
| Rung 9, Leaderless W=2 R=2 | 64 | 93.7 ms | 155.4 ms | 176.7 ms | 210.9 ms |
| Rung 9, Leaderless W=1 R=1 | 64 | 37.3 ms | 123.1 ms | 153.2 ms | 193.9 ms |
| Docker, Rung 8 store, around a real Partition | 16 | 4.44 ms | 7.99 ms | 10.3 ms | 21.7 ms |

**Recovery.** After a Leader is isolated, the next write succeeds in 136–185 ms (Rung 1, Docker). After a Leader is killed and restarted, six runs gave 178, 221, 222, 283, 334 and 413 ms (Rung 6): usually inside the 400 ms target, with a tail that crosses it. Replacing a dead Member under load paused writes for 37–76 ms. A Slot being moved is frozen for about 80 ms at 8 clients (Rung 7).

### Throughput

Local processes, no Faults, default mix (35% reads) unless said. Each Rung's target was 0.9× the Rung before on the same load.

| Store | Members / Nodes | Clients | Requests/s | Against the Rung before | Target met |
|---|---|---|---|---|---|
| One Node, no replication | 1 | 8 | 86,810 | — | baseline |
| Rung 1 | 3 | 8 | 42,743 | 0.49× of one Node | baseline set |
| Rung 1 | 5 | 8 | 29,458 | 0.34× of one Node | baseline set |
| Rung 2 | 3 | 8 | 41,570 | 0.97× | yes |
| Rung 3, code only, nothing on disk | 3 | 8 | 39,174 | 0.94× | yes |
| Rung 3, durable | 3 | 8 | 220 | 178× slower: a stated cost | baseline reset |
| Rung 3, durable | 3 | 64 | 1,412 | — | new baseline |
| Rung 4 | 3 | 64 | 1,392 | 0.99× | yes |
| Rung 5 | 3 | 64 | 1,378 | 0.99× | yes |
| Rung 6 | 3 | 64 | 1,387 | 1.01× | yes |
| Rung 7, one Group | 3 | 64 | 1,344 | 0.97× | yes |
| Rung 7, three Groups | 5 | 64 | 704 | 0.52× | **no** |
| Rung 8, three Groups, no Transactions | 5 | 64 | 710 | 1.01× | yes |
| Rung 8, 10% Transactions across Groups, 5,000 keys | 5 | 64 | 406, and 43 Transactions/s | a stated cost | — |
| Rung 9, Leaderless W=2 R=2 | 5 | 64 | 647 | 0.91× of Rung 8's store | no target |
| Rung 9, Leaderless W=1 R=1 | 5 | 64 | 1,280 | 1.80× of Rung 8's store | no target |

**Through a Partition.** Two Nodes cut off from three. "Work done" is requests answered that began during the Partition, against the same period with no Fault.

| Store | Simulation (300 seeds) | Real Partition in Docker (one run, 16 clients) | Writes lost |
|---|---|---|---|
| Consistent, Rung 8 | 18.8% | 73% | none; Linearizable |
| Leaderless, W=1 R=1 | 91.6% | 77% | none |
| Leaderless, W=2 R=2 | 43.5% | 2% | none |
| Leaderless, last-write-wins, W=2 R=2 | not measured | 1% | 82 of 300 simulated runs lost one; 5 of 4,231 adds in the real run |

### Observations

1. **Consensus halves throughput, and the disk then divides it by 178.** Three Members answer 42,743 requests/s where one Node answers 86,810. Making every acknowledgement wait for a disk flush takes 39,174 down to 220 at 8 clients. Group commit recovers some: 64 clients get 1,412. Everything after Rung 3 is measured against that, and nothing after it cost more than 3% until several Groups shared one disk.

2. **Checksums, time, scans, Transactions and Membership changes were free.** Rungs 4, 5 and 6 measured 0.99×, 0.99× and 1.01×. Each adds work per Entry that is nothing beside a flush.

3. **Several Groups on one machine are slower than one.** Three Groups answer 704 requests/s where one answers 1,344. Twelve replicas sync to the SSD that three did. This is the one throughput target missed, and evening out load across the Groups cost a further 18% (885 to 729), for the same reason. Splitting keys pays where Nodes have disks of their own, which this machine doesn't.

4. **The read index was slower than reading through the Log until there was a disk.** 0.90× in Rung 2, 1.37× in Rung 3. What it saves is a disk write per read, and Rung 2 had none to save. The target was missed, moved a Rung, and then met.

5. **A Transaction across two Groups costs about seven single writes.** 581 ms against 84 ms. Three of its steps are disk writes one after another, and the Node that took the request then asks two or three times whether it is decided, each asking a Linearizable read of about 100 ms. The asking could be done without the round trip to a Majority, and wasn't.

6. **Contention is worse than cost.** On 50 keys the same Transaction's median is 988 ms and its p99 4.7 s, with 142 attempts aborted. Before the waits were resized for a loaded machine it was 1.48 s and 513 aborts. Every wait in the store is a fixed number chosen on a quiet machine, and each was wrong under load.

7. **Having no Leader bought nothing while the disk was the wait.** The Leaderless variant at W=2 R=2 answers 647 requests/s against the consistent store's 710. Both wait for two disks. At W=1 R=1 it answers 1,280, because it waits for one.

8. **The Simulation and the real Partition rank the stores differently.** The consistent store did 18.8% of its work through a simulated Partition and 73% through a real one. The Leaderless variant at W=2 R=2 did 43.5% and 2%. In the real run every data Group kept its Majority on the large side, and the consistent store told the small side's clients "no Majority" at once, 5,530 times. A Leaderless Node waited 1.2 s before saying too few Nodes had answered, and clients that don't pause were soon all waiting on such a request. Only 49 requests failed in that run.

9. **A store is as available as its "no" is fast.** That is the finding of Rung 9, and it was in nobody's design. A Leaderless Node can't tell a Node that is cut off from one that is slow.

10. **What availability cost was not lost writes.** With sets where add wins, no Acknowledged add was lost in any run. The price was elsewhere: 3% of reads during a long Partition miss a write, 3% of removes take nothing out, a value is a set and not a string, and the record of every remove is kept for ever.

11. **The dangerous bugs passed most runs.** Reads from memory were stale in 0.7–9% of runs. A careless repair passed 599 of 600 runs with one Member damaged at a time. A store with nothing on disk survives one Member forgetting, by luck. Each needed a scenario built to cause it, and a History, not an end-state comparison, to see it.

12. **Sending again must cost nothing when it brings nothing.** Twice, in Rung 8's Transactions and Rung 9's Home Nodes, a Node that had fallen behind was kept behind by being sent the same things over and over, each one a disk write.

## Decision Log, walked through

Each entry of `ARCHITECTURE.md` → Decision Log, with what the Rungs showed about it.

| Decision | Evidence | Verdict |
|---|---|---|
| **The consensus core is a pure step function** | Every simulated run repeats from its seed; every failure in the retros is named by scenario and seed and kept as a test. `internal/purity` enforces it. The same core runs under the Simulation and the real shell | Held, and paid for itself in every Rung. Rung 9's core couldn't share the Simulation, though: a different contract needed its own (`internal/lsim`) |
| **Rung 1 ships a naive primary-backup store first** | Passes 60 runs with no Faults and fails all 180 with them: two primaries at once in 28–30 of 30 | Held. The pattern became the method for every Rung |
| **No checksums on disk until Rung 4** | Rung 4 showed four different outcomes of one flipped bit, including a Member that can't restart in 134 of 300 runs | Held. The failure would have been invisible otherwise |
| **Read index by default; the lease is an exempt variant** | Lease: 1.51× locally, 1.32× in Docker; Stale reads in 56 and 43 of 100 runs with a slow Leader clock, 0 with the read index | Held. It took a read-only client to see the lease fail: the usual mix hid it |
| **Time enters only through the Leader's stamp** | Own-clock Expiry: Members ended with different keys in 47–52 of 100 runs under skew, and failed once with perfect clocks after a restart. From the Log: 7,600 runs clean | Held, and for a stronger reason than first given: a restart alone breaks own-clock Expiry |
| **One Member at a time, with the current-Term rule** | Swapping the list: 100 of 100 runs unsafe. One at a time: 5,300 runs with 12,612 changes safe. Both rules shown to matter by switching them off | Held |
| **Hand-written storage and tree** | Torn writes and bit flips could be injected because the format was owned; 3,464 single-bit flips checked one by one | Held. Cost: the storage code produced its share of the bugs in Rungs 3 and 4 |
| **The shell stores before it acts, and the core is never told** | Group commit: 8× the clients gives 6.4× the throughput. A read that writes nothing still waits for its batch's sync (21 ms for a lease read; 88 ms for a Leaderless read) | Held for safety. Its stated cost is real and was never bought back: answering reads before the sync was not tried |
| **A damaged Member abstains until repaired** | The careless repair: Histories not Linearizable in up to 81 of 300 runs. Abstaining: 6,400 runs clean | Held. The cost is as stated: a Majority damaged at once stops the Group |
| **A Member that can't say how it voted doesn't start** | The first design, adopting the Leader's Term, was found unsafe before it shipped (Rung 4 retro). Term and vote are stored twice | Held. No run lost both copies, so the "stays down" path is tested only directly |
| **The Meta Group decides a move and the data Groups confirm it** | A table flipped with nobody confirming: two Groups serving one Slot in 50 of 50 runs. Confirmed Moves: 8,000 runs, none with two owners | Held. It did cost an extra Group, which is part of the 0.52× |
| **Versions are a Slot's Epoch and a Log index** | Versions stayed increasing across over 10,000 Moves | Held, then stretched: Rung 8 had to add a per-Slot offset on top |
| **One Store time, issued by the Meta Group** | Keys with a time-to-live expire identically after Moves in the Rung 7 suites | Held. "If the Meta Group stops, nothing expires" was accepted and never measured |
| **Sessions are store-wide and follow the Slot of their last request** | Breaking it fails 53 runs. Rung 8 leaned on it harder: a Transaction's answer, abort included, is kept in the Session's record | Held, with a limit found in Rung 8: a record can be removed, so nothing safety-critical may rest on it alone |
| **The Coordinator Group is one of the Groups involved, and begins in its own Log** | Decision in memory: half-committed in 93 of 300 runs. In the Log: 11,500 runs clean. The begin Entry is one of the five writes a Transaction costs | Held |
| **One version per Transaction, kept rising by a per-Slot offset** | With no offset every run fails on versions alone, with no Fault | Held. It had to be built before anything else in Rung 8 could be seen |
| **The older Transaction wins, and a retry keeps its age** | Three keys: 0 of 100 runs fail, against 82 with no order. Two keys: 638 aborts where plain waiting has none | Held for three Groups and up; unnecessary below that. It was also too eager for a Transaction that holds nothing yet, and was softened |
| **The Leaderless variant's value is a set, and add wins** | No Acknowledged add lost in 11,700 runs; last-write-wins lost one in 82 of 300, and in 62 of 300 with no Fault | Held, at the cost stated: same routes, different contract |
| **W of three Home Nodes, with Hints** | Hints made a long outage of one Node harmless (0 of 300 not Converged without the Exchange). W=2 R=2 did 2% of its work through a real Partition | Half held. Hints did their job. W and R were chosen for what they promise, not for how long a Node waits to find they can't be met |

**Decisions that turned out wrong, and were changed.** Each is recorded in its Rung's retro under "What the design got wrong":

- A Member that lost its vote may adopt the Leader's Term (Rung 4): unsafe.
- Replacement heals a Group that has lost its Majority (Rung 7): it can't; such a Group can't change its own Members.
- A "no" to a prepare holds nothing (Rung 8): a failed condition must keep its Locks, or the answer describes a store that never existed.
- Every step of a commit may be applied twice (Rung 8): not a prepare after its own finish. It took three fixes.
- Last-write-wins orders writes on one side correctly (Rung 9): not two at the same moment.
- Availability is the share of requests answered (Rung 9): that flatters a store whose clients wait.

## Known gaps

- **Three Groups on one machine miss the 0.9× target** (0.52×).
- **Real runs are single runs.** Rung 6 showed what that hides: recovery was 395 ms on one run and 178–413 ms over six.
- **Scans and Transactions are checked only in the Simulation and in-process.** `kvbench` counts and times Transactions and doesn't check them.
- **A real Partition was run once per store, in one shape.**
- **A Coordinator Group without a Majority** leaves other Groups' keys Locked; no scenario loses a Majority for good.
- **The Leaderless variant** can't fail fast, compares every shared key in every Exchange, never trims its file, and can't take back a Node that lost its disk.
- **Linearizability over the whole store can't be checked with 20 clients**, which was also the load that broke Rung 8's throughput.

## Screenshots

<!-- Add screenshot: terminal output of `go test ./...` with every package ok -->
<!-- Add screenshot: `harness/run.sh local 5` with DATA_GROUPS=3 CLIENTS=64, showing 710 answered/s, "linearizable: yes" and the end-state line -->
<!-- Add screenshot: `kvctl table` during a skewed-load run, showing Slots per Group, load shares and a Slot moving -->
<!-- Add screenshot: `harness/partition.sh consistent` and `W=2 R=2 harness/partition.sh leaderless` side by side, showing the "through the Fault" lines (73% and 2%) -->
<!-- Add screenshot: `kvbench-history.html`, the Porcupine timeline, for a naive-store seed that isn't Linearizable -->
<!-- Add screenshot: a failing naive seed printed by `go test -run TestDecisionInMemoryIsExposed -v`, showing the half-committed Transaction -->
