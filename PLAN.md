# distributed-kv-store — Implementation Plan

Requirements: [README.md](./README.md). Design: [ARCHITECTURE.md](./ARCHITECTURE.md) (A§n). Terms: [CONTEXT.md](./CONTEXT.md). Each phase is one Rung, and each Rung follows the same order: build the Faults it needs, ship the naive version, watch it fail, fix it, measure, write the retro.

## Implementation Checklist

### Phase 0 — Scaffolding (§P0)
- [x] P0.1 `git init`, Go module, directory layout, `.gitignore` → §P0
- [x] P0.2 Project `CLAUDE.md` (locked stack, determinism rules, conventions) → §P0
- [x] P0.3 Core contract types: events, outputs, the step interface (A§4.2) → §P0
- [x] P0.4 Determinism lint: a test that fails if core or state-machine packages import `time`, `net`, `os`, `sync` or `math/rand` → §P0

### Phase 1 — Rung 1: split brain and a lost Acknowledged write (§P1)
- [x] P1.1 Simulation skeleton: seeded scheduler, tick clock, in-memory network (drop, Partition), crash and restart → §P1
- [x] P1.2 State machine v1: get, put, delete, compare-and-set with versions, all as Log commands → §P1
- [x] P1.3 Verdicts: History recorder, Porcupine model, End-state comparison, client signals (A§8.2) → §P1
- [x] P1.4 Naive primary-backup core (acknowledges early, fails over on timeout) → §P1
- [x] P1.5 Expose: seeds that show two primaries and a lost Acknowledged write, recorded for the retro → §P1
- [x] P1.6 Raft election: Terms, votes, heartbeats, randomized timeouts → §P1
- [x] P1.7 Raft replication: append, consistency check, commit by Majority, apply → §P1
- [x] P1.8 Client path in the core: proposals, `not_leader`, `no_majority` → §P1
- [x] P1.9 Rung 1 Simulation suite: crashes and clean Partitions on 3 and 5 Members, many seeds → §P1
- [x] P1.10 Real shell: TCP transport, HTTP API (A§7), ticker, local process runner → §P1
- [x] P1.11 Docker Compose with toxiproxy, and the real-run client with History recording → §P1
- [x] P1.12 Baseline numbers: single Node vs 3 vs 5 Members, local and Docker → §P1
- [x] P1.13 `retros/rung-1.md`; set relative targets for later Rungs in README → §P1

### Phase 2 — Rung 2: Stale read and double apply (§P2)
- [x] P2.1 Simulation network Faults: delay, reorder, duplicate, one-way Partition → §P2
- [x] P2.2 Naive shortcuts: the Leader answers reads from memory; clients retry unanswered requests → §P2
- [x] P2.3 Expose: Stale read from a cut-off Leader; a retried compare-and-set applied twice → §P2
- [x] P2.4 Sessions: open, deduplicate on apply, saved responses (A§6.3) → §P2
- [x] P2.5 Read index, with the own-Term commit rule (A§6.2) → §P2
- [x] P2.6 Rung 2 suite under the new Faults; measure Log reads vs read index → §P2
- [x] P2.7 `retros/rung-2.md` → §P2

### Phase 3 — Rung 3: restart amnesia and an endless Log (§P3)
- [x] P3.1 Simulation disk: writes pending until durable, lost at a crash, stalls → §P3
- [x] P3.2 Expose: full restart breaks safety; Log grows without bound; a returning Node can't catch up → §P3
- [x] P3.3 Disk format: Log segments and vote file, no checksums (A§5.4) → §P3
- [x] P3.4 Durable-before-send in both shells; real file storage with fsync → §P3
- [x] P3.5 Copy-on-write ordered tree replaces the v1 state → §P3
- [x] P3.6 Snapshots: take, write, trim the Log (A§6.4) → §P3
- [x] P3.7 Catch-up by Snapshot then Log → §P3
- [x] P3.8 Rung 3 suite: full restart, crash mid-write, stalled disk; measure Snapshot cost, and read index against Log reads (target moved from Rung 2) → §P3
- [x] P3.9 `retros/rung-3.md` → §P3

### Phase 4 — Rung 4: a disk that lies (§P4)
- [x] P4.1 Simulation disk Faults: torn write, bit flip → §P4
- [x] P4.2 Expose: a corrupted record replayed as valid; Members diverge → §P4
- [x] P4.3 Checksums on Log records, vote file and Snapshots → §P4
- [x] P4.4 Recovery policy: what a damaged Member may keep, and repair from the others (decide, then Decision Log) → §P4
- [x] P4.5 Rung 4 suite; `retros/rung-4.md` → §P4

### Phase 5 — Rung 5: time (§P5)
- [x] P5.1 Simulation clock Faults: per-Member rate, jumps → §P5
- [x] P5.2 Expose: time-to-live judged by each Member's clock; Members disagree → §P5
- [x] P5.3 Leader stamps, Log time, Expiry, deterministic sweep (A§6.7) → §P5
- [x] P5.4 Session cleanup by Log time; `session_expired` → §P5
- [x] P5.5 Range scans → §P5
- [x] P5.6 Transactions within one Group → §P5
- [x] P5.7 Whole-store Porcupine model for scans and transactions → §P5
- [x] P5.8 Lease read variant behind a switch; demonstrate its Stale read under skew; measure → §P5
- [x] P5.9 Rung 5 suite; `retros/rung-5.md` → §P5

### Phase 6 — Rung 6: two Majorities (§P6)
- [x] P6.1 Expose: a Member list swapped in one step elects two Leaders → §P6
- [x] P6.2 Membership change Entries, one at a time, with both guards (A§6.5) → §P6
- [x] P6.3 New Member catches up before it counts → §P6
- [x] P6.4 Admin API and `kvctl`; replace a dead Member under load → §P6
- [x] P6.5 Unsafe recovery command (A§6.6) → §P6
- [x] P6.6 Rung 6 suite: changes during crashes and Partitions; `retros/rung-6.md` → §P6

### Phase 7 — Rung 7: several Groups (§P7)
- [x] P7.0 Confirm or replace the A§10 sketch; write A§11 and Decision Log entries → §P7
- [x] P7a.1 Several cores per Node: Group ids on messages, a data directory per Group, the Simulation hosting several Groups (A§11.9) → §P7
- [x] P7a.2 The Meta Group with a fixed Slot table; keys hashed to Slots; a Group's tree keyed by Slot (A§11.2, A§11.4) → §P7
- [x] P7a.3 Routing: forward once, answers carry the table version; clients learn (A§11.3) → §P7
- [x] P7a.4 Expose: a Group that doesn't check ownership serves a key it doesn't own → §P7
- [x] P7a.5 Expose: the Meta Group flips the table and nobody confirms; two Groups serve one Slot → §P7
- [x] P7a.6 The one-owner verdict and the across-Groups End-state check (A§11.12) → §P7
- [x] P7a.7 The Move: intent, copy while serving, freeze, accept, done, drop; carried on after a crash at each step (A§11.4) → §P7
- [x] P7a.8 Versions as Epoch and index (A§11.5) → §P7
- [x] P7a.9 Store-wide Sessions: ids from the Meta Group, registering with a floor, moving with a Slot (A§11.6) → §P7
- [x] P7a.10 Store time from the Meta Group (A§11.7) → §P7
- [x] P7a.11 Merged scans; `cross_group` for Transactions (A§11.8) → §P7
- [x] P7a.12 Real shell: `kvnode` hosting several Groups, `kvctl move-slot`, harness → §P7
- [x] P7a.13 Stage 7a suite, real runs with the pause of a Move measured, `retros/rung-7.md` → §P7
- [x] P7b.0 Break stage 7b (gossip, A§11.10) into tasks → §P7
- [x] P7b.1 `internal/gossip`, a pure core: the Node list with addresses, the counters detector, exchanging everything with a Node on first contact; the Simulation carries its messages between Nodes → §P7
- [x] P7b.2 The whole table and Store time ride on gossip; Nodes stop asking the Meta Group on a timer; routing avoids Nodes thought dead; how long news of a Move takes to reach every Node, against 7a → §P7
- [x] P7b.3 Expose: ownership decided by gossip (a Move announced by gossip, highest version wins); two owners under a Partition → §P7
- [x] P7b.4 Expose: Membership decided by gossip (a Group drops a Member that gossip calls dead, with no Entry); two Leaders under a Partition → §P7
- [x] P7b.5 The SWIM detector: ping, ping through others, suspicion, answering a suspicion → §P7
- [x] P7b.6 Detectors compared: time to notice a dead Node, false alarms under a slow Node, a one-way cut, a Partition and lost messages, and messages sent → §P7
- [x] P7b.7 Joining with one address, leaving; real shell: gossip on the Nodes' network, addresses learned as they arrive; `kvnode -join`, `kvctl nodes` → §P7
- [x] P7b.8 Stage 7b suite (the 7a suite with gossip carrying the table, plus gossip Faults), real runs, the 7b part of `retros/rung-7.md` → §P7
- [x] P7c.0 Design load, threshold and damping for rebalancing; break stage 7c (A§11.11) into tasks → §P7
- [x] P7c.1 The Slot table holds each Group's Member list; Meta records a change wanted and a change done; in the Simulation a Node starts a replica when the table says so and each Group's Leader makes the change → §P7
- [x] P7c.2 Naive replacement: one Meta Member's word, no wait; add a Spare, remove the dead Member; a returned Node drops its data when gossip says it was replaced. Expose: running Nodes replaced under a Partition, and an Acknowledged write lost to a wrong drop → §P7
- [x] P7c.3 Fix: a Majority of Meta Members and one wait; the Meta Group's own Members; a returned Node drops only on the Group's Committed Member list and becomes a Spare; a Move stuck on a lost Majority finishes after replacement → §P7
- [x] P7c.4 Load: per-Slot counts at each data Leader, smoothed, carried by gossip; the weight of a read measured on real processes and built in → §P7
- [x] P7c.5 Naive rebalancing: latest report, one line, no damping. Expose: Slots moving back and forth under steady and under shifting load, counted in Moves and in time frozen → §P7
- [x] P7c.6 Fix: two lines, one Move at a time, rest per Slot, smoothed load, nothing while a Node is suspected or being replaced; lines, window and rest settled by measurement → §P7
- [x] P7c.7 Real shell: Nodes start and drop replicas as the table says, replication takes addresses from gossip, `kvctl` shows replacements and load, `kvbench` can skew load onto a few Slots, harness Faults `kill-for-good` and `skewed-load` → §P7
- [x] P7c.8 Stage 7c suite (the 7b suite with automation on, plus Nodes lost for good and skewed load), real runs, the 7c part and the Rung 7 verdict in `retros/rung-7.md` → §P7

### Phase 8 — Rung 8: transactions across Groups (§P8)
- [x] P8.0 Confirm or replace the A§10 sketch; write A§12 and Decision Log entries → §P8
- [x] P8a.1 Naive Transactions across Groups: the receiving Node drives prepare and finish from memory; switches leave Locked keys readable and the version with no offset; the Simulation's workload sends them and the whole-store model checks them (A§12.2). The offset itself (A§12.6) was built here, behind its switch: without it every run fails on versions and nothing else can be seen → §P8
- [x] P8a.2 Expose: a driver that crashes between two finishes leaves a Transaction half-committed; so does a client's retry through a second Node → §P8
- [x] P8a.3 Expose: a version that goes backwards (now); a Stale read of a Locked key (after P8a.4: with the decision in memory the client is answered only after every finish, so the read can't be told apart) → §P8
- [x] P8a.4 Fix: begin, decision and confirmed Entries in the Coordinator Group; its Leader drives, a new Leader carries on, abort after a bound (A§12.2, A§12.4) → §P8
- [x] P8a.5 Fix: Locked keys ask the Coordinator Group, wait, then `locked`; old Locks are asked about; a Move waits for a Slot's Locks (A§12.3, A§12.7) → §P8
- [x] P8a.6 The per-Slot version offset (A§12.6), built in P8a.1: its regression seeds from P8a.3, and Moves resetting it → §P8
- [x] P8a.7 Stage 8a suite with the verdicts of A§12.9, crashes between every pair of steps, and the 8a part of `retros/rung-8.md` → §P8
- [x] P8b.1 Naive conflicts: Transactions wait for each other's Locks with no order and no bound. Expose: Transactions that never decide. It takes Transactions over three Groups: two keys can't form a ring (A§12.5) → §P8
- [x] P8b.2 Fix: age, older wins, `txn_conflict` carrying the age, the client's retry (A§12.5) → §P8
- [x] P8b.3 Measure a Move stalled behind a slow Transaction; then Moves abort undecided Transactions (A§12.7) → §P8
- [x] P8b.4 Forget a record when every Group has confirmed (A§12.2) → §P8
- [x] P8b.5 Real shell: the steps between processes, `kvctl` shows Locks and undecided Transactions, `kvbench` can send a share of Transactions across Groups → §P8
- [x] P8b.6 Stage 8b suite with Moves and automation on, real runs, the 8b part and the Rung 8 verdict in `retros/rung-8.md` → §P8

### Phase 9 — Rung 9: the Leaderless variant (§P9)
- [x] P9.0 Confirm or replace the A§10 sketch; design the checks; write A§13 and Decision Log entries → §P9
- [x] P9a.1 The Leaderless core, naive: Home Nodes on a ring, writes to W with Hints that are kept and never delivered, reads joining R answers, last-write-wins by a counter per key, on disk; run in the Simulation with a workload of adds, removes and reads (A§13.2–13.5, A§13.10) → §P9
- [x] P9a.2 The four verdicts and the stale-read rate (A§13.11) → §P9
- [x] P9a.3 Expose: last-write-wins loses Acknowledged adds under a Partition → §P9
- [x] P9a.4 Expose: Hints never delivered leave Home Nodes apart after healing → §P9
- [x] P9a.5 Fix: the set where add wins (A§13.5) → §P9
- [x] P9a.6 Fix: Hints delivered; writes sent on and retried (A§13.4) → §P9
- [x] P9a.7 Stage 9a suite at W=1 R=1, W=2 R=2 and W=3 R=1; availability beside the consistent store of Rung 8 under the same scenarios; the 9a part of `retros/rung-9.md` → §P9
- [x] P9b.1 Expose: with no Exchange, a Home Node away longer than the retries last stays behind → §P9
- [x] P9b.2 Fix: the Exchange (A§13.6) → §P9
- [x] P9b.3 Expose: a remove forgotten at once lets an element come back; then keep the record (A§13.7) → §P9
- [x] P9b.4 Repair on read; the stale-read rate at R=1 before and after (A§13.6) → §P9
- [x] P9b.5 Time-to-live by stamped deadline, with the disagreement under clock skew measured; scans (A§13.8, A§13.9) → §P9
- [x] P9b.6 Stage 9b suite and the 9b part of the retro → §P9
- [x] P9c.1 Real processes: a `kvnode` mode with HTTP, TCP and disk; `kvbench` workload and verdicts; local harness (A§13.12) → §P9
- [x] P9c.2 Docker with the network cut, for the Leaderless variant and for the store of Rung 8: `harness/partition.sh`. The compose file gained `KVNODE_ARGS`, with the user's leave; the store of Rung 8 reaches other Nodes' HTTP through a second proxy per link (`kvnode -forward-via`) → §P9
- [x] P9c.3 The comparison runs, the 9c part and the Rung 9 verdict in the retro → §P9

### Phase 10 — Writeup (§P10)
- [>] P10.1 `RESULTS.md` via `/docs`: Rung results and a Decision Log walkthrough → defer until: the last Rung in scope passes

## Project structure
```
cmd/kvnode/        the real shell: one Node as a process
cmd/kvctl/         admin and Unsafe recovery
cmd/kvbench/       real-run client: load, History recording, signals
internal/core/     contract types: events, outputs (A§4.2)
internal/naive/    Rung 1's primary-backup core
internal/raft/     the consensus core
internal/fsm/      state machine: commands, Sessions, Expiry
internal/tree/     copy-on-write ordered tree
internal/storage/  Log segments, vote file, Snapshots
internal/transport/ TCP between Nodes
internal/server/   HTTP API
internal/sim/      Simulation: scheduler, clock, network, disk
internal/check/    History, Porcupine model, End-state comparison, signals
internal/rungtest/ Fault scenarios per Rung, run against any core in the Simulation
internal/shardtest/ the Rung tests for several Groups (Rungs 7–8), apart so that no package's tests outrun Go's limit
harness/           run scripts for real runs
deploy/            Docker Compose
retros/            one retro per Rung
```
`internal/core`, `internal/naive`, `internal/raft`, `internal/fsm` and `internal/tree` are the pure packages P0.4 guards.

## Dependency handling
Only `github.com/anishathalye/porcupine`, imported by `internal/check` and tests (A§3). Adding anything else needs the tradeoff discussion and an A§3 entry first.

## §P0 — Scaffolding
The contract types come first because both shells and both cores (naive and Raft) are written against them.
**Verify:** `go build ./...` and `go vet ./...` pass; the determinism lint fails when a forbidden import is added to a pure package, and passes otherwise.

## §P1 — Rung 1
- **Reads and retries:** a get is a Log command, and clients never retry. An unanswered request is recorded as outcome unknown (A§6.2, A§6.3).
- **Crashes before Rung 3:** in Rungs 1–2 a crashed Member restarts with its state intact, as if its storage were perfect. Rung 3 takes that away.
- **Naive first (Decision Log):** P1.4–P1.5 land and are recorded before any Raft code.
- **Real shell** arrives here so the baseline can be measured. It stays thin: everything it does is also done by the Simulation.
**Verify:**
- P1.5: at least one recorded seed per failure (two primaries; lost Acknowledged write), each failing Porcupine or End-state comparison.
- P1.9: every seed in the suite passes all three verdicts on 3 and 5 Members; the P1.5 seeds now pass.
- P1.12: numbers recorded on local processes and Docker. Ask the user before running.

## §P2 — Rung 2
**Verify:** P2.3's seeds fail with the P2.2 shortcuts and pass after P2.4–P2.5. The suite passes under delay, reordering, duplication and one-way Partitions. Read index is measured against Log reads.

## §P3 — Rung 3
The v1 state is replaced by the tree here, because Snapshots need it.
**Verify:** P3.2's seeds fail before and pass after. Every Acknowledged write survives a full restart, a crash between write and durable, and a stalled disk. A Member returning after its Entries were trimmed catches up. The tree passes a randomized comparison against a simple reference.

## §P4 — Rung 4
P4.4 is a design decision inside the Rung: stop and get it approved before coding it.
**Verify:** with torn writes and bit flips injected, corruption is always detected and never applied, and Members end identical.

## §P5 — Rung 5
**Verify:** P5.2's seeds fail before and pass after. Members stay identical under clock skew and jumps. Scans and transactions pass the whole-store model. The lease variant's Stale read is reproduced from a seed with the lease on, and can't be reproduced with it off.

## §P6 — Rung 6
**Verify:** P6.1's seeds fail before and pass after. No Term ever has two Leaders during a change, including changes that straddle Terms. A dead Member is replaced while clients keep writing. Unsafe recovery prints what it discarded.

## §P7 — Rung 7
Design is A§11. Three stages (A§11.1), each ending with a retro section and the user's approval.
**Verify (7c):** P7c.2's and P7c.5's seeds fail naive and can't be reproduced after. A Node lost for good is replaced in every Group it was in, the Meta Group included, with no hand on it, and the run stays Linearizable with one owner per Slot. A minority side of a Partition replaces nobody. A returned Node never drops data its Group still counts on. With one Group carrying most of the load, the busiest Group ends near the low line; load that is even, or that no Move can even out, is left alone after the first few Moves. Moves made and time frozen are reported, naive against damped.

**Verify (7b):** P7b.3's and P7b.4's seeds fail with gossip deciding and can't be reproduced with it only informing. The 7a suite passes with the table carried by gossip and no timed reads of the Meta Group. After Faults stop, every live Node agrees on which Nodes are alive and on the table version. A Node started with one address is known to all. The two detectors' numbers are reported side by side.

**Verify (7a):** P7a.4's and P7a.5's seeds fail before and pass after. No run ever has two Groups serving one Slot. Single-key operations stay Linearizable while Slots move, including Moves interrupted by crashes and Partitions at every step. A retry that crosses a Move takes effect once. The pause of a Move is reported.

## §P8 — Rung 8
Design is A§12. Two stages (A§12.1), each ending with a retro part and the user's approval.

**Verify (8a):** P8a.2's and P8a.3's seeds fail naive and can't be reproduced after. With the Coordinator Group's Leader or another Group's Leader crashed between any two steps, every Transaction ends the same way in every Group, the run is Linearizable over the whole store, and after Faults stop no key is Locked and nothing is undecided. Versions never go down for a key.

**Verify (8b):** P8b.1's seed fails naive and can't be reproduced after. Under steady contention every Transaction is decided and none loses for ever. A Move during Transactions waits only for commits already decided. No record is left at the end. The Rung 7 load reaches 0.9× of Rung 7 on real processes, and the cost of a Transaction across Groups is stated.

## §P9 — Rung 9
Design is A§13. Three stages (A§13.1), each ending with a retro part and the user's approval. The Leaderless variant is its own packages and touches none of the consensus core.

**Verify (9a):** P9a.3's and P9a.4's seeds fail naive and can't be reproduced after. With add-wins sets, no run loses an Acknowledged add or brings back a removed element, at any of the three settings, under Partitions and crashes. Availability during a Partition is reported for each setting beside the consistent store's.

**Verify (9b):** P9b.1's and P9b.3's seeds fail naive and can't be reproduced after. Every run ends Converged, including after outages longer than any retry. The stale-read rate and the time two Nodes disagree about an expired element are reported.

**Verify (9c):** the same verdicts on real processes, and under a real Partition in Docker. The comparison table is in the retro.

## §P10 — Writeup
**Verify:** each Decision Log entry is walked through with the evidence from its Rung.

## Commit sequence
- One branch per phase (`p0-scaffold`, `p1-rung1`, …), merged by pull request. Never commit to `main`.
- One commit per checklist item where practical, message prefixed with the item (`P1.6: Raft election`). The item's checkbox flips in the same commit.
- Commit and push only when the user asks.
