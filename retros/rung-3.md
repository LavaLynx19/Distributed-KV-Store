# Rung 3 retro: restart amnesia and an endless Log

Status: **done**. Three failures were shown first: forgetting on restart, an endless Log, and a Member stranded by a trimmed Log. Durable state, Snapshots and catch-up by Snapshot hold Rung 3's guarantees in the Simulation and on real processes. The cost of durability is measured and becomes the baseline for Rung 4.

Environment: the Simulation, as before. Rung 3 gives each Member a simulated disk (P3.1): a write takes time to become durable, a crash loses the write in progress, a disk can stall, and a Member that restarts comes back with only what its disk holds. Until now a crash was a freeze: the Member kept its memory.

## Exposed: a store with nothing on disk (P3.2)

The Rung 2 store never asks the shell to store anything. With crashes that really lose memory, a restarted Member comes back empty: Term 0, no vote, no Log, no data.

### How often it fails
200 seeds per cell, the Rung 2 store with memory-losing crashes.

| Scenario | Members | Failed | Not Linearizable | Core's own safety check tripped |
|---|---|---|---|---|
| Leader crashes once and returns | 3 / 5 | 0 / 0 | 0 / 0 | 0 / 0 |
| Disks stall, nobody crashes | 3 / 5 | 0 / 0 | 0 / 0 | 0 / 0 |
| Random crashes and Partitions | 3 / 5 | 32 / 30 | 19 / 1 | 7 / 11 |
| Everything at once | 3 / 5 | 58 / 29 | 39 / 1 | 6 / 11 |
| Members restart one at a time | 3 / 5 | 198 / 188 | 171 / 57 | 25 / 64 |
| Every Member restarts at once | 3 / 5 | 200 / 200 | 200 / 200 | 0 / 0 |

- **One Member forgetting is survivable, by luck.** The others still hold everything and it catches up from them. That's why a test that crashes one node at a time, with pauses, can pass against a store with no durability at all.
- **A Majority forgetting is not.** Members that have forgotten their Logs can elect each other, and the new Leader doesn't have Entries the old one Committed. The safety check tripped in 151 runs is a follower being told to replace an Entry it knows is Committed.
- **A Member that forgets its vote can vote twice in one Term**, which is how two Leaders in a Term become possible. The Leader check never caught that directly in these runs; the lost Entries showed first.

### Everything gone (`full-restart`, 3 Members, seed 1)
- t=1017: a put of `k1 = "c0-37"` is acknowledged as version 90.
- t=1050: every Member crashes. t=1250: they all restart.
- t=1446: a read of `k1` answers **not found**.
- A read of `k0` at t=1496 returns a value at **version 12**. Before the crash `k0` was at version 89. Versions are Log positions, and the Log started again from 1.

### An endless Log
Nothing is ever removed from the Log. In Rung 2's 10-second real runs a Group wrote about 415,000 Entries to hold 50 keys, all kept in memory by every Member. A Member that falls behind is sent the Log from wherever it stopped, however far back that is.

### Reproduce
```
go test -run 'TestForget' -v ./internal/rungtest/
```

## Exposed, part two: a trimmed Log strands whoever fell behind (P3.6)

Snapshots fix the endless Log: each Member captures its state machine every so many Entries and drops the Log up to that point. That creates a new failure, which the plan listed third. A Member that was away needs Entries the Leader no longer has.

100 seeds per cell. The store takes a Snapshot every 20 Entries and has no way to send one to another Member (`raft.Config.NoSnapshotTransfer`).

| Scenario | Members | Failed | Not Linearizable | Members diverged |
|---|---|---|---|---|
| No Faults; messy network only; every Member restarts at once | 3 / 5 | 0 / 0 | 0 / 0 | 0 / 0 |
| Leader isolated | 3 / 5 | 99 / 100 | 0 / 0 | 99 / 100 |
| Leader crashes and returns | 3 / 5 | 99 / 100 | 0 / 0 | 99 / 100 |
| Leader deaf | 3 / 5 | 99 / 100 | 0 / 0 | 99 / 100 |
| Members restart one at a time | 3 / 5 | 99 / 100 | 0 / 0 | 99 / 100 |
| Random crashes and Partitions | 3 / 5 | 98 / 100 | 0 / 0 | 98 / 100 |
| Everything at once | 3 / 5 | 74 / 86 | 0 / 0 | 74 / 86 |

- **Not one History failed.** A Majority carries on without the stranded Member, and clients see nothing wrong. The store is one more failure away from losing its Majority, and nothing a client can observe says so.
- Every Member restarting at once strands nobody, because nobody is ahead of anybody else.
- This is the mirror image of Rung 2, where only the History caught the failures and the End-state comparison saw nothing.

### Reproduce
```
go test -run 'TestTrimmedLogStrandsAMember' -v ./internal/rungtest/
```

## Fix: durable state, Snapshots, catch-up (P3.3–P3.7)

- **One rule for durability (A§4.2).** A shell makes everything an output asks to store durable before it does anything else that output asks for. The core is never told "now it's durable": once a step returns it can assume so. Raft stores its Term and vote whenever either changes, and every Entry it appends or replaces.
- **Files (A§5.4).** A 16-byte state file and a Snapshot file, each replaced whole by write, sync, rename; and the Log as segment files of length-prefixed records. A record cut short by a crash is dropped on opening. No checksums: that is Rung 4.
- **Group commit.** The real shell takes every event that is ready, steps the core through all of them, syncs once, then acts on their outputs.
- **Copy-on-write tree (A§2.11).** The state machine keeps keys and Sessions in a treap whose shape depends only on which keys are present. Capturing the whole state is keeping two root pointers.
- **Snapshots and catch-up (A§6.4).** Each Member captures its state every so many Entries and trims its Log. A Leader sends its Snapshot to a follower that needs what was trimmed.
- **Restart.** A Member comes back with its Term, vote, Snapshot and Log. It does not remember what was Committed beyond the Snapshot, and learns that again from the Leader.

### Same scenarios, same harness
The Rung 3 store: writes take 1–6 units to reach disk, crashes lose whatever hadn't, and Logs are trimmed every 20 Entries.

13 scenarios × 3 and 5 Members × 1,000 seeds = **26,000 runs, 0 failures**, every one recovering after repair. The seeds pinned against each of the three exposures pass (`TestRung3`).

| Scenario (1,000 seeds each, 3 / 5 Members) | Recovery after repair: median | p99 | max |
|---|---|---|---|
| No Faults | 11 / 11 | 56 / 59 | 79 / 95 |
| Every Member restarts at once | 11 / 10 | 53 / 62 | 77 / 96 |
| Members restart one at a time | 18 / 15 | 211 / 190 | 315 / 340 |
| Blink (crash, restart a moment later) | 113 / 78 | 243 / 217 | 465 / 332 |
| Stalled disks | 36 / 27 | 398 / 347 | 485 / 444 |
| Everything at once | 88 / 75 | 341 / 282 | 480 / 318 |

Times are Simulation units (a tick is 10, an election timeout 100–200).

### What broke on the way

**1. A safety bug in catch-up by Snapshot, found by the suite.** The first version passed its unit tests and then failed about 1% of runs, including runs with no Faults at all: one Member ended a few Entries behind and never caught up.
- **What happened (seed 168, 3 Members, no Faults):** a follower had stored and acknowledged Entry 282. The Leader then sent it a Snapshot ending at 281. On installing it the follower replaced its whole Log, and Entry 282 was gone. The Leader still believed the follower held 282 and kept sending from there.
- **Why the Leader sent a Snapshot to a follower that was up to date:** its record of what a follower holds lags behind acknowledgements still in flight. When it trims its Log past that record, the follower looks as if it needs a Snapshot.
- **Why it matters beyond a stuck Member:** the Leader counts acknowledgements to decide what is Committed. A follower that drops an Entry it acknowledged can make the Leader count a copy that no longer exists.
- **Fix:** a follower that already holds the Snapshot's last Entry keeps everything after it. Raft's paper states this rule; I had read it as an optimisation and left it out. Regression test: `TestInstallKeepsAcknowledgedEntriesAfterTheSnapshot`.

**2. A blind spot in the suite, found by breaking the code on purpose.** With votes no longer stored, all 4,800 runs of the first Rung 3 suite still passed. A double vote needs a Member to vote, crash and restart inside one election, with a second candidate's request still on its way, and no scenario restarted Members that fast.
- **Fix:** a `blink` scenario (crash, restart 1–15 units later, on a slow network), and unit tests that check directly what must be stored before a message is sent.
- **After:** the same mutation shows two Leaders in one Term in 22 of 1,000 blink runs, and fails `TestVoteIsStoredAndSurvivesRestart` at once.

**3. A commit went out with a failing test.** My command checked the exit status of the wrong program in a pipeline. The failing test was itself at fault (it compared values while a Member was still catching up), and both were fixed in the next commit.

## Real runs (P3.8)

Environment: MacBook M4 Pro, `kvbench`, 10-second runs over 50 keys, a 10 ms tick, one run per configuration. Go syncs files on macOS with a full flush to the physical disk, which takes 15–35 ms here.

### The cost of durability (local processes, default mix, no Faults)
| Store | Members | Clients | Requests/s | Write p50 | Write p99 |
|---|---|---|---|---|---|
| Rung 2 (previous retro) | 3 | 8 | 41,570 | 170 µs | 330 µs |
| Rung 3 code, nothing on disk | 3 | 8 | 39,174 | 180 µs | 410 µs |
| Rung 3, durable | 3 | 8 | **220** | 36.2 ms | 57.1 ms |
| Rung 3, durable | 3 | 64 | **1,412** | 44.9 ms | 62.1 ms |
| Rung 3, durable | 5 | 64 | 982 | 65.4 ms | 92.2 ms |

- **Durability costs 178× at 8 clients** (39,174 → 220 requests/s). Every write waits for a full disk flush on the Leader and on a follower.
- **Group commit recovers some of it.** Eight times the clients give 6.4 times the throughput at nearly the same latency, because more requests share each flush.
- **The code change itself costs 6%** with nothing on disk (0.94× Rung 2), inside the 0.9× target.

### Read index against reads through the Log (durable, 90% gets, 3 Members, 64 clients)
| Where | Reads/s through the Log | Reads/s by read index | Ratio | Read p50 |
|---|---|---|---|---|
| Local | 1,238 | 1,701 | **1.37×** | 46.3 → 33.3 ms |
| Docker | 10,113 | 15,480 | **1.53×** | 5.53 → 3.66 ms |

The read index is now faster, as the target moved from Rung 2 requires. It is not as much faster as "no disk write" suggests. A Member does nothing else while its disk is busy, so a read still waits behind whatever flush the Leader or a follower is in the middle of.

### Snapshots (local, 3 Members, 64 clients, default mix)
| Snapshot every | Requests/s | Write p99 |
|---|---|---|
| 20,000 Entries (none in this run) | 1,412 | 62.1 ms |
| 500 Entries (about 27 in this run) | 1,338 (0.95×) | 71.1 ms |

With 50 small keys a Snapshot is tiny. This measures the cost of taking and storing one often, not of a large one.

### Docker
| Where | Members | Clients | Requests/s | Write p50 |
|---|---|---|---|---|
| Local | 3 | 64 | 1,412 | 44.9 ms |
| Docker | 3 | 64 | 12,243 | 4.91 ms |

**Docker is 8.7× faster than local here, the reverse of Rungs 1 and 2.** I haven't verified why, but the likely reason is that a sync inside Docker's Linux VM doesn't force the data onto the Mac's physical disk the way a native sync does. If so, the Docker numbers are for a weaker promise than the README makes, and the local ones are the honest measure.

### Under Faults, with real kills and restarts (clients retrying in Sessions)
| Where | Members | Fault | Requests/s | Lost / rejected | Longest pause in writes | Verdicts |
|---|---|---|---|---|---|---|
| Local | 3 | Leader killed (kill -9), restarted from disk | 229 | 0 / 0 | 176 ms | Linearizable; identical |
| Docker + toxiproxy | 3 | Leader killed, restarted from its volume | 2,209 | 0 / 0 | 159 ms | Linearizable; identical |
| Docker + toxiproxy | 5 | Leader isolated | 2,710 | 0 / 0 | 304 ms | Linearizable; identical |
| Local | 3 | Every Member killed at once, all restarted 3 s later | 147 | 0 / 0 | 3.25 s | Linearizable; identical |
| Docker | 3 | Every Member killed at once, all restarted 3 s later | 2,502 | 0 / 0 | 3.80 s | Linearizable; identical |

- A crash is now a real `kill -9`, and a restart starts from the data directory. Until this Rung the harness could only freeze a process.
- **Nothing acknowledged was lost in a full restart**, on real processes as in the Simulation. The pause is the 3 s the Members were down, plus an election.
- Losing the Leader pauses writes for 159–304 ms, inside the 400 ms target.

### Reproduce
```
go test -run 'TestRung3|TestForget|TestTrimmed' ./internal/rungtest/
CLIENTS=64 harness/run.sh local 3
CLIENTS=64 READS=log   READ_PCT=90 harness/run.sh local 3
CLIENTS=64 READS=index READ_PCT=90 harness/run.sh local 3
RETRY=1 harness/run.sh local 3 restart-all
```

## Verdict
| Check (README → Success Criteria) | Result |
|---|---|
| Exposed | **Pass**: forgetting on restart, an endless Log, and a stranded Member, pinned by `TestForgettingIsExposed` and `TestTrimmedLogStrandsAMember` |
| Faults: every Acknowledged write survives a full restart, a crash mid-write and a stalled disk | **Pass**: 26,000 simulated runs and 5 real Fault runs |
| Numbers: the cost of durability stated; baseline reset | **Done**: 178× at 8 clients; new baseline 1,412 requests/s (3 local Members, 64 clients) |
| Numbers: code with nothing on disk at least 0.9× Rung 2 | **Pass**: 0.94× |
| Numbers: read index faster than reads through the Log (moved from Rung 2) | **Pass**: 1.37× locally, 1.53× in Docker |
| Numbers: recovery within 400 ms of losing a Leader | **Pass**: 159–304 ms |
| Retro | This document |

## Lessons
1. **A rule that looks like an optimisation may be a safety rule.** "Keep the Log after the Snapshot if it matches" reads like a way to save a resend. Without it a follower can drop an Entry it acknowledged.
2. **Passing unit tests proved very little about the protocol.** The install bug passed every one I wrote for it. It took an acknowledgement in flight while the Leader trimmed its Log, which nobody would think to write as a test and 1 run in 100 produced unasked.
3. **Break the code to test the tests.** The suite could not see a Member forgetting its vote until a scenario was built for it. A suite that has never failed for a given bug says nothing about that bug.
4. **Each verdict has its own blind spot, and they differ by Rung.** Rung 2's failures showed only in the History. Rung 3's stranded Member showed only in the End-state comparison, with every History clean.
5. **One Member forgetting looks fine.** A store with no durability at all passes any test that crashes one node at a time and waits. It takes a Majority forgetting to lose data.
6. **Durability is the cost that dwarfs the others.** Consensus halved throughput in Rung 1. A disk flush per write divided it by 178.
7. **Be suspicious of a number that got better.** Docker went from 5× slower than local to 8.7× faster the moment disk syncs mattered. The likeliest explanation is that it isn't doing the same work.
