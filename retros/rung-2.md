# Rung 2 retro: Stale read and double apply

Status: **done**. Both shortcuts were shown failing first. Sessions and the read index hold Rung 2's guarantees in the Simulation and on real processes. One performance target was missed and has been moved to Rung 3 (see Verdict).

Environment: the Simulation, as in Rung 1. Rung 2 adds one-way Partitions, message delays wide enough to reorder, duplication and loss (P2.1).

## Starting point
The store Rung 1 ended with reads through the Log, and its clients never retry. Under every Rung 2 Fault it still holds its guarantees: 1,600 runs (4 new scenarios, 3 and 5 Members, 200 seeds), all three verdicts clean, every run recovering.

It pays for that in two ways. Every read costs a round of replication, and a client whose request times out has no safe way to try again. Rung 2 is about the two obvious ways to stop paying, and what each one breaks.

## Exposed: two shortcuts (P2.2–P2.3)

1. **Reads from memory.** A Member that believes it leads answers a read from its own state at once (`raft.ReadsFromMemory`).
2. **Retries.** A client that gets no definite answer sends the same request again, to another Member. The store can't tell the repeat from a new request.

### How often each fails
300 seeds per cell. A run fails if its History isn't Linearizable.

| Scenario | Members | Reads from memory | Retries |
|---|---|---|---|
| No Faults | 3 / 5 | 0 / 0 | 0 / 0 |
| Messy network only (delay, reordering, duplication, loss) | 3 / 5 | 0 / 0 | 0 / 0 |
| Leader isolated | 3 / 5 | 5 / 2 | 75 / 98 |
| Leader crashes and returns | 3 / 5 | 14 / 3 | 31 / 47 |
| Random crashes and Partitions | 3 / 5 | 27 / 12 | 130 / 157 |
| Leader mute (heard by nobody) | 3 / 5 | 0 / 0 | 23 / 32 |
| Leader deaf (hears nobody) | 3 / 5 | 6 / 15 | 188 / 184 |
| Messy network with random Faults | 3 / 5 | 28 / 22 | 156 / 174 |

- **In every failing run the Members ended identical and no Term had two Leaders.** The End-state comparison and the Leader check see nothing wrong. Only the History does.
- **Reads from memory fail rarely:** 0.7–9% of runs with a Leader change. That's the dangerous kind of bug: a test with a handful of Fault runs would pass.
- A slow, lossy, duplicating network alone breaks neither. Raft's messages are safe to repeat and reorder. It takes a change of Leader.

### Stale read (reads from memory, `crash-leader`, 3 Members, seed 14)
- t=295: a client's put to `k2` is acknowledged as version 20.
- t=300: the Leader crashes. Node 2 is elected in the next Term.
- t=424: another client reads `k2` from node 2 and gets the value from **version 19**.

Node 2 was the legitimate, current Leader. Entry 20 was in its Log, but it hadn't yet learned that the old Leader had Committed it, so it hadn't applied it. A new Leader only finds out what is Committed by committing an Entry of its own Term.

All three failing runs examined (`crash-leader` seed 14, `isolate-leader` seed 78, `leader-deaf` seed 43) had this cause: a **new** Leader answering before it had caught up, not an old one that didn't know it had been replaced. The old-Leader case is possible too, in the gap before a cut-off Leader notices the silence and steps down. That step-down is a matter of timing, not a guarantee.

### Double apply (retries, `crash-leader`, 3 Members, seed 4)
- t=316: client 0 sends "set `k2` to `c0-8` if its version is 4".
- The Leader commits it, as version 24, and crashes before answering.
- The client times out and retries with the new Leader. Its request is applied a second time. The version is now 24, not 4, so it fails.
- t=688: the client is told **version mismatch, found version 24**. Version 24 is its own write.

The client was told its write failed. It succeeded. No acknowledged write in the History has version 24.

### Reproduce
```
go test -run 'TestShortcuts' -v ./internal/rungtest/
```
`TestShortcutsAreExposed` pins three seeds for each shortcut.

## Fix: Sessions and the read index (P2.4–P2.5)

### Sessions (A§6.3)
A client opens a **Session** and numbers its requests. The state machine remembers, per Session, the last request number it applied and what it answered. A repeat of that number gets the saved answer and changes nothing. Because this happens when an Entry is applied, every Member does the same, and it survives a change of Leader. Clients can now retry safely.

### Read index (A§6.2)
A Leader answers a read from memory only when both of these hold:
1. **It has Committed an Entry of its own Term.** That's the only way it learns what its predecessor Committed. Until then reads wait. This is the condition the Stale reads above needed.
2. **A Majority has echoed a round of messages sent after the read arrived.** A Member that has voted in a newer Term refuses, so a replaced Leader never gathers the echoes.

Rounds are numbered, and only one is unconfirmed at a time. Reads that arrive meanwhile share the next round.

### Same scenarios, same harness
The Rung 2 store (read index, Sessions, retrying clients): 8 scenarios × 3 and 5 Members × 200 seeds = **3,200 runs, 0 failures**, every one recovering after repair. The six seeds pinned against the shortcuts pass (`TestRung2`).

**Each condition was removed once, to check the suite notices:**
| Removed | Result |
|---|---|
| The own-Term rule | A pinned Stale-read seed fails again |
| The confirmation round | 12 of 3,200 runs fail |

That 12 in 3,200 is the textbook Stale read, from a Leader that has been replaced without knowing. It is rarer than the new-Leader kind because a cut-off Leader steps down after about one election timeout, which leaves only a narrow gap. Narrow isn't closed.

## Real runs (P2.6)

Environment as in Rung 1: `kvbench`, 8 clients sending as fast as they can for 10 s over 50 keys, a 10 ms tick, nothing written to disk. Each configuration ran once.

### Throughput against Rung 1 (local processes, default mix, no Faults)
| | Members | Requests/s | vs Rung 1 |
|---|---|---|---|
| Rung 1 | 3 | 42,743 | 1.00× |
| Rung 2, reads through the Log | 3 | 42,658 | 1.00× |
| Rung 2, read index | 3 | 41,570 | **0.97×** |
| Rung 1 | 5 | 29,458 | 1.00× |
| Rung 2, read index | 5 | 28,884 | 0.98× |

### Read index against reads through the Log (90% gets, 3 Members)
| Where | Reads/s through the Log | Reads/s by read index | Ratio | Read p50 | Read p99 |
|---|---|---|---|---|---|
| Local | 38,652 | 34,790 | **0.90×** | 180 → 200 µs | 320 → 390 µs |
| Docker | 12,209 | 10,741 | **0.88×** | 570 → 670 µs | 930 µs → 1.1 ms |

**The read index is about 10% slower, not faster.** The likely reason, not yet tested: with nothing on disk, a read through the Log costs one round trip to a Majority and is sent the moment it arrives. A read by read index also costs a round trip, but only one confirmation round is out at a time, so a read that arrives while one is out waits for the next: up to two round trips. The batching saves messages, and here there is nothing for it to save. What the read index really avoids is a disk write per read, and Rung 1 and 2 have no disk.

### Under Faults, with clients retrying in Sessions
A Fault is injected 3 s into the run and repaired 3 s later. "Pause" is the longest stretch with no successful write.

| Where | Members | Fault | Requests/s | Rejected / lost | Longest pause in writes | Verdicts |
|---|---|---|---|---|---|---|
| Local | 3 | Leader frozen | 32,473 | 0 / 0 | 2.00 s | Linearizable; identical |
| Docker + toxiproxy | 3 | Leader isolated | 10,144 | 0 / 0 | 169 ms | Linearizable; identical |
| Docker + toxiproxy | 5 | Leader isolated | 8,308 | 0 / 0 | 313 ms | Linearizable; identical |
| Docker + toxiproxy | 3 | Leader frozen | 7,847 | 0 / 0 | 2.01 s | Linearizable; identical |

- **No request was lost or rejected in any Fault run.** In Rung 1 the same Faults left 8–12 requests per run with no answer. Retrying in a Session turns "unknown" into an answer.
- **An isolated Leader pauses writes for 169 ms (3 Members) and 313 ms (5 Members)**, inside the 400 ms target. An earlier run of the 5-Member case had one write take 414 ms, so this target has little room to spare at 5 Members.
- A frozen Leader still pauses writes for 2 s, which is the client's request timeout, not the store.

### What broke on the way: the recovery measurement
The first set of Fault runs reported recovery as "the next write to succeed after the 3 s mark". Two runs reported 0.3 ms: the script injects the Fault slightly after the mark, and a write got in first. The figure now is the longest pause in successful writes after a mark placed a second before the Fault, which doesn't depend on that timing. The four Fault runs were repeated with it. Rung 1's 136–185 ms figures used the old method. They are plausible and in line with these, but were measured the fragile way.

### Reproduce
```
go test -run 'TestRung2$' ./internal/rungtest/
READS=log   READ_PCT=90 harness/run.sh local 3
READS=index READ_PCT=90 harness/run.sh local 3
RETRY=1 harness/run.sh docker 3 isolate-leader
```

## Verdict
| Check (README → Success Criteria) | Result |
|---|---|
| Exposed | **Pass**: Stale reads and double apply, pinned by `TestShortcutsAreExposed` |
| Faults: no Stale read and exactly-once effect under one-way Partitions and delayed, reordered or duplicated messages | **Pass**: 3,200 simulated runs and 4 real Fault runs |
| Numbers: at least 0.9× Rung 1's throughput | **Pass**: 0.97× at 3 Members |
| Numbers: recovery within 400 ms of a Leader being cut off | **Pass**: 169 ms and 313 ms |
| Numbers: read index faster than reads through the Log | **Miss, moved to Rung 3** by the user's decision: 0.90× locally. The read index stays the default, since Rung 2's own promise (no Stale read) is met and its advantage needs a disk to show |
| Retro | This document |

## Lessons
1. **The Stale read I expected wasn't the one I got.** The design discussion was about a Leader that doesn't know it has been replaced. Every Stale read actually traced came from a new, legitimate Leader that hadn't caught up. Both need fixing, and only tracing which Member answered told them apart.
2. **Some failures are invisible to everything but the History.** In every failing run of both shortcuts the Members ended identical and no Term had two Leaders.
3. **Rare isn't safe.** Reads from memory failed in under 1% of some scenarios, and the textbook case in 12 of 3,200 runs. A handful of Fault tests would have passed both.
4. **An optimisation needs the cost it removes to exist.** The read index was built to beat Log reads, and came out 10% slower, because the cost it avoids (a disk write) isn't there yet. Measuring before claiming caught it.
5. **Check the measurement like the code.** A recovery figure of 0.3 ms was the tell that the metric, not the store, was wrong.
