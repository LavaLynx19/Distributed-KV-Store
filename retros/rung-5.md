# Rung 5 retro: time

Status: **done**. A store whose Members each trust their own clock gives wrong answers under clock Faults, and under a plain restart. The store this Rung ends with judges time by the Log alone, and adds range scans, Transactions within a Group, and Session cleanup. 14,500 simulated runs and 8 real ones are clean. The lease-read variant is built, measured at 1.3–1.5× read index, and shown to give Stale reads when a Leader's clock runs slow.

Environment: the Simulation. Each Member now has its own clock. It can run faster or slower than true time, which also speeds up or slows down that Member's ticks, and it can jump forwards or backwards. Clients give 40% of their puts a time-to-live of 100–600 units (a tick is 10).

## Exposed: each Member judges Expiry by its own clock (P5.2)

The naive store does the obvious thing. A put with a time-to-live stores a deadline: the Leader's clock reading plus the time-to-live. Each Member then decides whether the key still exists by comparing the deadline with its own clock.

100 seeds per cell, 3 / 5 Members.

| Faults | Not Linearizable | Linearizable, but Members end with different keys |
|---|---|---|
| None | 0 / 0 | 0 / 0 |
| Leader crashes and restarts, clocks perfect | 1 / 1 | 0 / 0 |
| Clocks at different speeds, Leader moving | 12 / 16 | 47 / 52 |
| Clocks jumping, Leader moving | 24 / 18 | 31 / 47 |

More than half the runs with a clock Fault end wrong one way or the other.

### 1. A key comes back (`clock-skew`, 3 Members, seed 1)
A client is told a key is gone. Leadership moves to a Member whose clock is behind, and a later read finds the key there again. No order of the requests explains that.

### 2. Members quietly hold different keys (`clock-skew`, 5 Members, seed 1)
Every answer the clients got was consistent. At the end node 1 holds `k2` and node 3 doesn't. Node 3's clock had passed the deadline and node 1's hadn't. Nothing is wrong yet, and the next change of Leader decides which of them is right.

### 3. It fails with perfect clocks too (`crash-leader`, 3 Members, seed 99)
This one I didn't expect. No clock was touched. A Member restarts and applies its Log again, later than it did the first time. A compare-and-set that found the key alive the first time finds it expired on the second pass, and is refused. That Member now holds different data from the others, permanently.

The flaw is deeper than skew: what a Member holds depends on when it applied each Entry, and not only on the Entries.

### Reproduce
```
go test -run 'TestOwnClock' -v ./internal/rungtest/
```

## Fix: time comes from the Log (P5.3)

- **The Leader stamps each command** with its clock reading. The stamp is put into the command by the shell of the Member that receives the request, so the consensus core still never sees time, and the disk format didn't change.
- **Log time** is the highest stamp applied so far. It never goes back.
- **A deadline** is Log time plus the time-to-live, fixed when the put is applied.
- **Applying any Entry that moves Log time removes every key that is due**, on every Member, at that Entry. A read never has to look at a deadline.
- **In a Group nobody is writing to**, the Leader proposes a time Entry when a deadline has passed by its own clock. Without it an idle key would never expire.

A Leader with a wrong clock now makes keys expire early or late, but every Member agrees on which keys exist after every Entry. That was the trade chosen in A§2.9.

### Same scenarios, same harness

`TestRung5`: 19 scenarios × 3 and 5 Members × 200 seeds = **7,600 runs, none unsafe**, and every run that wasn't Stalled (Rung 4's accepted cost) ended with identical Members. The four seeds above pass.

| Faults (100 seeds, 3 / 5 Members) | Own clock: wrong | Log time: wrong |
|---|---|---|
| Leader crashes and restarts | 1 / 1 | 0 / 0 |
| Clocks at different speeds | 59 / 68 | 0 / 0 |
| Clocks jumping | 55 / 65 | 0 / 0 |

### What it costs
A Leader whose clock is behind Log time can't move it, so keys can outlive their deadline. With true clocks no key with a deadline is left at the end of any of 200 runs. After 100 clock-jump runs, 42 such keys were left (3 Members) and 24 (5 Members), the same keys on every Member. They go when that Leader's clock catches up or a Leader with a later clock takes over.

## Sessions are cleaned up by Log time (P5.4)

A Session notes Log time when it is opened or used, and is removed once Log time has moved a set span past that. A request in a removed Session is answered `session_expired` and changes nothing.

`TestSessionCleanup` uses a span short enough that a Partition or a clock jump costs a client its Session in the middle of a request: 1,400 runs, clients lost their Session 2,873 times, none unsafe. The End-state comparison now also checks that Members agree on which Sessions exist.

**Testing the test:** with removed Sessions' requests applied anyway, 14 of 140 short runs fail.

## Range scans and Transactions (P5.5–P5.7)

- **A scan** returns the keys in a range, in order, up to a limit. It is a read, so it takes the read-index path.
- **A Transaction** is a list of conditions on keys' versions and a list of puts and deletes. If every condition holds, all the writes are applied in one Entry. Otherwise none are, and the answer names the conditions that failed.

Neither can be checked one key at a time. `check.StoreModel` checks a History against all keys at once. I expected that to be too slow for full-length runs and planned shorter ones. With the Simulation's three keys it wasn't: a 400-request History checks in a few milliseconds.

`TestScansAndTransactions`: 20% scans and 25% Transactions in the mix, 19 scenarios × 3 and 5 Members × 100 seeds = **3,800 runs, none unsafe, and the checker never ran out of time**.

**Testing the test** (360 short runs each):

| Break | Runs that fail |
|---|---|
| One false condition is let through | 358 |
| A Transaction's deletes are skipped | 332 |
| A scan returns one key past its limit | 342 |

## The lease variant (P5.8)

A Leader with a lease answers a read from memory without asking anyone. The argument for it is about time: a follower that hears from a Leader promises not to vote for anyone else for an election timeout, so the Leader may assume it is still the Leader for slightly less than that after a Majority last answered it.

The Leader counts that time in its own ticks, and the followers count theirs.

### Exposed: a Leader whose clock runs slow (`slow-leader`)
The Leader's clock drops to a fifth of true speed, and then it is cut off. It counts its lease five times too slowly. The others wait out their promise, elect a Leader and take writes. The old Leader goes on answering reads from data that is now out of date.

90% reads, 100 seeds, 3 / 5 Members:

| Reads by | Not Linearizable |
|---|---|
| Lease | 56 / 43 |
| Read index (the default) | 0 / 0 |

Pinned: 3 Members seed 1, 5 Members seed 2. The same seeds pass with the lease off.

With every Member's ticks at the same speed the lease is safe in all 17 other scenarios (1,700 runs). Clock jumps don't break it, because a jump changes what time it is and not how fast ticks come.

**It took a second try to see the failure.** With the usual mix of 35% reads, 200 runs showed nothing. A client next to the cut-off Leader soon sends a write, which can't commit, and then it waits on that write for the rest of the Partition and reads nothing. A Stale read needs a client that only reads. A lease is for read-heavy loads, so this is the realistic case, and the one the default mix hid.

### Testing the tests
| Break | Caught |
|---|---|
| A Snapshot forgets deadlines and Log time | Yes: 165 short runs fail |
| Log time follows the latest stamp, even backwards | No. Members still agree, so no verdict can see it. `TestLogTimeNeverGoesBack` covers it |
| The lease lasts 12 ticks longer | No, and I think it can't be: a Leader that hears nothing steps down after an election timeout anyway, which ends the lease first |
| Followers make no promise before voting | **No**, in 1,700 runs plus 600 of a scenario written to catch it. `TestPromiseNotToVote` covers the rule as a unit test only |

The last one is a real gap. The promise matters when a candidate with a higher Term turns up while the Leader's lease is running, has a Log as long as everyone's, and wins before the Leader hears of it. I couldn't make the Simulation produce that often enough to see.

## Real runs (P5.9)

M4 Pro, 10 s each.

### Cost of the Rung (local, 3 Members, 64 clients, default mix)
| | Requests/s | p50 |
|---|---|---|
| Rung 4 | 1,392 | 45.6 ms |
| Rung 5 | **1,378** | 46.1 ms |
| Rung 5, 40% of puts with a time-to-live of 50–200 ms | 1,274 | 49.4 ms |

Stamps cost nothing measurable. Short time-to-lives cost 8%: every deadline that passes adds a time Entry or rides on a write, and each is one more thing to commit.

### Lease against read index (90% gets, 3 Members, 64 clients)
| Environment | Read index | Lease | Gain | Read p50 |
|---|---|---|---|---|
| Local | 1,840 | 2,774 | **1.51×** | 34.4 → 21.3 ms |
| Docker | 17,232 | 22,673 | **1.32×** | 3.62 → 2.31 ms |

A lease read needs no message and no disk, and still takes 21 ms locally. The shell handles a batch of events, stores everything the batch asked for with one sync, and only then answers anything in the batch. A read that needs nothing stored waits for the writes it happened to share a batch with. Answering such reads before the sync would be the next thing to try, and it isn't done.

### Under Faults (clients retrying in Sessions, 8 clients, 40% of puts with a time-to-live)
| Environment | Members | Fault | Requests/s | Rejected / lost | Longest pause in writes | Verdicts |
|---|---|---|---|---|---|---|
| Local | 3 | Leader killed, restarted from its disk | 207 | 0 / 0 | 395 ms | Linearizable; identical |
| Docker + toxiproxy | 5 | Leader isolated | 1,991 | 0 / 0 | 174 ms | Linearizable; identical |

395 ms is inside the 400 ms target with nothing to spare. Earlier Rungs measured 159–313 ms for the same Fault. One run doesn't say whether this Rung made it slower or the election timeouts fell badly.

**Not covered by real runs:** `kvbench` doesn't send scans or Transactions. Over real sockets they are exercised only by the API tests in `internal/server`. The lease's Stale read is shown in the Simulation only: I have no way to slow one process's clock in a real run.

### Reproduce
```
go test -run 'TestRung5|TestOwnClock|TestSessionCleanup|TestScansAndTransactions|TestLease' ./internal/rungtest/
CLIENTS=64 harness/run.sh local 3
CLIENTS=64 TTL_PCT=40 harness/run.sh local 3
CLIENTS=64 READ_PCT=90 READS=index harness/run.sh local 3
CLIENTS=64 READ_PCT=90 READS=lease harness/run.sh local 3
RETRY=1 TTL_PCT=40 harness/run.sh local 3 kill-leader
```

## A flaky test, found on the way

`TestFullRestartFromSnapshots` and `TestReturningMemberIsCaughtUpBySnapshot` failed about one run in three when the storage tests ran beside them, with a put answered "outcome unknown". They fail the same way on `main`, so this Rung didn't cause it. The failing put was always at or just before a multiple of 25, which is how often those tests take a Snapshot.

Storing a Snapshot takes several syncs, and the Leader does nothing else meanwhile. With the disk busy that took longer than the tests' 50 ms election timeout, the followers elected someone else, and the Leader's pending put lost its answer. The tests now use 200 ms: 0 failures in 8 loaded runs, against 5 in 12 before.

The tests were too tight, but what they caught is real: a slow Snapshot write stalls the Leader for as long as it takes. `kvnode`'s defaults (100 ms timeout, a Snapshot every 20,000 Entries) make it rare. Writing the Snapshot off the main loop would remove it, and isn't done.

## Verdict
| Check (README → Success Criteria) | Result |
|---|---|
| Exposed | **Pass**: a key that comes back, Members that quietly differ, and the same after a plain restart (`TestOwnClockIsExposed`) |
| Faults: Members identical under clock skew and jumps | **Pass**: 7,600 simulated runs |
| Faults: scans and Transactions are Linearizable | **Pass**: 3,800 simulated runs against the whole-store model. Real runs don't cover them |
| Lease: its Stale read reproduced from a seed with the lease on, and not with it off | **Pass**: 56 and 43 of 100 runs against 0 |
| Numbers: at least 0.9× Rung 4 | **Pass**: 0.99× (1,378 against 1,392 requests/s) |
| Numbers: the lease measured | **Done**: 1.51× read index locally, 1.32× in Docker, at 90% reads |
| Numbers: recovery within 400 ms of losing a Leader | **Pass, barely**: 395 ms and 174 ms |
| Retro | This document |

## Lessons
1. **The naive store was wrong in a way I hadn't predicted.** I built it to fail under clock skew. It also fails with perfect clocks, because a restart applies the Log at a different time. The real rule is that state must be a function of the Entries and nothing else, and skew is only the easy way to see it broken.
2. **Keeping time out of the core cost one field.** The stamp rides in the command. The consensus code, the disk format and every pinned seed from Rungs 1–4 were untouched.
3. **A design that only acts on writes forgets the idle case.** Expiry at apply time is exact and simple, and does nothing at all in a quiet Group. The time Entry was an afterthought, and so was noticing that it has to stop before the end of a run for Members to be compared.
4. **A failure can hide behind the workload.** The lease's Stale read needed read-only clients. Rung 4's missing rule needed short batches. Both times the code was wrong, or could have been, and the default mix said nothing.
5. **Some rules I can only test in the small.** Two of the lease's rules and one of Log time's have no simulated run that fails without them. I'd rather say that than count the unit tests as the same kind of evidence.
6. **The whole-store checker was cheap, against expectation.** I had designed for short Histories and didn't need them at three keys. It will need them again when the key count grows in Rung 7.
