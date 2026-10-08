# Rung 4 retro: a disk that lies

Status: **done**. A store that trusts its disk gives wrong answers. A store that checks its disk and repairs it carelessly still loses Committed Entries. The store this Rung ends with does neither, in 6,400 simulated runs and 10 real ones. The price is that a Group whose Majority is damaged at once stops and waits for an operator.

Environment: the Simulation. Until this Rung its disk was an idealised structure in memory. It is now the real storage code (`internal/storage`) running on a filesystem held in memory (`storage.MemFS`), which keeps two views of every file: what has been written, and what has been synced. A crash keeps only what was synced. Rung 4 adds two ways for it to lie:
- **Torn writes.** A crash may keep part of a write that was in progress, and part of what it keeps may be zeros, where the file had grown but the data hadn't arrived.
- **Bit flips.** One bit inverts somewhere in what a Member stored. Nothing notices until the Member reads its disk again, which is when it restarts.

Every simulated run since Rung 3 now goes through the real file format. The Rung 3 suite passes unchanged on it (5,200 runs).

## Exposed: a store that trusts its disk (P4.2)

The Rung 3 store's files have no checksums (Decision Log: "No checksums on disk until Rung 4"). It believes whatever it reads back.

300 seeds per cell, 3 / 5 Members. A flip lands on any stored byte with equal chance.

| Faults | Unsafe (wrong answer or tripped check) | Not Linearizable | Core's safety check tripped | Members diverged | A Member can't restart |
|---|---|---|---|---|---|
| Bit flips, one Member at a time | 60 / 30 | 25 / 4 | 37 / 26 | 59 / 55 | 134 / 129 |
| Bit flips with everything else | 37 / 18 | 28 / 7 | 11 / 12 | 32 / 27 | 56 / 58 |

Torn writes alone (200 seeds, no flips): a Member can't restart in 37–106 runs, depending on the scenario.

One flipped bit does one of four things, depending on where it lands.

### 1. It is believed (`bit-flips`, 3 Members, seed 87)
Node 1 ends the run holding a key named **`c2`**, with the value `c1-29`, at version 84. No client ever wrote a key called `c2`: they use `k0`, `k1` and `k2`. The letter `k` is 0x6B and `c` is 0x63, one bit apart. A bit flipped in a stored Entry, node 1 restarted, read the Entry back, and applied a put to a key that never existed. Nodes 2 and 3 have no `c2`.

The History of this run is Linearizable and nothing crashed. The damage sits in one Member's state, waiting for that Member to become Leader.

### 2. It reaches a client (seed 7)
The History isn't Linearizable: a client was given an answer that no order of the requests could produce.

### 3. It makes the core stop on an impossible state (seed 9)
A flipped bit in an Entry's index or Term gives the core something that can't be true, such as a follower told to replace an Entry it knows is Committed, or an index in the hundreds of trillions.

### 4. It makes the disk unreadable (seed 1)
The Member can't parse what it finds and doesn't start. This is the least bad outcome, and it happens by accident: the damage happened to break the format.

**Torn writes** mostly end the fourth way: a half-written record doesn't parse. The Rung 3 store already copes with a write that was cut short cleanly. What it can't tell apart is a record that is all there but partly zeros.

### Reproduce
```
go test -run 'TestLyingDisk' -v ./internal/rungtest/
```

## Exposed, part two: a store that repairs itself and carries on (P4.4)

Checksums turn all four outcomes above into one: the Member notices, removes what it can't verify, and gets the missing Entries back from its Leader. That is what the careless store does. It then votes and stands for election like any other Member.

The trouble is that it may have acknowledged Entries it no longer holds. An Entry is Committed because a Majority stored it. If one of that Majority loses it and then votes for a Member that never had it, a Leader is elected without a Committed Entry.

300 seeds per cell, 3 / 5 Members.

| Faults | Unsafe | Not Linearizable | Core's safety check tripped |
|---|---|---|---|
| Bit flips, one Member at a time | 1 / 0 | 1 / 0 | 0 / 0 |
| Bit flips with everything else | 86 / 24 | 81 / 15 | 8 / 10 |
| Followers damaged, then their Leader lost (`half-repaired`) | 66 / 73 | 66 / 26 | 1 / 49 |

- **`rot-and-everything`, 3 Members, seed 6:** the History isn't Linearizable. An Acknowledged write is gone.
- **Seed 27:** a follower has Committed further than its new Leader's Log reaches, and the Leader's core stops on an Index it doesn't hold.
- **`half-repaired`, 3 Members, seed 7:** the same, in the scenario built to cause it.

With one Member damaged at a time and the rest healthy, the careless store passes 599 runs in 600. It is repaired before its vote matters. This is Rung 3's fifth lesson again: one Member forgetting looks fine.

### Reproduce
```
go test -run 'TestCarelessRepair' -v ./internal/rungtest/
```

## Fix: check everything, repair, abstain (P4.3–P4.4)

**On disk (A§5.4).**
- Every Log record carries a checksum of its length, a checksum of its body, and an end mark. The separate checksum on the length is what tells a flipped length apart from a record cut short by a crash.
- The Term and vote are kept in two copies, written one after the other. A Member reads both and uses the newer one that verifies.
- The Snapshot carries a checksum.
- A record cut short at the very end of the Log is an interrupted write and is dropped quietly, as in Rung 3. Anything else that fails to verify is damage.

**On finding damage (A§6.8).** The Member first writes a mark that says it is damaged, and only then removes what it can't verify: the Log from the bad record onward, or the whole Log if the Snapshot is bad. The mark comes first so that a crash during the repair can't leave a shortened Log that looks healthy.

**While Recovering.** A damaged Member doesn't vote and doesn't stand for election. It tells its Leader that it lost Entries, so the Leader stops counting on what it had acknowledged, and it takes Appends as usual. When it holds everything its Leader held, it clears the mark and takes part again.

**If both copies of the Term and vote are damaged**, the Member doesn't start. It can't say how it voted, and guessing could give two Leaders in one Term. I first planned to let it adopt its Leader's Term. That is unsafe, and the Decision Log records why ("A Member that can't say how it voted doesn't start").

### Same scenarios, same harness

`TestRung4`: 16 scenarios × 3 and 5 Members × 200 seeds = **6,400 runs, none unsafe**. 190 ended Stalled, and in 4 a Member couldn't start.

The three Rung 4 scenarios, 300 seeds per cell, 3 / 5 Members:

| Faults | Unsafe | Ended Stalled | A Member can't start |
|---|---|---|---|
| Bit flips, one Member at a time | 0 / 0 | 4 / 0 | 0 / 0 |
| Bit flips with everything else | 0 / 0 | 182 / 96 | 4 / 0 |
| Followers damaged, then their Leader lost | 0 / 0 | 0 / 0 | 0 / 0 |

Every run that didn't stall ended with identical Members.

**Stalled is the cost.** A run ends Stalled when a Majority is Recovering at once. Recovering Members don't vote, so no Leader can be elected, so nobody can bring them up to date. It doesn't clear by itself. In the scenario that damages a random Member every few hundred ticks on top of crashes and Partitions, that is 61% of runs with 3 Members and 32% with 5. With occasional damage it is 1.3% and 0%. Five Members tolerate it far better than three, because three can spare only one.

The Group stops even when one healthy Member holds every Entry. It can't know that it does. The way out is Unsafe recovery, which is Rung 6.

**Slow elections.** With one of three Members abstaining, an election needs both of the others, and one of them may be unable to win because its Log is behind. In 2 of 600 runs (seeds 251 and 353) that took 700–900 ticks' worth of failed rounds, longer than the clients kept trying. Both Groups did elect a Leader and converge. This is not new in Rung 4: a crashed Member has the same effect. Pre-vote would shorten it and isn't built.

### Testing the tests

I broke the fix three ways and ran the suite.

| Break | Caught by the suite as it was | After |
|---|---|---|
| Storage repairs without leaving the durable mark | Yes: 2 of 300 runs | — |
| A Recovering Member resumes on its first successful Append, caught up or not | **No**: 0 of 1,200 runs | **Yes**: 58 of 100 runs |
| The Leader ignores a follower saying it lost Entries | No | No: more stalls, nothing unsafe |

The second break went unnoticed for a plain reason: an Append carries up to 64 Entries, the simulated Logs are shorter than that, and so the first successful Append always caught the Member up. The rule and its absence behaved the same. Two things fixed that:
- `half-repaired`, a scenario that damages the Leader's followers and then removes the Leader while they catch up.
- `TestRecoveryInSmallSteps`, the same store with Appends capped at two Entries (`raft.Config.MaxBatch`), so catching up takes many messages.

The third break isn't caught by any simulated run, and I think that is correct: while the damaged Member abstains, the Leader's stale count can't elect anyone. With the careless store the same break is unsafe in 299 of 300 runs, so the rule matters as soon as abstaining is gone. `TestLeaderAcceptsThatARecoveringFollowerLostEntries` covers it as a unit test.

### What broke on the way
- **Torn writes did nothing at first.** The store buffered a write and handed it to the filesystem whole, so a crash could never land in the middle of one. The simulated disk needed the store to flush as it goes.
- **A flipped length looked like a short record.** A length field that flips to a large number runs off the end of the file, which is exactly what an interrupted write looks like. The store would have dropped it quietly, with every Entry after it. That is why the length has its own checksum.
- **The first flip generator favoured small files.** It picked a file and then a byte, so the 24-byte state files took a third of all flips. It now picks uniformly over every stored byte, and the seeds were re-pinned.
- **My first scenario for this section kept injecting Faults after the repair point**, and reported 276 of 300 runs diverged. The store was fine. The scenario was wrong.

## Real runs (P4.5)

M4 Pro, 10 s each, default mix (35% gets), reads by index. The corruption runs kill -9 a Member, flip one bit in the middle of its newest Log segment, and start it again 3 s later.

### Cost of checking (no Faults)
| Environment | Members | Clients | Rung 3 | Rung 4 | Ratio |
|---|---|---|---|---|---|
| Local | 3 | 64 | 1,412 | **1,392** | 0.99× |
| Local | 3 | 8 | 220 | 217 | 0.99× |
| Docker | 3 | 64 | 12,243 | 14,629 | 1.19× |

The checksums cost nothing measurable next to a disk flush. The Docker figure moves more between runs than the checksums could explain, and Rung 3's caveat about it still applies.

### Under Faults (clients retrying in Sessions, 8 clients)
| Environment | Members | Fault | Requests/s | Rejected / lost | Longest pause in writes | Verdicts |
|---|---|---|---|---|---|---|
| Local | 3 | A follower's Log corrupted | 241 | 0 / 0 | 50 ms | Linearizable; identical |
| Local | 3 | The Leader's Log corrupted | 241 | 0 / 0 | 181 ms | Linearizable; identical |
| Local | 5 | A follower's Log corrupted | 167 | 0 / 0 | 87 ms | Linearizable; identical |
| Local | 3 | Leader killed, restarted from its disk | 230 | 0 / 0 | 284 ms | Linearizable; identical |
| Docker + toxiproxy | 5 | Leader isolated | 3,228 | 0 / 0 | 134 ms | Linearizable; identical |

In both 3-Member corruption runs the Member logged that it found damage and was Recovering, and ended with the same data as the others. I ran those two twice and the 5-Member one once, before the damage line was added to the output, so for that one the evidence is the identical end state only.

### Reproduce
```
go test -run 'TestRung4|TestRecoveryInSmallSteps|TestLyingDisk|TestCarelessRepair' ./internal/rungtest/
go test ./internal/storage/
CLIENTS=64 harness/run.sh local 3
RETRY=1 harness/run.sh local 3 corrupt-follower
RETRY=1 harness/run.sh local 3 corrupt-leader
```

## Verdict
| Check (README → Success Criteria) | Result |
|---|---|
| Exposed | **Pass**: a believed bit flip, a wrong answer, a tripped check and an unreadable disk (`TestLyingDiskIsExposed`); a careless repair that loses Committed Entries (`TestCarelessRepairIsExposed`) |
| Faults: corruption is always detected and never applied; Members stay identical | **Pass**: 6,400 simulated runs, 3,464 single-bit flips checked one by one in `internal/storage`, and 3 real corruption runs. A Group with a Majority damaged at once stops rather than guess |
| Numbers: at least 0.9× Rung 3 | **Pass**: 0.99× (1,392 against 1,412 requests/s) |
| Numbers: recovery within 400 ms of losing a Leader | **Pass**: 134–284 ms |
| Retro | This document |

## Lessons
1. **Detecting damage is the easy half.** Checksums removed every wrong answer that came from believing a damaged disk. What a Member may do after it has lost something it promised to keep is the hard half, and getting it wrong is unsafe in up to 29% of the harsh runs.
2. **A Member that has lost Entries is no longer the Member that acknowledged them.** Its vote was given on the strength of a Log it doesn't have. Until it has caught up it shouldn't be counted.
3. **Safety here was bought with availability, and the bill is visible.** 61% of the harshest 3-Member runs end stopped. That number belongs in the retro next to "none unsafe", because the second is not impressive without the first.
4. **A rule can be invisible because of a constant somewhere else.** The catch-up rule looked untested, then looked unnecessary, and was neither. A batch size of 64 hid it.
5. **A scenario is code and has bugs.** The one I wrote to catch a bug reported 276 failures of its own making.
6. **My first design for a lost vote was wrong, and reading it back as a sequence of events showed it.** Adopting the Leader's Term lets a Member vote twice in a Term.
