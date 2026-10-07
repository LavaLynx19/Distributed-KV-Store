# Rung 1 retro: split brain and a lost Acknowledged write

Status: **in progress**. The naive store's failures and the Raft core's Simulation results are recorded below. Real-process runs and the baseline numbers (P1.10–P1.12) are still to come.

Environment: everything in this section ran in the Simulation (A§8.1): one process, virtual time, a seed per run. Each Member ticks every 10 time units and a message takes 1–8. Four clients work three keys, each client sitting beside one Member and sharing its view of the network.

## Exposed: the naive primary-backup store (P1.4–P1.5)

The naive store (`internal/naive`) has three flaws on purpose:
1. The primary answers the client once it has applied a write itself, before any backup has it.
2. The primary is "the lowest-numbered Member I've heard from in the last 5 ticks". Two Members that can't hear each other can both decide it's them.
3. A backup keeps the first Entry it applied at an Index and ignores a different one later.

### With no Faults it passes everything
60 runs (30 seeds on 3 Members, 30 on 5): every History Linearizable, every Member identical. That's why this design is tempting.

### Under Rung 1's Faults it fails every run
30 seeds per row. A run fails if its History isn't Linearizable, Members end with different data, or two Members lead at once.

| Scenario | Members | Failed | Not Linearizable | Members diverged | Two primaries at once |
|---|---|---|---|---|---|
| No Faults | 3 | 0 | 0 | 0 | 0 |
| No Faults | 5 | 0 | 0 | 0 | 0 |
| Primary cut off by a Partition, then healed | 3 | 30 | 30 | 24 | 30 |
| Primary cut off by a Partition, then healed | 5 | 30 | 30 | 11 | 30 |
| Primary crashes, then returns | 3 | 30 | 30 | 7 | 30 |
| Primary crashes, then returns | 5 | 30 | 30 | 11 | 30 |
| Random crashes, restarts and Partitions | 3 | 30 | 28 | 29 | 28 |
| Random crashes, restarts and Partitions | 5 | 30 | 30 | 30 | 30 |

### Split brain (scenario `isolate-leader`, 3 Members, seed 1)
- At t=300 node 1, the primary, is cut off from nodes 2 and 3.
- At t=355, about 5 ticks later, node 2 stops hearing node 1 and makes itself primary. **Nodes 1 and 2 are both primary.**
- Clients beside node 1 keep writing to node 1 and are acknowledged. Clients beside nodes 2 and 3 write to node 2 and are acknowledged.
- After the Partition heals, node 2 hears node 1 again and steps down. Everything node 2 acknowledged is gone from node 1's view.
- Verdicts: History not Linearizable; 6 differences between Members' final data.

### A lost Acknowledged write with no Partition (scenario `crash-leader`, 3 Members, seed 1)
- t=291: node 1 acknowledges `k2 = "c2-10"`.
- t=300: node 1 crashes. Node 2 takes over.
- t=356, 386, 399, …: node 2 acknowledges `k2 = "c2-12"`, then `"c0-18"`, then `"c1-18"`, and more for 1,500 time units.
- t=1800: node 1 returns, still believing it's primary, with the data it had at t=300. The others defer to it because it has the lowest number.
- t=1825: a client reads `k2` and gets **`"c2-10"`**. Every write acknowledged in between is lost.
- Verdict: History not Linearizable.

Here the Members' final data happened to match (0 differences), so the End-state comparison alone would have passed this run. Only the History caught it.

### Reproduce
```
go test -run 'TestNaive' -v ./internal/rungtest/
KV_EVIDENCE=harness/out/rung-1 go test -run TestNaiveIsExposed ./internal/rungtest/   # HTML timelines
```
`TestNaiveIsExposed` pins the seeds above. Once Raft replaces the naive core, the same scenarios and seeds must pass.

### Harness lesson so far
The first version of the simulated clients could reach every Member regardless of the Partition. With the primary cut off, all 30 runs still showed two primaries and diverged data, but every History was Linearizable: no client ever spoke to the second primary. Giving each client a home Member, so it sees the network as that Member does, made the lost writes visible (30 of 30). A Fault that clients don't experience can hide the failure it causes.

## Fix: Raft (P1.6–P1.9)

`internal/raft` replaces the naive core: Terms, one vote per Term, votes only for a candidate whose Log is at least as up to date, replication to a Majority before anything is acknowledged, and commit only through an Entry of the Leader's own Term. Reads go through the Log, and clients don't retry (A§6.2, A§6.3).

### Same scenarios, same harness
2,000 seeds per row, 16,000 runs in all. A run fails on any of: History not Linearizable, Members diverged, two Leaders in one Term.

| Scenario | Members | Failed | Recovery after repair: median / p99 / max | Per run: answered / rejected / lost |
|---|---|---|---|---|
| No Faults | 3 | 0 | 9 / 47 / 81 | 529 / 25 / 0 |
| No Faults | 5 | 0 | 9 / 56 / 75 | 516 / 24 / 0 |
| Leader cut off, then healed | 3 | 0 | 9 / 51 / 69 | 407 / 148 / 3 |
| Leader cut off, then healed | 5 | 0 | 9 / 52 / 80 | 424 / 115 / 2 |
| Leader crashes, then returns | 3 | 0 | 9 / 47 / 70 | 492 / 60 / 1 |
| Leader crashes, then returns | 5 | 0 | 9 / 50 / 80 | 487 / 55 / 1 |
| Random crashes, restarts, Partitions | 3 | 0 | 68 / 264 / 338 | 271 / 304 / 5 |
| Random crashes, restarts, Partitions | 5 | 0 | 49 / 255 / 371 | 303 / 247 / 5 |

- Times are in Simulation units (a tick is 10; an election timeout is 100–200). "Recovery" is from the moment every Fault is repaired to the first write answered OK.
- The four seeds pinned against the naive store all pass (`TestRaftPassesTheNaiveSeeds`).
- "Rejected" is the store saying no: not the Leader, or no Leader with a Majority. "Lost" is a request with no definite answer, almost always one pending on a Leader that then stepped down. Safety costs availability: under random Faults about half of all requests are refused.

### What broke on the way: a slow election, found by the liveness check
The first full run kept every safety verdict in all 1,600 runs, but in 3 of them no write succeeded in the 500 units after the Faults were repaired. The harness only noticed because it also requires the Group to work again; a store that refuses everything is trivially "safe".

- **Symptom (seed 56, 5 Members, random Faults):** after repair it took 5 Terms and about 550 units to elect a Leader.
- **Cause:** a Member reset its election timer whenever it saw a higher Term, even when it then refused the vote. Three of the five Members had Logs too far behind to win. Each time one of them stood, it pushed back the timers of the two Members that could win.
- **Fix:** a follower's timer is reset only by a granted vote or by word from the Leader (`becomeFollower`). Regression test: `TestRefusedVoteDoesNotResetElectionTimer`.
- **After:** 16,000 runs, every one recovers, worst case 371 units.

A stale Member can still force a new Term each time it times out, which interrupts a healthy Leader once the network heals. Raft's pre-vote extension removes that. It isn't needed for Rung 1's guarantees, so it's noted here and left out.

### Reproduce
```
go test ./internal/raft/                          # unit tests, including Figure 8
go test -run TestRaft ./internal/rungtest/        # 200 seeds per scenario; -short for 20
```
