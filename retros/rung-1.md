# Rung 1 retro: split brain and a lost Acknowledged write

Status: **in progress**. The naive store's failures are recorded below (P1.5). The fix and its results follow.

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
