# Rung 3 retro: restart amnesia and an endless Log

Status: **in progress**. The failures of a store that keeps nothing on disk are recorded below (P3.1–P3.2). The fix and its results follow.

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
