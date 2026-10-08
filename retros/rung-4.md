# Rung 4 retro: a disk that lies

Status: **in progress**. The failures of a store that trusts its disk are recorded below (P4.1–P4.2). The fix and its results follow.

Environment: the Simulation. Until this Rung its disk was an idealised structure in memory. It is now the real storage code (`internal/storage`) running on a filesystem held in memory (`storage.MemFS`), which keeps two views of every file: what has been written, and what has been synced. A crash keeps only what was synced. Rung 4 adds two ways for it to lie:
- **Torn writes.** A crash may keep part of a write that was in progress, and part of what it keeps may be zeros, where the file had grown but the data hadn't arrived.
- **Bit flips.** One bit inverts somewhere in what a Member stored. Nothing notices until the Member reads its disk again, which is when it restarts.

Every simulated run since Rung 3 now goes through the real file format. The Rung 3 suite passes unchanged on it (5,200 runs).

## Exposed: a store that trusts its disk (P4.2)

The Rung 3 store's files have no checksums (Decision Log: "No checksums on disk until Rung 4"). It believes whatever it reads back.

200 seeds per cell, 3 / 5 Members.

| Faults | Failed | Not Linearizable | Members diverged | Core's safety check tripped | A Member can't restart |
|---|---|---|---|---|---|
| Torn writes; Members restart one at a time | 79 / 87 | 0 / 0 | 0 / 2 | 0 / 0 | 79 / 87 |
| Torn writes; blink | 64 / 106 | 1 / 0 | 0 / 0 | 0 / 0 | 63 / 106 |
| Torn writes; every Member restarts at once | 37 / 70 | 0 / 1 | 0 / 0 | 0 / 0 | 37 / 69 |
| Bit flips, one Member at a time | 122 / 105 | 23 / 4 | 58 / 53 | 38 / 27 | 72 / 66 |
| Bit flips with everything else | 65 / 60 | 15 / 9 | 31 / 30 | 14 / 13 | 26 / 29 |

One flipped bit does one of four things, depending on where it lands.

### 1. It is believed (`bit-flips`, 3 Members, seed 4)
Node 1 ends the run holding a key named **`j0`**, with a value, at version 45. No client ever wrote `j0`: they use `k0`, `k1` and `k2`. The letter `k` is 0x6B and `j` is 0x6A. One bit flipped in a stored Entry, node 1 restarted, read the Entry back, and applied a put to a key that never existed. Nodes 2 and 3 have no `j0`.

Every History in this run is Linearizable and nothing crashed. The damage sits in one Member's state, waiting for that Member to become Leader.

### 2. It reaches a client (seed 14)
The History isn't Linearizable: a client was given an answer that no order of the requests could produce.

### 3. It makes a Member contradict what is Committed (seed 3)
The core's own check trips: a follower is told to replace an Entry it knows is Committed. In other runs a flipped bit in an Entry's index produced indexes like 281,474,976,710,716, and the core stopped on those too.

### 4. It makes the disk unreadable (seed 2)
The Member can't parse what it finds and doesn't start. This is the least bad outcome, and it happens by accident: the damage happened to break the format.

**Torn writes** mostly end the fourth way (37–53% of runs with crashes): a half-written record doesn't parse. The Rung 3 store already copes with a write that was cut short cleanly. What it can't tell apart is a record that is all there but partly zeros.

### Reproduce
```
go test -run 'TestLyingDisk' -v ./internal/rungtest/
```
