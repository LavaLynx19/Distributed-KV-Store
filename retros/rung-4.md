# Rung 4 retro: a disk that lies

Status: **in progress**. The failures of a store that trusts its disk are recorded below (P4.1–P4.2). The fix and its results follow.

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
