# Rung 2 retro: Stale read and double apply

Status: **in progress**. The two shortcuts' failures are recorded below (P2.2–P2.3). The fixes and their results follow.

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
