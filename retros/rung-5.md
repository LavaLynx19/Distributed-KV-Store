# Rung 5 retro: time

Status: **in progress**. The failures of a store whose Members each trust their own clock are recorded below (P5.1–P5.2). The fix and its results follow.

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
