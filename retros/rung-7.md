# Rung 7 retro: several Groups

Status: **stage 7a in progress**. Rung 7 is built in three stages (A§11.1). This file gets a part per stage.

# Stage 7a: Slots, the table, and moving a Slot

Environment: the Simulation, with 5 Nodes, three data Groups and the Meta Group, each Group on 3 of the Nodes, and 8 Slots. A Node hosts a replica of two or three Groups, so one Node crashing costs several Groups a Member. Clients ask any Node about any of 12 keys. A scenario asks the Meta Group to move a Slot to another Group every 200 units or so.

A run now gets a fourth verdict: **one owner**. At the moment a Group's Log makes it start serving a Slot, no other Group's Log may still have it serving that Slot.

## Exposed: a Group that answers for whatever it is asked (P7a.4)

The Move itself is done properly here. What is missing is the check: a Group asked about a key answers from whatever it holds, trusting the sender to have sent it to the right place.

50 seeds per cell.

| Faults | Not Linearizable |
|---|---|
| No Moves | 0 |
| Moves | 47 |
| Moves and crashes | 42 |
| Moves and Partitions | 35 |
| Moves interrupted at each step | 42 |
| Moves, crashes and Partitions | 35 |

Every Node caches the table and refreshes it every 20 units. For that long after a Move, some Node still sends the Slot's keys to the Group that used to own them. That Group has given the Slot away, and answers anyway: a read finds nothing, or a write lands where nobody will look for it.

A stale table is normal. It can't be prevented, only made harmless.

## Exposed, part two: a table flipped with nobody confirming (P7a.5)

Now every Group checks, by its own Log, that it owns what it serves. The Move is the naive one: the Meta Group changes the owner in the table at once, and the Groups follow the table. The old owner, seeing the table name someone else, sends the Slot over as it stands and lets go when the other has it.

| Faults (50 seeds) | Two Groups serving one Slot | Not Linearizable |
|---|---|---|
| No Moves | 0 | 0 |
| Moves | **50** | 11 |
| Moves and crashes | **50** | 12 |
| Moves and Partitions | **50** | 7 |
| Moves interrupted at each step | **50** | 3 |
| Moves, crashes and Partitions | **50** | 8 |

Every run with a Move has two owners. Seed 1: Slot 0 is served by Groups 1 and 3 at t=380.

The new owner starts when it is given the Slot. The old one stops when its own "let go" is Committed, a little later. In between both answer. Most runs get away with it, because the window is short and a client has to write to the old owner inside it. The ones that don't lose that write.

The History catches this in a fifth of the runs. The one-owner verdict catches it in all of them, which is why it was worth adding.

### Reproduce
```
go test -run 'TestUncheckedGroup|TestFlippedTable' -v ./internal/rungtest/
```

## Fix: the Move is confirmed by both Groups (P7a.7)

The Meta Group records the intent and nothing else. The Group that owns the Slot copies it to the other while still serving, then commits a freeze and stops. Only then does it send what changed during the copy. The other Group commits an accept and starts. The Meta Group records that it is done, and the first Group drops the Slot.

A Group serves a Slot from its accept to its freeze, by its own Log. The accept can only be built from the frozen Slot. So the two never overlap, whatever any table says.

### Same scenarios, same harness

`TestRung7a`: 6 scenarios × 200 seeds × two workloads = **2,400 runs, none unsafe, 10,401 Moves finished**, every run ending with each Slot served by exactly the Group the table names. The second workload adds time-to-lives, Transactions over two keys, range scans merged from every Group, and Sessions that are cleaned up.

### Two bugs the suite found in the fix

**A late step brought a Slot back** (`moves-and-everything`, seed 39). Slot 3 went from Group 1 to Group 3, and later back to Group 1. Then a duplicate of the first Move's accept, sent by a Leader that had lost touch, finally got through to Group 3. Group 3 no longer had the Slot, the accept carried a whole copy of it, and Group 3 took it. Two owners.

A Group that dropped a Slot forgot everything about it, including the Epoch. It now keeps the Epoch at which it last had each Slot and takes the Slot back only at a higher one.

Nothing in the History showed this. All 200 answers in that run were consistent. Only the one-owner verdict saw it, and only after I rewrote that verdict (next section).

**A later Move reused the copy progress of an earlier one** (`moves`, seed 7). The agent remembers how far it has copied a Slot. It kept that after the Move finished. The next time the same Slot left the same Group, under the same Leader, the agent thought the copy was already done, froze at once, and sent only "what changed". The target had nothing, refused, and the Slot stayed frozen for good. Safe, and stuck. The progress is now tied to the Move's Epoch, and a refused accept makes the agent send everything next time.

### A verdict that was wrong first

My first one-owner check sampled each Group's Leader every 5 units and asked whether it would serve the Slot. It reported two owners in 13 of 300 runs of the fixed store.

They were false. A Leader that has just restarted hasn't applied its Log yet. Its state machine still shows the Slot as served, although the freeze is Committed and it will apply it before answering anything. The check was reading a state the Group was no longer in.

The check now runs at the moment each Entry is applied. For each Group it keeps what the Group serves as of the furthest Entry any replica has applied, which is the Group's Committed state whoever leads it. With that, the 13 went away, and one real case remained: seed 39 above.

### A bug in the test client

Three runs in 200 weren't Linearizable with Transactions and crashes together (seed 79). The store was right. A Transaction's first attempt had timed out and then succeeded. A Move then put its two keys in different Groups, so the retry was refused as `cross_group`, and my client recorded "refused, nothing happened". A refusal on a retry says nothing about the earlier attempt. The rule is now in A§7.2.
