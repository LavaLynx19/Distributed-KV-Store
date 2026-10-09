# Rung 6 retro: changing who is in the Group

Status: **done**. A store that swaps its Member list in one step elects two Leaders. The store this Rung ends with changes one Member at a time under two rules, brings a new Member up to date before it counts, and can be forced back to life by an operator after losing its Majority. 5,300 simulated runs with 12,612 Membership changes are safe, and a dead Member was replaced under load on real processes with a pause of 37–76 ms.

Environment: the Simulation, now with two **Spares** beside the Group: Nodes that run from the start and belong to no Group. A run asks the Leader for Membership changes while clients carry on. At the end only the Nodes that are still Members are compared.

## Exposed: a Member list swapped in one step (P6.1)

The naive store does what the request says. Asked for a new Member list, the Leader appends an Entry carrying it, and every Member uses the new list from the moment it has the Entry.

The Group of three is asked to grow to five while the Leader and the two newcomers are cut off from the other two founders.

| Faults (100 seeds, 3 founders) | Unsafe | Not Linearizable | Two Leaders in one Term | Core's safety check tripped |
|---|---|---|---|---|
| Grow and shrink, nothing else wrong | 0 | 0 | 0 | 0 |
| Grow while the Leader and newcomers are cut off | **100** | 96 | 15 | 96 |

Every run fails.

- **Seed 11:** nodes 2 and 4 are both Leader in Term 2. Node 4 is a newcomer, elected by the three Nodes that hold the list of five. Node 2 is a founder, elected by the two founders that never heard of the change, who are a Majority of the three they know.
- **Seed 1:** both sides take writes, and the History isn't Linearizable.

The list of five needs three and the list of three needs two, and three plus two is five: the two Majorities have nobody in common. With five founders growing to seven the same scenario is safe, because the Leader and two newcomers aren't four. It takes a bigger jump to break a bigger Group.

### Reproduce
```
go test -run 'TestSwappingMembers' -v ./internal/rungtest/
```

## Fix: one Member at a time (P6.2)

If two lists differ by one Member, any Majority of one shares a Member with any Majority of the other. So a change adds or removes exactly one Member, and two more rules stop consecutive changes adding up to a bigger one:

1. **One change at a time.** A change is refused while another is uncommitted.
2. **Commit in your own Term first.** A new Leader may not change the list until an Entry of its own Term is Committed.

Around those:
- A Member goes back to the earlier list if a change is replaced in its Log before it Commits.
- A Node that isn't in its own list never stands for election.
- A Member ignores a request for its vote from a Node that isn't in its list, and doesn't take its Term. A removed Member may never hear it was removed, and would otherwise unseat Leaders for ever.
- A Leader that removes itself leads until the change is Committed, without counting itself, and then steps down.
- The list is stored with the Snapshot, so a Member whose Log no longer holds the change still knows it.

## Exposed, part two: a new Member that counts at once (P6.3)

One change at a time is safe. It isn't yet a good idea to add a Member the moment it is asked for, because the newcomer holds nothing.

A dead Spare is added to a Group of three, and then one follower is lost (`add-the-dead`, seed 1).

| | Longest pause in writes |
|---|---|
| The new Member counts from the moment it is added | about 2,100 units: the Group stops until the follower returns |
| The new Member must catch up first | under 110 units |

Three Members tolerate one loss. Adding the dead Node makes four, which need three, with one already gone. The Group has made itself weaker by growing. With five founders the same thing does no harm, since six still have four.

**Fix.** The Node to be added is first a **Learner**: the Leader sends it the Log and counts it for nothing. Only when it holds everything Committed does the Leader append the change. If it doesn't get there in 20 election timeouts the Leader gives up and says so. A dead Node is never added.

### Reproduce
```
go test -run 'TestCountingBeforeCatchingUp' -v ./internal/rungtest/
```

## Same scenarios, same harness

`TestRung6`: 26 scenarios × 3 and 5 founders × 100 seeds, plus one scenario that needs 4 founders = **5,300 runs, none unsafe, 12,612 Membership changes Committed**. That is every Fault from Rungs 1–5 with two Spares present, and seven scenarios that change the Group: growing and shrinking, during a Partition, with the Leader killed over and over, replacing a dead Member, and three directed ones described next.

### Testing the tests

I switched each rule off and ran the suite.

| Rule switched off | Caught by the first four scenarios | With a scenario written for it |
|---|---|---|
| One change at a time | **No**: 0 of 800 runs | `impatient-shrink`: **300 of 300** |
| Commit in your own Term first | **No**: 0 of 800 runs | `straddle`: **96 of 100** |
| A Node outside the Group doesn't stand | Yes: 4 of 800, the core's check trips | |
| Votes and Terms aren't taken from outside the Group | Yes: 106 of 800 end with Members that differ | |

Neither of the two main rules was tested by the scenarios I wrote first. Those ask for a change, wait for it to finish, and ask for the next, so two changes never overlap and a Leader is rarely new when asked.

- **`impatient-shrink`** cuts the Leader off with one follower and asks it, three times in ten units, to remove a Member on the other side. Each request is a single change. Without rule 1 the Leader's side whittles the list down to itself and commits alone, while the other side still has a Majority of the list it knows.
- **`straddle`** is the published flaw. With four founders, the Leader is cut off with a Spare and adds it, which can't Commit. The other three elect a Leader, which is cut off with one follower the instant it wins and asked to remove the old Leader. Without rule 2 it does, and commits with that one follower. Then the old Leader, the Spare and the founder that heard nothing of the new Term are joined, and elect the old Leader under the list of five. Two Groups now take writes.

Three more breaks, tried later:

| Break | Caught by the suite | Caught by |
|---|---|---|
| A Learner is added at once, caught up or not | No: it isn't unsafe | `TestCountingBeforeCatchingUpIsExposed`: the dead Node is added and the Group stops for 2,024 units |
| A Leader that has removed itself still counts itself | **No**: 0 of 5,300 | A unit test only (`TestLeaderRemovesItself`) |
| A change replaced in the Log stays in force | **No**: 0 of 5,300 | A unit test only (`TestReplacedChangeIsForgotten`) |

The last two are real gaps. Each needs a further failure on top of the mistake before anything goes wrong, and no scenario I have lines them up.

## Unsafe recovery (P6.5)

A Group that loses its Majority for good can never elect a Leader again. That is the price of never having two.

`kvctl unsafe-recover -data <dir> -members <ids>`, run on each stopped survivor, writes the survivors' list beside the Log. Started again, they act on that list and nothing older. They elect a Leader by the usual rule, so the survivor holding the most wins, and its first act is to append the list as a Membership change.

50 seeds per cell, 3 / 5 founders. A Majority is destroyed, chosen at random.

| | Take another write | End identical |
|---|---|---|
| Nothing done | 0 / 0 of 50 | — |
| Unsafe recovery after 400 units | 50 / 50 | 50 / 50 |
| The same, with the survivors cut off for 200 units before the loss | 50 / 50 | 50 / 50 |

**What it costs.** In the first recovery row every one of 200 Histories is still Linearizable: the survivors happened to hold everything. In the second, 72 of 100 (3 founders) and 67 of 100 (5) are not. Clients had been told writes succeeded that the survivors never received, and the recovered Group doesn't have them. Pinned: `lose-the-majority-while-behind`, 3 founders, seed 1.

**What it reports.** On three real processes with five keys written, two killed for good:
```
Unsafe recovery of the Member in harness/out/local/data3
  It holds Entries up to 6 (Term 1). The latest Term it saw is 4.
  Its Member list is the one the Group started with. It becomes [3].
  DISCARDED: every write the Group Committed after Entry 6, unless another survivor holds it.
  Run this on every survivor with the same list. The survivor holding the most will lead.
  Never start a discarded Member again with its old data: it would form a second Group.
```
Before it, a write to the survivor was answered `no_majority`. After it, the survivor led a Group of one, held all five keys, and took writes.

The report can't name the writes that were lost. The Members that knew are gone. It gives the last Entry the survivor holds, and the operator has to take it from there.

**It also lets a damaged Member vote again.** Rung 4 left a Group stopped for good when a Majority was Recovering at once, and said the way out would be this command. It clears that mark, and says it did.

### What I got wrong first
My first design appended the forced list to the survivor's Log as an ordinary Entry. That needs a Term for the Entry. Any Term I could choose offline was wrong: the Member's latest Term lets a survivor with a short Log and a high Term beat one holding more, and the last Entry's Term can make two different Entries look the same to the Log-matching check. The list is now kept outside the Log until a Leader elected in the normal way writes it in.

### Reproduce
```
go test -run 'TestUnsafeRecovery' -v ./internal/rungtest/
go test -run 'TestForceMembers' ./internal/storage/
```

## Real runs (P6.6)

M4 Pro, 10 s each, local processes, default mix.

### Cost of the Rung (3 Members, 64 clients, no Faults)
| | Requests/s | p50 |
|---|---|---|
| Rung 5 | 1,378 | 46.1 ms |
| Rung 6 | **1,387** | 45.7 ms |

### Replacing a dead Member under load (clients retrying in Sessions)
A follower is killed for good. `kvctl add` brings in the Spare and `kvctl remove` drops the dead Member, while clients keep writing.

| Founders | Clients | Requests/s | Rejected / lost | Longest pause in writes | Members at the end | Verdicts |
|---|---|---|---|---|---|---|
| 3 | 8 | 223 | 0 / 0 | 53 ms | [1 2 4] | Linearizable; identical |
| 3 | 64 | 1,374 | 0 / 0 | 37 ms | [1 3 4] | Linearizable; identical |
| 5 | 8 | 160 | 0 / 0 | 76 ms | [1 2 4 5 6] | Linearizable; identical |

Clients didn't notice. Adding the Spare took a quarter of a second with a short Log.

### Recovery after losing the Leader (3 Members, 8 clients, Leader killed and restarted)
Six runs: **178, 221, 222, 283, 334 and 413 ms**. The first run gave 413 ms, over the 400 ms target, and Rung 5's one run gave 395 ms. Five more show the spread: usually well inside, with a tail that crosses the line. The tail is an election that takes two rounds. One run per Rung was never enough to say this, and the earlier retros' single figures should be read that way.

Docker, 5 Members, Leader isolated: 1,939 requests/s, 0 rejected, 0 lost, 154 ms pause, Linearizable, identical.

**Not covered by real runs:** Membership changes in Docker. The Compose file starts no Spares, and I didn't change it. Unsafe recovery was run once by hand on real processes (above) and isn't part of `run.sh`.

### Reproduce
```
go test -run 'TestRung6|TestSwappingMembers|TestCountingBeforeCatchingUp|TestUnsafeRecovery' ./internal/rungtest/
CLIENTS=64 harness/run.sh local 3
RETRY=1 harness/run.sh local 3 replace-follower
CLIENTS=64 RETRY=1 harness/run.sh local 3 replace-follower
RETRY=1 harness/run.sh local 3 kill-leader
harness/local.sh start 3 1 && harness/local.sh members
```

## Known limits
- **Addresses are fixed at start.** Every Node is started with the address of every Node that may ever join. A change names a Node by id. Adding a Node nobody planned for means restarting the others with a longer list.
- **A removed Member that was down when it was removed never learns of it.** It stands for election for ever and is ignored. The operator should stop it. The Leader could tell it before letting go, and doesn't.
- **A Spare knows nothing about the Group**, not even who leads, so a client that asks one is told `no_majority` and must look elsewhere. So is a client that follows a stale hint to a removed Leader.

## Verdict
| Check (README → Success Criteria) | Result |
|---|---|
| Exposed | **Pass**: two Leaders in one Term and a History that isn't Linearizable (`TestSwappingMembersIsExposed`); a Group stopped by its own new Member (`TestCountingBeforeCatchingUpIsExposed`) |
| Faults: never two Leaders during a change, including changes that straddle Terms | **Pass**: 5,300 simulated runs. Both rules are shown to matter by switching them off |
| Faults: a dead Member replaced under load | **Pass**: three real runs, no request rejected or lost, pauses of 37–76 ms; and 200 simulated runs of `replace-the-dead` |
| Faults: Unsafe recovery reports what it discarded | **Pass, as far as it can**: it reports the last Entry held and the Members discarded. It can't name the lost writes |
| Numbers: at least 0.9× Rung 5 | **Pass**: 1.01× (1,387 against 1,378 requests/s) |
| Numbers: recovery within 400 ms of losing a Leader | **5 of 6 runs**: 178–334 ms, and one of 413 ms |
| Retro | This document |

## Lessons
1. **Scenarios that behave politely test nothing.** Mine asked for a change, waited, and asked for the next. Both safety rules could be deleted without a single failure in 800 runs. They exist for the operator who doesn't wait and the Leader that has just changed.
2. **Safe and sensible are different questions.** Adding a Member one at a time is safe. Adding a dead one that counts at once is still safe, and stops the Group. The Learner step is there for availability and no correctness check will ever ask for it.
3. **Some things can't be decided offline.** Unsafe recovery wanted to write an Entry, and an Entry needs a Term, and no Term chosen by a tool that can see one disk is right. The answer was to write less: a note beside the Log, and let an election decide.
4. **The command can't report what it most needs to.** "What was discarded" is known only to the Members that were lost. Saying that plainly is more useful than a number that looks like an answer.
5. **One measurement is an anecdote.** Earlier Rungs reported one run per Fault and called the recovery target met. Six runs of the same Fault here span 178 to 413 ms.
6. **The byte-compatible habit paid off again.** The Snapshot's Member list hides behind a bit that old files never set, so every pinned seed from Rung 4 still fails the same way for the same reason.
