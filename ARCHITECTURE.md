# distributed-kv-store — Architecture

Requirements are in [README.md](./README.md) and domain terms in [CONTEXT.md](./CONTEXT.md). This document records how the store is built and why. Sections are cited as A§n.

## 1. Overview
Each **Node** is a single-threaded consensus core wrapped in a shell. The core is a pure step function: an event goes in, and messages, disk writes and committed **Entries** come out. It owns no threads, clocks, sockets or files. The shell supplies those, and comes in two forms: a real one (TCP, files, the wall clock, an HTTP API) and a **Simulation** (seeded, in one process). The same core runs under both, so every **Fault** in the failure model can be injected and replayed from a seed.

## 2. Tech research
Each decision lists the alternatives considered. The user chose every option marked **Chosen**.

### 2.1 Consensus algorithm
| Option | For | Against |
|---|---|---|
| **Raft** — Chosen | One reference covers election, replication, **Membership change** and **Snapshots**. Designed to be understood. Widely discussed. | The most-trodden path. |
| Multi-Paxos | The theoretical root. | Leaves the Leader, Log and membership design open. |
| Viewstamped Replication | Used by TigerBeetle; recovery protocols suit corrupted disks. | Less material, less recognised. |

### 2.2 Language
| Option | For | Against |
|---|---|---|
| **Go 1.27** — Chosen | Known from payment-ledger, fast progress, strong standard library. | The scheduler is nondeterministic, so the core must stay single-threaded (§4.2). |
| Rust | Determinism easier to enforce; simulation libraries exist. | A new language on top of a hard problem. |
| Zig | Deterministic by construction. | Smallest ecosystem, still changing. |

### 2.3 Shape of the core
| Option | For | Against |
|---|---|---|
| **Pure step function** — Chosen | Trivially simulated. A Fault is just a different order of events. | All I/O lives in a separate shell; unfamiliar at first. |
| Goroutines behind interfaces | Idiomatic, quicker to start. | Scheduling stays uncontrolled, so a seed doesn't reproduce a run. |

### 2.4 Linearizability checker
| Option | For | Against |
|---|---|---|
| **Porcupine** — Chosen | A Go library (MIT), so it runs inside tests and Simulations. Checks per key and draws failing **Histories**. | Go only. Checking cost grows quickly with History length. |
| Jepsen / Knossos | Language-independent. | Needs a JVM and a separate step. |

### 2.5 Storage on disk
| Option | For | Against |
|---|---|---|
| **Hand-written Log and Snapshot files** — Chosen | Rungs 3 and 4 are about exactly this. Faults are easy to inject into a format we own. | More code. |
| Embedded storage engine | Less code. | Hides the durability and corruption lessons; hard to simulate; a large dependency. |

### 2.6 Transport
| Option | For | Against |
|---|---|---|
| **TCP between Nodes, HTTP/JSON for clients** — Chosen | Standard library only. One seam for consensus messages, plus a client API usable with curl. | Two protocols. |
| TCP with one format everywhere | A single protocol. | No easy client. |
| gRPC + protobuf | Familiar, generated clients. | Two dependencies; its internal goroutines and retries sit outside the Simulation. |

### 2.7 Linearizable reads
| Option | For | Against |
|---|---|---|
| **Through the Log, then confirm with a Majority** — Chosen | Rung 1 uses the obviously correct version. Rung 2 adds Raft's read index and measures the gain. Neither depends on clocks. | Two read paths over the project's life. |
| **Leader lease** — Chosen as a measured variant, off by default | Fastest: the Leader answers from memory. | A **Stale read** is possible under clock skew, which is in the failure model. See Decision Log. |

### 2.8 Real runs
| Option | For | Against |
|---|---|---|
| **Local processes and Docker Compose, measured on both** — Chosen | Local runs give undistorted numbers; Compose (with toxiproxy) gives real network Faults. The difference between them is itself reported. | Twice the run time and heat. Heavy runs need the user's approval (README). |
| Local only / Docker only | Half the runs. | Loses either the real-network Faults or the clean numbers. |

### 2.9 Expiry time source
| Option | For | Against |
|---|---|---|
| **Leader stamps Entries** — Chosen | Members judge **Expiry** against the latest stamp in the Log, never their own clock, so all Members agree. | A Leader with a wrong clock expires keys early or late (identically everywhere). |
| Leader writes explicit expire Entries | Reads never look at time. | More Entries; a key can outlive its deadline. |

### 2.10 Simulator scope
| Option | For | Against |
|---|---|---|
| **Minimal seeded simulator here** — Chosen | Enough to run every Rung's Faults. The general harness is the next project's subject. | Some overlap with that project. |
| Only the seams | Least work now. | Rungs 2–5 Faults would be hard to demonstrate. |
| Full simulator | Complete. | Duplicates the next project; delays Rung 1. |

### 2.11 In-memory state
| Option | For | Against |
|---|---|---|
| **Hand-written copy-on-write tree** — Chosen | A Snapshot is an old root pointer, so the core never pauses. Keys stay ordered for range scans. | Harder to write correctly; more memory churn. |
| Own structure, pause to snapshot | Simplest. | The core stalls during a Snapshot. |
| Existing B-tree library | Least code. | A dependency; hides the structure. |

### 2.12 Encoding
| Option | For | Against |
|---|---|---|
| **Hand-written binary on disk, standard-library encoding on the network** — Chosen | Care goes where Rung 4 needs it. Less code for messages. | Two encodings. |
| Hand-written binary for both | One format. | More code per message type. |

## 3. Chosen stack
| Concern | Choice |
|---|---|
| Language | Go 1.27, standard library |
| Consensus | Raft, written by hand |
| Storage | Own segment files for the Log, own Snapshot files |
| Node transport | TCP, standard-library encoding |
| Client API | HTTP/JSON (`net/http`) |
| Verification | Porcupine, End-state comparison, client-side signals |
| Real runs | Local processes; Docker Compose with toxiproxy |

**Dependencies:**
- `github.com/anishathalye/porcupine` (MIT): linearizability checking. Test and harness code only; the store itself imports nothing outside the standard library.
- Docker images for real Fault runs: toxiproxy (as used in payment-ledger). No Go dependency.

Any other dependency needs the tradeoff discussion and an entry here first.

## 4. Structure

### 4.1 Layers
```
            client (HTTP/JSON)                 simulated clients
                   │                                  │
        ┌──────────▼──────────┐           ┌───────────▼───────────┐
        │     real shell      │           │      Simulation       │
        │ TCP · files · clock │           │ seeded queue of events│
        └──────────┬──────────┘           └───────────┬───────────┘
                   │      events in / outputs out     │
                   └───────────────┬──────────────────┘
                        ┌──────────▼──────────┐
                        │    consensus core   │   one per Member
                        │   (pure, no I/O)    │
                        └──────────┬──────────┘
                                   │ Committed Entries
                        ┌──────────▼──────────┐
                        │    state machine    │   copy-on-write tree,
                        │   (pure, no I/O)    │   Sessions, Expiry
                        └─────────────────────┘
```

### 4.2 The core's contract
- **Events in:** a tick, a message from another Member, a client proposal, a read request, and (from Rung 3) "here is a Snapshot of the state machine".
- **Outputs:** Entries and vote state to make durable, messages to send, Committed Entries to apply, read requests now safe to answer, and a Snapshot to install or send.
- **Order rule:** the shell must make an output's writes durable before it does anything else the output asks for: sending its messages, applying its Committed Entries, answering its clients. A vote or an Entry that isn't on disk must never be acted on by anyone. Because of this rule the core needs no "now durable" event: once a step returns, the core may treat what it asked to store as stored.
- **Time:** the core counts ticks. It never reads a clock. Election and heartbeat timeouts are tick counts.
- **Randomness:** election jitter comes from a seeded source passed in at start.
- **Determinism rules:** no goroutines, no `time.Now`, no reliance on map iteration order, no global state. Given the same events in the same order, a core produces the same outputs.

The state machine obeys the same rules, so every Member that applies the same Entries holds the same state.

### 4.3 Shells
- **Real shell:** one goroutine drives the core and feeds it events from TCP readers, a ticker and the HTTP handlers. Goroutines exist only here. Each pass takes every event that is ready, steps the core through all of them, makes everything they asked to store durable with **one** sync, and only then acts on their outputs. Under load many requests share a sync.
- **Simulation:** holds every Member's core in one process and delivers events from a queue ordered by a seeded scheduler. It owns a fake clock, an in-memory network and a fake disk (§8.1).

## 5. Data model

### 5.1 Durable, per Member
| Item | Content |
|---|---|
| Vote state | Current **Term** and the Member voted for in it |
| **Log** | **Entries**: index, Term, kind, payload. From Rung 5 a command's payload carries the Leader's time stamp |
| **Snapshot** | Last included index and Term, and the state machine's full contents: keys and **Sessions**. From Rung 6, also the Member list as of that index, once it has ever changed |

Entry kinds: no-op, command, and from Rung 6 Membership change, whose payload is the Group's new Member list. Rungs 7–8 add more (§10).

The commit index is not stored. A restarted Member knows only that its Snapshot is Committed, and learns the rest again from the Leader. The shell rebuilds the state machine from the Snapshot plus the Entries the core hands over again as they are confirmed.

### 5.2 Commands
| Command | Effect |
|---|---|
| Put, Delete | Set or remove one key, optionally with a time-to-live |
| Compare-and-set | Set a key only if its current version matches |
| Transaction | A list of conditions and writes over several keys, applied all or nothing (Rung 5). A condition is a key and the version it must have, with 0 for a key that must not exist. A write is a put or a delete. If any condition fails nothing is written, and the answer names each failed condition with the version found. Every key a Transaction puts gets the same version, its Entry's index |
| Time | Carries a stamp and changes nothing else (§6.7) |
| Open Session | Registers a **Session** |

A get is also a command in Rung 1. From Rung 2 on, gets and range scans are reads that bypass the Log (§6.2).

### 5.3 State machine
- **Keys:** a copy-on-write ordered tree from key to value, version and Expiry stamp. A version increases on every write to the key.
- **Sessions:** Session id → the last request number applied and its response, plus the stamp of its last use.
- **Log time:** the highest Leader stamp applied so far. Expiry and Session cleanup compare against this and nothing else.

### 5.4 Disk format
A Member's data directory (`internal/storage`):

| File | Content | How it changes |
|---|---|---|
| `state.a`, `state.b` | Term and vote, 16 bytes, twice | Each replaced whole, one after the other: written to a temporary file, synced, renamed |
| `snapshot` | Index, Term, then the state machine's data. If it carries a Member list, the Term's top bit is set and the list sits between the two: a 4-byte count and 8 bytes per Member | Replaced whole, the same way |
| `log/<first>.seg` | Log segments, named by their first Entry's index. Each is a run of records | Appended to; a new segment starts every 4 MB |
| `damaged` | Empty. Present while the Member is **Recovering** (§6.8) | Created when damage is found, removed when the core says it has recovered |

Everything carries a CRC-32C checksum. The whole files end with one over their contents. A Log record is:

| Part | Size | |
|---|---|---|
| Length | 4 bytes | Size of the body |
| Checksum | 4 bytes | Of the length |
| Checksum | 4 bytes | Of the body |
| Body | | One Entry |
| End mark | 1 byte | `0xA5` |

- **A change is durable when the files and their directories have been synced.** Several changes can share one sync (§4.3).
- **Trimming** deletes the segments a Snapshot has made unnecessary. **Replacing a conflicting tail** deletes later segments first, then cuts the one holding the conflict, so a crash part way never leaves a gap.
- **A record that was never completely written is dropped on opening.** The file ends inside it, or zeros sit where its end should be with nothing after. Only the last write before a crash can look like this. It was never synced, so nothing in it was acknowledged.
- **A record that was complete and no longer matches its checksums is damage** (§6.8). The length has its own checksum because a damaged length would otherwise make a record seem to run past the end of the file, and pass for one never completely written. The end mark is non-zero because a file can grow before its data arrives, leaving zeros that a crash makes permanent.
- **Two copies of the Term and vote**, because a Member that loses them can't safely take part again (Decision Log). Either copy is enough, and a restart rewrites one that is damaged or behind. The newer copy is the one with the higher Term, or with a vote where the other has none.

## 6. Paths

### 6.1 Write
1. A client sends a command to any Member over HTTP, with its Session id and request number.
2. A Member that isn't the **Leader** answers `not_leader` with a hint.
3. The Leader stamps the command with its clock reading and appends it. The stamp is put in by the shell of whichever Member receives the request, as part of the payload: the core never sees time, and only a Leader's proposal reaches the Log.
4. The Leader and followers make it durable; once a **Majority** has, it is **Committed**.
5. Each Member applies it in Log order. The Leader answers the client only then, which makes it an **Acknowledged write**.

### 6.2 Read
Read paths, in the order they appear:
1. **Through the Log** (Rung 1): a read is an Entry like any other. Correct and slow.
2. **From the Leader's memory** (Rung 2, naive): the tempting shortcut, shipped to show the Stale read it allows. It doesn't survive the Rung.
3. **Read index** (Rung 2, then the default): the Leader notes its commit index, confirms with a Majority that it still leads, waits until that index is applied, then answers from memory. A new Leader must first commit an Entry of its own Term.
4. **Lease** (variant, off by default): the Leader answers from memory, without asking anyone, while its lease holds. A follower that hears from a Leader promises not to vote for anyone else for an election timeout, and so does a Member that has just started. The Leader's lease runs from when it sent the latest message a Majority has answered, for two ticks less than an election timeout. When the lease has run out the read falls back to read index. Leases are counted in each Member's own ticks, so a Leader whose ticks run slower than its followers' keeps answering after they have elected someone else: not Linearizable under clock skew.

### 6.3 Retries
In Rung 1 clients never retry: a request with no definite answer is recorded as outcome unknown, which the checker allows for. Rung 2 lets clients retry, shows a compare-and-set applying twice, and adds Sessions. From then on every command carries a Session id and a request number. Before applying, the state machine checks the Session: a request number already applied returns its saved response and changes nothing. Because this happens when an Entry is applied, it is identical on every Member and survives a change of Leader.

**Cleanup (Rung 5).** A Session records Log time (§6.7) whenever it is opened or used, and is removed once Log time has moved a set span past that (an hour by default, and the same on every Member). Sessions are checked each time Log time enters a new quarter of that span. A request in a removed Session is answered `session_expired` and changes nothing, so a late retry can't take effect a second time; the client opens a new Session and treats its last request's outcome as unknown. An idle Group keeps its Sessions until the next stamped Entry.

### 6.4 Snapshot and catch-up
- **Taking one.** After every so many applied Entries, the shell captures the state machine by keeping its tree roots, which is instant. The real shell encodes the capture on another goroutine while the core carries on. It then hands the result to the core, which stores it and drops its Log up to that Entry. Each Member does this for itself.
- **Sending one.** A Leader whose follower needs Entries it has dropped sends its Snapshot instead, at most once per election timeout, with ordinary heartbeats in between. The Log after the Snapshot follows.
- **Installing one.** A follower ignores a Snapshot of what it has already Committed. Otherwise it replaces its state machine's contents with it. What it does with its Log depends on whether it already holds the Snapshot's last Entry:
  - **It does:** its Log agrees with the Leader's up to there, and it keeps everything after. It may have acknowledged those Entries, and the Leader may be counting on them. A Leader can send a Snapshot to a follower that is nearly up to date, because its record of the follower lags behind acknowledgements still in flight.
  - **It doesn't:** its Log is behind or has diverged, and it is dropped.

### 6.5 Membership change
One Member is added or removed per change, as a Log Entry that carries the whole new Member list. A Member uses a new list as soon as the Entry is in its Log, Committed or not, and goes back to the list before it if the Entry is replaced. Two rules guard it:
- Only one change may be uncommitted at a time.
- A Leader may not append a change until it has Committed an Entry from its own Term. Without this, changes that straddle Terms can produce two Majorities (a published flaw in the original single-change scheme).

**Adding.** A Node to be added runs first as a **Spare**: it knows the Group's Member list, isn't in it, and so never stands for election. Asked to add it, the Leader makes it a **Learner**: it is sent the Log (or the Snapshot) like a follower and counts toward nothing. When it holds everything Committed, the Leader appends the change. If it hasn't caught up within 20 election timeouts the Leader gives up, and nothing has changed.

**Calling an addition off.** Asking the Leader for the list the Group already has, while a Learner is still catching up, ends the wait at once: nothing is in the Log yet, so there is nothing to undo. Rung 7c needs it when the Node being added is itself given up for dead (§11.11).

**Removing.** The Leader appends the change and stops sending to the removed Member at once. A Leader may remove itself: it leads until the change is Committed, without counting itself toward the Majority, and then steps down.

**A removed Member may never hear that it was removed**, and will then stand for election for ever. So a Member ignores a request for its vote from a Node that isn't in its list, and doesn't take that Node's Term either.

**Where the list is kept.** Each Member remembers the list at its Snapshot (or the one the Group started with) and one per change still in its Log. A restarted Member reads them from its disk, and its start-up flags only name the starting list.

**Addresses.** Every Node is started with the address of every Node that may ever join, Spares included. A change names a Node by id only.

### 6.6 Unsafe recovery
An operator command, run on each surviving Member while it is stopped, rewrites its Member list to the survivors: `kvctl unsafe-recover -data <dir> -members <ids>`. It is never automatic, and without `-confirm` it only says what it would do.

- **What it writes.** A file beside the Log holding the forced list and the index of the last Entry the Member held. The Log is not touched. When the Member starts, that list overrides the Snapshot's and every Membership change up to that index. A forced list that can't be read stops the Member starting.
- **What happens next.** The survivors elect a Leader among themselves by the usual rule, so the one holding the most wins. Its first act is to append the forced list as a Membership change, so every survivor, and every Member added later, ends with the same list in its Log.
- **What it reports.** The last index and Term the Member holds, the Member list it replaces if the disk says, the Members discarded, and that every write Committed after that index is lost unless another survivor holds it. It can't say which writes those were: the Members that knew are gone.
- **It also clears the mark that keeps a damaged Member out of elections** (§6.8), and says so. This is the way out for a Group that has stopped because a Majority is Recovering.
- **The discarded Members must never be started again with their old data.** They still hold the old list, and enough of them would elect a Leader of their own.

### 6.7 Expiry
- **Log time** is the highest stamp applied so far. A stamp lower than Log time leaves it where it is, so time never goes back when a Leader's clock does.
- A key written with a time-to-live stores a deadline: Log time, once its own Entry's stamp has been counted, plus the time-to-live. It stops existing when Log time reaches the deadline.
- **The sweep** runs as part of applying any Entry that moves Log time, and removes every key that is due. Keys are also held in deadline order, so the sweep looks only at those. Because it happens at the same Entry on every Member, a read on any Member at the same applied index gives the same answer, and a read never has to check a deadline.
- **A Group nobody is writing to** would never see a new stamp. So on each tick a Leader's shell checks whether a deadline has passed by its own clock, and if so proposes a time Entry: a command that carries a stamp and does nothing else. One is outstanding at a time.
- **A Leader whose clock is behind Log time can't move it.** Keys then live past their deadline, identically on every Member, until its clock catches up or a Leader with a later clock takes over (§2.9).

### 6.8 Damage and recovery
When a Member starts and its storage finds damage, the storage first leaves a durable mark, then removes what it can't verify:
- **A damaged Log record:** that record and everything after it.
- **A damaged Snapshot:** the Snapshot and the whole Log, which means nothing without it.
- **Both copies of the Term and vote damaged:** nothing is removed, and the Member doesn't start.

The Member that starts is **Recovering**. It may have acknowledged Entries it no longer holds, so:
- **It stays out of elections.** It doesn't stand, and it grants no votes. An Entry is Committed because a Majority holds it. If a Member that has lost it could still vote, a candidate without the Entry could reach a Majority.
- **It tells the Leader.** Every reply it sends is flagged, and the Leader lowers its record of what that follower holds to what the follower now says. What was already Committed stays Committed.
- **It is repaired like any follower that is behind:** by the Log, or by Snapshot if the Leader has trimmed past it.
- **It resumes** once an Append from a Leader of its Term or later leaves its Log matching the Leader's whole Log. A Leader holds every Committed Entry, so now this Member does too. The core tells the storage, which removes the mark.

The mark survives restarts, so a Member that restarts while Recovering is still Recovering.

**The cost.** A Group needs a Majority of Members fit to vote. If most are damaged at once, nobody can be elected, and the Group stops until an operator intervenes (§6.6). It stops with its safety intact.

## 7. API contract

### 7.1 Client (HTTP/JSON)
| Method and path | Purpose |
|---|---|
| `POST /v1/sessions` | Open a Session; returns its id |
| `GET /v1/kv/{key}` | Read a key: value and version |
| `PUT /v1/kv/{key}` | Write a key; optional `ttl` in milliseconds; optional `if_version` for compare-and-set |
| `DELETE /v1/kv/{key}` | Delete a key; optional `if_version` |
| `GET /v1/kv?start=&end=&limit=` | Range scan in key order: keys from `start` up to but not including `end`. An empty `end` means to the last key. At most `limit` keys, and never more than 1,000. Answers `{"items": [{key, value, version}]}` |
| `POST /v1/txn` | Transaction: `{"if": [{key, version}], "writes": [{op: "put" or "delete", key, value, ttl}]}`. A refusal is `version_mismatch` with `failed`: the conditions that didn't hold and the versions found |
| `GET /v1/status` | This Member's role, Term, Leader hint, commit index |

Requests carry `Session-Id` and `Request-Seq` headers so that a retry takes effect once (§6.3). A request without them is applied every time it arrives.

### 7.2 Errors
| HTTP | `reason` | Meaning | Client should |
|---|---|---|---|
| 409 | `version_mismatch` | Compare-and-set or a transaction condition failed | Treat as a definite answer |
| 404 | `not_found` | Key doesn't exist (or has expired) | Definite answer |
| 421 | `not_leader` | This Member isn't the Leader; includes a hint | Retry on the hint, same request number |
| 503 | `no_majority` | This Member knows of no Leader backed by a Majority: it is cut off, or an election is under way | Retry later, same request number |
| 504 | `timeout` | Outcome unknown | Retry, same request number |
| 410 | `session_expired` | The Session was cleaned up | Open a new Session; the outcome of the last request is unknown |
| 400 | `invalid` | Malformed request, or a Membership change that isn't one: the Node is already a Member, or isn't one, or is the last | Fix the request |
| 409 | `change_in_progress` | Another Membership change is under way, or the Leader has only just been elected. Nothing changed | Retry later |
| 421 | `wrong_group` | From Rung 7: no Group reachable from this Node owns the key's Slot by its own Log. Includes the table version the Node holds | Refresh the table, retry, same request number |
| 503 | `moving` | From Rung 7: the key's Slot is frozen for a Move (§11.4). Nothing changed | Retry shortly, same request number |
| 400 | `cross_group` | From Rung 7: a Transaction's keys are owned by more than one Group (§11.8). This attempt changed nothing | Use keys in one Group, or wait for Rung 8. If an earlier attempt of the same request ended unknown, the outcome is still unknown: a Move may have split the keys since |
| 503 | `member_unreachable` | The Node to be added didn't catch up with the Log. Nothing changed | Check the Node is running as a Spare, then retry |
| 500 | `internal` | A bug in the store | Report it; the outcome is unknown |

New reasons are added here first.

### 7.3 Admin
| Method and path | Purpose |
|---|---|
| `POST /v1/admin/members` | Add a Member: `{"id": 4}`. The Node must be running as a Spare. Answers once the change is Committed, with the new Member list |
| `DELETE /v1/admin/members/{id}` | Remove a Member. Answers once the change is Committed, with the new Member list |

`GET /v1/status` includes `members`: the list as that Node has it. `kvctl -nodes <urls> status|add <id>|remove <id>` wraps these, finds the Leader, and retries while the answer is `change_in_progress`.

A Node is started as a Spare by leaving it out of `kvnode -members`, which names the Members the Group begins with.

Unsafe recovery is a command-line action on a stopped Member, not an API call.

In a store with several Groups (§11) the admin calls are instead:

| Method and path | Purpose |
|---|---|
| `GET /v1/table` | The Slot table: for each Slot its Group, Epoch and, if a Move is under way, where it is going |
| `POST /v1/admin/moves` | Ask for a Move: `{"slot": 3, "to": 2}`. Answers once the intent is Committed; the Move then proceeds by itself, and `GET /v1/table` shows when it is done |

`kvctl table` and `kvctl move <slot> <group>` wrap these. Membership changes within a Group of such a store are not offered yet (stage 7c).

### 7.4 Debug (harness only)
| Method and path | Purpose |
|---|---|
| `GET /v1/debug/items` | This Member's own data, as applied so far: every key with its value and version. Not Linearizable, and not for clients. The harness uses it for the End-state comparison (§8.2). |

## 8. Verification

### 8.1 Simulation
A seed determines everything: the order events are delivered, which Faults fire and when, and each client's actions.

| Component | Can do |
|---|---|
| Clock | Advance per Member at different rates; jump forwards or backwards |
| Network | Drop, delay, reorder and duplicate messages; **Partition** any set of Members, in one direction or both |
| Disk | Stall a write; lose writes not yet made durable at a crash; tear a write; flip bits (Rung 4) |
| Process | Crash and restart a Member at any event |

A failing run prints its seed, and rerunning the seed reproduces it.

### 8.2 The three verdicts
1. **Linearizability:** clients record a History, and Porcupine checks it against a model of the store. Single-key operations are checked per key. Scans and transactions span keys, so a History containing either is checked against a whole-store model. With the Simulation's three keys that costs little; it grows quickly with the number of keys, so such Histories must stay small.
2. **End state:** after Faults stop and the Group settles, every Member's tree is identical.
3. **Client signals:** counts of each error reason, requests that never got a definite answer, and the time from losing a Leader to the next successful write.

### 8.3 Real runs
The same client and History recorder run against real processes: locally, and in Docker Compose with toxiproxy for Partitions and delay. Each Rung's relative numbers are measured on both, and the gap between them is reported.

## 9. Rung mechanics
| Rung | What ships first, to be seen failing | Fix | Section |
|---|---|---|---|
| 1 | Naive primary-backup: acknowledges before a Majority, fails over on a timeout | Raft election and replication; reads through the Log; clients don't retry | §4, §6.1, §6.2 |
| 2 | Leader answers reads from memory; clients retry with no Sessions | Read index; Sessions | §6.2, §6.3 |
| 3 | Nothing durable; Log never trimmed | Durable Log and vote, Snapshots, catch-up | §5.4, §6.4 |
| 4 | Records without checksums | Checksums; a damaged Member repairs from the others | §5.4 |
| 5 | Expiry by each Member's own clock | Leader-stamped Log time; scans; transactions | §6.7, §5.2 |
| 6 | Member list swapped in one step | One-at-a-time changes with both guards; Unsafe recovery | §6.5, §6.6 |
| 7–9 | See §10 | Sketches, confirmed at the Rung | §10 |

The lease variant is measured in Rung 5, once clock skew can be injected, and its Stale read is demonstrated on purpose.

## 10. Later Rungs: sketches
These are hypotheses chosen by the user, to be confirmed or replaced when the Rung starts and recorded in the Decision Log then. Rungs 1–6 must not rule them out.

| Rung | Sketch |
|---|---|
| 7 | Confirmed and replaced by §11. |
| 8 | Two-phase commit where prepare, decision and commit are Entries in the Logs of the Groups involved, so a new Leader reads the decision and nothing stays undecided. |
| 9 | The **Leaderless variant** first settles conflicts by last-write-wins, to expose Acknowledged writes disappearing, then by version vectors that keep both values. A convergence check replaces the linearizability check. |

What earlier Rungs must leave room for: Entry kinds are extensible (§5.1); a Node can host more than one core; the state machine can refuse a key it doesn't own; the client API can carry a routing-table version.

## 11. Rung 7: several Groups

Confirmed with the user at P7.0. It replaces the Rung 7 row of §10. Details marked *to settle* are decided in the task that builds them.

### 11.1 Stages
Rung 7 is built in three stages. Each ships its naive version first, is measured, gets a section in `retros/rung-7.md`, and needs the user's approval before the next.

| Stage | Adds | Shipped first, to be seen failing |
|---|---|---|
| 7a | Slots, the slot table, routing, slot moves started by command, store-wide Sessions and time, merged scans | A Group that doesn't check it owns a key; a table flipped with nobody confirming |
| 7b | Gossip: which Nodes exist, where, and which seem dead; the whole table and Store time ride on it | Ownership decided by gossip; Membership decided by gossip |
| 7c | Automatic replacement of dead Nodes; automatic rebalancing by load | A Node replaced on one observer's word |

### 11.2 Slots and the meta Group
- A key hashes (FNV-1a, 64-bit) to one of a fixed number of **Slots**: 64 by default, set when the store is created and never changed. The Simulation uses 8.
- One Group, the **Meta Group**, holds the **Slot table**: for each Slot, the Group that owns it, its **Epoch**, and whether a move is under way. It also holds the list of Groups, the Session registry (§11.6) and store time (§11.7).
- The table's version is the index of the Meta Group Entry that last changed it. Every answer to a client carries the version the answering Node holds.
- The Meta Group stores no keys and is on the path of no read or write. If it has no Majority, data Groups keep serving what they own; moves, new Sessions and store time stop.

### 11.3 Routing
- A client may ask any Node. A Node that hosts the owning Group's Leader handles the request. Otherwise it forwards it to the Node it believes does, and passes the answer back. The Node it is forwarded to handles it or refuses: a request is never passed on a second time. If the first Node was only guessing, and is told the Leader is elsewhere and that nothing happened, it tries that Node, once.
- The owning Group decides for itself whether it owns the Slot, from its own Log (§11.4), never from the forwarder's or the client's table. A Group that doesn't own it answers `wrong_group` with its table version.
- Answers carry the table version the Node holds, in a `Table-Version` header. `GET /v1/table` gives the table itself, so a client can work out the owner and go direct.

**In the real shell** a Node is one process hosting a replica of each of its Groups, with one address for other Nodes and one for clients. A replica's id among its Group's Members gives both its Node and its Group, so every Group's messages share the Nodes' connections. Requests between Nodes use `POST /v1/internal/group/{g}`. `kvnode -data-groups N` runs a Node of such a store; every Node is given the same number of Groups, Members per Group and Slots.

### 11.4 Moving a Slot
The Meta Group decides a move; the two data Groups carry it out and each records its part in its own Log. From Group A to Group B:

| Step | Who commits it | After it |
|---|---|---|
| 1. Intent | Meta Group: "Slot s moves A → B, Epoch e+1" | Nothing has changed for clients |
| 2. Copy | B commits what A sends as it arrives, held apart as incoming | A still owns and serves s. A sends the Slot as it stood at one Entry, while taking new writes |
| 3. Freeze | A: "s is frozen at Epoch e" | A answers `moving` for s: neither reads nor writes. A sends what changed since the copy, and its Log time |
| 4. Accept | B: "s is mine at Epoch e+1", with the final data | B owns and serves s |
| 5. Done | Meta Group: table says B, Epoch e+1 | The table version moves; routing follows |
| 6. Drop | A: removes the Slot's keys | |

- **Exactly one owner.** A serves s only before its freeze Entry. B serves s only after its accept Entry. B can build its accept only from A's frozen data. So there is no moment when both serve, whatever the Meta Group, the forwarders or the clients believe.
- **The pause** is from A's freeze to B's accept: one request between Nodes and one commit in B, however big the Slot. The agent sends the final part the moment the freeze is Committed.
- **Nothing times a Move out.** If the Group a Slot is going to is lost for good, the Slot stays frozen. Stage 7c, which replaces dead Nodes, is where that is dealt with.
- **A step can arrive late**, after its Move has finished and the Slot has since come and gone again. So a Group keeps, for a Slot it no longer has, the Epoch at which it last had it, and takes the Slot back only at a higher one.
- **Who carries it out.** On each Node an agent looks, for every Group whose Leader is there, at that Group's own state and the table, and proposes the next step. It keeps no state that matters. If the Leader that made the copy is replaced, the new one can't know what the target holds, and sends the whole Slot with the accept.
- **A crash at any step** leaves that step's Entry in a Log. A new Leader of A, B or the Meta Group reads its own Log and the intent, and carries on. Messages between Groups go Leader to Leader and are repeated until answered; every step may be applied twice without harm.
- **To make a Slot cheap to send**, a Group's tree is keyed by Slot and then key, so a Slot is one contiguous range (*to settle*: this changes key order inside a Group, and scans then merge across Slots as they do across Groups).

### 11.5 Versions
A key's version is the Slot's Epoch and the index of the Entry that wrote it, in the Log of the Group that owned the Slot then, packed into one number with the Epoch in the high 16 bits. It never repeats and always increases, across moves. Rungs 1–6 are the case Epoch 0.

A Transaction gives every key it writes one version, and its keys may be in Slots with different Epochs. So each Group keeps, per Slot, a version Epoch that is at least the Slot's Epoch: a Transaction uses the highest among its Slots and raises the others to it, and a Move carries it and adds one.

### 11.6 Sessions
- A client opens a Session with the Meta Group, once, and gets an id that means the same in every Group.
- A data Group must be told of a Session before it trusts it. A request can ask the Group to start a record of its Session at that request. The record then has a floor, the request's number, and the Group refuses anything below it. A client asks this only while no earlier attempt of the request can have taken effect, and never on a retry after an unknown outcome. That way a Session a Group once cleaned up and is told of again can't be made to apply an old request a second time. It relies on an attempt being delivered at most once: only the client repeats a request.
- **Moving.** Each Session record notes the Slot of its last request. When a Slot moves, the Sessions whose last request was in it go too. If the target already knows the Session it keeps the higher request number.
- A retry that reaches a Group which doesn't know the Session is answered `session_expired`, and the client treats the outcome as unknown, as now.

### 11.7 Store time
- The Meta Group's Leader commits a time Entry every so often, from its clock. That is **Store time**: the highest such stamp.
- Store time spreads to every Node (directly in 7a, by gossip from 7b). A data Group's Leader stamps commands with the latest Store time it has heard, never with its own clock. A Group's Log time is still the highest stamp in its own Log, so its Members still agree exactly.
- Groups differ only by how old their news is. A move carries deadlines unchanged, and the target first advances its Log time to at least the source's, so a key expires at the same Store time wherever it is.
- If the Meta Group stops, time stops, and nothing expires anywhere.

### 11.8 Scans and Transactions
- A range scan asks every Group and merges the answers. Each Group's part is Linearizable. The whole is not one moment: a key that moves during the scan can be seen twice or not at all (*to settle*: whether to detect a table change during the scan and retry).
- A Transaction whose keys are not all owned by one Group is refused with `cross_group`. Whether two keys share a Group can change when a Slot moves. Rung 8 removes the limit.

### 11.9 Nodes and Groups
- A Node hosts one core per Group it is a Member of, each with its own data directory. One event loop, one transport and one client API per Node; messages name their Group.
- **Placement** is one rule every Node works out for itself from the number of Nodes and of Members per Group. A Group gets Nodes to itself while there are enough: the Meta Group the first few, Group 1 the next, and so on. A Group there is no room for has its replicas on consecutive Nodes starting after Node g, overlapping others.
- Topology for runs: 5 Nodes, each Group on 3 of them, so a Node failure hits some Groups and not others. Twelve Nodes, with every Group on its own three, was tried once in stage 7a and works.
- On one machine every replica syncs to the same disk, so more Groups means more syncs queued behind each other (`retros/rung-7.md`). Several Groups pay off only where Nodes have disks of their own.
- `kvnode -shared-sync` lets the replicas in one Node share each flush of the drive: each hands its file's data to the drive, and one flush then covers them all. It is off by default, and helps only where a flush covers the whole drive, as on macOS.

### 11.10 Gossip (7b)
Details confirmed with the user at P7b.0.

- **Gossip informs and never decides.** Group membership and Slot ownership change only through Entries in a Raft Log. What a Node learns by gossip it uses to route requests and, in 7c, to report to the Meta Group.
- **What each Node holds and passes on:**
  - every Node it has heard of: its two addresses, whether it seems alive, suspected or dead, and a number that settles which report about it is newer;
  - the whole Slot table, with its version;
  - Store time.
- **Where the table comes from.** A Node that hosts a Meta Group replica puts that replica's applied table into its gossip. Every other Node takes the highest version it hears. No Node asks the Meta Group for the table on the way to routing a request any more. The table is still only a hint: each Group serves by its own Log (§11.3).
- **A pure core.** Gossip is a step function like the consensus core: a tick or a message in, messages out, randomness from a seeded source. The same code runs in the Simulation and the real shell.
- **Two detectors, built in turn and measured against each other:**
  1. **Counters.** Every Node bumps a counter each round and gossips everyone's counters. A Node whose counter hasn't risen for a set number of rounds is suspected, then dead.
  2. **SWIM.** Each round a Node pings one other. With no answer it asks a few others to ping for it. Still nothing: the Node is suspected, and dead after a wait unless it answers the suspicion by raising its own number.
  
  They are compared on time to notice a dead Node, false alarms (under a slow Node, a link cut one way, a Partition, lost messages) and messages sent. Stage 7c picks its wait from those numbers.
- **Joining and leaving.** A new Node is started with one address of any Node. It exchanges everything with that Node and is then known to all. It hosts no Group: it is a Spare until 7c gives it something. A Node that is shut down says so, and is marked as having left without being suspected first. This removes Rung 6's limit that every address is fixed at start: the network between Nodes takes addresses as gossip brings them.
- **What stays as in 7a.** The Groups' Members are still the founding Nodes, placed by the rule of §11.9. Changing them is 7c.
- **As built.** A round is one tick in the Simulation and ten in the real shell, where gossip rides on the Nodes' HTTP network. Each round a Node tells two others everything it knows, and now and then one it thinks dead, which is how the two sides of a healed Partition find each other. What others say about a Node tells that Node where it had got to: it catches up with its own counter after a restart, and raises its incarnation number if it is thought suspect, dead or gone. `kvnode -join id=host:port -founders N` starts a Spare; `GET /v1/nodes` and `kvctl nodes` show what a Node's gossip thinks.
- **Measured** (`retros/rung-7.md`): neither detector ever took a running Node for dead. Counters default to 10 rounds before suspicion and 25 before the verdict; at 5 they suspect healthy Nodes. SWIM sends twice the Messages and suspects often when Messages are lost.
- **Left for 7c.** Addresses learned by gossip are used for requests between Nodes. The Groups' replication still uses the addresses given at start.

### 11.11 Automation (7c)
Details confirmed with the user at P7c.0.

- **Who holds what.** The Slot table gains each Group's Member list and, for each Slot, the Store time of its last Move. A change of Members follows the pattern of a Move (§11.4): the Meta Group records the change it wants, the Group's Leader carries it out through its own Log (§6.5), and the Meta Group records it done. The placement rule of §11.9 only founds the store. A Node hosts a replica when the table says it should, and the Groups' replication takes addresses from gossip.
- **Declaring a Node dead.** Each Meta Member reports, by gossip, the Nodes its detector has held dead for longer than a wait. The Meta Leader acts only when a Majority of Meta Members report the same Node, so a minority side of a Partition can replace nobody. The wait is one figure for every size of Group, sized by measurement to outlast a Node restart. A two-speed rule was considered and dropped: with three Members every loss leaves a Group one failure from stopping, so it would always take the fast path.
- **Replacing it.** For each Group the Node was in, the Meta Group asks that Group's Leader to add a Spare and then remove the dead Member, one change at a time. One Spare takes over all of the dead Node's replicas. With no Spare the Group stays short. The Meta Group replaces its own dead Members by the same rule.
- **A replaced Node that returns.** Gossip only prompts it to ask. It drops a Group's data once that Group's Leader shows a Committed Member list without it, and is a Spare when it is a Member of nothing. A Node that can't reach the Group keeps its data and waits.
- **Load.** Each data Leader counts requests per Slot it owns: a write counts 64 and a read counts 1. That is the ratio measured on real processes, where one Group answered 1,371 writes a second or 90,712 reads, rounded to a power of two. The count is smoothed over a window and rides on gossip. The Meta Leader decides from what it has heard; only the Move goes into a Log. A new Meta Leader has heard nothing and waits a window before it moves anything.
- **Threshold.** Two lines. Rebalancing starts when the busiest Group carries more than the high line times the mean, and goes on, one Slot at a time from the busiest Group to the idlest, until it is under the low line. The lines start at 1.3 and 1.1 and are settled by measurement. A Slot is moved only if it is smaller than the gap between the two Groups.
- **Damping.** One Move at a time across the store. A Slot that has moved rests for a set time. Decisions use the smoothed load. Nothing is rebalanced while a Node is suspected or a replacement is under way. The first two are read from the Slot table, so a change of Meta Leader forgets neither.
- **A stuck Move** is never cancelled. A Group that has lost a Member and kept its Majority carries on with the Move while the Member is replaced. A Group that has lost its Majority can do neither: it can't change its own Members, so replacement can't bring it back. The Move then waits until enough Members return or an operator runs Unsafe recovery (§6.6), blocks rebalancing meanwhile, and shows in `kvctl table`. As first written at P7c.0 this said replacement would bring such a Group back; that was wrong, and is to be confirmed with the user.
- **Naive versions, built first:** one Meta Member's word replaces a Node with no wait; a returned Node drops its data on gossip's say-so; rebalancing acts on the latest report with one line and no damping.
- **As built (replacing).**
  - Each Group's row in the table carries the index, in the Group's own Log, of the Entry that set its Member list. A Group's Leader acts on a wish only when the table has caught up with the Group's list, and the Meta Group ignores a report older than the one it holds. Every replica must agree on that index, so a Snapshot now carries it.
  - If the Node being added is given up for dead before it is a Member, the next Spare takes over the wish, and the Group's Leader calls the first addition off (§6.5).
  - A Node drops a replica only when the Group's Leader, having confirmed it still leads, shows a Committed list without it that is no older than the list the replica holds. It keeps its Term and vote, so that if it is ever added to the same Group again it can't vote twice in one Term.
  - A replica that joined a Group, or was dropped, is marked so on its disk and never starts out believing it is a Member, whatever table its Node holds.
  - A Node that restarts holds the table the store was founded with until gossip tells it more. The Simulation now models this; it is what exposed the naive store.
  - A Group whose Majority is on the far side of a Partition can't be changed until the network heals. A change still wanted for it then is carried out, even if the Node to be removed is back.

### 11.12 Verification
- **Linearizability** is checked per key over the whole store, as before: a History doesn't care which Group answered.
- **One owner.** The Simulation samples every Group's own view and fails the run if two Groups would serve the same Slot at the same instant.
- **End state.** Members of each Group are identical, and across Groups every key is held exactly once, by the Group the table names.
- **Client signals** add: requests answered `wrong_group` or `moving`, forwarded requests, and the longest pause in writes to a moving Slot.

### 11.13 Risks accepted
- The Meta Group is one more thing to run, and moves, new Sessions and expiry all wait on it.
- A merged scan is weaker than a scan in Rungs 5–6.
- Automatic replacement and load balancing can cause churn. They can't break the one-owner rule, because they only ever ask for changes that go through Raft.

## Decision Log

### The consensus core is a pure step function
Go's scheduler is nondeterministic, and the next project needs runs that repeat exactly from a seed. So the core and the state machine take events and return outputs, with no goroutines, clocks, sockets or files inside. All I/O lives in a shell. This is less idiomatic than a goroutine per peer, and that's deliberate: don't "simplify" it by letting the core start a goroutine or read the time.

### Rung 1 ships a naive primary-backup store first
The first replication code acknowledges a write before a Majority has it and fails over on a timeout. It's wrong on purpose, so the harness can show split brain and a lost Acknowledged write before Raft replaces it. Don't fix it before the Rung 1 retro records the failure.

### No checksums on disk until Rung 4
Rung 3's file format has no checksums, so Rung 4 can show a corrupted record being replayed as valid. Adding them early would hide the failure the Rung exists to expose.

### Read index by default; the lease is an exempt variant
A Leader lease is the fastest way to serve reads, but with clock skew in the failure model it can return a Stale read. The default therefore confirms leadership with a Majority, which no clock can break. The lease stays behind a switch, measured for speed, with its Stale read demonstrated under skew. README names it as an exemption from the consistency promise.

### Time enters only through the Leader's stamp
Expiry and Session cleanup compare against the highest stamp in the applied Log, never a Member's own clock. A Leader with a bad clock makes keys expire early or late, but identically on every Member. Letting each Member consult its clock would be simpler and would make replicas diverge under skew. Rung 5 showed it is worse than that: a Member that restarts applies its Log again at a later time and reaches a different state, with every clock correct. What a Member holds must depend on its Entries and nothing else.

### One Member at a time, with the current-Term rule
Membership changes add or remove a single Member rather than using Raft's joint consensus. It's simpler, and the README only asks for one-at-a-time changes. The original single-change scheme has a published flaw when changes straddle Terms, so a Leader must commit an Entry of its own Term before appending a change.

### Hand-written storage and tree
The Log, Snapshot files and the copy-on-write tree are written by hand rather than taken from a library. They are what Rungs 3–5 are about, and owning the format is what makes disk Faults injectable. The cost is more code to get right.

### The shell stores before it acts, and the core is never told
The original contract had an event telling the core "these writes are now durable", so it could carry on while the disk worked. Rung 3 dropped it. The rule is simpler: a shell makes an output's writes durable before it does anything else that output asks for. A core can then treat whatever it asked to store as stored the moment a step returns, and there is no state in which a vote or an Entry exists in memory but not on disk and something has already been said about it. The cost is that a Member does nothing else while its disk is busy. Group commit recovers most of that under load: the real shell syncs once for every event that was ready. Don't reintroduce the event to make the core "asynchronous" without measuring what it would buy.

### A damaged Member abstains until repaired
A Member that finds damage on its disk drops what it can't verify and fetches it again. The tempting version lets it carry on as a full Member meanwhile. That loses Committed Entries: the Member acknowledged an Entry, no longer has it, and votes for a candidate that never did. In simulation that version produced Histories that weren't Linearizable in up to 81 of 300 runs. So a damaged Member stays out of elections until its Log matches a Leader's (§6.8). The cost is availability: with most Members damaged at once the Group stops, where the tempting version would have kept going and been wrong. Staying down until an operator replaces the Member was the other safe choice, and was rejected because it turns every flipped bit into a lost Member.

### A Member that can't say how it voted doesn't start
The first design let a Member that had lost its Term and vote adopt the Leader's Term and carry on. That is unsafe. The Member may have voted in a Term higher than the Leader has seen, for a candidate that was cut off and kept standing. When the Group reaches that Term, the Member would vote in it a second time, and two Leaders could be elected. Nothing the Member can learn from others tells it what it promised. So the Term and vote are stored twice, either copy is enough, and a Member with both copies damaged stays down until it can be replaced under a new identity (Rung 6).

### The Meta Group decides a move and the data Groups confirm it
A Slot's owner could be decided by the Meta Group alone, or agreed between the two data Groups with no Meta Group. The first leaves a gap in which the table says one Group and another is still serving; the second has no single order of moves and is two-phase commit arriving a Rung early. So the Meta Group commits the intent, the source commits that it has frozen, the target commits that it has taken over, and the Meta Group commits that it is done. Each Group serves a Slot by its own Log and nothing else. It costs an extra Group and more steps per move.

### Versions are a Slot's Epoch and a Log index
A version was the index of the Entry that wrote the key. With several Groups an index means nothing outside its Group, and a key that moves could get a lower version than it had, or one it had before for a different value, letting a stale compare-and-set succeed. A per-key counter would be simpler and repeats after a delete. The Meta Group already orders moves, so each move raises the Slot's Epoch and a version is the pair, Epoch first.

### One Store time, issued by the Meta Group
Rung 5 let each Group's Leader stamp Entries from its own clock. With several Groups a moved key's deadline would be judged against a different Leader's clock. Asking the Meta Group for the time on every write would put it in every write's path. So the Meta Group ticks time into its own Log, the value spreads, and data Leaders stamp with the latest they have heard. This narrows "Time enters only through the Leader's stamp": it now enters only through the Meta Group's Leader. If the Meta Group stops, nothing expires.

### Sessions are store-wide and follow the Slot of their last request
A Session per Group would be simplest and would forget a retry that lands on a new owner after a move. A single Session table in the Meta Group would put it in every write's path. So ids come from the Meta Group, each data Group keeps its own record of the Sessions it has served, and a record moves with the Slot its last request touched. A client sends one request at a time, so that is the only request a retry can be for.
