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
| `POST /v1/txn` | Transaction: `{"if": [{key, version}], "writes": [{op: "put" or "delete", key, value, ttl}]}`. From Rung 8 it also takes an optional `age` (§12.8). A refusal is `version_mismatch` with `failed`: the conditions that didn't hold and the versions found |
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
| 400 | `cross_group` | Rung 7 only (§12 removes it): a Transaction's keys are owned by more than one Group (§11.8). This attempt changed nothing | Use keys in one Group, or wait for Rung 8. If an earlier attempt of the same request ended unknown, the outcome is still unknown: a Move may have split the keys since |
| 503 | `locked` | From Rung 8: a key is Locked by a Transaction that is not yet decided (§12.3). Nothing changed | Retry shortly, same request number |
| 409 | `txn_conflict` | From Rung 8: a Transaction across Groups was aborted for an older Transaction, a Move, or a table that had changed (§12.5, §12.7). This attempt changed nothing. Carries `age` | Send it again as a new request, with that `age` |
| 503 | `member_unreachable` | The Node to be added didn't catch up with the Log. Nothing changed | Check the Node is running as a Spare, then retry |
| 400 | `not_offered` | From Rung 9, the Leaderless variant only: compare-and-set, Transactions and Sessions don't exist there (§13.3) | Use the consistent store |
| 503 | `too_few_nodes` | From Rung 9, the Leaderless variant only: fewer than W Nodes took a write, or fewer than R answered a read, in time (§13.4). A write may be on some of them | Send it again: adding or removing an element twice changes nothing |
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
| `GET /v1/table` | The Slot table: for each Slot its Group, Epoch, load as this Node has heard it and, if a Move is under way, where it is going; for each Group the Nodes its Members are on, its share of the load and, if a Node is being replaced, which and by which |
| `POST /v1/admin/moves` | Ask for a Move: `{"slot": 3, "to": 2}`. Answers once the intent is Committed; the Move then proceeds by itself, and `GET /v1/table` shows when it is done |

`kvctl table` and `kvctl move <slot> <group>` wrap these. The Members of a Group of such a store are changed by the store itself (§11.11): there is no call to add or remove one.

Between Nodes, `GET /v1/internal/members/{g}` asks a Node's replica of Group g who the Group's Members are. Only a Leader that has just confirmed it still leads answers with a list.

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
| 8 | Confirmed and replaced by §12. |
| 9 | Confirmed and replaced by §13. |

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

A Transaction gives every key it writes one version, and its keys may be in Slots with different Epochs. So each Group keeps, per Slot, a version Epoch that is at least the Slot's Epoch: a Transaction uses the highest among its Slots and raises the others to it, and a Move carries it and adds one. From Rung 8 a per-Slot offset is added to the index (§12.6).

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
- A Transaction whose keys are not all owned by one Group is refused with `cross_group`. Whether two keys share a Group can change when a Slot moves. Rung 8 removes the limit (§12).

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
- **Threshold.** Two lines. Rebalancing starts when the busiest Group carries more than the high line times the mean, and goes on, one Slot at a time from the busiest Group to the idlest, until it is under the low line. The lines are 1.5 and 1.2. Measured against 1.3 and 1.1 they end as even and make a third fewer Moves (`retros/rung-7.md`). The Slot moved is the one nearest half the gap between the two Groups, which would leave them level. A Slot over three quarters of the gap is left where it is: moving it would only move the problem, and could be undone by the next Move. (The first rule built was no more than half the gap. A real run showed it leaving two busy Slots together when moving one would have levelled three Groups.)
- **Damping.**
  - One Move at a time across the store, and none for 500 units of Store time after one finishes, until the load figures have caught up with it.
  - A Slot that has moved rests for 1,000 units.
  - Decisions use the smoothed load: each window of 100 units counts for a quarter. A new Meta Leader waits 500 units before it decides anything.
  - Nothing is rebalanced while a Member of any Group seems not to be alive, or a replacement is under way, or the store is close to idle.
  
  The Move under way and the time each Slot last moved are read from the Slot table, so a change of Meta Leader forgets neither.
- **A stuck Move** is never cancelled. A Group that has lost a Member and kept its Majority carries on with the Move while the Member is replaced. A Group that has lost its Majority can do neither: it can't change its own Members, so replacement can't bring it back. The Move then waits until enough Members return or an operator runs Unsafe recovery (§6.6), blocks rebalancing meanwhile, and shows in `kvctl table`. As first written at P7c.0 this said replacement would bring such a Group back; that was wrong. The user confirmed "never cancel" with the correction, when approving stage 7c.
- **Naive versions, built first:** one Meta Member's word replaces a Node with no wait; a Node keeps exactly the replicas the table it holds names it for; rebalancing acts on the latest report with one line and no damping.
- **As built (replacing).**
  - Each Group's row in the table carries the index, in the Group's own Log, of the Entry that set its Member list. A Group's Leader acts on a wish only when the table has caught up with the Group's list, and the Meta Group ignores a report older than the one it holds. Every replica must agree on that index, so a Snapshot now carries it.
  - If the Node being added is given up for dead before it is a Member, the next Spare takes over the wish, and the Group's Leader calls the first addition off (§6.5).
  - A Node drops a replica only when the Group's Leader, having confirmed it still leads, shows a Committed list without it that is no older than the list the replica holds. It keeps its Term and vote, so that if it is ever added to the same Group again it can't vote twice in one Term.
  - A replica that joined a Group, or was dropped, is marked so on its disk and never starts out believing it is a Member, whatever table its Node holds.
  - A Node that restarts holds the table the store was founded with until gossip tells it more. The Simulation now models this; it is what exposed the naive store.
  - A Group whose Majority is on the far side of a Partition can't be changed until the network heals. A change still wanted for it then is carried out, even if the Node to be removed is back.
  - A Node that says it is leaving counts as gone, like one given up for dead: after the wait it is replaced.
- **As built (the real shell).**
  - `kvnode -auto` (on by default) turns replacement and rebalancing on, and `-dead-wait` sets the wait (default 5 s). Gossip gives a Node up 2.5 s after it was last heard from, so a Node is replaced about 7.5 s after it stops.
  - A Node keeps beside each replica's directory a mark saying whether the replica joined its Group late or was dropped. A dropped replica isn't started again when the Node restarts.
  - Replication finds a Node that joined later by the address gossip brought (§11.10).
  - The Node whose replica answers a request counts it. Load is folded every half second; a Move is followed by 2.5 s with no other, and a moved Slot rests 5 s: the Simulation's waits in the same proportion to the window.
  - The decisions themselves are `internal/automation`, a pure package the Simulation and the real shell both call.

- **Measured** (`retros/rung-7.md`): 3,200 simulated runs with replacement and rebalancing on, none unsafe and none with a replica dropped while its Group counted on it. One busy Group goes from 2.7 times the mean load to 1.13 in about two Moves; without damping the store makes about ninety Moves a run and answers a third fewer requests. On real processes a killed Node was judged dead after 5 s and replaced with no request lost. Evening out load there lowered throughput from 885 to 729 requests a second, because every Group's Log shares one disk (§11.9).

### 11.12 Verification
- **Linearizability** is checked per key over the whole store, as before: a History doesn't care which Group answered.
- **One owner.** The Simulation samples every Group's own view and fails the run if two Groups would serve the same Slot at the same instant.
- **End state.** Members of each Group are identical, and across Groups every key is held exactly once, by the Group the table names.
- **Client signals** add: requests answered `wrong_group` or `moving`, forwarded requests, and the longest pause in writes to a moving Slot.

### 11.13 Risks accepted
- The Meta Group is one more thing to run, and moves, new Sessions and expiry all wait on it.
- A merged scan is weaker than a scan in Rungs 5–6.
- Automatic replacement and load balancing can cause churn. They can't break the one-owner rule, because they only ever ask for changes that go through Raft.
- A Group that has lost its Majority is beyond automation: it can't change its own Members. A Move it is part of waits, and blocks rebalancing, until Members return or an operator runs Unsafe recovery.
- On one machine, moving load onto more Groups makes the store slower, not faster.

## 12. Rung 8: Transactions across Groups

Confirmed with the user at P8.0. It replaces the Rung 8 row of §10. Details marked *to settle* are decided in the task that builds them. A Transaction is still one request (§5.2): conditions and writes, with no reads in between, so a key is only kept waiting between a prepare and the decision.

### 12.1 Stages
Two stages. Each ships its naive versions first, is measured, gets a part in `retros/rung-8.md`, and needs the user's approval before the next.

| Stage | Adds | Shipped first, to be seen failing |
|---|---|---|
| 8a | The steps of §12.2 as Entries, **Locks**, asking for a decision, carrying on after a crash, one version per Transaction | A decision kept only in memory; a Locked key that can still be read; a version with no offset |
| 8b | Older Transaction wins, Moves that abort undecided Transactions, forgetting records, the real shell, the numbers | Transactions that wait for each other with no order |

Until 8b, a Transaction that meets another's Lock is refused at once, a Move waits until its Slot has no Lock, and records are never forgotten. All three are safe and slow.

### 12.2 The steps
A Transaction's id is its Session id and request number. Its **Coordinator Group** is the Group that owns the Slot of its lowest key. A Transaction whose keys are all in one Group is one Entry, as in Rung 5, and takes none of these steps.

1. **Begin.** The Node that receives the request forwards it to the Coordinator Group's Leader, which commits a begin Entry: the id, the age (§12.5), the conditions, the writes, and the Group it believes owns each key. The begin Entry is also the Coordinator Group's own prepare.
2. **Prepare.** The Coordinator Group's Leader sends each other Group its part. That Group commits a prepare Entry. It answers yes only if it owns every Slot named by its own Log, none is frozen or about to move, every condition holds and no key is Locked by another Transaction. A yes Locks every key named, in a condition or a write, records the writes, and carries the version the Group would have given. A no says why. A no for a failed condition names the versions found and Locks the keys all the same, until the finish: otherwise what two Groups report about their conditions would be true at two different moments, and the answer to the client would describe a store that never existed. Any other no holds nothing.
3. **Decision.** When every Group has answered, or one has said no, the Coordinator Group commits a decision Entry: commit only if all said yes. It carries the Transaction's version (§12.6). The same Entry applies the Coordinator Group's own writes and releases its own Locks. The client is answered once this Entry is Committed.
4. **Finish.** The Leader sends the decision to each other Group, which commits a finish Entry: on commit it applies its writes with that version, and either way it releases its Locks.
5. **Confirmed.** Each Group's answer to finish is Committed in the Coordinator Group. When every Group has confirmed, the record is forgotten (8b). The answer is by then in the Session's record, for an abort as much as a commit, so a begin for the same request that arrives afterwards, late or as a retry, is answered from it and doesn't start the Transaction over.

Messages go Leader to Leader, name the Transaction and are repeated until answered. Every step may be applied twice without harm: a Group answers a step it has already taken the same way.

**A prepare that arrives after the finish.** A prepare is sent again when no answer comes, and a copy can reach a Group after it has finished the Transaction and forgotten it. A Leader that has been replaced and doesn't know it can send one much later still. Taken as new, it would Lock the keys again, and the next finish would write the Transaction's values a second time, over whatever had been written since. So a yes carries the version the Group offered, which no two prepares share, the decision records the offer it counted from each Group, and a finish commits only the prepare that made that offer. A Group holding any other lets go and writes nothing. A Group that commits also notes the Transaction in the Session's record (§11.6), and turns away a prepare numbered at or below it at once; that saves Locking keys for nothing, and nothing depends on it, because a Session's record can be removed.

**The same request under two Coordinator Groups.** A request whose first attempt was aborted can be sent again after a Move has given its lowest key to another Group, which takes it on as new. A Group that still holds the first attempt's Locks answers no to the second, and takes a finish only from the Coordinator Group whose prepare it holds.

### 12.3 Locks
- A **Lock** covers one key for one Transaction. Clients can't take one.
- A read, write, compare-and-set, scan or single-Group Transaction that touches a Locked key is not served from what the Group holds. The Group's Leader asks the Coordinator Group what was decided. If there is a decision, the Group commits its finish Entry first and then serves the request.
- If the answer is "not decided yet", or none comes, the request waits a bounded time and is then refused with `locked`. In the Simulation it waits 12 units, about two round trips, and is tried once more; a client that is refused tries again as it would after `moving`.
- The Coordinator Group answers the question as a Linearizable read. A Member that has been replaced as Leader may not have applied the begin Entry, and its "no record" would be wrong.
- "No record" from the Coordinator Group means abort, and the Lock is released. This is safe because begin is Committed before any prepare is sent and a record is forgotten only after every Group confirmed. So a prepare that arrives late, after all that, Locks its keys only until the first question.
- A Group's Leader also asks about every Lock older than a bound (30 units in the Simulation), whether or not a request is waiting, so no Lock outlives its Transaction. The answer names the offer the decision counted (§12.2).

### 12.4 Crashes
- Each step is an Entry. A new Leader of the Coordinator Group reads its table and carries on: prepares for a Transaction that has begun, finishes for one that is decided. A new Leader of another Group finds its Locks in its state and asks about them.
- The Coordinator Group may decide abort at any time before it has decided commit. Its Leader does so when not every answer has come within a bound (400 units in the Simulation, a little more than a client waits for one attempt). A Group that says yes afterwards is sent the abort.
- A client that loses its connection retries with the same request number. The Coordinator Group's Session record answers with the recorded outcome, or makes the retry wait while the Transaction is in flight. That record follows the Slot of the lowest key (§11.6), so it is still found after a Move.
- **The limit.** A Coordinator Group that has lost its Majority can decide nothing, and the other Groups keep their Locks until its Members return or an operator runs Unsafe recovery. "Nothing stays undecided" holds while every Group involved has a Majority.

### 12.5 Two Transactions and one key (8b)
- **Age.** A Transaction's age is the Store time the Node that took its first attempt had heard, then its Session id, then its request number. Every Group compares ages the same way. Store time moves in steps, so many Transactions share an age and the Session id then decides.
- **Older wins.** A prepare that finds a key Locked answers no, holds nothing, and says who holds the key and how old it is. If the holder is younger, the Leader that sent the prepare asks the holder's Coordinator Group to abort it, with an abort decision of its own that is ignored if the Transaction is already decided. Either way the Lock goes, and the prepare is sent again every 10 units until it does. A Node asks for a given Transaction to be aborted once, and not again for 100 units: each asking is an Entry in that Group's Log. For the same reason each further try for a Locked key waits twice as long as the last, up to eight times the first wait.
- **A Transaction that holds nothing waits first.** At its begin a Transaction has no key Locked anywhere, so it can't be part of a ring of Transactions waiting for each other. If its Coordinator Group's own keys are Locked by a younger one, it waits as long as a younger one would, and has the other aborted only after that. Aborting at once threw away the work of Transactions a moment from done: on real processes, six clients on the same keys took 350 ms a commit that way and 200 ms this way.
- **A Lock that has outlived its Transaction.** Before it waits, the Leader that sent the prepare asks the holder's Coordinator Group what was decided, and if the holder is over, tells the Group that still has its keys Locked (§12.3).
- If the holder is older, the prepare is sent again for 80 units, and then the Transaction aborts.
- **Two keys need none of this.** A Transaction's Coordinator Group owns its lowest key and Locks its own keys first. With two keys, each Transaction waits only for a key higher than one it holds, so no ring of waiting Transactions can close, even with no order. It takes three Groups prepared side by side.
- So no two Transactions can wait for each other: of any two, only the younger waits.
- **A Transaction that loses** is refused with `txn_conflict`, which carries its age. That attempt changed nothing. The client sends it again as a new request with that age, so it keeps getting older and must win in the end.
- The store trusts the age a client sends, except that it can't be later than the Store time the Node has heard. A client that lies about its age can push in front of others. It can't break atomicity or lose a write.
- Requests that aren't Transactions across Groups have no age. They wait and are refused as in §12.3.

### 12.6 One version per Transaction
- Every key a Transaction writes gets the same version, in every Group: the highest of the versions the Groups offered at prepare.
- A Group must never give a later write a lower version. So each Group keeps, per Slot, an offset. A version is the Slot's version Epoch and the Entry's index plus the offset (§11.5). When a Transaction's version is higher than the Group would have given, the Group raises the Slot's version Epoch to the Transaction's if that is higher, and raises the offset so that the next Entry's version is above it.
- A Move adds one to the version Epoch (§11.5), which is above anything the source gave, so the offset starts again at zero in the target.
- A version is then no longer exactly an Entry's index. It still never repeats and always increases for a key.

### 12.7 Moves
- Once the table shows an intent to move a Slot, its Group refuses new prepares for it, and a begin whose lowest key is in it.
- **8a:** the Group freezes the Slot only when no key in it is Locked. The freeze Entry itself is refused while one is, so the rule holds whoever proposes it.
- **8b:** once the Slot's copy is done, the Group asks the Coordinator Group of each Lock in the Slot to abort, in the way an older Transaction does (§12.5). A Transaction that had already committed is finished first. Then the Slot freezes. So a Move waits at most for commits already decided, and Transactions on a Slot being moved are refused with `txn_conflict` and retried.
- A Move carries no Lock and no decision record. It carries Session records, with the outcome, as before.

### 12.8 Client API
- `POST /v1/txn` takes an optional `age`. A `txn_conflict` refusal carries the `age` to send with the next attempt.
- `cross_group` is no longer returned.
- A Transaction's answer still has one version.
- `GET /v1/status` on a Node of a store with several Groups adds, per Group it hosts, `locked` (keys Locked) and `open` (Transactions it coordinates that aren't over). `kvctl table` prints them for each Group's Leader.
- A request outside any Session is still given a name by the Node that takes it, from that Node's clock and number, so that of two Transactions of the same age the one that arrived first is the older.

### 12.9 Verification
- **Linearizability** over the whole store, with Transactions whose keys are in different Groups in every run (§8.2: such Histories stay small).
- **Same outcome everywhere.** A Transaction that committed anywhere committed exactly once in every Group its begin named.
- **Nothing left.** After Faults stop, no key is Locked and no Transaction is undecided; from 8b no record is left at all.
- **Faults:** the Coordinator Group's Leader and each other Group's Leader crashed between every pair of steps, Partitions between Groups, and from 8b Moves and automation during Transactions.
- **Client signals** add: requests refused `locked` and `txn_conflict`, attempts per Transaction, and the longest time a key stayed Locked.
- **Numbers.** The Rung 7 load with no Transactions is run again and must reach 0.9× of Rung 7. Transactions across Groups are then measured on their own and their cost is stated in the retro as a known cost.
- **Measured** (`retros/rung-8.md`): on local processes with three Groups and 64 clients, the Rung 7 load answers 710 requests/s against Rung 7's 704. A Transaction across two Groups takes about 0.55 s where a single write takes 0.08 s: three writes one after the other, and two or three Linearizable reads by the Node waiting to hear the decision.

### 12.10 Risks accepted
- Two-phase commit stops when the Coordinator Group has no Majority (§12.4).
- A Transaction across two Groups costs at least five Committed writes where one in a single Group costs one: begin, decision and confirmed in the Coordinator Group, prepare and finish in the other. The client waits for three of them, and for the reads by which the Node that took its request learns the decision.
- Every wait in §12 is a fixed number, chosen for one machine under one load. Too short, and Transactions abort each other for nothing; too long, and a stuck one holds its keys.
- A client can lie about a Transaction's age (§12.5).
- A scan across Groups is still not one moment (§11.8).
- Moves made by the balancer now cost Transactions a retry.
- Under heavy contention the steps of Transactions, and of their retries, fill a Group's Log, and everything in that Group waits behind them. Nothing in the design sheds that load.

## 13. Rung 9: the Leaderless variant

Confirmed with the user at P9.0. It replaces the Rung 9 row of §10. Details marked *to settle* are decided in the task that builds them. The Leaderless variant is a second store, built beside the consistent one and sharing none of its consensus: no Leader, no Term, no Log, no Group, no Slot table. It exists to be compared with it.

### 13.1 Stages
Three stages. Each ships its naive versions first, is measured, gets a part in `retros/rung-9.md`, and needs the user's approval before the next. The user asked for two; the third holds what was added afterwards, real processes and Docker.

| Stage | Adds | Shipped first, to be seen failing |
|---|---|---|
| 9a | Home Nodes, writes to W of them with Hints, reads from R, a set as the value, the four verdicts | Last-write-wins; Hints kept and never delivered |
| 9b | The Exchange, repair on read, time-to-live, scans | No Exchange; a remove forgotten at once; reads that trust one Node |
| 9c | Real processes, Docker with the network cut, the comparison | — |

### 13.2 Shape
- Five Nodes, a fixed set. Each key has three **Home Nodes**, chosen by the key's hash on a ring of the Nodes (*to settle*: how many points each Node has on the ring).
- Any Node takes any request and carries it out with the key's Home Nodes. It need not be one of them.
- **W** and **R** are set for a run: how many Home Nodes must have a write before the client is told it succeeded, and how many must answer a read. Results are reported for W=1 R=1, W=2 R=2 and W=3 R=1.
- The core is a pure step function, as the consensus core is (§4.2), so the Simulation can run it under the same Faults.

### 13.3 What a value is, and the API
A key holds a **set of strings**. The routes are those of §7.1 and they mean something else, so this is the same routes with a different contract, not the same API.

| Request | Meaning here |
|---|---|
| `PUT /v1/kv/{key}` with `{"value": x, "ttl": …}` | Add the element `x` to the key's set |
| `DELETE /v1/kv/{key}?value=x` | Remove the element `x` |
| `GET /v1/kv/{key}` | The set, as `{"values": […]}`. An empty set is 200 with an empty list, not 404 |
| `GET /v1/kv?start=…&end=…` | Scan (9b) |
| Compare-and-set, `POST /v1/txn`, `POST /v1/sessions` | `not_offered` |

- There are no versions in answers. Nothing here has one order to number.
- Adding an element that is there, or removing one that isn't, changes nothing. So a request that got no answer can simply be sent again, and Sessions aren't needed.

### 13.4 Writes and Hints
- The Node that takes a write sends it to the key's three Home Nodes and answers the client when W of them have it on disk.
- If a Home Node can't be reached, the write is given to the next Node on the ring that isn't one, as a **Hint**: that Node keeps it for the Home Node, on disk, and counts toward W.
- If fewer than W Nodes in all have it in time, the client is told `too_few_nodes`. The write may be on some of them: the outcome is unknown, and sending it again is safe (§13.3).
- A Node holding a Hint keeps trying to deliver it to the Home Node, and drops it once that Node has it on disk.
- A write that reached W is still sent to the Home Nodes that hadn't answered, and sent again for a while if they don't.

### 13.5 Two writes at once
- **First, last-write-wins.** Each key has a counter. A write gives the key's whole set a number one above the highest the Node has seen for it, with the Node's id to break ties, and the higher number replaces the lower wherever they meet. Writes on one side of a Partition order correctly. Of two made on different sides, one side's whole set wins and the other's additions are gone, though their clients were told they succeeded.
- **Then, a set where add wins.** Each addition is marked with the Node that took it and that Node's count for the key. A key keeps, beside its elements and their marks, a record of every mark it has seen. Two copies of a key are joined by keeping an element if either has a mark for it that the other hasn't seen, or both have. A remove deletes the marks it has seen and no others, so an addition made elsewhere at the same time survives it.
- That record of marks seen is a version vector. It is what tells "never added here" from "added and removed", and it must outlive the elements (§13.7).

### 13.6 Converging (9b)
Three ways, each covering what the one before can't:
1. Writes sent on and retried (§13.4) reach a Home Node that was briefly away.
2. Hints reach one that was away while the write was made.
3. **The Exchange.** Every so often each Node picks another, and for the keys they are both Home Nodes of, each takes what the other has and it lacks. This reaches a Node that was away longer than any retry lasts, or whose Hint-holder died (*to settle*: comparing by hashes of ranges, not key by key).

**Repair on read.** A read asks R Home Nodes, joins their answers, returns that, and sends the joined set back to any that had less.

### 13.7 Removes
A Node that has applied a remove keeps the key's record of marks seen, even when the set is empty. If it dropped it, a Node that missed the remove would hand the element back in the next Exchange and it would be taken as new. In this Rung the record is never dropped: the cost is that a key once written is never wholly forgotten.

### 13.8 Time-to-live (9b)
An element added with a time-to-live carries a deadline: the wall clock of the Node that took the write, plus the time-to-live. Each Node stops showing the element once its own clock passes the deadline, and may then remove it locally. What is stored converges all the same. Under clock skew two Nodes disagree, for as long as the skew, about whether the element shows. This is Rung 5's naive store kept on purpose; the disagreement is measured.

### 13.9 Scans (9b)
A scan asks every Node for the keys in the range that it is a Home Node of, joins the answers key by key, and returns keys whose set isn't empty. It is as current as the least current Node that answered for each key.

### 13.10 Disk
A write, a Hint, and what an Exchange brings are on disk before they count or are acknowledged, as in the consistent store. A Node that restarts has what it had. The on-disk form is *to settle*; it is hand-written, as §3 requires.

### 13.11 Verification
The linearizability check doesn't apply. A run is judged on:
- **Converged.** After Faults stop and a quiet period, every Home Node of every key holds the same set, and no Hint is left.
- **No Acknowledged add lost.** Every element a client was told was added is in the final set, unless a remove of it was acknowledged that began after that add was acknowledged.
- **Nothing comes back.** An element whose remove was acknowledged, and for which no add began after that remove began, is not in the final set.
- **Availability,** a number: the share of requests answered while the Partition lasts, for this store at each W and R and for the consistent store of Rung 8 (5 Nodes, 3 Groups) under the same scenario and seeds.
- **Stale reads,** a rate: reads that missed an add acknowledged before the read began. Nothing here promises there are none.

The comparison is the Rung's result: availability during the Partition, against Acknowledged adds lost, side by side.

### 13.12 Real processes and Docker (9c)
- `kvnode` gets a mode for the Leaderless variant, with the same HTTP routes, TCP between Nodes and the disk of §13.10. `kvbench` gets its workload and its verdicts.
- The local harness can pause and kill Nodes. A real Partition needs Docker with the network cut. `deploy/docker-compose.yml` is edited only after saying what will change and getting the user's leave. The consistent store of Rung 8 has no Docker setup either, and needs one for the comparison to be made there.

### 13.13 Risks accepted
- A client must know that a value is a set. Nothing written for the consistent store works unchanged.
- A key once written is never wholly forgotten (§13.7).
- With W=1 a write acknowledged by one Node, which is then lost for good before sending it on, is gone. No Partition is needed.
- Expiry disagrees under clock skew (§13.8).
- The Node set is fixed. Adding or removing a Node moves keys between Home Nodes, and that is not built.

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

### The Coordinator Group is one of the Groups involved, and begins in its own Log
The decision of a Transaction across Groups could live in the Meta Group, or travel with the lowest key's Slot. The Meta Group commits about 1,400 writes a second here and already carries the table, Sessions and Store time; it would cap every Transaction and stop them all when it stops. A record that travels with a Slot only helps if a Group can disappear, and none can. So the Group that owns the lowest key decides, in a table of its own. Its first step is a begin Entry, one more Committed write per Transaction, so that its Leader drives the steps and a new Leader can always finish them. Without it, "no record" would have to mean abort for a Transaction whose prepares had been sent, and that abort would have to be remembered for ever.

### One version per Transaction, kept rising by a per-Slot offset
Across Groups there is no one index. Each key could take its own Group's version, which changes the answer's shape and ends "one Transaction, one version". The Meta Group could hand out a number, which puts it in every Transaction's path. So every key takes the highest version the Groups offered, and a Group whose own index is behind raises a per-Slot offset so that its later versions stay above. Raising the Slot's Epoch instead would use up its 16 bits within minutes. The cost: a version is no longer exactly an Entry's index.

### The older Transaction wins, and a retry keeps its age
A Transaction that meets a Lock could be refused at once, which lets two keep refusing each other, or all could prepare in one key order, which makes prepares one after another. So each has an age, the younger gives way, and only a younger one ever waits. Age is the Store time on the begin Entry, then the Session id: coarse, but one order every Group agrees on with no extra call. A Transaction that loses is told its age and sends it back, so it can't lose for ever. The store has to trust that number.

### The Leaderless variant's value is a set, and add wins
Version vectors, as first sketched, keep both of two concurrent values and hand them to the client to choose between. That is honest and changes the API into one where clients merge. Showing one by a fixed rule keeps the API and hides a write as surely as losing it. So the store joins concurrent writes itself, which it can only do for a value with a rule for joining: a set of strings, where an add made at the same time as a remove survives. The cost is that the routes are the same and the contract is not: a client written for the consistent store doesn't work against this one. In return adds and removes can be sent twice without harm, so Sessions aren't needed.

### W of three Home Nodes, with Hints
Every Node could hold every key and answer as soon as it has stored a write itself, which is the most available store there is. The user chose Dynamo's shape: three Home Nodes of five per key, a write acknowledged at W of them, and a Node that isn't a Home Node standing in for one that can't be reached. It is comparable with the consistent store, where a key is also on three Nodes of five, and W and R make the trade between availability and loss a setting that can be measured at several values. It costs a ring, a second path by which data moves, and results that are per setting.
