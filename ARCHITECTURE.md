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
- **Events in:** a tick, a message from another Member, a client proposal, a read request, and "these writes are now durable".
- **Outputs:** Entries and vote state to make durable, messages to send, Committed Entries to apply, read requests now safe to answer, and a Snapshot to install or send.
- **Order rule:** the shell must make an output's writes durable before it sends that output's messages. A vote or an Entry that isn't on disk must never be acted on by another Member.
- **Time:** the core counts ticks. It never reads a clock. Election and heartbeat timeouts are tick counts.
- **Randomness:** election jitter comes from a seeded source passed in at start.
- **Determinism rules:** no goroutines, no `time.Now`, no reliance on map iteration order, no global state. Given the same events in the same order, a core produces the same outputs.

The state machine obeys the same rules, so every Member that applies the same Entries holds the same state.

### 4.3 Shells
- **Real shell:** one goroutine drives the core and feeds it events from TCP readers, a ticker, the disk writer and the HTTP handlers. Goroutines exist only here.
- **Simulation:** holds every Member's core in one process and delivers events from a queue ordered by a seeded scheduler. It owns a fake clock, an in-memory network and a fake disk (§8.1).

## 5. Data model

### 5.1 Durable, per Member
| Item | Content |
|---|---|
| Vote state | Current **Term** and the Member voted for in it |
| **Log** | **Entries**: index, Term, kind, Leader's time stamp, payload |
| **Snapshot** | Last included index and Term, the Member list, the state machine's full contents |

Entry kinds: no-op, command, Membership change. Rungs 7–8 add more (§10).

### 5.2 Commands
| Command | Effect |
|---|---|
| Put, Delete | Set or remove one key, optionally with a time-to-live |
| Compare-and-set | Set a key only if its current version matches |
| Transaction | A list of conditions and writes over several keys, applied all or nothing (Rung 5) |
| Open Session | Registers a **Session** |

A get is also a command in Rung 1. From Rung 2 on, gets and range scans are reads that bypass the Log (§6.2).

### 5.3 State machine
- **Keys:** a copy-on-write ordered tree from key to value, version and Expiry stamp. A version increases on every write to the key.
- **Sessions:** Session id → the last request number applied and its response, plus the stamp of its last use.
- **Log time:** the highest Leader stamp applied so far. Expiry and Session cleanup compare against this and nothing else.

### 5.4 Disk format
The Log is a sequence of segment files of length-prefixed records, with a separate small file for vote state. Snapshots are written to a temporary file and renamed into place. Records carry **no checksum until Rung 4**, which exists to expose that gap (Decision Log). The exact layout is fixed in PLAN.md's Rung 3 task.

## 6. Paths

### 6.1 Write
1. A client sends a command to any Member over HTTP, with its Session id and request number.
2. A Member that isn't the **Leader** answers `not_leader` with a hint.
3. The Leader stamps the Entry with its clock reading and appends it.
4. The Leader and followers make it durable; once a **Majority** has, it is **Committed**.
5. Each Member applies it in Log order. The Leader answers the client only then, which makes it an **Acknowledged write**.

### 6.2 Read
Read paths, in the order they appear:
1. **Through the Log** (Rung 1): a read is an Entry like any other. Correct and slow.
2. **From the Leader's memory** (Rung 2, naive): the tempting shortcut, shipped to show the Stale read it allows. It doesn't survive the Rung.
3. **Read index** (Rung 2, then the default): the Leader notes its commit index, confirms with a Majority that it still leads, waits until that index is applied, then answers from memory. A new Leader must first commit an Entry of its own Term.
4. **Lease** (variant, off by default): the Leader answers from memory while its lease holds. Not Linearizable under clock skew.

### 6.3 Retries
In Rung 1 clients never retry: a request with no definite answer is recorded as outcome unknown, which the checker allows for. Rung 2 lets clients retry, shows a compare-and-set applying twice, and adds Sessions. From then on every command carries a Session id and a request number. Before applying, the state machine checks the Session: a request number already applied returns its saved response and changes nothing. Because this happens when an Entry is applied, it is identical on every Member and survives a change of Leader.

### 6.4 Snapshot and catch-up
Taking a Snapshot keeps the tree's current root and writes it out in the shell while the core carries on. Entries up to that point are then dropped. A follower that needs dropped Entries receives the Snapshot, then the Log after it.

### 6.5 Membership change
One Member is added or removed per change, as a Log Entry. A Member uses a new Member list as soon as the Entry is in its Log. Two rules guard it:
- Only one change may be uncommitted at a time.
- A Leader may not append a change until it has Committed an Entry from its own Term. Without this, changes that straddle Terms can produce two Majorities (a published flaw in the original single-change scheme).

A new Member first catches up without counting toward the Majority.

### 6.6 Unsafe recovery
An operator command, run on a surviving Member while the Group is stopped, rewrites its Member list to the survivors. It prints the last index it holds and warns that anything Committed beyond the survivors' Logs is lost. It is never automatic.

### 6.7 Expiry
A key written with a time-to-live stores a deadline: the Entry's stamp plus the time-to-live. It stops existing when Log time passes the deadline. Reads compare against Log time, so a read on any Member at the same applied index gives the same answer. Expired keys are removed lazily and by a periodic sweep that is itself deterministic.

## 7. API contract

### 7.1 Client (HTTP/JSON)
| Method and path | Purpose |
|---|---|
| `POST /v1/sessions` | Open a Session; returns its id |
| `GET /v1/kv/{key}` | Read a key: value and version |
| `PUT /v1/kv/{key}` | Write a key; optional `ttl`; optional `if_version` for compare-and-set |
| `DELETE /v1/kv/{key}` | Delete a key; optional `if_version` |
| `GET /v1/kv?start=&end=&limit=` | Range scan in key order |
| `POST /v1/txn` | Transaction: conditions and writes |
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
| 400 | `invalid` | Malformed request | Fix the request |
| 500 | `internal` | A bug in the store | Report it; the outcome is unknown |

New reasons are added here first.

### 7.3 Admin
| Method and path | Purpose |
|---|---|
| `POST /v1/admin/members` | Add a Member |
| `DELETE /v1/admin/members/{id}` | Remove a Member |

Unsafe recovery is a command-line action on a stopped Member, not an API call.

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
1. **Linearizability:** clients record a History, and Porcupine checks it against a model of the store. Single-key operations are checked per key. Scans and transactions span keys, so their Histories are checked against a whole-store model and kept short.
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
| 7 | Keys hash to a fixed number of slots; a versioned table maps slots to **Groups**. Data moves one slot at a time. SWIM-style gossip spreads which Nodes are alive and the table's version; every change of ownership is Committed through a Raft Group, never decided by gossip. Range scans ask every Group and merge. |
| 8 | Two-phase commit where prepare, decision and commit are Entries in the Logs of the Groups involved, so a new Leader reads the decision and nothing stays undecided. |
| 9 | The **Leaderless variant** first settles conflicts by last-write-wins, to expose Acknowledged writes disappearing, then by version vectors that keep both values. A convergence check replaces the linearizability check. |

What earlier Rungs must leave room for: Entry kinds are extensible (§5.1); a Node can host more than one core; the state machine can refuse a key it doesn't own; the client API can carry a routing-table version.

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
Expiry and Session cleanup compare against the highest stamp in the applied Log, never a Member's own clock. A Leader with a bad clock makes keys expire early or late, but identically on every Member. Letting each Member consult its clock would be simpler and would make replicas diverge under skew.

### One Member at a time, with the current-Term rule
Membership changes add or remove a single Member rather than using Raft's joint consensus. It's simpler, and the README only asks for one-at-a-time changes. The original single-change scheme has a published flaw when changes straddle Terms, so a Leader must commit an Entry of its own Term before appending a change.

### Hand-written storage and tree
The Log, Snapshot files and the copy-on-write tree are written by hand rather than taken from a library. They are what Rungs 3–5 are about, and owning the format is what makes disk Faults injectable. The cost is more code to get right.
