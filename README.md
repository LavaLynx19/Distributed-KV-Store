# distributed-kv-store

A replicated key-value store that keeps one consistent view of the data while **Nodes** crash, networks split and disks misbehave. It is built up one **Rung** at a time, and each Rung first exposes a specific failure, then fixes it. This is a learning project: the goal is to understand consensus deeply, not to ship a product.

Domain terms (**bold**) are defined in [CONTEXT.md](./CONTEXT.md).

## Problem Statement

Keeping several copies of data in agreement sounds simple, but it breaks as soon as a **Node** crashes or the network splits. Common failures are two **Leaders** accepting writes at once, an **Acknowledged write** that later disappears, a **Stale read** from a Node that doesn't know it was replaced, and replicas that quietly drift apart. This project builds a store that holds its guarantees while the **Faults** get worse, and it documents why each mechanism is needed.

## Goals

- Learn consensus deeply: a **Group** that elects a **Leader**, replicates a **Log** and stays safe under every **Fault** in the failure model.
- Never lose an **Acknowledged write** and never return a **Stale read**.
- Support get, put, delete, compare-and-set, time-to-live, range scans and multi-key transactions.
- Grow from one **Group**, to several Groups, to transactions across Groups.
- Be drivable by a **Simulation** from the start, so the next project (a deterministic simulation testing harness) can use this store as its target.
- Compare the consistent store against a **Leaderless variant**, to measure what availability costs.
- Produce a retro per **Rung**, with each decision traced to the Decision Log in ARCHITECTURE.md.

## Non-Goals

- Byzantine faults. A **Node** may crash, stall or have a corrupted disk, but it never lies on purpose.
- Multiple regions: no wide-area latencies and no region-aware placement.
- Redis protocol compatibility, authentication, TLS or access control.
- Production operations tooling (dashboards, backups, rolling upgrades) beyond what a **Rung** needs.
- Absolute throughput claims. Everything runs on one machine, so performance numbers are relative.

## User Stories

### Client
- As a client, I read, write and delete a key, and a read always reflects every **Acknowledged write** that finished before it.
- As a client, I write a key only if its current value is the one I expect (compare-and-set).
- As a client, I set a key to expire after a time, and it expires at the same point for every reader.
- As a client, I read all keys between two bounds in order.
- As a client, I change several keys atomically: all of them or none.
- As a client, when a request times out I retry it, and it takes effect exactly once.
- As a client on the minority side of a **Partition**, I get an error, never a wrong answer.

### Operator
- As an operator, I add, remove or replace a **Member** while the **Group** keeps serving.
- As an operator, I bring back a **Node** that was away for days, and it catches up by itself.
- As an operator, I let **Nodes** join and leave a multi-Group cluster, and keys move to follow them.
- As an operator, after a **Group** loses its **Majority** for good, I can force an **Unsafe recovery** and see what it discarded.

### Verifier
- As a verifier, I run any **Rung** under **Faults** with one command and get a verdict.
- As a verifier, I replay a failing **Simulation** from its seed and see the same failure.

## Constraints

### Consistency
- All reads and writes are **Linearizable**. A **Leader** that has been cut off must not answer a read with an old value.
- During a **Partition**, the side without a **Majority** refuses reads and writes.
- A **Term** has at most one **Leader**.
- A retried request takes effect exactly once.
- The **Leaderless variant** is exempt from these. It promises only that replicas converge after a **Partition** heals.
- A lease-based read mode is also exempt. It is off by default, exists to be measured, and can return a **Stale read** under clock skew.

### Durability
- A write is acknowledged only after it is **Committed**.
- Every **Acknowledged write** survives all **Nodes** restarting at once. Reads are served from memory; "in-memory" promises nothing more than that.
- A **Node** whose **Log** has been replaced by a **Snapshot** elsewhere catches up automatically.
- **Unsafe recovery** is the one deliberate exception. It must be an explicit operator action, and it must report what it discarded.

### Failure model
The store tolerates, and each **Rung**'s harness demonstrates:
- **Leader** and follower crashes, including a crash in the middle of a write to disk.
- **Partitions**: a minority cut off, a **Group** split in two, and one-way Partitions.
- Messages delayed, reordered and duplicated.
- A slow or stalled disk.
- Clock skew and clock jumps.
- Disk corruption: torn writes and flipped bits. Corruption is detected, never applied.

### Simulation
Time, network and disk are injectable everywhere, so a whole **Group** can run in one process under a **Simulation**, repeatable from a seed.

### Verification
Every run is judged three ways:
1. **Histories** checked for linearizability, using an existing checker (chosen in ARCHITECTURE.md).
2. End state: all **Members** of a **Group** hold identical data.
3. Client-side signals: errors, timeouts and the time to recover after a **Leader** is lost.

### Environment
- One MacBook (M4 Pro). **Groups** of 3 and 5 **Members**, with small keys and values.
- Targets are relative: the cost of consensus against a single **Node**, recovery time after losing a **Leader**, and throughput against **Group** size.
- Sustained heavy load runs need the user's approval first.

## Rung Ladder

Each **Rung** names a failure to watch happen first, then the capability that fixes it. Relative targets are set once Rung 1 gives a baseline.

| Rung | Failure to expose | Capability | Must hold afterwards |
|---|---|---|---|
| 1 | Split brain and a lost **Acknowledged write**: a naive primary-backup store acknowledges too early and fails over on a timeout | **Leader** election and **Majority** replication of the **Log**; get, put, delete, compare-and-set | At most one Leader per **Term**; no Acknowledged write lost under crashes and clean **Partitions**; **Histories** are **Linearizable** on 3 and 5 **Members** |
| 2 | **Stale read** from a cut-off Leader; a retried compare-and-set applies twice | Linearizable reads; **Sessions** | No Stale read and exactly-once effect under one-way Partitions and delayed, reordered or duplicated messages |
| 3 | Restart amnesia; a Log that never ends; a returning **Node** that can't catch up | Durable Log and vote; **Snapshots**; automatic catch-up | Every Acknowledged write survives a full restart, a crash mid-write and a stalled disk |
| 4 | A corrupted Log is replayed as valid and replicas diverge | Corruption detection and repair from other Members | Corruption is always detected, never applied; Members stay identical |
| 5 | Each Node expires keys by its own clock, so replicas disagree under clock skew | **Expiry** decided through the Log; range scans; multi-key transactions within one **Group** | Members identical under clock skew and jumps; scans and transactions are Linearizable |
| 6 | Two Majorities during a naive **Membership change**; a Majority lost for good | Membership change one Member at a time; **Unsafe recovery** | Never two Leaders during a change; a dead Member replaced under load; Unsafe recovery reports what it discarded |
| 7 | A stale routing map sends a key to a Group that no longer owns it, or two Groups serve it mid-move | Several Groups with keys split by hash; routing; moving keys; Nodes joining and leaving by gossip; range scans that ask every Group and merge | Each key is owned by exactly one Group at any moment; single-key operations stay Linearizable while keys move |
| 8 | A transaction across Groups is half-committed when its coordinator crashes | Atomic transactions across Groups, with the coordinator's decisions replicated | Atomic under coordinator and participant crashes; nothing stays undecided |
| 9 | The price of availability: the **Leaderless variant** accepts writes on both sides of a Partition and Acknowledged writes are overwritten | Leaderless variant behind the same API; a convergence check | Replicas converge after healing; reported next to the consistent store as availability during the Partition against writes lost |

Rungs 1–6 are the consensus core. Rungs 7–9 build on it. Earlier Rungs must not rule out a later one.

## Success Criteria

### Per Rung
A **Rung** is done when all four checks pass:
1. **Exposed:** the named failure was observed and recorded before the fix.
2. **Faults:** the Rung's guarantees hold under its **Faults**, with a **Linearizable** **History**, identical **Members** and the client-side signals recorded.
3. **Numbers:** the relative targets for the Rung are measured.
4. **Retro:** a written retro covers what broke, why, and what changed.

### Project
- All 9 **Rungs** pass.
- The design can be whiteboarded and every choice defended from the Decision Log.
- One command runs any Rung under its **Faults** and gives a verdict.
- A **Simulation** can drive the store from a seed, ready for the simulation-testing project.
