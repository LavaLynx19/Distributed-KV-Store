# distributed-kv-store

Vocabulary for a replicated key-value store that stays consistent under faults, built up across progressively harder Rungs.

## Cluster

**Node**:
One running copy of the store, with its own memory and its own disk.
_Avoid_: Server, instance, peer, replica

**Member**:
A Node that currently belongs to a Group and counts toward its Majority.
_Avoid_: Participant, voter

**Group**:
A set of Members that together hold one copy of a share of the keys and agree on every change to it.
_Avoid_: Shard, partition, cluster, replica set

**Majority**:
More than half of a Group's Members.
_Avoid_: Quorum

**Leader**:
The one Member of a Group that accepts writes during a Term.
_Avoid_: Primary, master

**Term**:
A numbered period in which a Group has at most one Leader.
_Avoid_: Epoch, generation, view

**Membership change**:
Adding, removing or replacing a Member of a Group while it keeps serving.
_Avoid_: Reconfiguration, resize

## Data

**Log**:
A Group's ordered record of every change it has agreed on.
_Avoid_: Journal, WAL, history

**Entry**:
One change in the Log.
_Avoid_: Record, command, op

**Committed**:
Said of an Entry that a Majority has stored durably, so it can never be undone.
_Avoid_: Durable, accepted, applied

**Acknowledged write**:
A write whose client was told it succeeded. It must be Committed first.
_Avoid_: Successful write, confirmed write

**Snapshot**:
A Group's full state as of one Entry, which replaces the Log up to that Entry.
_Avoid_: Checkpoint, backup, dump

**Session**:
A client's identity with the store, used to recognise a retried request so it takes effect once.
_Avoid_: Connection, client id, idempotency key

**Expiry**:
The moment a key with a time-to-live stops existing, which is the same Entry on every Member.
_Avoid_: Timeout, eviction

## Guarantees

**Linearizable**:
Every operation appears to take effect at a single instant between its request and its response, in an order all clients agree on.
_Avoid_: Strongly consistent, consistent, serializable

**Stale read**:
A read that returns a value older than an Acknowledged write that finished before the read began.
_Avoid_: Dirty read, old read

**Partition**:
A network failure in which some Nodes can't reach others. Never used for splitting keys between Groups.
_Avoid_: Split, netsplit

**Fault**:
A failure the harness injects on purpose: a crash, a Partition, a delayed or duplicated message, a slow or corrupted disk, or a skewed clock.
_Avoid_: Error, chaos, failure injection

**Recovering**:
Said of a Member that found part of what it stored damaged and hasn't yet been brought back up to date. It takes no part in elections until it has.
_Avoid_: Repairing, degraded, quarantined

**Unsafe recovery**:
An operator action that forces the surviving minority of a Group to continue after its Majority is lost for good. It can discard Acknowledged writes.
_Avoid_: Force restart, disaster recovery

**Leaderless variant**:
A second mode of the store, with no Leader, in which every Node accepts writes during a Partition and conflicts are settled afterwards.
_Avoid_: AP mode, eventual mode, Dynamo mode

## Process

**Rung**:
One step of the build, with a named failure to expose, the capability that fixes it, and guarantees that must hold afterwards.
_Avoid_: Phase, milestone, stage

**History**:
The recorded sequence of every client request and response in a run, with times, used to check whether the run was Linearizable.
_Avoid_: Trace, log, transcript

**Simulation**:
Running a whole Group in one process with time, network and disk under the test's control, so a run can be repeated exactly from a seed.
_Avoid_: Mock run, emulation
