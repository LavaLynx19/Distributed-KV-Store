# distributed-kv-store — project rules

This file overrides the global and workspace CLAUDE.md files wherever they conflict. Anything not covered here follows those files.

## Sources of truth
- Requirements are in `README.md`, domain terms in `CONTEXT.md`, design and the `## Decision Log` in `ARCHITECTURE.md` (A§n), and the task order and checklist in `PLAN.md`.
- For CODE work, read the current `PLAN.md` item and the A§ sections it cites. Flip the checklist marker in the same change that finishes the item.
- Name code after `CONTEXT.md` terms: `Node`, `Member`, `Group`, `Leader`, `Term`, `Majority`, `Log`, `Entry`, `Snapshot`, `Session`, `History`. If a new domain concept needs a name, add it to `CONTEXT.md` first.

## Locked stack
Go 1.27 standard library, hand-written Raft, hand-written storage, TCP between Nodes, HTTP/JSON for clients. The only dependency is `github.com/anishathalye/porcupine`, imported by `internal/check` and tests. Rationale is in A§2–3. Any other dependency needs the tradeoff discussion and an A§3 update first.

## Determinism (A§4.2)
The pure packages are `internal/core`, `internal/naive`, `internal/raft`, `internal/fsm`, `internal/tree`, and from Rung 7 `internal/shard`, `internal/meta`, `internal/shardfsm`, `internal/gossip` and `internal/automation`. Given the same events in the same order, they must produce the same outputs. In them:
- Take time as ticks and as stamps passed in. Take randomness from the seeded source passed in at construction.
- Do all work in the calling goroutine and return.
- When output order could depend on a map, sort the keys first.
- Keep all state in the struct the caller owns.

`internal/purity` enforces the import and goroutine rules. A change that needs a clock, a socket, a file or a goroutine belongs in a shell (`cmd/kvnode`, `internal/transport`, `internal/storage`, `internal/server`, `internal/sim`).

## Rung conventions
- Each Rung ships its naive version first and records seeds that show it failing, before the fix (README → Success Criteria, "Exposed"). The Decision Log names the deliberate gaps: leave them until their Rung.
- A failing Simulation run prints its seed. Record the seed in the retro, and keep it as a regression test once fixed.
- Every run gets the three verdicts of A§8.2. A Rung is done only when all four README checks pass, including `retros/rung-N.md`.
- Rung 7's design is A§11, in three stages; each stage needs the user's approval before the next. Rungs 8–9 are sketches (A§10). Their `.0` task in `PLAN.md` confirms the design with the user before any code.

## Errors
Client-facing errors come from the A§7.2 table: HTTP status plus `reason`. New reasons go into A§7.2 first.

## Running things
- Simulation tests are plain `go test` and are cheap. Run them freely.
- Real runs (local processes or Docker Compose) under sustained load need the user's approval first, with the exact run list.
- Ask before editing: `deploy/docker-compose.yml`, `.gitignore`.

## Key files
`cmd/{kvnode,kvctl,kvbench}`, `internal/{core,naive,raft,fsm,tree,storage,transport,server,sim,check,rungtest,purity}`, and for several Groups `internal/{shard,meta,shardfsm,mover,gossip,automation,cluster}`, `harness/`, `deploy/`, `retros/`.
