# Venue Design Documents

This directory holds Venue's **design documentation**. Repository rules, package
ownership boundaries and testing requirements live in the root `AGENTS.md`; this
directory answers "what is the design, why is it this way, and where does it
differ from Locus".

## Document map

| Document | Contents | Read it before |
| --- | --- | --- |
| [`architecture.md`](architecture.md) | The design of the current implementation: layering and package responsibilities, public contracts and optional capabilities, lifecycle, key data flows (write, claim, complete, cleanup, recovery, import, reconciliation), configuration model, logging and errors, concurrency and durability invariants, testing conventions | changing any layer's implementation |
| [`locus-alignment.md`](locus-alignment.md) | Per-capability alignment matrix against Locus `v2.0.0`, mechanism differences and guarantee comparison, deliberate divergences, outstanding gaps (G1–G9), unconfirmed items | deciding whether something is "aligned with Locus" and what to prioritize |
| [`sqlite-storage-design.md`](sqlite-storage-design.md) | The storage-engine migration from BadgerDB to SQLite: directory and file layout, tables and indexes, PRAGMA and connection policy, CAS SQL, paging, backup and corruption recovery, configuration surface, migration phases and risks | changing storage or metadata behaviour, or working on the engine migration |
| [`locus-feature-gaps.md`](locus-feature-gaps.md) | Work tracker: feature gaps and batch plan (W1–W5), mechanism divergences (M1–M6), documentation/behaviour mismatches (D1–D10) | planning work and checking completion status |

## Baseline and evidence conventions

- Behavioural baseline: Locus `v2.0.0`, commit `292bd2cea7051ec277d97ca708443e668b40a2d4`.
- The Locus v2.0.0 sources are **not** part of this repository. When a claim needs
  Locus evidence, cite it as `locus/src/...` (for example
  `locus/src/Locus.Storage/FileScheduler.cs:602-609`) and keep a read-only
  reference checkout available locally for verification.
- Venue evidence is always repository-relative, for example
  `pkg/pool/storage_pool.go:170` or `venue.go:75`.
- A conclusion needs evidence from both sides. An item with only one side must be
  marked "single-sided evidence, unconfirmed".
- Classifications: `PRESENT` (already equivalent), `OBSERVABLE-GAP` (observable
  difference, to be closed), `MECHANISM` (different internal mechanism, so the
  observable guarantee must be compared), `DIVERGENCE` (deliberate difference,
  with rationale and caller-visible impact).

## Maintenance rules

- **Behavioural changes update the documents.** When you change a public
  contract, status semantic, configuration field or persistence layout, update
  `architecture.md` in the same change; update `locus-alignment.md` when the
  alignment conclusion moves; update `sqlite-storage-design.md` when the engine,
  schema or backup design changes; update `locus-feature-gaps.md` when a work
  item is completed or added.
- Do not write progress narration or temporary task notes in these documents.
  Completion status belongs in the `locus-feature-gaps.md` status column.
- Never document unverified behaviour as fact. Record what cannot be confirmed in
  the "unconfirmed" section of the relevant document and backfill it after
  verification.
- Known documentation debt (README/AGENTS statements that disagree with the code)
  is collected in [`architecture.md`](architecture.md); clean it up together with
  the code fix.

## Verification gates

Every behavioural change must pass all of these:

```text
go build ./...
CGO_ENABLED=0 go build ./...
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
golangci-lint run
go test -run '^$' -bench=. -benchmem ./...
```

The SQLite integration must build and test **without cgo**: the pure-Go driver
requirement is a hard constraint, not a preference.

If the default Go cache is not writable in the current environment, point the Go
cache and temporary directories at an ignored repository-local directory of your
choice; do not commit machine-specific paths to documentation or configuration.
