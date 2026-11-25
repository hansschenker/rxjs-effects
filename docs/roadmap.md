# Release scope

The 1.0 release candidate consolidates the existing project rather than creating a new runtime.

| Milestone | Implementation |
| --- | --- |
| M01 — Typed Effect core | Implemented |
| M02 — Coordination | all, race, coordinate, retry policies |
| M03 — Resource safety | Scope, LIFO asynchronous finalizers, scoped acquisition/use/release |
| M04 — Typed sequences | Separate Stream abstraction, transformation, recovery and sequential composition |
| M05 — Verification | Runtime, type, virtual-time, lifecycle and property-based tests |

## Release gate

Run npm run verify. A release candidate is not a verified stable release until type checking, tests, library build and documentation build pass. Do not publish automatically.

## Explicit non-goals for 1.0

Complete compatibility with Effect-TS, arbitrary-depth stack-safe composition, asynchronous acquisition with interruption masking, automatic fiber supervision, and general backpressure are not part of this release. These would need distinct contracts and verification, not additional generic parameters alone.
