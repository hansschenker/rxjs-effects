# Roadmap

## M01 — Typed single-result core (v0.1)

Result, Effect, constructors, typed sequential composition, explicit dependency supply, recovery, bounded retry, timeout, single-outcome validation, and lifecycle tests.

## M02 — Coordination

Effect-aware parallel composition, race contracts, retry schedules, virtual-time tests, and explicit sharing policies.

## M03 — Resource safety

Scopes, acquire/use/release, ownership, asynchronous cleanup protocol, and interruption semantics. RxJS teardown is synchronous; waiting for asynchronous finalizers needs an explicit additional contract.

## M04 — Typed sequences

Stream type with zero-to-many values, typed terminal failures, coordination and optional demand protocols. Do not conflate Stream with single-result Effect.

## M05 — Verification

Property-based algebraic laws with explicit equivalence assumptions, cancellation/termination traces, stack safety, and subscription-tree supervision.

The aim is not parity with the Effect-TS runtime. New guarantees must be specified and tested before they are claimed.
