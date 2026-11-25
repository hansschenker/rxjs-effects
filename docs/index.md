# rxjs-effects

**Type algebra meets reactive execution.**

TypeScript expresses the contract. RxJS executes it. Domain functions supply the meaning.

```ts
type Effect<A, E = never, R = unknown> =
  (environment: R) => Observable<Result<A, E>>;
```

An Effect is a function describing a single-outcome computation. The environment is an object of dependencies; expected errors are typed values. Operators compose those contracts, while RxJS manages subscriptions, scheduling, teardown and coordination.

## Intuition → Specification → Verification

**Intuition:** a description of a computation that can succeed or fail.

**Specification:** on subscription, execute the factory; on normal termination, emit exactly one Success or Failure outcome and complete. Defects use the error channel. Unsubscription stops delivery and triggers teardown.

**Verification:** the Vitest suite checks cold execution, typed error unions, dependency intersections, short-circuiting, cardinality, retry, timeout and cancellation.

No new execution runtime. No class declarations, async/await or generator syntax.
