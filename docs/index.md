# rxjs-effects

**Type algebra meets reactive execution.**

TypeScript expresses the contract. RxJS executes it. Domain functions supply the meaning.

```ts
type Effect<A, E = never, R = unknown> =
  (environment: R) => Observable<Result<A, E>>;
```

An Effect describes one outcome. A Stream describes zero or more successes and optionally a terminal typed failure. Both reuse RxJS subscriptions, timing and cancellation.

## Intuition → Specification → Verification

An expected failure is a typed package, not an RxJS error notification. Operators interpret the package and decide whether to transform, continue, recover or terminate. The verification suite tests contracts, type inference, algebraic laws, time, cancellation and scope finalization.

No additional execution engine. No classes, generators or async/await programming style.

Contributor: **myProfessorAstra-62** (AI-assisted design and implementation).
