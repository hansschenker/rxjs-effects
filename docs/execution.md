# Execution contract

## What flows?

`Result<A, E>` is a discriminated union. RxJS transports the package without interpreting the domain. Effect operators inspect its discriminant to decide whether to transform, continue or recover.

## Lifecycle

- Construct: describe a computation; pure configuration may be evaluated immediately.
- Subscribe: `run(effect)(environment)` invokes the factory using `defer`.
- Normal termination: `single()` verifies exactly one outcome and emits it after the source completes.
- Defect: propagate the RxJS error notification; do not convert automatically.
- Cancellation: unsubscribe; no subsequent outcome or completion notification. Source teardown must actually cancel external work.
- Sharing: absent unless explicitly introduced; externally supplied hot sources are the caller's responsibility.

A never-ending source can wait indefinitely. Apply a timeout if a deadline is required. The single-result boundary intentionally waits for completion, rather than accepting the first value of a possibly invalid sequence.

## Composition

```ts
flatMap(f)(source)
```

On Success, subscribe to the dependent computation. On Failure, propagate the failure value without invoking f. Error types form a union; required object environments form an intersection. TypeScript cannot ensure externally constructed sources meet their coldness contract.

## Coordination

Use RxJS on a stream of inputs to coordinate separate effects. Each inner effect has its own result contract.

| Policy | RxJS |
| --- | --- |
| Allow overlap | mergeMap |
| Queue | concatMap |
| Latest only | switchMap |
| Ignore while busy | exhaustMap |

```ts
const results$ = counts$.pipe(
  switchMap(count => run(checkStock(count))(undefined))
);
```

Expected failures do not terminate the outer stream. Defects do unless handled explicitly. No sharing is implied.
