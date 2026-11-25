# rxjs-effects

**Type algebra meets reactive execution.**

A class-free, function-first TypeScript library for typed effectful computations and reactive sequences. RxJS 7.8.2 provides the execution substrate; there is no separate fiber runtime, no generator syntax and no application-level async/await.

## Type contracts

```ts
type Result<A, E> =
  | { readonly kind: "Success"; readonly value: A }
  | { readonly kind: "Failure"; readonly error: E };

type Effect<A, E = never, R = unknown> =
  (environment: R) => Observable<Result<A, E>>;

type Stream<A, E = never, R = unknown> = {
  readonly open: (environment: R) => Observable<Result<A, E>>;
};
```

An Effect emits exactly one outcome on normal termination, after source completion. A Stream emits zero or more successes, optionally ending with one typed failure. Expected errors are values; unexpected defects use the RxJS error channel. Cancellation is silent to the cancelled subscriber. Each connection owns its execution unless a source explicitly shares it.

```ts
import { type Effect, succeed, fail, flatMap, run, pipe } from "./src";

const checkStock = (count: number): Effect<number, "NegativeCount"> =>
  count < 0 ? fail("NegativeCount") : succeed(count);

const program = pipe(
  checkStock(10),
  flatMap((count: number) => succeed(count * 2))
);

run(program)(undefined).subscribe(console.log);
// { kind: "Success", value: 20 }
```

## Library surface

| Area | Operations |
| --- | --- |
| Construction | succeed, fail, sync, suspend, fromObservable, tryObservable |
| Composition | pipe, map, flatMap, mapError, recover |
| Dependencies | access, provide |
| Coordination | all, race, coordinate (Merge / Concat / Switch / Exhaust) |
| Time | timeout, retryExpected, retryWith |
| Resource lifetime | createScope, scoped, acquireUseRelease |
| Typed sequences | stream, runStream, fromValues, streamFromEffect, mapStream, mapStreamError, recoverStream, flatMapStream |

## Execution contract

- run is cold when its sources are cold and enforces single-result cardinality.
- all preserves input tuple order and fails fast on the first typed failure or defect.
- race selects the first settled outcome, including failure.
- coordinate keeps expected failures local to each input; runStream ends at its first typed failure.
- scoped awaits asynchronous cleanup before publishing a result or defect. On cancellation, cleanup continues independently, and cleanup errors are sent to the explicit cancellation reporter.
- acquireUseRelease supports synchronous acquisition and Observable-based release. Asynchronous acquisition and complete Effect-TS fiber supervision are not claimed.
- Immediate bounded retries use RxJS queueScheduler to avoid synchronous retry recursion. Deep arbitrary nested composition is not guaranteed stack-safe.
- Unsubscription only cancels underlying work when its source implements cancellation.
- No automatic sharing, native push backpressure, or npm publication is implied.

## Verify locally

```sh
npm install
npm run verify
npm run docs:dev
```

GitHub Actions runs type checking, Vitest, the Vite library build, and the VitePress documentation build. The package remains private until the release candidate passes verification and publication is explicitly requested.

## Contributors

- Hans Schenker — project vision and reactive programming design.
- **myProfessorAstra-62** — AI-assisted architecture, implementation, documentation and test design (OpenAI ChatGPT).

Independent project inspired by fp-ts, Effect's typed contracts and Reactive Extensions.
