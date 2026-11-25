# rxjs-effects

**Typed effectful computations on the RxJS execution model.**

Capture values, expected failures and required dependencies in types. Delegate execution, timing, cancellation and coordination to RxJS.

## Design principles

- RxJS 7.8.2 as the execution substrate; no separate fiber runtime.
- Class-free, function-first TypeScript; no application-level async/await or generator syntax.
- Cold by construction when sources are cold; sharing is explicit.
- Expected failures are discriminated-union values, not RxJS error notifications.
- Unexpected exceptions remain defects on the Observable error channel.
- One outcome on normal termination; cancellation delivers no further notifications.

## Core contract

```ts
type Result<A, E> =
  | { kind: "Success"; value: A }
  | { kind: "Failure"; error: E };

type Effect<A, E = never, R = unknown> =
  (environment: R) => Observable<Result<A, E>>;
```

The public run boundary defers evaluation until subscription and verifies that the source completes with exactly one outcome. A source that emits zero or multiple outcomes is a contract violation, not a typed domain failure. A source may never terminate.

```ts
import { pipe } from "rxjs";
import { type Effect, succeed, fail, flatMap, run } from "./src";

const checkStock = (count: number): Effect<number, "NegativeCount"> =>
  count < 0 ? fail("NegativeCount") : succeed(count);

const program = pipe(
  checkStock(10),
  flatMap((count: number) => succeed(count * 2))
);

run(program)(undefined).subscribe(console.log);
// { kind: "Success", value: 20 }
```

## Start locally

```sh
npm install
npm run typecheck
npm test
npm run build
npm run docs:dev
```

See the VitePress documentation for execution contracts, failure semantics and the roadmap.

## Contributors

- Hans Schenker — project vision and reactive programming design.
- ChatGPT (OpenAI) — architecture, implementation, documentation and test design.

This is an independent experimental library, inspired by fp-ts and Effect's typed effect contracts. It is not an implementation of the Effect-TS runtime.
