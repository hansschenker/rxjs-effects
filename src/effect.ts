import {
  defer, of, single, map as rxMap, concatMap, catchError, timeout as rxTimeout,
  repeat, filter, take, throwError, materialize, reduce, queueScheduler, timer, type Observable, type SchedulerLike
} from "rxjs";
import { success, failure, type Result } from "./result";

// R describes an environment object (unknown means no required capabilities).
// A valid execution is cold and emits exactly one outcome on normal termination.
// Public boundaries validate cardinality; external source teardown is cooperative.
export type Effect<A, E = never, R = unknown> =
  (environment: R) => Observable<Result<A, E>>;

export const run =
  <A, E, R>(effect: Effect<A, E, R>) =>
  (environment: R): Observable<Result<A, E>> =>
    defer(() => effect(environment)).pipe(single());

export const succeed =
  <A>(value: A): Effect<A> =>
  () => of(success(value));

export const fail =
  <const E>(error: E): Effect<never, E> =>
  () => of(failure(error));

export const suspend =
  <A, E, R>(factory: () => Effect<A, E, R>): Effect<A, E, R> =>
  environment => defer(() => run(factory())(environment));

export const sync =
  <A>(factory: () => A): Effect<A> =>
  () => defer(() => of(success(factory())));

// The supplied Observable must complete with exactly one value.
// Source errors remain defects; do not pass a pre-started Promise.
export const fromObservable =
  <A, R = unknown>(factory: (environment: R) => Observable<A>): Effect<A, never, R> =>
  environment => defer(() => factory(environment)).pipe(rxMap(success));

// Explicitly translate source defects to domain failures. Provisional values
// are withheld until completion; cardinality violations remain defects.
export const tryObservable =
  <E>(toError: (cause: unknown) => E) =>
  <A, R = unknown>(factory: (environment: R) => Observable<A>): Effect<A, E, R> =>
  environment => defer(() => factory(environment)).pipe(
    materialize(),
    reduce((state: { seen: boolean; result?: Result<A, E> }, notification) => {
      switch (notification.kind) {
        case "N":
          if (state.seen) throw new RangeError("Effect source emitted multiple values");
          return { seen: true, result: success(notification.value!) };
        case "E":
          return { seen: state.seen, result: failure(toError(notification.error)) };
        case "C":
          if (!state.seen) throw new RangeError("Effect source completed without a value");
          return state;
      }
    }, { seen: false } as { seen: boolean; result?: Result<A, E> }),
    rxMap(state => state.result!)
  );

export const access =
  <R, A>(read: (environment: R) => A): Effect<A, never, R> =>
  environment => defer(() => of(success(read(environment))));

export const provide =
  <R>(environment: R) =>
  <A, E>(source: Effect<A, E, R>): Effect<A, E> =>
  () => run(source)(environment);

export const map =
  <A, B>(project: (value: A) => B) =>
  <E, R>(source: Effect<A, E, R>): Effect<B, E, R> =>
  environment => run(source)(environment).pipe(
    rxMap((result): Result<B, E> =>
      result.kind === "Success" ? success(project(result.value)) : result)
  );

export const mapError =
  <E, F>(project: (error: E) => F) =>
  <A, R>(source: Effect<A, E, R>): Effect<A, F, R> =>
  environment => run(source)(environment).pipe(
    rxMap((result): Result<A, F> =>
      result.kind === "Failure" ? failure(project(result.error)) : result)
  );

export const flatMap =
  <A, B, E2, R2>(project: (value: A) => Effect<B, E2, R2>) =>
  <E1, R1>(source: Effect<A, E1, R1>): Effect<B, E1 | E2, R1 & R2> =>
  environment => run(source)(environment).pipe(
    concatMap((result): Observable<Result<B, E1 | E2>> =>
      result.kind === "Failure"
        ? of(result)
        : run(project(result.value))(environment))
  );

export const recover =
  <E1, B, E2, R2>(handler: (error: E1) => Effect<B, E2, R2>) =>
  <A, R1>(source: Effect<A, E1, R1>): Effect<A | B, E2, R1 & R2> =>
  environment => run(source)(environment).pipe(
    concatMap((result): Observable<Result<A | B, E2>> =>
      result.kind === "Success"
        ? of(result)
        : run(handler(result.error))(environment))
  );

export const timeout =
  <const E2>(milliseconds: number, error: E2, scheduler?: SchedulerLike) =>
  <A, E1, R>(source: Effect<A, E1, R>): Effect<A, E1 | E2, R> =>
  environment => run(source)(environment).pipe(
    rxTimeout({ first: milliseconds, scheduler, with: () => of(failure(error)) })
  );

// A retry policy counts additional attempts, not total executions.
// queueScheduler trampolines synchronous retries without forcing an async tick.
export type RetryPolicy =
  | { readonly kind: "Immediate"; readonly maxRetries: number }
  | { readonly kind: "Delayed"; readonly maxRetries: number;
      readonly milliseconds: number; readonly scheduler?: SchedulerLike };

export const retryWith =
  (policy: RetryPolicy) =>
  <A, E, R>(source: Effect<A, E, R>): Effect<A, E, R> =>
  environment => defer(() => {
    const { maxRetries } = policy;
    if (!Number.isSafeInteger(maxRetries) || maxRetries < 0 || maxRetries >= Number.MAX_SAFE_INTEGER) {
      return throwError(() => new RangeError("Invalid maxRetries"));
    }
    if (policy.kind === "Delayed" && (!Number.isFinite(policy.milliseconds) || policy.milliseconds < 0)) {
      return throwError(() => new RangeError("Invalid retry delay"));
    }
    // Schedule the repeat notifier, not just the source subscription.
    // The queue remains active across subsequent retry notifications so
    // repeat cannot recurse through synchronous source completion.
    const delay = () => policy.kind === "Immediate"
      ? timer(0, queueScheduler)
      : timer(policy.milliseconds, policy.scheduler);
    return run(source)(environment).pipe(
      repeat({ count: maxRetries + 1, delay }),
      filter((result, index) => result.kind === "Success" || index === maxRetries),
      take(1)
    );
  });

export const retryExpected = (maxRetries: number) =>
  retryWith({ kind: "Immediate", maxRetries });
