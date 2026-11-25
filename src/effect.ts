import {
  defer, of, single, map as rxMap, concatMap, catchError, timeout as rxTimeout,
  repeat, filter, take, throwError, type Observable, type SchedulerLike
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

// Explicitly translate source errors to expected domain failures.
// Cardinality violations are checked downstream by run, outside this catch.
export const tryObservable =
  <E>(toError: (cause: unknown) => E) =>
  <A, R = unknown>(factory: (environment: R) => Observable<A>): Effect<A, E, R> =>
  environment => defer(() => factory(environment)).pipe(
    rxMap(success),
    catchError((cause: unknown) => of(failure(toError(cause))))
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

// Retry only normal typed failures, never defects. maxRetries excludes the first attempt.
export const retryExpected =
  (maxRetries: number) =>
  <A, E, R>(source: Effect<A, E, R>): Effect<A, E, R> =>
  environment => defer(() => {
    if (!Number.isSafeInteger(maxRetries) || maxRetries < 0 || maxRetries >= Number.MAX_SAFE_INTEGER) {
      return throwError(() => new RangeError("maxRetries must be a non-negative safe integer below MAX_SAFE_INTEGER"));
    }
    return run(source)(environment).pipe(
      repeat({ count: maxRetries + 1 }),
      filter((result, index) => result.kind === "Success" || index === maxRetries),
      take(1)
    );
  });
