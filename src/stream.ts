import { defer, of, takeWhile, map as rxMap, concatMap, type Observable } from "rxjs";
import { run, type Effect } from "./effect";
import { success, failure, type Result } from "./result";

// Distinct from Effect: zero or more successes followed by at most one failure.
// An object wrapper prevents accidental assignment of a Stream to Effect.
export type Stream<A, E = never, R = unknown> = {
  readonly open: (environment: R) => Observable<Result<A, E>>;
};

export const stream =
  <A, E, R = unknown>(open: (environment: R) => Observable<Result<A, E>>): Stream<A, E, R> =>
  ({ open });

export const runStream =
  <A, E, R>(source: Stream<A, E, R>) =>
  (environment: R): Observable<Result<A, E>> =>
    defer(() => source.open(environment)).pipe(
      takeWhile(result => result.kind === "Success", true)
    );

export const fromValues =
  <A, R = unknown>(factory: (environment: R) => Observable<A>): Stream<A, never, R> =>
  stream(environment => defer(() => factory(environment)).pipe(rxMap(success)));

export const streamFromEffect =
  <A, E, R>(effect: Effect<A, E, R>): Stream<A, E, R> =>
  stream(environment => run(effect)(environment));

export const mapStream =
  <A, B>(project: (value: A) => B) =>
  <E, R>(source: Stream<A, E, R>): Stream<B, E, R> =>
  stream(environment => runStream(source)(environment).pipe(
    rxMap((result): Result<B, E> =>
      result.kind === "Success" ? success(project(result.value)) : result)
  ));

export const mapStreamError =
  <E, F>(project: (error: E) => F) =>
  <A, R>(source: Stream<A, E, R>): Stream<A, F, R> =>
  stream(environment => runStream(source)(environment).pipe(
    rxMap((result): Result<A, F> =>
      result.kind === "Failure" ? failure(project(result.error)) : result)
  ));

export const recoverStream =
  <E, B, F, R2>(handler: (error: E) => Stream<B, F, R2>) =>
  <A, R1>(source: Stream<A, E, R1>): Stream<A | B, F, R1 & R2> =>
  stream(environment => runStream(source)(environment).pipe(
    concatMap((result): Observable<Result<A | B, F>> =>
      result.kind === "Success" ? of(result) : runStream(handler(result.error))(environment))
  ));

// Stream-local sequential effect composition. The first typed failure of
// either source or inner effect terminates the output (and cancels upstream).
export const flatMapStream =
  <A, B, F, R2>(project: (value: A) => Effect<B, F, R2>) =>
  <E, R1>(source: Stream<A, E, R1>): Stream<B, E | F, R1 & R2> =>
  stream(environment => runStream(source)(environment).pipe(
    concatMap((result): Observable<Result<B, E | F>> =>
      result.kind === "Failure" ? of(result) : run(project(result.value))(environment)),
    takeWhile(result => result.kind === "Success", true)
  ));
