import {
  defer, from, of, mergeMap, concatMap, switchMap, exhaustMap, scan, filter,
  map as rxMap, take, throwError, type Observable, type OperatorFunction
} from "rxjs";
import { run, type Effect } from "./effect";
import { success, failure, type Result } from "./result";

// The never environment permits heterogeneous effects under strict contravariance.
// Public signatures compute the actual required environment before erasing it.
type AnyEffect = Effect<unknown, unknown, never>;
export type ValueOf<F> = F extends Effect<infer A, infer _E, infer _R> ? A : never;
export type ErrorOf<F> = F extends Effect<infer _A, infer E, infer _R> ? E : never;
export type EnvironmentOf<F> = F extends Effect<infer _A, infer _E, infer R> ? R : never;
export type ValuesOf<T extends readonly AnyEffect[]> = {
  -readonly [K in keyof T]: ValueOf<T[K]>
};
type ArrayEnvironment<F> =
  (F extends AnyEffect ? (environment: EnvironmentOf<F>) => void : never) extends
    (environment: infer R) => void ? R : unknown;
export type EnvironmentsOf<T extends readonly AnyEffect[]> =
  T extends readonly [] ? unknown :
  T extends readonly [infer H extends AnyEffect, ...infer Rest extends readonly AnyEffect[]]
    ? EnvironmentOf<H> & EnvironmentsOf<Rest>
    : ArrayEnvironment<T[number]>;

type ParallelState = {
  values: unknown[];
  count: number;
  outcome: Result<unknown[], unknown> | undefined;
};

// Fail-fast parallel composition. Result order is input order, not arrival order.
// A synchronous failure can prevent later sources from being subscribed.
export const all = <const T extends readonly AnyEffect[]>(
  ...effects: T
): Effect<ValuesOf<T>, ErrorOf<T[number]>, EnvironmentsOf<T>> =>
  environment => defer(() => {
    if (effects.length === 0) {
      return of(success([] as unknown as ValuesOf<T>));
    }
    return from(effects).pipe(
      mergeMap((effect, index) => run(effect)(environment as never).pipe(
        rxMap(result => ({ index, result }))
      )),
      scan((state: ParallelState, { index, result }): ParallelState => {
        if (result.kind === "Failure") return { ...state, outcome: failure(result.error) };
        const values = state.values.slice();
        values[index] = result.value;
        const count = state.count + 1;
        return { values, count, outcome: count === effects.length ? success(values) : undefined };
      }, { values: new Array<unknown>(effects.length), count: 0, outcome: undefined } as ParallelState),
      rxMap(state => state.outcome),
      filter((result): result is Result<unknown[], unknown> => result !== undefined),
      take(1)
    ) as unknown as Observable<Result<ValuesOf<T>, ErrorOf<T[number]>>>;
  });

// First settled outcome wins, including expected failure. Defects still error.
// Unsubscribe losers; external work must implement its teardown.
export const race = <const T extends readonly [AnyEffect, ...AnyEffect[]]>(
  ...effects: T
): Effect<ValueOf<T[number]>, ErrorOf<T[number]>, EnvironmentsOf<T>> =>
  environment => defer(() => effects.length === 0
    ? throwError(() => new RangeError("race requires at least one effect"))
    : from(effects).pipe(
      mergeMap(effect => run(effect)(environment as never)),
      take(1)
    )) as Observable<Result<ValueOf<T[number]>, ErrorOf<T[number]>>>;

export type CoordinationPolicy =
  | { readonly kind: "Merge"; readonly concurrency?: number }
  | { readonly kind: "Concat" }
  | { readonly kind: "Switch" }
  | { readonly kind: "Exhaust" };

// Expected failures are per-input results, not termination of the input stream.
export const coordinate =
  (policy: CoordinationPolicy) =>
  <T, A, E, R>(project: (value: T) => Effect<A, E, R>, environment: R):
    OperatorFunction<T, Result<A, E>> =>
  source => defer(() => {
    const execute = (value: T) => defer(() => run(project(value))(environment));
    switch (policy.kind) {
      case "Merge": {
        const concurrency = policy.concurrency ?? Infinity;
        if (concurrency !== Infinity && (!Number.isSafeInteger(concurrency) || concurrency < 1)) {
          return throwError(() => new RangeError("Invalid concurrency"));
        }
        return source.pipe(mergeMap(execute, concurrency));
      }
      case "Concat": return source.pipe(concatMap(execute));
      case "Switch": return source.pipe(switchMap(execute));
      case "Exhaust": return source.pipe(exhaustMap(execute));
    }
  });
