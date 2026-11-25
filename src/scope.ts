import {
  Subscription, ReplaySubject, defer, from, of, concatMap, ignoreElements,
  endWith, catchError, toArray, map as rxMap, observeOn, asapScheduler,
  finalize, throwError, type Observable
} from "rxjs";
import { run, suspend, type Effect } from "./effect";
import type { Result } from "./result";

export type CleanupReport = { readonly errors: readonly unknown[] };
export type Finalizer = () => Observable<unknown>;
export type Scope = {
  readonly own: <S extends Subscription>(subscription: S) => S;
  readonly addFinalizer: (finalizer: Finalizer) => void;
  readonly close: () => Observable<CleanupReport>;
  readonly closed$: Observable<CleanupReport>;
};

// close is the explicit execution boundary for cleanup. It starts once, cancels
// owned subscriptions, then runs finalizers in LIFO order, even after failures.
// Replay is intentional: every observer sees the same final cleanup report.
export const createScope = (): Scope => {
  const owner = new Subscription();
  const finalizers: Finalizer[] = [];
  const report = new ReplaySubject<CleanupReport>(1);
  const closed$ = report.asObservable();
  let closing = false;

  const own = <S extends Subscription>(subscription: S): S => {
    owner.add(subscription);
    return subscription;
  };

  const addFinalizer = (finalizer: Finalizer): void => {
    if (closing) throw new Error("Scope is closing or closed");
    finalizers.push(finalizer);
  };

  const close = (): Observable<CleanupReport> => {
    if (closing) return closed$;
    closing = true;
    const errors: unknown[] = [];
    try { owner.unsubscribe(); } catch (cause) { errors.push(cause); }
    const work = finalizers.splice(0).reverse();
    from(work).pipe(
      concatMap(finalizer => defer(finalizer).pipe(
        ignoreElements(),
        catchError((cause: unknown) => { errors.push(cause); return of(undefined); }),
        endWith(undefined)
      )),
      toArray()
    ).subscribe({
      complete: () => { report.next({ errors: [...errors] }); report.complete(); },
      error: (cause: unknown) => {
        report.next({ errors: [...errors, cause] }); report.complete();
      }
    });
    return closed$;
  };
  return { own, addFinalizer, close, closed$ };
};

type Settled<A, E> =
  | { readonly kind: "Outcome"; readonly result: Result<A, E> }
  | { readonly kind: "Defect"; readonly cause: unknown };

// Normal outcomes and defects wait for cleanup. Cancellation starts cleanup
// but cannot deliver a result to a closed subscriber. The mandatory reporter
// receives cleanup failures in that case. asapScheduler is an intentional
// asynchronous settlement boundary, allowing use-source teardown to finish.
export const scoped =
  <A, E, R>(
    use: (scope: Scope) => Effect<A, E, R>,
    onCancelledCleanup: (report: CleanupReport) => void
  ): Effect<A, E, R> =>
  environment => defer(() => {
    const scope = createScope();
    let settled = false;
    return run(suspend(() => use(scope)))(environment).pipe(
      rxMap((result): Settled<A, E> => ({ kind: "Outcome", result })),
      catchError((cause: unknown) => of<Settled<A, E>>({ kind: "Defect", cause })),
      observeOn(asapScheduler),
      concatMap(exit => scope.close().pipe(
        concatMap((report): Observable<Result<A, E>> => {
          settled = true;
          if (report.errors.length > 0) {
            const causes = exit.kind === "Defect" ? [exit.cause, ...report.errors] : report.errors;
            return throwError(() => new AggregateError(causes, "Scope cleanup failed"));
          }
          return exit.kind === "Outcome" ? of(exit.result) : throwError(() => exit.cause);
        })
      )),
      finalize(() => {
        if (!settled) {
          scope.close().subscribe(report => {
            if (report.errors.length > 0) onCancelledCleanup(report);
          });
        }
      })
    );
  });

// Acquisition is synchronous, so acquisition and registering release cannot
// be separated by an asynchronous cancellation boundary. Release may be async.
export const acquireUseRelease =
  <Resource, A, E, R>(
    acquire: (environment: R) => Resource,
    use: (resource: Resource) => Effect<A, E, R>,
    release: (resource: Resource, environment: R) => Observable<unknown>,
    onCancelledCleanup: (report: CleanupReport) => void
  ): Effect<A, E, R> =>
  scoped(scope => environment => defer(() => {
    const resource = acquire(environment);
    scope.addFinalizer(() => release(resource, environment));
    return run(suspend(() => use(resource)))(environment);
  }), onCancelledCleanup);
