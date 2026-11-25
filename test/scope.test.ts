import { describe, it, expect, vi } from "vitest";
import { EMPTY, NEVER, Subject, Subscription, of, throwError, tap, asapScheduler } from "rxjs";
import { createScope, scoped, acquireUseRelease, succeed, fail, fromObservable, run, type CleanupReport } from "../src";

describe("scope", () => {
  it("cancels owned subscriptions before LIFO cleanup and replays the same report", () => {
    const scope = createScope();
    const trace: string[] = [];
    scope.own(new Subscription(() => trace.push("cancel")));
    scope.addFinalizer(() => { trace.push("first"); return EMPTY; });
    scope.addFinalizer(() => { trace.push("last"); return EMPTY; });
    const a = vi.fn(), b = vi.fn();
    scope.close().subscribe(a);
    scope.close().subscribe(b);
    expect(trace).toEqual(["cancel","last","first"]);
    expect(a).toHaveBeenCalledWith({errors:[]});
    expect(b).toHaveBeenCalledWith({errors:[]});
    expect(() => scope.addFinalizer(() => EMPTY)).toThrow();
  });

  it("runs remaining cleanup after failure and waits for completion", () => {
    const scope = createScope(), gate = new Subject<void>();
    const error = new Error("cleanup"), next = vi.fn(), other = vi.fn(() => EMPTY);
    scope.addFinalizer(other);
    scope.addFinalizer(() => throwError(() => error));
    scope.addFinalizer(() => gate);
    scope.close().subscribe(next);
    expect(next).not.toHaveBeenCalled();
    gate.complete();
    expect(other).toHaveBeenCalledTimes(1);
    expect(next).toHaveBeenCalledWith({ errors:[error] });
  });

  it("waits for asynchronous cleanup before exposing the outcome", () =>
    new Promise<void>((resolve, reject) => {
      const release = new Subject<void>(), next = vi.fn();
      const effect = acquireUseRelease(
        () => "resource", resource => succeed(resource),
        () => release, report => reject(report)
      );
      run(effect)(undefined).subscribe({next, error: reject, complete: () => {
        try { expect(next).toHaveBeenCalledWith({kind:"Success",value:"resource"}); resolve(); }
        catch (error) { reject(error); }
      }});
      asapScheduler.schedule(() => {
        try { expect(next).not.toHaveBeenCalled(); release.complete(); }
        catch(error) { reject(error); }
      });
    }));

  it("releases after typed failure", () =>
    new Promise<void>((resolve, reject) => {
      const release = vi.fn(() => EMPTY);
      run(acquireUseRelease(() => 1, () => fail("bad"), release, reject))(undefined)
        .subscribe({ next: result => {
          try {
            expect(result).toEqual({kind:"Failure",error:"bad"});
            expect(release).toHaveBeenCalledTimes(1);
          } catch (error) { reject(error); }
        }, error: reject, complete: resolve });
    }));

  it("continues cleanup on cancellation and reports its failure", () => {
    const release = new Subject<void>(), report = vi.fn(), next = vi.fn();
    const effect = acquireUseRelease(
      () => "resource", () => fromObservable(() => NEVER), () => release, report
    );
    const sub = run(effect)(undefined).subscribe(next);
    sub.unsubscribe();
    const defect = new Error("cleanup");
    release.error(defect);
    expect(report).toHaveBeenCalledWith({errors:[defect]});
    expect(next).not.toHaveBeenCalled();
  });

  it("combines original and cleanup defects on normal settlement", () =>
    new Promise<void>((resolve, reject) => {
      const original = new Error("use"), cleanup = new Error("release");
      const effect = scoped(scope => {
        scope.addFinalizer(() => throwError(() => cleanup));
        return fromObservable(() => throwError(() => original));
      }, reject);
      run(effect)(undefined).subscribe({
        next: () => reject(new Error("unexpected outcome")),
        complete: () => reject(new Error("unexpected completion")),
        error: cause => {
          try { expect(cause).toBeInstanceOf(AggregateError); expect(cause.errors).toEqual([original, cleanup]); resolve(); }
          catch (error) { reject(error); }
        }
      });
    }));
});
