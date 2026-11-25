import { pipe } from "../src";
import { describe, it, expect, expectTypeOf, vi } from "vitest";
import { EMPTY, NEVER, Observable, of, throwError, timer, map as rxMap, Subject, switchMap } from "rxjs";
import {
  type Effect, type Result, access, fail, flatMap, fromObservable, map,
  mapError, provide, recover, retryExpected, run, succeed, suspend, sync,
  timeout, tryObservable
} from "../src";

const collect = <A, E>(effect: Effect<A, E>) => {
  const values: Result<A, E>[] = [];
  run(effect)(undefined).subscribe(value => values.push(value));
  return values;
};

describe("effect contracts", () => {
  it("delivers one typed success or failure", () => {
    expect(collect(succeed(3))).toEqual([{ kind: "Success", value: 3 }]);
    expect(collect(fail("NegativeCount"))).toEqual([{ kind: "Failure", error: "NegativeCount" }]);
  });

  it("defers execution and does not share subscriptions", () => {
    const factory = vi.fn(() => 42);
    const result$ = run(sync(factory))(undefined);
    expect(factory).not.toHaveBeenCalled();
    result$.subscribe();
    result$.subscribe();
    expect(factory).toHaveBeenCalledTimes(2);
  });

  it("defers construction through suspend", () => {
    const factory = vi.fn(() => succeed(1));
    const effect = suspend(factory);
    expect(factory).not.toHaveBeenCalled();
    expect(collect(effect)).toEqual([{ kind: "Success", value: 1 }]);
    expect(factory).toHaveBeenCalledTimes(1);
  });

  it("maps success and short-circuits expected failure", () => {
    const project = vi.fn((n: number) => n + 1);
    expect(collect(pipe(succeed(2), map(project)))).toEqual([{ kind: "Success", value: 3 }]);
    expect(collect(pipe(fail("bad"), map(project)))).toEqual([{ kind: "Failure", error: "bad" }]);
    expect(project).toHaveBeenCalledTimes(1);
  });

  it("composes error unions and environment intersections", () => {
    type Inventory = { inventory: number };
    type Payment = { amount: number };
    const reserve: Effect<number, "OutOfStock", Inventory> =
      env => run(env.inventory > 0 ? succeed(env.inventory) : fail("OutOfStock"))(undefined);
    const charge = (count: number): Effect<string, "PaymentFailed", Payment> =>
      env => run(env.amount >= count ? succeed("receipt") : fail("PaymentFailed"))(undefined);
    const program = pipe(reserve, flatMap(charge));
    expectTypeOf(program).toEqualTypeOf<Effect<string, "OutOfStock" | "PaymentFailed", Inventory & Payment>>();
    expect(collect(provide({ inventory: 2, amount: 3 })(program))).toEqual([{ kind: "Success", value: "receipt" }]);
    expect(collect(provide({ inventory: 0, amount: 3 })(program))).toEqual([{ kind: "Failure", error: "OutOfStock" }]);
  });

  it("does not execute a dependent effect after failure", () => {
    const project = vi.fn((n: number) => succeed(n + 1));
    expect(collect(pipe(fail("bad"), flatMap(project)))).toEqual([{ kind: "Failure", error: "bad" }]);
    expect(project).not.toHaveBeenCalled();
  });

  it("recovers expected failure and narrows its error type", () => {
    const program = pipe(fail("bad"), recover((_: "bad") => succeed(7)));
    expectTypeOf(program).toEqualTypeOf<Effect<number>>();
    expect(collect(program)).toEqual([{ kind: "Success", value: 7 }]);
    expect(collect(pipe(fail("bad"), mapError((e: string) => e.toUpperCase())))).toEqual([{ kind: "Failure", error: "BAD" }]);
    expect(collect(provide({ n: 2 })(access((env: { n: number }) => env.n)))).toEqual([{ kind: "Success", value: 2 }]);
  });

  it("rejects empty and multi-value sources as defects", () => {
    for (const source of [EMPTY, of(1, 2)]) {
      const next = vi.fn(), error = vi.fn(), complete = vi.fn();
      run(fromObservable(() => source))(undefined).subscribe({ next, error, complete });
      expect(next).not.toHaveBeenCalled();
      expect(error).toHaveBeenCalledTimes(1);
      expect(complete).not.toHaveBeenCalled();
    }
  });

  it("distinguishes defects from explicitly mapped source failures", () => {
    const defect = new Error("defect");
    const handler = vi.fn(() => succeed(0));
    const error = vi.fn();
    run(pipe(sync(() => { throw defect; }), recover(handler)))(undefined).subscribe({ error });
    expect(error).toHaveBeenCalledWith(defect);
    expect(handler).not.toHaveBeenCalled();
    expect(collect(tryObservable(() => "Offline" as const)(() => throwError(() => defect))))
      .toEqual([{ kind: "Failure", error: "Offline" }]);
  });

  it("retries expected failures only", () => {
    let attempts = 0;
    const effect = suspend(() => ++attempts < 3 ? fail("busy") : succeed(9));
    expect(collect(pipe(effect, retryExpected(2)))).toEqual([{ kind: "Success", value: 9 }]);
    expect(attempts).toBe(3);
    const defect = vi.fn(() => { throw new Error("defect"); });
    run(pipe(sync(defect), retryExpected(3)))(undefined).subscribe({ error: () => {} });
    expect(defect).toHaveBeenCalledTimes(1);
    expect(collect(pipe(fail("busy"), retryExpected(2)))).toEqual([{ kind: "Failure", error: "busy" }]);
  });

  it("cancels scheduled work on unsubscribe and emits no terminal notification", () => {
    vi.useFakeTimers();
    try {
      const teardown = vi.fn(), next = vi.fn(), complete = vi.fn(), error = vi.fn();
      const effect = fromObservable(() => new Observable<number>(subscriber => {
        const id = setTimeout(() => { subscriber.next(1); subscriber.complete(); }, 100);
        return () => { clearTimeout(id); teardown(); };
      }));
      const subscription = run(effect)(undefined).subscribe({ next, complete, error });
      subscription.unsubscribe();
      vi.advanceTimersByTime(200);
      expect(teardown).toHaveBeenCalledTimes(1);
      expect(next).not.toHaveBeenCalled();
      expect(complete).not.toHaveBeenCalled();
      expect(error).not.toHaveBeenCalled();
    } finally { vi.useRealTimers(); }
  });

  it("uses RxJS timeout and switchMap policies", () => {
    vi.useFakeTimers();
    try {
      const timeoutResult = vi.fn();
      run(pipe(fromObservable(() => NEVER), timeout(50, "TimedOut")))(undefined).subscribe(timeoutResult);
      vi.advanceTimersByTime(50);
      expect(timeoutResult).toHaveBeenCalledWith({ kind: "Failure", error: "TimedOut" });
      const counts$ = new Subject<number>();
      const next = vi.fn();
      const subscription = counts$.pipe(switchMap(n =>
        run(fromObservable(() => timer(100).pipe(rxMap(() => n))))(undefined)
      )).subscribe(next);
      counts$.next(1);
      vi.advanceTimersByTime(50);
      counts$.next(2);
      vi.advanceTimersByTime(100);
      expect(next.mock.calls).toEqual([[{ kind: "Success", value: 2 }]]);
      subscription.unsubscribe();
    } finally { vi.useRealTimers(); }
  });
});
