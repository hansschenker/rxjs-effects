import { describe, it, expect, expectTypeOf, vi } from "vitest";
import { Observable, of, timer, map as rxMap, Subject } from "rxjs";
import { TestScheduler } from "rxjs/testing";
import { all, race, coordinate, access, fail, succeed, run, fromObservable, type Effect } from "../src";

describe("coordination", () => {
  it("infers tuple values, unions failures and intersects environments", () => {
    const a: Effect<number, "A", { a: number }> = env => run(succeed(env.a))(undefined);
    const b: Effect<string, "B", { b: string }> = env => run(succeed(env.b))(undefined);
    const program = all(a, succeed(true), b);
    expectTypeOf(program).toEqualTypeOf<Effect<[number, boolean, string], "A" | "B", { a: number } & { b: string }>>();
    const next = vi.fn();
    run(program)({ a: 2, b: "x" }).subscribe(next);
    expect(next).toHaveBeenCalledWith({ kind: "Success", value: [2, true, "x"] });
  });

  it("handles empty tuple and synchronous fail-fast", () => {
    const next = vi.fn();
    run(all())(undefined).subscribe(next);
    expect(next).toHaveBeenCalledWith({ kind: "Success", value: [] });
    const factory = vi.fn(() => of(1));
    run(all(fail("bad"), fromObservable(factory)))(undefined).subscribe(next);
    expect(factory).not.toHaveBeenCalled();
    expect(next).toHaveBeenLastCalledWith({ kind: "Failure", error: "bad" });
  });

  it("parallel all retains input order", () => {
    const scheduler = new TestScheduler((a, b) => expect(a).toEqual(b));
    scheduler.run(({ cold, expectObservable }) => {
      const a = fromObservable(() => cold("---a|", { a: 1 }));
      const b = fromObservable(() => cold("-b|", { b: "x" }));
      expectObservable(run(all(a,b))(undefined)).toBe("----(r|)", { r: { kind: "Success", value: [1,"x"] } });
    });
  });

  it("cancels losing work on race and failure of all", () => {
    const teardown = vi.fn();
    const pending = fromObservable(() => new Observable<number>(() => teardown));
    const next = vi.fn();
    run(race(pending, succeed(7)))(undefined).subscribe(next);
    expect(teardown).toHaveBeenCalledTimes(1);
    expect(next).toHaveBeenLastCalledWith({ kind: "Success", value: 7 });
    run(all(pending, fail("bad")))(undefined).subscribe(next);
    expect(teardown).toHaveBeenCalledTimes(2);
    expect(next).toHaveBeenLastCalledWith({ kind: "Failure", error: "bad" });
  });

  it("distinguishes the four input policies in virtual time", () => {
    const scheduler = new TestScheduler((a, b) => expect(a).toEqual(b));
    scheduler.run(({ hot, cold, expectObservable }) => {
      const input = hot("a-b-----|", { a: 1, b: 2 });
      const project = (n: number) => fromObservable(() => cold("---(v|)", { v: n }));
      const values = { a: { kind: "Success", value: 1 }, b: { kind: "Success", value: 2 } };
      expectObservable(input.pipe(coordinate({ kind: "Merge" })(project, undefined))).toBe("---a-b--|", values);
      expectObservable(input.pipe(coordinate({ kind: "Concat" })(project, undefined))).toBe("---a--b-|", values);
      expectObservable(input.pipe(coordinate({ kind: "Switch" })(project, undefined))).toBe("-----b--|", values);
      expectObservable(input.pipe(coordinate({ kind: "Exhaust" })(project, undefined))).toBe("---a----|", values);
    });
  });

  it("keeps typed per-input failures local", () => {
    const next = vi.fn();
    of(-1, 1).pipe(coordinate({ kind: "Concat" })(
      (n: number) => n < 0 ? fail("bad") : succeed(n), undefined
    )).subscribe(next);
    expect(next.mock.calls).toEqual([[{kind:"Failure",error:"bad"}],[{kind:"Success",value:1}]]);
  });
});
