import { describe, it, expect, vi } from "vitest";
import fc from "fast-check";
import { of, concat, throwError, pipe } from "rxjs";
import { TestScheduler } from "rxjs/testing";
import { type Effect, type Result, run, succeed, fail, map, flatMap, retryExpected, retryWith, suspend, tryObservable } from "../src";
const collect = <A,E>(effect: Effect<A,E>) => {
  const result: Result<A,E>[] = [];
  run(effect)(undefined).subscribe(value => result.push(value));
  return result;
};
describe("algebraic laws for pure total projections", () => {
  it("functor identity and composition", () => fc.assert(fc.property(fc.integer(), n => {
    const source = succeed(n), f = (x:number) => x+1, g = (x:number) => x*2;
    expect(collect(pipe(source,map((x:number) => x)))).toEqual(collect(source));
    expect(collect(pipe(source,map(f),map(g)))).toEqual(collect(pipe(source,map((x:number) => g(f(x))))));
  })));
  it("monad identity and associativity, including typed failures", () => fc.assert(fc.property(fc.integer(), n => {
    const f = (x:number) => x < 0 ? fail("Negative") : succeed(x+1);
    const g = (x:number) => succeed(x*2);
    const source = f(n);
    expect(collect(pipe(succeed(n),flatMap(f)))).toEqual(collect(f(n)));
    expect(collect(pipe(source,flatMap((x:number) => succeed(x))))).toEqual(collect(source));
    expect(collect(pipe(source,flatMap(f),flatMap(g)))).toEqual(
      collect(pipe(source,flatMap((x:number) => pipe(f(x),flatMap(g))))));
  })));
  it("trampolines many immediate retries", () => {
    const attempts = vi.fn(() => fail("busy"));
    expect(collect(pipe(suspend(attempts),retryExpected(10000)))).toEqual([{kind:"Failure",error:"busy"}]);
    expect(attempts).toHaveBeenCalledTimes(10001);
  });
  it("uses scheduled retry delays", () => {
    const scheduler = new TestScheduler((a,b) => expect(a).toEqual(b));
    scheduler.run(({expectObservable}) => {
      let count = 0;
      const effect = suspend(() => ++count < 3 ? fail("busy") : succeed(7));
      expectObservable(run(pipe(effect,retryWith({kind:"Delayed",maxRetries:2,milliseconds:10,scheduler})))(undefined))
        .toBe("20ms (a|)", {a:{kind:"Success",value:7}});
    });
  });
  it("discards provisional values on source error; cardinality defects stay defects", () => {
    const project = vi.fn(() => "Offline" as const);
    const effect = tryObservable(project)(() => concat(of(1),throwError(() => new Error("network"))));
    expect(collect(effect)).toEqual([{kind:"Failure",error:"Offline"}]);
    const error = vi.fn();
    run(tryObservable(project)(() => of(1,2)))(undefined).subscribe({error});
    expect(error).toHaveBeenCalledTimes(1);
    expect(project).toHaveBeenCalledTimes(1);
  });
});
