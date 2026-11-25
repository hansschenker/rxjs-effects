import { pipe } from "../src";
import { describe, it, expect, vi } from "vitest";
import { of} from "rxjs";
import { success, failure, stream, runStream, fromValues, streamFromEffect, mapStream, mapStreamError, recoverStream, flatMapStream, fail, succeed, type Stream } from "../src";
const collect = <A,E>(source: Stream<A,E>) => {
  const values: unknown[] = [];
  runStream(source)(undefined).subscribe(value => values.push(value));
  return values;
};
describe("typed streams", () => {
  it("allows zero or more successes", () => {
    expect(collect(fromValues(() => of(1,2,3)))).toEqual([success(1),success(2),success(3)]);
    expect(collect(fromValues(() => of()))).toEqual([]);
  });
  it("terminates on first typed failure", () => {
    const project = vi.fn((n:number) => n*2);
    const source = stream(() => of(success(1),failure("bad"),success(2)));
    expect(collect(pipe(source,mapStream(project)))).toEqual([success(2),failure("bad")]);
    expect(project).toHaveBeenCalledTimes(1);
  });
  it("recovers with a replacement sequence and maps errors", () => {
    const source = stream(() => of(success(1),failure("bad"),success(2)));
    expect(collect(pipe(source,recoverStream((_: "bad") => fromValues(() => of(3,4))))))
      .toEqual([success(1),success(3),success(4)]);
    expect(collect(pipe(source,mapStreamError((e:string) => e.toUpperCase())))).toEqual([success(1),failure("BAD")]);
  });
  it("sequential composition stops at first inner typed failure", () => {
    const project = vi.fn((n:number) => n === 2 ? fail("bad") : succeed(n*10));
    expect(collect(pipe(fromValues(() => of(1,2,3)),flatMapStream(project))))
      .toEqual([success(10),failure("bad")]);
    expect(project).toHaveBeenCalledTimes(2);
    expect(collect(streamFromEffect(succeed(1)))).toEqual([success(1)]);
  });
});
