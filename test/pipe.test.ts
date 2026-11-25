import { describe, it, expect, expectTypeOf } from "vitest";
import { pipe, succeed, flatMap, map, fail, run, type Effect } from "../src";
describe("typed value-first pipe", () => {
  it("preserves intermediate types and domain errors", () => {
    const program = pipe(succeed(2),
      flatMap((n: number): Effect<string, "bad"> => n > 0 ? succeed(String(n)) : fail("bad")),
      map((s: string) => s.length));
    expectTypeOf(program).toEqualTypeOf<Effect<number, "bad">>();
    const results: unknown[] = [];
    run(program)(undefined).subscribe(result => results.push(result));
    expect(results).toEqual([{ kind: "Success", value: 1 }]);
  });
  it("is general-purpose function application", () => {
    expect(pipe(2, (x: number) => x + 1, String)).toBe("3");
    expect(pipe("value")).toBe("value");
  });
});
