// Value-first functional application, unlike RxJS's standalone pipe(...operators).
// Overloads preserve inferred intermediate contracts without any at call sites.
export function pipe<A>(value: A): A;
export function pipe<A, B>(value: A, step1: (value: A) => B): B;
export function pipe<A, B, C>(value: A, step1: (value: A) => B, step2: (value: B) => C): C;
export function pipe<A, B, C, D>(value: A, step1: (value: A) => B, step2: (value: B) => C, step3: (value: C) => D): D;
export function pipe<A, B, C, D, E>(value: A, step1: (value: A) => B, step2: (value: B) => C, step3: (value: C) => D, step4: (value: D) => E): E;
export function pipe<A, B, C, D, E, F>(value: A, step1: (value: A) => B, step2: (value: B) => C, step3: (value: C) => D, step4: (value: D) => E, step5: (value: E) => F): F;
export function pipe<A, B, C, D, E, F, G>(value: A, step1: (value: A) => B, step2: (value: B) => C, step3: (value: C) => D, step4: (value: D) => E, step5: (value: E) => F, step6: (value: F) => G): G;
export function pipe<A, B, C, D, E, F, G, H>(value: A, step1: (value: A) => B, step2: (value: B) => C, step3: (value: C) => D, step4: (value: D) => E, step5: (value: E) => F, step6: (value: F) => G, step7: (value: G) => H): H;
export function pipe<A, B, C, D, E, F, G, H, I>(value: A, step1: (value: A) => B, step2: (value: B) => C, step3: (value: C) => D, step4: (value: D) => E, step5: (value: E) => F, step6: (value: F) => G, step7: (value: G) => H, step8: (value: H) => I): I;
export function pipe(value: unknown, ...steps: ((value: never) => unknown)[]): unknown {
  return steps.reduce<unknown>((current, step) => step(current as never), value);
}
