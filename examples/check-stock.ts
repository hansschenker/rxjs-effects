import { pipe } from "../src";
import { timer, map as rxMap, Subject, switchMap } from "rxjs";
import { type Effect, succeed, fail, flatMap, fromObservable, run } from "../src";

export const checkStock = (count: number): Effect<number, "NegativeCount"> =>
  count < 0 ? fail("NegativeCount") : succeed(count);

type Inventory = { readonly available: number };
type Reservation = { readonly count: number };

const reserveStock = (count: number): Effect<Reservation, "OutOfStock", Inventory> =>
  environment => run(count > environment.available
    ? fail("OutOfStock")
    : succeed({ count }))(undefined);

export const order = (count: number) =>
  pipe(checkStock(count), flatMap(reserveStock));

// Inferred error union and environment requirement.
export const result$ = run(order(10))({ available: 20 });

export const delayedCheck = (count: number) =>
  pipe(fromObservable(() => timer(1000).pipe(rxMap(() => count))), flatMap(checkStock));

export const counts$ = new Subject<number>();
export const latest$ = counts$.pipe(
  switchMap(count => run(delayedCheck(count))(undefined))
);
// Subscribe to latest$ to connect; the inner timer is cancelled on replacement.
// counts$ is live; each subscription to latest$ owns an independent pipeline.
