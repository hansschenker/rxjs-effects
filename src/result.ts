export type Success<A> = { readonly kind: "Success"; readonly value: A };
export type Failure<E> = { readonly kind: "Failure"; readonly error: E };
export type Result<A, E> = Success<A> | Failure<E>;

export const success = <A>(value: A): Success<A> =>
  ({ kind: "Success", value });

export const failure = <const E>(error: E): Failure<E> =>
  ({ kind: "Failure", error });

export const isSuccess = <A, E>(result: Result<A, E>): result is Success<A> =>
  result.kind === "Success";

export const isFailure = <A, E>(result: Result<A, E>): result is Failure<E> =>
  result.kind === "Failure";
