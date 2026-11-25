# Execution contract

## Effect and Stream

An Effect validates exactly one outcome with RxJS single(); it waits for completion before publishing the outcome. A source that never completes can wait indefinitely. Empty and multi-value sources are defects. A Stream is a distinct object with an open function: zero to many successes, then optionally one Failure followed by completion.

Expected failures are ordinary typed packages; unexpected exceptions use the RxJS error channel. Unsubscription stops notifications and triggers teardown. No underlying I/O is automatically cancellable unless the source implements it. Externally supplied hot sources retain their own sharing behavior.

## Composition

On Success, flatMap subscribes to the dependent Effect; on Failure, it propagates the package. Error types combine by union; object environment requirements combine by intersection. recover handles expected failures only. tryObservable explicitly translates source errors; it does not translate cardinality defects.

## Coordination

all subscribes in input order with overlap, retains result order, and stops at the first failure or defect. A synchronous early failure can skip later subscriptions. all() succeeds with an empty tuple. race selects the first settled outcome including expected failure. Both unsubscribe pending children. Synchronous ties favor source order.

coordinate uses a discriminated policy: Merge, Concat, Switch, Exhaust. It emits a typed result per completed input; expected failure does not terminate the surrounding input stream. Stream operators instead terminate after a typed failure. flatMapStream is sequential: an outer failure is queued after earlier successes; an inner failure terminates immediately and drops pending work.

## Timing

timeout measures time to a validated outcome; a timeout unsubscribes upstream and produces the configured failure. retryExpected and retryWith retry only expected failures. Immediate retries use queueScheduler; delayed retries use timer and optionally a supplied scheduler. No global stack-safety guarantee is made.

## Resource ownership

createScope owns subscriptions and a LIFO list of Observable finalizers. close() is an explicit eager, idempotent lifetime operation. It cancels owned subscriptions, executes every finalizer even if another fails, and replays a report of cleanup errors. A nonterminating finalizer prevents cleanup completion. New finalizers cannot be registered after closing starts.

scoped waits for cleanup before delivering an outcome or defect. It introduces an explicit asapScheduler settlement boundary so use-source teardown finishes before normal cleanup starts. On cancellation, cleanup continues independently; errors are sent to the required cancellation reporter, not a closed subscriber. On normal cleanup failure, the defect channel carries an AggregateError (including the original defect when present).

acquireUseRelease supports synchronous acquisition, scoped use and Observable-based release. Register release immediately after acquisition; do not acquire resources asynchronously through this helper. It is not a complete structured-concurrency/fiber runtime.

## Sharing and backpressure

No implicit multicast is introduced except the scope cleanup report. Sharing an execution is an explicit choice at the RxJS boundary. The library does not add a general pull-based demand protocol to RxJS.
