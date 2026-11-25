# Core API

| Function | Contract |
| --- | --- |
| success / failure | Construct outcome values |
| succeed / fail | Construct immediate single-outcome Effects |
| sync | Defer a synchronous value-producing function |
| suspend | Defer construction of an Effect |
| fromObservable | Lift a single-result Observable factory; preserve defects |
| tryObservable | Explicitly map source errors into typed failures |
| access | Read required dependencies |
| provide | Supply the required environment |
| run | Defer execution and enforce outcome cardinality |
| map | Transform successful values |
| mapError | Transform expected failures |
| flatMap | Compose dependent Effects; union errors and intersect environments |
| recover | Replace expected failures with another Effect |
| timeout | Cancel work at a deadline and return a typed failure |
| retryExpected | Re-execute only expected failures, up to the configured retry count |

`retryExpected(2)` makes at most three attempts. It does not retry defects. `timeout` uses the optional RxJS scheduler and measures time until the validated outcome is delivered.

All operations are function-first. Combinators are composable with RxJS's standalone `pipe`. The `run` boundary returns a normal Observable, so existing RxJS operators can coordinate executions.
