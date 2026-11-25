# API

| Operation | Contract |
| --- | --- |
| success / failure | Construct outcome values |
| succeed / fail | Construct one-outcome Effects |
| sync / suspend | Defer evaluation / construction |
| fromObservable | Lift a single-value source factory |
| tryObservable | Explicitly map source errors; cardinality remains a defect |
| access / provide | Read / supply environment |
| run | Cold execution boundary; single-result validation |
| map / mapError | Transform value / failure |
| flatMap / recover | Dependent composition / typed recovery |
| all | Parallel, ordered tuple, fail-fast |
| race | First settled outcome, cancel losers |
| coordinate | Merge, Concat, Switch or Exhaust input policy |
| timeout | Typed failure at deadline |
| retryExpected / retryWith | Bounded typed retry; immediate or delayed |
| createScope | Own subscriptions and finalizers |
| scoped | Await cleanup on settlement; report cancelled cleanup failures |
| acquireUseRelease | Synchronous acquisition, Observable release |
| stream / runStream | Construct / execute typed reactive sequence |
| fromValues / streamFromEffect | Lift values / a single Effect |
| mapStream / mapStreamError | Transform stream values / failure |
| recoverStream | Continue with replacement stream after typed failure |
| flatMapStream | Sequential effects; terminate on typed failure |

Combinators are function-first and work with RxJS pipe. The run and runStream boundaries return standard Observables.
