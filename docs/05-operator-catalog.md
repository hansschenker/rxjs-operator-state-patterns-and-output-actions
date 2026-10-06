# Operator catalog

Analysis created by SuperGrok.

Each row is a behavioral signature. State transition is what is remembered. Action policies are the ordered actions, including subscribe, unsubscribe, schedule, and cancel. `partition` is listed once; the source catalog listed it under both Join Creation and Transformation.

`map` and `filter` are Count / Index because the callback receives `index`. `throttleTime` is Open / Locked (Phase / Flag), with Replace Latest only for a trailing emit. `mergeMap` has a FIFO queue only when `concurrent` is bounded.


## Creation

| Operator | State transition | Action policies |
|---|---|---|
| `ajax` | Lifecycle / Terminal + Timer / Deadline | Synthesize / Inject + Terminate; subscribe starts the request, unsubscribe cancels it |
| `bindCallback` | Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `bindNodeCallback` | Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `defer` | Lifecycle / Terminal | Synthesize / Inject + Forward; subscribe runs the factory |
| `empty` | Lifecycle / Terminal | Terminate |
| `from` | Count / Index + Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `fromEvent` | Active Registry + Lifecycle / Terminal | Forward; subscribe adds the listener, unsubscribe removes it |
| `fromEventPattern` | Active Registry + Lifecycle / Terminal | Forward; add/remove handler |
| `generate` | Accumulate / Fold + Phase / Flag + Lifecycle / Terminal | Emit Derived State + Terminate |
| `interval` | Count / Index + Timer / Deadline + Lifecycle / Terminal | Synthesize / Inject + Trigger / Sample |
| `of` | Count / Index + Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `range` | Count / Index + Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `throwError` | Lifecycle / Terminal | Terminate |
| `timer` | Timer / Deadline + Count / Index + Lifecycle / Terminal | Synthesize / Inject + Trigger / Sample |
| `iif` | Phase / Flag + Lifecycle / Terminal | Route / Group + Forward; subscribe the chosen source |

## Join Creation

| Operator | State transition | Action policies |
|---|---|---|
| `combineLatest` | Slot Table + Readiness + Lifecycle / Terminal | Combine / Join |
| `concat` | FIFO Queue + Active Selection + Lifecycle / Terminal | Serialize + Forward; subscribe the next source on complete |
| `forkJoin` | Slot Table + Readiness + Phase / Flag + Lifecycle / Terminal | Combine / Join + Terminate |
| `merge` | Active Registry + Lifecycle / Terminal | Merge / Interleave + Forward |
| `partition` | Identity + Count / Index | Route / Group + Gate / Suppress |
| `race` | Active Selection + Lifecycle / Terminal | Current-Wins + Forward; unsubscribe the losers |
| `zip` | FIFO Queue + Slot Table + Readiness + Lifecycle / Terminal | Combine / Join + Serialize |

## Transformation

| Operator | State transition | Action policies |
|---|---|---|
| `buffer` | Buffer / Collect + Window / Session + Lifecycle / Terminal | Batch / Aggregate |
| `bufferCount` | Buffer / Collect + Count / Index + Lifecycle / Terminal | Batch / Aggregate |
| `bufferTime` | Buffer / Collect + Timer / Deadline + Window / Session + Lifecycle / Terminal | Batch / Aggregate; schedule and cancel the window |
| `bufferToggle` | Buffer / Collect + Window / Session + Active Registry + Lifecycle / Terminal | Batch / Aggregate |
| `bufferWhen` | Buffer / Collect + Window / Session + Timer / Deadline + Lifecycle / Terminal | Batch / Aggregate |
| `concatMap` | FIFO Queue + Active Selection + Count / Index + Lifecycle / Terminal | Serialize + Transform + Forward; subscribe the next inner on complete |
| `concatMapTo` | FIFO Queue + Active Selection + Lifecycle / Terminal | Serialize + Synthesize / Inject + Forward |
| `exhaust` | Phase / Flag + Lifecycle / Terminal | Gate / Suppress + Forward |
| `exhaustMap` | Phase / Flag + Count / Index + Lifecycle / Terminal | Gate / Suppress + Transform + Forward; subscribe only when idle |
| `expand` | Active Registry + Lifecycle / Terminal | Merge / Interleave + Transform; subscribe each projected inner |
| `groupBy` | Membership Set + Active Registry + Lifecycle / Terminal | Route / Group + Synthesize / Inject |
| `map` | Count / Index + Lifecycle / Terminal | Transform |
| `mapTo` | Identity + Lifecycle / Terminal | Synthesize / Inject |
| `mergeMap` | Active Registry + FIFO Queue + Count / Index + Lifecycle / Terminal | Merge / Interleave + Transform + Forward; subscribe inners up to concurrency |
| `mergeMapTo` | Active Registry + FIFO Queue + Lifecycle / Terminal | Merge / Interleave + Synthesize / Inject + Forward |
| `mergeScan` | Accumulate / Fold + Active Registry + Lifecycle / Terminal | Emit Derived State + Merge / Interleave + Transform |
| `pairwise` | History / Shift + Phase / Flag + Lifecycle / Terminal | Read Stored State + Combine / Join + Gate / Suppress |
| `pluck` | Identity + Lifecycle / Terminal | Transform |
| `scan` | Accumulate / Fold + Count / Index + Lifecycle / Terminal | Emit Derived State |
| `switchScan` | Accumulate / Fold + Active Selection + Lifecycle / Terminal | Emit Derived State + Current-Wins; unsubscribe the previous inner |
| `switchMap` | Active Selection + Count / Index + Lifecycle / Terminal | Current-Wins + Transform + Forward; unsubscribe old, subscribe new |
| `switchMapTo` | Active Selection + Lifecycle / Terminal | Current-Wins + Synthesize / Inject + Forward; unsubscribe old, subscribe new |
| `window` | Window / Session + Lifecycle / Terminal | Synthesize / Inject + Route / Group |
| `windowCount` | Window / Session + Count / Index + Lifecycle / Terminal | Synthesize / Inject + Route / Group |
| `windowTime` | Window / Session + Timer / Deadline + Lifecycle / Terminal | Synthesize / Inject + Route / Group; schedule window boundaries |
| `windowToggle` | Window / Session + Active Registry + Lifecycle / Terminal | Synthesize / Inject + Route / Group |
| `windowWhen` | Window / Session + Lifecycle / Terminal | Synthesize / Inject + Route / Group |

## Filtering

| Operator | State transition | Action policies |
|---|---|---|
| `audit` | Replace Latest + Timer / Deadline + Phase / Flag + Lifecycle / Terminal | Trigger / Sample |
| `auditTime` | Replace Latest + Timer / Deadline + Phase / Flag + Lifecycle / Terminal | Trigger / Sample; schedule the window once per quiet period |
| `debounce` | Replace Latest + Timer / Deadline + Lifecycle / Terminal | Trigger / Sample; cancel and reschedule the duration selector |
| `debounceTime` | Replace Latest + Timer / Deadline + Lifecycle / Terminal | Trigger / Sample; cancel the previous timer, schedule a new one |
| `distinct` | Membership Set + Lifecycle / Terminal | Gate / Suppress |
| `distinctUntilChanged` | Replace Latest + Lifecycle / Terminal | Gate / Suppress |
| `distinctUntilKeyChanged` | Replace Latest + Lifecycle / Terminal | Gate / Suppress |
| `elementAt` | Count / Index + Lifecycle / Terminal | Gate / Suppress + Synthesize / Inject + Terminate |
| `filter` | Count / Index + Lifecycle / Terminal | Gate / Suppress |
| `first` | Count / Index + Phase / Flag + Lifecycle / Terminal | Gate / Suppress + Synthesize / Inject + Terminate |
| `ignoreElements` | Identity + Lifecycle / Terminal | Gate / Suppress + Terminate |
| `last` | Replace Latest + Phase / Flag + Lifecycle / Terminal | Read Stored State + Synthesize / Inject + Terminate |
| `sample` | Replace Latest + Phase / Flag + Lifecycle / Terminal | Trigger / Sample + Gate / Suppress |
| `sampleTime` | Replace Latest + Timer / Deadline + Phase / Flag + Lifecycle / Terminal | Trigger / Sample |
| `single` | Count / Index + Replace Latest + Lifecycle / Terminal | Gate / Suppress + Read Stored State + Terminate |
| `skip` | Count / Index + Lifecycle / Terminal | Gate / Suppress |
| `skipLast` | Buffer / Collect + Count / Index + Lifecycle / Terminal | Gate / Suppress + Read Stored State |
| `skipUntil` | Phase / Flag + Lifecycle / Terminal | Gate / Suppress + Forward |
| `skipWhile` | Phase / Flag + Count / Index + Lifecycle / Terminal | Gate / Suppress + Forward |
| `take` | Count / Index + Lifecycle / Terminal | Forward + Terminate |
| `takeLast` | Buffer / Collect + Lifecycle / Terminal | Read Stored State + Batch / Aggregate + Terminate |
| `takeUntil` | Phase / Flag + Lifecycle / Terminal | Forward + Terminate; unsubscribe on the notifier |
| `takeWhile` | Phase / Flag + Count / Index + Lifecycle / Terminal | Gate / Suppress + Terminate |
| `throttle` | Phase / Flag + Timer / Deadline + Replace Latest + Lifecycle / Terminal | Forward + Gate / Suppress + Trigger / Sample |
| `throttleTime` | Phase / Flag + Timer / Deadline + Replace Latest + Lifecycle / Terminal | Forward + Gate / Suppress + Trigger / Sample |

## Join

| Operator | State transition | Action policies |
|---|---|---|
| `combineLatestAll` | Active Registry + Slot Table + Readiness + Lifecycle / Terminal | Combine / Join; subscribe each inner |
| `concatAll` | FIFO Queue + Active Selection + Lifecycle / Terminal | Serialize + Forward |
| `exhaustAll` | Phase / Flag + Lifecycle / Terminal | Gate / Suppress + Forward |
| `mergeAll` | Active Registry + FIFO Queue + Lifecycle / Terminal | Merge / Interleave + Forward |
| `switchAll` | Active Selection + Lifecycle / Terminal | Current-Wins + Forward; unsubscribe the previous inner |
| `startWith` | Phase / Flag + Lifecycle / Terminal | Synthesize / Inject + Forward |
| `withLatestFrom` | Slot Table + Readiness + Lifecycle / Terminal | Combine / Join + Trigger / Sample + Gate / Suppress |

## Multicasting

| Operator | State transition | Action policies |
|---|---|---|
| `multicast` | Active Registry + Lifecycle / Terminal | Forward; connect subscribes the source once |
| `publish` | Active Registry + Lifecycle / Terminal | Forward; connect subscribes the source |
| `publishBehavior` | Replace Latest + Active Registry + Lifecycle / Terminal | Read Stored State + Forward |
| `publishLast` | Replace Latest + Phase / Flag + Active Registry + Lifecycle / Terminal | Read Stored State + Terminate |
| `publishReplay` | Buffer / Collect + Active Registry + Lifecycle / Terminal | Read Stored State + Forward |
| `share` | Active Registry + Lifecycle / Terminal | Forward; connect on first subscriber, disconnect at refcount zero |

## Error Handling

| Operator | State transition | Action policies |
|---|---|---|
| `catchError` | Lifecycle / Terminal + Active Selection | Terminate (catch) + Current-Wins + Forward; subscribe the handler source |
| `retry` | Count / Index + Phase / Flag + Lifecycle / Terminal | Forward + Terminate; resubscribe until the count is exhausted |
| `retryWhen` | Active Selection + Count / Index + Lifecycle / Terminal | Forward + Gate / Suppress; resubscribe when the notifier emits |

## Utility

| Operator | State transition | Action policies |
|---|---|---|
| `tap` | Count / Index + Lifecycle / Terminal | Forward; side-effect action, no value change |
| `delay` | FIFO Queue + Timer / Deadline + Lifecycle / Terminal | Delay / Time-Shift; schedule each notification |
| `delayWhen` | FIFO Queue + Timer / Deadline + Lifecycle / Terminal | Delay / Time-Shift; subscribe a duration selector per value |
| `dematerialize` | Identity + Lifecycle / Terminal | Transform + Terminate |
| `materialize` | Identity + Lifecycle / Terminal | Transform |
| `observeOn` | FIFO Queue + Timer / Deadline + Lifecycle / Terminal | Delay / Time-Shift |
| `subscribeOn` | Timer / Deadline + Lifecycle / Terminal | Forward; schedule subscribe and unsubscribe |
| `timeInterval` | Timer / Deadline + Lifecycle / Terminal | Transform |
| `timestamp` | Timer / Deadline + Lifecycle / Terminal | Transform |
| `timeout` | Timer / Deadline + Lifecycle / Terminal | Forward + Terminate; cancel and rearm the deadline on each next |
| `timeoutWith` | Timer / Deadline + Active Selection + Lifecycle / Terminal | Forward + Current-Wins + Terminate; unsubscribe source, subscribe fallback |
| `toArray` | Buffer / Collect + Lifecycle / Terminal | Batch / Aggregate + Terminate |

## Conditional

| Operator | State transition | Action policies |
|---|---|---|
| `defaultIfEmpty` | Phase / Flag + Lifecycle / Terminal | Forward + Synthesize / Inject + Terminate |
| `every` | Accumulate / Fold + Count / Index + Lifecycle / Terminal | Emit Derived State + Terminate |
| `find` | Count / Index + Phase / Flag + Lifecycle / Terminal | Gate / Suppress + Synthesize / Inject + Terminate |
| `findIndex` | Count / Index + Lifecycle / Terminal | Emit Derived State + Synthesize / Inject + Terminate |
| `isEmpty` | Phase / Flag + Lifecycle / Terminal | Emit Derived State + Terminate |

## Aggregate

| Operator | State transition | Action policies |
|---|---|---|
| `count` | Count / Index + Lifecycle / Terminal | Emit Derived State + Terminate |
| `max` | Accumulate / Fold + Phase / Flag + Lifecycle / Terminal | Emit Derived State + Terminate |
| `min` | Accumulate / Fold + Phase / Flag + Lifecycle / Terminal | Emit Derived State + Terminate |
| `reduce` | Accumulate / Fold + Count / Index + Lifecycle / Terminal | Emit Derived State + Terminate |

`scan` and `reduce` share Accumulate / Fold. `scan` emits the derived state on every next. `reduce` emits it once, on complete.
