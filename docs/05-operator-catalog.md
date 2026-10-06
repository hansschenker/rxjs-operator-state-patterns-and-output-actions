# Operator catalog

Analysis created by SuperGrok.

Each row is a behavioral signature. State transition is what is remembered. Action policies are the ordered actions, including subscribe, unsubscribe, schedule, and cancel. `partition` is listed once; the source catalog listed it under both Join Creation and Transformation. `exhaust` is the deprecated alias of `exhaustAll` and is listed once, under Join.

`map` and `filter` are Count / Index because the callback receives `index`. `throttleTime` is Open / Locked (Phase / Flag), with Replace Latest only for a trailing emit. `mergeMap` has a FIFO queue only when `concurrent` is bounded.


## Creation

| Operator | State transition | Action policies |
|---|---|---|
| `ajax` | Lifecycle / Terminal + Timer / Deadline | Synthesize / Inject + Terminate; subscribe starts the request, unsubscribe cancels it |
| `bindCallback` | Replace Latest + Phase / Flag + Lifecycle / Terminal | Synthesize / Inject + Read Stored State + Terminate; call once on first subscribe, replay to later subscribers |
| `bindNodeCallback` | Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `defer` | Lifecycle / Terminal | Forward; subscribe the factory result |
| `empty` | Lifecycle / Terminal | Terminate |
| `from` | Count / Index + Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `fromEvent` | Active Registry + Lifecycle / Terminal | Forward; subscribe adds the listener, unsubscribe removes it |
| `fromEventPattern` | Active Registry + Lifecycle / Terminal | Forward; add/remove handler |
| `generate` | Accumulate / Fold + Lifecycle / Terminal | Emit Derived State (or Transform with `resultSelector`) + Terminate |
| `interval` | Count / Index + Timer / Deadline + Lifecycle / Terminal | Synthesize / Inject + Trigger / Sample |
| `of` | Count / Index + Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `range` | Count / Index + Lifecycle / Terminal | Synthesize / Inject + Terminate |
| `throwError` | Lifecycle / Terminal | Terminate |
| `timer` | Timer / Deadline + Count / Index + Lifecycle / Terminal | Emit Derived State + Trigger / Sample + Terminate; completes after `0` unless a period is given |
| `iif` | Lifecycle / Terminal | Forward; evaluate the condition and subscribe the chosen source at subscribe time |

## Join Creation

| Operator | State transition | Action policies |
|---|---|---|
| `combineLatest` | Active Registry + Slot Table + Readiness + Lifecycle / Terminal | Combine / Join + Terminate |
| `concat` | FIFO Queue + Active Selection + Lifecycle / Terminal | Serialize + Forward; subscribe the next source on complete |
| `forkJoin` | Slot Table + Readiness + Phase / Flag + Lifecycle / Terminal | Combine / Join + Terminate |
| `merge` | Active Registry + FIFO Queue + Lifecycle / Terminal | Merge / Interleave + Forward; queue only when `concurrent` is bounded |
| `partition` | Count / Index + Lifecycle / Terminal | Gate / Suppress, applied as two independent `filter` subscriptions of the source |
| `race` | Active Registry + Active Selection + Lifecycle / Terminal | First-Wins + Forward; unsubscribe the losers on the first `next`, forward any earlier complete or error |
| `zip` | FIFO Queue + Slot Table + Readiness + Active Registry + Lifecycle / Terminal | Combine / Join + Terminate; complete when a completed source's queue is empty |

## Transformation

| Operator | State transition | Action policies |
|---|---|---|
| `buffer` | Buffer / Collect + Window / Session + Lifecycle / Terminal | Batch / Aggregate |
| `bufferCount` | Buffer / Collect + Count / Index + Lifecycle / Terminal | Batch / Aggregate |
| `bufferTime` | Buffer / Collect + Timer / Deadline + Window / Session + Lifecycle / Terminal | Batch / Aggregate; schedule and cancel the window |
| `bufferToggle` | Buffer / Collect + Window / Session + Active Registry + Lifecycle / Terminal | Batch / Aggregate |
| `bufferWhen` | Buffer / Collect + Window / Session + Active Selection + Lifecycle / Terminal | Batch / Aggregate; resubscribe the closing notifier after each flush |
| `concatMap` | FIFO Queue + Active Selection + Count / Index + Lifecycle / Terminal | Serialize + Transform + Forward; subscribe the next inner on complete |
| `concatMapTo` | FIFO Queue + Active Selection + Lifecycle / Terminal | Serialize + Synthesize / Inject + Forward |
| `exhaustMap` | Phase / Flag + Count / Index + Lifecycle / Terminal | Gate / Suppress + Transform + Forward; subscribe only when idle |
| `expand` | Active Registry + FIFO Queue + Count / Index + Lifecycle / Terminal | Forward + Transform + Merge / Interleave; emit each value and feed it back to `project`, inners up to `concurrent` |
| `groupBy` | Membership Set + Active Registry + Lifecycle / Terminal | Route / Group + Synthesize / Inject |
| `map` | Count / Index + Lifecycle / Terminal | Transform |
| `mapTo` | Identity + Lifecycle / Terminal | Synthesize / Inject |
| `mergeMap` | Active Registry + FIFO Queue + Count / Index + Lifecycle / Terminal | Merge / Interleave + Transform + Forward; subscribe inners up to concurrency |
| `mergeMapTo` | Active Registry + FIFO Queue + Lifecycle / Terminal | Merge / Interleave + Synthesize / Inject + Forward |
| `mergeScan` | Accumulate / Fold + Active Registry + FIFO Queue + Count / Index + Lifecycle / Terminal | Emit Derived State + Merge / Interleave + Transform |
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
| `debounce` | Replace Latest + Timer / Deadline + Lifecycle / Terminal | Trigger / Sample + Read Stored State on complete; cancel and reschedule the duration selector |
| `debounceTime` | Replace Latest + Timer / Deadline + Lifecycle / Terminal | Trigger / Sample + Read Stored State on complete; cancel the previous timer, schedule a new one |
| `distinct` | Membership Set + Lifecycle / Terminal | Gate / Suppress |
| `distinctUntilChanged` | Replace Latest + Lifecycle / Terminal | Gate / Suppress |
| `distinctUntilKeyChanged` | Replace Latest + Lifecycle / Terminal | Gate / Suppress |
| `elementAt` | Count / Index + Lifecycle / Terminal | Gate / Suppress + Synthesize / Inject + Terminate |
| `filter` | Count / Index + Lifecycle / Terminal | Gate / Suppress |
| `first` | Count / Index + Phase / Flag + Lifecycle / Terminal | Gate / Suppress + Synthesize / Inject + Terminate |
| `ignoreElements` | Identity + Lifecycle / Terminal | Gate / Suppress + Terminate |
| `last` | Replace Latest + Phase / Flag + Count / Index + Lifecycle / Terminal | Read Stored State + Synthesize / Inject + Terminate |
| `sample` | Replace Latest + Phase / Flag + Lifecycle / Terminal | Trigger / Sample + Gate / Suppress |
| `sampleTime` | Replace Latest + Timer / Deadline + Phase / Flag + Lifecycle / Terminal | Trigger / Sample |
| `single` | Count / Index + Replace Latest + Lifecycle / Terminal | Gate / Suppress + Read Stored State + Terminate |
| `skip` | Count / Index + Lifecycle / Terminal | Gate / Suppress |
| `skipLast` | Buffer / Collect + Count / Index + Lifecycle / Terminal | Gate / Suppress + Read Stored State |
| `skipUntil` | Phase / Flag + Lifecycle / Terminal | Gate / Suppress + Forward |
| `skipWhile` | Phase / Flag + Count / Index + Lifecycle / Terminal | Gate / Suppress + Forward |
| `take` | Count / Index + Lifecycle / Terminal | Forward + Terminate |
| `takeLast` | Buffer / Collect + Lifecycle / Terminal | Read Stored State + Batch / Aggregate + Terminate |
| `takeUntil` | Lifecycle / Terminal | Forward + Terminate; on notifier `next`, complete and unsubscribe source and notifier |
| `takeWhile` | Phase / Flag + Count / Index + Lifecycle / Terminal | Gate / Suppress + Terminate |
| `throttle` | Phase / Flag + Timer / Deadline + Replace Latest + Lifecycle / Terminal | Forward + Gate / Suppress + Trigger / Sample |
| `throttleTime` | Phase / Flag + Timer / Deadline + Replace Latest + Lifecycle / Terminal | Forward + Gate / Suppress + Trigger / Sample |

## Join

| Operator | State transition | Action policies |
|---|---|---|
| `combineLatestAll` | Buffer / Collect + Phase / Flag + Active Registry + Slot Table + Readiness + Lifecycle / Terminal | Combine / Join; collect inners, subscribe all of them when the outer completes |
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
| `catchError` | Active Selection + Lifecycle / Terminal | Gate / Suppress (the error) + Current-Wins + Forward; subscribe the handler source |
| `retry` | Count / Index + Phase / Flag + Lifecycle / Terminal | Forward + Terminate; resubscribe until the count is exhausted |
| `retryWhen` | Active Selection + Lifecycle / Terminal | Forward + Gate / Suppress + Terminate; resubscribe on notifier `next`, complete or error with the notifier |

## Utility

| Operator | State transition | Action policies |
|---|---|---|
| `tap` | Identity + Lifecycle / Terminal | Forward; side-effect action, no value change |
| `delay` | FIFO Queue + Timer / Deadline + Lifecycle / Terminal | Delay / Time-Shift; schedule each `next`, forward error immediately, complete after the last pending `next` |
| `delayWhen` | Active Registry + Count / Index + Lifecycle / Terminal | Delay / Time-Shift + Gate / Suppress; one duration per value, emit on its first `next`, drop the value if it completes silently, order not preserved |
| `dematerialize` | Identity + Lifecycle / Terminal | Transform + Terminate |
| `materialize` | Identity + Lifecycle / Terminal | Transform + Terminate; error and complete both become `next(Notification)` then `complete` |
| `observeOn` | FIFO Queue + Timer / Deadline + Lifecycle / Terminal | Delay / Time-Shift |
| `subscribeOn` | Timer / Deadline + Lifecycle / Terminal | Forward; schedule subscribe and unsubscribe |
| `timeInterval` | Replace Latest + Lifecycle / Terminal | Transform + Read Stored State |
| `timestamp` | Identity + Lifecycle / Terminal | Transform |
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
