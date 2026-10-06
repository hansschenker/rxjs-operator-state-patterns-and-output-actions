# Evaluation of the operator catalog

Evaluation created by Claude Code (Claude Fable 5.1), 2026-10-06. Every behavioral claim below was checked against the RxJS 7.8.2 source (`src/internal`), not recalled from memory.

Each row of [05-operator-catalog.md](05-operator-catalog.md) was tested three ways:

1. Does the row describe behavior RxJS 7.8 actually has?
2. Does it use the ST and AP terms as [02](02-state-transition-patterns.md) and [03](03-action-policies.md) define them?
3. Is the same behavior tagged the same way in sibling rows?

## Verdict

The catalog is sound as a whole. Of its 111 rows, 84 stand as written at the catalog's level of granularity, 26 are corrected in section 6, and the duplicate `exhaust` row is removed. The three modeling choices announced in the intro are confirmed from source (see section 5). The defects cluster in a few places: `delayWhen`, `tap`, `retryWhen`, `partition`, `timestamp`, `timeInterval`, `catchError`, and `delay` state behavior RxJS does not have. A second group uses a taxonomy term against its own definition. A third group is tagged differently from sibling rows with identical behavior.

| Severity | Meaning | Rows |
|---|---|---|
| Wrong | The row describes behavior RxJS does not have | 8 |
| Misapplied | Real behavior, but the ST/AP term contradicts its definition | 7 |
| Incomplete | A semantically significant component is missing | 10 |
| Inconsistent | The same behavior is tagged differently in another row | 5 groups |

## 1. Wrong

### `delayWhen` — FIFO Queue

Row says: `FIFO Queue + Timer / Deadline`, "subscribe a duration selector per value".

RxJS 7.8: `mergeMap((value, index) => innerFrom(selector(value, index)).pipe(take(1), mapTo(value)))`. Each value is released when its own duration observable first emits. Durations differ per value, so output order can differ from input order. No queue exists. The pending state is one duration subscription per in-flight value, which is Active Registry. The selector receives `index`. A duration that completes without emitting drops the value (`take(1)` on an empty inner).

[06-operator-examples.md](06-operator-examples.md) repeats the error: "order is kept". It is kept in that example only because every duration is the same `interval(100)`.

### `tap` — Count / Index

Row says: `Count / Index`.

RxJS 7.8: `tap(observerOrNext?: Partial<TapObserver<T>> | ((value: T) => void))`. No index is passed to any tap callback. By the catalog's own criterion ("Count / Index because the callback receives `index`") `tap` is Identity. ST-01 in 02 also lists `tap` among operators that are Identity "if the emission index is ignored". There is no index to ignore.

### `retryWhen` — Count / Index

Row says: `Active Selection + Count / Index`.

RxJS 7.8: `retryWhen` owns an error `Subject` and one notifier subscription. It keeps no counter. Counting, when it happens, lives in user code inside the notifier. The row also omits Terminate: when the notifier completes the result completes, when the notifier errors the result errors.

### `partition` — Route / Group, and the state column

Row says: `Identity + Count / Index`, `Route / Group + Gate / Suppress`.

RxJS 7.8: `[filter(predicate)(innerFrom(source)), filter(not(predicate))(innerFrom(source))]`. Two independent subscriptions to the source, each a plain `filter`. No value is routed anywhere. A cold source runs twice and its side effects run twice. Route / Group describes the observable behavior only when the source is hot or shared.

The state column contradicts itself. Identity means nothing is stored. Count / Index means a counter is stored. The predicate receives `index`, so Count / Index is the correct one. This is also the only row in the catalog without Lifecycle / Terminal, although 02 states that every operator has that component.

### `timestamp` — Timer / Deadline

Row says: `Timer / Deadline`.

RxJS 7.8: `map((value) => ({ value, timestamp: timestampProvider.now() }))`. A clock is read. No timer is armed, restarted, cancelled, or expires, which is how ST-13 defines the pattern. State is Identity.

### `timeInterval` — Timer / Deadline

Row says: `Timer / Deadline`.

RxJS 7.8: stores `last = scheduler.now()`, and on each value emits `now - last` and overwrites `last`. That is Replace Latest, read by the action. No timer exists.

### `catchError` — "Terminate (catch)"

Row says: `Terminate (catch) + Current-Wins + Forward`.

RxJS 7.8: the operator exists to stop a terminate from reaching downstream. The source error is consumed, the selector's observable becomes the current producer, and its notifications are forwarded. Downstream terminates only if the selector throws or the handler source itself terminates. The consumed error is Gate / Suppress, not Terminate.

### `delay` — "schedule each notification"

Row says: "schedule each notification".

RxJS 7.8: `delay(due) = delayWhen(() => timer(due, scheduler))`, built on `mergeMap`. Only `next` is delayed. A source error is forwarded immediately. `complete` is not scheduled; it waits until the last pending `next` has been released. The row's Delay / Time-Shift policy is correct for values and wrong for errors.

## 2. Misapplied

| Operator | Term | Why it does not fit |
|---|---|---|
| `zip` | Serialize | AP-13 means "forward one producer at a time, subscribe the next on complete". `zip` subscribes all sources at once and pairs by index. Combine / Join is already the right policy; Serialize adds a false claim. |
| `iif` | Phase / Flag, Route / Group | `iif` is `defer(() => condition() ? a : b)`. The condition is evaluated once at subscribe and not stored, so there is no phase. AP-09 routes a value to a destination; `iif` picks a source. The row should match `defer`. |
| `defer` | Synthesize / Inject | `defer` creates no value. The factory returns a source whose notifications are forwarded unchanged. The action is Subscribe then Forward. |
| `generate` | Phase / Flag | The `condition` is re-evaluated each step and not stored. With a `resultSelector` the emitted value is `f(state)`, which is Transform, not Emit Derived State. |
| `bufferWhen` | Timer / Deadline | The closing selector returns an arbitrary observable, and `buffer`, which is also notifier-driven, carries no Timer / Deadline. One closing notifier is active at a time and is resubscribed on each close, which is Active Selection. |
| `takeUntil` | Phase / Flag | There is no phase. The notifier's first `next` is a Terminate event. The action text "unsubscribe on the notifier" should read "complete; unsubscribe source and notifier". |
| `race` | Current-Wins | AP-14 says "a new winner replaces the selection". `race` selects once, on the first `next`, and never replaces. Before a winner exists, a `complete` or `error` from any source is forwarded and ends the race. "First-Wins" is the accurate reading; AP-14 lists `race` as an example, so the definition in 03 should be widened or the row annotated. |

`interval`, `range`, and `timer` emit their counter. By AP-05 ("reads \(s'\)") that is Emit Derived State, not Synthesize / Inject. This is a nit, since the emitted value does not come from any input either way.

## 3. Incomplete

| Operator | Missing | RxJS 7.8 |
|---|---|---|
| `timer` | Terminate | `timer(due)` without a period emits `0` and completes. Only `timer(due, period)` runs on. |
| `combineLatestAll` | Buffer / Collect + Phase / Flag | `joinAllInternals` is `toArray()` then `mergeMap(sources => combineLatest(sources))`. No inner is subscribed until the outer completes; inners are collected first. "subscribe each inner" hides the wait. The 06 example output is right but its comment omits this. |
| `expand` | Forward, Count / Index, FIFO Queue | Every value, source or inner, is forwarded downstream and also fed to `project`. `project` receives `(value, index)`. `expand` takes `concurrent`, so the bounded-queue caveat from `mergeMap` applies. |
| `mergeScan` | Count / Index, FIFO Queue | `accumulator(acc, value, index)`. Takes `concurrent` (default `Infinity`), same caveat. |
| `last` | Count / Index | `predicate(value, index, source)`. `first` is tagged Count / Index for the same signature. |
| `materialize` | Terminate | After wrapping `error` or `complete` into a `Notification`, it emits `complete`. A source error becomes `next(ErrorNotification)` then `complete`. That is a Terminate decision the operator makes, and it changes the terminal kind. |
| `bindCallback`, `bindNodeCallback` | Replace Latest + Phase / Flag, Read Stored State | Each returned observable holds an `AsyncSubject` and an `uninitialized` flag. The wrapped function runs once, on first subscribe. Later subscribers replay the cached result and do not call the function again. |
| `merge` | FIFO Queue caveat | `merge(...sources, concurrent)` has the same bounded queue as `mergeMap` and `mergeAll`. The `mergeAll` row carries it; the `merge` row does not. |
| `debounce`, `debounceTime` | Read Stored State on complete | On source complete both operators emit the pending value, then complete. `audit` and `throttle` behave differently here, so the flush is worth naming. |
| `share` | reset semantics | Defaults are `resetOnError`, `resetOnComplete`, `resetOnRefCountZero` all `true`. The source connection is rebuilt after a terminal, so Terminal is absorbing per subscriber but not for the shared connection. `connector` (which yields `shareReplay`) is omitted. |

Smaller omissions, listed once: `bufferCount` with `startBufferEvery`, `bufferTime` with `bufferCreationInterval`, `windowCount` with `startWindowEvery`, and `windowTime` with `windowCreationInterval` all produce overlapping regions and therefore carry Active Registry, as `bufferToggle` and `windowToggle` already do. `publishReplay` with `windowTime` carries Timer / Deadline. `retry` with `delay` carries Timer / Deadline and with `resetOnSuccess` resets its counter.

## 4. Inconsistent

**Terminate.** `forkJoin` and `toArray` list Terminate. `combineLatest` and `zip` do not, although `combineLatest` decides completion by counting its live sources down to zero, and `zip` completes when any completed source's queue drains. Either both groups name the decision or neither does. ST-15 in 02 gives the rule: name it when the operator decides termination. Both groups decide.

Correction, 2026-10-06: an earlier version of this paragraph said `combineLatest` completes immediately when a source completes without emitting. It does not. That short-circuit belongs to `forkJoin`. `combineLatest` stays silent and completes only when every source has completed. See [12](12-join-family-step-tables.md), rows CL4 and FJ4. The Terminate tag on the row stands; the reason given for it was wrong.

**Active Registry.** `merge`, `combineLatestAll`, `bufferToggle`, and `groupBy` list Active Registry. `combineLatest`, `forkJoin`, `zip`, and `race` subscribe all their sources concurrently and do not list it. ST-10 ("zero or more resources are active together") covers all of them.

**Count / Index.** Tagged on `tap`, which has no index. Missing on `last`, `expand`, `mergeScan`, `switchScan`, and `delayWhen`, whose callbacks all receive one. The intro gives the rule; the rows apply it unevenly.

**Duplicates.** `exhaust` (Transformation) and `exhaustAll` (Join) are the same operator with identical rows; `exhaust` is the deprecated alias. The intro deduplicates `partition` but not this pair.

**`defer` and `iif`.** `iif` is implemented as `defer`. The two rows should carry the same patterns and policies.

## 5. Confirmed from source

The intro's three modeling choices hold:

- `map` and `filter` receive `(value, index)`. Count / Index is correct.
- `throttle` stores `sendValue = value` on every source `next`, regardless of configuration. The slot becomes observable only when `trailing` is on, so "Replace Latest only for a trailing emit" is correct at the semantic level and conservative at the implementation level.
- `mergeInternals` pushes to its buffer only when `active >= concurrent`. The FIFO queue is semantic only for a bounded `concurrent`.

Also confirmed: `audit` emits on duration `next` and drops the pending value when the duration completes silently; `race` picks its winner on the first `next` only; `scan` without a seed emits the first value as the seed; `skipLast` is a delay line; `single` reads its stored candidate on complete and errors on a second match; `withLatestFrom` ignores completion of the secondary sources.

## 6. Corrected rows

Paste-ready replacements in the catalog's format. Applied to the catalog on 2026-10-06.

| Operator | State transition | Action policies |
|---|---|---|
| `bindCallback` | Replace Latest + Phase / Flag + Lifecycle / Terminal | Synthesize / Inject + Read Stored State + Terminate; call once on first subscribe, replay to later subscribers |
| `defer` | Lifecycle / Terminal | Forward; subscribe the factory result |
| `generate` | Accumulate / Fold + Lifecycle / Terminal | Emit Derived State (or Transform with `resultSelector`) + Terminate |
| `timer` | Timer / Deadline + Count / Index + Lifecycle / Terminal | Emit Derived State + Trigger / Sample + Terminate; completes after `0` unless a period is given |
| `iif` | Lifecycle / Terminal | Forward; evaluate the condition and subscribe the chosen source at subscribe time |
| `combineLatest` | Active Registry + Slot Table + Readiness + Lifecycle / Terminal | Combine / Join + Terminate |
| `merge` | Active Registry + FIFO Queue + Lifecycle / Terminal | Merge / Interleave + Forward; queue only when `concurrent` is bounded |
| `partition` | Count / Index + Lifecycle / Terminal | Gate / Suppress, applied as two independent `filter` subscriptions of the source |
| `race` | Active Registry + Active Selection + Lifecycle / Terminal | First-Wins + Forward; unsubscribe the losers on the first `next`, forward any earlier complete or error |
| `zip` | FIFO Queue + Slot Table + Readiness + Active Registry + Lifecycle / Terminal | Combine / Join + Terminate; complete when a completed source's queue is empty |
| `bufferWhen` | Buffer / Collect + Window / Session + Active Selection + Lifecycle / Terminal | Batch / Aggregate; resubscribe the closing notifier after each flush |
| `expand` | Active Registry + FIFO Queue + Count / Index + Lifecycle / Terminal | Forward + Transform + Merge / Interleave; emit each value and feed it back to `project`, inners up to `concurrent` |
| `mergeScan` | Accumulate / Fold + Active Registry + FIFO Queue + Count / Index + Lifecycle / Terminal | Emit Derived State + Merge / Interleave + Transform |
| `debounce` | Replace Latest + Timer / Deadline + Lifecycle / Terminal | Trigger / Sample + Read Stored State on complete; cancel and reschedule the duration selector |
| `debounceTime` | Replace Latest + Timer / Deadline + Lifecycle / Terminal | Trigger / Sample + Read Stored State on complete; cancel the previous timer, schedule a new one |
| `last` | Replace Latest + Phase / Flag + Count / Index + Lifecycle / Terminal | Read Stored State + Synthesize / Inject + Terminate |
| `takeUntil` | Lifecycle / Terminal | Forward + Terminate; on notifier `next`, complete and unsubscribe source and notifier |
| `combineLatestAll` | Buffer / Collect + Phase / Flag + Active Registry + Slot Table + Readiness + Lifecycle / Terminal | Combine / Join; collect inners, subscribe all of them when the outer completes |
| `catchError` | Active Selection + Lifecycle / Terminal | Gate / Suppress (the error) + Current-Wins + Forward; subscribe the handler source |
| `retryWhen` | Active Selection + Lifecycle / Terminal | Forward + Gate / Suppress + Terminate; resubscribe on notifier `next`, complete or error with the notifier |
| `tap` | Identity + Lifecycle / Terminal | Forward; side-effect action, no value change |
| `delay` | FIFO Queue + Timer / Deadline + Lifecycle / Terminal | Delay / Time-Shift; schedule each `next`, forward error immediately, complete after the last pending `next` |
| `delayWhen` | Active Registry + Count / Index + Lifecycle / Terminal | Delay / Time-Shift + Gate / Suppress; one duration per value, emit on its first `next`, drop the value if it completes silently, order not preserved |
| `materialize` | Identity + Lifecycle / Terminal | Transform + Terminate; error and complete both become `next(Notification)` then `complete` |
| `timeInterval` | Replace Latest + Lifecycle / Terminal | Transform + Read Stored State |
| `timestamp` | Identity + Lifecycle / Terminal | Transform |

The duplicate `exhaust` row was removed; `exhaustAll` remains under Join.

## 7. Follow-up edits in companion docs

Applied on 2026-10-06.

- [02](02-state-transition-patterns.md), ST-01: `tap` and `pluck` are now the exact Identity cases, since their callbacks receive no index. `map` and `filter` remain the teaching cases.
- [03](03-action-policies.md), AP-09: the prose now states that `partition` returns two `filter` observables with independent subscriptions, so it routes only when the source is shared. The summary table row says the same in short.
- [03](03-action-policies.md), AP-14: widened rather than split. The definition now names two selection rules, Latest-wins (`switchMap`, `switchAll`, `switchScan`, `timeoutWith`) and First-Wins (`race`), and notes that an early complete or error ends a race. The summary table row names both.
- [06](06-operator-examples.md), `delayWhen`: the comment now says order is kept in that example only because every duration is `interval(100)`, and adds that different durations can reorder and a silent completion drops the value.
- [06](06-operator-examples.md), `combineLatestAll`: a comment line now states that inners are collected until the outer completes, then subscribed together.
