# Action policies

Analysis created by SuperGrok.

An action policy is the doing half of an operator. The state transition has already answered what is remembered.

\[
G(s, e, s') = a^*
\]

\(a^*\) means zero or more actions. Silence is a legal result. So is a sequence: unsubscribe the old inner, subscribe the new one, then forward its values.

Downstream `next`, `error`, and `complete` are only some of those actions. Subscribe, unsubscribe, schedule, and cancel belong here too. Output is the projection of \(a^*\) onto notifications a subscriber can see.

| ID | Pattern | What it does |
|---|---|---|
| AP-01 | Forward | Pass the notification through unchanged. |
| AP-02 | Transform | Emit \(f(\text{value}, \text{index})\). |
| AP-03 | Gate / Suppress | Emit or drop. |
| AP-04 | Synthesize / Inject | Create a value that is not the current input. |
| AP-05 | Emit Derived State | Emit the state the transition just computed. |
| AP-06 | Read Stored State | Emit something previously stored. |
| AP-07 | Combine / Join | Derive one value from several remembered inputs. |
| AP-08 | Batch / Aggregate | Emit a collection instead of individual values. |
| AP-09 | Route / Group | Choose a destination: group, partition, window. |
| AP-10 | Delay / Time-Shift | Emit later, on a scheduler. |
| AP-11 | Trigger / Sample | Emit a stored value when a timer or notifier fires. |
| AP-12 | Merge / Interleave | Forward from every active producer. |
| AP-13 | Serialize | Forward one producer at a time, in order. |
| AP-14 | Current-Wins | Forward only the selected producer. Cancelling the old one is part of the policy. |
| AP-15 | Terminate | Complete or error, and stop. |

## AP-01 Forward

Pass the notification through unchanged. `take` does this until the count is exhausted. `tap` forwards every notification and adds a side-effect action that does not change the value.

## AP-02 Transform

Emit the projected value, not the input. `map` and `pluck`. `materialize` turns a notification into `next(Notification)`. `dematerialize` turns it back. `timestamp` attaches a clock reading.

## AP-03 Gate / Suppress

Emit or drop. `filter` drops when the predicate fails. `distinct` drops a key already in the set. `exhaustMap` drops a source value while busy. `ignoreElements` drops every `next` and still forwards complete and error. A gate is not a transition: the memory may stay the same while the action list is empty.

## AP-04 Synthesize / Inject

Create a value that did not come from the current input. `of`, `interval`, `startWith`, `defaultIfEmpty`, and the window observable emitted by `window`.

## AP-05 Emit Derived State

Emit the state the transition just computed. `scan` emits each new accumulator. `reduce` and `count` emit once, on complete. The difference from Transform: Transform maps the input, Emit Derived State reads \(s'\).

## AP-06 Read Stored State

Emit something stored earlier, not the value that just arrived. `pairwise` reads the shift register. `last` and `takeLast` read the stored candidate on source complete. `publishBehavior` reads the current value for a new subscriber.

## AP-07 Combine / Join

Derive one output from several remembered inputs. `combineLatest` reads every slot once all are ready. `zip` reads the head of every queue. `withLatestFrom` combines only when the primary fires.

## AP-08 Batch / Aggregate

Emit many stored values as one value. `bufferCount` flushes an array. `toArray` flushes on complete. `window` is not this policy: it emits a window observable and routes values into it.

## AP-09 Route / Group

Choose a destination. The value is unchanged. `partition` sends each value to one of two outputs. `groupBy` routes later values to the existing group.

## AP-10 Delay / Time-Shift

Emit later, on a scheduler. `delay` schedules each notification and uses the queue so order survives the wait. `observeOn` shifts the scheduler without changing the value. `subscribeOn` schedules subscribe and unsubscribe rather than the values.

## AP-11 Trigger / Sample

Emit a stored value when some other event fires. `debounceTime` emits when the silence timer expires. `sampleTime` emits on an independent clock. `throttleTime` uses this only for the optional trailing emit. A trigger without a stored value produces no action.

## AP-12 Merge / Interleave

Forward from every active producer, with no ordering between them. `merge` and `mergeMap`. An inner `next` is forwarded as soon as it arrives, even if another inner is active.

## AP-13 Serialize

Forward one producer at a time, in order. `concat` and `concatMap` subscribe the next source only when the active one completes. The queue is the state. The policy is the decision to refuse overlap.

## AP-14 Current-Wins

Forward only the currently selected producer. A new winner replaces the selection, and the old producer is cancelled.

```text
source next(x)
  unsubscribe(oldInner)
  subscribe(project(x))
inner next(y)
  forward(y)
```

`switchMap` and `race` are this policy. `exhaustMap` is not: it never replaces the active inner.

## AP-15 Terminate

Complete or error, and stop. `take` completes after the nth forwarded value. `throwError` errors immediately. `timeout` errors when the deadline expires. `retry` turns a source error into a resubscribe action until the attempt count is exhausted, and only then runs Terminate.

Terminal state is absorbing, so a terminate action is paired with the lifecycle transition. The policy decides that this event ends the stream. The transition records that no later event may produce another notification.
