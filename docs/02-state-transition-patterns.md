# State transition patterns

Analysis created by SuperGrok.

A state transition pattern is the memory half of an operator.

\[
T(s, e) = s'
\]

It does not decide what gets emitted. An operator can carry several patterns at once, because its state is usually a product \(S = S_1 \times S_2 \times \cdots \times S_n\).

| ID | Pattern | Rule |
|---|---|---|
| ST-01 | Identity | \(s' = s\). Nothing new is stored. |
| ST-02 | Replace Latest | One slot, overwritten by the newest value or key. |
| ST-03 | Accumulate / Fold | \(s' = f(s, x)\). The reducer is the transition. |
| ST-04 | Count / Index | A counter or emission index advances. |
| ST-05 | Phase / Flag | A small control state: idle/busy, open/locked, skipping/forwarding. |
| ST-06 | History / Shift | A bounded shift register of recent values. |
| ST-07 | Membership Set | A set of seen keys grows or is tested. |
| ST-08 | Buffer / Collect | Values accumulate as data to emit later. |
| ST-09 | FIFO Queue | Pending work or notifications wait in order. |
| ST-10 | Active Registry | Zero or more resources are active together. |
| ST-11 | Active Selection | One current resource is replaced. |
| ST-12 | Slot Table + Readiness | Each input has a slot and a ready bit. |
| ST-13 | Timer / Deadline | A timer is armed, restarted, cancelled, or expires. |
| ST-14 | Window / Session | A region of values is opened, filled, and closed. |
| ST-15 | Lifecycle / Terminal | Active, complete, errored, unsubscribed. Terminal states absorb further events. |

## ST-01 Identity

Nothing new is stored. This is the teaching form of `map`, `filter`, `tap`, and `pluck` if the emission index is ignored. The interesting behavior is on the action side.

## ST-02 Replace Latest

One slot, overwritten. Old contents are discarded, not queued. `distinctUntilChanged` stores the previous value. `debounceTime` stores the value to emit if silence arrives. `sample` stores the value a later tick will read. `last` keeps replacing a candidate and reads it on complete.

## ST-03 Accumulate / Fold

The reducer is the transition. `scan` emits each new accumulator. `reduce`, `max`, `min`, and `every` use the same kind of fold and emit once, on complete or on an early decision. The seed is the initial state.

## ST-04 Count / Index

State is an ordinal. `take` and `skip` count values seen. `count` emits the counter on complete. This is also the hidden state in `map` and `filter`: both callbacks receive a zero-based index. Dropping that counter is a teaching abstraction.

## ST-05 Phase / Flag

A small control state. `exhaustMap` is idle/busy: idle accepts a new inner, busy ignores it. `throttleTime` is Open/Locked, not Replace Latest. `skipUntil` moves from skipping to forwarding and does not move back. `defaultIfEmpty` is a seen flag. This is the pattern that is a classical finite-state machine. The others often are not, because their state space can grow.

## ST-06 History / Shift

A bounded register, not a single slot. `pairwise` is the length-2 case. The first value is gated, because a pair does not exist yet.

## ST-07 Membership Set

The state answers "have I seen this key?" `distinct` suppresses a value whose key is already in the set. The set does not forget. It is unbounded unless something flushes it.

## ST-08 Buffer / Collect

Values accumulate as data to emit later. A flush is an action. The transition only appends or clears. `buffer`, `toArray`, and `takeLast` use it. `skipLast` uses a fixed-size buffer as a delay line. A buffer stores data. A queue stores work.

## ST-09 FIFO Queue

Pending work waits in order. `concat` and `concatMap` keep one active source or inner, and queue the rest. `delay` and `observeOn` queue notifications so order survives the scheduler. `mergeMap` does not have this pattern by default. The queue becomes semantic only when `concurrent` is finite and that limit is reached.

## ST-10 Active Registry

Zero or more resources are active together. `merge` and `mergeMap` add an inner on each source value and remove it on inner complete. `groupBy` keeps a registry of live groups. `share` uses the registry for downstream subscribers. `fromEvent` uses it for the listener.

## ST-11 Active Selection

One designated current resource. A new candidate replaces it. `switchMap` is the pattern. `race` selects the first source to emit. `catchError` moves the selection to a handler. The cancel of the old inner is not part of this transition. It is an action that accompanies the replacement.

## ST-12 Slot Table + Readiness

Each input has a slot and a ready bit. `combineLatest` emits only after every ready bit is set, then on any later update. `forkJoin` uses the same table but waits until every source has completed. `withLatestFrom` fills slots for the secondary sources and lets only the primary trigger a read. `zip` is slots plus per-source queues, because it pairs by index rather than by latest value.

## ST-13 Timer / Deadline

State holds a timer token, a deadline, an active flag. Events are start, restart, cancel, and expire. The machine should store the logical deadline, not a raw timer handle. `debounceTime` restarts the timer on every value. `auditTime` arms a window on source activity and does not restart it. `sampleTime` runs a clock that is not armed by values. `timeout` rearms a deadline on each next.

## ST-14 Window / Session

The state controls a region of values, not just a bag. `window` and `buffer` share the session transition. Overlapping windows combine this pattern with Active Registry. The buffer is the contents. The window is the boundary and the lifetime.

## ST-15 Lifecycle / Terminal

Terminal states are absorbing. No further downstream notifications occur. Every operator has this component. It is named when termination is part of the behavior: `take` completes from the count, `retry` leaves the errored phase by resubscribing, `empty` and `throwError` exist only to enter a terminal state.

## How to read a real operator

Pick the memory, then stop. Do not smuggle the emit rule into the transition. `debounceTime` remembers the latest value and a restartable deadline. Emission is the action that runs when the timer-expiry event arrives. `scan` and `reduce` are both Accumulate / Fold. Only the action policy differs.
