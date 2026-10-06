# Operational tests for the action policy patterns

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Companion to [03-action-policies.md](03-action-policies.md) and the mirror of [08](08-state-transition-pattern-tests.md).

The runnable suite is [tests/action-policy-patterns.test.ts](../tests/action-policy-patterns.test.ts). Run it with `npm test`. Result on 2026-10-06: 15 tests, 15 passing, against RxJS 7.8.2 under Vitest 5.0.3.

## What is different about testing actions

A state transition is hidden; a test can only infer it from behavior. An action is the behavior. That makes the action tests more direct, but it widens what must be observed, because an action policy is more than the downstream notifications:

- **Value and kind.** What left, and whether it was `next`, `error`, or `complete`.
- **Frame.** When it left, relative to the input that caused it, or to no input at all.
- **Subscription marbles.** Which producers were subscribed, when, and when they were cancelled. This is the only way to see Serialize, Merge / Interleave, Current-Wins, and the teardown half of Terminate.
- **Side-effect probes.** A counter inside `tap` shows that an action ran even though the stream did not change.

Each test keeps the same shape as in 08: a property, one operator that satisfies it, and one neighbouring policy's operator that fails it. The neighbour is what makes the test discriminate.

## Summary

| ID | Pattern | Observable property | Passes | Fails |
|---|---|---|---|---|
| AP-01 | Forward | Same value, same kind, same frame for every notification | `tap` | `map` |
| AP-02 | Transform | Output is a function of the current value and index only | `map` | `scan` |
| AP-03 | Gate / Suppress | Each input is forwarded unchanged or dropped, and the gate reopens | `filter` | `takeWhile` |
| AP-04 | Synthesize / Inject | A value appears that no input event caused | `defaultIfEmpty`, `startWith` | `map` |
| AP-05 | Emit Derived State | The emitted value is the reducer's new state, never an input | `scan`, `reduce` | `last` |
| AP-06 | Read Stored State | A value captured earlier is emitted on a later event | `last`, `shareReplay` | `tap` |
| AP-07 | Combine / Join | One output is derived from inputs on different sources | `withLatestFrom` | `merge` |
| AP-08 | Batch / Aggregate | Many inputs leave as one collection when the batch closes | `bufferCount` | `windowCount` |
| AP-09 | Route / Group | Every value reaches a destination chosen by key, unchanged | `groupBy` | `filter` |
| AP-10 | Delay / Time-Shift | Every value survives, order and spacing kept, frames shifted by a constant | `delay` | `debounceTime` |
| AP-11 | Trigger / Sample | Emission at the trigger's frame with the stored value; an empty trigger emits nothing | `sample` | `delay` |
| AP-12 | Merge / Interleave | Values from every active producer leave as they arrive | `merge` | `concat` |
| AP-13 | Serialize | One producer at a time, in order, nothing lost | `concatMap` | `mergeMap`, `switchMap` |
| AP-14 | Current-Wins | Only the selected producer is forwarded; the others are cancelled | `race`, `switchMap` | `merge` |
| AP-15 | Terminate | The stream ends at a frame the source did not end, and the source is unsubscribed | `take`, `timeout` | `filter` |

## AP-01 Forward

Property. Every notification leaves with the value, kind, and frame it arrived with. A side effect may run, but the stream is untouched.

```text
a-b-#   tap(() => seen++)         ->  a-b-#       seen = 2
a-b-#   map(x => x.toUpperCase())  ->  A-B-#
```

The error passes through `tap` as an error. `map` keeps the frames and changes the values, so it is Transform.

## AP-02 Transform

Property. The output for a given value and index is the same whatever came before.

```text
map((x, i) => x + i)   ab|  ->  a0 b1      cb|  ->  c0 b1      second outputs equal
scan(concat, "")       ab|  ->  a  ab      cb|  ->  c  cb      second outputs differ
```

`scan` answers `b` with the history folded in. That is Emit Derived State.

## AP-03 Gate / Suppress

Property. Each input is either forwarded unchanged at its own frame or dropped. A drop does not close the gate.

```text
abcb|   filter(x => x !== 'b')     ->  a-c-|
abcb|   takeWhile(x => x !== 'b')  ->  a|
```

Both `b`s are dropped by `filter`, and `c` between them still passes. `takeWhile` drops the first `b` by ending the stream, so `c` never gets its turn. One is a gate, the other is Terminate.

## AP-04 Synthesize / Inject

Property. A value appears that no input event caused.

```text
---|   defaultIfEmpty('d')  ->  ---(d|)
-a|    startWith('s')       ->  sa|
---|   map(() => 'd')       ->  ---|
```

`defaultIfEmpty` emits on an empty source. `startWith` emits at frame 0, before any input exists. `map` with a constant looks like injection but needs an input to fire, so on an empty source it produces nothing.

## AP-05 Emit Derived State

Property. The emitted value is the reducer's new state at the frame it was computed. It was never an input.

```text
1 2 3 |   scan(+, 10)    ->  11 13 16
1 2 3 |   reduce(+, 10)  ->  ---(16|)
1 2 3 |   last()         ->  ---(3|)
```

`scan` and `reduce` emit the same derived state on different schedules, which is the cleanest demonstration that state and policy are separate. `last` emits `3`, an input that was stored. That is Read Stored State.

## AP-06 Read Stored State

Property. A value captured at one frame is emitted at a later frame, in response to a different event.

```text
ab--|        last()                    ->  ----(b|)
hot ab---|   shareReplay(1), late sub at frame 3  ->  ---b-|
ab--|        tap                       ->  ab--|
```

`last` reads `b` back on complete, three frames after it arrived. `shareReplay(1)` hands the stored `b` to a subscriber who arrived after it. `tap` emits `b` at frame 1 and never again.

## AP-07 Combine / Join

Property. One output is derived from inputs that arrived on different sources at different frames.

```text
primary     --a-b|
secondary   x----|
withLatestFrom   --p-q|     p = [a, x]   q = [b, x]
merge            x-a-b|
```

`merge` forwards every input on its own. Nothing is joined.

## AP-08 Batch / Aggregate

Property. Several inputs leave as one collection, emitted when the batch closes.

```text
abcd|   bufferCount(2)              ->  -x-y|      x = [a, b]   y = [c, d]
abcd|   windowCount(2), map(_ => w) ->  ww-w|
```

`bufferCount` emits at frame 1, after `b` closed the first batch, and the array holds the values. `windowCount` emits a destination at frame 0, before any value has arrived in it, and again whenever one opens. The values never appear on the main output. That is Route / Group, which [03](03-action-policies.md) is careful to keep apart from batching.

## AP-09 Route / Group

Property. Every value reaches a destination chosen by its key, unchanged. The main output emits one destination per distinct key.

```text
abab|   groupBy(x => x), map(g => g.key)                          ->  ab--|
abab|   groupBy(x => x), mergeMap(g => g.pipe(toArray(), join))   ->  ----(xy|)   x = aa   y = bb
abab|   filter(x => x === 'a')                                    ->  a-a-|
```

Two keys, two destinations, each holding exactly its own values. `filter` keeps the `a`s and loses the `b`s. Dropping is a gate; routing loses nothing.

## AP-10 Delay / Time-Shift

Property. Every value survives, with its order and spacing, and every frame is shifted by the same constant.

```text
ab--|   delay(3)         ->  ---a(b|)
ab--|   debounceTime(3)  ->  ----(b|)
```

Both emit later. Only `delay` keeps `a`, and keeps `b` one frame behind it as it arrived.

## AP-11 Trigger / Sample

Property. Emission happens at the trigger's frame, carrying the stored value. A trigger that finds nothing stored emits nothing.

```text
source   a-b----|
ticks    ---n--n|
sample   ---b---|
delay(3) ---a-b-|
```

Frame 3 is a tick frame, not an input frame. Frame 6 is a tick with nothing new, so nothing leaves. `delay` emits at input frame plus three and loses nothing, which is the previous pattern.

## AP-12 Merge / Interleave

Property. Values from every active producer leave as they arrive. All producers are subscribed at once.

```text
s1   a-c|
s2   -b-d|
merge(s1, s2)    abcd|       s1: ^--!   s2: ^---!
concat(s1, s2)   a-c-b-d|    s2: ---^---!
```

Under `concat` the second producer is not even subscribed until frame 3. `b` would have been ready at frame 1.

## AP-13 Serialize

Property. One producer at a time, in producer order, nothing lost.

```text
outer ab|   inner a = a-a|   inner b = b-b|
concatMap   a-ab-b|     b: ---^--!
mergeMap    abab|
switchMap   ab-b|
```

The flattening triad, seen from the action side. `concatMap` keeps all four values and never overlaps. `mergeMap` keeps all four and overlaps. `switchMap` does not overlap but loses the second `a`. Only the first is Serialize.

## AP-14 Current-Wins

Property. Only the selected producer is forwarded, and the others are cancelled. Cancellation shows in the subscription marbles.

```text
s1   -a-a|
s2   b-b-b|
race(s1, s2)     b-b-b|     s1: (^!)    s2: ^----!       First-Wins
outer ab|  switchMap   ab-b|   inner a: ^!                Latest-wins
merge(s1, s2)    babab|     s1: ^---!   s2: ^----!
```

`race` selects `s2` at frame 0 and cancels `s1` at the same frame. The selection never changes. `switchMap` selects the newest and cancels the previous at the frame of arrival. `merge` selects nothing and cancels nothing. Both selection rules are Current-Wins as [03](03-action-policies.md) now defines it.

## AP-15 Terminate

Property. The operator ends the stream at a frame the source did not end, and unsubscribes the source at that frame.

```text
a-b|      take(1)             ->  (a|)              source: (^!)
a----b|   timeout(2)          ->  a-#  TimeoutError  source: ^-!
a----b|   filter(() => false) ->  ------|           source: ^-----!
```

`take` completes on a count, `timeout` errors on the clock at a frame with no source event. In both the source is torn down at the terminating frame. `filter` drops every value and still lets the source run to its own completion. Suppressing is not terminating.

## How to use the suite

A row's action column is a conjunction of policies. To check a row, put the operator into each experiment its shape allows and read off the passes. To specify a new operator, choose the policies first and inherit their experiments as acceptance tests.

The suite closes the loop on three corrections from [07](07-catalog-evaluation.md). `catchError` would fail the Terminate experiment, since downstream does not end. `zip` would fail Serialize, since both sources are subscribed at frame 0. `race` passes Current-Wins only under the widened definition that admits First-Wins.

## Limits

- As in 08, one scenario is a witness, not a proof.
- The policy tests observe the projection of the action list onto what a subscriber and the subscription marbles can see. Scheduling actions that are cancelled before they fire leave no trace, so a Schedule followed by Cancel is only visible through its absence.
- `partition` cannot be put into the Route / Group experiment as one machine, because it is two `filter` subscriptions. Its test is the Gate / Suppress experiment, applied twice.
- Several policies share a positive operator with the state tests. That is expected: the same operator is one machine, and the two suites look at its two halves.
