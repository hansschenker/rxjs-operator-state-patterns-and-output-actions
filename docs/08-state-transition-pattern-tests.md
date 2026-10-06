# Operational tests for the state transition patterns

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Companion to [02-state-transition-patterns.md](02-state-transition-patterns.md).

The runnable suite is [tests/state-transition-patterns.test.ts](../tests/state-transition-patterns.test.ts). Run it with `npm install` once, then `npm test`. Result on 2026-10-06: 15 tests, 15 passing, against RxJS 7.8.2 under Vitest 5.0.3.

## What an operational test is

[02](02-state-transition-patterns.md) defines each pattern in prose. An operational test restates that definition as an experiment a subscriber can run:

1. A property that is visible from outside: the notifications, their virtual frames, the subscription timing of inner observables, or a probe callback.
2. One operator that has the pattern and satisfies the property.
3. One operator from a neighbouring pattern that does not, and fails the same property.

The neighbour is the important half. Without it a test only confirms. With it the test discriminates, and that is what makes a row in the catalog decidable rather than a judgment call.

Time is virtual. Every test runs inside `TestScheduler.run`, so one marble character is one frame, `|` is complete, `#` is error, and `(ab|)` groups notifications on one frame. Subscription marbles use `^` for subscribe and `!` for unsubscribe.

## Summary

| ID | Pattern | Observable property | Passes | Fails |
|---|---|---|---|---|
| ST-01 | Identity | The response to an event does not depend on earlier events | `tap` | `distinctUntilChanged` |
| ST-02 | Replace Latest | After a blocked period exactly the newest value emerges, once; older values never do | `sample` | `buffer` |
| ST-03 | Accumulate / Fold | Changing the first input changes the last output | `scan` | `pairwise` |
| ST-04 | Count / Index | Identical values are treated differently by position alone | `filter((v, i) => ...)`, `take` | `tap` |
| ST-05 | Phase / Flag | The boundary sits at an external event, not at a count | `skipUntil` | `skip` |
| ST-06 | History / Shift | Changing an input more than k positions back does not change later outputs | `pairwise` | `scan` |
| ST-07 | Membership Set | A key seen at any distance is suppressed, and the set does not forget | `distinct` | `distinctUntilChanged` |
| ST-08 | Buffer / Collect | Every retained value is released together, as one collection | `buffer` | `sample` |
| ST-09 | FIFO Queue | Release order equals arrival order even when release conditions resolve out of order | `concatMap` | `delayWhen` |
| ST-10 | Active Registry | Two inner subscriptions are open at the same frame | `mergeMap` | `concatMap` |
| ST-11 | Active Selection | A new candidate unsubscribes the current one | `switchMap` | `exhaustMap` |
| ST-12 | Slot Table + Readiness | Silence until every slot is filled, then the latest value per slot | `combineLatest` | `zip` |
| ST-13 | Timer / Deadline | Advancing the clock alone produces an action, and input restarts it | `debounceTime` | `timestamp` |
| ST-14 | Window / Session | Moving the boundary events regroups the same values | `buffer` | `bufferCount` |
| ST-15 | Lifecycle / Terminal | After complete the source is unsubscribed and later events are unobservable | `take` | none; `retry` shows postponement |

## ST-01 Identity

Property. Feed the same event after two different prefixes. An operator with no state answers it the same way both times.

```text
tap                    a-a|  ->  a-a|        b-a|  ->  b-a|
distinctUntilChanged   a-a|  ->  a--|        b-a|  ->  b-a|
```

The second `a` is answered differently by `distinctUntilChanged` because one slot remembers the prefix. That slot is Replace Latest, the nearest neighbour.

## ST-02 Replace Latest

Property. Block the read, send two values, then read. Exactly the newest emerges. A second read with nothing new produces nothing, because the slot was emptied.

```text
source   ab------|
ticks    ---n--n-|
sample   ---b----|
buffer   ---x--y-(y|)     x = [a, b]   y = []
```

`buffer` keeps `a` and yields a value on every flush, even an empty one. Data that survives is Buffer / Collect, not a slot.

## ST-03 Accumulate / Fold

Property. Run the same sequence twice, changing only the first input. The fold carries that change to the last output.

```text
scan(+)    1 2 3  ->  1  3  6         10 2 3  ->  10 12 15      last outputs differ
pairwise   1 2 3  ->  [1,2] [2,3]     10 2 3  ->  [10,2] [2,3]  last outputs equal
```

`pairwise` forgets the first input after two steps. Bounded memory is History / Shift.

## ST-04 Count / Index

Property. Four identical values. Only an ordinal can select every other one, or stop after two.

```text
aaaa|  filter((_, i) => i % 2 === 0)  ->  a-a-|
aaaa|  take(2)                        ->  a(a|)
```

The neighbour is observed through a different channel. A probe passed to `tap` records the number of arguments it receives on each call: `[1, 1, 1, 1]`. There is no index to read, so `tap` is Identity. This is the one pattern whose test needs a callback probe rather than a marble, because a counter that no callback reads is invisible downstream.

## ST-05 Phase / Flag

Property. Two sources with different value densities, one gate at frame 3. A phase flips at the gate in both runs. A counter flips after the same number of values in both runs.

```text
gate              ---n|
skipUntil(gate)   a-a-a-a|  ->  ----a-a|      aaa-a--|  ->  ----a--|
skip(2)           a-a-a-a|  ->  ----a-a|      aaa-a--|  ->  --a-a--|
```

On the sparse source the two operators agree. On the dense source they part: the phase boundary stayed at frame 3, the count boundary moved to frame 2. That is the whole difference between a flag and an ordinal.

## ST-06 History / Shift

Property. Change an input more than k positions back. Outputs beyond k positions are unchanged.

```text
pairwise   abcd|  ->  [a,b] [b,c] [c,d]      zbcd|  ->  [z,b] [b,c] [c,d]
scan(+)    1234|  ->  1 3 6 10               9234|  ->  9 11 14 18
```

For `pairwise` with k = 2, everything after the first output is identical across the two runs. For `scan` the last outputs differ. The mirror image of ST-03.

## ST-07 Membership Set

Property. A key suppresses its later occurrences at any distance, with any values in between, forever.

```text
abcab|  distinct              ->  abc--|
abcab|  distinctUntilChanged  ->  abcab|
```

`distinctUntilChanged` sees no adjacent repeats and passes everything. One slot cannot remember a key seen three values ago.

## ST-08 Buffer / Collect

Property. Values are retained as data and released together as one collection.

```text
source   abc---|
tick     ----n|
buffer   ----x-(y|)     x = [a, b, c]   y = []
sample   ----c-|
```

`sample` retains only `c`. Retaining all of them, and releasing them as one value, is the buffer. Releasing them one at a time, in order, would be a queue (ST-09).

## ST-09 FIFO Queue

Property. Give the second value a shorter release condition than the first. A queue still releases them in arrival order.

```text
source      ab|            durations: a = 5 frames, b = 1 frame
concatMap   -----a(b|)
delayWhen   --b--(a|)
```

`b` is ready at frame 2 and `concatMap` makes it wait until `a` has gone. `delayWhen` releases `b` at frame 2 and `a` at frame 5. There is no queue in `delayWhen`, which is the correction applied to the catalog in [07](07-catalog-evaluation.md).

## ST-10 Active Registry

Property. Two inner subscriptions are open during the same frames. Subscription marbles make this visible.

```text
source    ab|      inner a = a-a|     inner b = b-b|
mergeMap  abab|    a: ^--!     b: -^--!
concatMap a-ab-b|  a: ^--!     b: ---^--!
```

Under `mergeMap` both inners are live during frames 1 to 3. Under `concatMap` the second inner is not subscribed until the first has completed. One at a time is a queue, not a registry.

## ST-11 Active Selection

Property. A new candidate unsubscribes the current resource at the frame it arrives.

```text
source      ab|      inner a = a-a|     inner b = b-b|
switchMap   ab-b|    a: ^!       b: -^--!
exhaustMap  a-a|     a: ^--!     b: (never subscribed)
```

`switchMap` tears `a` down at frame 1. `exhaustMap` never admits `b` at all, so nothing is replaced. A busy/idle flag refuses; a selection replaces.

## ST-12 Slot Table + Readiness

Property. Silence until every input has filled its slot. Then the output reads the latest value per slot, not the oldest.

```text
s1              a-b--|
s2              ---x-|
combineLatest   ---p-|     p = [b, x]
zip             ---q-|     q = [a, x]
```

Both are silent until frame 3, so readiness alone does not separate them. The value does: `combineLatest` has overwritten `a` with `b`, `zip` still holds `a` at the head of a queue.

## ST-13 Timer / Deadline

Property. An action happens at a frame with no input event, and a new input restarts the countdown.

```text
a-----|   debounceTime(3)  ->  ---a--|
a-a----|  debounceTime(3)  ->  -----a-|
a-----|   timestamp        ->  x-----|     x = { value: a, timestamp: 0 }
```

`timestamp` produces output only at input frames. Advancing the clock by itself does nothing, so there is no timer, only a clock read. This is the test that would have caught the `timestamp` and `timeInterval` rows.

## ST-14 Window / Session

Property. Keep the values, move the boundary events, and the grouping moves with them.

```text
source          a-b-c-d-|
buffer(---n---n|)   ---x---y(z|)     x = [a, b]   y = [c, d]   z = []
buffer(-n---n--|)   -u---v--(w|)     u = [a]      v = [b, c]   w = [d]
bufferCount(2)      --x---y-|        x = [a, b]   y = [c, d]
```

`bufferCount` groups by count and has no boundary to move. A session is defined by its open and close events, not by how many values fall inside.

## ST-15 Lifecycle / Terminal

Property. Once the operator has completed, the source is unsubscribed and nothing later can be observed.

```text
a-b-c-d|   take(2)  ->  a-(b|)     source: ^-!
hot a-b-c| take(1)  ->  (a|)       source: (^!)
```

The source is torn down at the frame of completion, so `c` and `d` never happen for this subscriber. Every operator has this component, so there is no neighbour that lacks it. The second half of the test shows instead that a source terminal is not always the operator terminal:

```text
flaky   a-#
retry(1)   a-a-#     subscriptions: ^-!  then  --^-!
```

The source errors twice. The operator errors once, after the retry count is spent. The attempt count and the errored phase are state.

## How to use the suite

To classify a new or custom operator, put it in the role of the positive operator in each experiment it can fit. Each pass is a pattern it carries. A catalog row is the conjunction of its passes. An operator can and usually does pass several, because its state is a product.

To check the catalog, treat each row as a claim and run the matching experiments. The eight rows corrected in [07](07-catalog-evaluation.md) all fail the experiment for the pattern they claimed and pass the one for the pattern they received.

## Limits

- One scenario per pattern is a witness, not a proof. A pattern is present if the property holds for all inputs. The suite shows one input where it holds and one neighbour where it does not.
- Count / Index needs a probe. A counter nobody reads leaves no trace downstream.
- Lifecycle / Terminal has no negative case. Its test checks the absorbing property and shows one operator that postpones it.
- The experiments fix a scenario shape. An operator that does not take a notifier cannot be put into the `sample` experiment directly. The property still applies, but the scenario must be rebuilt around the operator's own trigger.
