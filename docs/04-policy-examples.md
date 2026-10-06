# One example per action policy

Analysis created by SuperGrok.

Each block is one ordered action list. The state half is named only so the action has something to read.

## AP-01 Forward

`take(2)` lets the value through until the count runs out.

```text
state: count = 0
event: next(A)
action: next(A)
state: count = 1
```

`tap` is the same forward, plus a side effect that does not change the notification.

## AP-02 Transform

`map(x => x * 10)` emits the projected value, not the input.

```text
event: next(3)          index = 0
action: next(30)
```

## AP-03 Gate / Suppress

`filter(x => x > 0)` either forwards or produces an empty action list.

```text
event: next(-1)
action: []

event: next(4)
action: next(4)
```

## AP-04 Synthesize / Inject

`startWith(0)` emits a value that did not come from the source.

```text
subscribe
action: next(0)
event: next(A)
action: next(A)
```

## AP-05 Emit Derived State

`scan((acc, x) => acc + x, 0)` emits the state the fold just computed.

```text
state: acc = 0
event: next(2)
state: acc = 2
action: next(2)

event: next(5)
state: acc = 7
action: next(7)
```

`reduce` uses the same fold and emits `acc` once, on complete.

## AP-06 Read Stored State

`pairwise` emits the previous value together with the current one.

```text
event: next(A)
state: history = [A]
action: []

event: next(B)
state: history = [A, B]
action: next([A, B])
```

## AP-07 Combine / Join

`combineLatest` reads every slot once all of them are ready.

```text
event: A next(1)
state: slots = (1, ?), ready = (true, false)
action: []

event: B next(10)
state: slots = (1, 10), ready = (true, true)
action: next([1, 10])
```

## AP-08 Batch / Aggregate

`bufferCount(2)` emits one array instead of the individual values.

```text
event: next(A)
state: buffer = [A]
action: []

event: next(B)
state: buffer = []
action: next([A, B])
```

## AP-09 Route / Group

`partition(x => x % 2 === 0)` does not change the value. It chooses the destination.

```text
event: next(2)
action: even.next(2)

event: next(3)
action: odd.next(3)
```

## AP-10 Delay / Time-Shift

`delay(100)` schedules the same notification for later.

```text
event: next(A) at t = 0
action: schedule(next(A), t = 100)

event: next(B) at t = 10
action: schedule(next(B), t = 110)
```

## AP-11 Trigger / Sample

`debounceTime(30)` emits the stored value when the silence timer fires.

```text
event: next(A) at t = 0
action: cancel(timer0), schedule(timer1, t = 30)
state: latest = A

event: next(B) at t = 10
action: cancel(timer1), schedule(timer2, t = 40)
state: latest = B

event: timer2 expires
action: next(B)
```

## AP-12 Merge / Interleave

`mergeMap` forwards from every active inner as soon as that inner emits.

```text
event: next(1)
action: subscribe(inner1)

event: next(2)
action: subscribe(inner2)

event: inner2 next('b')
action: next('b')

event: inner1 next('a')
action: next('a')
```

## AP-13 Serialize

`concatMap` refuses that overlap. The second inner waits.

```text
event: next(1)
action: subscribe(inner1)
state: active = inner1, queue = []

event: next(2)
action: []
state: queue = [2]

event: inner1 complete
action: subscribe(inner2)
```

## AP-14 Current-Wins

`switchMap` replaces the selected producer. Cancel is part of the action list.

```text
event: next(1)
action: subscribe(inner1)
state: current = inner1

event: next(2)
action: unsubscribe(inner1), subscribe(inner2)
state: current = inner2

event: inner1 next('late')
action: []

event: inner2 next('fresh')
action: next('fresh')
```

## AP-15 Terminate

`take(1)` forwards once, then ends the stream.

```text
event: next(A)
action: next(A), complete
state: completed
```

`retry(2)` is the exception: a source error becomes a resubscribe action twice, and only the third failure runs Terminate.
