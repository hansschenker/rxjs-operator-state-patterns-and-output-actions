# RxJS operators: one minimal example each

Analysis created by SuperGrok.

Notification comments are the sequence a subscriber sees. `partition` is listed twice in the source catalog, so it appears once.

Creation operators are sources: they produce the events. The rest step on an upstream.

`mapTo`, `mergeMapTo`, `switchMapTo`, `concatMapTo`, `pluck`, `publish`, `publishBehavior`, `publishLast`, `publishReplay`, and `retryWhen` are the older names. In current RxJS the same signatures are written with `map(() => ...)`, `mergeMap(() => ...)`, `switchMap(() => ...)`, `concatMap(() => ...)`, `map(x => x.id)`, `connectable` / `share({ connector })`, and `retry({ delay })`.

## Creation

```ts
ajax.getJSON<User>('/api/user')
// next(user), complete    or    error

const read = bindCallback((path: string, cb: (data: string) => void) =>
  cb('contents of ' + path)
)
read('a.txt')
// next('contents of a.txt'), complete
// callback-last. An error-first Node callback belongs to bindNodeCallback.

bindNodeCallback(fs.readFile)('a.txt')
// next(buffer), complete    or    error(err)

defer(() => Math.random() > 0.5 ? of('a') : of('b'))
// factory runs on subscribe: next('a') or next('b'), complete

empty()
// complete
// RxJS 7 prefers the EMPTY constant; empty() is the older creation function.

from([1, 2, 3])
// 1, 2, 3, complete

fromEvent(button, 'click')
// next(event) on each click; unsubscribe removes the listener

fromEventPattern(
  handler => emitter.on('data', handler),
  handler => emitter.off('data', handler)
)
// next(payload) while subscribed

generate({
  initialState: 1,
  condition: x => x < 8,
  iterate: x => x * 2,
  resultSelector: x => x
})
// 1, 2, 4, complete

interval(1000)
// 0, 1, 2, ... never completes

of('a', 'b')
// a, b, complete

range(1, 3)
// 1, 2, 3, complete

throwError(() => new Error('boom'))
// error

timer(500)
// 0, complete
timer(500, 1000)
// 0 at 500ms, then 1, 2, ... every 1000ms

iif(() => loggedIn, of('in'), of('out'))
// condition runs on subscribe: next('in') or next('out'), complete
```

## Join creation

```ts
combineLatest([of(1), of(10)])
// [1, 10], complete
// waits until every source has a slot, then emits on any update

concat(of(1, 2), of(3))
// 1, 2, 3, complete
// subscribes to the second source only after the first completes

forkJoin([of(1, 2), of(10)])
// [2, 10], complete
// last value of each, once, when all complete

merge(of(1), of(2))
// 1 and 2, order between sources is not fixed, complete

const [even$, odd$] = partition(of(1, 2, 3, 4), x => x % 2 === 0)
// even$ subscribed: 2, 4, complete
// odd$  subscribed: 1, 3, complete
// both results are observables; each sequence appears only if that one is subscribed

race(slow$, fast$)
// values from whichever emits first; the other is unsubscribed

zip(of('a', 'b'), of(1, 2, 3))
// ['a', 1], ['b', 2], complete
// index-aligned; stops at the shortest source
```

## Transformation

```ts
source$.pipe(buffer(clicks$))
// next([values since last click]) on each click

of(1, 2, 3, 4).pipe(bufferCount(2))
// [1, 2], [3, 4], complete

clicks$.pipe(bufferTime(1000))
// next([clicks in that second]) on each time boundary

source$.pipe(bufferToggle(open$, () => close$))
// one array per open/close session; sessions may overlap

source$.pipe(bufferWhen(() => interval(1000)))
// flush when the closing selector's inner emits

of(1, 2).pipe(concatMap(id => fetchUser(id)))
// subscribe the second request only after the first inner completes

of(1, 2).pipe(concatMapTo(of('x')))
// x, x, complete

of(innerA$, innerB$).pipe(exhaust())
// if innerA is active when innerB arrives, innerB is ignored

of(1, 2).pipe(exhaustMap(id => fetchUser(id)))
// a second id arriving while the first request is in flight is dropped

of(3).pipe(expand(n => n > 1 ? of(n - 1) : EMPTY))
// 3, 2, 1, complete

of({ t: 'a' }, { t: 'b' }, { t: 'a' }).pipe(groupBy(x => x.t))
// next(groupA), next(groupB); later 'a' values go to groupA

of(1, 2).pipe(map((x, i) => x + i))
// 1, 3, complete

of(1, 2).pipe(mapTo('x'))
// x, x, complete

of(1, 2).pipe(mergeMap(id => fetchUser(id)))
// both requests subscribed immediately; responses interleave

of(1, 2).pipe(mergeMapTo(of('x')))
// x, x, complete; both inners active together

of(1, 2).pipe(mergeScan((acc, x) => of(acc + x), 0))
// 1, 3, complete
// accumulator step is an inner; inners may overlap if a later step arrives first

of('a', 'b', 'c').pipe(pairwise())
// ['a', 'b'], ['b', 'c'], complete

of({ id: 7 }).pipe(pluck('id'))
// 7, complete

of(1, 2, 3).pipe(scan((acc, x) => acc + x, 0))
// 1, 3, 6, complete

of(1, 2).pipe(switchScan((acc, x) => of(acc + x), 0))
// same fold as mergeScan, but a new source value unsubscribes the in-flight step

of(1, 2).pipe(switchMap(id => fetchUser(id)))
// unsubscribe the request for 1 when 2 arrives; only the latest inner is forwarded

of(1, 2).pipe(switchMapTo(of('x')))
// x from the latest inner only; the previous inner is unsubscribed

clicks$.pipe(window(pause$))
// next(window$) at each boundary; clicks are routed into the open window

of(1, 2, 3, 4).pipe(windowCount(2))
// next(win1), next(win2); win1 emits 1, 2; win2 emits 3, 4

clicks$.pipe(windowTime(1000))
// a new window observable on each time boundary

source$.pipe(windowToggle(open$, () => close$))
// a window per open/close session

source$.pipe(windowWhen(() => interval(1000)))
// close and open the next window when the selector's inner emits
```

## Filtering

```ts
source$.pipe(audit(() => interval(100)))
// remember the latest value; emit it when the duration inner first emits; do not restart that window

of(1, 2, 3).pipe(auditTime(50))
// emit the latest value at the end of a window that started on the first value

input$.pipe(debounce(() => typingPause$))
// emit the latest value only after the duration selector emits

input$.pipe(debounceTime(300))
// emit the latest value only after 300ms of silence

of(1, 1, 2, 1).pipe(distinct())
// 1, 2, complete

of(1, 1, 2, 2, 3).pipe(distinctUntilChanged())
// 1, 2, 3, complete

of({ id: 1 }, { id: 1 }, { id: 2 }).pipe(distinctUntilKeyChanged('id'))
// {id:1}, {id:2}, complete

of('a', 'b', 'c').pipe(elementAt(1))
// b, complete

of(1, -2, 3).pipe(filter(x => x > 0))
// 1, 3, complete

of(1, 2, 3).pipe(first())
// 1, complete

of(1, 2).pipe(ignoreElements())
// complete
// every next is dropped

of(1, 2, 9).pipe(last())
// 9, complete

source$.pipe(sample(ticks$))
// on each tick, emit the latest source value, if one arrived since the last sample

source$.pipe(sampleTime(1000))
// same read, on an independent clock

of(7).pipe(single())
// 7, complete
// errors if the source emits zero or more than one match

of(1, 2, 3).pipe(skip(2))
// 3, complete

of(1, 2, 3, 4).pipe(skipLast(2))
// 1, 2, complete

source$.pipe(skipUntil(go$))
// drop until go$ emits, then forward

of(1, 2, 3, 4).pipe(skipWhile(x => x < 3))
// 3, 4, complete

of(1, 2, 3).pipe(take(2))
// 1, 2, complete

of(1, 2, 3, 4).pipe(takeLast(2))
// 3, 4, complete
// flushed only when the source completes

source$.pipe(takeUntil(stop$))
// forward, then complete when stop$ emits

of(1, 2, 3).pipe(takeWhile(x => x < 3))
// 1, 2, complete

clicks$.pipe(throttle(() => interval(200)))
// emit the first click, ignore clicks until the duration inner emits

clicks$.pipe(throttleTime(200))
// emit the first click, suppress for 200ms; default is leading on, trailing off
```

## Join

```ts
of(of(1, 2), of(10)).pipe(combineLatestAll())
// [2, 10] once both inners have a slot, then on any later inner update

of(of(1), of(2, 3)).pipe(concatAll())
// 1, 2, 3, complete

of(long$, short$).pipe(exhaustAll())
// short$ is ignored if it arrives while long$ is active

of(of(1), of(2)).pipe(mergeAll())
// 1 and 2 interleaved, complete

of(of(1, 2), of(3)).pipe(switchAll())
// unsubscribes of(1, 2) when of(3) arrives; forwards only the latest inner

of(2, 3).pipe(startWith(1))
// 1, 2, 3, complete

clicks$.pipe(withLatestFrom(user$))
// [click, latestUser] on each click, after user$ has emitted at least once
```

## Multicasting

```ts
const shared = source$.pipe(multicast(() => new Subject<number>()))
shared.subscribe(console.log)
shared.connect()
// one upstream subscription after connect(); each connected subscriber sees the same notifications

const published = source$.pipe(publish())
published.subscribe(console.log)
published.connect()
// multicast with a plain Subject; no replay

const behavior = source$.pipe(publishBehavior(0))
behavior.subscribe(console.log)
// this subscriber receives 0 immediately, from the BehaviorSubject
behavior.connect()
// live source values follow connect()

const last = http$.pipe(publishLast())
last.subscribe(console.log)
last.connect()
// subscribers receive the final value only after the source completes

const replayed = source$.pipe(publishReplay(2))
replayed.connect()
replayed.subscribe(console.log)
// a subscriber after connect() receives up to the last 2 values, then live values

const hot = source$.pipe(share())
hot.subscribe(a)
hot.subscribe(b)
// one upstream subscription while anyone is listening; torn down at refcount zero
```

## Error handling

```ts
http$.pipe(
  catchError(err => of({ fallback: true }))
)
// source error is caught; subscriber sees next({ fallback: true }), complete

flaky$.pipe(retry(2))
// resubscribe twice on error; the third error is forwarded

source$.pipe(
  retryWhen(errors => errors.pipe(delay(1000)))
)
// resubscribe when the notifier emits; notifier error or complete ends the retry
```

## Utility

```ts
of(1, 2).pipe(tap(x => log(x)))
// 1, 2, complete
// log runs; the value is unchanged

of(1).pipe(delay(200))
// 1 at 200ms, complete also delayed

of('a', 'b').pipe(delayWhen(() => interval(100)))
// each value waits until its duration inner emits; order is kept

of(Notification.createNext(1)).pipe(dematerialize())
// next(1), complete
// Notification.createComplete() becomes complete; Notification.createError(err) becomes error

of(1).pipe(materialize())
// next(Notification next 1), next(Notification complete), complete

source$.pipe(observeOn(asyncScheduler))
// same values, delivered on the destination scheduler

source$.pipe(subscribeOn(asyncScheduler))
// subscribe and unsubscribe are scheduled; values are not shifted

of('a', 'b').pipe(timeInterval())
// { value: 'a', interval }, { value: 'b', interval }, complete

of('a').pipe(timestamp())
// { value: 'a', timestamp }, complete

request$.pipe(timeout(3000))
// forward the response, or error if no next arrives within 3000ms

request$.pipe(timeoutWith(3000, of('slow')))
// on expiry, unsubscribe the source and subscribe the fallback

of(1, 2, 3).pipe(toArray())
// [1, 2, 3], complete
```

## Conditional

```ts
EMPTY.pipe(defaultIfEmpty('none'))
// none, complete

of(2, 4, 6).pipe(every(x => x % 2 === 0))
// true, complete

of(1, 2, 3).pipe(find(x => x > 1))
// 2, complete

of(1, 2, 3).pipe(findIndex(x => x > 1))
// 1, complete

of(1).pipe(isEmpty())
// false, complete
EMPTY.pipe(isEmpty())
// true, complete
```

## Aggregate

```ts
of(1, 2, 3).pipe(count())
// 3, complete

of(2, 9, 4).pipe(max())
// 9, complete

of(2, 9, 4).pipe(min())
// 2, complete

of(1, 2, 3).pipe(reduce((acc, x) => acc + x, 0))
// 6, complete
// same fold as scan; emitted once, on complete
```
