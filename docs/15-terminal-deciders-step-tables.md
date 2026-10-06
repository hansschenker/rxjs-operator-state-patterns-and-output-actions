# Step tables for the terminal deciders

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Derived from the RxJS 7.8.2 source of `take`, `takeLast`, `defaultIfEmpty`, `throwIfEmpty`, `single`, `find`, `findIndex`, `every`, `isEmpty`, `first`, `last`, and `elementAt`. Every row is verified by [tests/terminal-deciders-step-tables.test.ts](../tests/terminal-deciders-step-tables.test.ts). Result on 2026-10-06: 14 tests, 14 passing.

## The sixth shape

A terminal decider is an operator that ends the stream by its own decision rather than by forwarding the source's. The shape has two questions, and both are simpler than anything in the first five families. At which event is the decision made, a value or the source's completion? And what does the operator do when the source completes without ever having emitted?

The second question is where readers go wrong, because the twelve operators give three different kinds of answer and the kind is not guessable from the name. The structure that makes the family tractable is that three of the twelve are compositions of the other nine. `first`, `last`, and `elementAt` are each a `filter`, then `take(1)` or `takeLast(1)`, then `defaultIfEmpty` or `throwIfEmpty`. Their behavior on an empty source is the behavior of their last stage.

## Conventions

**Events.** `next(x)`, `complete`, `error(e)` from the source. `call` for the operator factory being invoked, because two rows in this family act before any subscription exists. `unsubscribe` from downstream.

**Actions.** `Next(v)`, `Complete`, `Error(kind)`. The family raises four error kinds of its own: `EmptyError`, `SequenceError`, `NotFoundError`, and `ArgumentOutOfRangeError`. Reaching `Done` tears the source down; that is implied.

**States.** Small. A count, a slot, a flag, or a ring buffer. This is the family where Phase / Flag is the whole state for most members, so these are the catalog's few classical finite-state machines.

## The four primitives

### take(n)

State: `Counting(k)`, values forwarded so far.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| TK0 | — | call, n ≤ 0 | | returns `EMPTY`; the source is never subscribed | `a-b\|` → `\|`, source subscriptions `[]` |
| TK1 | Counting(k), k + 1 < n | next(x) | Counting(k+1) | Next(x) | |
| TK2 | Counting(n−1) | next(x) | Done | Next(x), Complete | `a-b-c\|` → `a-(b\|)`; source `^-!` |
| TK3 | Counting(k) | complete | Done | Complete | `--\|` → `--\|` |
| TK4 | any | error(e) | Done | Error(e) | `a-#` → `a-#` |

TK0 is a row with a consequence outside the marble: a cold source's subscribe-time side effects never run under `take(0)`.

### takeLast(n)

State: `Buffer`, a ring of at most `n` values.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| TL0 | — | call, n ≤ 0 | | returns `EMPTY`; the source is never subscribed | `a-b\|` → `\|`, subscriptions `[]` |
| TL1 | Buffer | next(x) | Buffer with x, oldest dropped beyond n | — | |
| TL2 | Buffer | complete | Done | Next each, in order; Complete | `a-b-c-\|` → `------(bc\|)` |
| TL3 | Buffer, empty | complete | Done | Complete | `--\|` → `--\|` |
| TL4 | Buffer | error(e) | Done | Error(e), buffer dropped | `a-b-#` → `----#` |

### defaultIfEmpty(d)

State: `Empty`, `Seen`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| DE1 | Empty, Seen | next(x) | Seen | Next(x) | `a-\|` → `a-\|` |
| DE2 | Empty | complete | Done | Next(d), Complete | `--\|` → `--(d\|)` |
| DE2' | Seen | complete | Done | Complete | `a-\|` → `a-\|` |
| DE3 | any | error(e) | Done | Error(e); no default | `--#` → `--#` |

### throwIfEmpty(factory)

Same states. The factory defaults to `() => new EmptyError()`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| TE1 | Empty, Seen | next(x) | Seen | Next(x) | `a-\|` → `a-\|` |
| TE2 | Empty | complete | Done | Error(factory()) | `--\|` → `--#`, `EmptyError` |
| TE2' | Seen | complete | Done | Complete | `a-\|` → `a-\|` |
| TE3 | any | error(e) | Done | Error(e); the factory is not consulted | |

## The independent deciders

### single(predicate?)

State: `Empty` (nothing seen), `Seen` (values seen, none matched), `Stored(x)` (one match).

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| SG1 | Empty, Seen | next(x), matching | Stored(x) | — | |
| SG1' | Stored(_) | next(x), matching | Done | Error(SequenceError), at once | `ab--\|` → `-#`; source `^!` |
| SG1'' | any | next(x), not matching | Seen | — | |
| SG2 | Stored(x) | complete | Done | Next(x), Complete | `a-\|` → `--(a\|)` |
| SG3 | Empty | complete | Done | Error(EmptyError) | `--\|` → `--#` |
| SG3' | Seen | complete | Done | Error(NotFoundError) | `ab\|`, no match → `--#` |
| SG4 | any | error(e) | Done | Error(e) | `a-#` → `--#` |

`single` is the only member that can fail in three distinct ways, and the only one whose failure can arrive before the source completes. SG1' fires at the second match, at frame 1 in the witness, with the source still live and torn down there.

### find(predicate) / findIndex(predicate)

State: `Counting(i)`, the index of the next value.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| FD1 | Counting(i) | next(x), matching | Done | Next(x), or Next(i); Complete | `abc\|` → `-(b\|)`, `-(1\|)`; source `^!` |
| FD1' | Counting(i) | next(x), not matching | Counting(i+1) | — | |
| FD2 | Counting(i) | complete | Done | Next(undefined), or Next(−1); Complete | `abc\|`, no match → `---(u\|)`, `---(m\|)` |
| FD3 | any | error(e) | Done | Error(e) | |

### every(predicate)

State: `Counting(i)`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| EV1 | Counting(i) | next(x), predicate false | Done | Next(false), Complete | `abc\|` → `-(f\|)`; source `^!` |
| EV1' | Counting(i) | next(x), predicate true | Counting(i+1) | — | |
| EV2 | Counting(i) | complete | Done | Next(true), Complete | `abc\|`, all pass → `---(t\|)` |
| EV3 | Counting(0) | complete | Done | Next(true), Complete | `--\|` → `--(t\|)`, vacuous truth |
| EV4 | any | error(e) | Done | Error(e) | `a-#` → `--#` |

### isEmpty()

State: `Waiting`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| IE1 | Waiting | next(x) | Done | Next(false), Complete | `a-\|` → `(f\|)`; source `(^!)` |
| IE2 | Waiting | complete | Done | Next(true), Complete | `--\|` → `--(t\|)` |
| IE3 | Waiting | error(e) | Done | Error(e) | `--#` → `--#` |

## The compositions

```text
first(p?, d?)     = filter(p)            ∘ take(1)      ∘ (d given ? defaultIfEmpty(d) : throwIfEmpty(EmptyError))
last(p?, d?)      = filter(p)            ∘ takeLast(1)  ∘ (d given ? defaultIfEmpty(d) : throwIfEmpty(EmptyError))
elementAt(i, d?)  = filter((_, k) => k === i) ∘ take(1) ∘ (d given ? defaultIfEmpty(d) : throwIfEmpty(ArgumentOutOfRangeError))
```

"d given" is tested by argument count, so `first(undefined, undefined)` has a default of `undefined` and does not throw. Each row below is the composition of the primitive rows above.

| Row | Operator | Case | Result | Witness |
|---|---|---|---|---|
| FI1 | first | a match arrives | Next(match), Complete at its frame | `a-b\|` → `(a\|)`; `abc\|` with p = b → `-(b\|)` |
| FI2 | first(p, d) | no match, complete | Next(d), Complete | `ab\|` → `--(d\|)` |
| FI3 | first | no match, complete, no default | Error(EmptyError) | `--\|` → `--#` |
| LA1 | last | complete | Next(last match), Complete | `a-b-\|` → `----(b\|)`; `abc\|` with p ≠ c → `---(b\|)` |
| LA2 | last(p, d) | no match, complete | Next(d), Complete | `--\|` → `--(d\|)` |
| LA3 | last | no match, complete, no default | Error(EmptyError) | `--\|` → `--#` |
| EA0 | elementAt | call with i < 0 | throws `ArgumentOutOfRangeError` synchronously, outside the observable | `elementAt(-1)` throws |
| EA1 | elementAt(i) | the ith value arrives | Next(it), Complete at its frame | `abc\|` with 1 → `-(b\|)`; source `^!` |
| EA2 | elementAt(i, d) | fewer than i + 1 values | Next(d), Complete | `ab\|` with (5, d) → `--(d\|)` |
| EA3 | elementAt(i) | fewer than i + 1 values, no default | Error(ArgumentOutOfRangeError) | `ab\|` with 5 → `--#` |

Two consequences of the composition are worth knowing. The index a predicate receives in `first` and `last` is the source index from `filter`, not a count of matches. And `elementAt` with a default and `elementAt` without one differ only in the last stage, so a too-short source is the same event answered two ways.

## The empty-source matrix

| Operator | Source completes with no values | Kind of answer | Decision made at |
|---|---|---|---|
| `take(n)` | Complete, nothing | complete empty | the nth value, or complete |
| `takeLast(n)` | Complete, nothing | complete empty | complete |
| `first()` | `EmptyError` | error | the first match, or complete |
| `first(p, d)` | `d` | sentinel | |
| `last()` | `EmptyError` | error | complete |
| `last(p, d)` | `d` | sentinel | |
| `single()` | `EmptyError` | error | complete; `SequenceError` at a second match |
| `single(p)`, values but no match | `NotFoundError` | error | complete |
| `elementAt(i)` | `ArgumentOutOfRangeError` | error | the ith value, or complete |
| `elementAt(i, d)` | `d` | sentinel | |
| `find(p)` | `undefined` | sentinel | the first match, or complete |
| `findIndex(p)` | `-1` | sentinel | |
| `defaultIfEmpty(d)` | `d` | sentinel | complete |
| `throwIfEmpty()` | `EmptyError` | error | complete |
| `every(p)` | `true` | sentinel | the first failing value, or complete |
| `isEmpty()` | `true` | sentinel | the first value, or complete |

Three kinds of answer: complete empty, a sentinel, an error. The two `take`s are the only ones that complete empty. Five error by default, and three of those turn into a sentinel when given a default argument. The rest have a sentinel built in, and `every` and `isEmpty` agree that an empty source is `true`.

## Decision time

Deciders at a value tear the source down at that frame, so later source values never exist for them: `take`, `first`, `elementAt`, `find`, `findIndex`, `isEmpty` answering `false`, `every` answering `false`, and `single` raising `SequenceError`. Deciders at complete hold a count, a slot, a flag, or a buffer until the source finishes: `takeLast`, `last`, `single`, `defaultIfEmpty`, `throwIfEmpty`, `every` answering `true`, `isEmpty` answering `true`. Most members have both branches, and the two branches give different outputs: `every` and `isEmpty` emit `false` on a value and `true` on complete; `find` emits the match on a value and `undefined` on complete.

## Error

Uniform in a sixth family. A source error passes through every decider at once. Nothing stored is emitted, so `takeLast`, `last`, and `single` lose what they held. `throwIfEmpty` does not consult its factory, because a source error is not an empty completion. The four errors the family raises itself are all raised on complete, except `SequenceError`, which is raised on a value.

## Contrast with the first five families

| | Timing | Flattening | Join | Multicasting | Buffering | Deciders |
|---|---|---|---|---|---|---|
| Question shape | Source completes while a timer is armed | Outer completes while inners run | A source completes before, after, or with a backlog | A subscriber joins or leaves; a terminal arrives | A region is open when something else ends it | The source completes empty |
| Completion | Three policies | One | Four conditions | Configurable | One | Three kinds of answer |
| Error | Uniform | Uniform | Uniform | Configurable | Uniform, one silent teardown | Uniform |
| Where members differ | Completion | Admission | Readiness | Reset switches | Reading a notifier's terminal | Decision time and empty-source answer |
| State | Timers and slots | Registries and queues | Slots and queues | Connection and subject | Open regions | A count, a slot, a flag |

This is the family where the source's `complete` is the input that triggers the operator's most interesting decision, and the output on that one event varies more than anywhere else: nothing, a sentinel, a stored value, a buffer, or one of three errors. Error never waits, for the sixth time.

## What the tables show that the rows could not

- Three of the twelve are compositions, and every quirk of `first`, `last`, and `elementAt` is a quirk of `filter`, `take`, `takeLast`, `defaultIfEmpty`, or `throwIfEmpty`.
- The empty-source answer comes in three kinds, and a default argument switches three operators from error to sentinel.
- `single` has three failure modes, one of them before the source completes.
- `take(0)` and `takeLast(0)` return `EMPTY` and never subscribe the source.
- `elementAt` with a negative index throws when the operator is created, outside the observable protocol altogether.
- `every` and `isEmpty` are both `true` on an empty source; `find` and `findIndex` return `undefined` and `-1`.

## Relation to the catalog

Tags that would let the rows carry this: `empty source: completes empty` on `take` and `takeLast`; `empty source: sentinel` on `find`, `findIndex`, `every`, `isEmpty`, `defaultIfEmpty`; `empty source: error, sentinel with a default` on `first`, `last`, `elementAt`; `empty source: error` on `single` and `throwIfEmpty`; `decides at value` or `decides at complete` on each; `n ≤ 0 never subscribes` on `take` and `takeLast`; `composition of filter, take, defaultIfEmpty or throwIfEmpty` on `first`, `last`, `elementAt`.

## Next family

Six families cover every operator in the catalog whose state is a timer, a registry, a queue, a slot, a connection, a region, or a decision. What remains with real subtlety is error handling: `retry` with its `count`, `delay`, and `resetOnSuccess` options, `retryWhen` and the meaning of its notifier completing, and `catchError` with a handler that rethrows or returns the source itself. Their question is the seventh shape: an error that is not the end, and the rules for how many times and how soon.
