# Step tables for the multicasting family

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Derived from the RxJS 7.8.2 source of `share`, `shareReplay`, and `connectable`. Every row is verified by [tests/multicasting-family-step-tables.test.ts](../tests/multicasting-family-step-tables.test.ts) unless marked "by inspection". Result on 2026-10-06: 16 tests, 16 passing.

## The fourth shape

The first three families were driven by one source, or several, and by timers. Multicasting adds a channel the others never had: the subscribers themselves. A subscriber joining or leaving is an event, and the machine's main job is to decide what those events do to a single shared connection. The second novelty is that a terminal notification from the source may not be terminal for the machine. With the default resets, `share` recovers from complete and from error and connects again for the next subscriber. There is no absorbing state at all unless a reset is switched off.

## Conventions

**Events.** `sub` when a subscriber joins, `unsub` when one leaves. From the source: `snext(x)`, `scomplete`, `serror(e)`. `grace` when the reset notifier fires. For `connectable`: `connect` and `disconnect`, both called by user code.

**Actions.** `NewSubject` calls the connector. `Attach` subscribes the joining subscriber to the current subject. `Connect` subscribes the source; `Disconnect` unsubscribes it. `Broadcast(x)`, `Broadcast(Complete)`, `Broadcast(Error)` deliver to every attached subscriber through the subject. `Replay(buffer)` is what a `ReplaySubject` does to a joining subscriber. `Schedule(grace)` and `Cancel(grace)` arm and disarm the reset notifier. `Reset` forgets the subject and the connection and clears the terminal flags.

**States for `share`.** `Cold`: no subject, no connection, no subscribers. `Hot(n)`: subject and connection live, `n ≥ 1` subscribers. `Lingering`: connection live, no subscribers, no reset pending. `Grace`: connection live, no subscribers, reset notifier armed. `Ended`: the subject has terminated and the terminal was kept, so there is no live connection and no reset will come. `Ended` is the only absorbing state.

**Witnesses.** The source is cold, so its subscription log is the connection log: one entry per `Connect`, closed on `Disconnect` or on the source's own terminal. Subscriber marbles show join as `^` and leave as `!`.

## share(options): one machine, three switches

The switches are `resetOnRefCountZero`, `resetOnComplete`, and `resetOnError`. Each is `true`, `false`, or a notifier factory. The table carries the switch as a condition column.

| Row | State | Event | Condition | State' | Actions | Witness |
|---|---|---|---|---|---|---|
| SH1 | Cold | sub | | Hot(1) | NewSubject, Attach, Connect | `a-b-c-d-e\|`, sub `^--!` → `a-b` |
| SH1' | Cold | sub | | | Attach precedes Connect | `(ab\|)` → `(ab\|)` |
| SH2 | Hot(n) | sub | | Hot(n+1) | Attach | sub `-^-!` → `--b`, one connection |
| SH3 | Hot(n) | snext(x) | | Hot(n) | Broadcast(x) | every witness |
| SH4 | Hot(n), n ≥ 2 | unsub | | Hot(n−1) | — | first leave at 3, connection continues |
| SH5 | Hot(1) | unsub | refCountZero = true | Cold | Disconnect, Reset | connection `^--!` |
| SH5' | Hot(1) | unsub | refCountZero = false | Lingering | — | connection `^-------!` |
| SH5'' | Hot(1) | unsub | refCountZero = notifier | Grace | Schedule(grace) | `timer(3)` armed at 3 |
| SH6 | Grace | sub | | Hot(1) | Cancel(grace), Attach | sub at 5 → `------d-e\|`, one connection |
| SH7 | Grace | grace | | Cold | Disconnect, Reset | connection `^-----!` |
| SH8 | Lingering | sub | | Hot(1) | Attach | sub at 5 → `------d-\|` |
| SH9 | Hot(n) | scomplete | onComplete = true | Cold | Reset, Broadcast(Complete) | `a\|`, sub at 2 → `--a\|`; connections `^!`, `--^!` |
| SH9' | Hot(n) | scomplete | onComplete = false | Ended | Broadcast(Complete) | sub at 2 → `--\|`; one connection |
| SH10 | Ended | sub | | Ended | Attach, then Complete or Error at once | same witnesses |
| SH11 | Cold, after a Reset | sub | | Hot(1) | NewSubject, Attach, Connect | second connection in SH5, SH7, SH9, SH12 |
| SH12 | Hot(n) | serror(e) | onError = true | Cold | Reset, Broadcast(Error) | `a#`, sub at 2 → `--a#`; connections `^!`, `--^!` |
| SH12' | Hot(n) | serror(e) | onError = false | Ended | Broadcast(Error) | sub at 2 → `--#`; one connection |
| SH13 | Lingering, Grace | scomplete, serror | | as SH9, SH12 | as SH9, SH12, with nobody attached | SR witness, frame 6 |
| SH14 | Lingering, Grace | snext(x) | | same | — | `c` at 4 is lost in SH5' and SH6 |

SH1' is an ordering fact with a consequence: the first subscriber is attached to the subject before the source is connected, so a source that emits synchronously on subscribe reaches that subscriber. The other order would lose those values.

SH5 against SH5' against SH5'' is the whole `resetOnRefCountZero` option. With `true` the connection dies with the last subscriber. With `false` it lives on and feeds a `Subject` nobody hears, so SH14 loses `c`. With a notifier it gets a grace period: a subscriber who returns inside it joins the same connection (SH6) and still missed whatever arrived while nobody listened. The SH7 witness shows one more thing: the grace is re-armed at every zero crossing. The subscriber who reconnected at 7 leaves at 10, and that connection lingers to 13.

SH9 and SH12 are why this family has no absorbing state by default. Complete and error are broadcast, and then forgotten. The next `sub` finds `Cold` and connects the source again from the start. SH9' and SH12' are the only way to make a terminal stick, and then SH10 says every later subscriber gets the terminal at once and nothing ever reconnects.

One ordering detail by inspection: in SH9 and SH12 the `Reset` runs before the `Broadcast`. A subscriber that resubscribes from inside its own complete handler therefore starts a new connection.

## shareReplay(n): share with a ReplaySubject and fixed switches

`shareReplay` is `share` with `connector: () => new ReplaySubject(n)`, `resetOnError: true`, `resetOnComplete: false`, and `resetOnRefCountZero` set to its own `refCount` option, which defaults to `false`.

| Row | State | Event | Condition | State' | Actions | Witness |
|---|---|---|---|---|---|---|
| SR1 | Hot, Lingering | sub | | Hot(n+1) | Attach, Replay(buffer), then live | sub at 5 → `-----c\|` |
| SR2 | Hot(1) | unsub | refCount = false | Lingering | — | connection `^-----!` |
| SR3 | Lingering | snext(x) | | Lingering | — , x enters the buffer | `c` at 4 is replayed at 5 |
| SR4 | Hot, Lingering | scomplete | | Ended | Broadcast(Complete), buffer kept | connection ends at 6, no reset |
| SR4' | Ended | sub | | Ended | Attach, Replay(buffer), Complete | sub at 8 → `--------(c\|)` |
| SR5 | Hot | serror(e) | | Cold | Reset, buffer discarded, Broadcast(Error) | `a-#`, sub at 4 → `----a-#`; connections `^-!`, `----^-!` |
| SR6 | Hot(1) | unsub | refCount = true | Cold | Disconnect, Reset, buffer discarded | sub at 5 → `-----a-b-c-\|`; connections `^--!`, `-----^-----!` |

SR3 is the same event as SH14 with the opposite outcome: a value arriving with no subscribers is buffered, not lost, because the subject is a `ReplaySubject`. SR4 and SR5 are an asymmetry the catalog row cannot hold. Completion is kept forever, so every future subscriber gets the buffer and then complete without the source ever running again. Error is reset, so the buffer is thrown away and the next subscriber reconnects. SR6 shows what `refCount: true` costs: the buffer lives in the subject, the subject dies with the connection, and a returning subscriber sees the source from the start.

## connectable(source): the manual machine

There is no reference count. Subscribers attach to whatever subject is current when they arrive; the source runs only between `connect` and `disconnect`. With the default `resetOnDisconnect: true`, the subject is replaced whenever the connection ends, by `disconnect` or by the source's own terminal.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| CN1 | Detached(S) | sub | Detached(S), n+1 | Attach | sub at 0 → `--a-b-c\|` |
| CN2 | Detached(S) | connect | Connected(S) | Connect | connection `--^----!` |
| CN3 | Connected(S) | snext(x) | same | Broadcast(x) | same witness |
| CN4 | Connected(S) | connect | same | — , the same Subscription is returned | verified |
| CN5 | Connected(S) | disconnect | Detached(S'), old subscribers stay on S | Disconnect, NewSubject | sub1 → `a-b`, never completes |
| CN6 | Connected(S) | scomplete | Detached(S') | Broadcast(Complete) on S, NewSubject | `a\|` → `a\|` |
| CN6' | Connected(S) | serror(e) | Detached(S') | Broadcast(Error) on S, NewSubject | by inspection, same finalizer |
| CN7 | Detached(S') | sub | Detached(S'), n+1 | Attach | sub at 4 receives nothing until connect |
| CN8 | Detached(S') | connect | Connected(S') | Connect, source from the start | sub at 4 → `-----a-b-c\|`; connections `^--!`, `-----^----!` |
| CN9 | Connected(S) | sub | Connected(S), n+1 | Attach, live values only | sub at 3 → `----b-c\|` |
| CN10 | Connected(S) | unsub | Connected(S), n−1, even to zero | — | connection `^----!` with no subscriber left |
| CN11 | with resetOnDisconnect = false | disconnect, scomplete | Detached(S), same subject | — , a terminated S stays terminal | by inspection |

CN5 is the row to know before using `connectable`. A `disconnect` replaces the subject for future subscribers but does nothing for current ones. They remain attached to a subject that will never receive another notification, including a terminal one. They are stranded until they unsubscribe themselves. CN10 is the mirror of the whole `share` family: with no reference count, the last subscriber leaving changes nothing, and the source keeps running into an empty subject.

## The cross-cutting cases

| Case | share() | share, refCountZero off | share, grace notifier | shareReplay(n) | shareReplay, refCount on | connectable |
|---|---|---|---|---|---|---|
| First subscriber | Connect (SH1) | Connect | Connect | Connect | Connect | Nothing until connect (CN1, CN2) |
| Last subscriber leaves | Disconnect, Reset (SH5) | Keep; values lost (SH5', SH14) | Grace; keep if someone returns, else Disconnect (SH6, SH7) | Keep; values buffered (SR2, SR3) | Disconnect; buffer lost (SR6) | Nothing (CN10) |
| Late subscriber mid-stream | Live values (SH2) | Live values | Live values | Last n, then live (SR1) | Last n, then live | Live values (CN9) |
| Source completes | Broadcast, Reset; next sub reconnects (SH9, SH11) | same | same | Broadcast; keep; late subs get buffer and complete (SR4, SR4') | same | Broadcast; new subject; nothing until connect (CN6, CN8) |
| Source errors | Broadcast, Reset; next sub reconnects (SH12) | same | same | Broadcast, Reset; buffer lost; next sub reconnects (SR5) | same | Broadcast; new subject (CN6') |
| A terminal is absorbing | No | No | No | Complete yes, error no | Complete yes, error no | No, and stranded subscribers never see one (CN5) |

A value that arrives while nobody is subscribed has three possible fates in this one family: it never happens because the source was disconnected, it is lost in a `Subject`, or it is kept in a `ReplaySubject`. Which one depends on a switch, not on the operator name.

## Contrast with the first three families

| | Timing | Flattening | Join | Multicasting |
|---|---|---|---|---|
| Question shape | Source completes while a timer is armed | Outer completes while inners are active | A source completes before, after, or with a backlog | A subscriber joins or leaves; a terminal arrives |
| Completion | Three policies | One policy | Four conditions | Configurable: forget it, or keep it |
| Error | Uniform: drop and error now | Uniform: cancel and error now | Uniform: cancel and error now | Broadcast now, then configurable: forget it, or keep it |
| Where members differ | Completion | Admission | Readiness | Reset policy and subject kind |
| Absorbing state | Done | Done | Done | None by default |

Error still never waits: it is broadcast the instant it arrives. But this is the first family where an error is not the end. With `resetOnError: true` the machine forgets it and the next subscriber gets a fresh source. The absorbing state that every previous table had is, here, something you have to ask for.

## What the tables show that the rows could not

- With default switches, complete and error are events the machine recovers from. The catalog's Lifecycle / Terminal component is present per subscriber and absent for the connection.
- The first subscriber is attached before the source is connected. Synchronous sources work.
- The grace notifier is re-armed at every zero crossing and cancelled by any return.
- `shareReplay` treats its two terminals in opposite ways: completion is kept with the buffer, error resets and discards it.
- `refCount: true` on `shareReplay` discards the buffer with the connection. A returning subscriber replays nothing.
- `connectable` strands the subscribers it had when it is disconnected. They never complete.
- The deprecated `publish`, `multicast` with a subject instance, and `refCount` build on one subject that is never replaced, so they have the SH9' and SH12' behavior with no way to switch it. `share()` is the factory-based version with every switch on.

## Relation to the catalog

Tags that would let the rows carry this: `reconnects after complete and error` on `share()`; `complete kept, error resets, buffer dies with the connection under refCount` on `shareReplay`; `no refcount, disconnect strands current subscribers` on `connectable`; and a shared note for the family, `a value with no subscribers is lost, buffered, or never produced, by switch`.

## Next family

The buffering and windowing family is the natural fifth: `buffer`, `bufferTime`, `bufferCount`, `bufferToggle`, `bufferWhen` and their `window` twins. Its question is what happens to an open region when the source completes or errors, and whether overlapping regions each get their own answer. Doc 10 found that `buffer` flushes a partial buffer on complete; the family will not all agree.
