# Step tables for the timing family

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Derived from the RxJS 7.8.2 source of `debounceTime`, `audit`, `sample`, `throttle`, and `debounce` in `src/internal/operators`. Every row is verified by [tests/timing-family-step-tables.test.ts](../tests/timing-family-step-tables.test.ts). Result on 2026-10-06: 38 tests, 38 passing.

## Why this family, and why tables

The catalog rows for `debounceTime`, `auditTime`, `sampleTime`, and `throttleTime` all read much the same: Replace Latest plus Timer / Deadline, sometimes Phase / Flag, with Trigger / Sample as the policy. The rows are correct and they are not enough. They cannot say what happens when the source completes while a value is pending, and that case is decided differently by every member of the family. Only the full transition function on the product state shows it.

A step table is that function written out:

\[
\operatorname{step}(s, e) = (s', a^*)
\]

one row per reachable pair of state and event, with the ordered actions. Rows the invariants make unreachable are listed and marked impossible, so the table is complete, not just illustrative.

## Conventions

**Events.** `next(x)` and `complete` and `error(e)` from the source. `expire` when the armed timer fires. `tick` for the free-running clock of `sampleTime`. `unsubscribe` from downstream. For the selector variants one more: `durComplete`, the duration observable completing without having emitted.

**Actions.** `Next(x)`, `Complete`, `Error(e)`, `Schedule(d)` to arm a timer `d` frames ahead, `Cancel` to disarm it. For the selector variants `Schedule` is a subscription to the duration observable and `Cancel` is its unsubscription. Reaching `Done` always tears down the source; that action is implied and not repeated.

**States.** Named per operator below. `Done` is terminal and absorbing. A dash in the action column means the transition happens and no action is taken. Storing a value is a transition, never an action.

**Witnesses.** Every duration is 3 frames. Marbles are `TestScheduler.run` marbles, one character per frame. Each scenario avoids a source event and a timer expiry on the same frame, so no row depends on same-frame scheduling order. If a real pipeline does collide, the cold source's event is processed before the timer, because its action was scheduled first.

## debounceTime(d)

State: `Idle`, `Pending(x)`, `Done`. Invariant: a value is stored exactly when the timer is armed, so `Idle + expire` cannot occur.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| D1 | Idle | next(x) | Pending(x) | Schedule(d) | `a-----\|` → `---a--\|` |
| D2 | Pending(_) | next(y) | Pending(y) | Cancel, Schedule(d) | `a-b----\|` → `-----b-\|` |
| D3 | Pending(x) | expire | Idle | Next(x) | as D1 |
| D4 | Idle | complete | Done | Complete | as D1, frame 6 |
| D5 | Pending(x) | complete | Done | Cancel, Next(x), Complete | `a-\|` → `--(a\|)` |
| D6 | Pending(x) | error(e) | Done | Cancel, Error(e) | `a-#` → `--#` |
| D7 | Pending(x) | unsubscribe | Done | Cancel | sub `^-!`, source `^-!` |

D2 is the restart. In the source the timer is not literally cancelled and rescheduled; the operator records the arrival time and, when the task wakes, reschedules itself for the remainder if the deadline moved. That is an implementation choice with the same observable behavior, so the table states the semantic action.

D5 is the row the catalog could not express. The pending value is flushed the instant the source completes, one frame before its deadline in the witness.

## auditTime(d)

State: `Idle`, `Pending(x)`, `Draining(x)`, `Done`. `Draining` means the source has completed while a window is open. `Draining + next` and `Draining + error` cannot occur, because the source is already finished.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| A1 | Idle | next(x) | Pending(x) | Schedule(d) | `a-----\|` → `---a--\|` |
| A2 | Pending(_) | next(y) | Pending(y) | — | `a-b----\|` → `---b---\|` |
| A3 | Pending(x) | expire | Idle | Next(x) | as A1 |
| A4 | Idle | complete | Done | Complete | as A1, frame 6 |
| A5 | Pending(x) | complete | Draining(x) | — | `a-\|` → `---(a\|)` |
| A6 | Draining(x) | expire | Done | Next(x), Complete | same witness, frame 3 |
| A7 | Pending(x) | error(e) | Done | Cancel, Error(e) | `a-#` → `--#` |
| A8 | Pending(x), Draining(x) | unsubscribe | Done | Cancel | sub `^-!`, source `^-!` |

A2 against D2 is the steady-state difference: `auditTime` keeps the deadline of the open window, `debounceTime` moves it. A5 and A6 against D5 is the completion difference: `auditTime` holds the pending value until the window closes and only then completes. The source completing does not shorten the window.

## sampleTime(p)

State: `Empty`, `Pending(x)`, `Done`. The clock is armed once at subscribe and ticks every `p` frames regardless of values.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| S0 | — | subscribe | Empty | Schedule clock, period p | `a---b--\|` → `---a--b\|` |
| S1 | Empty | next(x) | Pending(x) | — | `ab-----\|` → `---b---\|` |
| S2 | Pending(_) | next(y) | Pending(y) | — | same witness |
| S3 | Pending(x) | tick | Empty | Next(x) | same witness, frame 3 |
| S4 | Empty | tick | Empty | — | `a------\|` → `---a---\|`, frame 6 |
| S5 | Pending(x) | complete | Done | Cancel clock, Complete | `a-\|` → `--\|` |
| S5' | Empty | complete | Done | Cancel clock, Complete | as S4, frame 7 |
| S6 | any | error(e) | Done | Cancel clock, Error(e) | `a-#` → `--#` |
| S7 | any | unsubscribe | Done | Cancel clock | sub `^-!`, source `^-!` |

S0 is the row that separates `sampleTime` from the other three: values never arm or restart anything. In the witness `b` arrives at frame 4 and waits for the fixed tick at 6, not for frame 7. S5 is the third completion policy: the pending value is dropped.

## throttleTime(d)

State: `Open`, `Locked(∅)`, `Locked(x)`, `Draining(x)`, `Done`. `Locked` means a window is armed; the slot may or may not hold a value. The three configurations share the state space and differ in which rows emit.

### Leading only, the default

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| TL1 | Open | next(x) | Locked(∅) | Next(x), Schedule(d) | `ab--c-\|` → `a---c-\|` |
| TL2 | Locked(_) | next(y) | Locked(y) | — | same witness, `b` is swallowed |
| TL3 | Locked(_) | expire | Open | — | same witness, frame 3 |
| TL4 | Open | complete | Done | Complete | `a----\|` → `a----\|` |
| TL5 | Locked(_) | complete | Done | Cancel, Complete | `ab\|` → `a-\|` |
| TL6 | any | error(e) | Done | Cancel, Error(e) | `a-#` → `a-#` |
| TL7 | Locked(_) | unsubscribe | Done | Cancel | sub `^-!`, source `^-!` |

In this configuration a value stored while locked is never emitted by any row. The implementation keeps it in the slot and lets the next `Open + next` overwrite it. The stale slot is unobservable, so the table treats `Locked(y)` and `Locked(∅)` alike.

### Trailing only

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| TT1 | Open | next(x) | Locked(x) | Schedule(d) | `ab--c-----\|` → `---b--c---\|` |
| TT2 | Locked(_) | next(y) | Locked(y) | — | same witness |
| TT3 | Locked(x) | expire | Locked(∅) | Next(x), Schedule(d) | same witness, frames 3 and 6 |
| TT4 | Locked(∅) | next(y) | Locked(y) | — | same witness, `c` at 4 |
| TT5 | Locked(∅) | expire | Open | — | `a------b\|` → `---a------(b\|)` |
| TT6 | Locked(x) | complete | Draining(x) | — | `a-\|` → `---(a\|)` |
| TT7 | Draining(x) | expire | Done | Next(x), Complete | same witness, frame 3 |
| TT8 | Open, Locked(∅) | complete | Done | Cancel, Complete | `a---\|` → `---a\|` |
| TT9 | Locked(x) | error(e) | Done | Cancel, Error(e) | `a-#` → `--#` |
| TT10 | Locked, Draining | unsubscribe | Done | Cancel | sub `^-!`, source `^-!` |

TT3 is easy to miss. A trailing emission does not open the operator; it starts a new window. A value arriving right after a trailing emit is locked again and waits for the next expiry. TT5 shows the window that does open the operator: one that expires with nothing stored. The TT5 witness then runs into TT6 and TT7, because `b` at frame 7 is still pending when the source completes at 8.

TT8 matters for completion: if the slot is empty when the source completes, `throttleTime` completes at once even though a window is still armed. Only a stored value makes it wait.

### Leading and trailing

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| TB1 | Open | next(x) | Locked(∅) | Next(x), Schedule(d) | `ab--c-----\|` → `a--b--c---\|` |
| TB2 | Locked(_) | next(y) | Locked(y) | — | same witness |
| TB3 | Locked(y) | expire | Locked(∅) | Next(y), Schedule(d) | same witness, frames 3 and 6 |
| TB4 | Locked(∅) | expire | Open | — | same witness, frame 9 |
| TB5 | Locked(y) | complete | Draining(y) | — | `ab\|` → `a--(b\|)` |
| TB6 | Draining(y) | expire | Done | Next(y), Complete | same witness, frame 3 |
| TB7 | Open, Locked(∅) | complete | Done | Cancel, Complete | `a-\|` → `a-\|` |
| TB8 | Locked(y) | error(e) | Done | Cancel, Error(e) | `ab#` → `a-#` |
| TB9 | Locked, Draining | unsubscribe | Done | Cancel | sub `^-!`, source `^-!` |

## The cross-cutting cases

These are the cells where the family splits. The catalog rows cannot carry them; the tables do.

| Case | debounceTime | auditTime | sampleTime | throttleTime leading | throttleTime trailing, both |
|---|---|---|---|---|---|
| Source completes while a value is pending | Flush now, then complete (D5) | Wait for the window, emit, complete (A5, A6) | Drop, complete now (S5) | Drop, complete now (TL5) | Wait for the window, emit, complete (TT6, TT7, TB5, TB6) |
| Source completes with the timer armed but nothing pending | impossible | impossible | Complete now (S5') | Complete now (TL5) | Complete now (TT8, TB7) |
| A new value arrives while the timer is armed | Restart the deadline (D2) | Keep the deadline (A2) | No timer to touch (S2) | Keep the deadline (TL2) | Keep the deadline (TT2, TB2) |
| The timer fires with nothing pending | impossible | impossible | Nothing (S4) | Open (TL3) | Open (TT5, TB4) |
| The timer fires with a value pending | Emit, Idle (D3) | Emit, Idle (A3) | Emit, Empty (S3) | n/a, the slot is dead | Emit, start a new window (TT3, TB3) |
| Error while a value is pending | Drop, error now | Drop, error now | Drop, error now | Error now | Drop, error now |
| Unsubscribe while pending | Cancel, release source | Cancel, release source | Cancel clock, release source | Cancel, release source | Cancel, release source |

Four operators, three completion policies: flush, wait, drop. The `throttleTime` configurations sit on two of them. Error is uniform across the family: no operator flushes on error.

## The selector variants and the extra event

`auditTime`, `sampleTime`, and `throttleTime` are defined as `audit`, `sample`, and `throttle` over a `timer` or `interval`. `debounceTime` is a separate implementation but behaves as `debounce` over a `timer`. The selector variants accept any duration observable, which adds one event the time variants never see: `durComplete`, the duration completing without emitting. That event is unreachable for the time variants, because `timer` emits before it completes and the emit handler unsubscribes the duration first.

It introduces one more state. `Stranded(x)`: a value is stored, no duration is active, and the source is still live.

| Row | Operator | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|---|
| V1 | debounce | Pending(x) | durComplete | Stranded(x) | — | `a----\|` → `-----(a\|)` |
| V1' | debounce | Stranded(x) | complete | Done | Next(x), Complete | same witness |
| V2 | audit | Pending(x) | durComplete | Stranded(x) | — | `a----\|` → `-----\|` |
| V2' | audit | Stranded(x) | complete | Done | Complete, x dropped | same witness |
| V3 | audit | Draining(x) | durComplete | Done | Complete, x dropped | `a\|` → `--\|` |
| V4 | audit | Stranded(x) | next(y) | Pending(y) | Schedule, x lost | `a---b----\|` → `-----b---\|` |
| V5 | throttle, trailing | Locked(x) | durComplete | Open, slot stale | — | `ab---\|` → `a----\|` |
| V5' | throttle, trailing | Draining(x) | durComplete | Done | Complete, x dropped | by inspection of `cleanupThrottling` |
| V6 | sample | any | notifier complete | same, clock dead | — | `a----\|` → `-----\|` |

`debounce` is the only one that survives a silent duration with its value intact: V1' flushes it on complete because the flush checks the slot, not the duration. `audit` and `throttle` strand the value and then drop it, either at once if the source has already completed or later when a new value overwrites it. `sample` loses its clock for good, so nothing stored afterwards ever leaves.

For `debounce`, `Stranded(x) + next(y)` goes to `Pending(y)` with a fresh duration, like D2 without the cancel. For `throttle` in `Open` with a stale slot, the next value behaves exactly as from a clean `Open`: leading emits it, trailing-only locks on it. The stale value is never emitted.

## What the tables show that the rows could not

- Completion policy is a property of the operator, not of the family. Three policies across four operators, and `throttleTime` changes policy with its configuration.
- Restart versus keep is the whole steady-state difference between `debounceTime` and `auditTime`. On completion they differ again, in the opposite direction from what the steady state suggests: the one that restarts flushes early, the one that keeps waits.
- A trailing emit from `throttleTime` opens a new window, not the operator. Values arriving right after a trailing emit are locked out for a full duration.
- `throttleTime` with trailing waits on complete only if something is stored. An armed window with an empty slot does not delay completion.
- `sampleTime` is the only member whose timer is not driven by values at all. The pattern label Timer / Deadline is shared, the transition rules are not.
- A silent duration is a real event in the selector variants, with a real state behind it. Each operator answers it differently, and two of the four lose data.

## Relation to the catalog

Nothing here contradicts a catalog row. The rows name the components of the product state and the policies. The tables give the transition function on that product. The one addition worth carrying back into [05](05-operator-catalog.md) is the completion policy as a tag in the action column, since it is the cell readers most often get wrong: `flush on complete` for `debounceTime`, `wait on complete` for `auditTime` and trailing `throttleTime`, `drop on complete` for `sampleTime` and leading `throttleTime`.

## Next family

The same treatment fits the flattening family, where the open question is the mirror image of this one: what happens when the outer completes while inners are active. `mergeMap`, `concatMap`, `switchMap`, and `exhaustMap` give four different answers about which inners finish and when the output completes, and `concatMap` adds a queue that must drain.
