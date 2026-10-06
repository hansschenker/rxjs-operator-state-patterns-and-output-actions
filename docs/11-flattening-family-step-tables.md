# Step tables for the flattening family

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Derived from the RxJS 7.8.2 source of `mergeInternals`, `switchMap`, and `exhaustMap` in `src/internal/operators`. `mergeMap` and `concatMap` are `mergeInternals` with concurrency `Infinity` and `1`; the `*All` variants are the same machines with an identity project. Every row is verified by [tests/flattening-family-step-tables.test.ts](../tests/flattening-family-step-tables.test.ts). Result on 2026-10-06: 21 tests, 21 passing.

## The mirror question

[Doc 10](10-timing-family-step-tables.md) asked what happens when the source completes while a timer is armed, and found three answers in one family. The flattening family has the mirror question: what happens when the outer completes while inners are still running. The answer turns out to be one answer, shared by all four operators. The differences the catalog rows cannot hold are elsewhere: when the user's `project` function is called, which index it receives, and what happens to queued work under completion versus error.

## Conventions

**Events.** From the outer: `onext(x)`, `ocomplete`, `oerror(e)`. From an inner: `inext(y)`, `icomplete`, `ierror(e)`. From downstream: `unsubscribe`.

**Actions.** `Project(x, i)` calls the user's function; it is an action because user code observes it. `Subscribe` and `Unsubscribe` of an inner. `Next(y)`, `Complete`, `Error(e)`. Reaching `Done` tears down the outer and every inner; that is implied and not repeated. Storing a value in the queue is a transition, not an action.

**States.** Shared vocabulary: `Idle` means the outer is live and no inner is active. `Draining` means the outer has completed while inners, or queued values, remain. `Done` is terminal and absorbing. Each operator adds its own detail: a count `n`, a queue length `q`, or a single `Current` or `Busy` flag.

**Impossible pairs.** `Draining` cannot receive `onext`, `ocomplete`, or `oerror`, because the outer has finished. `Idle` cannot receive `inext`, `icomplete`, or `ierror`, because no inner exists. These rows are omitted from every table below for that reason.

**Witnesses.** Inner observables are cold marbles keyed by the outer value, `a-a|` for `a` and `b-b|` for `b` unless stated. Subscription marbles show when each inner was opened and cancelled. A probe records every `Project` call with its frame and index. Each scenario avoids a dequeue and another event on the same frame where the order would change the output.

## mergeMap(project), unbounded

State: `Idle`, `Active(n)` with `n ≥ 1`, `Draining(n)`, `Done`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| M1 | Idle, Active(n) | onext(x) | Active(n+1) | Project(x, i), Subscribe | outer `ab\|` → `abab\|`; a `^--!`, b `-^--!` |
| M2 | Active(n), Draining(n) | inext(y) | same | Next(y) | same witness |
| M3 | Active(n) | icomplete | Active(n−1), or Idle | — | same witness, frame 3 |
| M4 | Draining(n), n ≥ 2 | icomplete | Draining(n−1) | — | same witness, frame 3 |
| M5 | Draining(1) | icomplete | Done | Complete | same witness, frame 4 |
| M6 | Idle | ocomplete | Done | Complete | outer `a---\|`, inner `a\|` → `a---\|` |
| M7 | Active(n) | ocomplete | Draining(n) | — | first witness, frame 2 |
| M8 | Idle, Active(n) | oerror(e) | Done | Error(e) | outer `a-#`, inner `a--a\|` → `a-#`; inner `^-!` |
| M9 | Active(n), Draining(n) | ierror(e) | Done | Error(e) | inner a `a-#`, b `b-b\|`, outer `ab---\|` → `ab#`; b `-^!`, outer `^-!` |
| M10 | any | unsubscribe | Done | — | X1 |

`Project` runs at arrival with the source position as index: `(a, 0)` at frame 0, `(b, 1)` at frame 1. M7 then M4 then M5 is the mirror question answered: the outer completes at frame 2 with two inners live, nothing is emitted for it, and the output completes at frame 4 when the last inner does.

## mergeMap(project, c), bounded

State adds a queue: `Active(n, q)`, `Draining(n, q)`. Invariant: `q ≥ 1` implies `n = c`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| MB1 | Idle, Active(n, 0) with n < c | onext(x) | Active(n+1, 0) | Project(x, i), Subscribe | outer `abc\|`, c = 2 → `abacbc\|` |
| MB2 | Active(c, q) | onext(x) | Active(c, q+1) | — | same witness, `c` at frame 2 |
| MB3 | Active(c, q ≥ 1), Draining(c, q ≥ 1) | icomplete | same n, q−1 | Project(head, i), Subscribe | same witness, `c` projected at frame 3 |
| MB4 | Active(n, 0) | icomplete | Active(n−1, 0), or Idle | — | same witness, frame 5 |
| MB5 | Draining(1, 0) | icomplete | Done | Complete | same witness, frame 6 |
| MB6 | Active(n, q) | ocomplete | Draining(n, q) | — | same witness, frame 3 |
| MB7 | Idle | ocomplete | Done | Complete | as M6 |
| MB8 | Active(n, q) | oerror(e) | Done | Error(e), queue dropped | as C9 |
| MB9 | Active, Draining | ierror(e) | Done | Error(e), queue dropped | as C10 |

In the witness the inners are `a-a|`, `b--b|`, `c-c|`. `c` arrives at frame 2 with two inners live and is stored without being projected. `a` completes at frame 3; the probe records `(c, 2)` at frame 3, and `c`'s subscription opens at 3. The outer completed at 3 as well, so the state is `Draining(2, 0)` from there, and the output completes at 6 when `c`'s inner does. MB2 against MB1 is the difference the catalog's "FIFO Queue only when `concurrent` is bounded" was pointing at.

## concatMap(project)

`mergeInternals` with `c = 1`. State: `Idle`, `Busy(q)`, `Draining(q)`, `Done`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| C1 | Idle | onext(x) | Busy(0) | Project(x, i), Subscribe | outer `ab\|` → `a-ab-b\|` |
| C2 | Busy(q) | onext(x) | Busy(q+1) | — | same witness, `b` at frame 1 |
| C3 | Busy(q ≥ 1), Draining(q ≥ 1) | icomplete | q−1 | Project(head, i), Subscribe | same witness, `b` projected at 3; b `---^--!` |
| C4 | Busy(0) | icomplete | Idle | — | by MB4 |
| C5 | Draining(0) | icomplete | Done | Complete | same witness, frame 6 |
| C6 | Busy(q) | ocomplete | Draining(q) | — | same witness, frame 2 |
| C7 | Idle | ocomplete | Done | Complete | as M6 |
| C8 | Busy, Draining | inext(y) | same | Next(y) | same witness |
| C9 | Busy(q) | oerror(e) | Done | Error(e), queue dropped | outer `ab#`, a `a--a\|` → `a-#`; a `^-!`, b never subscribed, probe `[a]` |
| C10 | Busy, Draining | ierror(e) | Done | Error(e), queue dropped | a `a-#`, outer `ab---\|` → `a-#`; b never subscribed, outer `^-!` |
| C11 | any | unsubscribe | Done | — | X2 |

C2 and C3 together say something the row "FIFO Queue + Serialize" does not: `project` is not called when a value arrives. It is called when the value is dequeued. The probe shows `(b, 1)` at frame 3, two frames after `b` arrived. Side effects inside `project` happen late, and a `project` that reads mutable state reads it at dequeue time. C6 then C3 then C5 is the mirror question: the outer completes at 2 with `b` still queued, `b` is still projected and run, and the output completes at 6. C9 is the asymmetry: on error the queue is dropped, nothing queued is ever projected.

## switchMap(project)

State: `Idle`, `Current`, `Draining`, `Done`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| SW1 | Idle | onext(x) | Current | Project(x, i), Subscribe | outer `ab\|` → `ab-b\|` |
| SW2 | Current | onext(x) | Current | Unsubscribe(current), Project(x, i), Subscribe | same witness; a `^!`, b `-^--!` |
| SW3 | Current, Draining | inext(y) | same | Next(y) | same witness |
| SW4 | Current | icomplete | Idle | — | outer `a---\|`, inner `a\|`, frame 1 |
| SW5 | Draining | icomplete | Done | Complete | first witness, frame 4 |
| SW6 | Idle | ocomplete | Done | Complete | outer `a---\|`, inner `a\|` → `a---\|` |
| SW7 | Current | ocomplete | Draining | — | first witness, frame 2 |
| SW8 | Current | oerror(e) | Done | Error(e) | outer `a-#`, inner `a--a\|` → `a-#`; inner `^-!` |
| SW9 | Current, Draining | ierror(e) | Done | Error(e) | inner `a-#`, outer `a---\|` → `a-#`; outer `^-!` |
| SW10 | any | unsubscribe | Done | — | X3 |

The action order in SW2 is verified, not assumed: when `project` runs for `b`, the subscription to `a`'s inner is already closed. A `project` that tears down a resource belonging to the previous inner can rely on that.

## exhaustMap(project)

State: `Idle`, `Busy`, `Draining`, `Done`.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| E1 | Idle | onext(x) | Busy | Project(x, i), Subscribe | outer `ab\|` → `a-a\|` |
| E2 | Busy | onext(x) | Busy | — | same witness; b never subscribed, probe `[(a, 0)]` |
| E3 | Busy, Draining | inext(y) | same | Next(y) | same witness |
| E4 | Busy | icomplete | Idle | — | outer `ab--c\|`, frame 3 |
| E5 | Draining | icomplete | Done | Complete | first witness, frame 3 |
| E6 | Idle | ocomplete | Done | Complete | outer `a---\|`, inner `a\|` → `a---\|` |
| E7 | Busy | ocomplete | Draining | — | first witness, frame 2 |
| E8 | Busy | oerror(e) | Done | Error(e) | outer `a-#`, inner `a--a\|` → `a-#`; inner `^-!` |
| E9 | Busy, Draining | ierror(e) | Done | Error(e) | inner `a-#`, outer `a---\|` → `a-#`; outer `^-!` |
| E10 | any | unsubscribe | Done | — | X4 |

E2 has a consequence the catalog's Count / Index tag hides. The index is advanced only when `project` is called, and `project` is not called for a dropped value. In the witness `ab--c|` with `a-a|` inners, `b` is dropped at frame 1 and `c` is admitted at frame 4 with index 1, not 2. `exhaustMap`'s index counts admitted values. Every other member of the family passes the source position.

## The cross-cutting cases

| Case | mergeMap | mergeMap(c) | concatMap | switchMap | exhaustMap |
|---|---|---|---|---|---|
| Outer next while an inner is active | Admit, run alongside (M1) | Admit below c, queue at c (MB1, MB2) | Queue (C2) | Cancel current, admit new (SW2) | Drop (E2) |
| When `project` runs | At arrival | At arrival, or at dequeue if queued | At arrival for the first, at dequeue for the rest (C3) | At arrival, after the cancel (SW2) | At arrival, only when idle |
| Index passed to `project` | Source position | Source position | Source position | Source position | Admitted count (E2) |
| Outer completes while inners are active | Wait for all (M7) | Wait for all and the queue (MB6) | Wait for the inner and the queue (C6) | Wait for the current (SW7) | Wait for the current (E7) |
| Outer completes with nothing active | Complete now | Complete now | Complete now | Complete now | Complete now |
| Inner completes with queued work | n/a | Dequeue, project, subscribe (MB3) | Dequeue, project, subscribe (C3) | n/a | n/a |
| Inner error | Error now, cancel outer and other inners (M9) | Same, drop the queue | Error now, cancel outer, drop the queue (C10) | Error now, cancel outer (SW9) | Error now, cancel outer (E9) |
| Outer error while inners are active | Error now, cancel inners (M8) | Same, drop the queue | Error now, cancel inner, drop the queue (C9) | Error now, cancel inner (SW8) | Error now, cancel inner (E8) |
| Unsubscribe | Cancel everything | Cancel everything, drop the queue | Same | Same | Same |

Completion is uniform. Every member waits for whatever is active, and `concatMap` and bounded `mergeMap` also drain their queue before completing. Error is uniform too, and asymmetric to completion: an error ends everything at once and drops the queue. The family differs in admission, which is the row the catalog already carries as Merge / Interleave, Serialize, Current-Wins, and Gate / Suppress. The two things the rows could not carry are the timing of `project` and the meaning of its index.

## Contrast with the timing family

| | Timing family | Flattening family |
|---|---|---|
| Completion while pending | Three policies: flush, wait, drop | One policy: wait, and drain |
| Error while pending | Uniform: drop and error now | Uniform: cancel everything and error now |
| Where the members differ | Completion and deadline handling | Admission, and the `project` contract |
| State the rows do not name | Draining, Stranded | Draining, the queue length |

The two families are each other's complement. Timing operators agree on admission, since every value is stored, and disagree on completion. Flattening operators agree on completion and disagree on admission.

## What the tables show that the rows could not

- The mirror question has one answer: the outer's completion is never the operator's completion while an inner is live. All four need the `Draining` state, which no pattern names.
- `concatMap` and bounded `mergeMap` call `project` at dequeue, not at arrival. Side effects in `project` run late, and a queued value's `project` is never called if an error arrives first.
- `exhaustMap` passes an index that counts admitted values. A consumer that uses the index to correlate with source positions will be wrong after the first drop.
- In `switchMap`, the cancel of the previous inner happens before `project` is called for the new one.
- Completion drains the queue; error drops it. The two terminal events treat pending work in opposite ways.
- `expand` and `mergeScan` are built on the same `mergeInternals`, so the MB rows are theirs as well. `expand` adds a `Next(x)` to every `Project` row and feeds inner values back as outer events; `mergeScan` replaces the stored value with an accumulator.

## Relation to the catalog

Nothing contradicts a row. Three tags would let the rows carry what the tables found: `project at dequeue` on `concatMap`, `mergeMap`, `expand`, and `mergeScan`; `index counts admitted` on `exhaustMap`; and `queue drains on complete, drops on error` on `concatMap` and `mergeMap`.

## Next family

The join creators are the natural third: `combineLatest`, `zip`, `forkJoin`, `withLatestFrom`. Their open question is a third shape again: what happens when one source completes before it has emitted, or completes with values still queued. `zip` in particular completes only when a finished source's queue is empty, which no row says.
