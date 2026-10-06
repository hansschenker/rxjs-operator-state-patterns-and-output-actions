# Step tables for the join family

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Derived from the RxJS 7.8.2 source of `combineLatestInit`, `zip`, `forkJoin`, and `withLatestFrom`. Every row is verified by [tests/join-family-step-tables.test.ts](../tests/join-family-step-tables.test.ts). Result on 2026-10-06: 16 tests, 16 passing.

## The third shape

[Doc 10](10-timing-family-step-tables.md) asked what happens when the source completes while a timer is armed. [Doc 11](11-flattening-family-step-tables.md) asked what happens when the outer completes while inners are active. The join family has a third shape: several sources, none of them privileged except in `withLatestFrom`, and the question is what one source's completion means for the others. Three cases matter. A source completes before it has emitted. A source completes after emitting, while the others continue. A source completes with values still queued, which only `zip` can have.

The answers differ in every cell, and one of them corrects a sentence in [doc 07](07-catalog-evaluation.md).

## Conventions

**Events.** Per source `i`: `next_i(x)`, `complete_i`, `error_i`. For `withLatestFrom` the primary and the secondaries are different roles, so the events are `pnext`, `pcomplete`, `perror` and `snext`, `scomplete`, `serror`. From downstream: `unsubscribe`.

**Actions.** `Next([...])`, `Complete`, `Error(e)`. Reaching `Done` cancels every source; that is implied. Filling a slot or pushing onto a queue is a transition.

**States.** Each operator keeps one cell per source: a slot that holds the latest value, or a queue. `Filling` or `NotReady` means some cell is still empty. `Ready` means every cell has been filled at least once. A live count or completed flags record which sources are finished. `Done` is terminal.

**Impossible pairs.** A source completes at most once, so `complete_i` for an already completed source does not occur. After `Done` nothing occurs.

**Witnesses.** Two sources per scenario, `s1` and `s2`, both cold. Scenarios avoid two events on the same frame where the order would change the output. Where two events do share a frame, both orders give the same result and the text says so.

## combineLatest([s1, s2])

State: a slot per source, a live count `a`. `Filling` while any slot is empty, `Ready` after.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| CL1 | Filling, slot_i empty, the last empty one | next_i(x) | Ready | Next([slots]) | `a-b---\|`, `---x-y\|` → `---p-q\|`, p = [b, x] |
| CL1' | Filling, slot_i empty, others still empty | next_i(x) | Filling | — | |
| CL2 | Filling, slot_i full | next_i(x) | Filling | — | same witness, `b` at 2 overwrites `a` |
| CL3 | Ready | next_i(x) | Ready | Next([slots]) | same witness, q = [b, y] at 5 |
| CL4 | Filling, a ≥ 2 | complete_i | Filling, a−1 | — | `-\|`, `x-y---\|` → `------\|` |
| CL5 | Ready, a ≥ 2 | complete_i | Ready, a−1 | — | `a\|`, `--x-y\|` → `--p-q\|` |
| CL6 | any, a = 1 | complete_i | Done | Complete | every witness's final frame |
| CL7 | any | error_i | Done | Error(e) | `a-#`, `-x---\|` → `-p#`; s2 `^-!` |
| CL8 | any | unsubscribe | Done | — | X1 |

CL4 is the row that matters. A source that completes without emitting does not end the join and does not make it emit. Readiness can now never be reached, yet the join stays subscribed to everything else until the live count hits zero. In the witness `s2` runs to its own completion at frame 6 and the output is six frames of silence. CL5 is the retention rule: a completed source's last value keeps combining with later values from the others.

## zip([s1, s2])

State: a queue per source with length `q_i`, a completed flag `c_i` per source.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| Z1 | some other queue empty | next_i(x) | q_i + 1 | — | `ab--\|`, `--x-y\|` → `--p-(q\|)` |
| Z2 | every other queue non-empty | next_i(x) | every queue −1 | Next([heads]) | same witness, p = [a, x] at 2 |
| Z2' | after Z2, some j with q_j = 0 and c_j | — | Done | Complete | same witness, frame 4 |
| Z3 | q_i = 0 | complete_i | Done | Complete | `-\|`, `xy----\|` → `-\|`; s2 `^!` |
| Z4 | q_i ≥ 1 | complete_i | c_i = true | — | `ab\|`, `---x-y\|` → `---p-(q\|)` |
| Z5 | Z2' reached by draining a completed queue | | Done | Complete | same witness, frame 5, before s2 completes at 6 |
| Z6 | any | error_i | Done | Error(e) | `a-#`, `x----\|` → `p-#`; s2 `^-!` |
| Z7 | any | unsubscribe | Done | — | X2 |

`zip` is the only member whose readiness is not a one-time event. Every emission requires every queue to be non-empty, and that is re-evaluated on each arrival. It is also the only member that never discards a value before readiness: values queue (Z1). The price is Z3: a source that completes with nothing queued ends the join at once, and whatever the other sources had queued is dropped. In the witness `x` and `y` are lost at frame 1. Z4 and Z5 are the opposite case: a source that completes with a backlog keeps pairing until the backlog is gone, and the join completes at that moment, ahead of the other source's own completion.

## forkJoin([s1, s2])

State: a slot per source, a has-value flag per source, remaining completions `c`, remaining emissions `e`. The actions fire in the finalizer that runs after a source completes.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| FJ1 | any | next_i(x) | slot_i = x; e−1 on the first | — | `ab\|`, `--x-\|` → `----(p\|)`, p = [b, x] |
| FJ2 | has value, c ≥ 2 | complete_i | c−1 | — | same witness, frame 2 |
| FJ3 | has value, c = 1 | complete_i | Done | Next([slots]), Complete | same witness, frame 4 |
| FJ4 | no value yet | complete_i | Done | Complete | `-\|`, `x---\|` → `-\|`; s2 `^!` |
| FJ5 | any | error_i | Done | Error(e) | `a-#`, `x----\|` → `--#`; s2 `^-!` |
| FJ6 | any | unsubscribe | Done | — | X3 |

FJ4 is the short-circuit. A source that completes without a value makes the join complete now, without emitting, and cancel the others. In FJ3 the emission count is necessarily zero, because any source that completed empty would have fired FJ4 first. FJ1 shows that only the last value per source is kept: `a` is overwritten by `b`, and the single emission is `[b, x]`.

## primary.pipe(withLatestFrom(secondary))

State: a slot per secondary, `NotReady` while any is empty, `Ready` after. The primary has no slot.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| WL1 | NotReady, the last empty slot | snext(x) | Ready | — | `a-b-c-\|` with `-x-y-\|` → `--p-q-\|` |
| WL1' | NotReady, other slots still empty | snext(x) | NotReady | — | |
| WL2 | Ready | snext(x) | Ready | — | same witness, `y` at 3 replaces `x` |
| WL3 | NotReady | pnext(v) | NotReady | — | same witness, `a` at 0 is dropped |
| WL4 | Ready | pnext(v) | Ready | Next([v, slots]) | same witness, p = [b, x], q = [c, y] |
| WL5 | any | scomplete | same | — | `---a-\|` with `x\|` → `---p-\|` |
| WL6 | any | serror(e) | Done | Error(e) | `a-b--\|` with `x-#` → `p-#`; primary `^-!` |
| WL7 | any | pcomplete | Done | Complete | `a-\|` with `----x\|` → `--\|`; secondary `^-!` |
| WL8 | any | perror(e) | Done | Error(e) | default forwarding |
| WL9 | any | unsubscribe | Done | — | X4 |

The asymmetry is the whole operator. A secondary's completion is a no-op (WL5): its last value stays in the slot and keeps being read. A secondary's error is fatal (WL6). The primary's completion ends everything (WL7), even when readiness was never reached. And WL3 is data loss by design: primary values that arrive before every secondary has spoken are dropped, not queued.

## The cross-cutting cases

| Case | combineLatest | zip | forkJoin | withLatestFrom |
|---|---|---|---|---|
| A source completes before emitting | Nothing can ever be emitted; silence until all complete (CL4, CL6) | Complete now; other queues dropped (Z3) | Complete now, no value (FJ4) | Secondary: ignored, never ready, primary values dropped (WL5, WL3). Primary: complete now (WL7) |
| A source completes after emitting | Its last value keeps combining; completion waits for the rest (CL5) | Its backlog keeps pairing; complete when the backlog drains (Z4, Z5) | Its last value is kept; emit once when all complete (FJ2, FJ3) | Secondary: ignored, value retained (WL5). Primary: complete (WL7) |
| Values before readiness | Overwrite the slot (CL2) | Queue, nothing lost (Z1) | Overwrite the slot (FJ1) | Primary: dropped (WL3). Secondary: overwrite |
| When output is emitted | Every update after readiness (CL1, CL3) | Whenever every queue is non-empty (Z2) | Once, at the last completion (FJ3) | Every primary value after readiness (WL4) |
| What is combined | Latest per slot | Head of each queue | Last value per slot | Primary value plus latest per secondary slot |
| Completion condition | All sources complete | A completed source's queue is empty | All complete, or any completes empty | Primary completes |
| Any source errors | Error now, cancel the rest | same | same | same, secondary or primary |

Four completion conditions, four readiness policies, and no two members agree on what to do with a source that completes empty. Error is uniform here as it was in the other two families.

## Correction to doc 07

The Terminate paragraph in [07](07-catalog-evaluation.md) originally said that `combineLatest` completes immediately when any source completes without emitting. CL4 shows it does not. That rule is FJ4, and it belongs to `forkJoin` alone. `combineLatest` has no early exit: it counts live sources down to zero and completes then, silently if it never became ready. The paragraph has been corrected and points here. The Terminate tag on the `combineLatest` row stands, because counting down to zero is still a completion the operator decides; only the reason given for it was wrong.

The practical consequence is worth stating plainly. `combineLatest` of an infinite source and a source that completes empty never emits and never completes. `forkJoin` of the same pair completes at once.

## Contrast with the first two families

| | Timing | Flattening | Join |
|---|---|---|---|
| Question shape | Source completes while a timer is armed | Outer completes while inners are active | A source completes before, after, or with a backlog |
| Completion | Three policies | One policy: wait and drain | Four conditions, one per operator |
| Error | Uniform: drop and error now | Uniform: cancel everything, error now | Uniform: cancel everything, error now |
| Where members differ | Completion and deadline handling | Admission and the `project` contract | Readiness, what is combined, completion condition |
| States the rows do not name | Draining, Stranded | Draining, queue length | Filling with a dead source, a completed source's backlog |

Across all three families error has never once waited. Completion has waited, flushed, dropped, drained, short-circuited, and been ignored. If a reader takes one rule away from twelve step tables, it is that completion is where operators differ and error is where they agree.

## What the tables show that the rows could not

- `combineLatest` and `forkJoin` share a row, Slot Table + Readiness, and answer an empty source in opposite ways. One waits for everyone, the other gives up at once.
- `zip` is the only join that loses nothing before readiness, and the only one that can complete with another source's values still unconsumed.
- `withLatestFrom` ignores a secondary's completion but not its error. The two terminal events of the same source are treated as unrelated.
- A completed source is not a dead source in `combineLatest` and `withLatestFrom`. Its last value keeps participating.
- Readiness is a one-time event for three members and a per-emission check for `zip`. That is the structural reason `zip` needs queues and the others need slots.

## Relation to the catalog

Tags that would let the rows carry this: `completes when all complete` on `combineLatest`; `completes when a finished queue empties, queues before ready` on `zip`; `short-circuits on an empty source` on `forkJoin`; `secondary completion ignored, primary dropped before ready` on `withLatestFrom`.

## Next family

Multicasting is the one family where terminal is not absorbing. `share` with its three reset options rebuilds the connection after complete, after error, and after the last subscriber leaves, and `shareReplay` keeps a buffer across those resets. The question shape is a fourth one: what happens to the connection when the subscriber count changes, and whether a terminal event is the end of anything at all.
