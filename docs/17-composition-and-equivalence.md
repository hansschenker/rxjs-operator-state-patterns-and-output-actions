# Composition and equivalence

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Verified by [tests/composition-and-equivalence.test.ts](../tests/composition-and-equivalence.test.ts) against RxJS 7.8.2. Result on 2026-10-06: 23 tests, 23 passing. Every law marked "holds" was checked on 200 randomly generated sources with a fixed seed, so each check is reproducible.

## What was left

Seven families took single operators to their full machines. Two parts of the Mealy model were still unused. Mealy machines compose, and `pipe` is their composition. Mealy machines have an equivalence, and two pipelines that are equivalent can be swapped. This document does both, in the same way as the families: state the rule, then verify it, and record where it fails.

## Part 1. Composition

### The cascade rule

Two machines in series, \(M_1\) feeding \(M_2\), form one machine whose state is the product \(S_1 \times S_2\). One step of the composite on an event \(e\) from the source:

1. Run \(M_1\): \(\operatorname{step}_1(s_1, e) = (s_1', a_1^*)\).
2. Walk \(a_1^*\) in order. Each action that is a downstream notification is fed to \(M_2\) as its event: \(\operatorname{step}_2(s_2, n) = (s_2', a_2^*)\), and \(a_2^*\) joins the composite's actions. Each action that is a resource action of \(M_1\), such as a Schedule or a Subscribe of an inner, joins the composite's actions directly.
3. If \(M_2\) reaches `Done` during the walk, stop. The remaining notifications in \(a_1^*\) are not delivered, and the composite emits `Unsubscribe` upstream, which takes \(M_1\) to `Done` through its own unsubscribe row.

The composite is `Done` exactly when \(M_2\) is `Done`. \(M_1\) reaching `Done` by completing only hands \(M_2\) a `complete` event, which \(M_2\) may hold.

Three consequences are not obvious from the rule and each has a witness.

### C1. A multi-action step is not atomic

`takeLast(2)` flushes `b`, `c`, and `complete` in one step at frame 3. Under `take(1)` the walk delivers `b`, `take` completes and is `Done`, and `c` is never delivered.

```text
abc|   takeLast(2), take(1)   ->   ---(b|)
```

\(M_2\)'s state changes between \(M_1\)'s actions. A table row's action list is an ordered sequence, not a set, and composition is where the order matters.

### C2. Downstream Done cancels upstream in the middle of its step

`tap` records every value it sees; `take(1)` follows it. The probe sees only `a`. The source is unsubscribed inside the delivery of `a`, before `b` is ever scheduled to run.

```text
abc|   tap(probe), take(1)   ->   (a|)      probe: [a]      source: (^!)
```

### C3. Downstream Done propagates upstream as unsubscribe, all the way

`mergeMap` with two inners, then `take(1)`. The first inner value completes the composite at frame 0. The outer is released, the live inner is cancelled, and the second inner, which the outer would have started at frame 1, is never subscribed.

```text
outer ab|   inner a = a-a|, b = b-b|
mergeMap, take(1)   ->   (a|)      outer: (^!)     inner a: (^!)     inner b: never
```

This is why every step table in docs 10 to 16 has an unsubscribe row. Those rows looked like housekeeping in isolation. In composition they are the rows that run whenever a later stage decides to stop.

### C4. The composite's Done is the downstream machine's Done

`take(1)` is `Done` at frame 0 and releases the source there. `delay(5)` holds the value and the completion. The composite completes at frame 5.

```text
a-b|   take(1), delay(5)   ->   -----(a|)      source: (^!)
```

### A composite table, derived

The rule produces a table from two tables. `scan(+, 0)` has state `Acc(s)`; `take(2)` has `Counting(k)`. Their cascade:

| Composite state | Event | Composite state' | Actions |
|---|---|---|---|
| Acc(s), Counting(0) | next(x) | Acc(s+x), Counting(1) | Next(s+x) |
| Acc(s), Counting(1) | next(x) | Done | Next(s+x), Complete, Unsubscribe(source) |
| Acc(s), Counting(k) | complete | Done | Complete |
| Acc(s), Counting(k) | error(e) | Done | Error(e) |

Each row is read off the two component tables by the walk in the cascade rule. Nothing about the composite had to be observed; it was computed, and the witness confirms it.

## Part 2. Equivalence

### The definition

Two pipelines are trace-equivalent when, for every source, they produce the same notifications at the same frames. For deterministic machines this is the same as bisimulation, and it is the relation that lets one pipeline replace another. A marble test checks trace equality for one source. A law claims it for all sources. The gap between them is what this part is about.

The suite closes most of the gap with a generator. A source is one to eight frames, each a letter or silence, ending in `complete`, `error`, or nothing. A law is checked by running both sides on 200 such sources from a fixed seed and comparing the traces. A failure returns the source that broke it. This is not a proof, but it is two hundred witnesses instead of one, and every counterexample below was found this way or confirmed this way.

### Laws that hold

| Row | Law | Checked on |
|---|---|---|
| L1 | `take(n) ∘ take(m)` = `take(min(n, m))` | 200 sources, n and m in 0..3 |
| L2 | `skip(n) ∘ skip(m)` = `skip(n + m)` | 200 sources, n and m in 0..2 |
| L3 | `map(f) ∘ map(g)` = `map(g ∘ f)` | 200 sources |
| L4 | `filter(p) ∘ filter(q)` = `filter(p ∧ q)`, predicates ignoring the index | 200 sources |
| L5 | `delay(d) ∘ delay(e)` = `delay(d + e)`, errors included | 200 sources, d and e in 1..3 |
| L6 | `distinctUntilChanged` is idempotent | 200 sources |
| L7 | `defaultIfEmpty(d) ∘ defaultIfEmpty(e)` = `defaultIfEmpty(d)` | 200 sources |
| L9 | `concatMap(f)` = `map(f) ∘ concatAll()` on the trace | 200 sources, random inners |
| L10 | `switchMap(f)` = `map(f) ∘ switchAll()` on the trace | 200 sources, random inners |
| L12 | `first()` = `take(1) ∘ throwIfEmpty()`, `EmptyError` included | 200 sources |
| L13 | `startWith(a) ∘ startWith(b)` = `startWith(b, a)` | 200 sources |
| L14 | `mergeMap(f, 1)` = `concatMap(f)` | 200 sources, random inners |
| L15 | `tap(noop)` is the identity on the trace | 200 sources |
| L16 | `materialize ∘ dematerialize` is the identity, errors included | 200 sources |

L5 is the one most worth a second look. `delay` forwards errors at once and holds completion behind pending values, and both halves of the law do so twice. The traces still agree, including the frame of the error and the frame of completion, because the maximum of two delayed completions is the delayed maximum.

### Laws with a side condition

**L8.** `scan(f, s) ∘ last()` = `reduce(f, s)` on every source with at least one value. On a source that completes empty, `scan ∘ last` raises `EmptyError` and `reduce` emits the seed. The generator finds this on its own when restricted to empty completing sources. The side condition is the empty-source matrix of [doc 15](15-terminal-deciders-step-tables.md), read across a composition: `last()` is an error decider and `reduce` with a seed is not.

### Laws that fail

**L4'.** Filter fusion fails for index-aware predicates. The second `filter` numbers the values it receives, so its index is not the source index.

```text
abc|   filter(x ≠ a), filter((_, i) => i === 0)   ->   -b-|
abc|   filter((x, i) => x ≠ a ∧ i === 0)          ->   ---|
```

This is the Count / Index pattern breaking a law that holds for Identity. The catalog row for `filter` says Count / Index for exactly this reason, and the law shows what the tag buys.

**L11.** `take` and `skip` do not commute. `take(3) ∘ skip(1)` yields two values; `skip(1) ∘ take(3)` yields three. The generator finds a counterexample within its first few sources.

### Equivalence has levels

Trace equivalence is equivalence of output. Two pipelines can agree on every trace and still differ in the actions a user can observe: when a callback is called, and in what order relative to a cancellation. Both laws below hold on the trace and fail on the actions.

**L9'.** `concatMap(f)` calls `f` for a queued value when it is dequeued. `map(f) ∘ concatAll()` calls `f` when the value arrives, because `map` is eager and the queue sits in `concatAll` behind it. On the outer `ab|` with the usual inners, the traces are identical, `a-ab-b|`, and the call frames are `[0, 3]` against `[0, 1]`. A side effect inside `f` runs two frames earlier under the composed form. This is row C3 of [doc 11](11-flattening-family-step-tables.md), seen from the other side.

**L10'.** `switchMap(f)` cancels the previous inner before calling `f` for the new value. `map(f) ∘ switchAll()` calls `f` first, since `map` runs before `switchAll` sees the value, and cancels afterwards. On the outer `ab|` the traces are identical, `ab-b|`. When `f` runs for `b`, the inner for `a` is already closed under `switchMap` and still open under the composition. Row SW2 of doc 11 verified the first order; the composition reverses it.

So there are at least three equivalences, and the catalog has been working at the middle one:

| Level | What must agree | How it is observed | Example that separates it from the next |
|---|---|---|---|
| Trace | Notifications and frames | Marbles | L9, L10 hold |
| Action | Also subscribe, unsubscribe, schedule, and user callbacks, in order | Subscription marbles and probes | L9', L10' fail |
| Implementation | The closure variables | Reading the source | `debounceTime`'s reschedule-on-wake, doc 10 |

A refactor that replaces one side of L9 with the other is safe for subscribers and unsafe for anyone who put a side effect in `f`.

## What this adds to the model

- A step table is not only a description. Two tables determine the table of their cascade, so the catalog's tables compose, and the rows that make composition work are the unsubscribe rows.
- A law is a claim about all sources, and a marble test is one source. The generator in the suite is the practical middle: many sources, reproducible, with counterexamples reported.
- The patterns predict which laws hold. Fusion of two `filter`s holds under Identity and fails under Count / Index. `scan ∘ last` equals `reduce` except where the terminal-decider matrix says `last` errors.
- Trace equivalence is not the only one. The flattening family's `project` findings from doc 11 reappear here as the difference between the trace level and the action level.

## Limits

- Two hundred random sources are many witnesses, not a proof. A law that holds here holds on short sources over a five-letter alphabet with at most one error. A proof would be a derivation over the component tables, as in the `scan ∘ take` example, carried out for every row.
- The generator produces no overlapping notifications on one frame and no hot sources, so same-frame ordering and multicasting are outside what it can refute.
- Action-level equivalence is checked by hand-built probes, not by the generator. A generator for action traces would need to record subscription logs and callback frames for both sides, which is a natural next step.

## Where this leaves the model

The formula on the first page of the README, step from state and event to new state and actions, has now been used in every way a Mealy machine can be used. Single operators were given their tables. Tables were composed. Pipelines were compared for equivalence, at two levels, with the differences explained by the patterns the catalog started from. What the catalog began as, a classification of operators by what they remember and what they do, has become something closer to an algebra: a vocabulary whose terms predict which laws hold and whose tables can be put together to compute the behavior of a pipeline nobody has run.
