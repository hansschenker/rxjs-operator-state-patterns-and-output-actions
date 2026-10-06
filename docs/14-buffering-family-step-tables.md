# Step tables for the buffering and windowing family

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Derived from the RxJS 7.8.2 source of `buffer`, `bufferCount`, `bufferTime`, `bufferToggle`, `bufferWhen` and their window twins. Every row is verified by [tests/buffering-family-step-tables.test.ts](../tests/buffering-family-step-tables.test.ts). Result on 2026-10-06: 20 tests, 20 passing.

## The fifth shape, and the twins

Ten operators in two rows of five. Each buffer operator has a window twin that opens and closes regions by the same rule. The buffer twin emits a region's contents as one array when the region closes. The window twin emits a `Subject` when the region opens and routes values into it. The catalog already says this much: Batch / Aggregate against Route / Group.

The question shape is what happens to an open region when something other than its own boundary ends it: the source completing, the source erroring, the boundary notifier completing, the boundary notifier erroring. The twins answer the source's events identically up to the difference between arrays and subjects, with one exception. They answer the boundary's events differently in two places, and those two are the findings of this document.

## Conventions

**Events.** From the source: `next(x)`, `complete`, `error(e)`. From a boundary notifier: `bnext`, `bcomplete`, `berror(e)`. From an openings notifier: `open(o)`. From the closing notifier of region `i`: `close_i`, `closeComplete_i`, `closeError_i(e)`. From a timer: `expire_i` for a region's span, `tick` for a creation interval. From downstream: `unsubscribe`.

**Actions, buffer side.** `Emit(i)` is `Next(array)` of region `i`'s contents, empty arrays included unless a table says otherwise. **Window side.** `Open` is `NewSubject` plus `Next(window)` downstream. `WNext(x)` delivers `x` to each open window. `WComplete(i)` and `WError(i)` terminate window `i`. Both sides share `Complete`, `Error(e)`, `Subscribe` and `Unsubscribe` of a notifier, `Schedule` and `Cancel` of a timer. Reaching `Done` tears everything down.

**States.** A set of open regions. Each region has contents on the buffer side and a `Subject` on the window side. Where the set can be empty, the table says so, because that is where values are dropped.

**Witnesses.** Buffers are observed as marbles. Windows are observed with a recorder that subscribes to each window as it is emitted and writes `0:a@1` for value `a` in window 0 at frame 1, `0:C@2` for window 0 completing at frame 2, `1:E@3` for window 1 erroring at frame 3. The outer stream is written `W@f` for a window emitted at frame `f`, `C@f` and `E@f` for its own terminal.

## buffer(notifier) / window(notifier)

One region, always open. State: `Open(contents)`.

| Row | State | Event | State' | Buffer actions | Window actions | Witness |
|---|---|---|---|---|---|---|
| B0 | — | subscribe | Open(empty) | — | Open | window `W@0` |
| B1 | Open | next(x) | Open, x appended | — | WNext(x) | `ab--c--\|` with `---n--\|` |
| B2 | Open | bnext | Open(empty) | Emit, empty included | WComplete, Open | → `---x---(y\|)`, x = [a, b]; `W@3`, `0:C@3` |
| B2' | Open(empty) | bnext | Open(empty) | Emit([]) | WComplete, Open | `----a\|` with `-n\|` → `-x---(y\|)`, x = [] |
| B3 | Open | bcomplete | Open | — | — | boundary completes at 6, `c` stays |
| B4 | Open | berror(e) | Done | Error(e), contents dropped | WError, Error(e) | `--#` → `--#`; window `0:a@0 0:E@2` |
| B5 | Open | complete | Done | Emit, Complete | WComplete, Complete | y = [c] at 7; `1:c@4 1:C@7` |
| B6 | Open | error(e) | Done | Error(e), contents dropped | WError, Error(e) | `a-#` → `--#`; window `0:a@0 0:E@2` |

The twins agree everywhere. B3 is the first "ignored terminal": the boundary completing leaves the region open until the source itself completes.

## bufferCount(size, every) / windowCount(size, every)

Regions by count. A region opens at every value whose index is a multiple of `every` and closes after `size` values. `every` defaults to `size`.

| Row | State | Event | State' | Buffer actions | Window actions | Witness |
|---|---|---|---|---|---|---|
| C0 | — | subscribe | no region / one region | — | Open | `bufferCount` has no buffer until a value arrives; `windowCount` emits `W@0` |
| C1 | regions | next(x), index a multiple of every | +1 region, x in every open region | — , Emit(i) for any region reaching size | WNext(x), WComplete(i) for any region reaching size, Open for the next | `abcd\|` → `-x-y\|`; windows `W@0 W@1 W@3` |
| C1' | regions | next(x), otherwise | x in every open region | Emit(i) if full | WNext(x), WComplete(i) if full | |
| C2 | regions | complete | Done | Emit each open region, Complete | WComplete each, Complete | `abc\|` → `-x-(y\|)`, y = [c] |
| C3 | regions | error(e) | Done | Error(e), dropped | WError each, Error(e) | `ab#` → `--#`; `0:a@0 0:b@1 0:E@2` |
| C4 | every > size, no region open | next(x) | no region | — , x dropped | — , x dropped | `abcde\|` with (2, 3) → `-x--y\|`, `c` missing |

Here the twins differ in a way the shared rule hides. `bufferCount` creates a buffer when the first value for it arrives, so a buffer is never empty and C2 never emits `[]`. `windowCount` opens the next window the moment the previous one fills, before any value for it exists. On `abcd|` with size 2 it emits three windows, and the third completes empty at frame 4. On `abcde|` with (2, 3) it opens window 1 at frame 2, the frame of the dropped `c`, and that window then collects `d` and `e`. The buffer twin emits two arrays; the window twin's count of regions is one higher whenever the source ends on a boundary.

## bufferTime(span, interval, max) / windowTime(span, interval, max)

Regions by timer. Without a creation interval, one region at a time, restarted on each close. With one, a new region every `interval` frames, overlapping. `max` closes a region early.

| Row | State | Event | State' | Buffer actions | Window actions | Witness |
|---|---|---|---|---|---|---|
| T0 | — | subscribe | one region, timer armed | Schedule(expire) | Open, Schedule(expire) | windows `W@0` |
| T1 | regions | next(x) | x in every open region | Emit(i) and, without interval, restart if max reached | WNext(x), WComplete(i) and restart if max reached | `ab---\|` with (5, null, 2) → `-x---(y\|)` |
| T2 | regions | expire_i | −1 region; +1 if no interval | Emit(i), empty included | WComplete(i), Open if no interval | `a----\|` with 2 → `--x-y(y\|)`, y = [] |
| T3 | with interval | tick | +1 region | Schedule(expire) | Open, Schedule(expire) | (4, 2): `W@0 W@2 W@4 W@6` |
| T4 | regions | complete | Done | Emit each in order, empty included, Complete, Cancel timers | WComplete each, Complete, Cancel timers | `-a-b-c-\|` with (4, 2) → `----x-y(zw\|)`, w = [] |
| T5 | regions | error(e) | Done | Error(e), dropped | WError each, Error(e) | `a#` → `-#`; `0:a@0 0:E@1` |

T3 and T4 together are the overlap witness: regions `[0,4)`, `[2,6)`, `[4,8)`, `[6,10)`; `b` at 3 lands in two of them and `c` at 5 in two; completion at 7 flushes `[c]` and then `[]` in creation order. `bufferTime` is the only buffer operator besides `buffer` that routinely emits empty arrays, and it does so on every idle expiry.

## bufferToggle(openings, closing) / windowToggle(openings, closing)

Regions by two notifiers. Each `open(o)` starts a region and subscribes `closing(o)` for it. The set of open regions can be empty.

| Row | State | Event | State' | Buffer actions | Window actions | Witness |
|---|---|---|---|---|---|---|
| G1 | regions | open(o) | +1 region | Subscribe(closing(o)) | Open, Subscribe(closing(o)) | `--o---o\|` → `W@2 W@6` |
| G2 | region i open | close_i | −1 region | Emit(i), Unsubscribe(closing_i) | WComplete(i), Unsubscribe(closing_i) | `[b]` at 4, `[d]` at 8 |
| G3 | region i open | closeComplete_i | unchanged | — | — | `-\|` closing: `[b, c]` flushed only at source complete |
| G4 | regions | closeError_i(e) | Done | Error(e) | WError each, Error(e) | `-#` closing → `--#`; `0:E@2` |
| G5 | regions | openings complete | unchanged | — | — | openings end at 7, region 2 closes at 8 |
| G6 | regions | openings error(e) | Done | Error(e) | Error(e); open windows torn down silently | `-o-#` → `---#`; window `0:b@2` with no `E` |
| G7 | no region open | next(x) | no region | — , x dropped | — , x dropped | `a` at 1 and `c` at 5 are missing |
| G8 | regions | complete | Done | Emit each, Complete | WComplete each, Complete | `----x---y-\|`; `C@10` |
| G9 | regions | error(e) | Done | Error(e), dropped | WError each, Error(e) | `a-b#` → `---#`; `0:b@2 0:E@3` |

G6 is the first finding. `windowToggle` errors its open windows when the source errors (G9) and when a closing notifier errors (G4). When the openings notifier errors, the result errors but the open windows are only unsubscribed. A subscriber to such a window sees it stop and never learns why. The buffer twin has no equivalent, because a dropped buffer has no subscribers to inform. G7 is the row to remember for both twins: outside every region, values do not exist.

## bufferWhen(closing) / windowWhen(closing)

One region at a time, open from subscribe. The closing selector is called afresh for each region.

| Row | State | Event | State' | Buffer actions | Window actions | Witness |
|---|---|---|---|---|---|---|
| W0 | — | subscribe | Open(empty) | Subscribe(closing()) | Open, Subscribe(closing()) | `W@0` |
| W1 | Open | next(x) | x appended | — | WNext(x) | |
| W2 | Open | close | Open(empty) | Emit, Unsubscribe(old), Subscribe(closing()) | WComplete, Open, Unsubscribe(old), Subscribe(closing()) | `--c\|` closing: `--x-y-(z\|)`; `W@0 W@2 W@4` |
| W3 | Open | closeComplete | buffer: Open, no notifier | — ; region stranded until complete | **WComplete, Open, Subscribe(closing())**, as W2 | `--\|` closing: buffer `------(x\|)`, x = [a, b, c]; windows `W@0 W@2 W@4` |
| W4 | Open | closeError(e) | Done | Error(e) | WError, Error(e) | `-#` closing → `-#`; `0:a@0 0:E@1` |
| W5 | Open | complete | Done | Emit, Complete | WComplete, Complete | z = [c] at 6; `2:C@6` |
| W6 | Open | error(e) | Done | Error(e), dropped | WError, Error(e) | `a#` → `-#`; `0:a@0 0:E@1` |

W3 is the second finding, and the larger one. The two twins are given the same closing selector, one that completes without emitting. `bufferWhen` treats that completion as nothing: the buffer stays open with no notifier left to close it, and everything is flushed as one array when the source completes. `windowWhen` treats it as a boundary: it completes the window, opens the next, and calls the selector again. The window witness for a silently completing selector is identical to the witness for a firing one. The buffer witness is a single array at the end.

A consequence by inspection: `windowWhen(() => EMPTY)` recurses without bound, because each new window's selector completes synchronously and opens another. `bufferWhen(() => EMPTY)` is a single buffer flushed on complete.

## Where the twins differ

| Event | Buffer twin | Window twin |
|---|---|---|
| Region opens | Nothing visible downstream | `Next(window)` |
| Region closes | `Next(array)`, empty included except `bufferCount` | `WComplete` |
| Next region after a count boundary | Created with its first value; never empty | Opened eagerly when the previous fills; may complete empty |
| Source error | Open regions dropped | Open windows receive the error |
| Boundary or closing notifier error | Open regions dropped | Open windows receive the error |
| Openings notifier error (`Toggle`) | Open regions dropped | Open windows torn down silently, no error |
| Closing notifier completes (`When`) | Region stranded until source complete | Treated as a boundary; roll over |
| Boundary completes (`buffer`, `Toggle`) | Ignored | Ignored |

## The cross-cutting cases

| Case | buffer / window | Count | Time | Toggle | When |
|---|---|---|---|---|---|
| Source completes with open regions | Flush one, or complete one (B5) | Flush partial buffers; complete windows, possibly an empty one (C2) | Flush all in order, empty included (T4) | Flush all, empty included (G8) | Flush one (W5) |
| Source errors | Drop; windows errored (B6) | same (C3) | same (T5) | same (G9) | same (W6) |
| Boundary or closing completes | Ignored (B3) | n/a | n/a | Ignored (G3, G5) | Buffer: stranded. Window: roll over (W3) |
| Boundary or closing errors | Error; windows errored (B4) | n/a | n/a | Closing: error, windows errored (G4). Openings: error, windows silent (G6) | Error; window errored (W4) |
| Value with no open region | Impossible | Possible when every > size; dropped (C4) | Impossible | Possible; dropped (G7) | Impossible |
| Empty region closes | `[]` emitted (B2') | Never happens for buffers | `[]` emitted (T2) | `[]` emitted | `[]` emitted |

Completion is uniform across the family: every open region is flushed or completed, and the result completes. Error is uniform in its half: every open buffer is dropped, every open window is errored, with the one `windowToggle` exception. The notifiers' own terminals are where the family splits, and they split between twins, not between operators.

## Contrast with the first four families

| | Timing | Flattening | Join | Multicasting | Buffering |
|---|---|---|---|---|---|
| Question shape | Source completes while a timer is armed | Outer completes while inners run | A source completes before, after, or with a backlog | A subscriber joins or leaves; a terminal arrives | A region is open when something other than its boundary ends it |
| Completion | Three policies | One policy | Four conditions | Configurable | One policy: flush or complete every region |
| Error | Uniform | Uniform | Uniform | Configurable | Uniform, with one silent teardown |
| Where members differ | Completion | Admission | Readiness | Reset switches | How a notifier's own terminal is read |
| New state the rows do not name | Draining, Stranded | Draining, queue | Dead source in Filling | Lingering, Grace, Ended | Stranded region; empty eager window |

Error still never waits, in a fifth family. And the family brings back a state from the first: `Stranded`, a region whose only closer has gone silent, which doc 10 found in `audit` and `throttle` and which `bufferWhen` has while `windowWhen` does not.

## What the tables show that the rows could not

- The two twins of `When` read the same notifier's completion in opposite ways. One strands, one rolls over.
- `windowToggle` tells its windows about a source error and a closing error, but not about an openings error.
- `windowCount` can complete an empty window that `bufferCount` would never have created, whenever the source ends exactly on a boundary.
- `bufferCount` and `bufferToggle` can drop values, by a gap in the count or by a gap between regions. The others cannot.
- `buffer` and `bufferTime` emit empty arrays as a matter of course. `bufferCount` cannot. `bufferToggle` and `bufferWhen` do when a region closes with nothing collected.
- A boundary notifier completing is ignored by `buffer`, `window`, and both `Toggle` twins. Only `windowWhen` acts on it.

## Relation to the catalog

Tags that would let the rows carry this: `empty regions emitted` on `buffer`, `bufferTime`, `bufferToggle`, `bufferWhen`; `may drop values between regions` on `bufferCount`, `windowCount`, `bufferToggle`, `windowToggle`; `closing completion strands` on `bufferWhen` and `closing completion rolls over` on `windowWhen`; `openings error tears windows down silently` on `windowToggle`; `eager next window` on `windowCount`.

## Next family

The terminal deciders are the natural sixth: `take`, `takeLast`, `first`, `last`, `single`, `elementAt`, `find`, `defaultIfEmpty`, `throwIfEmpty`, `every`, `isEmpty`. Their question is the simplest shape of all and the one most often got wrong: what each does when the source completes empty, and at which event the decision is made.
