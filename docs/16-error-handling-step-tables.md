# Step tables for the error-handling family

Created by Claude Code (Claude Fable 5.1), 2026-10-06. Derived from the RxJS 7.8.2 source of `retry`, `retryWhen`, and `catchError`. Every row is verified by [tests/error-handling-step-tables.test.ts](../tests/error-handling-step-tables.test.ts) unless marked "by inspection". Result on 2026-10-06: 16 tests, 16 passing.

## The seventh shape

Six families agreed on one thing: a source error is acted on the instant it arrives. This family keeps that rule and changes what "acted on" means. An error can be answered with a resubscription, a replacement observable, a different error, or a silent completion. The questions are how many times, how soon, and what happens when the retry policy itself ends or the handler itself fails. Two of the answers are footguns that the catalog rows cannot express.

## Conventions

**Events.** From the source: `next(x)`, `complete`, `error(e)`. From a delay or retry notifier: `nnext`, `ncomplete`, `nerror(e')`. From the handler's observable in `catchError`: `hnext(y)`, `hcomplete`, `herror(e'')`. From downstream: `unsubscribe`. `call` for the operator factory.

**Actions.** `Next`, `Complete`, `Error(which)`, `Subscribe(source)` for a fresh attempt, `Unsubscribe`, `Subscribe(notifier)`, `Schedule(d)`, `Call(selector)`. Reaching `Done` tears everything down; that is implied.

**States.** `Attempt(k)`: the source is subscribed, `k` retries used so far. `Waiting(k)`: the source has errored and a notifier decides when the next attempt starts; nothing is subscribed to the source. `Handler`: `catchError` is forwarding its replacement observable. `Done`.

**Witnesses.** The source is cold, so its subscription log is the attempt log. `#` carries the string `error` unless the witness names another value.

## retry(count | { count, delay, resetOnSuccess })

Defaults: `count = Infinity`, no delay, `resetOnSuccess = false`.

| Row | State | Event | Condition | State' | Actions | Witness |
|---|---|---|---|---|---|---|
| R0 | — | call | count ≤ 0 | | identity: errors pass, nothing resubscribes | `a#` → `a#`, one attempt |
| R1 | Attempt(k) | next(x) | | Attempt(k), or Attempt(0) if resetOnSuccess | Next(x) | see R1 witness |
| R2 | Attempt(k) | complete | | Done | Complete | `a\|` → `a\|` |
| R3 | Attempt(k) | error(e) | k < count, no delay | Attempt(k+1) | Subscribe(source) at once | `a-#` with retry(1) → `a-a-#`; attempts `^-!`, `--^-!` |
| R3' | Attempt(k) | error(e) | k < count, delay = d | Waiting(k+1) | Schedule(d) | `a#` with delay 3 → `a---a#`; attempts `^!`, `----^!` |
| R3'' | Attempt(k) | error(e) | k < count, delay = factory | Waiting(k+1) | Subscribe(factory(e, k+1)) | factory sees 1, then 2 |
| R4 | Attempt(count) | error(e) | | Done | Error(e), the source's error | `a-a-#` ends with the original error |
| R5 | Waiting(k) | nnext | | Attempt(k) | Unsubscribe(notifier), Subscribe(source) | `a-a--a#` with delay n → timer(n) |
| R6 | Waiting(k) | ncomplete, without nnext | | Done | Complete | `a#` with delay () → EMPTY → `a\|`, one attempt |
| R7 | Waiting(k) | nerror(e') | | Done | Error(e'), the notifier's error | delay () → throwError → `a#` carrying `give up` |
| R8 | any | unsubscribe | | Done | — | |

R1 is what `resetOnSuccess` means in practice. `count` is the number of consecutive failures tolerated, not the number of failures. A source that always emits one value before failing is retried forever under `{ count: 1, resetOnSuccess: true }`, and gives up after its second attempt without the flag. R3'' and R5 show the retry number handed to a delay factory is 1-based: the first wait is for retry 1.

R6 is the first footgun. A delay factory whose notifier completes without emitting does not retry and does not forward the error. It completes the result. The failure vanishes. R7 is its sibling: a notifier that errors replaces the source's error with its own.

Two details by inspection. `retry(1)` means two attempts, since the count is retries, not tries. And a source that errors synchronously inside `subscribe` is resubscribed after `subscribe` returns, so a synchronously failing source does not recurse.

## retryWhen(notifier)

The notifier function is called once, on the first error, with a `Subject` of errors that lives for the whole subscription.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| RW1 | Attempt, no error yet | error(e) | Waiting | NewSubject(errors$), Subscribe(notifier(errors$)), errors$.next(e) | `a#` → first `a` |
| RW1' | Attempt, errors$ exists | error(e) | Waiting | errors$.next(e) | every later error |
| RW2 | Waiting | nnext | Attempt | Subscribe(source) | identity notifier → `aaaa`, attempts every frame |
| RW2' | Waiting | nnext, delayed | Attempt | Subscribe(source) | `delay(2)` notifier → `a--a--a`; attempts `^!`, `---^!`, `------^!` |
| RW3 | Waiting, Attempt | ncomplete | Done | Complete | `take(2)` notifier → `aa\|`; attempts `^!`, `-^!`, `--(^!)` |
| RW4 | Waiting, Attempt | nerror(e') | Done | Error(e') | mergeMap to throwError → `aa#` carrying `give up` |
| RW5 | Attempt | next(x) | Attempt | Next(x) | |
| RW6 | Attempt | complete | Done | Complete | |
| RW7 | any | unsubscribe | Done | — | |

There is no count. The number of retries is the number of times the notifier emits, and the only way to give up with the original error is to make the notifier error with it, as RW4 does with a different value for clarity.

RW3 is the second footgun, and the sharper one. The common idiom `retryWhen(errors => errors.pipe(take(n)))` does not error after `n` failures. On the `n`th error `take` emits, which starts a new attempt, and then completes, which completes the result at the same frame and cancels the attempt it just started. The attempt log shows the third subscription opened and closed on frame 2, `--(^!)`. The result completes successfully having never succeeded.

By inspection: if the notifier emits while the source is live rather than waiting for an error, `retryWhen` subscribes the source again without unsubscribing the live attempt. Two attempts then run at once.

## catchError(selector)

The selector receives the error and `caught`, which is the source with this same `catchError` applied again.

| Row | State | Event | State' | Actions | Witness |
|---|---|---|---|---|---|
| CE1 | Source | next(x) | Source | Next(x) | `a-#` → the `a` is forwarded and stays |
| CE2 | Source | complete | Done | Complete; selector never called | `a-\|` → `a-\|`, selector count 0 |
| CE3 | Source | error(e) | Handler | Call(selector(e, caught)), Unsubscribe(source), Subscribe(result) | fallback `x-y\|` → `a-x-y\|`; source `^-!`, fallback `--^--!` |
| CE3' | Source | error(e), selector throws e' | Done | Error(e') | throwing selector → `a-#` carrying `boom` |
| CE4 | Handler | hnext(y) | Handler | Next(y) | `x`, `y` at 2 and 4 |
| CE5 | Handler | hcomplete | Done | Complete | frame 5 |
| CE6 | Handler | herror(e'') | Done | Error(e''); not caught again | fallback `x#` → `a-x#` |
| CE7 | Source | error(e), selector returns caught | Source, fresh attempt | Subscribe(source), with catchError reapplied | `a#` → `aaaa`, an attempt every frame |
| CE8 | any | unsubscribe | Done | — | |

`catchError` catches one level. A handler that fails, by throwing (CE3') or by returning an observable that errors (CE6), errors the result. CE7 is the only way this operator retries: `caught` is the source wrapped in the same `catchError`, so returning it restarts the source and re-arms the catch, without end.

## The cross-cutting cases

| Case | retry | retryWhen | catchError |
|---|---|---|---|
| How many times | `count`, default unbounded; `resetOnSuccess` makes it consecutive failures (R1, R4) | As many times as the notifier emits (RW2) | Once per error, one level deep; `caught` makes it unbounded (CE7) |
| How soon | At once, after a fixed delay, or when a per-error notifier emits (R3, R3', R5) | When the notifier emits (RW2, RW2') | At once (CE3) |
| The policy ends by completing | Delay notifier completes: result completes, error swallowed (R6) | Notifier completes: result completes, in-flight retry cancelled (RW3) | Handler completes: result completes (CE5) |
| The policy ends by erroring | Delay notifier errors: that error (R7) | Notifier errors: that error (RW4) | Handler errors or selector throws: that error, not caught (CE3', CE6) |
| Giving up with the original error | Count exhausted (R4) | Only by making the notifier rethrow it | Only by rethrowing it from the selector |
| Source completes | Complete (R2) | Complete (RW6) | Complete, selector never called (CE2) |
| Values before the error | Forwarded and kept | Forwarded and kept | Forwarded and kept (CE1) |
| What is resubscribed | The source, from the start | The source, from the start | The handler's observable, or the source via `caught` |

Partial progress is never undone. Every value the source emitted before failing has already reached the subscriber, and a retry appends a fresh run after it. A subscriber therefore sees `a a a` from a source that only ever produces one `a` before failing.

## The two footguns, stated once

A notifier that completes without emitting turns an error into a success. This is R6 and RW3. In `retry` it needs a delay factory whose observable completes empty. In `retryWhen` it is the standard `take(n)` idiom, which also cancels the attempt it has just started. In neither case does the subscriber learn that anything failed. The repair is the same in both: a policy that wants to give up must error, not complete.

## Contrast with the first six families

| | Timing | Flattening | Join | Multicasting | Buffering | Deciders | Error handling |
|---|---|---|---|---|---|---|---|
| Question shape | Source completes while a timer is armed | Outer completes while inners run | A source completes before, after, or with a backlog | A subscriber joins or leaves | A region is open when something else ends it | The source completes empty | An error that is not the end |
| Completion | Three policies | One | Four conditions | Configurable | One | Three kinds | Can be produced from an error |
| Error | Uniform | Uniform | Uniform | Configurable | Uniform | Uniform | Retried, replaced, swallowed, or exchanged |
| Where members differ | Completion | Admission | Readiness | Reset switches | Reading a notifier's terminal | Decision time, empty answer | Who decides the retry: a count, a notifier, a handler |
| New state | Draining, Stranded | Draining, queue | Dead source | Lingering, Grace, Ended | Stranded region | Count, slot, flag | Waiting, Handler |

The rule from six families survives in refined form. A source error is still acted on the instant it arrives; this is the family where the action is something other than forwarding it. And the family contributes a converse that no other has: here an error can become a completion, which is the one transition the terminal deciders, for all their variety, never make.

## What the tables show that the rows could not

- `retry`'s count is consecutive failures only under `resetOnSuccess`; without the flag it is total failures. `retry(1)` is two attempts.
- A delay factory sees a 1-based retry number.
- A retry notifier that completes without emitting swallows the error and completes the result, in both `retry` and `retryWhen`. In `retryWhen` it also cancels the attempt it just triggered.
- `retryWhen` has no count and no way to give up with the original error except by rethrowing it from the notifier.
- `catchError` catches one level; a failing handler fails the result.
- `caught` is the source with `catchError` reapplied, so returning it is unbounded retry.
- Values emitted before the error are kept, and each retry appends a fresh run after them.

## Relation to the catalog

Tags that would let the rows carry this: `count is total failures, consecutive with resetOnSuccess` and `delay notifier completing swallows the error` on `retry`; `retries per notifier emission, notifier completing swallows the error and cancels the attempt` on `retryWhen`; `catches one level, caught resubscribes` on `catchError`; and for the family, `values before the error are kept`.

## Where this leaves the catalog

Seven families now have full step tables: timing, flattening, join, multicasting, buffering and windowing, terminal deciders, and error handling. Between them they cover every operator in [05](05-operator-catalog.md) whose state is a timer, a registry, a queue, a slot, a connection, a region, a decision, or a retry policy. What remains in the catalog is the stateless and near-stateless core: `map`, `filter`, `tap`, `scan`, `reduce`, the `distinct` family, `skip` and its variants, `delay` and the scheduler operators, and the aggregates. Their rows in 05 and their experiments in [08](08-state-transition-pattern-tests.md) and [09](09-action-policy-pattern-tests.md) already say what a step table would, because their state is one component and their action list is one line.
