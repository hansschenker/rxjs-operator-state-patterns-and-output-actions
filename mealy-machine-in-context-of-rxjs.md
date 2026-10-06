# The Mealy machine in the context of RxJS: a reading guide

Written by Claude Code (Claude Fable 5.1), 2026-10-06. This is the chain of reasoning behind docs 07 to 16 and the nine test suites, laid out as the steps a reader needs in order, each with the place in the repository where it is applied. Step 14 summarises the seven operator families that the method was applied to; step 15 says what is left.

## 1. An operator is a machine, not a function

A Mealy machine is \(M = (S, s_0, E, A, T, G)\): states, an initial state, input events, actions, a transition function, and an action function. One step is

\[
\operatorname{step}(s, e) = (s', a^*)
\]

Read an event, look at the current state, move to a new state, perform zero or more ordered actions. An RxJS operator is exactly this. Every operator is causal, meaning its behavior at any moment depends only on what has happened so far, and causal stream functions are Mealy machines. This is a principled fit, not a metaphor.

It is Mealy and not Moore because actions depend on the event, not on the state alone. `debounceTime` flushes on complete, not because its state changed.

Where: [docs/01-model.md](docs/01-model.md), [README](README.md).

## 2. The event alphabet is wider than the source

A textbook machine has one input. An operator hears several channels: the source's next, complete, and error; each inner observable's next, complete, and error; timer expiry; notifier and duration events; and downstream unsubscribe. All of these are events \(e \in E\). The interesting behavior lives where channels cross, such as the source completing while a timer is armed.

Where: the "Conventions" section of every step-table document, docs 10 to 16. Each family widened the alphabet again: inner notifications in [docs/11](docs/11-flattening-family-step-tables.md), per-source events in [docs/12](docs/12-join-family-step-tables.md), subscriber join and leave in [docs/13](docs/13-multicasting-family-step-tables.md), a notifier's own terminal in [docs/14](docs/14-buffering-family-step-tables.md), and a retry notifier's terminal in [docs/16](docs/16-error-handling-step-tables.md).

## 3. The action alphabet is wider than output

Downstream `next`, `error`, and `complete` are actions. So are subscribing an inner, unsubscribing it, scheduling a timer, and cancelling one. Output is the projection of the action list onto what a subscriber sees. Storing a value is not an action at all; it is a transition.

This is what lets `switchMap` fit: cancelling the old inner is an action, not an output decision.

Where: [docs/03-action-policies.md](docs/03-action-policies.md), AP-14; the `Schedule` and `Cancel` columns in [docs/10](docs/10-timing-family-step-tables.md).

## 4. Split the machine into two halves

Two questions, asked separately. What does the operator remember, and how does that memory change? That is \(T\), the state transition pattern. Given the event and the state, what ordered actions occur? That is \(G\), the action policy. The repository names 15 of each, ST-01 to ST-15 and AP-01 to AP-15.

State is a product. A real operator carries several ST patterns at once and several AP patterns at once. `scan` and `reduce` are the cleanest check: same fold, different policy.

Where: [docs/02](docs/02-state-transition-patterns.md), [docs/03](docs/03-action-policies.md), the row format of [docs/05](docs/05-operator-catalog.md).

## 5. Three levels of state, and the rule for choosing one

Teaching state is what a learner needs. Semantic state is what user code can observe, including the index passed to a callback. Implementation state is timer handles and queues inside the runtime. The catalog classifies at the semantic level: by what a subscriber or a callback can observe, never by what the closure happens to store.

The index is the test case. `map` and `filter` are Count / Index because their callbacks receive an index. `tap` is Identity because its callback does not.

Where: [docs/01](docs/01-model.md), "Stateless operators still have a transition"; the `tap` correction in [docs/07](docs/07-catalog-evaluation.md).

## 6. The implementation already is the machine

Every RxJS 7 operator is a closure with a few variables and three handlers. The variables are \(S\). The handlers for next, complete, and error are \(T\) and \(G\) fused together, one per event. This means a claim about an operator can be checked against source, line by line, and that the model describes what the code does rather than an idealisation of it.

Where: the source excerpts quoted in [docs/07](docs/07-catalog-evaluation.md); the derivation at the start of each table in docs 10 to 16. The clearest example is [docs/11](docs/11-flattening-family-step-tables.md): `mergeMap`, `concatMap`, `expand`, and `mergeScan` are one closure, `mergeInternals`, with different arguments, so they share one table.

## 7. A catalog row is a falsifiable claim

Once an operator is a machine, a row like "Replace Latest + Timer / Deadline, Trigger / Sample" is a conjunction of claims that can each be wrong. The evaluation asked three questions of every row. Does RxJS 7.8 behave this way? Does the row use the pattern terms as 02 and 03 define them? Is the same behavior tagged the same way in sibling rows? The answers sort into Wrong, Misapplied, Incomplete, and Inconsistent.

The decisive cases: `delayWhen` is built on `mergeMap`, so there is no queue. `partition` is two `filter` subscriptions, so nothing is routed. `timestamp` reads a clock, so no timer is armed. `catchError` suppresses a terminate rather than issuing one. `zip` subscribes every source at once, so it is not Serialize. `race` selects once and never replaces, so Current-Wins needed a First-Wins variant.

Where: [docs/07](docs/07-catalog-evaluation.md) and the corrected rows now in [docs/05](docs/05-operator-catalog.md).

## 8. A pattern definition becomes an operational test

A pattern is a property that holds for all inputs. A test cannot check all inputs, but it can run a discriminating experiment: a property observable from outside, one operator that has the pattern and satisfies it, and one operator from the neighbouring pattern that fails the same property. The neighbour is the half that makes the test decide rather than confirm.

Four observation channels: the values emitted, the frame each left on, the subscription marbles showing which producers were opened and cancelled when, and a probe callback for state that leaves no trace downstream.

Time is virtual. `TestScheduler.run` makes one marble character one frame, so timing claims become exact and repeatable.

Where: [docs/08](docs/08-state-transition-pattern-tests.md) and `tests/state-transition-patterns.test.ts`; [docs/09](docs/09-action-policy-pattern-tests.md) and `tests/action-policy-patterns.test.ts`.

## 9. Neighbours are where the errors were

Every misapplied row in the catalog sat on a boundary between adjacent patterns: timer versus clock read, queue versus registry, serialize versus join, gate versus terminate. The pattern tests are built around exactly those boundaries, so each test is also the check that would have caught the corresponding catalog error. `delayWhen` overtakes under the queue test. `timestamp` emits nothing when only the clock advances. `tap` receives one argument.

Where: the "Fails" columns of the summary tables in [docs/08](docs/08-state-transition-pattern-tests.md) and [docs/09](docs/09-action-policy-pattern-tests.md).

## 10. Know what a pattern test cannot see

One scenario is a witness, not a proof. A counter nobody reads is invisible, so Count / Index needs a probe. Every operator has Lifecycle / Terminal, so it has no negative case. A Schedule followed by a Cancel leaves no trace at all. And several patterns share a positive operator across the two suites, which is expected: one operator, two halves.

Where: the "Limits" sections of [docs/08](docs/08-state-transition-pattern-tests.md) and [docs/09](docs/09-action-policy-pattern-tests.md).

## 11. From components to the full machine

A row lists the components of the state. It does not give the transition function on their product. The step table does: one row per reachable pair of state and event, the new state, the ordered actions, and a marble witness. Pairs the invariants rule out are listed and marked impossible, so the table is complete rather than illustrative.

Writing the table forces states into the open that no pattern names. `Draining` is a source that has completed while a window is still armed, or an outer that has completed while inners still run. `Stranded` is a stored value, or an open region, whose only closer has gone silent. `Lingering`, `Grace`, and `Ended` are a shared connection with no subscribers, with a reset pending, or with a kept terminal. `Waiting` is a failed source between an error and its retry. These are the states where family members differ, and the cross-channel cases from step 2 are the events that reach them.

Where: the tables and the cross-cutting matrix of each document from [docs/10](docs/10-timing-family-step-tables.md) to [docs/16](docs/16-error-handling-step-tables.md). Every one of them ends with a contrast table against the families before it.

## 12. The method for one family

1. Read the operator's source and list its closure variables.
2. Name the states as combinations of those variables, and write the invariants that make some combinations unreachable.
3. List the events from every channel the operator listens to.
4. Fill the table: every reachable state against every event, with the new state and the ordered actions.
5. Mark the unreachable pairs as impossible, with the invariant that excludes them.
6. Write one marble witness per row, avoiding a source event and a timer on the same frame.
7. Run the witnesses. A failing witness means the table is wrong or the expectation is; find out which from the source.
8. Extract the cross-cutting matrix: the cells where the family members answer the same event differently.
9. Carry the result back to the catalog as a tag the rows can hold, such as `flush on complete`, `wait on complete`, `drop on complete`.

Where: docs 10 to 16 are this method applied seven times, and the seven files `tests/*-step-tables.test.ts` are step 7 each time. Step 7 earned its place: in four of the seven families one witness failed on the first run, and every time the table was right and the expectation was wrong. The failures are recorded in the documents because they were informative, such as the grace timer that is re-armed at every zero crossing in [docs/13](docs/13-multicasting-family-step-tables.md).

## 13. State the semantic action, note the implementation

`debounceTime` does not cancel and reschedule its timer on each value; it records the arrival time and, when the task wakes, reschedules itself for the remainder. The observable behavior is a restart, so the table says Cancel, Schedule and the prose notes the implementation. `throttleTime` in its default configuration keeps a stale value in its slot that no row ever emits; the table treats the slot as empty. `retry` and `retryWhen` defer a resubscription that would happen inside `subscribe` until `subscribe` has returned, which keeps a synchronously failing source from recursing; the tables say "resubscribe at once" because that is what a subscriber can observe. The rule from step 5 applies at every level.

Where: the notes under the `debounceTime` and leading `throttleTime` tables in [docs/10](docs/10-timing-family-step-tables.md); the "by inspection" notes in [docs/16](docs/16-error-handling-step-tables.md).

## 14. The seven families

Each family was chosen for one question that its catalog rows could not answer, and each document ends by contrasting the family with the ones before it.

| | Family | Operators | The question | What the tables found | Where |
|---|---|---|---|---|---|
| 1 | Timing | `debounceTime`, `auditTime`, `sampleTime`, `throttleTime`, and the selector variants | The source completes while a timer is armed | Three completion policies in one family: flush, wait, drop. A silent duration strands or drops in `audit` and `throttle`, is flushed by `debounce` | [docs/10](docs/10-timing-family-step-tables.md), 38 tests |
| 2 | Flattening | `mergeMap`, `concatMap`, `switchMap`, `exhaustMap`, the `*All` variants | The outer completes while inners are active | One policy: wait and drain. The differences are when `project` runs and what index it gets; `exhaustMap` counts admitted values only | [docs/11](docs/11-flattening-family-step-tables.md), 21 tests |
| 3 | Join | `combineLatest`, `zip`, `forkJoin`, `withLatestFrom` | A source completes before emitting, after emitting, or with a backlog | Four completion conditions. `combineLatest` has no empty-source short-circuit; `forkJoin` does. This corrected a sentence in doc 07 | [docs/12](docs/12-join-family-step-tables.md), 16 tests |
| 4 | Multicasting | `share` under each reset switch, `shareReplay`, `connectable` | A subscriber joins or leaves; a terminal arrives | No absorbing state by default. A value with no subscribers is lost, buffered, or never produced, by switch. `connectable` strands subscribers on disconnect | [docs/13](docs/13-multicasting-family-step-tables.md), 16 tests |
| 5 | Buffering and windowing | the five `buffer` operators and their `window` twins | A region is open when something other than its boundary ends it | Twins read a notifier's completion differently: `bufferWhen` strands, `windowWhen` rolls over. `windowToggle` tears windows down silently on an openings error | [docs/14](docs/14-buffering-family-step-tables.md), 20 tests |
| 6 | Terminal deciders | `take`, `takeLast`, `first`, `last`, `single`, `elementAt`, `find`, `findIndex`, `every`, `isEmpty`, `defaultIfEmpty`, `throwIfEmpty` | The source completes empty | Three kinds of answer: complete empty, sentinel, error. Three operators are compositions of the primitives | [docs/15](docs/15-terminal-deciders-step-tables.md), 14 tests |
| 7 | Error handling | `retry`, `retryWhen`, `catchError` | An error that is not the end | A notifier that completes without emitting swallows the error. `retryWhen` with `take(n)` also cancels the attempt it just started. `catchError` catches one level | [docs/16](docs/16-error-handling-step-tables.md), 16 tests |

Four rules hold across all seven, and none of them is visible from a single family:

- **Error never waits.** In every family a source error is acted on the instant it arrives. Six families forward it; the seventh converts it into a retry, a replacement, a different error, or a completion.
- **Completion is where operators differ.** Flush, wait, drop, drain, four conditions, a configurable reset, a flush of every region, three kinds of empty-source answer, and a completion produced from an error. No two families answer `complete` the same way.
- **The recurring footgun is a helper observable's own terminal.** A duration, a boundary, a closing selector, a reset notifier, or a retry notifier that completes without emitting is read differently by almost every operator that takes one: ignored by `buffer` and `withLatestFrom`, a strand in `audit` and `bufferWhen`, a boundary in `windowWhen`, a swallowed error in `retry` and `retryWhen`, a dead clock in `sample`. "What happens when the notifier completes?" is the question to ask of any operator that accepts one.
- **Every family added a state no pattern names.** `Draining`, `Stranded`, `Lingering`, `Grace`, `Ended`, `Waiting`, `Handler`, a `Filling` join with a dead source, an eager empty window. The patterns are components; these are the product states where behavior forks, and they appear only when the table is written out.

## 15. Where this leads

The seven families cover every operator in [docs/05](docs/05-operator-catalog.md) whose state is a timer, a registry, a queue, a slot, a connection, a region, a decision, or a retry policy. What remains in the catalog is the stateless and near-stateless core: `map`, `filter`, `tap`, `scan`, `reduce`, the `distinct` family, `skip` and its variants, `delay` and the scheduler operators, the aggregates. Their rows and their pattern tests already say what a step table would, because their state is one component and their action list is one line.

The two remaining parts of the model are composition and equivalence, and [docs/17](docs/17-composition-and-equivalence.md) does both. Mealy machines compose by cascade, which is what `pipe` does: one step of the composite runs the first machine, then feeds each of its downstream notifications through the second in order, and stops the moment the second is `Done`. Two tables therefore determine the table of their cascade, and the unsubscribe rows of every family turn out to be the rows that composition exercises. Equivalence is trace equality on every source, which is bisimulation for deterministic machines. A marble test is one source; the suite checks each law on 200 random sources from a fixed seed and reports the counterexample when one fails. Fourteen laws hold, one holds with a side condition that the terminal-decider matrix predicts, two fail, and two hold on the trace but not on the actions, which separates three levels of equivalence: trace, action, and implementation.

Where: [docs/17](docs/17-composition-and-equivalence.md) and `tests/composition-and-equivalence.test.ts`.

## Glossary, as the terms are used here

- **Causal**: the output at any moment depends only on inputs so far. Every operator is causal.
- **Product state**: the state of an operator is a tuple of components, one per pattern it carries.
- **Invariant**: a relation between components that always holds, such as "a value is stored exactly when the timer is armed". Invariants make pairs unreachable.
- **Reachable**: a pair of state and event the machine can actually encounter. Tables list every reachable pair.
- **Absorbing**: a terminal state that no event leaves. `Done` in every table.
- **Frame**: one unit of virtual time; one marble character.
- **Witness**: a concrete marble scenario that exercises one row or one property.
- **Discriminating experiment**: a property plus a positive operator plus a neighbour that fails it.
- **Neighbour**: an operator from the adjacent pattern, used to make a test decide.
- **Subscription marble**: `^` for subscribe, `!` for unsubscribe; shows when producers were opened and cancelled.
- **Probe**: a callback used to observe state that leaves no trace downstream, such as the arguments `tap` receives.
- **Draining**: the source has completed while a window or timer is still armed, or the outer has completed while inners still run; the operator is not yet `Done`.
- **Stranded**: a stored value or an open region whose only closer has gone silent, while the source is still live. `audit`, `throttle`, and `bufferWhen` reach it; `debounce` and `windowWhen` do not.
- **Lingering**: a shared connection with no subscribers and no reset pending. Values arriving now are lost in a `Subject` or kept in a `ReplaySubject`.
- **Grace**: a shared connection with no subscribers and a reset notifier armed. A returning subscriber cancels the reset.
- **Ended**: a shared subject whose terminal was kept. The only absorbing state in the multicasting family.
- **Waiting**: a retrying operator between a source error and the next attempt; nothing is subscribed to the source.
- **Handler**: `catchError` forwarding its replacement observable. Errors there are not caught again.
- **Sentinel**: a value an operator emits to stand for "nothing", such as `undefined` from `find`, `-1` from `findIndex`, `true` from `every` and `isEmpty` on an empty source.
- **Connection log, attempt log**: the subscription log of a cold source read as a record of connects or retries, one entry per subscription.
- **Eager window**: a window `windowCount` opens the moment the previous one fills, before any value for it exists. It may complete empty.

## The documents in reading order

1. [README](README.md) and [docs/01](docs/01-model.md): the model and the formula.
2. [docs/02](docs/02-state-transition-patterns.md) and [docs/03](docs/03-action-policies.md): the two vocabularies.
3. [docs/05](docs/05-operator-catalog.md): the vocabularies applied to every operator.
4. [docs/07](docs/07-catalog-evaluation.md): the catalog treated as claims and checked against source.
5. [docs/08](docs/08-state-transition-pattern-tests.md) and [docs/09](docs/09-action-policy-pattern-tests.md): the vocabularies turned into experiments.
6. [docs/10](docs/10-timing-family-step-tables.md): the first family taken to the full machine, and the method in full.
7. [docs/11](docs/11-flattening-family-step-tables.md) to [docs/16](docs/16-error-handling-step-tables.md): the other six families, in the order they were written. Each one's contrast table assumes the ones before it. Read each with its `tests/*-step-tables.test.ts` file open; every row names the test that proves it.
