# The Mealy machine in the context of RxJS: a reading guide

Written by Claude Code (Claude Fable 5.1), 2026-10-06. This is the chain of reasoning behind docs 07 to 10 and the three test suites, laid out as the steps a reader needs in order, each with the place in the repository where it is applied. Read it before the next operator family.

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

Where: the event lists at the top of [docs/10](docs/10-timing-family-step-tables.md); the `complete`, `expire`, `tick`, `durComplete`, and `unsubscribe` columns of its tables.

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

Where: the source excerpts quoted in [docs/07](docs/07-catalog-evaluation.md); the derivation at the start of each table in [docs/10](docs/10-timing-family-step-tables.md).

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

Writing the table forces states into the open that no pattern names. `Draining` is a source that has completed while a window is still armed. `Stranded` is a stored value whose duration has gone silent. These are the states where family members differ, and the cross-channel cases from step 2 are the events that reach them.

Where: [docs/10](docs/10-timing-family-step-tables.md), the tables and the cross-cutting matrix.

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

Where: the whole of [docs/10](docs/10-timing-family-step-tables.md) is this method applied once; `tests/timing-family-step-tables.test.ts` is step 7.

## 13. State the semantic action, note the implementation

`debounceTime` does not cancel and reschedule its timer on each value; it records the arrival time and, when the task wakes, reschedules itself for the remainder. The observable behavior is a restart, so the table says Cancel, Schedule and the prose notes the implementation. `throttleTime` in its default configuration keeps a stale value in its slot that no row ever emits; the table treats the slot as empty. The rule from step 5 applies at every level.

Where: the notes under the `debounceTime` and leading `throttleTime` tables in [docs/10](docs/10-timing-family-step-tables.md).

## 14. Where this leads

The flattening family is next, and its open question mirrors the timing family's: what happens when the outer completes while inners are still active. Beyond that, two parts of the model are still unused. Mealy machines compose by cascade, which is what `pipe` does. Their natural equivalence is bisimulation, which is what marble tests approximate. Composition and equivalence are what would turn the catalog from a classification into an algebra.

Where: the closing sections of [docs/10](docs/10-timing-family-step-tables.md).

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
- **Draining**: the source has completed while a window or timer is still armed; the operator is not yet `Done`.
- **Stranded**: a value is stored, no duration is active, and the source is still live. Reachable only in the selector variants.

## The documents in reading order

1. [README](README.md) and [docs/01](docs/01-model.md): the model and the formula.
2. [docs/02](docs/02-state-transition-patterns.md) and [docs/03](docs/03-action-policies.md): the two vocabularies.
3. [docs/05](docs/05-operator-catalog.md): the vocabularies applied to every operator.
4. [docs/07](docs/07-catalog-evaluation.md): the catalog treated as claims and checked against source.
5. [docs/08](docs/08-state-transition-pattern-tests.md) and [docs/09](docs/09-action-policy-pattern-tests.md): the vocabularies turned into experiments.
6. [docs/10](docs/10-timing-family-step-tables.md): one family taken to the full machine.
