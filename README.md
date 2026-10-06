# RxJS operator state patterns and output actions

Analysis created by SuperGrok.

This repository records a behavioral reading of RxJS operators as Mealy-style machines. An operator is not only a function from value to value. It is a machine that remembers something, then performs an ordered list of actions.

\[
\text{RxJS Operator Behavior} = \text{State Transition Pattern} + \text{Action Policy Pattern}
\]

\[
\operatorname{step}(s, e) = (s', a^*)
\]

- State transition \(T(s, e) = s'\): what the operator remembers, and how that memory changes.
- Action policy \(G(s, e, s') = a^*\): what it does, in order. Emit, drop, subscribe, unsubscribe, schedule, cancel, complete, error.

Downstream `next` / `error` / `complete` are a subset of the actions. Cancelling an inner subscription is an action too. That is why the second half is an action policy, not only an output policy.

\[
\text{Output actions} \subseteq \text{Actions}
\]

The 15 + 15 lists are a behavioral vocabulary, not a proof that the set is complete. A real operator is a product of state components plus an ordered action list.

## Contents

- [Reading guide: the Mealy machine in the context of RxJS](mealy-machine-in-context-of-rxjs.md) — the chain of reasoning behind docs 07 to 10, in 14 steps, with a glossary
- [Model](docs/01-model.md) — Mealy reading, the formula, and the corrections that made Action Policy the formal term
- [State transition patterns](docs/02-state-transition-patterns.md) — ST-01 to ST-15
- [Action policies](docs/03-action-policies.md) — AP-01 to AP-15
- [One example per policy](docs/04-policy-examples.md)
- [Operator catalog](docs/05-operator-catalog.md) — each operator from the uploaded list, as state transition :: action policies
- [One example per operator](docs/06-operator-examples.md)
- [Evaluation of the operator catalog](docs/07-catalog-evaluation.md) — every row checked against RxJS 7.8.2 source; corrections applied
- [Operational tests for the state transition patterns](docs/08-state-transition-pattern-tests.md) — one discriminating experiment per ST pattern, runnable
- [Operational tests for the action policy patterns](docs/09-action-policy-pattern-tests.md) — one discriminating experiment per AP pattern, runnable
- [Step tables for the timing family](docs/10-timing-family-step-tables.md) — full state × event tables for `debounceTime`, `auditTime`, `sampleTime`, `throttleTime`, every row verified, including complete-while-pending

## Running the tests

The patterns in `docs/08` and `docs/09` and the step tables in `docs/10` are backed by Vitest suites in `tests/`, run in virtual time with `TestScheduler`.

```sh
npm install
npm test
```

## The two questions

State transition pattern: what does the operator remember, and how does that memory change when an event occurs?

Action policy pattern: given the event and the machine state, what ordered actions should occur?

Operator names stop being the primary unit. `switchMap`, `concatMap`, `auditTime`, and `sampleTime` become a memory shape plus a policy.

```text
filter          Count / Index        +  Gate / Suppress
scan            Accumulate / Fold    +  Emit Derived State
reduce          Accumulate / Fold    +  Emit Derived State, once, on complete
concatMap       FIFO Queue + Active  +  Serialize + Forward
switchMap       Active Selection     +  Current-Wins + Forward
exhaustMap      Busy / Idle          +  Gate new work + Forward
debounceTime    Replace Latest       +  Trigger on silence
throttleTime    Open / Locked        +  Forward leading, Gate while locked
```

`scan` and `reduce` are the cleanest check of the split: same fold, different action policy. The flattening family is the other check: same project-to-inner idea, different admission and cancellation actions.

## Modeling choices kept in this catalog

- `map` and `filter` are Count / Index in a faithful model, because the callback receives `index`. Identity is the teaching abstraction.
- `throttle` / `throttleTime` are Open / Locked, not Replace Latest. Replace Latest appears only when a trailing emit is configured.
- `mergeMap` grows a FIFO queue only when `concurrent` is bounded.
- Timing is relative to the chosen scheduler, not to `setTimeout`.
- `mapTo`, `mergeMapTo`, `switchMapTo`, `concatMapTo`, `pluck`, `publish`, `publishBehavior`, `publishLast`, `publishReplay`, and `retryWhen` are the older names.

## Sentence to keep

An RxJS operator is a reactive machine that updates internal state in response to events and executes an ordered policy of actions.

\[
(s, e) \xrightarrow{\text{operator}} (s', a^*)
\]
