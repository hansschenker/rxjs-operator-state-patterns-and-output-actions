# Model

Analysis created by SuperGrok.

## The machine

An RxJS operator can be modeled as a Mealy-style stateful transducer. The operator is the whole machine, not merely the transition function.

\[
M = (S, s_0, E, A, T, G)
\]

- \(S\): operator state
- \(s_0\): initial state
- \(E\): input events
- \(A\): actions, including notifications and resource operations
- \(T\): state transition
- \(G\): action policy

Combined:

\[
\operatorname{step} : S \times E \rightarrow S \times A^*
\]

Receive an event, inspect current state, change state, emit zero or more ordered actions.

Not every operator is a finite-state machine. `scan` can range over the whole value domain. `buffer` can grow without a fixed bound. `mergeMap` can hold an unbounded set of inners. `distinct` can remember an unbounded key set. The accurate claim is: an RxJS operator is an extended Mealy machine whose state space need not be finite.

## Stateless operators still have a transition

`map` and `filter` are not exceptions. Their transition is identity if the index is ignored:

\[
T(s, e) = s
\]

The interesting part is \(G\). In exact RxJS semantics the projector and the predicate receive `(value, index)`, so the semantic state includes a counter. Three levels stay useful:

- Teaching state: what a reader needs in order to use the operator.
- Semantic state: what user code can observe, including the index.
- Implementation state: timers, handles, queues that a runtime needs and a specification can name as tokens.

## From output policy to action policy

The teaching sentence can stay close to the original:

\[
\text{Operator} = \text{State Transition} + \text{Output Policy}
\]

The formal model is:

\[
\operatorname{step}(s, e) = (s', a^*)
\]

\[
A = \{\operatorname{Next}(v),\ \operatorname{Error}(err),\ \operatorname{Complete},\ \operatorname{Subscribe}(r),\ \operatorname{Unsubscribe}(r),\ \operatorname{Schedule}(t),\ \operatorname{Cancel}(t),\ \ldots\}
\]

Output is a projection of \(A^*\). This is what makes `switchMap` fit: cancelling the old inner is an action, not an output decision.

```text
switchMap
  state:   activeInner := newInner
  actions: unsubscribe(oldInner), subscribe(newInner), forward(inner.next)
```

Prefixes used here: ST for state transition pattern, AP for action policy pattern. OP was dropped because it reads as "operator".

## Corrections kept from the slide review

1. The formal second component is an action policy. Output policy is the downstream-visible subset.
2. `map` and `filter` carry Count / Index when the model is exact.
3. `debounceTime`, `auditTime`, `sampleTime`, and `throttleTime` do not share one memory rule. `throttleTime` defaults to leading on, trailing off, so Open / Locked is the central state.
4. A max-wait guarantee is about the pending cycle, not about every source value. Superseded values are never emitted. While a value remains pending, that cycle cannot stay unflushed longer than `maxWait`, unless the subscription errors or is cancelled. Time is scheduler time.

## What the split is for

Documentation, classification, testing, comparison, and design of new operators. Tests can be written against events: source next, inner next, timer expiry, complete, error, unsubscribe. A new operator can be specified by choosing memory and actions first, then interpreting those actions on a scheduler.
