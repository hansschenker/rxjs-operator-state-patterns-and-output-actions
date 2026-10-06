/**
 * Step tables for the join family, verified row by row against RxJS 7.8.2.
 *
 * Operators: combineLatest, zip, forkJoin (creation functions) and withLatestFrom
 * (operator). Row IDs (CL1, Z2, FJ3, WL4, X5) match docs/12-join-family-step-tables.md.
 * Two sources per witness. Scenarios avoid two events on the same frame where the
 * order would change the output.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import { combineLatest, forkJoin, withLatestFrom, zip } from "rxjs";

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

describe("combineLatest([s1, s2])", () => {
  it("CL2 a value before readiness overwrites its slot; CL1 the last slot filling emits; CL3 every later value emits; CL6 completes when all complete", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const s1 = cold("a-b---|");
      const s2 = cold("---x-y|");
      // `a` is overwritten by `b` before s2 has a value. Readiness at 3 emits [b, x], not [a, x].
      expectObservable(combineLatest([s1, s2])).toBe("---p-q|", { p: ["b", "x"], q: ["b", "y"] });
    });
  });

  it("CL5 a source that completes after emitting keeps combining through its last value; completion waits for the rest", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const s1 = cold("a|");
      const s2 = cold("--x-y|");
      expectObservable(combineLatest([s1, s2])).toBe("--p-q|", { p: ["a", "x"], q: ["a", "y"] });
    });
  });

  it("CL4 a source that completes without emitting does not end the join: silence until every source has completed", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("-|");
      const s2 = cold("x-y---|");
      // No short-circuit. Nothing can ever be emitted, yet s2 runs to its own completion at 6.
      expectObservable(combineLatest([s1, s2])).toBe("------|");
      expectSubscriptions(s2.subscriptions).toBe("^-----!");
    });
  });

  it("CL7 any source erroring errors now and cancels the others", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("a-#");
      const s2 = cold("-x---|");
      expectObservable(combineLatest([s1, s2])).toBe("-p#", { p: ["a", "x"] });
      expectSubscriptions(s2.subscriptions).toBe("^-!");
    });
  });
});

describe("zip([s1, s2])", () => {
  it("Z1 a value queues when another queue is empty; Z2 a value that makes every queue non-empty emits the heads", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const s1 = cold("ab--|");
      const s2 = cold("--x-y|");
      // a and b queue. x pairs with a at 2, y pairs with b at 4. s1 completed at 4 with its queue drained: complete.
      expectObservable(zip([s1, s2])).toBe("--p-(q|)", { p: ["a", "x"], q: ["b", "y"] });
    });
  });

  it("Z4 a source that completes with queued values keeps pairing; Z5 the join completes when that queue drains", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const s1 = cold("ab|");
      const s2 = cold("---x-y|");
      // s1 completes at 2 holding [a, b]. Pairs at 3 and 5; complete at 5, before s2 completes at 6.
      expectObservable(zip([s1, s2])).toBe("---p-(q|)", { p: ["a", "x"], q: ["b", "y"] });
    });
  });

  it("Z3 a source that completes with an empty queue completes the join now, dropping the other queues", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("-|");
      const s2 = cold("xy----|");
      expectObservable(zip([s1, s2])).toBe("-|");
      expectSubscriptions(s2.subscriptions).toBe("^!");
    });
  });

  it("Z6 any source erroring errors now and cancels the others", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("a-#");
      const s2 = cold("x----|");
      expectObservable(zip([s1, s2])).toBe("p-#", { p: ["a", "x"] });
      expectSubscriptions(s2.subscriptions).toBe("^-!");
    });
  });
});

describe("forkJoin([s1, s2])", () => {
  it("FJ1 values overwrite the slot; FJ2 a completion with a value waits for the rest; FJ3 the last completion emits once and completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const s1 = cold("ab|");
      const s2 = cold("--x-|");
      expectObservable(forkJoin([s1, s2])).toBe("----(p|)", { p: ["b", "x"] });
    });
  });

  it("FJ4 a source that completes without emitting completes the join now, without a value, and cancels the others", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("-|");
      const s2 = cold("x---|");
      // The short-circuit that combineLatest does not have (compare CL4).
      expectObservable(forkJoin([s1, s2])).toBe("-|");
      expectSubscriptions(s2.subscriptions).toBe("^!");
    });
  });

  it("FJ5 any source erroring errors now and cancels the others", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("a-#");
      const s2 = cold("x----|");
      expectObservable(forkJoin([s1, s2])).toBe("--#");
      expectSubscriptions(s2.subscriptions).toBe("^-!");
    });
  });
});

describe("primary.pipe(withLatestFrom(secondary))", () => {
  it("WL3 primary values before readiness are dropped; WL1 the secondary's first value makes it ready; WL4 each primary value then emits; WL2 later secondary values replace the slot; WL7 primary complete completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const primary = cold("a-b-c-|");
      const secondary = cold("-x-y-|");
      // a at 0 is dropped. x at 1 readies. b pairs with x, c pairs with y. Secondary completes at 5: ignored.
      expectObservable(primary.pipe(withLatestFrom(secondary))).toBe("--p-q-|", { p: ["b", "x"], q: ["c", "y"] });
    });
  });

  it("WL5 a secondary that completes is ignored and its last value stays usable", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const primary = cold("---a-|");
      const secondary = cold("x|");
      expectObservable(primary.pipe(withLatestFrom(secondary))).toBe("---p-|", { p: ["a", "x"] });
    });
  });

  it("WL7 the primary completing ends the join even when never ready, cancelling the secondary", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const primary = cold("a-|");
      const secondary = cold("----x|");
      expectObservable(primary.pipe(withLatestFrom(secondary))).toBe("--|");
      expectSubscriptions(secondary.subscriptions).toBe("^-!");
    });
  });

  it("WL6 a secondary erroring errors the join and cancels the primary", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const primary = cold("a-b--|");
      const secondary = cold("x-#");
      expectObservable(primary.pipe(withLatestFrom(secondary))).toBe("p-#", { p: ["a", "x"] });
      expectSubscriptions(primary.subscriptions).toBe("^-!");
    });
  });
});

describe("teardown: unsubscribe cancels every source (X1 to X4)", () => {
  it("combineLatest, zip, forkJoin, withLatestFrom", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const a = cold("a----|");
      const b = cold("x----|");
      expectObservable(combineLatest([a, b]), "^-!").toBe("p-", { p: ["a", "x"] });
      expectSubscriptions(a.subscriptions).toBe("^-!");
      expectSubscriptions(b.subscriptions).toBe("^-!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const a = cold("a----|");
      const b = cold("x----|");
      expectObservable(zip([a, b]), "^-!").toBe("p-", { p: ["a", "x"] });
      expectSubscriptions(a.subscriptions).toBe("^-!");
      expectSubscriptions(b.subscriptions).toBe("^-!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const a = cold("a----|");
      const b = cold("x----|");
      expectObservable(forkJoin([a, b]), "^-!").toBe("--");
      expectSubscriptions(a.subscriptions).toBe("^-!");
      expectSubscriptions(b.subscriptions).toBe("^-!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const a = cold("a----|");
      const b = cold("x----|");
      expectObservable(a.pipe(withLatestFrom(b)), "^-!").toBe("p-", { p: ["a", "x"] });
      expectSubscriptions(a.subscriptions).toBe("^-!");
      expectSubscriptions(b.subscriptions).toBe("^-!");
    });
  });
});
