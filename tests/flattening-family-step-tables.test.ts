/**
 * Step tables for the flattening family, verified row by row against RxJS 7.8.2.
 *
 * Operators: mergeMap (unbounded and bounded), concatMap, switchMap, exhaustMap,
 * plus the *All variants, which are the same machines with an identity project.
 * Row IDs (M1, MB2, C3, SW4, E5, X6) match docs/11-flattening-family-step-tables.md.
 * Inner observables are cold marbles keyed by the outer value. A probe records
 * every call to project with its frame and index, so the rows about when and
 * whether project is called are verified, not inferred.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import {
  Observable,
  concatAll,
  concatMap,
  exhaustAll,
  exhaustMap,
  mergeAll,
  mergeMap,
  switchAll,
  switchMap,
} from "rxjs";

type Cold = (marbles: string, values?: Record<string, unknown>) => Observable<string>;
type Call = { frame: number; value: string; index: number };

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

/** Build keyed inners and a project function that records each call. */
function projector(ts: TestScheduler, inner: Record<string, Observable<string>>) {
  const calls: Call[] = [];
  const project = (value: string, index: number): Observable<string> => {
    calls.push({ frame: ts.frame, value, index });
    return inner[value];
  };
  return { calls, project };
}

describe("mergeMap(project), unbounded", () => {
  it("M1 Live + onext admits at once; M2 inext forwards; M3 icomplete decrements; M7 ocomplete while active waits; M4/M5 last icomplete completes", () => {
    const ts = scheduler();
    let calls: Call[] = [];
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      const p = projector(ts, inner);
      calls = p.calls;
      // Outer completes at 2 with two inners live. Output completes at 4, when the last inner does.
      expectObservable(cold("ab|").pipe(mergeMap(p.project))).toBe("abab|");
      expectSubscriptions(inner.a.subscriptions).toBe("^--!");
      expectSubscriptions(inner.b.subscriptions).toBe("-^--!");
    });
    expect(calls).toEqual([
      { frame: 0, value: "a", index: 0 },
      { frame: 1, value: "b", index: 1 },
    ]);
  });

  it("M6 ocomplete with nothing active completes at once", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const inner = { a: cold("a|") };
      expectObservable(cold("a---|").pipe(mergeMap((x: string) => inner[x as "a"]))).toBe("a---|");
    });
  });

  it("M8 oerror while an inner is active errors now and cancels the inner", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a--a|") };
      expectObservable(cold("a-#").pipe(mergeMap((x: string) => inner[x as "a"]))).toBe("a-#");
      expectSubscriptions(inner.a.subscriptions).toBe("^-!");
    });
  });

  it("M9 ierror errors now, cancels the outer and every other inner", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-#"), b: cold("b-b|") };
      const outer = cold("ab---|");
      expectObservable(outer.pipe(mergeMap((x: string) => inner[x as "a" | "b"]))).toBe("ab#");
      expectSubscriptions(inner.b.subscriptions).toBe("-^!");
      expectSubscriptions(outer.subscriptions).toBe("^-!");
    });
  });
});

describe("mergeMap(project, 2), bounded", () => {
  it("MB1 below the limit admits; MB2 at the limit queues without calling project; MB3 icomplete with a queue dequeues and projects then; MB6 ocomplete with a queue waits for it to drain", () => {
    const ts = scheduler();
    let calls: Call[] = [];
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b--b|"), c: cold("c-c|") };
      const p = projector(ts, inner);
      calls = p.calls;
      // c arrives at 2 with two inners live and waits. a completes at 3, c is projected and subscribed at 3.
      // Outer completes at 3; output completes at 6, when c's inner does.
      expectObservable(cold("abc|").pipe(mergeMap(p.project, 2))).toBe("abacbc|");
      expectSubscriptions(inner.a.subscriptions).toBe("^--!");
      expectSubscriptions(inner.b.subscriptions).toBe("-^---!");
      expectSubscriptions(inner.c.subscriptions).toBe("---^--!");
    });
    expect(calls).toEqual([
      { frame: 0, value: "a", index: 0 },
      { frame: 1, value: "b", index: 1 },
      { frame: 3, value: "c", index: 2 },
    ]);
  });
});

describe("concatMap(project)", () => {
  it("C1 idle + onext admits; C2 busy + onext queues, project deferred; C3 icomplete with a queue dequeues; C6 ocomplete with a queue drains it before completing", () => {
    const ts = scheduler();
    let calls: Call[] = [];
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      const p = projector(ts, inner);
      calls = p.calls;
      // Outer completes at 2 with b still queued. b is projected at 3, runs to 6, then the output completes.
      expectObservable(cold("ab|").pipe(concatMap(p.project))).toBe("a-ab-b|");
      expectSubscriptions(inner.a.subscriptions).toBe("^--!");
      expectSubscriptions(inner.b.subscriptions).toBe("---^--!");
    });
    expect(calls).toEqual([
      { frame: 0, value: "a", index: 0 },
      { frame: 3, value: "b", index: 1 },
    ]);
  });

  it("C9 oerror with a queue errors now, cancels the inner, and never projects the queued value", () => {
    const ts = scheduler();
    let calls: Call[] = [];
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a--a|"), b: cold("b|") };
      const p = projector(ts, inner);
      calls = p.calls;
      expectObservable(cold("ab#").pipe(concatMap(p.project))).toBe("a-#");
      expectSubscriptions(inner.a.subscriptions).toBe("^-!");
      expectSubscriptions(inner.b.subscriptions).toBe([]);
    });
    expect(calls.map((c) => c.value)).toEqual(["a"]);
  });

  it("C10 ierror errors now, cancels the outer, and drops the queue", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-#"), b: cold("b-b|") };
      const outer = cold("ab---|");
      expectObservable(outer.pipe(concatMap((x: string) => inner[x as "a" | "b"]))).toBe("a-#");
      expectSubscriptions(inner.b.subscriptions).toBe([]);
      expectSubscriptions(outer.subscriptions).toBe("^-!");
    });
  });
});

describe("switchMap(project)", () => {
  it("SW1 none + onext admits; SW2 some + onext cancels the current inner before projecting the new one; SW7 ocomplete while active waits; SW5 icomplete then completes", () => {
    const ts = scheduler();
    let aClosedWhenBProjected: boolean | undefined;
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      const project = (x: string): Observable<string> => {
        if (x === "b") {
          // The previous inner is already unsubscribed when project runs for the replacement.
          aClosedWhenBProjected = inner.a.subscriptions[0].unsubscribedFrame !== Infinity;
        }
        return inner[x as "a" | "b"];
      };
      // Outer completes at 2 with b live. Output completes at 4.
      expectObservable(cold("ab|").pipe(switchMap(project))).toBe("ab-b|");
      expectSubscriptions(inner.a.subscriptions).toBe("^!");
      expectSubscriptions(inner.b.subscriptions).toBe("-^--!");
    });
    expect(aClosedWhenBProjected).toBe(true);
  });

  it("SW6 ocomplete with no inner completes at once", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const inner = { a: cold("a|") };
      expectObservable(cold("a---|").pipe(switchMap((x: string) => inner[x as "a"]))).toBe("a---|");
    });
  });

  it("SW8 oerror while an inner is active errors now and cancels it", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a--a|") };
      expectObservable(cold("a-#").pipe(switchMap((x: string) => inner[x as "a"]))).toBe("a-#");
      expectSubscriptions(inner.a.subscriptions).toBe("^-!");
    });
  });

  it("SW9 ierror errors now and cancels the outer", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-#") };
      const outer = cold("a---|");
      expectObservable(outer.pipe(switchMap((x: string) => inner[x as "a"]))).toBe("a-#");
      expectSubscriptions(outer.subscriptions).toBe("^-!");
    });
  });
});

describe("exhaustMap(project)", () => {
  it("E1 idle + onext admits; E2 busy + onext drops without calling project; E7 ocomplete while busy waits; E5 icomplete then completes", () => {
    const ts = scheduler();
    let calls: Call[] = [];
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      const p = projector(ts, inner);
      calls = p.calls;
      // Outer completes at 2 while a is busy. Output completes at 3. b is never subscribed.
      expectObservable(cold("ab|").pipe(exhaustMap(p.project))).toBe("a-a|");
      expectSubscriptions(inner.a.subscriptions).toBe("^--!");
      expectSubscriptions(inner.b.subscriptions).toBe([]);
    });
    expect(calls).toEqual([{ frame: 0, value: "a", index: 0 }]);
  });

  it("E2' the index counts admitted values only: a dropped value does not advance it", () => {
    const ts = scheduler();
    let calls: Call[] = [];
    ts.run(({ cold, expectObservable }) => {
      const inner = { a: cold("a-a|"), b: cold("b|"), c: cold("c-c|") };
      const p = projector(ts, inner);
      calls = p.calls;
      // b at 1 is dropped while a is busy. c at 4 is the second admitted value and receives index 1, not 2.
      expectObservable(cold("ab--c|").pipe(exhaustMap(p.project))).toBe("a-a-c-c|");
    });
    expect(calls).toEqual([
      { frame: 0, value: "a", index: 0 },
      { frame: 4, value: "c", index: 1 },
    ]);
  });

  it("E6 ocomplete while idle completes at once", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const inner = { a: cold("a|") };
      expectObservable(cold("a---|").pipe(exhaustMap((x: string) => inner[x as "a"]))).toBe("a---|");
    });
  });

  it("E8 oerror while busy errors now and cancels the inner; E9 ierror errors now and cancels the outer", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a--a|") };
      expectObservable(cold("a-#").pipe(exhaustMap((x: string) => inner[x as "a"]))).toBe("a-#");
      expectSubscriptions(inner.a.subscriptions).toBe("^-!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-#") };
      const outer = cold("a---|");
      expectObservable(outer.pipe(exhaustMap((x: string) => inner[x as "a"]))).toBe("a-#");
      expectSubscriptions(outer.subscriptions).toBe("^-!");
    });
  });
});

describe("the *All variants are the same machines with an identity project", () => {
  it("ALL mergeAll, concatAll, switchAll, exhaustAll reproduce the *Map witnesses", () => {
    const run = (op: (s: Observable<Observable<string>>) => Observable<string>, expected: string) => {
      scheduler().run(({ cold, expectObservable }) => {
        const inner = { a: cold("a-a|"), b: cold("b-b|") };
        const outer = (cold as unknown as Cold)("ab|", inner) as unknown as Observable<Observable<string>>;
        expectObservable(op(outer)).toBe(expected);
      });
    };
    run((s) => s.pipe(mergeAll()), "abab|");
    run((s) => s.pipe(concatAll()), "a-ab-b|");
    run((s) => s.pipe(switchAll()), "ab-b|");
    run((s) => s.pipe(exhaustAll()), "a-a|");
  });
});

describe("teardown: unsubscribe while an inner is active cancels the inner and releases the outer (X1 to X4)", () => {
  const cases: Array<[string, (p: (x: string) => Observable<string>) => (s: Observable<string>) => Observable<string>]> = [
    ["mergeMap", (p) => mergeMap(p)],
    ["concatMap", (p) => concatMap(p)],
    ["switchMap", (p) => switchMap(p)],
    ["exhaustMap", (p) => exhaustMap(p)],
  ];
  for (const [name, make] of cases) {
    it(name, () => {
      scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
        const inner = { a: cold("a--a|") };
        const outer = cold("a----|");
        expectObservable(make((x: string) => inner[x as "a"])(outer), "^-!").toBe("a-");
        expectSubscriptions(inner.a.subscriptions).toBe("^-!");
        expectSubscriptions(outer.subscriptions).toBe("^-!");
      });
    });
  }
});
