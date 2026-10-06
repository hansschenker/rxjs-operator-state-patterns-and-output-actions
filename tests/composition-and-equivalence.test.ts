/**
 * Composition and equivalence, verified against RxJS 7.8.2.
 *
 * Part 1, composition: `pipe` is the cascade of two Mealy machines. Four witnesses
 * (C1 to C4) show the rules that follow: a multi-action step is processed one
 * notification at a time, the downstream machine's Done propagates upstream as an
 * unsubscribe, and the composite's Done is the downstream machine's Done.
 *
 * Part 2, equivalence: an operator law is checked on a few hundred randomly
 * generated sources, not on one marble. `checkLaw` returns the first
 * counterexample it finds, so a failing law is reported with the source that
 * breaks it. Row IDs (C1, L1, ...) match docs/17-composition-and-equivalence.md.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import {
  Observable,
  OperatorFunction,
  concatAll,
  concatMap,
  defaultIfEmpty,
  delay,
  dematerialize,
  distinctUntilChanged,
  filter,
  first,
  last,
  map,
  materialize,
  mergeMap,
  reduce,
  scan,
  skip,
  startWith,
  switchAll,
  switchMap,
  take,
  takeLast,
  tap,
  throwIfEmpty,
} from "rxjs";

type Note = { f: number; k: "N" | "E" | "C"; v?: unknown; e?: string };
type Cold = TestScheduler["createColdObservable"];

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

function trace<T>(ts: TestScheduler, source: Observable<T>): Note[] {
  const notes: Note[] = [];
  source.subscribe({
    next: (v) => notes.push({ f: ts.frame, k: "N", v }),
    error: (err: unknown) => notes.push({ f: ts.frame, k: "E", e: err instanceof Error ? err.name : String(err) }),
    complete: () => notes.push({ f: ts.frame, k: "C" }),
  });
  return notes;
}

/** Deterministic PRNG so a counterexample is reproducible from its seed. */
function rng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s / 4294967296;
  };
}

/** One to eight frames, each a letter a..e or silence; ends in complete, error, or neither. */
function randomMarbles(rand: () => number): string {
  const len = 1 + Math.floor(rand() * 8);
  let m = "";
  for (let i = 0; i < len; i++) m += rand() < 0.5 ? "abcde"[Math.floor(rand() * 5)] : "-";
  const t = rand();
  return m + (t < 0.5 ? "|" : t < 0.75 ? "#" : "");
}

type Sides<T, R> = { lhs: OperatorFunction<T, R>; rhs: OperatorFunction<T, R> };
type LawResult = { holds: true; checked: number } | { holds: false; marbles: string; left: Note[]; right: Note[]; checked: number };

/** Compare the two sides on `runs` random sources. `make` may draw parameters and build inners per run. */
function checkLaw<R>(
  make: (rand: () => number, cold: Cold) => Sides<string, R>,
  options: { runs?: number; seed?: number; only?: (marbles: string) => boolean } = {}
): LawResult {
  const { runs = 200, seed = 7, only } = options;
  const rand = rng(seed);
  let checked = 0;
  for (let i = 0; i < runs; i++) {
    const marbles = randomMarbles(rand);
    if (only && !only(marbles)) continue;
    checked++;
    const ts = scheduler();
    let left: Note[] = [];
    let right: Note[] = [];
    ts.run(({ cold }) => {
      const { lhs, rhs } = make(rand, cold as Cold);
      const src = cold(marbles);
      left = trace(ts, src.pipe(lhs));
      right = trace(ts, src.pipe(rhs));
    });
    if (JSON.stringify(left) !== JSON.stringify(right)) return { holds: false, marbles, left, right, checked };
  }
  return { holds: true, checked };
}

const hasValue = (m: string) => /[a-e]/.test(m);
const completesEmpty = (m: string) => !hasValue(m) && m.endsWith("|");

describe("composition: pipe is the cascade of two machines", () => {
  it("C1 a multi-action step is fed downstream one notification at a time; the downstream machine's state changes in between", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // takeLast(2) flushes b, c, complete in one step at frame 3. take(1) takes b and completes; c is suppressed.
      expectObservable(cold("abc|").pipe(takeLast(2), take(1))).toBe("---(b|)");
    });
  });

  it("C2 downstream Done cancels upstream in the middle of its step: tap sees only the value that take accepted", () => {
    const seen: string[] = [];
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("abc|");
      expectObservable(source.pipe(tap((x) => seen.push(x)), take(1))).toBe("(a|)");
      expectSubscriptions(source.subscriptions).toBe("(^!)");
    });
    expect(seen).toEqual(["a"]);
  });

  it("C3 downstream Done propagates upstream as unsubscribe: the outer and every live inner of mergeMap are cancelled, the queued inner is never subscribed", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const outer = cold("ab|");
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      expectObservable(outer.pipe(mergeMap((x: string) => inner[x as "a" | "b"]), take(1))).toBe("(a|)");
      expectSubscriptions(outer.subscriptions).toBe("(^!)");
      expectSubscriptions(inner.a.subscriptions).toBe("(^!)");
      expectSubscriptions(inner.b.subscriptions).toBe([]);
    });
  });

  it("C4 the composite is Done when the downstream machine is Done, not when the upstream one is", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b|");
      // take(1) is Done at frame 0 and the source is released there. The composite completes at 5.
      expectObservable(source.pipe(take(1), delay(5))).toBe("-----(a|)");
      expectSubscriptions(source.subscriptions).toBe("(^!)");
    });
  });
});

describe("equivalence: laws that hold on every random source tried", () => {
  it("L1 take(n) ∘ take(m) = take(min(n, m))", () => {
    const r = checkLaw((rand) => {
      const n = Math.floor(rand() * 4);
      const m = Math.floor(rand() * 4);
      return { lhs: (s: Observable<string>) => s.pipe(take(n), take(m)), rhs: take(Math.min(n, m)) };
    });
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L2 skip(n) ∘ skip(m) = skip(n + m)", () => {
    const r = checkLaw((rand) => {
      const n = Math.floor(rand() * 3);
      const m = Math.floor(rand() * 3);
      return { lhs: (s: Observable<string>) => s.pipe(skip(n), skip(m)), rhs: skip(n + m) };
    });
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L3 map(f) ∘ map(g) = map(g ∘ f)", () => {
    const f = (x: string) => x + x;
    const g = (x: string) => x.toUpperCase();
    const r = checkLaw(() => ({ lhs: (s: Observable<string>) => s.pipe(map(f), map(g)), rhs: map((x: string) => g(f(x))) }));
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L4 filter(p) ∘ filter(q) = filter(p ∧ q), for predicates that ignore the index", () => {
    const p = (x: string) => x !== "a";
    const q = (x: string) => x < "d";
    const r = checkLaw(() => ({ lhs: (s: Observable<string>) => s.pipe(filter(p), filter(q)), rhs: filter((x: string) => p(x) && q(x)) }));
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L5 delay(d) ∘ delay(e) = delay(d + e), errors included", () => {
    const r = checkLaw((rand) => {
      const d = 1 + Math.floor(rand() * 3);
      const e = 1 + Math.floor(rand() * 3);
      return { lhs: (s: Observable<string>) => s.pipe(delay(d), delay(e)), rhs: delay(d + e) };
    });
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L6 distinctUntilChanged is idempotent", () => {
    const r = checkLaw(() => ({
      lhs: (s: Observable<string>) => s.pipe(distinctUntilChanged(), distinctUntilChanged()),
      rhs: distinctUntilChanged<string>(),
    }));
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L7 defaultIfEmpty(d) ∘ defaultIfEmpty(e) = defaultIfEmpty(d)", () => {
    const r = checkLaw(() => ({
      lhs: (s: Observable<string>) => s.pipe(defaultIfEmpty("d"), defaultIfEmpty("e")),
      rhs: defaultIfEmpty<string, string>("d"),
    }));
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L12 first() = take(1) ∘ throwIfEmpty(), EmptyError included", () => {
    const r = checkLaw(() => ({ lhs: first<string>(), rhs: (s: Observable<string>) => s.pipe(take(1), throwIfEmpty()) }));
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L13 startWith(a) ∘ startWith(b) = startWith(b, a)", () => {
    const r = checkLaw(() => ({ lhs: (s: Observable<string>) => s.pipe(startWith("x"), startWith("y")), rhs: startWith("y", "x") }));
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L15 tap(noop) is the identity on the notification trace", () => {
    const r = checkLaw(() => ({ lhs: tap<string>(() => undefined), rhs: (s: Observable<string>) => s }));
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L16 materialize ∘ dematerialize is the identity, errors included", () => {
    const r = checkLaw(() => ({ lhs: (s: Observable<string>) => s.pipe(materialize(), dematerialize()), rhs: (s: Observable<string>) => s }));
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L14 mergeMap(f, 1) = concatMap(f), with random inners", () => {
    const r = checkLaw((rand, cold) => {
      const inner = Object.fromEntries("abcde".split("").map((c) => [c, cold(randomMarbles(rand))]));
      const f = (x: string) => inner[x];
      return { lhs: mergeMap(f, 1), rhs: concatMap(f) };
    });
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L9 concatMap(f) = map(f) ∘ concatAll() on the notification trace, with random inners", () => {
    const r = checkLaw((rand, cold) => {
      const inner = Object.fromEntries("abcde".split("").map((c) => [c, cold(randomMarbles(rand))]));
      const f = (x: string) => inner[x];
      return { lhs: concatMap(f), rhs: (s: Observable<string>) => s.pipe(map(f), concatAll()) };
    });
    expect(r).toEqual({ holds: true, checked: 200 });
  });

  it("L10 switchMap(f) = map(f) ∘ switchAll() on the notification trace, with random inners", () => {
    const r = checkLaw((rand, cold) => {
      const inner = Object.fromEntries("abcde".split("").map((c) => [c, cold(randomMarbles(rand))]));
      const f = (x: string) => inner[x];
      return { lhs: switchMap(f), rhs: (s: Observable<string>) => s.pipe(map(f), switchAll()) };
    });
    expect(r).toEqual({ holds: true, checked: 200 });
  });
});

describe("equivalence: laws with a side condition, and laws that fail", () => {
  it("L8 scan(f, s) ∘ last() = reduce(f, s) on sources with at least one value, and not on a source that completes empty", () => {
    const f = (acc: string, x: string) => acc + x;
    const make = () => ({
      lhs: (s: Observable<string>) => s.pipe(scan(f, ""), last()),
      rhs: reduce(f, ""),
    });
    expect(checkLaw(make, { only: hasValue })).toMatchObject({ holds: true });
    const broken = checkLaw(make, { only: completesEmpty, runs: 400 });
    expect(broken).toMatchObject({ holds: false });
    if (!broken.holds) {
      // scan ∘ last raises EmptyError; reduce emits the seed.
      expect(broken.left.map((n) => n.k + (n.e ?? ""))).toEqual(["EEmptyError"]);
      expect(broken.right.map((n) => n.k + (n.v ?? ""))).toEqual(["N", "C"]);
    }
  });

  it("L4' filter fusion fails for index-aware predicates: the second filter re-indexes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const p = (x: string) => x !== "a";
      // Left: the first value that passes p, because the second filter sees it at index 0.
      expectObservable(cold("abc|").pipe(filter(p), filter((_, i) => i === 0))).toBe("-b-|");
      // Right: the value at source index 0 must pass p, and it does not.
      expectObservable(cold("abc|").pipe(filter((x, i) => p(x) && i === 0))).toBe("---|");
    });
  });

  it("L11 take and skip do not commute", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("abcd|").pipe(take(3), skip(1))).toBe("-b(c|)");
      expectObservable(cold("abcd|").pipe(skip(1), take(3))).toBe("-bc(d|)");
    });
    const r = checkLaw(() => ({ lhs: (s: Observable<string>) => s.pipe(take(3), skip(1)), rhs: (s: Observable<string>) => s.pipe(skip(1), take(3)) }));
    expect(r).toMatchObject({ holds: false });
  });
});

describe("equivalence has levels: equal traces, different actions", () => {
  it("L9' concatMap defers project to dequeue; map ∘ concatAll calls it at arrival. Same trace, different call frames", () => {
    const framesConcatMap: number[] = [];
    const framesMapConcatAll: number[] = [];
    const run = (frames: number[], build: (f: (x: string) => Observable<string>) => OperatorFunction<string, string>) => {
      const ts = scheduler();
      ts.run(({ cold, expectObservable }) => {
        const inner = { a: cold("a-a|"), b: cold("b-b|") };
        const f = (x: string) => {
          frames.push(ts.frame);
          return inner[x as "a" | "b"];
        };
        expectObservable(cold("ab|").pipe(build(f))).toBe("a-ab-b|");
      });
    };
    run(framesConcatMap, (f) => concatMap(f));
    run(framesMapConcatAll, (f) => (s) => s.pipe(map(f), concatAll()));
    expect(framesConcatMap).toEqual([0, 3]);
    expect(framesMapConcatAll).toEqual([0, 1]);
  });

  it("L10' switchMap cancels the previous inner before calling project; map ∘ switchAll calls project first. Same trace, different order", () => {
    const order = (build: (f: (x: string) => Observable<string>) => OperatorFunction<string, string>) => {
      let aClosedWhenBProjected: boolean | undefined;
      scheduler().run(({ cold, expectObservable }) => {
        const inner = { a: cold("a-a|"), b: cold("b-b|") };
        const f = (x: string) => {
          if (x === "b") aClosedWhenBProjected = inner.a.subscriptions[0].unsubscribedFrame !== Infinity;
          return inner[x as "a" | "b"];
        };
        expectObservable(cold("ab|").pipe(build(f))).toBe("ab-b|");
      });
      return aClosedWhenBProjected;
    };
    expect(order((f) => switchMap(f))).toBe(true);
    expect(order((f) => (s) => s.pipe(map(f), switchAll()))).toBe(false);
  });
});
