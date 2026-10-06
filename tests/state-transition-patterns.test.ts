/**
 * Operational tests for the 15 state transition patterns (ST-01 to ST-15).
 *
 * Each test is a discriminating experiment: an observable property that holds
 * when the pattern is present, shown on an operator that has it and on a
 * neighbouring operator that does not. Time is virtual (TestScheduler.run), so
 * one marble character is one frame. See docs/08-state-transition-pattern-tests.md.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import {
  Observable,
  buffer,
  bufferCount,
  combineLatest,
  concatMap,
  debounceTime,
  delayWhen,
  distinct,
  distinctUntilChanged,
  exhaustMap,
  filter,
  map,
  mergeMap,
  pairwise,
  retry,
  sample,
  scan,
  skip,
  skipUntil,
  switchMap,
  take,
  tap,
  timer,
  timestamp,
  zip,
} from "rxjs";

type Note<T> = { frame: number; kind: "N" | "E" | "C"; value?: T };

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

/** Subscribe inside run() and collect every notification with its virtual frame. */
function record<T>(ts: TestScheduler, source: Observable<T>): Note<T>[] {
  const notes: Note<T>[] = [];
  source.subscribe({
    next: (value) => notes.push({ frame: ts.frame, kind: "N", value }),
    error: () => notes.push({ frame: ts.frame, kind: "E" }),
    complete: () => notes.push({ frame: ts.frame, kind: "C" }),
  });
  return notes;
}

const values = <T>(notes: Note<T>[]): T[] =>
  notes.filter((n) => n.kind === "N").map((n) => n.value as T);

const sum = scan((acc: number, x: number) => acc + x, 0);

describe("ST-01 Identity", () => {
  it("the response to an event does not depend on what came before: tap passes, distinctUntilChanged fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Same final event `a` after two different prefixes. Identity answers identically both times.
      expectObservable(cold("a-a|").pipe(tap(() => undefined))).toBe("a-a|");
      expectObservable(cold("b-a|").pipe(tap(() => undefined))).toBe("b-a|");
      // Neighbour, Replace Latest: the prefix is remembered, so the second `a` is answered differently.
      expectObservable(cold("a-a|").pipe(distinctUntilChanged())).toBe("a--|");
      expectObservable(cold("b-a|").pipe(distinctUntilChanged())).toBe("b-a|");
    });
  });
});

describe("ST-02 Replace Latest", () => {
  it("after a blocked period only the newest value emerges, once; older values are gone: sample passes, buffer fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const source = cold("ab------|");
      const ticks = cold("---n--n-|");
      // `a` was overwritten by `b`. The second tick finds the slot empty and produces nothing.
      expectObservable(source.pipe(sample(ticks))).toBe("---b----|");
      // Neighbour, Buffer / Collect: `a` survives, and an empty flush still yields a value.
      expectObservable(source.pipe(buffer(ticks))).toBe("---x--y-(y|)", { x: ["a", "b"], y: [] });
    });
  });
});

describe("ST-03 Accumulate / Fold", () => {
  it("every earlier input still influences the latest output: scan passes, pairwise fails", () => {
    const ts = scheduler();
    let foldA: Note<number>[] = [];
    let foldB: Note<number>[] = [];
    let shiftA: Note<[number, number]>[] = [];
    let shiftB: Note<[number, number]>[] = [];
    ts.run(({ cold }) => {
      // Only the first input differs between the two runs.
      foldA = record(ts, cold("abc|", { a: 1, b: 2, c: 3 }).pipe(sum));
      foldB = record(ts, cold("abc|", { a: 10, b: 2, c: 3 }).pipe(sum));
      shiftA = record(ts, cold("abc|", { a: 1, b: 2, c: 3 }).pipe(pairwise()));
      shiftB = record(ts, cold("abc|", { a: 10, b: 2, c: 3 }).pipe(pairwise()));
    });
    expect(values(foldA)).toEqual([1, 3, 6]);
    expect(values(foldB)).toEqual([10, 12, 15]);
    // The fold carries the first input all the way to the last output.
    expect(values(foldA).at(-1)).not.toEqual(values(foldB).at(-1));
    // Neighbour, History / Shift: two steps later the first input is forgotten.
    expect(values(shiftA).at(-1)).toEqual(values(shiftB).at(-1));
  });
});

describe("ST-04 Count / Index", () => {
  it("identical values are told apart by position alone: filter by index passes, tap sees no index", () => {
    const arity: number[] = [];
    scheduler().run(({ cold, expectObservable }) => {
      // Four identical values. Only an ordinal can select every other one, or the first two.
      expectObservable(cold("aaaa|").pipe(filter((_, i) => i % 2 === 0))).toBe("a-a-|");
      expectObservable(cold("aaaa|").pipe(take(2))).toBe("a(a|)");
      // Neighbour, Identity: the probe shows tap's callback receives exactly one argument.
      cold("aaaa|")
        .pipe(tap((...args: unknown[]) => arity.push(args.length)))
        .subscribe();
    });
    expect(arity).toEqual([1, 1, 1, 1]);
  });
});

describe("ST-05 Phase / Flag", () => {
  it("the boundary follows an external event, not a count: skipUntil passes, skip fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const gate = cold("---n|");
      // Two sources with different densities. The phase flips at frame 3 in both.
      expectObservable(cold("a-a-a-a|").pipe(skipUntil(gate))).toBe("----a-a|");
      expectObservable(cold("aaa-a--|").pipe(skipUntil(gate))).toBe("----a--|");
      // Neighbour, Count / Index: the boundary moves with the values, not with time.
      expectObservable(cold("a-a-a-a|").pipe(skip(2))).toBe("----a-a|");
      expectObservable(cold("aaa-a--|").pipe(skip(2))).toBe("--a-a--|");
    });
  });
});

describe("ST-06 History / Shift", () => {
  it("only the last k inputs influence an output: pairwise passes, scan fails", () => {
    const ts = scheduler();
    let shiftA: Note<[string, string]>[] = [];
    let shiftB: Note<[string, string]>[] = [];
    let foldA: Note<number>[] = [];
    let foldB: Note<number>[] = [];
    ts.run(({ cold }) => {
      shiftA = record(ts, cold("abcd|").pipe(pairwise()));
      shiftB = record(ts, cold("zbcd|").pipe(pairwise()));
      foldA = record(ts, cold("abcd|", { a: 1, b: 2, c: 3, d: 4 }).pipe(sum));
      foldB = record(ts, cold("zbcd|", { z: 9, b: 2, c: 3, d: 4 }).pipe(sum));
    });
    expect(values(shiftA)).toEqual([["a", "b"], ["b", "c"], ["c", "d"]]);
    expect(values(shiftB)).toEqual([["z", "b"], ["b", "c"], ["c", "d"]]);
    // Beyond k = 2 the changed input has left the register.
    expect(values(shiftA).slice(1)).toEqual(values(shiftB).slice(1));
    // Neighbour, Accumulate / Fold: the change never leaves.
    expect(values(foldA).at(-1)).not.toEqual(values(foldB).at(-1));
  });
});

describe("ST-07 Membership Set", () => {
  it("a key seen at any distance is suppressed, and the set does not forget: distinct passes, distinctUntilChanged fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("abcab|").pipe(distinct())).toBe("abc--|");
      // Neighbour, Replace Latest: one slot remembers only the previous value.
      expectObservable(cold("abcab|").pipe(distinctUntilChanged())).toBe("abcab|");
    });
  });
});

describe("ST-08 Buffer / Collect", () => {
  it("every retained value is released together as one collection: buffer passes, sample fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const source = cold("abc---|");
      const tick = cold("----n|");
      expectObservable(source.pipe(buffer(tick))).toBe("----x-(y|)", { x: ["a", "b", "c"], y: [] });
      // Neighbour, Replace Latest: only the newest value is retained.
      expectObservable(source.pipe(sample(tick))).toBe("----c-|");
    });
  });
});

describe("ST-09 FIFO Queue", () => {
  it("release order equals arrival order even when release conditions resolve out of order: concatMap passes, delayWhen fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const duration: Record<string, number> = { a: 5, b: 1 };
      const source = cold("ab|");
      // `b` is ready long before `a`, yet it waits its turn.
      expectObservable(source.pipe(concatMap((x) => timer(duration[x]).pipe(map(() => x))))).toBe(
        "-----a(b|)"
      );
      // Neighbour, Active Registry: each value is released on its own clock, so `b` overtakes `a`.
      expectObservable(source.pipe(delayWhen((x) => timer(duration[x])))).toBe("--b--(a|)");
    });
  });
});

describe("ST-10 Active Registry", () => {
  it("several inners are alive at the same time: mergeMap passes, concatMap fails", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      const pick = (x: string) => inner[x as keyof typeof inner];
      expectObservable(cold("ab|").pipe(mergeMap(pick))).toBe("abab|");
      // Both inner subscriptions overlap during frames 1 to 3.
      expectSubscriptions(inner.a.subscriptions).toBe("^--!");
      expectSubscriptions(inner.b.subscriptions).toBe("-^--!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      const pick = (x: string) => inner[x as keyof typeof inner];
      // Neighbour, FIFO Queue: the second inner starts only after the first ends.
      expectObservable(cold("ab|").pipe(concatMap(pick))).toBe("a-ab-b|");
      expectSubscriptions(inner.a.subscriptions).toBe("^--!");
      expectSubscriptions(inner.b.subscriptions).toBe("---^--!");
    });
  });
});

describe("ST-11 Active Selection", () => {
  it("a new candidate replaces the current one and tears it down: switchMap passes, exhaustMap fails", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      const pick = (x: string) => inner[x as keyof typeof inner];
      expectObservable(cold("ab|").pipe(switchMap(pick))).toBe("ab-b|");
      // The first inner is unsubscribed at frame 1, the moment the second is selected.
      expectSubscriptions(inner.a.subscriptions).toBe("^!");
      expectSubscriptions(inner.b.subscriptions).toBe("-^--!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      const pick = (x: string) => inner[x as keyof typeof inner];
      // Neighbour, Phase / Flag (busy/idle): the candidate is refused, nothing is replaced.
      expectObservable(cold("ab|").pipe(exhaustMap(pick))).toBe("a-a|");
      expectSubscriptions(inner.a.subscriptions).toBe("^--!");
      expectSubscriptions(inner.b.subscriptions).toBe([]);
    });
  });
});

describe("ST-12 Slot Table + Readiness", () => {
  it("nothing until every slot is filled, then the latest value per slot: combineLatest passes, zip fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const s1 = cold("a-b--|");
      const s2 = cold("---x-|");
      // Readiness at frame 3. Slot one holds `b`; `a` was overwritten, not queued.
      expectObservable(combineLatest([s1, s2])).toBe("---p-|", { p: ["b", "x"] });
      // Neighbour, FIFO Queue per source: the head of the queue is `a`.
      expectObservable(zip([s1, s2])).toBe("---q-|", { q: ["a", "x"] });
    });
  });
});

describe("ST-13 Timer / Deadline", () => {
  it("the clock alone produces an action, and input restarts the deadline: debounceTime passes, timestamp fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // An emission at frame 3 with no source event at frame 3.
      expectObservable(cold("a-----|").pipe(debounceTime(3))).toBe("---a--|");
      // The second `a` restarts the timer, so one emission at frame 5.
      expectObservable(cold("a-a----|").pipe(debounceTime(3))).toBe("-----a-|");
    });
    const ts = scheduler();
    ts.run(({ cold, expectObservable }) => {
      // Neighbour, clock read: output only at input frames. Advancing time alone does nothing.
      expectObservable(cold("a-----|").pipe(timestamp(ts))).toBe("x-----|", {
        x: { value: "a", timestamp: 0 },
      });
    });
  });
});

describe("ST-14 Window / Session", () => {
  it("membership follows open/close boundaries, not value count: buffer passes, bufferCount fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const source = cold("a-b-c-d-|");
      // Same values, two different boundary streams, two different groupings.
      expectObservable(source.pipe(buffer(cold("---n---n|")))).toBe("---x---y(z|)", {
        x: ["a", "b"],
        y: ["c", "d"],
        z: [],
      });
      expectObservable(source.pipe(buffer(cold("-n---n--|")))).toBe("-u---v--(w|)", {
        u: ["a"],
        v: ["b", "c"],
        w: ["d"],
      });
      // Neighbour, Count / Index: grouping is fixed by count, boundaries do not exist.
      expectObservable(source.pipe(bufferCount(2))).toBe("--x---y-|", { x: ["a", "b"], y: ["c", "d"] });
    });
  });
});

describe("ST-15 Lifecycle / Terminal", () => {
  it("a terminal state is absorbing and tears down the source; retry postpones it: take and retry", () => {
    scheduler().run(({ cold, hot, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c-d|");
      // Complete at frame 2 and the source is unsubscribed at frame 2. `c` and `d` never happen.
      expectObservable(source.pipe(take(2))).toBe("a-(b|)");
      expectSubscriptions(source.subscriptions).toBe("^-!");
      // A hot source keeps emitting, but nothing reaches a completed subscriber.
      const live = hot("a-b-c|");
      expectObservable(live.pipe(take(1))).toBe("(a|)");
      expectSubscriptions(live.subscriptions).toBe("(^!)");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      // The source reaches its terminal state twice. The operator reaches its own only once the count is spent.
      const flaky = cold("a-#");
      expectObservable(flaky.pipe(retry(1))).toBe("a-a-#");
      expectSubscriptions(flaky.subscriptions).toBe(["^-!", "--^-!"]);
    });
  });
});
