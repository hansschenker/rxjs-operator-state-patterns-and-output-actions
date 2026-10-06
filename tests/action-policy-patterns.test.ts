/**
 * Operational tests for the 15 action policy patterns (AP-01 to AP-15).
 *
 * Each test is a discriminating experiment on the action side of an operator:
 * what is emitted, at which virtual frame, which subscriptions are opened or
 * cancelled, and which side effects run. One operator that carries the policy
 * passes; a neighbouring policy's operator fails the same property.
 * See docs/09-action-policy-pattern-tests.md.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import {
  Observable,
  TimeoutError,
  bufferCount,
  concat,
  concatMap,
  debounceTime,
  defaultIfEmpty,
  delay,
  filter,
  groupBy,
  last,
  map,
  merge,
  mergeMap,
  race,
  reduce,
  sample,
  scan,
  shareReplay,
  startWith,
  switchMap,
  take,
  takeWhile,
  tap,
  timeout,
  toArray,
  windowCount,
  withLatestFrom,
} from "rxjs";

type Note<T> = { frame: number; kind: "N" | "E" | "C"; value?: T; error?: unknown };

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

/** Subscribe inside run() and collect every notification with its virtual frame. */
function record<T>(ts: TestScheduler, source: Observable<T>): Note<T>[] {
  const notes: Note<T>[] = [];
  source.subscribe({
    next: (value) => notes.push({ frame: ts.frame, kind: "N", value }),
    error: (error: unknown) => notes.push({ frame: ts.frame, kind: "E", error }),
    complete: () => notes.push({ frame: ts.frame, kind: "C" }),
  });
  return notes;
}

const values = <T>(notes: Note<T>[]): T[] =>
  notes.filter((n) => n.kind === "N").map((n) => n.value as T);

describe("AP-01 Forward", () => {
  it("every notification leaves with the same value, kind and frame it arrived with: tap passes, map fails", () => {
    let seen = 0;
    scheduler().run(({ cold, expectObservable }) => {
      // next, next, error: all three pass through untouched. The side effect ran, the stream did not change.
      expectObservable(cold("a-b-#").pipe(tap(() => seen++))).toBe("a-b-#");
      // Neighbour, Transform: same frames, different values.
      expectObservable(cold("a-b-#").pipe(map((x) => x.toUpperCase()))).toBe("x-y-#", { x: "A", y: "B" });
    });
    expect(seen).toBe(2);
  });
});

describe("AP-02 Transform", () => {
  it("the output is a function of the current value and index only: map passes, scan fails", () => {
    const ts = scheduler();
    let mapA: Note<string>[] = [];
    let mapB: Note<string>[] = [];
    let foldA: Note<string>[] = [];
    let foldB: Note<string>[] = [];
    ts.run(({ cold }) => {
      // Same second input `b` at index 1, two different first inputs.
      mapA = record(ts, cold("ab|").pipe(map((x, i) => x + i)));
      mapB = record(ts, cold("cb|").pipe(map((x, i) => x + i)));
      foldA = record(ts, cold("ab|").pipe(scan((acc, x) => acc + x, "")));
      foldB = record(ts, cold("cb|").pipe(scan((acc, x) => acc + x, "")));
    });
    expect(values(mapA)).toEqual(["a0", "b1"]);
    expect(values(mapB)).toEqual(["c0", "b1"]);
    // Transform answers (b, 1) identically whatever came before.
    expect(values(mapA)[1]).toEqual(values(mapB)[1]);
    // Neighbour, Emit Derived State: the answer to `b` carries the history.
    expect(values(foldA)[1]).not.toEqual(values(foldB)[1]);
  });
});

describe("AP-03 Gate / Suppress", () => {
  it("each input is forwarded unchanged or dropped, and the gate reopens: filter passes, takeWhile fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Both `b`s are dropped. `c`, arriving between them, still passes: the gate is per event.
      expectObservable(cold("abcb|").pipe(filter((x) => x !== "b"))).toBe("a-c-|");
      // Neighbour, Terminate: the first failing value ends the stream; `c` never passes.
      expectObservable(cold("abcb|").pipe(takeWhile((x) => x !== "b"))).toBe("a|");
    });
  });
});

describe("AP-04 Synthesize / Inject", () => {
  it("a value appears that no input event caused: defaultIfEmpty and startWith pass, map fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // An empty source. The value `d` has no input behind it.
      expectObservable(cold("---|").pipe(defaultIfEmpty("d"))).toBe("---(d|)");
      // `s` is emitted at subscribe, one frame before the first input exists.
      expectObservable(cold("-a|").pipe(startWith("s"))).toBe("sa|");
      // Neighbour, Transform: with no input there is nothing to transform.
      expectObservable(cold("---|").pipe(map(() => "d"))).toBe("---|");
    });
  });
});

describe("AP-05 Emit Derived State", () => {
  it("the emitted value is the reducer's new state, not an input: scan and reduce pass, last fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const nums = { a: 1, b: 2, c: 3 };
      // None of 11, 13, 16 was ever an input. Each is s' at the frame it was computed.
      expectObservable(cold("abc|", nums).pipe(scan((acc, x) => acc + x, 10))).toBe("xyz|", { x: 11, y: 13, z: 16 });
      // Same state, same policy, emitted once on complete.
      expectObservable(cold("abc|", nums).pipe(reduce((acc, x) => acc + x, 10))).toBe("---(z|)", { z: 16 });
      // Neighbour, Read Stored State: what comes out is an input that was stored, not a derived value.
      expectObservable(cold("abc|", nums).pipe(last())).toBe("---(c|)", { c: 3 });
    });
  });
});

describe("AP-06 Read Stored State", () => {
  it("a value captured earlier is emitted on a later event: last and shareReplay pass, tap fails", () => {
    scheduler().run(({ cold, hot, expectObservable }) => {
      // `b` arrived at frame 1 and leaves at frame 4, when complete arrives.
      expectObservable(cold("ab--|").pipe(last())).toBe("----(b|)");
      // A late subscriber at frame 3 is handed the stored `b` immediately.
      const shared = hot("ab---|").pipe(shareReplay(1));
      expectObservable(shared).toBe("ab---|");
      expectObservable(shared, "---^").toBe("---b-|");
      // Neighbour, Forward: `b` leaves at frame 1, the frame it arrived. Nothing is read back later.
      expectObservable(cold("ab--|").pipe(tap(() => undefined))).toBe("ab--|");
    });
  });
});

describe("AP-07 Combine / Join", () => {
  it("one output is derived from inputs that arrived on different sources: withLatestFrom passes, merge fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const primary = cold("--a-b|");
      const secondary = cold("x----|");
      expectObservable(primary.pipe(withLatestFrom(secondary))).toBe("--p-q|", { p: ["a", "x"], q: ["b", "x"] });
      // Neighbour, Merge / Interleave: every input is forwarded on its own, nothing is joined.
      expectObservable(merge(primary, secondary)).toBe("x-a-b|");
    });
  });
});

describe("AP-08 Batch / Aggregate", () => {
  it("many inputs leave as one collection, emitted when the batch closes: bufferCount passes, windowCount fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Two values in, one array out, at the frame of the second value.
      expectObservable(cold("abcd|").pipe(bufferCount(2))).toBe("-x-y|", { x: ["a", "b"], y: ["c", "d"] });
      // Neighbour, Route / Group: a destination is emitted when it opens, before any value is in it,
      // and the values themselves never appear on the main output.
      expectObservable(cold("abcd|").pipe(windowCount(2), map(() => "w"))).toBe("ww-w|");
    });
  });
});

describe("AP-09 Route / Group", () => {
  it("every value reaches a destination chosen by key, unchanged: groupBy passes, filter fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const byKey = groupBy((x: string) => x);
      // One destination per distinct key, emitted when the key is first seen.
      expectObservable(cold("abab|").pipe(byKey, map((g) => g.key))).toBe("ab--|");
      // Each destination holds exactly the values with its key.
      expectObservable(
        cold("abab|").pipe(
          byKey,
          mergeMap((g) => g.pipe(toArray(), map((arr) => arr.join(""))))
        )
      ).toBe("----(xy|)", { x: "aa", y: "bb" });
      // Neighbour, Gate / Suppress: the `b`s are dropped, not sent somewhere else.
      expectObservable(cold("abab|").pipe(filter((x) => x === "a"))).toBe("a-a-|");
    });
  });
});

describe("AP-10 Delay / Time-Shift", () => {
  it("every value survives with its order and spacing, shifted by a constant: delay passes, debounceTime fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // `a` and `b` both leave, one frame apart as they arrived, three frames late.
      expectObservable(cold("ab--|").pipe(delay(3))).toBe("---a(b|)");
      // Neighbour, Trigger / Sample: also later, but `a` is gone and the spacing with it.
      expectObservable(cold("ab--|").pipe(debounceTime(3))).toBe("----(b|)");
    });
  });
});

describe("AP-11 Trigger / Sample", () => {
  it("emission happens at the trigger's frame with the stored value; an empty trigger emits nothing: sample passes, delay fails", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const source = cold("a-b----|");
      const ticks = cold("---n--n|");
      // Frame 3 is a tick frame, not an input frame. Frame 6 finds nothing stored.
      expectObservable(source.pipe(sample(ticks))).toBe("---b---|");
      // Neighbour, Delay / Time-Shift: frames follow the inputs plus three, and every value leaves.
      expectObservable(source.pipe(delay(3))).toBe("---a-b-|");
    });
  });
});

describe("AP-12 Merge / Interleave", () => {
  it("values from every active producer leave as they arrive: merge passes, concat fails", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("a-c|");
      const s2 = cold("-b-d|");
      expectObservable(merge(s1, s2)).toBe("abcd|");
      // Both producers are subscribed at frame 0.
      expectSubscriptions(s1.subscriptions).toBe("^--!");
      expectSubscriptions(s2.subscriptions).toBe("^---!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("a-c|");
      const s2 = cold("-b-d|");
      // Neighbour, Serialize: `b` would have been ready at frame 1 but waits until `s1` is done.
      expectObservable(concat(s1, s2)).toBe("a-c-b-d|");
      expectSubscriptions(s2.subscriptions).toBe("---^---!");
    });
  });
});

describe("AP-13 Serialize", () => {
  it("one producer at a time, in order, nothing lost: concatMap passes, mergeMap and switchMap fail", () => {
    const inners = (cold: (m: string) => Observable<string>) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      return { inner, pick: (x: string) => inner[x as keyof typeof inner] };
    };
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const { inner, pick } = inners(cold);
      // All four inner values, in producer order, no overlap.
      expectObservable(cold("ab|").pipe(concatMap(pick))).toBe("a-ab-b|");
      expectSubscriptions((inner.b as ReturnType<typeof cold>).subscriptions).toBe("---^--!");
    });
    scheduler().run(({ cold, expectObservable }) => {
      const { pick } = inners(cold);
      // Neighbour, Merge / Interleave: all four values, but overlapping.
      expectObservable(cold("ab|").pipe(mergeMap(pick))).toBe("abab|");
    });
    scheduler().run(({ cold, expectObservable }) => {
      const { pick } = inners(cold);
      // Neighbour, Current-Wins: in order, but the first producer's second value is lost.
      expectObservable(cold("ab|").pipe(switchMap(pick))).toBe("ab-b|");
    });
  });
});

describe("AP-14 Current-Wins", () => {
  it("only the selected producer is forwarded and the others are cancelled: race and switchMap pass, merge fails", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("-a-a|");
      const s2 = cold("b-b-b|");
      // First-Wins: `s2` emits first and is selected for good. `s1` is cancelled at frame 0.
      expectObservable(race(s1, s2)).toBe("b-b-b|");
      expectSubscriptions(s1.subscriptions).toBe("(^!)");
      expectSubscriptions(s2.subscriptions).toBe("^----!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const inner = { a: cold("a-a|"), b: cold("b-b|") };
      // Latest-wins: the new candidate is selected and the old producer is cancelled at that frame.
      expectObservable(cold("ab|").pipe(switchMap((x: string) => inner[x as keyof typeof inner]))).toBe("ab-b|");
      expectSubscriptions(inner.a.subscriptions).toBe("^!");
    });
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const s1 = cold("-a-a|");
      const s2 = cold("b-b-b|");
      // Neighbour, Merge / Interleave: no selection, no cancellation, both run to the end.
      expectObservable(merge(s1, s2)).toBe("babab|");
      expectSubscriptions(s1.subscriptions).toBe("^---!");
      expectSubscriptions(s2.subscriptions).toBe("^----!");
    });
  });
});

describe("AP-15 Terminate", () => {
  it("the operator ends the stream at a frame the source did not, and unsubscribes it: take and timeout pass, filter fails", () => {
    const ts = scheduler();
    let timedOut: Note<string>[] = [];
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      // Complete on count, at frame 0, with the source torn down at the same frame.
      const counted = cold("a-b|");
      expectObservable(counted.pipe(take(1))).toBe("(a|)");
      expectSubscriptions(counted.subscriptions).toBe("(^!)");
      // Error on the clock, at frame 2, where the source has no event at all.
      const slow = cold("a----b|");
      timedOut = record(ts, slow.pipe(timeout(2)));
      expectSubscriptions(slow.subscriptions).toBe("^-!");
      // Neighbour, Gate / Suppress: drops everything, yet the source runs to its own completion.
      const gated = cold("a----b|");
      expectObservable(gated.pipe(filter(() => false))).toBe("------|");
      expectSubscriptions(gated.subscriptions).toBe("^-----!");
    });
    expect(timedOut).toEqual([
      { frame: 0, kind: "N", value: "a" },
      { frame: 2, kind: "E", error: expect.any(TimeoutError) },
    ]);
  });
});
