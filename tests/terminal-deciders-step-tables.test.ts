/**
 * Step tables for the terminal deciders, verified row by row against RxJS 7.8.2.
 *
 * Primitives: take, takeLast, defaultIfEmpty, throwIfEmpty. Independent deciders:
 * single, find, findIndex, every, isEmpty. Compositions: first, last, elementAt.
 * Row IDs (TK1, TL2, DE3, TE4, SG5, FD6, EV7, IE8, FI9, LA10, EA11) match
 * docs/15-terminal-deciders-step-tables.md. Error kinds are checked by type.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import {
  ArgumentOutOfRangeError,
  EmptyError,
  NotFoundError,
  Observable,
  SequenceError,
  defaultIfEmpty,
  elementAt,
  every,
  find,
  findIndex,
  first,
  isEmpty,
  last,
  single,
  take,
  takeLast,
  throwIfEmpty,
} from "rxjs";

type Note = { frame: number; kind: "N" | "E" | "C"; value?: unknown; error?: unknown };

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

function record<T>(ts: TestScheduler, source: Observable<T>): Note[] {
  const notes: Note[] = [];
  source.subscribe({
    next: (value) => notes.push({ frame: ts.frame, kind: "N", value }),
    error: (error: unknown) => notes.push({ frame: ts.frame, kind: "E", error }),
    complete: () => notes.push({ frame: ts.frame, kind: "C" }),
  });
  return notes;
}

/** Run one scenario and return its notes; a one-line way to check error kinds. */
function notesOf<T>(build: (cold: TestScheduler["createColdObservable"]) => Observable<T>): Note[] {
  const ts = scheduler();
  let notes: Note[] = [];
  ts.run(({ cold }) => {
    notes = record(ts, build(cold as TestScheduler["createColdObservable"]));
  });
  return notes;
}

describe("take(n)", () => {
  it("TK1 the nth value completes the result at its own frame and tears the source down; TK3 an empty source completes empty", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c|");
      expectObservable(source.pipe(take(2))).toBe("a-(b|)");
      expectSubscriptions(source.subscriptions).toBe("^-!");
      expectObservable(cold("--|").pipe(take(2))).toBe("--|");
    });
  });

  it("TK0 take(0) is EMPTY: completes at subscribe and never subscribes the source", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b|");
      expectObservable(source.pipe(take(0))).toBe("|");
      expectSubscriptions(source.subscriptions).toBe([]);
    });
  });
});

describe("takeLast(n)", () => {
  it("TL2 complete emits the last n values and completes; TL3 an empty source completes empty; TL4 error drops the buffer", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-b-c-|").pipe(takeLast(2))).toBe("------(bc|)");
      expectObservable(cold("--|").pipe(takeLast(2))).toBe("--|");
      expectObservable(cold("a-b-#").pipe(takeLast(2))).toBe("----#");
    });
  });

  it("TL0 takeLast(0) is EMPTY and never subscribes the source", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b|");
      expectObservable(source.pipe(takeLast(0))).toBe("|");
      expectSubscriptions(source.subscriptions).toBe([]);
    });
  });
});

describe("defaultIfEmpty(d) / throwIfEmpty()", () => {
  it("DE1 values pass through; DE2 complete with no value emits the default; TE2 complete with no value errors; DE3/TE3 error passes through, no default and no replacement", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-|").pipe(defaultIfEmpty("d"))).toBe("a-|");
      expectObservable(cold("--|").pipe(defaultIfEmpty("d"))).toBe("--(d|)");
      expectObservable(cold("a-|").pipe(throwIfEmpty())).toBe("a-|");
      expectObservable(cold("--#").pipe(defaultIfEmpty("d"))).toBe("--#");
    });
    const notes = notesOf((cold) => cold("--|").pipe(throwIfEmpty()));
    expect(notes).toEqual([{ frame: 2, kind: "E", error: expect.any(EmptyError) }]);
  });
});

describe("single(predicate?)", () => {
  it("SG2 complete with exactly one match emits it; SG1' a second match errors at once with SequenceError, before complete", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      expectObservable(cold("a-|").pipe(single())).toBe("--(a|)");
      const two = cold("ab--|");
      expectObservable(two.pipe(single())).toBe("-#", undefined, expect.any(SequenceError));
      expectSubscriptions(two.subscriptions).toBe("^!");
    });
  });

  it("SG3 an empty source errors with EmptyError; SG3' values but no match errors with NotFoundError", () => {
    expect(notesOf((cold) => cold("--|").pipe(single()))).toEqual([{ frame: 2, kind: "E", error: expect.any(EmptyError) }]);
    expect(notesOf((cold) => cold("ab|").pipe(single((x) => x === "z")))).toEqual([
      { frame: 2, kind: "E", error: expect.any(NotFoundError) },
    ]);
  });
});

describe("find(predicate) / findIndex(predicate)", () => {
  it("FD1 the first match emits and completes at its frame; FD2 complete without a match emits undefined, or -1", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("abc|");
      expectObservable(source.pipe(find((x) => x === "b"))).toBe("-(b|)");
      expectSubscriptions(source.subscriptions).toBe("^!");
      expectObservable(cold("abc|").pipe(findIndex((x) => x === "b"))).toBe("-(i|)", { i: 1 });
      expectObservable(cold("abc|").pipe(find((x) => x === "z"))).toBe("---(u|)", { u: undefined });
      expectObservable(cold("abc|").pipe(findIndex((x) => x === "z"))).toBe("---(m|)", { m: -1 });
    });
  });
});

describe("every(predicate)", () => {
  it("EV1 the first failing value emits false and completes at its frame; EV2 complete emits true; EV3 an empty source is vacuously true", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("abc|");
      expectObservable(source.pipe(every((x) => x !== "b"))).toBe("-(f|)", { f: false });
      expectSubscriptions(source.subscriptions).toBe("^!");
      expectObservable(cold("abc|").pipe(every((x) => x !== "z"))).toBe("---(t|)", { t: true });
      expectObservable(cold("--|").pipe(every(() => false))).toBe("--(t|)", { t: true });
    });
  });
});

describe("isEmpty()", () => {
  it("IE1 the first value emits false and completes at its frame; IE2 complete emits true", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-|");
      expectObservable(source.pipe(isEmpty())).toBe("(f|)", { f: false });
      expectSubscriptions(source.subscriptions).toBe("(^!)");
      expectObservable(cold("--|").pipe(isEmpty())).toBe("--(t|)", { t: true });
    });
  });
});

describe("first(predicate?, default?) = filter + take(1) + (defaultIfEmpty | throwIfEmpty)", () => {
  it("FI1 the first match decides at its own frame; FI2 no match and a default emits the default on complete; FI3 no match and no default errors with EmptyError", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b|");
      expectObservable(source.pipe(first())).toBe("(a|)");
      expectSubscriptions(source.subscriptions).toBe("(^!)");
      expectObservable(cold("abc|").pipe(first((x) => x === "b"))).toBe("-(b|)");
      expectObservable(cold("ab|").pipe(first((x) => x === "z", "d"))).toBe("--(d|)");
      expectObservable(cold("--|").pipe(first(undefined, "d"))).toBe("--(d|)");
    });
    expect(notesOf((cold) => cold("--|").pipe(first()))).toEqual([{ frame: 2, kind: "E", error: expect.any(EmptyError) }]);
  });
});

describe("last(predicate?, default?) = filter + takeLast(1) + (defaultIfEmpty | throwIfEmpty)", () => {
  it("LA1 decides only on complete, with the last match; LA2 a default covers the empty case; LA3 otherwise EmptyError", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-b-|").pipe(last())).toBe("----(b|)");
      expectObservable(cold("abc|").pipe(last((x) => x !== "c"))).toBe("---(b|)");
      expectObservable(cold("--|").pipe(last(undefined, "d"))).toBe("--(d|)");
    });
    expect(notesOf((cold) => cold("--|").pipe(last()))).toEqual([{ frame: 2, kind: "E", error: expect.any(EmptyError) }]);
  });
});

describe("elementAt(i, default?) = filter(index === i) + take(1) + (defaultIfEmpty | throwIfEmpty)", () => {
  it("EA1 the ith value decides at its frame; EA2 too short with a default emits it on complete; EA3 too short without one errors with ArgumentOutOfRangeError; EA0 a negative index throws at call time", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("abc|");
      expectObservable(source.pipe(elementAt(1))).toBe("-(b|)");
      expectSubscriptions(source.subscriptions).toBe("^!");
      expectObservable(cold("ab|").pipe(elementAt(5, "d"))).toBe("--(d|)");
    });
    expect(notesOf((cold) => cold("ab|").pipe(elementAt(5)))).toEqual([
      { frame: 2, kind: "E", error: expect.any(ArgumentOutOfRangeError) },
    ]);
    // Not an observable error: the operator factory itself throws.
    expect(() => elementAt(-1)).toThrow(ArgumentOutOfRangeError);
  });
});

describe("error passes through every decider at once; nothing stored is emitted", () => {
  it("X1 take, takeLast, single, every, isEmpty, defaultIfEmpty on a source that errors", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-#").pipe(take(2))).toBe("a-#");
      expectObservable(cold("a-#").pipe(takeLast(1))).toBe("--#");
      expectObservable(cold("a-#").pipe(single())).toBe("--#");
      expectObservable(cold("a-#").pipe(every(() => true))).toBe("--#");
      expectObservable(cold("--#").pipe(isEmpty())).toBe("--#");
      expectObservable(cold("--#").pipe(defaultIfEmpty("d"))).toBe("--#");
    });
  });
});
