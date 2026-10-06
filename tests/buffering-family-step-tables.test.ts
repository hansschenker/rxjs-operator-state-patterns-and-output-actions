/**
 * Step tables for the buffering and windowing family, verified row by row
 * against RxJS 7.8.2. Five pairs of twins: buffer/window, bufferCount/windowCount,
 * bufferTime/windowTime, bufferToggle/windowToggle, bufferWhen/windowWhen.
 * Row IDs (B1, C2, T3, G4, W5) match docs/14-buffering-family-step-tables.md.
 *
 * Buffers are observed with marbles. Windows are observed with `observeWindows`,
 * which subscribes to every window as it is emitted and records, per window,
 * each value, error, or completion with its frame: "0:a@1" is value a in
 * window 0 at frame 1, "0:C@2" is window 0 completing at frame 2, "1:E@3" is
 * window 1 erroring at frame 3. The outer stream is recorded as "W@f" for a
 * window emitted, "C@f" and "E@f" for its own terminal.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import {
  Observable,
  buffer,
  bufferCount,
  bufferTime,
  bufferToggle,
  bufferWhen,
  timer,
  window,
  windowCount,
  windowTime,
  windowToggle,
  windowWhen,
} from "rxjs";

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

function observeWindows(ts: TestScheduler, source: Observable<Observable<string>>) {
  const outer: string[] = [];
  const inner: string[] = [];
  let i = 0;
  source.subscribe({
    next: (w) => {
      const idx = i++;
      outer.push(`W@${ts.frame}`);
      w.subscribe({
        next: (v) => inner.push(`${idx}:${v}@${ts.frame}`),
        error: () => inner.push(`${idx}:E@${ts.frame}`),
        complete: () => inner.push(`${idx}:C@${ts.frame}`),
      });
    },
    error: () => outer.push(`E@${ts.frame}`),
    complete: () => outer.push(`C@${ts.frame}`),
  });
  return { outer: () => outer.join(" "), inner: () => inner.join(" ") };
}

describe("buffer(notifier) / window(notifier)", () => {
  it("B1 values join the open region; B2 a boundary closes it and opens the next; B3 the boundary completing is ignored; B5 source complete flushes the open region", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Boundary at 3, boundary completes at 6 (ignored), source completes at 7 with `c` still buffered.
      expectObservable(cold("ab--c--|").pipe(buffer(cold("---n--|")))).toBe("---x---(y|)", { x: ["a", "b"], y: ["c"] });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("ab--c--|").pipe(window(cold("---n--|"))));
    });
    expect(w.outer()).toBe("W@0 W@3 C@7");
    expect(w.inner()).toBe("0:a@0 0:b@1 0:C@3 1:c@4 1:C@7");
  });

  it("B2' a boundary with nothing collected emits an empty buffer", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("----a|").pipe(buffer(cold("-n|")))).toBe("-x---(y|)", { x: [], y: ["a"] });
    });
  });

  it("B4 the boundary erroring errors the result; the open window receives the error, the open buffer is dropped", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a----|").pipe(buffer(cold("--#")))).toBe("--#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a----|").pipe(window(cold("--#"))));
    });
    expect(w.outer()).toBe("W@0 E@2");
    expect(w.inner()).toBe("0:a@0 0:E@2");
  });

  it("B6 source error: the buffer is dropped, the window receives the error", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-#").pipe(buffer(cold("-----|")))).toBe("--#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a-#").pipe(window(cold("-----|"))));
    });
    expect(w.outer()).toBe("W@0 E@2");
    expect(w.inner()).toBe("0:a@0 0:E@2");
  });
});

describe("bufferCount(size, every) / windowCount(size, every)", () => {
  it("C1 a region opens at every multiple of `every` and closes at `size`; C2 complete flushes partial buffers; windowCount opens the next window eagerly, so it may complete an empty one", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("abcd|").pipe(bufferCount(2))).toBe("-x-y|", { x: ["a", "b"], y: ["c", "d"] });
      expectObservable(cold("abc|").pipe(bufferCount(2))).toBe("-x-(y|)", { x: ["a", "b"], y: ["c"] });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("abcd|").pipe(windowCount(2)));
    });
    // A third window is opened at frame 3, the moment the second one fills, and completes empty at 4.
    expect(w.outer()).toBe("W@0 W@1 W@3 C@4");
    expect(w.inner()).toBe("0:a@0 0:b@1 0:C@1 1:c@2 1:d@3 1:C@3 2:C@4");
  });

  it("C4 with every > size there are gaps: a value arriving between regions belongs to none and is dropped", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("abcde|").pipe(bufferCount(2, 3))).toBe("-x--y|", { x: ["a", "b"], y: ["d", "e"] });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("abcde|").pipe(windowCount(2, 3)));
    });
    // `c` at frame 2 reaches no window. The second window is opened at 2 and receives d and e.
    expect(w.outer()).toBe("W@0 W@2 C@5");
    expect(w.inner()).toBe("0:a@0 0:b@1 0:C@1 1:d@3 1:e@4 1:C@4");
  });

  it("C3 source error: buffers dropped, every open window receives the error", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("ab#").pipe(bufferCount(3))).toBe("--#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("ab#").pipe(windowCount(3)));
    });
    expect(w.outer()).toBe("W@0 E@2");
    expect(w.inner()).toBe("0:a@0 0:b@1 0:E@2");
  });
});

describe("bufferTime(span, interval, max) / windowTime(span, interval, max)", () => {
  it("T2 without a creation interval the region restarts on each expiry, and an expiry with nothing collected emits an empty buffer; T4 complete flushes the open region", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a----|").pipe(bufferTime(2))).toBe("--x-y(y|)", { x: ["a"], y: [] });
    });
  });

  it("T3 with a creation interval regions overlap; a value joins every open region; T4 complete flushes all open regions in order", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Regions [0,4) [2,6) [4,8) [6,10). b at 3 is in two of them, c at 5 in two. Complete at 7 flushes [c] and [].
      expectObservable(cold("-a-b-c-|").pipe(bufferTime(4, 2))).toBe("----x-y(zw|)", {
        x: ["a", "b"],
        y: ["b", "c"],
        z: ["c"],
        w: [],
      });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("-a-b-c-|").pipe(windowTime(4, 2)));
    });
    expect(w.outer()).toBe("W@0 W@2 W@4 W@6 C@7");
    expect(w.inner()).toBe("0:a@1 0:b@3 1:b@3 0:C@4 1:c@5 2:c@5 1:C@6 2:C@7 3:C@7");
  });

  it("T1 a region reaching max size closes early", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("ab---|").pipe(bufferTime(5, null, 2))).toBe("-x---(y|)", { x: ["a", "b"], y: [] });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("ab---|").pipe(windowTime(5, null, 2)));
    });
    expect(w.outer()).toBe("W@0 W@1 C@5");
    expect(w.inner()).toBe("0:a@0 0:b@1 0:C@1 1:C@5");
  });

  it("T5 source error: buffers dropped, every open window receives the error", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a#").pipe(bufferTime(3))).toBe("-#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a#").pipe(windowTime(3)));
    });
    expect(w.outer()).toBe("W@0 E@1");
    expect(w.inner()).toBe("0:a@0 0:E@1");
  });
});

describe("bufferToggle(openings, closing) / windowToggle(openings, closing)", () => {
  it("G1 an opening starts a region; G2 its closing notifier ends it; G7 a value with no open region is dropped; G5 openings completing is ignored; G8 complete with nothing open completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Regions [2,4) and [6,8). a at 1 and c at 5 fall outside every region.
      expectObservable(cold("-a-b-c-d--|").pipe(bufferToggle(cold("--o---o|"), () => timer(2)))).toBe("----x---y-|", {
        x: ["b"],
        y: ["d"],
      });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("-a-b-c-d--|").pipe(windowToggle(cold("--o---o|"), () => timer(2))));
    });
    expect(w.outer()).toBe("W@2 W@6 C@10");
    expect(w.inner()).toBe("0:b@3 0:C@4 1:d@7 1:C@8");
  });

  it("G3 a closing notifier that completes without emitting leaves the region open; G8 source complete then flushes it", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-b-c|").pipe(bufferToggle(cold("-o|"), () => cold("-|")))).toBe("-----(x|)", { x: ["b", "c"] });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a-b-c|").pipe(windowToggle(cold("-o|"), () => cold("-|"))));
    });
    expect(w.outer()).toBe("W@1 C@5");
    expect(w.inner()).toBe("0:b@2 0:c@4 0:C@5");
  });

  it("G4 a closing notifier erroring errors the result and the open windows", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a---|").pipe(bufferToggle(cold("-o|"), () => cold("-#")))).toBe("--#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a---|").pipe(windowToggle(cold("-o|"), () => cold("-#"))));
    });
    expect(w.outer()).toBe("W@1 E@2");
    expect(w.inner()).toBe("0:E@2");
  });

  it("G6 the openings erroring errors the result, but the open windows are torn down silently, without an error", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-b---|").pipe(bufferToggle(cold("-o-#"), () => timer(10)))).toBe("---#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a-b---|").pipe(windowToggle(cold("-o-#"), () => timer(10))));
    });
    expect(w.outer()).toBe("W@1 E@3");
    // No "0:E@3": the window's subscribers never learn why it stopped.
    expect(w.inner()).toBe("0:b@2");
  });

  it("G9 source error: buffers dropped, every open window receives the error", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-b#").pipe(bufferToggle(cold("-o|"), () => timer(10)))).toBe("---#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a-b#").pipe(windowToggle(cold("-o|"), () => timer(10))));
    });
    expect(w.outer()).toBe("W@1 E@3");
    expect(w.inner()).toBe("0:b@2 0:E@3");
  });
});

describe("bufferWhen(closing) / windowWhen(closing)", () => {
  it("W0 a region is open from subscribe; W2 the closing notifier firing closes it and opens the next with a fresh notifier; W5 complete flushes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("-a-b-c|").pipe(bufferWhen(() => cold("--c|")))).toBe("--x-y-(z|)", { x: ["a"], y: ["b"], z: ["c"] });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("-a-b-c|").pipe(windowWhen(() => cold("--c|"))));
    });
    expect(w.outer()).toBe("W@0 W@2 W@4 C@6");
    expect(w.inner()).toBe("0:a@1 0:C@2 1:b@3 1:C@4 2:c@5 2:C@6");
  });

  it("W3 the closing notifier completing without emitting: bufferWhen strands the region until source complete, windowWhen treats it as a boundary and rolls over", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // The notifier completes silently at 2, 4, 6. Nothing is flushed until the source completes.
      expectObservable(cold("-a-b-c|").pipe(bufferWhen(() => cold("--|")))).toBe("------(x|)", { x: ["a", "b", "c"] });
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("-a-b-c|").pipe(windowWhen(() => cold("--|"))));
    });
    // Identical to the firing case above: completion of the notifier is a boundary for windows.
    expect(w.outer()).toBe("W@0 W@2 W@4 C@6");
    expect(w.inner()).toBe("0:a@1 0:C@2 1:b@3 1:C@4 2:c@5 2:C@6");
  });

  it("W4 the closing notifier erroring errors the result and the open window", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a---|").pipe(bufferWhen(() => cold("-#")))).toBe("-#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a---|").pipe(windowWhen(() => cold("-#"))));
    });
    expect(w.outer()).toBe("W@0 E@1");
    expect(w.inner()).toBe("0:a@0 0:E@1");
  });

  it("W6 source error: the buffer is dropped, the window receives the error", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a#").pipe(bufferWhen(() => timer(10)))).toBe("-#");
    });
    const ts = scheduler();
    let w!: ReturnType<typeof observeWindows>;
    ts.run(({ cold }) => {
      w = observeWindows(ts, cold("a#").pipe(windowWhen(() => timer(10))));
    });
    expect(w.outer()).toBe("W@0 E@1");
    expect(w.inner()).toBe("0:a@0 0:E@1");
  });
});
