/**
 * Step tables for the timing family, verified row by row against RxJS 7.8.2.
 *
 * Operators: debounceTime, auditTime, sampleTime, throttleTime in its three
 * configurations, plus the silent-duration rows of the selector variants
 * debounce, audit, throttle and sample. Row IDs (D1, A2, S3, TL4, TT5, TB6, V7)
 * match docs/10-timing-family-step-tables.md. Every duration is 3 frames.
 * Scenarios avoid a source event and a timer expiry on the same frame, so
 * the tables never depend on same-frame scheduling order.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import {
  Observable,
  OperatorFunction,
  audit,
  auditTime,
  debounce,
  debounceTime,
  sample,
  sampleTime,
  throttle,
  throttleTime,
} from "rxjs";

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

const leading = throttleTime<string>(3);
const trailing = throttleTime<string>(3, undefined, { leading: false, trailing: true });
const both = throttleTime<string>(3, undefined, { leading: true, trailing: true });

describe("debounceTime(3)", () => {
  it("D1 Idle + next arms the deadline; D3 Pending + expire emits and returns to Idle; D4 Idle + complete completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-----|").pipe(debounceTime(3))).toBe("---a--|");
    });
  });

  it("D2 Pending + next replaces the value and restarts the deadline", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // `b` at frame 2 moves the deadline from 3 to 5. `a` is never emitted.
      expectObservable(cold("a-b----|").pipe(debounceTime(3))).toBe("-----b-|");
    });
  });

  it("D5 Pending + complete flushes the pending value, then completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Complete at frame 2, one frame before the deadline. `a` leaves at 2, not 3.
      expectObservable(cold("a-|").pipe(debounceTime(3))).toBe("--(a|)");
    });
  });

  it("D6 Pending + error drops the pending value and errors", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-#").pipe(debounceTime(3))).toBe("--#");
    });
  });
});

describe("auditTime(3)", () => {
  it("A1 Idle + next opens the window; A3 Pending + expire emits and returns to Idle; A4 Idle + complete completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-----|").pipe(auditTime(3))).toBe("---a--|");
    });
  });

  it("A2 Pending + next replaces the value and keeps the deadline", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // The window opened at 0 still closes at 3. It emits the latest value, `b`.
      expectObservable(cold("a-b----|").pipe(auditTime(3))).toBe("---b---|");
    });
  });

  it("A5 Pending + complete waits (Draining); A6 Draining + expire emits, then completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Complete at frame 2. Nothing happens until the window closes at 3.
      expectObservable(cold("a-|").pipe(auditTime(3))).toBe("---(a|)");
    });
  });

  it("A7 Pending + error drops the pending value and errors", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-#").pipe(auditTime(3))).toBe("--#");
    });
  });
});

describe("sampleTime(3)", () => {
  it("S1 Empty + next stores; S2 Pending + next replaces; S3 Pending + tick emits and empties", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("ab-----|").pipe(sampleTime(3))).toBe("---b---|");
    });
  });

  it("S4 Empty + tick does nothing", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Tick at 3 emits `a`. Tick at 6 finds nothing stored.
      expectObservable(cold("a------|").pipe(sampleTime(3))).toBe("---a---|");
    });
  });

  it("S0 the clock is armed at subscribe and never restarted by values", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // `b` arrives at 4 and waits for the fixed tick at 6, not for 4 + 3.
      expectObservable(cold("a---b--|").pipe(sampleTime(3))).toBe("---a--b|");
    });
  });

  it("S5 Pending + complete drops the pending value and completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-|").pipe(sampleTime(3))).toBe("--|");
    });
  });

  it("S6 Pending + error drops the pending value and errors", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-#").pipe(sampleTime(3))).toBe("--#");
    });
  });
});

describe("throttleTime(3) leading only (default)", () => {
  it("TL1 Open + next emits and locks; TL2 Locked + next is stored but never emitted; TL3 Locked + expire opens", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // `a` leaves at once. `b` is swallowed. The lock lifts at 3; `c` at 4 leaves at once.
      expectObservable(cold("ab--c-|").pipe(leading)).toBe("a---c-|");
    });
  });

  it("TL4 Open + complete completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a----|").pipe(leading)).toBe("a----|");
    });
  });

  it("TL5 Locked(y) + complete drops the stored value and completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("ab|").pipe(leading)).toBe("a-|");
    });
  });

  it("TL6 Locked + error errors", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-#").pipe(leading)).toBe("a-#");
    });
  });
});

describe("throttleTime(3) trailing only", () => {
  it("TT1 Open + next locks and stores without emitting; TT2 Locked + next replaces; TT3 Locked(x) + expire emits x and opens a new window; TT4 Locked(empty) + next stores; TT5 Locked(empty) + expire opens", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Windows: 0..3 emits b at 3 and relocks 3..6; c stored at 4, emitted at 6, relocks 6..9; 9 opens.
      expectObservable(cold("ab--c-----|").pipe(trailing)).toBe("---b--c---|");
    });
  });

  it("TT5 after an empty window the operator is Open: a value arriving then is locked afresh", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // a at 3 relocks until 6; the 6 expiry finds nothing and opens; b at 7 starts a new window to 10.
      // Complete at 8 arrives while b is pending, so the trailing emit and complete happen at 10 (TT6, TT7).
      expectObservable(cold("a------b|").pipe(trailing)).toBe("---a------(b|)");
    });
  });

  it("TT6 Locked(x) + complete waits (Draining); TT7 Draining + expire emits x, then completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-|").pipe(trailing)).toBe("---(a|)");
    });
  });

  it("TT8 Locked(empty) + complete completes at once", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // a emitted at 3 opened a fresh window with nothing stored. Complete at 4 does not wait for 6.
      expectObservable(cold("a---|").pipe(trailing)).toBe("---a|");
    });
  });

  it("TT9 Locked(x) + error drops x and errors", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-#").pipe(trailing)).toBe("--#");
    });
  });
});

describe("throttleTime(3) leading and trailing", () => {
  it("TB1 Open + next emits and locks; TB2 Locked + next stores; TB3 Locked(y) + expire emits y and relocks; TB4 Locked(empty) + expire opens", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("ab--c-----|").pipe(both)).toBe("a--b--c---|");
    });
  });

  it("TB5 Locked(y) + complete waits (Draining); TB6 Draining + expire emits y, then completes", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("ab|").pipe(both)).toBe("a--(b|)");
    });
  });

  it("TB7 Locked(empty) + complete completes at once", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-|").pipe(both)).toBe("a-|");
    });
  });

  it("TB8 Locked(y) + error drops y and errors", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("ab#").pipe(both)).toBe("a-#");
    });
  });
});

describe("teardown: unsubscribe while pending cancels and releases the source (D7, A8, S7, TL7, TT10, TB9)", () => {
  // Leading configurations emit `a` at frame 0 before the unsubscribe; the others hold it.
  const cases: Array<[string, OperatorFunction<string, string>, string]> = [
    ["debounceTime", debounceTime(3), "--"],
    ["auditTime", auditTime(3), "--"],
    ["sampleTime", sampleTime(3), "--"],
    ["throttleTime leading", leading, "a-"],
    ["throttleTime trailing", trailing, "--"],
    ["throttleTime both", both, "a-"],
  ];
  for (const [name, op, expected] of cases) {
    it(name, () => {
      scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
        const source = cold("a----|");
        expectObservable(source.pipe(op), "^-!").toBe(expected);
        expectSubscriptions(source.subscriptions).toBe("^-!");
      });
    });
  }
});

describe("selector variants: the duration completes without emitting (silent duration)", () => {
  const silent = (cold: (m: string) => Observable<string>) => () => cold("--|");

  it("V1 debounce: the value stays pending and is still flushed on complete", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a----|").pipe(debounce(silent(cold)))).toBe("-----(a|)");
    });
  });

  it("V2 audit, Pending: the value is stranded, and complete drops it", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a----|").pipe(audit(silent(cold)))).toBe("-----|");
    });
  });

  it("V3 audit, Draining: the silent close completes the stream and drops the value", () => {
    scheduler().run(({ cold, expectObservable }) => {
      // Complete at 1 while the duration is still open; the duration closes silently at 2.
      expectObservable(cold("a|").pipe(audit(silent(cold)))).toBe("--|");
    });
  });

  it("V4 audit, Stranded + next: a new duration starts and the stranded value is lost", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const selector = (x: string): Observable<string> => (x === "a" ? cold("--|") : cold("-n|"));
      // a is stranded at 2. b at 4 opens a duration that emits at 5, releasing b. a never leaves.
      expectObservable(cold("a---b----|").pipe(audit(selector))).toBe("-----b---|");
    });
  });

  it("V5 throttle with trailing: the window vanishes, the stored value is never emitted", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const op = throttle(silent(cold), { leading: true, trailing: true });
      // a leaves at once. b is stored at 1. The duration closes silently at 2. Complete at 5 drops b.
      expectObservable(cold("ab---|").pipe(op)).toBe("a----|");
    });
  });

  it("V6 sample: a notifier that completes leaves the clock dead; pending values never leave", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a----|").pipe(sample(cold("--|")))).toBe("-----|");
    });
  });
});
