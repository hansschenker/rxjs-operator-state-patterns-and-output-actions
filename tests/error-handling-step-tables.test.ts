/**
 * Step tables for the error-handling family, verified row by row against RxJS 7.8.2.
 *
 * Operators: retry with count, delay and resetOnSuccess; retryWhen; catchError.
 * Row IDs (R1, RW2, CE3) match docs/16-error-handling-step-tables.md. The source is
 * cold, so its subscription log is the attempt log: one entry per subscription.
 * The TestScheduler's `#` carries the string "error" unless a value is given.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import { EMPTY, catchError, delay, mergeMap, of, retry, retryWhen, take, throwError, timer } from "rxjs";

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

describe("retry(count | config)", () => {
  it("R3 an error below the count resubscribes at once; R4 the error at the count is forwarded; retry(1) is two attempts", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-#");
      expectObservable(source.pipe(retry(1))).toBe("a-a-#");
      expectSubscriptions(source.subscriptions).toBe(["^-!", "--^-!"]);
    });
  });

  it("R0 retry(0) is identity: the error passes and nothing is resubscribed; R2 completion passes", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      expectObservable(source.pipe(retry(0))).toBe("a#");
      expectSubscriptions(source.subscriptions).toBe("^!");
      expectObservable(cold("a|").pipe(retry(3))).toBe("a|");
    });
  });

  it("R1 resetOnSuccess: a value resets the count, so a source that always emits before failing is retried without end", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      expectObservable(source.pipe(retry({ count: 1, resetOnSuccess: true })), "^----!").toBe("aaaaa");
      expectSubscriptions(source.subscriptions).toBe(["^!", "-^!", "--^!", "---^!", "----^!"]);
    });
    scheduler().run(({ cold, expectObservable }) => {
      // Without the reset the same source gives up after the second attempt.
      expectObservable(cold("a#").pipe(retry({ count: 1 }))).toBe("aa#");
    });
  });

  it("R3' a numeric delay waits before resubscribing; the source is dead in the meantime", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      expectObservable(source.pipe(retry({ count: 1, delay: 3 }))).toBe("a---a#");
      expectSubscriptions(source.subscriptions).toBe(["^!", "----^!"]);
    });
  });

  it("R3'' a delay factory receives the error and a 1-based retry number; R5 its notifier emitting resubscribes", () => {
    const seen: number[] = [];
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      const op = retry<string>({
        count: 2,
        delay: (_err, n) => {
          seen.push(n);
          return timer(n);
        },
      });
      // Attempt 1 fails at 1, waits 1; attempt 2 fails at 3, waits 2; attempt 3 fails at 6 and the count is spent.
      expectObservable(source.pipe(op)).toBe("a-a--a#");
      expectSubscriptions(source.subscriptions).toBe(["^!", "--^!", "-----^!"]);
    });
    expect(seen).toEqual([1, 2]);
  });

  it("R6 a delay notifier that completes without emitting completes the result: the error is swallowed, not retried, not forwarded", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      expectObservable(source.pipe(retry({ count: 5, delay: () => EMPTY }))).toBe("a|");
      expectSubscriptions(source.subscriptions).toBe("^!");
    });
  });

  it("R7 a delay notifier that errors forwards its own error, not the source's", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const source = cold("a#");
      expectObservable(source.pipe(retry({ count: 5, delay: () => throwError(() => "give up") }))).toBe("a#", undefined, "give up");
    });
  });
});

describe("retryWhen(notifier)", () => {
  it("RW1/RW2 each error is pushed to the notifier; each notifier emission resubscribes; there is no count", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      expectObservable(source.pipe(retryWhen((errors) => errors)), "^---!").toBe("aaaa");
      expectSubscriptions(source.subscriptions).toBe(["^!", "-^!", "--^!", "---^!"]);
    });
  });

  it("RW2' a delayed notifier delays the resubscription", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      expectObservable(source.pipe(retryWhen((errors) => errors.pipe(delay(2)))), "^------!").toBe("a--a--a");
      expectSubscriptions(source.subscriptions).toBe(["^!", "---^!", "------^!"]);
    });
  });

  it("RW3 the notifier completing completes the result, and cancels the retry it has just started", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      // take(2) emits on the second error, which resubscribes, then completes, which ends the result at the same frame.
      expectObservable(source.pipe(retryWhen((errors) => errors.pipe(take(2))))).toBe("aa|");
      expectSubscriptions(source.subscriptions).toBe(["^!", "-^!", "--(^!)"]);
    });
  });

  it("RW4 the notifier erroring forwards that error", () => {
    scheduler().run(({ cold, expectObservable }) => {
      const source = cold("a#");
      const giveUpAfterOne = retryWhen<string>((errors) =>
        errors.pipe(mergeMap((err, i) => (i < 1 ? of(err) : throwError(() => "give up"))))
      );
      expectObservable(source.pipe(giveUpAfterOne)).toBe("aa#", undefined, "give up");
    });
  });
});

describe("catchError(selector)", () => {
  it("CE1 values before the error are forwarded and kept; CE3 the error switches to the selector's observable; CE4/CE5 its values and completion are forwarded", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-#");
      const fallback = cold("x-y|");
      expectObservable(source.pipe(catchError(() => fallback))).toBe("a-x-y|");
      expectSubscriptions(source.subscriptions).toBe("^-!");
      expectSubscriptions(fallback.subscriptions).toBe("--^--!");
    });
  });

  it("CE2 completion passes and the selector is never called", () => {
    let called = 0;
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(
        cold("a-|").pipe(
          catchError(() => {
            called++;
            return EMPTY;
          })
        )
      ).toBe("a-|");
    });
    expect(called).toBe(0);
  });

  it("CE3' a selector that throws errors the result with the thrown value", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(
        cold("a-#").pipe(
          catchError(() => {
            throw "boom";
          })
        )
      ).toBe("a-#", undefined, "boom");
    });
  });

  it("CE6 an error from the selector's observable is not caught again", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("a-#").pipe(catchError(() => cold("x#")))).toBe("a-x#");
    });
  });

  it("CE7 returning `caught` resubscribes the source with catchError reapplied: retry without end", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      expectObservable(source.pipe(catchError((_err, caught) => caught)), "^---!").toBe("aaaa");
      expectSubscriptions(source.subscriptions).toBe(["^!", "-^!", "--^!", "---^!"]);
    });
  });
});
