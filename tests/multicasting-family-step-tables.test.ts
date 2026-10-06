/**
 * Step tables for the multicasting family, verified row by row against RxJS 7.8.2.
 *
 * Operators: share with each reset option, shareReplay with refCount off and on,
 * and connectable with manual connect and disconnect. Row IDs (SH1, SR2, CN3)
 * match docs/13-multicasting-family-step-tables.md. The source is cold, so its
 * subscription log is the connection log: one entry per connect, closed on
 * disconnect. Subscriber marbles use `^` and `!` for join and leave.
 */
import { describe, expect, it } from "vitest";
import { TestScheduler } from "rxjs/testing";
import { Subscription, connectable, share, shareReplay, timer } from "rxjs";

const scheduler = (): TestScheduler =>
  new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

describe("share(), all resets on (default)", () => {
  it("SH1 first subscriber connects; SH2 later subscribers attach; SH4 a leave with others left keeps the connection; SH5 the last leave disconnects and resets; SH11 the next subscriber reconnects from scratch", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c-d-e|");
      const shared = source.pipe(share());
      expectObservable(shared, "^--!").toBe("a-b");
      expectObservable(shared, "-^-!").toBe("--b");
      // Both leave at 3: refCount 0, disconnect at 3. A subscriber at 5 starts a new connection and sees `a` again.
      expectObservable(shared, "-----^--!").toBe("-----a-b");
      expectSubscriptions(source.subscriptions).toBe(["^--!", "-----^--!"]);
    });
  });

  it("SH1' the subscriber is attached to the subject before the source is connected, so synchronous values are not lost", () => {
    scheduler().run(({ cold, expectObservable }) => {
      expectObservable(cold("(ab|)").pipe(share())).toBe("(ab|)");
    });
  });

  it("SH9 source complete resets; SH11 a subscriber after completion reconnects and replays nothing", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a|");
      const shared = source.pipe(share());
      expectObservable(shared).toBe("a|");
      expectObservable(shared, "--^").toBe("--a|");
      expectSubscriptions(source.subscriptions).toBe(["^!", "--^!"]);
    });
  });

  it("SH12 source error resets; the next subscriber reconnects", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      const shared = source.pipe(share());
      expectObservable(shared).toBe("a#");
      expectObservable(shared, "--^").toBe("--a#");
      expectSubscriptions(source.subscriptions).toBe(["^!", "--^!"]);
    });
  });
});

describe("share({ resetOnRefCountZero: false })", () => {
  it("SH5' the last leave keeps the connection; SH14 values with no subscribers are lost; SH8 a returning subscriber joins the same connection", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c-d-|");
      const shared = source.pipe(share({ resetOnRefCountZero: false }));
      expectObservable(shared, "^--!").toBe("a-b");
      // `c` at 4 goes into a Subject nobody listens to. The subscriber at 5 gets `d` and the completion.
      expectObservable(shared, "-----^").toBe("------d-|");
      expectSubscriptions(source.subscriptions).toBe("^-------!");
    });
  });
});

describe("share({ resetOnRefCountZero: () => timer(3) })", () => {
  it("SH5'' the last leave arms a grace timer; SH6 a subscriber inside the grace cancels it and joins the same connection", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c-d-e|");
      const shared = source.pipe(share({ resetOnRefCountZero: () => timer(3) }));
      expectObservable(shared, "^--!").toBe("a-b");
      // Grace runs 3..6. The subscriber at 5 cancels the reset. `c` at 4 was still lost.
      expectObservable(shared, "-----^").toBe("------d-e|");
      expectSubscriptions(source.subscriptions).toBe("^--------!");
    });
  });

  it("SH7 the grace timer firing disconnects and resets; the next subscriber reconnects", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c-d-e|");
      const shared = source.pipe(share({ resetOnRefCountZero: () => timer(3) }));
      expectObservable(shared, "^--!").toBe("a-b");
      // The subscriber at 7 reconnects. Leaving at 10 arms a second grace, so the new connection lingers to 13.
      expectObservable(shared, "-------^--!").toBe("-------a-b");
      expectSubscriptions(source.subscriptions).toBe(["^-----!", "-------^-----!"]);
    });
  });
});

describe("share({ resetOnComplete: false }) and share({ resetOnError: false })", () => {
  it("SH9' completion is kept; SH10 a later subscriber gets complete at once and nothing reconnects", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a|");
      const shared = source.pipe(share({ resetOnComplete: false }));
      expectObservable(shared).toBe("a|");
      expectObservable(shared, "--^").toBe("--|");
      expectSubscriptions(source.subscriptions).toBe("^!");
    });
  });

  it("SH12' the error is kept; a later subscriber gets the error at once and nothing reconnects", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a#");
      const shared = source.pipe(share({ resetOnError: false }));
      expectObservable(shared).toBe("a#");
      expectObservable(shared, "--^").toBe("--#");
      expectSubscriptions(source.subscriptions).toBe("^!");
    });
  });
});

describe("shareReplay(1), refCount off (default)", () => {
  it("SR2 the last leave keeps the connection; SR3 values with no subscribers are buffered; SR1 a late subscriber gets the buffer; SR4 after completion a subscriber gets the buffer and complete", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c-|");
      const shared = source.pipe(shareReplay(1));
      expectObservable(shared, "^--!").toBe("a-b");
      // `c` arrived at 4 with nobody listening and is replayed at 5.
      expectObservable(shared, "-----^").toBe("-----c|");
      // The source completed at 6 and the connection is kept. A subscriber at 8 gets the replay and the completion.
      expectObservable(shared, "--------^").toBe("--------(c|)");
      expectSubscriptions(source.subscriptions).toBe("^-----!");
    });
  });

  it("SR5 source error resets and discards the buffer; the next subscriber reconnects", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-#");
      const shared = source.pipe(shareReplay(1));
      expectObservable(shared).toBe("a-#");
      expectObservable(shared, "----^").toBe("----a-#");
      expectSubscriptions(source.subscriptions).toBe(["^-!", "----^-!"]);
    });
  });
});

describe("shareReplay({ bufferSize: 1, refCount: true })", () => {
  it("SR6 the last leave disconnects and discards the buffer; the next subscriber reconnects and sees the source from the start", () => {
    scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c-|");
      const shared = source.pipe(shareReplay({ bufferSize: 1, refCount: true }));
      expectObservable(shared, "^--!").toBe("a-b");
      expectObservable(shared, "-----^").toBe("-----a-b-c-|");
      expectSubscriptions(source.subscriptions).toBe(["^--!", "-----^-----!"]);
    });
  });
});

describe("connectable(source)", () => {
  it("CN1 subscribers attach and receive nothing; CN2 connect subscribes the source; CN3 values broadcast; CN9 a late subscriber gets live values only; CN4 connect while connected returns the same connection", () => {
    const ts = scheduler();
    let sameConnection: boolean | undefined;
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c|");
      const c = connectable(source);
      expectObservable(c).toBe("--a-b-c|");
      expectObservable(c, "---^").toBe("----b-c|");
      ts.schedule(() => {
        const first = c.connect();
        sameConnection = c.connect() === first;
      }, 2);
      expectSubscriptions(source.subscriptions).toBe("--^----!");
    });
    expect(sameConnection).toBe(true);
  });

  it("CN10 a subscriber leaving does not disconnect; there is no reference count", () => {
    const ts = scheduler();
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c|");
      const c = connectable(source);
      expectObservable(c, "^-!").toBe("a-");
      ts.schedule(() => c.connect(), 0);
      expectSubscriptions(source.subscriptions).toBe("^----!");
    });
  });

  it("CN5 disconnect replaces the subject and strands the subscribers attached to the old one; CN7 new subscribers attach to the new subject; CN8 connect again serves them from the start", () => {
    const ts = scheduler();
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a-b-c|");
      const c = connectable(source);
      let connection: Subscription | undefined;
      // Attach before connecting so the synchronous `a` is received.
      expectObservable(c).toBe("a-b");
      ts.schedule(() => {
        connection = c.connect();
      }, 0);
      ts.schedule(() => connection?.unsubscribe(), 3);
      // The subscriber at 4 is on the new subject. Connect at 5 runs the source again for it.
      expectObservable(c, "----^").toBe("-----a-b-c|");
      ts.schedule(() => c.connect(), 5);
      expectSubscriptions(source.subscriptions).toBe(["^--!", "-----^----!"]);
    });
  });

  it("CN6 source completion is broadcast and replaces the subject; CN8 a subscriber after completion waits for a new connect", () => {
    const ts = scheduler();
    ts.run(({ cold, expectObservable, expectSubscriptions }) => {
      const source = cold("a|");
      const c = connectable(source);
      expectObservable(c).toBe("a|");
      ts.schedule(() => c.connect(), 0);
      expectObservable(c, "--^").toBe("---a|");
      ts.schedule(() => c.connect(), 3);
      expectSubscriptions(source.subscriptions).toBe(["^!", "---^!"]);
    });
  });
});
