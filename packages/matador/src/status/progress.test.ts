import { describe, expect, it } from 'bun:test';
import { ProgressReporter, type ProgressScheduler } from './progress.js';
import type { StatusProgress } from './types.js';

class ManualScheduler implements ProgressScheduler {
  time = 0;
  private tasks: { at: number; fn: () => void; cancelled: boolean }[] = [];

  now(): number {
    return this.time;
  }

  schedule(fn: () => void, delayMs: number): () => void {
    const task = { at: this.time + delayMs, fn, cancelled: false };
    this.tasks.push(task);
    return () => {
      task.cancelled = true;
    };
  }

  advance(ms: number): void {
    this.time += ms;
    const due = this.tasks.filter((t) => !t.cancelled && t.at <= this.time);
    this.tasks = this.tasks.filter((t) => !due.includes(t));
    for (const task of due) task.fn();
  }
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

function setup(intervalMs = 1000) {
  const scheduler = new ManualScheduler();
  const writes: StatusProgress[] = [];
  const invalid: number[] = [];
  const reporter = new ProgressReporter(
    async (progress) => {
      writes.push(progress);
    },
    intervalMs,
    (percent) => invalid.push(percent),
    scheduler,
  );
  return { scheduler, writes, invalid, reporter };
}

describe('ProgressReporter', () => {
  it('writes the first report at once', async () => {
    const { writes, reporter } = setup();
    reporter.report(10);
    await settle();
    expect(writes).toEqual([{ percent: 10 }]);
  });

  it('coalesces reports inside the interval and writes the latest value', async () => {
    const { scheduler, writes, reporter } = setup();
    reporter.report(10);
    reporter.report(20);
    reporter.report(30, { done: 3 });
    await settle();
    expect(writes).toEqual([{ percent: 10 }]);

    scheduler.advance(1000);
    await settle();
    expect(writes).toEqual([
      { percent: 10 },
      { percent: 30, details: { done: 3 } },
    ]);
  });

  it('keeps only one write in flight', async () => {
    const scheduler = new ManualScheduler();
    const writes: StatusProgress[] = [];
    let release: () => void = () => {};
    const reporter = new ProgressReporter(
      (progress) => {
        writes.push(progress);
        return new Promise<void>((resolve) => {
          release = resolve;
        });
      },
      0,
      () => {},
      scheduler,
    );

    reporter.report(10);
    reporter.report(20);
    reporter.report(30);
    await settle();
    expect(writes).toEqual([{ percent: 10 }]);

    release();
    await settle();
    expect(writes).toEqual([{ percent: 10 }, { percent: 30 }]);
  });

  it('stop drops the pending value and waits for the write in flight', async () => {
    const scheduler = new ManualScheduler();
    const writes: StatusProgress[] = [];
    let release: () => void = () => {};
    const reporter = new ProgressReporter(
      (progress) => {
        writes.push(progress);
        return new Promise<void>((resolve) => {
          release = resolve;
        });
      },
      1000,
      () => {},
      scheduler,
    );

    reporter.report(10);
    reporter.report(50);
    let stopped = false;
    const stopping = reporter.stop().then(() => {
      stopped = true;
    });
    await settle();
    expect(stopped).toBe(false);

    release();
    await stopping;
    scheduler.advance(5000);
    await settle();
    expect(writes).toEqual([{ percent: 10 }]);
    expect(reporter.current).toEqual({ percent: 50 });
  });

  it('ignores reports after stop', async () => {
    const { writes, reporter } = setup();
    await reporter.stop();
    reporter.report(10);
    await settle();
    expect(writes).toEqual([]);
    expect(reporter.current).toBeUndefined();
  });

  it('clamps percent to the range 0 to 100', async () => {
    const { scheduler, writes, reporter } = setup(0);
    reporter.report(150);
    await settle();
    scheduler.advance(0);
    reporter.report(-5);
    await settle();
    expect(writes).toEqual([{ percent: 100 }, { percent: 0 }]);
  });

  it('ignores a percent that is not a finite number', async () => {
    const { writes, invalid, reporter } = setup();
    reporter.report(Number.NaN);
    reporter.report(Number.POSITIVE_INFINITY);
    await settle();
    expect(writes).toEqual([]);
    expect(invalid).toEqual([Number.NaN, Number.POSITIVE_INFINITY]);
    expect(reporter.current).toBeUndefined();
  });

  it('does not stop on a write that rejects', async () => {
    const scheduler = new ManualScheduler();
    let calls = 0;
    const reporter = new ProgressReporter(
      async () => {
        calls++;
        throw new Error('store down');
      },
      0,
      () => {},
      scheduler,
    );
    reporter.report(10);
    await settle();
    reporter.report(20);
    await settle();
    expect(calls).toBe(2);
  });
});
