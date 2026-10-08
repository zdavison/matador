import type { JsonSerializable } from '../checkpoint/index.js';
import type { StatusProgress } from './types.js';

/**
 * Time source and timer for ProgressReporter. Tests replace it.
 */
export interface ProgressScheduler {
  now(): number;
  /** Runs fn after delayMs. Returns a function that cancels the timer. */
  schedule(fn: () => void, delayMs: number): () => void;
}

export const realScheduler: ProgressScheduler = {
  now: () => Date.now(),
  schedule: (fn, delayMs) => {
    const timer = setTimeout(fn, delayMs);
    timer.unref?.();
    return () => clearTimeout(timer);
  },
};

/**
 * Coalesces progress reports for one execution of a tracked subscriber.
 *
 * - The first report is written at once.
 * - After that, the latest value is written at most once in each interval.
 * - Only one write is in flight at a time.
 * - stop() drops the pending value and waits for the write in flight.
 */
export class ProgressReporter {
  private latest: StatusProgress | undefined;
  private pending: StatusProgress | undefined;
  private inFlight: Promise<void> | undefined;
  private cancelTimer: (() => void) | undefined;
  private lastWriteAt = Number.NEGATIVE_INFINITY;
  private stopped = false;

  constructor(
    private readonly write: (progress: StatusProgress) => Promise<void>,
    private readonly intervalMs: number,
    private readonly onInvalid: (percent: number) => void,
    private readonly scheduler: ProgressScheduler = realScheduler,
  ) {}

  /** The latest reported value, written or not. */
  get current(): StatusProgress | undefined {
    return this.latest;
  }

  report(percent: number, details?: JsonSerializable): void {
    if (this.stopped) {
      return;
    }
    if (!Number.isFinite(percent)) {
      this.onInvalid(percent);
      return;
    }
    const clamped = Math.min(100, Math.max(0, percent));
    const progress: StatusProgress =
      details === undefined
        ? { percent: clamped }
        : { percent: clamped, details };
    this.latest = progress;
    this.pending = progress;
    this.flush();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.pending = undefined;
    this.cancelTimer?.();
    this.cancelTimer = undefined;
    await this.inFlight;
  }

  private flush(): void {
    if (
      this.stopped ||
      this.pending === undefined ||
      this.inFlight !== undefined ||
      this.cancelTimer !== undefined
    ) {
      return;
    }

    const waitMs = this.lastWriteAt + this.intervalMs - this.scheduler.now();
    if (waitMs > 0) {
      this.cancelTimer = this.scheduler.schedule(() => {
        this.cancelTimer = undefined;
        this.flush();
      }, waitMs);
      return;
    }

    const value = this.pending;
    this.pending = undefined;
    this.lastWriteAt = this.scheduler.now();
    this.inFlight = this.write(value)
      .catch(() => undefined)
      .finally(() => {
        this.inFlight = undefined;
        this.flush();
      });
  }
}
