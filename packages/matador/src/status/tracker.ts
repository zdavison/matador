import type { JsonSerializable } from '../checkpoint/index.js';
import {
  RecordTooLargeError,
  ResultNotSerializableError,
  StatusStoreError,
} from '../errors/index.js';
import type { SafeHooks } from '../hooks/index.js';
import type { RetryDecision } from '../retry/index.js';
import type { Envelope } from '../types/index.js';
import {
  ProgressReporter,
  type ProgressScheduler,
  realScheduler,
} from './progress.js';
import type {
  StatusProgress,
  StatusRecord,
  StatusRecordError,
  StatusStore,
  TrackingStatus,
} from './types.js';

type RecordFields = {
  readonly progress?: StatusProgress | undefined;
  readonly result?: JsonSerializable | undefined;
  readonly error?: StatusRecordError | undefined;
};

/**
 * True if the envelope belongs to a tracked subscriber.
 * Fanout sets docket.trackingId only on those envelopes.
 */
export function isTrackedEnvelope(envelope: Envelope): boolean {
  return envelope.docket.trackingId !== undefined;
}

function trackingIdOf(envelope: Envelope): string {
  const trackingId = envelope.docket.trackingId;
  if (trackingId === undefined) {
    throw new Error(
      `Envelope ${envelope.id} has no tracking ID. Call isTrackedEnvelope() first.`,
    );
  }
  return trackingId;
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

function toRecordError(error: Error): StatusRecordError {
  return { name: error.name, message: error.message };
}

function recordSize(record: StatusRecord): number {
  return new TextEncoder().encode(JSON.stringify(record)).byteLength;
}

/**
 * Converts a callback return value to JSON.
 * Throws if JSON.stringify throws, or if the value has no JSON form.
 */
function toJsonResult(result: unknown): JsonSerializable | undefined {
  if (result === undefined) {
    return undefined;
  }
  const text = JSON.stringify(result);
  if (text === undefined) {
    throw new TypeError(`A value of type ${typeof result} has no JSON form`);
  }
  return JSON.parse(text) as JsonSerializable;
}

/**
 * The only unit that writes status records.
 * Matador makes a tracker only if a statusStore is configured.
 */
export class StatusTracker {
  constructor(
    private readonly store: StatusStore,
    private readonly hooks: SafeHooks,
    private readonly scheduler: ProgressScheduler = realScheduler,
  ) {}

  /**
   * Writes the 'queued' records before the publish.
   * Checks all sizes before the first write.
   * @throws RecordTooLargeError if any record is too large
   * @throws StatusStoreError if the store fails
   */
  async createQueued(envelopes: readonly Envelope[]): Promise<void> {
    const writes = envelopes.map((envelope) => {
      const trackingId = trackingIdOf(envelope);
      const subscriberName = envelope.docket.targetSubscriber;
      const record = this.buildRecord(envelope, 'queued', {});
      this.assertSize(trackingId, subscriberName, record);
      return { trackingId, subscriberName, record };
    });

    try {
      await Promise.all(
        writes.map((w) =>
          this.store.set(w.trackingId, w.subscriberName, w.record),
        ),
      );
    } catch (error) {
      const trackingId = writes[0]?.trackingId ?? 'unknown';
      throw new StatusStoreError('create', trackingId, toError(error));
    }
  }

  async markPublishFailed(envelope: Envelope, error: Error): Promise<void> {
    await this.write('update', envelope, 'failed', {
      error: toRecordError(error),
    });
  }

  async markActive(envelope: Envelope): Promise<void> {
    await this.write('update', envelope, 'active', {});
  }

  startProgress(envelope: Envelope): ProgressReporter {
    return new ProgressReporter(
      (progress) => this.write('progress', envelope, 'active', { progress }),
      this.store.config.progressIntervalMs,
      (percent) =>
        this.hooks.logger.warn(
          `[Matador] 🟡 Ignored progress value ${percent} for tracking ID '${envelope.docket.trackingId}'. The percent must be a finite number.`,
        ),
      this.scheduler,
    );
  }

  async markCompleted(
    envelope: Envelope,
    result: unknown,
    reporter?: ProgressReporter,
  ): Promise<void> {
    await reporter?.stop();
    const progress = reporter?.current;
    const trackingId = trackingIdOf(envelope);
    const subscriberName = envelope.docket.targetSubscriber;

    let json: JsonSerializable | undefined;
    try {
      json = toJsonResult(result);
    } catch (error) {
      const cause = new ResultNotSerializableError(
        subscriberName,
        toError(error),
      );
      await this.write('update', envelope, 'failed', {
        progress,
        error: toRecordError(cause),
      });
      return;
    }

    const record = this.buildRecord(envelope, 'completed', {
      progress,
      result: json,
    });
    const size = recordSize(record);
    const maxBytes = this.store.config.maxRecordBytes;
    if (size > maxBytes) {
      const cause = new RecordTooLargeError(
        trackingId,
        subscriberName,
        size,
        maxBytes,
      );
      await this.write('update', envelope, 'failed', {
        progress,
        error: toRecordError(cause),
      });
      return;
    }

    await this.write('update', envelope, 'completed', {
      progress,
      result: json,
    });
  }

  async markFailure(
    envelope: Envelope,
    error: Error,
    decision: RetryDecision,
    reporter?: ProgressReporter,
  ): Promise<void> {
    await reporter?.stop();
    const status: TrackingStatus =
      decision.action === 'retry' ? 'retrying' : 'failed';
    await this.write('update', envelope, status, {
      progress: reporter?.current,
      error: toRecordError(error),
    });
  }

  read(
    trackingId: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | undefined>;
  read(
    trackingId: string,
    subscriberName: string,
  ): Promise<StatusRecord | undefined>;
  async read(
    trackingId: string,
    subscriberName?: string,
  ): Promise<
    Readonly<Record<string, StatusRecord>> | StatusRecord | undefined
  > {
    try {
      return subscriberName === undefined
        ? await this.store.getAll(trackingId)
        : await this.store.get(trackingId, subscriberName);
    } catch (e) {
      const error = toError(e);
      await this.hooks.onStatusStoreError({
        trackingId,
        subscriberName,
        operation: 'read',
        error,
      });
      throw new StatusStoreError('read', trackingId, error);
    }
  }

  private buildRecord(
    envelope: Envelope,
    status: TrackingStatus,
    fields: RecordFields,
  ): StatusRecord {
    return {
      status,
      ...(fields.progress !== undefined && { progress: fields.progress }),
      ...(fields.result !== undefined && { result: fields.result }),
      ...(fields.error !== undefined && { error: fields.error }),
      envelope,
      updatedAt: new Date().toISOString(),
    };
  }

  private assertSize(
    trackingId: string,
    subscriberName: string,
    record: StatusRecord,
  ): void {
    const size = recordSize(record);
    const maxBytes = this.store.config.maxRecordBytes;
    if (size > maxBytes) {
      throw new RecordTooLargeError(trackingId, subscriberName, size, maxBytes);
    }
  }

  /**
   * Writes a record after the send. Never throws: a status store problem
   * must not change message handling.
   */
  private async write(
    operation: 'update' | 'progress',
    envelope: Envelope,
    status: TrackingStatus,
    fields: RecordFields,
  ): Promise<void> {
    const trackingId = trackingIdOf(envelope);
    const subscriberName = envelope.docket.targetSubscriber;
    const record = this.buildRecord(envelope, status, fields);

    const size = recordSize(record);
    const maxBytes = this.store.config.maxRecordBytes;
    if (size > maxBytes) {
      const error = new RecordTooLargeError(
        trackingId,
        subscriberName,
        size,
        maxBytes,
      );
      if (operation === 'progress') {
        this.hooks.logger.warn(
          `[Matador] 🟡 Dropped a progress value for tracking ID '${trackingId}'. ${error.message}`,
        );
        return;
      }
      await this.hooks.onStatusStoreError({
        trackingId,
        subscriberName,
        operation,
        error,
      });
      return;
    }

    try {
      await this.store.set(trackingId, subscriberName, record);
    } catch (e) {
      const error = toError(e);
      this.hooks.logger.error(
        `[Matador] 🔴 Status store ${operation} failed for tracking ID '${trackingId}'.`,
        error,
      );
      await this.hooks.onStatusStoreError({
        trackingId,
        subscriberName,
        operation,
        error,
      });
    }
  }
}
