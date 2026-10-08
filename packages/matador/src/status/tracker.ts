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

/**
 * Maximum length of an error string in a stored record.
 * A long error text must not make a terminal record too large.
 */
export const MAX_RECORD_ERROR_LENGTH = 1024;

function bound(text: string): string {
  return text.length > MAX_RECORD_ERROR_LENGTH
    ? text.slice(0, MAX_RECORD_ERROR_LENGTH)
    : text;
}

function toRecordError(error: Error): StatusRecordError {
  return { name: error.name, message: bound(error.message) };
}

/**
 * Copies the envelope for storage with bounded error strings.
 * The live envelope that the pipeline uses is not changed.
 */
function boundedEnvelope(envelope: Envelope): Envelope {
  const { firstError, lastError } = envelope.docket;
  if (
    (firstError === undefined || firstError.length <= MAX_RECORD_ERROR_LENGTH) &&
    (lastError === undefined || lastError.length <= MAX_RECORD_ERROR_LENGTH)
  ) {
    return envelope;
  }
  return {
    ...envelope,
    docket: {
      ...envelope.docket,
      ...(firstError !== undefined && { firstError: bound(firstError) }),
      ...(lastError !== undefined && { lastError: bound(lastError) }),
    },
  };
}

/**
 * Returns the last progress value of the reporter, or undefined if it
 * cannot be serialized. Bad progress details must not block a terminal status.
 */
function serializableProgress(
  reporter?: ProgressReporter,
): StatusProgress | undefined {
  const progress = reporter?.current;
  if (progress === undefined) {
    return undefined;
  }
  try {
    JSON.stringify(progress);
    return progress;
  } catch {
    return undefined;
  }
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
    } catch (e) {
      const error = toError(e);
      const trackingId = writes[0]?.trackingId ?? 'unknown';
      await this.hooks.onStatusStoreError({
        trackingId,
        operation: 'create',
        error,
      });
      throw new StatusStoreError('create', trackingId, error);
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
    try {
      await reporter?.stop();
      const progress = serializableProgress(reporter);

      let json: JsonSerializable | undefined;
      try {
        json = toJsonResult(result);
      } catch (error) {
        const cause = new ResultNotSerializableError(
          envelope.docket.targetSubscriber,
          toError(error),
        );
        await this.write('update', envelope, 'failed', {
          progress,
          error: toRecordError(cause),
        });
        await this.reportError(envelope, 'update', cause);
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
          trackingIdOf(envelope),
          envelope.docket.targetSubscriber,
          size,
          maxBytes,
        );
        await this.write('update', envelope, 'failed', {
          progress,
          error: toRecordError(cause),
        });
        await this.reportError(envelope, 'update', cause);
        return;
      }

      await this.write('update', envelope, 'completed', {
        progress,
        result: json,
      });
    } catch (error) {
      await this.reportError(envelope, 'update', toError(error));
    }
  }

  async markFailure(
    envelope: Envelope,
    error: Error,
    decision: RetryDecision,
    reporter?: ProgressReporter,
  ): Promise<void> {
    try {
      await reporter?.stop();
      const status: TrackingStatus =
        decision.action === 'retry' ? 'retrying' : 'failed';
      await this.write('update', envelope, status, {
        progress: serializableProgress(reporter),
        error: toRecordError(error),
      });
    } catch (e) {
      await this.reportError(envelope, 'update', toError(e));
    }
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
      envelope: boundedEnvelope(envelope),
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
   * Logs an error and passes it to the hook. Never throws.
   */
  private async reportError(
    envelope: Envelope,
    operation: 'update' | 'progress',
    error: Error,
  ): Promise<void> {
    const trackingId = envelope.docket.trackingId ?? 'unknown';
    const subscriberName = envelope.docket.targetSubscriber;
    try {
      this.hooks.logger.error(
        `[Matador] 🔴 Status store ${operation} failed for tracking ID '${trackingId}'.`,
        error,
      );
    } catch {
      // A logger problem must not change message handling.
    }
    await this.hooks.onStatusStoreError({
      trackingId,
      subscriberName,
      operation,
      error,
    });
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
    try {
      const trackingId = trackingIdOf(envelope);
      const subscriberName = envelope.docket.targetSubscriber;
      let record = this.buildRecord(envelope, status, fields);
      const maxBytes = this.store.config.maxRecordBytes;
      let size = recordSize(record);

      if (size > maxBytes && operation === 'update') {
        // Keep the status change. Leave out the optional large fields once.
        record = this.buildRecord(envelope, status, { error: fields.error });
        size = recordSize(record);
      }

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
        await this.reportError(envelope, operation, error);
        return;
      }

      await this.store.set(trackingId, subscriberName, record);
    } catch (e) {
      await this.reportError(envelope, operation, toError(e));
    }
  }
}
