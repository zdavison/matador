import type { JsonSerializable } from '../checkpoint/index.js';
import type { Envelope } from '../types/index.js';

/**
 * Lifecycle status of one tracked subscriber for one send.
 * 'completed' and 'failed' are terminal.
 */
export type TrackingStatus =
  | 'queued'
  | 'active'
  | 'retrying'
  | 'completed'
  | 'failed';

/**
 * Progress reported by a subscriber with ctx.progress().
 */
export interface StatusProgress {
  /** A number from 0 to 100 */
  readonly percent: number;
  /** Optional small JSON value, for example counters */
  readonly details?: JsonSerializable | undefined;
}

/**
 * Error summary kept in a status record. Has no stack trace.
 */
export interface StatusRecordError {
  readonly name: string;
  readonly message: string;
}

/**
 * Status of one tracked subscriber for one send.
 *
 * The record does not copy values that the envelope holds:
 * the tracking ID is docket.trackingId, the subscriber is docket.targetSubscriber,
 * the creation time is docket.createdAt and the attempt count is docket.attempts.
 */
export interface StatusRecord {
  readonly status: TrackingStatus;
  readonly progress?: StatusProgress | undefined;
  /** Set only when status is 'completed'. */
  readonly result?: JsonSerializable | undefined;
  /** Set when status is 'retrying' or 'failed'. Covers errors that are not in the docket. */
  readonly error?: StatusRecordError | undefined;
  /** The latest envelope, including data and docket. */
  readonly envelope: Envelope;
  /** ISO 8601 */
  readonly updatedAt: string;
}

/**
 * Settings that each store implements. Stores take these in their constructor options.
 * The tracker enforces them, so no store reimplements the size check or the coalescing.
 */
export interface StatusStoreConfig {
  /** Limit on the JSON-serialized record, in bytes. */
  readonly maxRecordBytes: number;
  /** Minimum time between two progress writes for one record, in milliseconds. */
  readonly progressIntervalMs: number;
}

export const DEFAULT_MAX_RECORD_BYTES = 65_536;
export const DEFAULT_PROGRESS_INTERVAL_MS = 1_000;

/**
 * Fills the defaults. Each store calls this in its constructor.
 */
export function resolveStatusStoreConfig(
  options: Partial<StatusStoreConfig> = {},
): StatusStoreConfig {
  return {
    maxRecordBytes: options.maxRecordBytes ?? DEFAULT_MAX_RECORD_BYTES,
    progressIntervalMs:
      options.progressIntervalMs ?? DEFAULT_PROGRESS_INTERVAL_MS,
  };
}

/**
 * Persistence for status records, keyed by tracking ID and subscriber name.
 * Writes are plain replacements. v1 has no atomic operations.
 */
export interface StatusStore {
  /** The resolved settings. The tracker reads these values. */
  readonly config: StatusStoreConfig;

  /** Returns all records for one tracking ID, keyed by subscriber name. */
  getAll(
    trackingId: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | undefined>;

  /** Returns one record. */
  get(
    trackingId: string,
    subscriberName: string,
  ): Promise<StatusRecord | undefined>;

  /** Writes the record. Replaces any record with the same tracking ID and subscriber name. */
  set(
    trackingId: string,
    subscriberName: string,
    record: StatusRecord,
  ): Promise<void>;

  /** Deletes all records for one tracking ID. */
  delete(trackingId: string): Promise<void>;

  /** Optional: deletes records with updatedAt before olderThan. Returns the count. */
  cleanup?(olderThan: Date): Promise<number>;
}
