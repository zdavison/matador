import type {
  CheckpointClearedContext,
  CheckpointHitContext,
  CheckpointLoadedContext,
  CheckpointMissContext,
} from '../checkpoint/index.js';
import type { StatusStoreOperation } from '../errors/index.js';
import type { RetryDecision } from '../retry/index.js';
import type { ConnectionState, Transport } from '../transport/index.js';
import type { Envelope, SubscriberDefinition } from '../types/index.js';

/**
 * Logger interface for Matador internal logging.
 */
export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

/**
 * Default logger that uses console.
 */
export const consoleLogger: Logger = {
  debug: (message, ...args) => console.debug(message, ...args),
  info: (message, ...args) => console.info(message, ...args),
  warn: (message, ...args) => console.warn(message, ...args),
  error: (message, ...args) => console.error(message, ...args),
};

/**
 * Context for enqueue success hook.
 */
export interface EnqueueSuccessContext {
  readonly envelope: Envelope;
  readonly queue: string;
  /** The transport that was used (e.g., 'local', 'rabbitmq') */
  readonly transport: string;
}

/**
 * Context for transport fallback hook.
 */
export interface TransportFallbackContext {
  /** The envelope that was being sent */
  readonly envelope: Envelope;
  /** The queue the message was being sent to */
  readonly queue: string;
  /** The transport that failed */
  readonly failedTransport: Transport['name'];
  /** The transport that will be tried next */
  readonly nextTransport: Transport['name'];
  /** The error from the failed transport */
  readonly error: Error;
}

/**
 * Context for enqueue error hook.
 */
export interface EnqueueErrorContext {
  readonly envelope: Envelope;
  readonly error: Error;
  /** The transport that failed (e.g., 'local', 'rabbitmq') */
  readonly transport: string;
}

/**
 * Context for decode error hook.
 */
export interface DecodeErrorContext {
  readonly error: Error;
  readonly rawMessage: Uint8Array;
  readonly sourceQueue: string;
  /** The transport that received the message (e.g., 'local', 'rabbitmq') */
  readonly transport: string;
}

/**
 * Context for worker success hook.
 */
export interface WorkerSuccessContext {
  readonly envelope: Envelope;
  readonly subscriber: SubscriberDefinition;
  readonly result: unknown;
  readonly durationMs: number;
  /** The transport that received the message (e.g., 'local', 'rabbitmq') */
  readonly transport: string;
}

/**
 * Context for worker error hook.
 */
export interface WorkerErrorContext {
  readonly envelope: Envelope;
  readonly subscriber: SubscriberDefinition;
  readonly error: Error;
  readonly durationMs: number;
  readonly decision: RetryDecision;
  /** The transport that received the message (e.g., 'local', 'rabbitmq') */
  readonly transport: string;
}

/**
 * Context for the status store error hook.
 */
export interface StatusStoreErrorContext {
  readonly trackingId: string;
  /** Not set for a read of all records of a tracking ID */
  readonly subscriberName?: string | undefined;
  readonly operation: StatusStoreOperation;
  readonly error: Error;
}

/**
 * Execution function passed to onWorkerWrap for APM wrapping.
 */
export type WorkerExecuteFn = () => Promise<void>;

/**
 * All available hooks for Matador.
 */
export interface MatadorHooks {
  /**
   * Logger for internal Matador logging.
   * Defaults to console logger if not provided.
   */
  logger?: Logger;

  /**
   * Called when an event is successfully enqueued.
   */
  onEnqueueSuccess?(context: EnqueueSuccessContext): void | Promise<void>;

  /**
   * Called when transport fallback occurs during send.
   * Only fires when using MultiTransport with fallbackEnabled=true.
   */
  onTransportFallback?(context: TransportFallbackContext): void | Promise<void>;

  /**
   * Called when enqueue fails completely.
   */
  onEnqueueError?(context: EnqueueErrorContext): void | Promise<void>;

  /**
   * Wraps entire worker processing (for APM context).
   * Must call execute() to run the actual processing.
   */
  onWorkerWrap?(
    envelope: Envelope,
    subscriber: SubscriberDefinition,
    execute: WorkerExecuteFn,
  ): Promise<void>;

  /**
   * Called before processing begins.
   */
  onWorkerBeforeProcess?(
    envelope: Envelope,
    subscriber: SubscriberDefinition,
  ): void | Promise<void>;

  /**
   * Called after successful processing.
   */
  onWorkerSuccess?(context: WorkerSuccessContext): void | Promise<void>;

  /**
   * Called after processing error.
   */
  onWorkerError?(context: WorkerErrorContext): void | Promise<void>;

  /**
   * Called when message decoding fails.
   */
  onDecodeError?(context: DecodeErrorContext): void | Promise<void>;

  /**
   * Called when transport connection state changes.
   */
  onConnectionStateChange?(state: ConnectionState): void | Promise<void>;

  /**
   * Loads universal metadata to add to all envelopes.
   */
  loadUniversalMetadata?():
    | Record<string, unknown>
    | Promise<Record<string, unknown>>;

  /**
   * Dynamic retry delay lookup.
   */
  getRetryDelay?(
    envelope: Envelope,
    attemptNumber: number,
  ): number | undefined | Promise<number | undefined>;

  /**
   * Dynamic max attempts lookup.
   */
  getAttempts?(
    envelope: Envelope,
  ): number | undefined | Promise<number | undefined>;

  /**
   * Dynamic max deliveries (poison threshold) lookup.
   */
  getMaxDeliveries?(
    envelope: Envelope,
  ): number | undefined | Promise<number | undefined>;

  // === Checkpoint Hooks (Resumable Subscribers) ===

  /**
   * Called when a checkpoint is loaded for a retry.
   * Only fires for resumable subscribers on retry attempts.
   */
  onCheckpointLoaded?(context: CheckpointLoadedContext): void | Promise<void>;

  /**
   * Called when an io() operation uses a cached value (checkpoint hit).
   * Useful for observability and metrics.
   */
  onCheckpointHit?(context: CheckpointHitContext): void | Promise<void>;

  /**
   * Called when an io() operation executes (cache miss).
   * Useful for observability and metrics.
   */
  onCheckpointMiss?(context: CheckpointMissContext): void | Promise<void>;

  /**
   * Called when a checkpoint is cleared.
   * Fires on successful completion or when moving to dead-letter queue.
   */
  onCheckpointCleared?(context: CheckpointClearedContext): void | Promise<void>;

  // === Status Hooks (Tracked Subscribers) ===

  /**
   * Called when a status store write or read fails, or when a record is too large.
   * Message handling does not change: the status record is for observation only.
   */
  onStatusStoreError?(context: StatusStoreErrorContext): void | Promise<void>;
}
