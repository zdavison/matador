import type { Jsonifiable } from 'type-fest';

/**
 * Unique event routing key type alias.
 */
export type EventKey = string;

/**
 * JSON-serializable record type for metadata.
 * Uses type-fest's Jsonifiable which accepts:
 * - Primitives (string, number, boolean, null)
 * - Arrays of Jsonifiable values
 * - Objects with Jsonifiable values
 * - Objects with a toJSON() method
 */
export type JsonRecord = Record<string, Jsonifiable>;

/**
 * Static properties required on Event classes for schema registration.
 */
export interface EventStatic<T = unknown> {
  /** Unique routing key for the event */
  readonly key: string;

  /** Human-readable description of the event */
  readonly description?: string;

  /** Alternative names/keys for backwards compatibility */
  readonly aliases?: readonly string[];

  /** Create an instance from data (for deserialization) */
  new (data: T): Event<T>;
}

/**
 * Base interface for all events.
 * Events represent something that happened in the system.
 */
export interface Event<T = unknown> {
  /** The event data/payload */
  readonly data: T;

  /** Event-specific metadata (merged with EventOptions metadata on dispatch) */
  readonly metadata?: JsonRecord | undefined;
}

/**
 * Options for dispatching an event.
 */
export interface EventOptions {
  /** Delay processing by this many milliseconds */
  readonly delayMs?: number | undefined;

  /** Correlation ID for request tracing */
  readonly correlationId?: string | undefined;

  /**
   * Event-specific metadata to include in the docket.
   * This metadata will be merged with:
   * 1. Event instance metadata (if defined on the event)
   * 2. Universal metadata from the loadUniversalMetadata hook
   * With EventOptions metadata taking precedence over event metadata,
   * and both taking precedence over universal metadata.
   */
  readonly metadata?: JsonRecord | undefined;

  /**
   * Whether to buffer this send for retry on reconnect if all transports fail.
   * When true (default), failed sends are held in memory and retried automatically
   * when the transport reconnects.
   * When false, failures are reported immediately in result.errors with no retry.
   *
   * @default true
   */
  readonly buffer?: boolean | undefined;

  /**
   * Whether to throw an error when a buffered failure is reported.
   * Only meaningful when buffer is true. When true, the send is buffered
   * for retry AND reported as an error so the caller is aware it failed.
   *
   * @default false
   */
  readonly throwOnBufferedFailure?: boolean | undefined;
}

/**
 * Abstract base class for creating Matador events.
 * Extend this class to define custom events.
 *
 * @example
 * ```typescript
 * class UserCreatedEvent extends MatadorEvent {
 *   static readonly key = 'user.created'
 *   static readonly description = 'Fired when a new user is created'
 *
 *   constructor(
 *     public data: { userId: string; email: string },
 *     public metadata?: JsonRecord,
 *   ) {
 *     super()
 *   }
 * }
 * ```
 */
export abstract class MatadorEvent<T = unknown> implements Event<T> {
  static readonly key: string;
  static readonly description?: string;
  static readonly aliases?: readonly string[];

  /** The event data/payload - must be defined by subclass */
  abstract readonly data: T;

  /** Event-specific metadata */
  readonly metadata?: JsonRecord | undefined;
}

/**
 * Type helper to extract the data type from an event class.
 */
export type EventData<E extends Event<unknown>> = E extends Event<infer T>
  ? T
  : never;

/**
 * Type helper to get the event class type.
 */
export type EventClass<T = unknown> = EventStatic<T> &
  (new (
    data: T,
  ) => Event<T>);
