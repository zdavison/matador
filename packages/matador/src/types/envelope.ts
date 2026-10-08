import type { Importance } from './common.js';
import type { Event, EventStatic } from './event.js';

/**
 * Message envelope containing the event data and routing/observability metadata.
 * This is the transport-agnostic message format used throughout Matador.
 */
export interface Envelope<T = unknown> {
  /** Unique message ID (UUID v4) */
  readonly id: string;

  /** The event data */
  readonly data: T;

  /** Routing, processing state, and observability metadata */
  readonly docket: Docket;
}

/**
 * Metadata associated with an envelope for routing, processing state, and observability.
 */
export interface Docket {
  // === Routing ===

  /** Event key for routing */
  readonly eventKey: string;

  /** Human-readable description of the event (for observability/logging) */
  readonly eventDescription?: string | undefined;

  /** Target subscriber name for 1:1 routing */
  readonly targetSubscriber: string;

  /** Original queue before any dead-letter routing */
  originalQueue?: string | undefined;

  /** Scheduled processing time for delayed messages (ISO 8601 string) */
  scheduledFor?: string | undefined;

  // === Processing State ===

  /**
   * Attempt counter managed by Matador (1-based).
   * Incremented on each retry. Used when transport doesn't track attempts.
   */
  attempts: number;

  /** When the envelope was first created (ISO 8601 string) */
  readonly createdAt: string;

  /** Error message from first failure (for debugging) */
  firstError?: string | undefined;

  /** Error message from most recent failure */
  lastError?: string | undefined;

  // === Observability ===

  /** Importance level for monitoring */
  readonly importance: Importance;

  /** Correlation ID for request tracing */
  readonly correlationId?: string | undefined;

  /**
   * Tracking ID for status records. Set only on envelopes for subscribers with track: true.
   * One send has one tracking ID for all of its tracked subscribers.
   */
  readonly trackingId?: string | undefined;

  /**
   * Custom metadata provided by the application.
   * This is the merged result of universal metadata (from loadUniversalMetadata hook)
   * and event-specific metadata (from dispatch options). Event-specific metadata
   * overrides universal metadata when keys conflict.
   */
  readonly metadata?: Record<string, unknown> | undefined;
}

/**
 * Fields from Docket that can be specified when creating an envelope.
 */
type DocketCreateFields = Pick<
  Docket,
  | 'eventKey'
  | 'eventDescription'
  | 'targetSubscriber'
  | 'importance'
  | 'correlationId'
  | 'trackingId'
>;

/**
 * Options for creating an envelope.
 */
export interface CreateEnvelopeOptions<T> extends DocketCreateFields {
  /** Optional custom ID (defaults to UUID v4) */
  readonly id?: string | undefined;

  /** The event data */
  readonly data: T;

  /**
   * Event-specific metadata to include in the docket.
   * Will be merged with universal metadata, with these values taking precedence.
   */
  readonly metadata?: Record<string, unknown> | undefined;

  /**
   * Universal metadata loaded from the loadUniversalMetadata hook.
   * This is provided by the fanout engine, not by the caller.
   * @internal
   */
  readonly universalMetadata?: Record<string, unknown> | undefined;

  /** Delay processing by this many milliseconds */
  readonly delayMs?: number | undefined;
}

/**
 * Creates a new envelope with the provided options.
 */
export function createEnvelope<T>(
  options: CreateEnvelopeOptions<T>,
): Envelope<T> {
  const now = new Date().toISOString();

  // Merge universal metadata with event-specific metadata
  // Event-specific metadata takes precedence
  const mergedMetadata =
    options.universalMetadata || options.metadata
      ? { ...options.universalMetadata, ...options.metadata }
      : undefined;

  return {
    id: options.id ?? crypto.randomUUID(),
    data: options.data,
    docket: {
      // Routing
      eventKey: options.eventKey,
      ...(options.eventDescription !== undefined && {
        eventDescription: options.eventDescription,
      }),
      targetSubscriber: options.targetSubscriber,
      ...(options.delayMs !== undefined &&
        options.delayMs > 0 && {
          scheduledFor: new Date(Date.now() + options.delayMs).toISOString(),
        }),
      // Processing state
      attempts: 1,
      createdAt: now,
      // Observability
      importance: options.importance,
      ...(options.correlationId !== undefined && {
        correlationId: options.correlationId,
      }),
      ...(options.trackingId !== undefined && {
        trackingId: options.trackingId,
      }),
      ...(mergedMetadata !== undefined && { metadata: mergedMetadata }),
    },
  };
}

/**
 * Options for creating a dummy envelope.
 * All fields are optional and will use sensible defaults if not provided.
 */
export interface CreateDummyEnvelopeOptions {
  readonly id?: string | undefined;
  readonly eventKey?: string | undefined;
  readonly eventDescription?: string | undefined;
  readonly targetSubscriber?: string | undefined;
  readonly importance?: Importance | undefined;
  readonly correlationId?: string | undefined;
  readonly metadata?: Record<string, unknown> | undefined;
  readonly delayMs?: number | undefined;
}

/**
 * Type helper to extract the data type from an Event, or return T as-is.
 */
type ExtractData<T> = T extends Event<infer D> ? D : T;

/**
 * Checks if a value is a Matador Event (has data property and constructor with key).
 */
function isEvent<T>(value: unknown): value is Event<T> {
  return (
    typeof value === 'object' &&
    value !== null &&
    'data' in value &&
    typeof (value.constructor as EventStatic).key === 'string'
  );
}

/**
 * Helper to create a test envelope for a given event instance or raw data.
 * Useful for unit testing subscriber callbacks directly.
 *
 * When passing an Event instance, the eventKey and eventDescription will be
 * automatically extracted from the event class (unless overridden in options).
 *
 * @example With raw data
 * ```typescript
 * const envelope = createDummyEnvelope({ userId: '123', email: 'test@example.com' });
 * ```
 *
 * @example With an Event instance
 * ```typescript
 * const event = new UserCreatedEvent({ userId: '123', email: 'test@example.com' });
 * const envelope = createDummyEnvelope(event);
 * // eventKey is automatically set to UserCreatedEvent.key
 * await mySubscriber.callback(envelope, event.data);
 * ```
 *
 * @example With options
 * ```typescript
 * const event = new UserCreatedEvent({ userId: '123', email: 'test@example.com' });
 * const envelope = createDummyEnvelope(event, {
 *   metadata: { traceId: 'abc-123' },
 *   correlationId: 'request-456',
 * });
 * ```
 */
export function createDummyEnvelope<T>(
  dataOrEvent: T,
  options?: CreateDummyEnvelopeOptions,
): Envelope<ExtractData<T>> {
  let data: ExtractData<T>;
  let eventKey = options?.eventKey ?? 'dummy.event.key';
  let eventDescription = options?.eventDescription;

  if (isEvent(dataOrEvent)) {
    data = dataOrEvent.data as ExtractData<T>;
    const eventStatic = dataOrEvent.constructor as EventStatic;
    eventKey = options?.eventKey ?? eventStatic.key;
    eventDescription = options?.eventDescription ?? eventStatic.description;
  } else {
    data = dataOrEvent as ExtractData<T>;
  }

  return createEnvelope({
    data,
    eventKey,
    eventDescription,
    targetSubscriber: options?.targetSubscriber ?? 'dummy-subscriber',
    importance: options?.importance ?? 'can-ignore',
    id: options?.id,
    correlationId: options?.correlationId,
    metadata: options?.metadata,
    delayMs: options?.delayMs,
  });
}
