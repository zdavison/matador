import type { Docket, Envelope } from '../types/index.js';
import { CodecDecodeError } from './codec.js';
import type { EncodedMessage, HeaderAwareCodec } from './header-aware-codec.js';

/**
 * Header names used by Matador in RabbitMQ messages.
 */
const HEADERS = {
  // Routing
  EVENT_KEY: 'x-matador-event-key',
  TARGET_SUBSCRIBER: 'x-matador-subscriber',
  SCHEDULED_FOR: 'x-matador-scheduled-for',
  ORIGINAL_QUEUE: 'x-matador-original-queue',

  // Processing state
  ATTEMPTS: 'x-matador-attempts',
  CREATED_AT: 'x-matador-created-at',
  FIRST_ERROR: 'x-matador-first-error',
  LAST_ERROR: 'x-matador-last-error',

  // Observability
  IMPORTANCE: 'x-matador-importance',
  CORRELATION_ID: 'x-matador-correlation-id',
  TRACKING_ID: 'x-matador-tracking-id',

  // v1 compatibility headers
  V1_EVENT_ID: 'x-event-id',
  V1_CORRELATION_ID: 'x-correlation-id',
  V1_USER_ID: 'x-user-id',
} as const;

/**
 * Body structure for v2 messages.
 * Only contains id, data, and optionally metadata (since it can be large).
 */
interface V2Body {
  readonly id: string;
  readonly data: unknown;
  readonly metadata?: Record<string, unknown>;
}

/**
 * v1 message format (for backwards compatibility).
 */
interface V1Body {
  key: string;
  data: unknown;
  metadata: unknown;
  universal?: {
    event_id?: string | null;
    user_id?: string | null;
    correlation_id?: string | null;
    [key: string]: unknown;
  };
  before?: unknown;
  options?: { delayMs?: number };
  targetSubscriber: string;
}

/**
 * RabbitMQ-specific codec that stores routing/metadata in headers.
 *
 * Benefits:
 * - No duplication between body and headers
 * - RabbitMQ can route/filter based on headers
 * - Smaller message body
 * - v1 message format compatibility built-in
 */
export class RabbitMQCodec implements HeaderAwareCodec {
  readonly contentType = 'application/json';

  encode(envelope: Envelope): EncodedMessage {
    const { docket } = envelope;

    // Body only contains id, data, and metadata (since metadata can be large)
    const body: V2Body = {
      id: envelope.id,
      data: envelope.data,
      ...(docket.metadata !== undefined && { metadata: docket.metadata }),
    };

    // All docket fields go in headers
    const headers: Record<string, unknown> = {
      // Routing
      [HEADERS.EVENT_KEY]: docket.eventKey,
      [HEADERS.TARGET_SUBSCRIBER]: docket.targetSubscriber,

      // Processing state
      [HEADERS.ATTEMPTS]: docket.attempts,
      [HEADERS.CREATED_AT]: docket.createdAt,

      // Observability
      [HEADERS.IMPORTANCE]: docket.importance,
    };

    // Optional fields
    if (docket.scheduledFor !== undefined) {
      headers[HEADERS.SCHEDULED_FOR] = docket.scheduledFor;
    }
    if (docket.originalQueue !== undefined) {
      headers[HEADERS.ORIGINAL_QUEUE] = docket.originalQueue;
    }
    if (docket.firstError !== undefined) {
      headers[HEADERS.FIRST_ERROR] = docket.firstError;
    }
    if (docket.lastError !== undefined) {
      headers[HEADERS.LAST_ERROR] = docket.lastError;
    }
    if (docket.correlationId !== undefined) {
      headers[HEADERS.CORRELATION_ID] = docket.correlationId;
    }
    if (docket.trackingId !== undefined) {
      headers[HEADERS.TRACKING_ID] = docket.trackingId;
    }

    return {
      body: new TextEncoder().encode(JSON.stringify(body)),
      headers,
      contentType: this.contentType,
    };
  }

  decode(body: Uint8Array, headers: Record<string, unknown>): Envelope {
    if (body.length === 0) {
      throw new CodecDecodeError('Cannot decode empty buffer');
    }

    let json: string;
    try {
      json = new TextDecoder('utf-8').decode(body);
    } catch (error) {
      throw new CodecDecodeError('Invalid UTF-8 encoding', error);
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(json);
    } catch (error) {
      throw new CodecDecodeError('Invalid JSON', error);
    }

    // Detect format: v1 has 'key' and 'targetSubscriber' at top level
    if (this.isV1Body(parsed)) {
      return this.decodeV1(parsed, headers);
    }

    // v2 format
    if (!this.isV2Body(parsed)) {
      throw new CodecDecodeError('Invalid message body structure');
    }

    return this.decodeV2(parsed, headers);
  }

  private isV1Body(value: unknown): value is V1Body {
    if (typeof value !== 'object' || value === null) return false;
    const obj = value as Record<string, unknown>;
    return (
      typeof obj.key === 'string' &&
      typeof obj.targetSubscriber === 'string' &&
      'data' in obj &&
      !('payload' in obj)
    );
  }

  private isV2Body(value: unknown): value is V2Body {
    if (typeof value !== 'object' || value === null) return false;
    const obj = value as Record<string, unknown>;
    return (
      typeof obj.id === 'string' && 'data' in obj && !('key' in obj) // Distinguish from v1 which also has 'data'
    );
  }

  private decodeV2(body: V2Body, headers: Record<string, unknown>): Envelope {
    // Required headers
    const eventKey = this.requireStringHeader(headers, HEADERS.EVENT_KEY);
    const targetSubscriber = this.requireStringHeader(
      headers,
      HEADERS.TARGET_SUBSCRIBER,
    );
    const attempts = this.requireNumberHeader(headers, HEADERS.ATTEMPTS);
    const createdAt = this.requireStringHeader(headers, HEADERS.CREATED_AT);
    const importance = this.requireStringHeader(headers, HEADERS.IMPORTANCE);

    // Optional headers
    const scheduledFor = this.optionalStringHeader(
      headers,
      HEADERS.SCHEDULED_FOR,
    );
    const originalQueue = this.optionalStringHeader(
      headers,
      HEADERS.ORIGINAL_QUEUE,
    );
    const firstError = this.optionalStringHeader(headers, HEADERS.FIRST_ERROR);
    const lastError = this.optionalStringHeader(headers, HEADERS.LAST_ERROR);
    const correlationId = this.optionalStringHeader(
      headers,
      HEADERS.CORRELATION_ID,
    );
    const trackingId = this.optionalStringHeader(headers, HEADERS.TRACKING_ID);

    const docket: Docket = {
      // Routing
      eventKey,
      targetSubscriber,
      ...(scheduledFor !== undefined && { scheduledFor }),
      ...(originalQueue !== undefined && { originalQueue }),
      // Processing state
      attempts,
      createdAt,
      ...(firstError !== undefined && { firstError }),
      ...(lastError !== undefined && { lastError }),
      // Observability
      importance: importance as Docket['importance'],
      ...(correlationId !== undefined && { correlationId }),
      ...(trackingId !== undefined && { trackingId }),
      ...(body.metadata !== undefined && { metadata: body.metadata }),
    };

    return {
      id: body.id,
      data: body.data,
      docket,
    };
  }

  /**
   * Decodes a v1 format message for backwards compatibility.
   * v1 format: { key, data, metadata, universal, targetSubscriber, ... }
   */
  private decodeV1(body: V1Body, headers: Record<string, unknown>): Envelope {
    const { event_id, user_id, correlation_id, ...otherUniversal } =
      body.universal ?? {};

    // Merge metadata: event metadata + remaining universal fields
    const mergedMetadata: Record<string, unknown> = {};
    if (body.metadata && typeof body.metadata === 'object') {
      Object.assign(mergedMetadata, body.metadata);
    }
    if (user_id !== undefined && user_id !== null) {
      mergedMetadata.user_id = user_id;
    }
    Object.assign(mergedMetadata, otherUniversal);

    // Try to get attempts from header (v1 also uses x-matador-attempts)
    const attempts =
      typeof headers[HEADERS.ATTEMPTS] === 'number'
        ? (headers[HEADERS.ATTEMPTS] as number)
        : 1;

    // Try to get importance from header
    const importance =
      typeof headers[HEADERS.IMPORTANCE] === 'string'
        ? (headers[HEADERS.IMPORTANCE] as string)
        : 'should-investigate';

    // Get correlation ID from either v1 or v2 header
    const correlationId =
      (headers[HEADERS.CORRELATION_ID] as string | undefined) ??
      (headers[HEADERS.V1_CORRELATION_ID] as string | undefined) ??
      correlation_id ??
      undefined;

    // Get event ID from v1 header or body
    const eventId =
      (headers[HEADERS.V1_EVENT_ID] as string | undefined) ??
      event_id ??
      crypto.randomUUID();

    const now = new Date().toISOString();

    // Calculate scheduledFor if delayMs was specified (though it's likely already passed)
    let scheduledFor: string | undefined;
    if (body.options?.delayMs) {
      scheduledFor = new Date(Date.now() + body.options.delayMs).toISOString();
    }

    const docket: Docket = {
      // Routing
      eventKey: body.key,
      targetSubscriber: body.targetSubscriber,
      ...(scheduledFor !== undefined && { scheduledFor }),
      // Processing state
      attempts,
      createdAt: now,
      // Observability
      importance: importance as Docket['importance'],
      ...(correlationId !== undefined && { correlationId }),
      ...(Object.keys(mergedMetadata).length > 0 && {
        metadata: mergedMetadata,
      }),
    };

    return {
      id: eventId,
      data: body.data,
      docket,
    };
  }

  private requireStringHeader(
    headers: Record<string, unknown>,
    name: string,
  ): string {
    const value = headers[name];
    if (value === undefined) {
      throw new CodecDecodeError(`Missing required header: ${name}`);
    }
    if (typeof value !== 'string') {
      throw new CodecDecodeError(
        `Header ${name} must be string, got ${typeof value}`,
      );
    }
    return value;
  }

  private requireNumberHeader(
    headers: Record<string, unknown>,
    name: string,
  ): number {
    const value = headers[name];
    if (value === undefined) {
      throw new CodecDecodeError(`Missing required header: ${name}`);
    }
    if (typeof value !== 'number') {
      throw new CodecDecodeError(
        `Header ${name} must be number, got ${typeof value}`,
      );
    }
    return value;
  }

  private optionalStringHeader(
    headers: Record<string, unknown>,
    name: string,
  ): string | undefined {
    const value = headers[name];
    if (value === undefined) return undefined;
    if (typeof value !== 'string') return undefined;
    return value;
  }
}
