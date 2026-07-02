import { TransportSendError } from '../errors/index.js';
import type { SafeHooks } from '../hooks/index.js';
import type { SchemaRegistry } from '../schema/index.js';
import type { Topology } from '../topology/index.js';
import { resolveTargetQueueName } from '../topology/index.js';
import type { SendOptions, Transport } from '../transport/index.js';
import type {
  AnySubscriber,
  Envelope,
  Event,
  EventClass,
  EventOptions,
} from '../types/index.js';
import { createEnvelope } from '../types/index.js';

/**
 * A send that failed (all transports exhausted) and was held in-memory for retry on reconnect.
 */
interface BufferedSend {
  readonly queue: string;
  readonly envelope: Envelope;
  readonly sendOptions: SendOptions | undefined;
  readonly subscriberName: string;
}

/**
 * Configuration for the fanout engine.
 */
export interface FanoutConfig {
  readonly transport: Transport;
  readonly schema: SchemaRegistry;
  readonly hooks: SafeHooks;
  readonly topology: Topology;
  readonly defaultQueue: string;
  readonly maxRetryBufferSize?: number | undefined;
}

/**
 * Result of sending an event.
 */
export interface SendResult {
  readonly eventKey: string;
  readonly subscribersSent: number;
  readonly subscribersSkipped: number;
  readonly errors: readonly SendError[];
}

/**
 * Error during send.
 */
export interface SendError {
  readonly subscriberName: string;
  readonly queue: string;
  readonly error: Error;
}

/**
 * Engine for fanning out events to subscribers.
 *
 * Handles:
 * 1. Getting subscribers from schema
 * 2. Filtering by enabled() hook
 * 3. Creating envelopes for each subscriber
 * 4. Sending to appropriate queues via transport
 */
export class FanoutEngine {
  private readonly transport: Transport;
  private readonly schema: SchemaRegistry;
  private readonly hooks: SafeHooks;
  private readonly topology: Topology;
  private readonly defaultQueue: string;
  private enqueuingCount = 0;
  private readonly retryBuffer: BufferedSend[] = [];
  private readonly maxRetryBufferSize: number;
  private readonly disposeOnConnected: (() => void) | undefined;

  constructor(config: FanoutConfig) {
    this.transport = config.transport;
    this.schema = config.schema;
    this.hooks = config.hooks;
    this.topology = config.topology;
    this.defaultQueue = config.defaultQueue;
    this.maxRetryBufferSize = config.maxRetryBufferSize ?? 5000;

    // If the transport supports it, add a callback to flush the retry buffer when the transport reconnects
    // The returned value is a function to unsubscribe from the callback
    this.disposeOnConnected = this.transport.onConnected?.(() => {
      void this.flushRetryBuffer();
    });
  }

  dispose(): void {
    // Unsubscribe from the callback (that flushes the retry buffer when the transport reconnects)
    this.disposeOnConnected?.();
  }

  /**
   * Current count of events being enqueued.
   */
  get eventsBeingEnqueuedCount(): number {
    return this.enqueuingCount;
  }

  /**
   * Sends an event to all registered subscribers.
   */
  async send<T>(
    eventClass: EventClass<T>,
    event: Event<T>,
    options: EventOptions = {},
  ): Promise<SendResult> {
    const eventKey = eventClass.key;
    const subscribers = this.schema.getSubscribers(eventKey);

    const errors: SendError[] = [];
    let sent = 0;
    let skipped = 0;

    // Load universal metadata
    const universalMetadata = await this.hooks.loadUniversalMetadata();

    // Merge event.metadata with options.metadata (options takes precedence)
    const mergedMetadata =
      event.metadata || options.metadata
        ? { ...event.metadata, ...options.metadata }
        : undefined;

    for (const subscriber of subscribers) {
      // Check if subscriber is enabled
      const enabled = await this.isSubscriberEnabled(subscriber);
      if (!enabled) {
        skipped++;
        continue;
      }

      // Determine target queue
      const targetQueue = subscriber.targetQueue ?? this.defaultQueue;
      const qualifiedQueue = resolveTargetQueueName(this.topology, targetQueue);

      // Create envelope
      const envelope = createEnvelope({
        eventKey,
        eventDescription: eventClass.description,
        targetSubscriber: subscriber.name,
        data: event.data,
        importance: subscriber.importance ?? 'should-investigate',
        correlationId: options.correlationId,
        metadata: mergedMetadata,
        universalMetadata,
        delayMs: options.delayMs,
      });

      // Send to transport
      const sendOptions: SendOptions | undefined =
        options.delayMs !== undefined ? { delay: options.delayMs } : undefined;

      this.enqueuingCount++;
      try {
        const usedTransport = await this.transport.send(
          qualifiedQueue,
          envelope,
          sendOptions,
        );
        sent++;

        await this.hooks.onEnqueueSuccess({
          envelope,
          queue: qualifiedQueue,
          transport: usedTransport,
        });
      } catch (error) {
        const cause = error instanceof Error ? error : new Error(String(error));
        const shouldBuffer = options.buffer !== false;

        if (shouldBuffer && this.retryBuffer.length < this.maxRetryBufferSize) {
          this.retryBuffer.push({
            queue: qualifiedQueue,
            envelope,
            sendOptions,
            subscriberName: subscriber.name,
          });
          this.hooks.logger.warn(
            `[Matador] 🟡 Message for '${subscriber.name}' buffered for retry on reconnect (buffer: ${this.retryBuffer.length}/${this.maxRetryBufferSize}).`,
          );

          if (options.throwOnBufferedFailure) {
            const err = new TransportSendError(qualifiedQueue, cause);
            // By adding the error, the main Send in Matador will throw
            errors.push({
              subscriberName: subscriber.name,
              queue: qualifiedQueue,
              error: err,
            });
            await this.hooks.onEnqueueError({
              envelope,
              error: err,
              transport: this.transport.name,
            });
          }
        } else {
          if (shouldBuffer) {
            this.hooks.logger.error(
              `[Matador] 🔴 Retry buffer full (${this.maxRetryBufferSize}). Message for '${subscriber.name}' dropped and will not be retried.`,
            );
          }
          const err = new TransportSendError(qualifiedQueue, cause);
          errors.push({
            subscriberName: subscriber.name,
            queue: qualifiedQueue,
            error: err,
          });

          await this.hooks.onEnqueueError({
            envelope,
            error: err,
            transport: this.transport.name,
          });
        }
      } finally {
        this.enqueuingCount--;
      }
    }

    return {
      eventKey,
      subscribersSent: sent,
      subscribersSkipped: skipped,
      errors,
    };
  }

  /**
   * Flushes the retry buffer
   *
   * This is called when the transport reconnects, and is used to retry any messages that were buffered while the transport was disconnected
   */
  private async flushRetryBuffer(): Promise<void> {
    if (this.retryBuffer.length === 0) return;

    this.hooks.logger.info(
      `[Matador] ⏳ Flushing ${this.retryBuffer.length} buffered message(s)...`,
    );

    // Drain the buffer atomically so concurrent flush calls don't double-send.
    const toFlush = this.retryBuffer.splice(0);

    for (const item of toFlush) {
      this.enqueuingCount++;
      try {
        const usedTransport = await this.transport.send(
          item.queue,
          item.envelope,
          item.sendOptions,
        );
        await this.hooks.onEnqueueSuccess({
          envelope: item.envelope,
          queue: item.queue,
          transport: usedTransport,
        });
      } catch (error) {
        // Re-buffer on failure; it will be retried on the next reconnect.
        if (this.retryBuffer.length < this.maxRetryBufferSize) {
          this.retryBuffer.push(item);
        } else {
          const cause =
            error instanceof Error ? error : new Error(String(error));
          const err = new TransportSendError(item.queue, cause);
          await this.hooks.onEnqueueError({
            envelope: item.envelope,
            error: err,
            transport: this.transport.name,
          });
        }
      } finally {
        this.enqueuingCount--;
      }
    }

    const remaining = this.retryBuffer.length;
    if (remaining > 0) {
      this.hooks.logger.warn(
        `[Matador] 🟡 ${remaining} buffered message(s) could not be flushed; will retry on next reconnect.`,
      );
    } else {
      this.hooks.logger.info(
        '[Matador] 🟢 All buffered messages flushed successfully.',
      );
    }
  }

  private async isSubscriberEnabled(
    subscriber: AnySubscriber,
  ): Promise<boolean> {
    if (!subscriber.enabled) {
      return true;
    }

    try {
      const result = await subscriber.enabled();
      return result;
    } catch {
      // If enabled check fails, consider it enabled
      return true;
    }
  }
}
