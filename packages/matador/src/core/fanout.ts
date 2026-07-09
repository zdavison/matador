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
 * Number of consecutive flush failures tolerated within a single flush pass
 * before bailing out early and re-buffering the untried remainder.
 */
const MAX_CONSECUTIVE_FLUSH_FAILURES = 10;

/**
 * A send that failed (all transports exhausted) and was held in-memory for retry on reconnect.
 */
interface BufferedSend {
  readonly queue: string;
  readonly envelope: Envelope;
  readonly sendOptions: SendOptions | undefined;
  readonly subscriberName: string;
  /** Number of retry attempts made since this was first buffered. */
  attempts: number;
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

  /**
   * Maximum number of retry attempts for a buffered message before it's
   * dropped and reported via onEnqueueError, instead of being retried forever.
   *
   * @default undefined (no limit — retries until the buffer flushes successfully)
   */
  readonly maxRetryAttempts?: number | undefined;

  /**
   * How often (in ms) to attempt flushing the retry buffer, independent of
   * transport reconnect events. This covers the case where the transport
   * stays connected but an individual publish keeps failing (e.g. broker
   * nack, publish confirm timeout) — onConnected never fires again for
   * that, so nothing would otherwise retry it.
   *
   * @default 30000. Pass 0 to disable the interval and rely on
   * onConnected/manual flushes only.
   */
  readonly retryIntervalMs?: number | undefined;
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
  private readonly maxRetryAttempts: number | undefined;
  private readonly disposeOnConnected: (() => void) | undefined;
  private readonly retryTimer: ReturnType<typeof setInterval> | undefined;

  constructor(config: FanoutConfig) {
    this.transport = config.transport;
    this.schema = config.schema;
    this.hooks = config.hooks;
    this.topology = config.topology;
    this.defaultQueue = config.defaultQueue;
    this.maxRetryBufferSize = config.maxRetryBufferSize ?? 5000;
    this.maxRetryAttempts = config.maxRetryAttempts;

    // If the transport supports it, add a callback to flush the retry buffer when the transport reconnects
    // The returned value is a function to unsubscribe from the callback
    this.disposeOnConnected = this.transport.onConnected?.(() => {
      void this.flushRetryBuffer();
    });

    // Also flush periodically: onConnected only fires on connection-level
    // reconnects, so it never fires when the transport stays connected but
    // an individual publish keeps failing (e.g. broker nack, confirm timeout).
    const retryIntervalMs = config.retryIntervalMs ?? 30_000;
    if (retryIntervalMs > 0) {
      this.retryTimer = setInterval(() => {
        void this.flushRetryBuffer();
      }, retryIntervalMs);
      this.retryTimer.unref?.();
    }
  }

  dispose(): void {
    // Unsubscribe from the callback (that flushes the retry buffer when the transport reconnects)
    this.disposeOnConnected?.();
    clearInterval(this.retryTimer);
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
            attempts: 0,
          });
          this.hooks.logger.warn(
            `[Matador] 🟡 Message for '${subscriber.name}' buffered for retry on reconnect.`,
            {
              queue: qualifiedQueue,
              subscriberName: subscriber.name,
              bufferSize: this.retryBuffer.length,
              maxBufferSize: this.maxRetryBufferSize,
            },
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
   * Attempts a single buffered item during a flush pass
   * @returns true if the attempt succeeded; false on failure
   */
  private async attemptFlushItem(item: BufferedSend): Promise<boolean> {
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
      return true;
    } catch (error) {
      await this.handleFlushFailure(item, error);
      return false;
    } finally {
      this.enqueuingCount--;
    }
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
    let consecutiveFailures = 0;

    for (let i = 0; i < toFlush.length; i++) {
      const item = toFlush[i];
      if (!item) continue;

      const succeeded = await this.attemptFlushItem(item);

      // A success means the transport is actually working, so it resets the
      // streak — isolated failures scattered across a healthy pass shouldn't
      // trip the breaker, only a sustained run of them should.
      consecutiveFailures = succeeded ? 0 : consecutiveFailures + 1;

      if (consecutiveFailures >= MAX_CONSECUTIVE_FLUSH_FAILURES) {
        // A run of failures this long is more likely a systemic/connection-level
        // issue (broker down, slow, rejecting everything) than something specific
        // to these messages.
        // Stopping early so that we don't retry 5k messages for nothing
        const untried = toFlush.slice(i + 1);
        if (untried.length > 0) {
          await this.rebufferUntried(untried, consecutiveFailures);
        }
        break;
      }
    }

    const remaining = this.retryBuffer.length;
    if (remaining > 0) {
      this.hooks.logger.warn(
        `[Matador] 🟡 ${remaining} buffered message(s) could not be flushed; will retry later.`,
      );
    } else {
      this.hooks.logger.info(
        '[Matador] 🟢 All buffered messages flushed successfully.',
      );
    }
  }

  /**
   * Re-buffers messages that were never attempted because a flush pass
   * stopped out early. Respects maxRetryBufferSize: concurrent sends can have
   * refilled the buffer while this flush was running, so anything beyond
   * remaining capacity is dropped and reported instead of silently growing
   * the buffer past its cap.
   */
  private async rebufferUntried(
    untried: BufferedSend[],
    consecutiveFailures: number,
  ): Promise<void> {
    const capacity = Math.max(
      0,
      this.maxRetryBufferSize - this.retryBuffer.length,
    );
    const toRebuffer = untried.slice(0, capacity);
    const dropped = untried.slice(capacity);

    if (toRebuffer.length > 0) {
      this.retryBuffer.unshift(...toRebuffer);
    }

    const droppedSuffix =
      dropped.length > 0 ? `, ${dropped.length} dropped (buffer full).` : '.';
    this.hooks.logger.warn(
      `[Matador] 🟡 Stopping this flush pass after ${consecutiveFailures} consecutive failures; ${toRebuffer.length} untried message(s) re-buffered for the next attempt${droppedSuffix}`,
    );

    for (const item of dropped) {
      this.hooks.logger.error(
        `[Matador] 🔴 Retry buffer full (${this.maxRetryBufferSize}). Message for '${item.subscriberName}' dropped and will not be retried.`,
      );
      const err = new TransportSendError(
        item.queue,
        new Error('Retry buffer full'),
      );
      await this.hooks.onEnqueueError({
        envelope: item.envelope,
        error: err,
        transport: this.transport.name,
      });
    }
  }

  /**
   * Handles a failed flush attempt for a single buffered item: drops it once
   * maxRetryAttempts is exceeded, otherwise re-buffers it (unless the buffer
   * is full, in which case it's dropped too).
   */
  private async handleFlushFailure(
    item: BufferedSend,
    error: unknown,
  ): Promise<void> {
    item.attempts++;
    const cause = error instanceof Error ? error : new Error(String(error));
    const exceededAttempts =
      this.maxRetryAttempts !== undefined &&
      item.attempts >= this.maxRetryAttempts;

    if (
      !exceededAttempts &&
      this.retryBuffer.length < this.maxRetryBufferSize
    ) {
      // Re-buffer on failure; it will be retried on the next reconnect or flush interval.
      this.retryBuffer.push(item);
      return;
    }

    if (exceededAttempts) {
      this.hooks.logger.error(
        `[Matador] 🔴 Message for '${item.subscriberName}' exceeded max retry attempts (${this.maxRetryAttempts}) and will not be retried further.`,
      );
    }

    const err = new TransportSendError(item.queue, cause);
    await this.hooks.onEnqueueError({
      envelope: item.envelope,
      error: err,
      transport: this.transport.name,
    });
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
