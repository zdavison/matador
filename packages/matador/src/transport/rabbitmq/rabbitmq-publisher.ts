import { randomUUID } from 'node:crypto';
import type { ConfirmChannel, Message, Options } from 'amqplib';
import { TransportSendError } from '../../errors/index.js';

/** Tracks confirms, returns, and flow control for a single publish channel. */
export class RabbitMQPublisher {
  private readonly pending = new Map<string, (error?: Error) => void>();
  private readonly queued = new Map<string, () => void>();
  private writable = true;
  private failure: Error | undefined;

  constructor(
    readonly channel: ConfirmChannel,
    private readonly timeoutMs: number,
  ) {
    channel.on('return', (message: Message) => {
      const id: unknown = message.properties.correlationId;
      if (typeof id === 'string') {
        this.pending.get(id)?.(
          new Error('RabbitMQ returned an unroutable publish'),
        );
      }
    });
    channel.on('drain', () => {
      this.writable = true;
      for (const publish of this.queued.values()) {
        if (!this.writable) break;
        publish();
      }
    });
    channel.on('error', (error: Error) => this.fail(error));
    channel.on('close', () =>
      this.fail(new Error('RabbitMQ publish channel closed')),
    );
  }

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Options.Publish,
  ): Promise<void> {
    if (this.failure) {
      return Promise.reject(new TransportSendError(routingKey, this.failure));
    }

    // A single envelope can be published more than once, including to multiple
    // transports and queues. Give each attempt its own return correlation ID.
    const id = randomUUID();
    return new Promise<void>((resolve, reject) => {
      const settle = (error?: Error) => {
        if (!this.pending.delete(id)) return;
        this.queued.delete(id);
        clearTimeout(timeout);
        if (error) reject(new TransportSendError(routingKey, error));
        else resolve();
      };
      const timeout = setTimeout(() => {
        settle(
          new Error(
            `Publish not confirmed by broker within ${this.timeoutMs}ms`,
          ),
        );
      }, this.timeoutMs);
      this.pending.set(id, settle);

      const publish = () => {
        this.queued.delete(id);
        try {
          // For mandatory publishes, RabbitMQ emits basic.return before the
          // confirm. Once returned, the confirm cannot settle this attempt.
          this.writable = this.channel.publish(
            exchange,
            routingKey,
            content,
            options.mandatory ? { ...options, correlationId: id } : options,
            (error: Error | null) => settle(error ?? undefined),
          );
        } catch (error) {
          settle(error instanceof Error ? error : new Error(String(error)));
        }
      };
      if (this.writable) publish();
      else this.queued.set(id, publish);
    });
  }

  /** Settles every pending confirm and drops writes waiting for drain. */
  fail(error: Error): void {
    this.failure ??= error;
    for (const settle of this.pending.values()) settle(error);
  }
}
