import type {
  Channel,
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage,
  Options,
} from 'amqplib';
import amqplib from 'amqplib';
import { RabbitMQCodec } from '../../codec/rabbitmq-codec.js';
import {
  DelayedMessagesNotSupportedError,
  TransportNotConnectedError,
  TransportSendError,
} from '../../errors/index.js';
import { type Logger, consoleLogger } from '../../hooks/index.js';
import type { QueueDefinition, Topology } from '../../topology/types.js';
import {
  getDeadLetterQueueName,
  getRetryQueueName,
  resolveQueueName,
} from '../../topology/types.js';
import type { Envelope } from '../../types/index.js';
import type { TransportCapabilities } from '../capabilities.js';
import {
  ConnectionManager,
  type ConnectionManagerConfig,
} from '../connection-manager.js';
import type {
  MessageHandler,
  MessageReceipt,
  SendOptions,
  SubscribeOptions,
  Subscription,
  Transport,
} from '../transport.js';

/**
 * Configuration options for the RabbitMQ transport.
 */
export interface RabbitMQTransportConfig {
  /** RabbitMQ connection URL */
  readonly url: string;

  /** Connection name displayed in RabbitMQ management UI */
  readonly connectionName: string;

  /** Connection manager configuration */
  readonly connection?: Partial<ConnectionManagerConfig> | undefined;

  /** Use quorum queues for durability (default: true) */
  readonly quorumQueues?: boolean | undefined;

  /** Default prefetch count per consumer (default: 10) */
  readonly defaultPrefetch?: number | undefined;

  /** Enable the delayed message exchange plugin if available (default: true) */
  readonly enableDelayedMessages?: boolean | undefined;

  /**
   * How long to wait for the broker to confirm a publish before failing,
   * in milliseconds (default: 5000).
   *
   * Publishes use a confirm channel: `send()` resolves only once the broker
   * acks the message, and rejects on nack or timeout, so callers can await
   * delivery confirmation rather than treating publishes as fire-and-forget.
   */
  readonly publishTimeoutMs?: number | undefined;

  /** Logger for transport events (defaults to console) */
  readonly logger?: Logger | undefined;
}

/**
 * Internal structure for tracking a queue's dedicated channel.
 */
interface QueueChannel {
  readonly channel: Channel;
  readonly consumers: ActiveConsumer[];
}

/**
 * Internal structure for tracking active consumers.
 */
interface ActiveConsumer {
  readonly consumerTag: string;
  readonly queue: string;
  active: boolean;
}

/**
 * Redacts credentials from an AMQP URL.
 * Replaces username and password with '****' regardless of their length.
 *
 * @example
 * redactAmqpUrl('amqp://user:pass@host:5672') // 'amqp://****:****@host:5672'
 */
export function redactAmqpUrl(url: string): string {
  const regex = /^(amqps?:\/\/)[^:]+:[^@]+@/;
  return url.replace(regex, '$1****:****@');
}

/**
 * RabbitMQ transport implementation using amqplib.
 */
export class RabbitMQTransport implements Transport {
  readonly name = 'rabbitmq';

  private _capabilities: TransportCapabilities = {
    deliveryModes: ['at-least-once'],
    delayedMessages: false,
    deadLetterRouting: 'native',
    attemptTracking: true,
    concurrencyModel: 'prefetch',
    ordering: 'none',
    priorities: true,
  };

  get capabilities(): TransportCapabilities {
    return this._capabilities;
  }

  private connection: ChannelModel | null = null;
  private publishChannel: ConfirmChannel | null = null;
  private readonly connectionManager: ConnectionManager;
  private readonly queueChannels = new Map<string, QueueChannel>();
  private topology: Topology | null = null;
  private readonly codec = new RabbitMQCodec();

  private readonly config: Required<
    Omit<RabbitMQTransportConfig, 'connection' | 'logger'>
  > & {
    readonly connection: Partial<ConnectionManagerConfig>;
  };

  private readonly logger: Logger;
  private delayedExchangeAvailable = false;

  constructor(config: RabbitMQTransportConfig) {
    this.logger = config.logger ?? consoleLogger;
    this.config = {
      url: config.url,
      connectionName: config.connectionName,
      connection: config.connection ?? {},
      quorumQueues: config.quorumQueues ?? true,
      defaultPrefetch: config.defaultPrefetch ?? 10,
      enableDelayedMessages: config.enableDelayedMessages ?? true,
      publishTimeoutMs: config.publishTimeoutMs ?? 5000,
    };

    this.connectionManager = new ConnectionManager(
      () => this.doConnect(),
      () => this.doDisconnect(),
      this.config.connection,
    );
  }

  async connect(): Promise<void> {
    await this.connectionManager.connect();
  }

  async disconnect(): Promise<void> {
    await this.connectionManager.disconnect();
  }

  isConnected(): boolean {
    return this.connectionManager.isConnected();
  }

  async applyTopology(topology: Topology): Promise<void> {
    this.topology = topology;

    if (!this.publishChannel) {
      throw new TransportNotConnectedError(this.name, 'applyTopology');
    }

    const channel = this.publishChannel;

    // Create the main exchange for routing messages to queues
    const mainExchange = this.getMainExchangeName(topology);
    await channel.assertExchange(mainExchange, 'direct', { durable: true });

    // Create dead-letter exchange if DLQ is enabled
    const dlxExchange = this.getDLXExchangeName(topology);
    if (
      topology.deadLetter.unhandled.enabled ||
      topology.deadLetter.undeliverable.enabled
    ) {
      await channel.assertExchange(
        dlxExchange,
        topology.naming?.dlxExchangeType ?? 'direct',
        { durable: true },
      );
    }

    // Check for delayed message exchange plugin
    if (this.config.enableDelayedMessages) {
      await this.setupDelayedExchange(topology);
    }

    // Create work queues
    for (const queueDef of topology.queues) {
      await this.assertWorkQueue(channel, topology, queueDef);
    }

    // Create DLQs
    if (topology.deadLetter.unhandled.enabled) {
      await this.assertDeadLetterQueues(channel, topology, 'unhandled');
    }

    if (topology.deadLetter.undeliverable.enabled) {
      await this.assertDeadLetterQueues(channel, topology, 'undeliverable');
    }
  }

  async send(
    queue: string,
    envelope: Envelope,
    options?: SendOptions,
  ): Promise<Transport['name']> {
    if (!this.publishChannel || !this.topology) {
      throw new TransportNotConnectedError(this.name, 'send');
    }

    const encoded = this.codec.encode(envelope);
    const buffer = Buffer.from(encoded.body);

    const publishOptions: Options.Publish = {
      persistent: true,
      contentType: encoded.contentType,
      messageId: envelope.id,
      timestamp: Date.now(),
      headers: encoded.headers,
    };

    if (options?.priority !== undefined) {
      publishOptions.priority = options.priority;
    }

    // Handle delayed messages
    if (options?.delay !== undefined && options.delay > 0) {
      if (!this.delayedExchangeAvailable) {
        throw new DelayedMessagesNotSupportedError(this.name);
      }

      const delayedExchange = this.getDelayedExchangeName(this.topology);
      publishOptions.headers = {
        ...publishOptions.headers,
        'x-delay': options.delay,
      };
      await this.confirmPublish(
        this.publishChannel,
        delayedExchange,
        queue,
        buffer,
        publishOptions,
      );
      return this.name;
    }

    // Transport-specific options
    if (options?.transport?.rabbitmq?.expiration !== undefined) {
      publishOptions.expiration = String(options.transport.rabbitmq.expiration);
    }

    if (options?.transport?.rabbitmq?.persistent !== undefined) {
      publishOptions.persistent = options.transport.rabbitmq.persistent;
    }

    const routingKey = options?.transport?.rabbitmq?.routingKey ?? queue;
    const exchange = this.getMainExchangeName(this.topology);

    await this.confirmPublish(
      this.publishChannel,
      exchange,
      routingKey,
      buffer,
      publishOptions,
    );
    return this.name;
  }

  async subscribe(
    queue: string,
    handler: MessageHandler,
    options: SubscribeOptions = {},
  ): Promise<Subscription> {
    if (!this.connection || !this.topology) {
      throw new TransportNotConnectedError(this.name, 'subscribe');
    }

    // Get or create a dedicated channel for this queue
    const queueChannel = await this.getOrCreateQueueChannel(queue, options);
    const { channel } = queueChannel;

    const consumer: ActiveConsumer = {
      consumerTag: '',
      queue,
      active: true,
    };

    const { consumerTag } = await channel.consume(
      queue,
      async (msg: ConsumeMessage | null) => {
        if (!msg || !consumer.active) return;

        const attemptNumber = this.getAttemptNumber(msg);
        const receipt: MessageReceipt = {
          handle: { channel, msg },
          redelivered: msg.fields.redelivered,
          attemptNumber,
          deliveryCount: this.getDeliveryCount(msg, attemptNumber),
          sourceQueue: queue,
          sourceTransport: this.name,
        };

        try {
          const headers = (msg.properties.headers ?? {}) as Record<
            string,
            unknown
          >;
          const envelope = this.codec.decode(
            new Uint8Array(msg.content),
            headers,
          );
          await handler(envelope, receipt);
        } catch (error) {
          // Handler errors should be caught in the pipeline
          this.logger.error(
            '[Matador] 🔴 Handler error in message processing',
            error,
          );
        }
      },
      { noAck: false }, // Always manually ack
    );

    // Update the consumer tag
    (consumer as { consumerTag: string }).consumerTag = consumerTag;

    // Track the consumer
    queueChannel.consumers.push(consumer);

    return {
      unsubscribe: async () => {
        consumer.active = false;
        try {
          await channel.cancel(consumerTag);
        } catch {
          // Channel may already be closed
        }

        // Remove consumer from tracking
        const idx = queueChannel.consumers.indexOf(consumer);
        if (idx !== -1) {
          queueChannel.consumers.splice(idx, 1);
        }

        // Close channel if no more consumers on this queue
        if (queueChannel.consumers.length === 0) {
          try {
            await channel.close();
          } catch {
            // Ignore
          }
          this.queueChannels.delete(queue);
        }
      },
      get isActive() {
        return consumer.active;
      },
    };
  }

  async complete(receipt: MessageReceipt): Promise<void> {
    const { channel, msg } = receipt.handle as {
      channel: Channel;
      msg: ConsumeMessage;
    };

    try {
      channel.ack(msg);
    } catch {
      // Channel may be closed, ignore
    }
  }

  async sendToDeadLetter(
    receipt: MessageReceipt,
    dlqName: string,
    envelope: Envelope,
    reason: string,
  ): Promise<void> {
    if (!this.publishChannel || !this.topology) {
      throw new TransportNotConnectedError(this.name, 'sendToDeadLetter');
    }

    // Add error info to envelope
    const dlqEnvelope: Envelope = {
      ...envelope,
      docket: {
        ...envelope.docket,
        lastError: reason,
        firstError: envelope.docket.firstError ?? reason,
        originalQueue: receipt.sourceQueue,
      },
    };

    const encoded = this.codec.encode(dlqEnvelope);
    const buffer = Buffer.from(encoded.body);
    const dlxExchange = this.getDLXExchangeName(this.topology);
    const dlqQueueName = `${receipt.sourceQueue}.${dlqName}`;

    const publishOptions: Options.Publish = {
      persistent: true,
      contentType: encoded.contentType,
      messageId: envelope.id,
      timestamp: Date.now(),
      headers: {
        ...encoded.headers,
        'x-matador-dead-letter-reason': reason,
      },
    };

    await this.confirmPublish(
      this.publishChannel,
      dlxExchange,
      dlqQueueName,
      buffer,
      publishOptions,
    );

    // Complete the original message
    await this.complete(receipt);
  }

  /**
   * Publishes on the confirm channel and resolves once the broker
   * acknowledges the message, rejecting on broker nack, channel error, or
   * after `publishTimeoutMs` elapses without a confirm.
   *
   * Mirrors Matador v1's promise-wrapped amqplib publish callback to give
   * callers confirmed (at-least-once) delivery semantics.
   */
  private confirmPublish(
    channel: ConfirmChannel,
    exchange: string,
    routingKey: string,
    buffer: Buffer,
    options: Options.Publish,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let timeout: NodeJS.Timeout | null = setTimeout(() => {
        timeout = null;
        reject(
          new TransportSendError(
            routingKey,
            new Error(
              `Publish not confirmed by broker within ${this.config.publishTimeoutMs}ms`,
            ),
          ),
        );
      }, this.config.publishTimeoutMs);

      channel.publish(exchange, routingKey, buffer, options, (err) => {
        if (timeout) {
          clearTimeout(timeout);
        } else {
          // Already timed out and rejected; nothing left to settle.
          return;
        }
        if (err) {
          reject(new TransportSendError(routingKey, err));
        } else {
          resolve();
        }
      });
    });
  }

  // Private methods

  /**
   * Gets or creates a dedicated channel for a queue subscription.
   *
   * We create separate channels per subscribed queue to enable independent
   * prefetch/concurrency control.
   */
  private async getOrCreateQueueChannel(
    queue: string,
    options: SubscribeOptions,
  ): Promise<QueueChannel> {
    const existing = this.queueChannels.get(queue);
    if (existing) {
      return existing;
    }

    if (!this.connection) {
      throw new TransportNotConnectedError(
        this.name,
        'getOrCreateQueueChannel',
      );
    }

    // Create a dedicated channel for this queue to control prefetch independently
    const channel = await this.connection.createChannel();

    const prefetch =
      options.transport?.rabbitmq?.prefetch ??
      options.concurrency ??
      this.config.defaultPrefetch ??
      10;

    await channel.prefetch(prefetch);

    const queueChannel: QueueChannel = {
      channel,
      consumers: [],
    };

    this.queueChannels.set(queue, queueChannel);

    return queueChannel;
  }

  private async doConnect(): Promise<void> {
    this.logger.info(
      `[Matador] ⏳ Connecting to RabbitMQ at '${redactAmqpUrl(this.config.url)}'.`,
    );
    const connection = await amqplib.connect(this.config.url, {
      clientProperties: { connection_name: this.config.connectionName },
    });
    this.connection = connection;

    // Handle connection errors - let ConnectionManager handle reconnection
    connection.on('error', (err: Error) => {
      this.logger.error('[Matador] 🔴 RabbitMQ connection error', err);
    });

    connection.on('close', () => {
      if (this.connectionManager.isConnected()) {
        // Unexpected close, trigger reconnection
        this.connectionManager.handleConnectionLost(
          new Error('Connection closed unexpectedly'),
        );
      }
    });

    // Create the publish channel.
    // A confirm channel so publishes can await broker acknowledgement.
    this.publishChannel = await connection.createConfirmChannel();

    // Handle publish channel errors to prevent unhandled error events
    this.publishChannel.on('error', (err: Error) => {
      this.logger.error('[Matador] 🔴 RabbitMQ publish channel error', err);
    });

    // Re-apply topology if we have one (reconnection scenario)
    if (this.topology) {
      await this.applyTopology(this.topology);
    }
  }

  private async doDisconnect(): Promise<void> {
    // Cancel all consumers and close queue channels
    for (const queueChannel of this.queueChannels.values()) {
      for (const consumer of queueChannel.consumers) {
        consumer.active = false;
        try {
          await queueChannel.channel.cancel(consumer.consumerTag);
        } catch {
          // Ignore errors during cleanup
        }
      }
      try {
        await queueChannel.channel.close();
      } catch {
        // Ignore
      }
    }
    this.queueChannels.clear();

    // Close publish channel
    if (this.publishChannel) {
      try {
        await this.publishChannel.close();
      } catch {
        // Ignore
      }
      this.publishChannel = null;
    }

    // Close connection
    if (this.connection) {
      try {
        await this.connection.close();
      } catch {
        // Ignore
      }
      this.connection = null;
    }

    // Reset capabilities
    this.delayedExchangeAvailable = false;
    this._capabilities = {
      ...this._capabilities,
      delayedMessages: false,
    };
  }

  private async setupDelayedExchange(topology: Topology): Promise<void> {
    if (!this.connection) {
      return;
    }

    // Default to disabled
    this.delayedExchangeAvailable = false;

    const delayedExchange = this.getDelayedExchangeName(topology);
    const connection = this.connection;

    // Use a promise-based approach to ensure all error paths resolve cleanly
    // This prevents any error from propagating and affecting other channels
    return new Promise<void>((resolve) => {
      let resolved = false;
      const safeResolve = () => {
        if (!resolved) {
          resolved = true;
          resolve();
        }
      };

      connection
        .createChannel()
        .then((probeChannel) => {
          // Handle channel errors - this fires when RabbitMQ closes the channel
          probeChannel.on('error', () => {
            safeResolve();
          });

          // Handle channel close
          probeChannel.on('close', () => {
            safeResolve();
          });

          // Try to declare a delayed exchange
          // This will fail if the plugin is not installed
          probeChannel
            .assertExchange(delayedExchange, 'x-delayed-message', {
              durable: true,
              arguments: { 'x-delayed-type': 'direct' },
            })
            .then(() => {
              this.delayedExchangeAvailable = true;
              this._capabilities = {
                ...this._capabilities,
                delayedMessages: true,
              };
              this.logger.debug(
                '[Matador] 🔌 Delayed message exchange plugin detected',
              );
              // Close the probe channel gracefully
              probeChannel.close().catch(() => {});
              safeResolve();
            })
            .catch(() => {
              // assertExchange failed - plugin not available
              // Channel is already closed by RabbitMQ, no need to close
              this.logger.warn(
                '[Matador] 🟡 RabbitMQ delayed message exchange plugin not available. ' +
                  'Delayed messages will not be supported.',
              );
              safeResolve();
            });
        })
        .catch(() => {
          // Failed to create channel - shouldn't happen but handle it
          safeResolve();
        });
    });
  }

  private buildWorkQueueOptions(
    topology: Topology,
    queueDef: QueueDefinition,
  ): Options.AssertQueue {
    const queueOptions: Options.AssertQueue = {
      durable: true,
      arguments: {} as Record<string, unknown>,
    };

    if (this.config.quorumQueues && !queueDef.exact) {
      queueOptions.arguments['x-queue-type'] = 'quorum';
    }

    if (
      topology.deadLetter.unhandled.enabled ||
      topology.deadLetter.undeliverable.enabled
    ) {
      queueOptions.arguments['x-dead-letter-exchange'] =
        this.getDLXExchangeName(topology);
    }

    if (queueDef.priorities) {
      queueOptions.arguments['x-max-priority'] = 10;
    }

    if (queueDef.consumerTimeout) {
      queueOptions.arguments['x-consumer-timeout'] = queueDef.consumerTimeout;
    }

    return queueOptions;
  }

  private async assertWorkQueue(
    channel: Channel,
    topology: Topology,
    queueDef: QueueDefinition,
  ): Promise<void> {
    const queueName = resolveQueueName(
      topology.namespace,
      queueDef,
      topology.naming,
    );

    const rabbitmqOptions = queueDef.transport?.rabbitmq?.options;
    const queueOptions =
      rabbitmqOptions ?? this.buildWorkQueueOptions(topology, queueDef);
    await channel.assertQueue(queueName, queueOptions);

    // Bind queue to main exchange
    const mainExchange = this.getMainExchangeName(topology);
    await channel.bindQueue(queueName, mainExchange, queueName);

    // Bind to delayed exchange if available
    if (this.delayedExchangeAvailable) {
      const delayedExchange = this.getDelayedExchangeName(topology);
      await channel.bindQueue(queueName, delayedExchange, queueName);
    }

    // Create retry queue if retry is enabled.
    // Skip exact:true queues - they are owned by another team (cross-namespace).
    // The retry queue derives its `x-dead-letter-exchange` from
    // `${topology.namespace}.exchange`, which would conflict if multiple
    // namespaces share the broker and the queue. This mirrors the existing
    // `if (queueDef.exact) continue` guard in `assertDeadLetterQueues`.
    if (topology.retry.enabled && !queueDef.exact) {
      await this.assertRetryQueue(channel, topology, queueDef);
    }
  }

  private async assertRetryQueue(
    channel: Channel,
    topology: Topology,
    queueDef: QueueDefinition,
  ): Promise<void> {
    const workQueueName = resolveQueueName(
      topology.namespace,
      queueDef,
      topology.naming,
    );
    const retryQueueName = getRetryQueueName(
      topology.namespace,
      queueDef.name,
      topology.naming,
    );
    const mainExchange = this.getMainExchangeName(topology);

    const retryQueueOptions: Options.AssertQueue = {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': mainExchange,
        'x-dead-letter-routing-key': workQueueName,
        'x-message-ttl': topology.retry.defaultDelayMs,
      } as Record<string, unknown>,
    };

    if (this.config.quorumQueues) {
      retryQueueOptions.arguments['x-queue-type'] = 'quorum';
    }

    await channel.assertQueue(retryQueueName, retryQueueOptions);
    await channel.bindQueue(retryQueueName, mainExchange, retryQueueName);
  }

  private async assertDeadLetterQueues(
    channel: Channel,
    topology: Topology,
    dlqType: 'unhandled' | 'undeliverable',
  ): Promise<void> {
    const dlxExchange = this.getDLXExchangeName(topology);
    const dlConfig = topology.deadLetter[dlqType];

    for (const queueDef of topology.queues) {
      if (queueDef.exact) continue;

      const dlqName = getDeadLetterQueueName(
        topology.namespace,
        queueDef.name,
        dlqType,
        topology.naming,
      );

      const dlqOptions: Options.AssertQueue = {
        durable: true,
        arguments: {} as Record<string, unknown>,
      };

      if (dlConfig.maxLength) {
        dlqOptions.arguments['x-max-length'] = dlConfig.maxLength;
      }

      // DLQs follow the same quorum setting as work and retry queues, so a
      // broker node restart does not take the queue (and its messages) down
      // with it.
      if (this.config.quorumQueues) {
        dlqOptions.arguments['x-queue-type'] = 'quorum';
      }

      await channel.assertQueue(dlqName, dlqOptions);
      await channel.bindQueue(dlqName, dlxExchange, dlqName);
    }
  }

  private getMainExchangeName(topology: Topology): string {
    return (
      topology.naming?.mainExchange?.(topology.namespace) ??
      `${topology.namespace}.exchange`
    );
  }

  private getDLXExchangeName(topology: Topology): string {
    return (
      topology.naming?.dlxExchange?.(topology.namespace) ??
      `${topology.namespace}.dlx`
    );
  }

  private getDelayedExchangeName(topology: Topology): string {
    return (
      topology.naming?.delayedExchange?.(topology.namespace) ??
      `${topology.namespace}.delayed`
    );
  }

  private getAttemptNumber(msg: ConsumeMessage): number {
    const headerValue = msg.properties.headers?.['x-matador-attempts'];
    if (typeof headerValue === 'number') {
      return headerValue;
    }
    // Check for x-death header (native DLX redelivery count)
    const xDeath = msg.properties.headers?.['x-death'];
    if (Array.isArray(xDeath) && xDeath.length > 0) {
      const deathCount = xDeath.reduce(
        (sum: number, death: { count?: number }) => sum + (death.count ?? 0),
        0,
      );
      return deathCount + 1;
    }
    return 1;
  }

  /**
   * Gets the native delivery count for poison message detection.
   * This tracks how many times the message was delivered without acknowledgment,
   * which helps detect crash loops.
   */
  private getDeliveryCount(msg: ConsumeMessage, attemptNumber: number): number {
    // Check for explicit delivery count header (some RabbitMQ setups track this)
    const deliveryCount = msg.properties.headers?.['x-delivery-count'];
    if (typeof deliveryCount === 'number') {
      return deliveryCount;
    }

    // Check x-death header for dead-letter redelivery count
    const xDeath = msg.properties.headers?.['x-death'];
    if (Array.isArray(xDeath) && xDeath.length > 0) {
      const deathCount = xDeath.reduce(
        (sum: number, death: { count?: number }) => sum + (death.count ?? 0),
        0,
      );
      // Add 1 because we're currently being delivered again
      return deathCount + 1;
    }

    // If redelivered flag is set but no other tracking, count as 2 (first + this delivery)
    if (msg.fields.redelivered) {
      return Math.max(2, attemptNumber);
    }

    // Default to attempt number
    return attemptNumber;
  }
}
