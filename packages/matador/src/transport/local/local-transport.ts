import {
  LocalTransportNoActiveSubscriberError,
  TransportNotConnectedError,
} from '../../errors/index.js';
import { type Logger, consoleLogger } from '../../hooks/index.js';
import type { Topology } from '../../topology/types.js';
import { resolveQueueName } from '../../topology/types.js';
import type { Envelope } from '../../types/index.js';
import type { TransportCapabilities } from '../capabilities.js';
import type {
  MessageHandler,
  MessageReceipt,
  SendOptions,
  SubscribeOptions,
  Subscription,
  Transport,
} from '../transport.js';

/**
 * Capabilities of the LocalTransport.
 */
const localCapabilities: TransportCapabilities = {
  deliveryModes: ['at-least-once', 'at-most-once'],
  delayedMessages: true, // Implemented with setTimeout
  deadLetterRouting: 'manual',
  attemptTracking: false,
  concurrencyModel: 'none',
  ordering: 'queue',
  priorities: false,
};

/**
 * Internal message structure for the local queue.
 */
interface QueuedMessage {
  readonly envelope: Envelope;
  readonly id: string;
  completed: boolean;
}

/**
 * Internal subscription structure.
 */
interface ActiveSubscription {
  readonly handler: MessageHandler;
  readonly options: SubscribeOptions;
  active: boolean;
}

/**
 * Local in-memory transport for testing and fallback.
 * Messages are stored in memory and delivered synchronously.
 */
export class LocalTransport implements Transport {
  readonly name = 'local';
  readonly capabilities = localCapabilities;

  private connected = false;
  private readonly queues = new Map<string, QueuedMessage[]>();
  private readonly subscriptions = new Map<string, ActiveSubscription[]>();
  private readonly completedMessages: MessageReceipt[] = [];
  private readonly delayedTimers = new Set<ReturnType<typeof setTimeout>>();
  private messageIdCounter = 0;
  private readonly logger: Logger;

  constructor(logger?: Logger) {
    this.logger = logger ?? consoleLogger;
  }

  async connect(): Promise<void> {
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    // Cancel all delayed message timers
    for (const timer of this.delayedTimers) {
      clearTimeout(timer);
    }
    this.delayedTimers.clear();

    // Deactivate all subscriptions
    for (const subs of this.subscriptions.values()) {
      for (const sub of subs) {
        sub.active = false;
      }
    }

    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  async applyTopology(topology: Topology): Promise<void> {
    if (!this.connected) {
      throw new TransportNotConnectedError(this.name, 'applyTopology');
    }

    // Initialize queues for the topology.
    // Note: Retry queues are not pre-created here because LocalTransport handles
    // retries by re-enqueueing messages with a delay to the original queue, matching
    // how the pipeline schedules retries via transport.send() with a delay option.
    for (const queueDef of topology.queues) {
      const queueName = resolveQueueName(
        topology.namespace,
        queueDef,
        topology.naming,
        topology.prefix,
      );
      if (!this.queues.has(queueName)) {
        this.queues.set(queueName, []);
      }
    }
  }

  async send(
    queue: string,
    envelope: Envelope,
    options?: SendOptions,
  ): Promise<Transport['name']> {
    if (!this.connected) {
      throw new TransportNotConnectedError(this.name, 'send');
    }

    // Handle delayed messages (non-blocking, like real transports)
    if (options?.delay !== undefined && options.delay > 0) {
      this.scheduleDelayedMessage(queue, envelope, options.delay);
      return this.name;
    }

    await this.enqueue(queue, envelope);
    return this.name;
  }

  /**
   * Schedules a message for delayed delivery.
   * Returns immediately (non-blocking) to match real transport behavior.
   */
  private scheduleDelayedMessage(
    queue: string,
    envelope: Envelope,
    delayMs: number,
  ): void {
    const timer = setTimeout(() => {
      this.delayedTimers.delete(timer);
      // Fire and forget - errors are logged in enqueue/deliverToSubscribers
      this.enqueue(queue, envelope).catch((error) => {
        this.logger.error(
          '[Matador] 🔴 Failed to enqueue delayed message',
          error,
        );
      });
    }, delayMs);
    this.delayedTimers.add(timer);
  }

  private async enqueue(queue: string, envelope: Envelope): Promise<void> {
    const subs = this.subscriptions.get(queue);
    if (!subs || subs.length === 0) {
      throw new LocalTransportNoActiveSubscriberError(queue);
    }

    await this.storeAndDeliver(queue, envelope);
  }

  /**
   * Stores a message and delivers it to any active subscribers, without
   * requiring one to be present. Dead-letter queues are meant to hold
   * messages for later manual inspection, so they don't need a live consumer.
   */
  private async storeAndDeliver(
    queue: string,
    envelope: Envelope,
  ): Promise<void> {
    const messages = this.getOrCreateQueue(queue);
    const messageId = `${++this.messageIdCounter}`;

    const queuedMessage: QueuedMessage = {
      envelope,
      id: messageId,
      completed: false,
    };

    messages.push(queuedMessage);

    await this.deliverToSubscribers(queue, queuedMessage);
  }

  private async deliverToSubscribers(
    queue: string,
    message: QueuedMessage,
  ): Promise<void> {
    const subs = this.subscriptions.get(queue) ?? [];

    for (const sub of subs) {
      if (!sub.active || message.completed) continue;

      const receipt: MessageReceipt = {
        handle: message,
        redelivered: false,
        attemptNumber: message.envelope.docket.attempts,
        deliveryCount: message.envelope.docket.attempts,
        sourceQueue: queue,
        sourceTransport: this.name,
      };

      try {
        await sub.handler(message.envelope, receipt);
      } catch (error) {
        // Handler errors should be caught in the pipeline
        this.logger.error(
          '[Matador] 🔴 Handler error in message processing',
          error,
        );
      }
    }
  }

  async subscribe(
    queue: string,
    handler: MessageHandler,
    options: SubscribeOptions = {},
  ): Promise<Subscription> {
    if (!this.connected) {
      throw new TransportNotConnectedError(this.name, 'subscribe');
    }

    const subscription: ActiveSubscription = {
      handler,
      options,
      active: true,
    };

    const subs = this.subscriptions.get(queue) ?? [];
    subs.push(subscription);
    this.subscriptions.set(queue, subs);

    // Deliver any pending messages
    const messages = this.queues.get(queue) ?? [];
    for (const message of messages) {
      if (message.completed) continue;
      await this.deliverToSubscribers(queue, message);
    }

    return {
      unsubscribe: async () => {
        subscription.active = false;
        const remaining = subs.filter((s) => s !== subscription);
        if (remaining.length > 0) {
          this.subscriptions.set(queue, remaining);
        } else {
          this.subscriptions.delete(queue);
        }
      },
      get isActive() {
        return subscription.active;
      },
    };
  }

  async complete(receipt: MessageReceipt): Promise<void> {
    const message = receipt.handle as QueuedMessage;
    message.completed = true;
    this.completedMessages.push(receipt);
  }

  async sendToDeadLetter(
    receipt: MessageReceipt,
    dlqName: string,
    envelope: Envelope,
    _reason: string,
  ): Promise<void> {
    const dlqQueueName = `${receipt.sourceQueue}.${dlqName}`;
    await this.storeAndDeliver(dlqQueueName, envelope);
    await this.complete(receipt);
  }

  // Test helpers

  /**
   * Gets the current size of a queue.
   */
  getQueueSize(queue: string): number {
    const messages = this.queues.get(queue);
    if (!messages) return 0;
    return messages.filter((m) => !m.completed).length;
  }

  /**
   * Gets all completed message receipts.
   */
  getCompleted(): readonly MessageReceipt[] {
    return this.completedMessages;
  }

  /**
   * Gets pending (uncompleted) messages from a queue.
   */
  getPendingMessages(queue: string): readonly Envelope[] {
    const messages = this.queues.get(queue);
    if (!messages) return [];
    return messages.filter((m) => !m.completed).map((m) => m.envelope);
  }

  /**
   * Clears all state (for test isolation).
   */
  clear(): void {
    this.queues.clear();
    this.subscriptions.clear();
    this.completedMessages.length = 0;
    this.messageIdCounter = 0;

    for (const timer of this.delayedTimers) {
      clearTimeout(timer);
    }
    this.delayedTimers.clear();
  }

  /**
   * Receives one message from the queue without a subscription.
   * Useful for testing.
   */
  async receiveOne(
    queue: string,
  ): Promise<{ envelope: Envelope; receipt: MessageReceipt } | null> {
    const messages = this.queues.get(queue);
    if (!messages) return null;

    const pending = messages.find((m) => !m.completed);
    if (!pending) return null;

    const receipt: MessageReceipt = {
      handle: pending,
      redelivered: false,
      attemptNumber: pending.envelope.docket.attempts,
      deliveryCount: pending.envelope.docket.attempts,
      sourceQueue: queue,
      sourceTransport: this.name,
    };

    return { envelope: pending.envelope, receipt };
  }

  private getOrCreateQueue(queue: string): QueuedMessage[] {
    let messages = this.queues.get(queue);
    if (!messages) {
      messages = [];
      this.queues.set(queue, messages);
    }
    return messages;
  }
}
