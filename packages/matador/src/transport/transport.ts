import type { Topology } from '../topology/types.js';
import type { Envelope } from '../types/index.js';
import type { TransportCapabilities } from './capabilities.js';

/**
 * Transport-specific send options.
 * Each transport can define its own options under its transport name key.
 */
export interface TransportSendOptions {
  /** RabbitMQ-specific send options */
  readonly rabbitmq?: RabbitMQSendOptions | undefined;
}

/**
 * RabbitMQ-specific options for sending messages.
 */
export interface RabbitMQSendOptions {
  /** Message expiration in milliseconds */
  readonly expiration?: number | undefined;

  /** Message persistence mode */
  readonly persistent?: boolean | undefined;

  /** Routing key override */
  readonly routingKey?: string | undefined;
}

/**
 * Options for sending a message.
 */
export interface SendOptions {
  /** Delay delivery by this many milliseconds */
  readonly delay?: number | undefined;

  /** Priority level (0-255, higher = more important) */
  readonly priority?: number | undefined;

  /** Transport-specific options */
  readonly transport?: TransportSendOptions | undefined;
}

/**
 * Transport-specific subscribe options.
 * Each transport can define its own options under its transport name key.
 */
export interface TransportSubscribeOptions {
  /** RabbitMQ-specific subscribe options */
  readonly rabbitmq?: RabbitMQSubscribeOptions | undefined;
}

/**
 * RabbitMQ-specific options for subscribing.
 */
export interface RabbitMQSubscribeOptions {
  /** Consumer tag */
  readonly consumerTag?: string | undefined;

  /** Prefetch count (overrides concurrency option) */
  readonly prefetch?: number | undefined;

  /** Exclusive consumer */
  readonly exclusive?: boolean | undefined;
}

/**
 * Options for subscribing to a queue.
 */
export interface SubscribeOptions {
  /** Concurrency hint (number of concurrent handlers) */
  readonly concurrency?: number | undefined;

  /** Override default delivery semantics */
  readonly deliveryMode?: 'at-least-once' | 'at-most-once' | undefined;

  /** Transport-specific options */
  readonly transport?: TransportSubscribeOptions | undefined;
}

/**
 * Receipt for a received message, used for acknowledgment.
 */
export interface MessageReceipt {
  /** Opaque handle for the transport to identify the message */
  readonly handle: unknown;

  /** True if this is a redelivery (transport-reported if capable) */
  readonly redelivered: boolean;

  /** 1-based attempt number (transport-reported if capable, else from envelope) */
  readonly attemptNumber: number;

  /**
   * Native delivery count from the transport.
   * Tracks how many times this specific message was delivered without acknowledgment.
   * Used for poison message detection to prevent crash loops.
   * For transports that don't track this, defaults to attemptNumber.
   */
  readonly deliveryCount: number;

  /** Original queue/topic the message came from */
  readonly sourceQueue: string;

  /**
   * The name of the transport that received this message (e.g., 'local', 'rabbitmq').
   * For MultiTransport, this is the actual underlying transport, not the wrapper name.
   */
  readonly sourceTransport: string;
}

/**
 * Handler function for processing received messages.
 */
export type MessageHandler = (
  envelope: Envelope,
  receipt: MessageReceipt,
) => Promise<void>;

/**
 * Subscription handle for managing active subscriptions.
 */
export interface Subscription {
  /** Cancels the subscription */
  unsubscribe(): Promise<void>;

  /** Whether the subscription is currently active */
  readonly isActive: boolean;
}

/**
 * Transport interface - the minimal abstraction for message delivery.
 * Transports handle only I/O; all business logic lives in Matador core.
 */
export interface Transport {
  /** Transport identifier */
  readonly name: string;

  /** Capabilities supported by this transport */
  readonly capabilities: TransportCapabilities;

  /**
   * Establishes connection to the message broker.
   * Should handle initial connection with retries.
   */
  connect(): Promise<void>;

  /**
   * Gracefully disconnects from the message broker.
   * Should close all consumers before connection.
   */
  disconnect(): Promise<void>;

  /**
   * Returns whether the transport is currently connected.
   */
  isConnected(): boolean;

  /**
   * Translates and applies the generic topology to the transport.
   * Creates necessary queues, exchanges, topics, etc.
   */
  applyTopology(topology: Topology): Promise<void>;

  /**
   * Sends a message to the specified queue.
   * @returns The name of the transport that was used (useful for MultiTransport)
   */
  send(
    queue: string,
    envelope: Envelope,
    options?: SendOptions,
  ): Promise<Transport['name']>;

  /**
   * Subscribes to messages on the specified queue.
   * The handler receives decoded envelopes and receipts.
   */
  subscribe(
    queue: string,
    handler: MessageHandler,
    options?: SubscribeOptions,
  ): Promise<Subscription>;

  /**
   * Registers a callback to fire each time the transport successfully (re)connects.
   * Returns a function that removes the listener when called.
   * Optional: only meaningful for transports that can lose and regain a connection.
   */
  onConnected?(callback: () => void): () => void;

  /**
   * Acknowledges/completes a message.
   * Called after processing is done (success, retry scheduled, or dead-lettered).
   */
  complete(receipt: MessageReceipt): Promise<void>;

  /**
   * Sends a message to the dead-letter queue.
   * For transports with native DL routing, may use native mechanism.
   * For others, sends to DLQ then completes original.
   */
  sendToDeadLetter?(
    receipt: MessageReceipt,
    dlqName: string,
    envelope: Envelope,
    reason: string,
  ): Promise<void>;
}
