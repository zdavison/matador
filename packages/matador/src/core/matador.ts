import type { CheckpointStore } from '../checkpoint/index.js';
import type { Codec } from '../codec/index.js';
import { JsonCodec } from '../codec/index.js';
import {
  InvalidSchemaError,
  NotStartedError,
  ShutdownInProgressError,
  SomeSendError,
} from '../errors/index.js';
import type { MatadorHooks } from '../hooks/index.js';
import { SafeHooks } from '../hooks/index.js';
import { ProcessingPipeline } from '../pipeline/index.js';
import type { RetryPolicy } from '../retry/index.js';
import { StandardRetryPolicy } from '../retry/index.js';
import type { MatadorSchema } from '../schema/index.js';
import { SchemaRegistry, isSchemaEntryTuple } from '../schema/index.js';
import type { Topology } from '../topology/index.js';
import {
  findQueueDefinition,
  resolveTargetQueueName,
} from '../topology/index.js';
import type { Subscription, Transport } from '../transport/index.js';
import type {
  AnySubscriber,
  Dispatcher,
  Event,
  EventClass,
  EventOptions,
} from '../types/index.js';
import type { SendResult } from './fanout.js';
import { FanoutEngine } from './fanout.js';
import type { HandlersState, ShutdownConfig } from './shutdown.js';
import { ShutdownManager } from './shutdown.js';

/**
 * Configuration for Matador.
 */
export interface MatadorConfig {
  /** Transport for message delivery */
  readonly transport: Transport;

  /** Topology configuration */
  readonly topology: Topology;

  /** Event schema mapping event keys to event classes and subscribers */
  readonly schema: MatadorSchema;

  /** Queues to consume from (empty = no consumption) */
  readonly consumeFrom?: readonly string[] | undefined;

  /** Custom codec (defaults to JSON) */
  readonly codec?: Codec | undefined;

  /** Custom retry policy */
  readonly retryPolicy?: RetryPolicy | undefined;

  /** Shutdown configuration */
  readonly shutdownConfig?: Partial<ShutdownConfig> | undefined;

  /**
   * Checkpoint store for resumable subscribers.
   * Required for persisting io() results across retries.
   * If not provided, resumable subscribers will work but checkpoints
   * won't persist across retries.
   */
  readonly checkpointStore?: CheckpointStore | undefined;
}

/**
 * Matador - Transport-agnostic event processing library.
 *
 * Main orchestrator that wires together:
 * - Transport: Message delivery
 * - Schema: Event-subscriber registry
 * - Pipeline: Message processing
 * - Fanout: Event sending
 * - Shutdown: Graceful termination
 */
export class Matador implements Dispatcher {
  private readonly transport: Transport;
  private readonly topology: Topology;
  private readonly schema;
  private readonly codec: Codec;
  private readonly retryPolicy: RetryPolicy;
  private readonly hooks;
  private readonly pipeline;
  private readonly fanout;
  private readonly shutdownManager;
  private readonly consumeFrom: readonly string[];
  private readonly subscriptions: Subscription[] = [];
  private started = false;

  /**
   * Creates a new Matador instance.
   *
   * @param config - Static configuration (transport, topology, schema, etc.)
   * @param hooks - Lifecycle hooks for logging, monitoring, and dynamic configuration.
   *                Passed separately to support NestJS dependency injection.
   */
  constructor(config: MatadorConfig, hooks?: MatadorHooks) {
    this.transport = config.transport;
    this.topology = config.topology;
    this.consumeFrom = config.consumeFrom ?? [];

    // Initialize components
    this.schema = new SchemaRegistry();
    this.codec = config.codec ?? new JsonCodec();
    this.retryPolicy = config.retryPolicy ?? new StandardRetryPolicy();
    this.hooks = new SafeHooks(hooks);

    // Register schema from config
    this.registerSchema(config.schema);

    // Create pipeline
    this.pipeline = new ProcessingPipeline({
      transport: this.transport,
      schema: this.schema,
      codec: this.codec,
      retryPolicy: this.retryPolicy,
      hooks: this.hooks,
      checkpointStore: config.checkpointStore,
      dispatcher: this,
    });

    // Create fanout engine
    const defaultQueue = this.topology.queues[0]?.name ?? 'default';
    this.fanout = new FanoutEngine({
      transport: this.transport,
      schema: this.schema,
      hooks: this.hooks,
      topology: this.topology,
      defaultQueue,
    });

    // Create shutdown manager
    this.shutdownManager = new ShutdownManager(
      () => this.fanout.eventsBeingEnqueuedCount,
      () => this.unsubscribeAll(),
      () => this.transport.disconnect(),
      config.shutdownConfig,
    );
  }

  /**
   * Registers an event class with its subscribers.
   */
  register<T>(
    eventClass: EventClass<T>,
    subscribers: readonly AnySubscriber[],
  ): this {
    this.schema.register(eventClass, subscribers);
    return this;
  }

  /**
   * Registers events from a schema object.
   * Supports both object format and tuple format entries.
   *
   * @example
   * ```typescript
   * // Tuple format
   * matador.registerSchema({
   *   [UserCreatedEvent.key]: [UserCreatedEvent, [emailSubscriber]],
   *   [OrderPlacedEvent.key]: [OrderPlacedEvent, [invoiceSubscriber]],
   * });
   *
   * // Object format
   * matador.registerSchema({
   *   [UserCreatedEvent.key]: { eventClass: UserCreatedEvent, subscribers: [emailSubscriber] },
   * });
   * ```
   */
  registerSchema(schema: MatadorSchema): this {
    for (const entry of Object.values(schema)) {
      if (isSchemaEntryTuple(entry)) {
        // Tuple format: [EventClass, Subscriber[]]
        const [eventClass, subscribers] = entry;
        this.schema.register(eventClass, subscribers);
      } else {
        // Object format: { eventClass, subscribers }
        this.schema.register(entry.eventClass, entry.subscribers);
      }
    }
    return this;
  }

  /**
   * Starts Matador - connects transport and begins consuming.
   * This method is idempotent - calling it multiple times is safe.
   */
  async start(): Promise<void> {
    // Idempotent: if already started, just return
    if (this.started) {
      return;
    }

    // Validate schema
    const validation = this.schema.validate();
    if (!validation.valid) {
      const errors = validation.issues.filter((i) => i.severity === 'error');
      throw new InvalidSchemaError(
        'Schema validation failed',
        errors.map((e) => e.message).join(', '),
      );
    }

    // Connect transport
    this.hooks.logger.info(
      `[Matador] ⏳ Starting backend '${this.transport.name}'...`,
    );
    await this.transport.connect();
    this.hooks.logger.info(
      `[Matador] 🟢 Backend '${this.transport.name}' started.`,
    );

    // Apply topology
    await this.transport.applyTopology(this.topology);

    // Subscribe to queues
    if (this.consumeFrom.length > 0) {
      this.hooks.logger.info(
        `[Matador] 🟢 Worker subscribing to '${this.consumeFrom.join(',')}'.`,
      );
    } else {
      this.hooks.logger.info(
        '[Matador] 🟡 Worker not subscribing to any queues (consumeFrom is empty).',
      );
    }

    for (const queueName of this.consumeFrom) {
      const qualifiedName = resolveTargetQueueName(this.topology, queueName);
      const queueDef = findQueueDefinition(this.topology, queueName);

      const subscription = await this.transport.subscribe(
        qualifiedName,
        async (envelope, receipt) => {
          this.shutdownManager.incrementProcessing();
          try {
            const rawMessage = this.codec.encode(envelope);
            await this.pipeline.process(rawMessage, receipt);
          } finally {
            this.shutdownManager.decrementProcessing();
          }
        },
        queueDef?.concurrency !== undefined
          ? { concurrency: queueDef.concurrency }
          : undefined,
      );

      this.subscriptions.push(subscription);
    }

    this.started = true;
  }

  /**
   * Sends an event to all registered subscribers.
   *
   * @example
   * ```typescript
   * // Pass the event class and data directly
   * await matador.send(UserCreatedEvent, { userId: '123' });
   *
   * // Or pass an event instance
   * const event = new UserCreatedEvent({ userId: '123' });
   * await matador.send(event);
   * ```
   */
  async send<T>(
    eventClass: EventClass<T>,
    data: T,
    options?: EventOptions,
  ): Promise<SendResult>;
  async send<T>(event: Event<T>, options?: EventOptions): Promise<SendResult>;
  async send<T>(
    eventOrClass: Event<T> | EventClass<T>,
    dataOrOptions?: T | EventOptions,
    maybeOptions?: EventOptions,
  ): Promise<SendResult> {
    if (!this.started) {
      throw new NotStartedError('send');
    }

    if (!this.shutdownManager.isEnqueueAllowed) {
      throw new ShutdownInProgressError();
    }

    // Determine if first arg is an event instance or event class
    const isEventClass =
      typeof eventOrClass === 'function' && 'key' in eventOrClass;

    if (isEventClass) {
      // Called as: send(EventClass, data, options?)
      const eventClass = eventOrClass as EventClass<T>;
      const data = dataOrOptions as T;
      const options = maybeOptions;
      const event = new eventClass(data);
      const result = await this.fanout.send(eventClass, event, options);
      if (result.errors.length > 0)
        throw new SomeSendError(result.eventKey, result.errors);
      return result;
    }
    // Called as: send(event, options?)
    const event = eventOrClass as Event<T>;
    const options = dataOrOptions as EventOptions | undefined;
    const eventClass = event.constructor as EventClass<T>;
    const result = await this.fanout.send(eventClass, event, options);
    if (result.errors.length > 0)
      throw new SomeSendError(result.eventKey, result.errors);
    return result;
  }

  /**
   * Gets current handler state.
   */
  getHandlersState(): HandlersState {
    return this.shutdownManager.getHandlersState();
  }

  /**
   * Checks if Matador is idle (no processing or enqueuing).
   */
  isIdle(): boolean {
    return this.shutdownManager.getHandlersState().isIdle;
  }

  /**
   * Waits for all handlers to become idle.
   */
  async waitForIdle(timeoutMs = 30000): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;

    while (!this.isIdle()) {
      if (Date.now() > deadline) {
        return false;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    return true;
  }

  /**
   * Stops receiving new messages without performing full shutdown.
   * After calling this, no new messages will be delivered from RabbitMQ.
   * The transport remains connected for later shutdown.
   *
   * Use this when you need to coordinate shutdown across multiple systems:
   * 1. Call stopReceiving() to stop new message delivery
   * 2. Wait for both Matador and your other systems to idle
   * 3. Call shutdown() to disconnect (or let NestJS lifecycle handle it)
   *
   * @returns true if receiving was stopped, false if not started or already stopped
   */
  async stopReceiving(): Promise<boolean> {
    if (!this.started) {
      return false;
    }

    return this.shutdownManager.publicStopReceiving();
  }

  /**
   * Gracefully shuts down Matador.
   */
  async shutdown(): Promise<void> {
    if (!this.started) {
      return;
    }

    this.fanout.dispose();
    await this.shutdownManager.shutdown();
    this.started = false;
  }

  /**
   * Checks if transport is connected.
   */
  isConnected(): boolean {
    return this.transport.isConnected();
  }

  private async unsubscribeAll(): Promise<void> {
    for (const subscription of this.subscriptions) {
      await subscription.unsubscribe();
    }
    this.subscriptions.length = 0;
  }
}
