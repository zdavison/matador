import {
  type BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import {
  type Event,
  type EventClass,
  type EventOptions,
  Matador,
  SchemaRegistry,
  type SendResult,
  type StatusRecord,
  isSchemaEntryTuple,
} from '@zdavison/matador';
import { MATADOR_OPTIONS } from '../constants.js';
// biome-ignore lint/style/useImportType: must be a runtime value import — Nest reads it from emitDecoratorMetadata to inject this constructor dependency; `import type` erases it and breaks DI.
import { SubscriberDiscoveryService } from '../discovery/subscriber-discovery.service.js';
import type { MatadorModuleOptions } from '../types.js';

/**
 * Injectable service that wraps Matador and integrates with NestJS lifecycle.
 *
 * This service handles:
 * - Building the Matador instance with discovered subscribers
 * - Starting Matador at the configured lifecycle hook
 * - Graceful shutdown with drain timeout
 * - Preventing event sending during shutdown
 *
 * @example
 * ```typescript
 * @Injectable()
 * export class OrderService {
 *   constructor(private readonly matador: MatadorService) {}
 *
 *   async createOrder(data: CreateOrderDto) {
 *     // ... create order logic ...
 *     await this.matador.send(OrderCreatedEvent, { orderId: order.id });
 *   }
 * }
 * ```
 */
@Injectable()
export class MatadorService
  implements
    OnModuleInit,
    OnApplicationBootstrap,
    OnModuleDestroy,
    BeforeApplicationShutdown,
    OnApplicationShutdown
{
  private readonly logger = new Logger(MatadorService.name);
  private matador!: Matador;
  private isShuttingDown = false;
  private isStarted = false;

  constructor(
    @Inject(MATADOR_OPTIONS) private readonly options: MatadorModuleOptions,
    private readonly discoveryService: SubscriberDiscoveryService,
  ) {}

  /**
   * Called when MatadorModule is initialized.
   * Builds Matador instance and starts if startOn === 'onModuleInit'.
   */
  async onModuleInit(): Promise<void> {
    this.initializeMatador();

    if (this.shouldAutoStart() && this.options.startOn === 'onModuleInit') {
      await this.doStart();
    }
  }

  /**
   * Called after all modules are initialized and the app is ready to start.
   * Starts Matador if startOn === 'onApplicationBootstrap' (default).
   */
  async onApplicationBootstrap(): Promise<void> {
    const startOn = this.options.startOn ?? 'onApplicationBootstrap';
    if (this.shouldAutoStart() && startOn === 'onApplicationBootstrap') {
      await this.doStart();
    }
  }

  /**
   * Called when MatadorModule is destroyed.
   * Shuts down Matador if shutdownOn === 'onModuleDestroy'.
   */
  async onModuleDestroy(): Promise<void> {
    if (this.options.shutdownOn === 'onModuleDestroy') {
      await this.doShutdown();
    }
  }

  /**
   * Called when the application receives a shutdown signal (SIGTERM, etc).
   * Shuts down Matador if shutdownOn === 'beforeApplicationShutdown' (default).
   */
  async beforeApplicationShutdown(): Promise<void> {
    const shutdownOn = this.options.shutdownOn ?? 'beforeApplicationShutdown';
    if (shutdownOn === 'beforeApplicationShutdown') {
      await this.doShutdown();
    }
  }

  /**
   * Called after beforeApplicationShutdown completes.
   * Shuts down Matador if shutdownOn === 'onApplicationShutdown'.
   */
  async onApplicationShutdown(): Promise<void> {
    if (this.options.shutdownOn === 'onApplicationShutdown') {
      await this.doShutdown();
    }
  }

  /**
   * Sends an event to all registered subscribers.
   *
   * @throws Error if called during shutdown
   *
   * @example
   * ```typescript
   * // Pass the event class and data directly
   * await matadorService.send(UserCreatedEvent, { userId: '123' });
   *
   * // Or pass an event instance
   * const event = new UserCreatedEvent({ userId: '123' });
   * await matadorService.send(event);
   * ```
   */
  async send<T>(
    eventClass: EventClass<T>,
    data: T,
    options?: EventOptions,
  ): Promise<SendResult>;
  async send<T>(event: Event<T>, options?: EventOptions): Promise<SendResult>;
  async send<T>(
    eventOrClass: EventClass<T> | Event<T>,
    dataOrOptions?: T | EventOptions,
    options?: EventOptions,
  ): Promise<SendResult> {
    if (this.isShuttingDown) {
      throw new Error('Cannot send events during shutdown');
    }

    // Determine if first arg is an event instance or event class
    const isEventClass =
      typeof eventOrClass === 'function' && 'key' in eventOrClass;

    if (isEventClass) {
      return this.matador.send(
        eventOrClass as EventClass<T>,
        dataOrOptions as T,
        options,
      );
    }
    return this.matador.send(
      eventOrClass as Event<T>,
      dataOrOptions as EventOptions | undefined,
    );
  }

  /**
   * Reads the status records of a tracked send.
   * @returns A map from subscriber name to record, or undefined if no record exists
   * @throws StatusStoreRequiredError if no statusStore is configured
   */
  getStatus(
    trackingId: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | undefined>;
  /**
   * Reads the status record of one tracked subscriber.
   */
  getStatus(
    trackingId: string,
    subscriberName: string,
  ): Promise<StatusRecord | undefined>;
  async getStatus(
    trackingId: string,
    subscriberName?: string,
  ): Promise<
    Readonly<Record<string, StatusRecord>> | StatusRecord | undefined
  > {
    return subscriberName === undefined
      ? this.matador.getStatus(trackingId)
      : this.matador.getStatus(trackingId, subscriberName);
  }

  /**
   * Gets the underlying Matador instance for advanced operations.
   */
  getMatador(): Matador {
    return this.matador;
  }

  /**
   * Starts consuming (if autoStart was false).
   */
  async start(): Promise<void> {
    return this.doStart();
  }

  /**
   * Checks if connected to transport.
   */
  isConnected(): boolean {
    return this.matador?.isConnected() ?? false;
  }

  /**
   * Checks if shutdown is in progress.
   */
  isShutdownInProgress(): boolean {
    return this.isShuttingDown;
  }

  /**
   * Waits for all pending messages to be processed.
   */
  async waitForIdle(timeoutMs?: number): Promise<boolean> {
    return this.matador.waitForIdle(timeoutMs);
  }

  /**
   * Stops receiving new messages without performing full shutdown.
   * After calling this, no new messages will be delivered from the transport.
   * The transport remains connected for later shutdown.
   *
   * Use this when you need to coordinate shutdown across multiple systems:
   * 1. Call stopReceiving() to stop new message delivery
   * 2. Wait for both Matador and your other systems to idle
   * 3. Let NestJS lifecycle handle disconnect via app.close()
   *
   * @returns true if receiving was stopped, false if not started or already stopped
   *
   * @example
   * ```typescript
   * // In your graceful shutdown handler:
   * await matadorService.stopReceiving();
   * await Promise.all([
   *   matadorService.waitForIdle(30000),
   *   myOtherService.waitForPendingWork()
   * ]);
   * await app.close(); // NestJS lifecycle will disconnect transport
   * ```
   */
  async stopReceiving(): Promise<boolean> {
    if (!this.isStarted) {
      return false;
    }

    this.logger.log('[Matador] ⏳ Stopping message receiving');
    const result = await this.matador.stopReceiving();
    if (result) {
      this.logger.log('[Matador] 🟢 Message receiving stopped');
    }
    return result;
  }

  /**
   * Initializes the Matador instance with discovered schema.
   */
  private initializeMatador(): void {
    const mergedSchema = this.discoveryService.getMergedSchema(this.options);

    // Validate schema at startup
    const registry = new SchemaRegistry();
    for (const entry of Object.values(mergedSchema)) {
      if (isSchemaEntryTuple(entry)) {
        const [eventClass, subscribers] = entry;
        registry.register(eventClass, subscribers);
      } else {
        registry.register(entry.eventClass, entry.subscribers);
      }
    }

    const validation = registry.validate();
    if (!validation.valid) {
      const errors = validation.issues.filter((i) => i.severity === 'error');
      if (errors.length > 0) {
        throw new Error(
          `Invalid Matador schema: ${errors.map((i) => i.message).join(', ')}`,
        );
      }
    }

    this.matador = new Matador(
      {
        transport: this.options.transport,
        topology: this.options.topology,
        schema: mergedSchema,
        consumeFrom: this.options.consumeFrom
          ? [...this.options.consumeFrom]
          : undefined,
        codec: this.options.codec,
        retryPolicy: this.options.retryPolicy,
        checkpointStore: this.options.checkpointStore,
        statusStore: this.options.statusStore,
        shutdownConfig: this.options.shutdownConfig,
      },
      this.options.hooks,
    );

    this.logger.log('[Matador] 🟢 matador-nest initialized.');
  }

  private shouldAutoStart(): boolean {
    return this.options.autoStart !== false;
  }

  private async doStart(): Promise<void> {
    if (this.isStarted) {
      return;
    }

    await this.matador.start();
    this.isStarted = true;
    this.logger.log('[Matador] 🟢 Started');
  }

  private async doShutdown(): Promise<void> {
    if (!this.isStarted || this.isShuttingDown) {
      return;
    }

    this.isShuttingDown = true;
    this.logger.log(
      '[Matador] ⏳ Graceful shutdown initiated, draining in-flight messages',
    );

    await this.matador.shutdown();
    this.logger.log('[Matador] 🟢 Shutdown complete');
  }
}
