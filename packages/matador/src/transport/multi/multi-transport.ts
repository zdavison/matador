import { AllTransportsFailedError } from '../../errors/index.js';
import type { TransportFallbackContext } from '../../hooks/index.js';
import type { Topology } from '../../topology/types.js';
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
 * Symbol key used to store the source transport on wrapped receipts.
 */
const SOURCE_TRANSPORT = Symbol('sourceTransport');

/**
 * Extracts the source transport from a tagged receipt.
 */
function getSourceTransport(receipt: MessageReceipt): Transport | undefined {
  return (receipt as unknown as Record<symbol, Transport | undefined>)[
    SOURCE_TRANSPORT
  ];
}

/**
 * Configuration for MultiTransport.
 */
export interface MultiTransportConfig {
  /**
   * Ordered list of transports.
   * First transport is primary, rest are used as fallbacks when primary fails
   * (if fallback is enabled), or can be explicitly selected via the getDesiredBackend hook.
   */
  readonly transports: readonly Transport[];

  /**
   * Whether to automatically try fallback transports when the primary/selected fails.
   * When true (default), send() will try each transport in order until one succeeds.
   * When false, send() will only use the selected transport and throw immediately on failure.
   *
   * @default true
   */
  readonly fallbackEnabled?: boolean;
}

/**
 * Hooks for MultiTransport.
 */
export interface MultiTransportHooks {
  /**
   * Hook to dynamically select which backend to use.
   * Return the transport name (e.g., 'local', 'rabbitmq') or undefined to use primary.
   *
   * @example
   * ```typescript
   * getDesiredBackend: async () => {
   *   if (process.env.SANDBOX === 'true') return 'local';
   *   const backend = await runtimeConfig.get('events.backend');
   *   return backend || 'rabbitmq';
   * }
   * ```
   */
  readonly getDesiredBackend?: () =>
    | string
    | undefined
    | Promise<string | undefined>;

  /**
   * Called when a fallback transport is used successfully after the selected one fails.
   * Only called when fallbackEnabled is true.
   */
  readonly onEnqueueFallback?: (context: TransportFallbackContext) => void;
}

/**
 * Computes merged capabilities from multiple transports.
 * Uses the most restrictive/conservative interpretation.
 */
function mergeCapabilities(
  transports: readonly Transport[],
): TransportCapabilities {
  const primary = transports[0];
  if (!primary) {
    throw new Error('At least one transport is required');
  }

  // For multi-transport scenarios, we use primary's capabilities since that's
  // what we'll use for subscriptions and most operations
  return primary.capabilities;
}

/**
 * A transport wrapper that manages multiple transports.
 *
 * Features:
 * - All transports are connected upfront
 * - send() tries transports in order until one succeeds (fallback behavior)
 * - Specific transport can be selected via getDesiredBackend hook
 * - Subscriptions are created on ALL transports
 *
 * @example
 * ```typescript
 * const transport = new MultiTransport(
 *   { transports: [rabbitMQTransport, localTransport] },
 *   { onEnqueueFallback: (ctx) => console.warn(`Fallback to ${ctx.nextTransport}`) },
 * );
 * ```
 */
export class MultiTransport implements Transport {
  readonly name: string;
  readonly capabilities: TransportCapabilities;
  readonly primary: Transport;

  /** All available transports, in order of preference (primary first). */
  readonly transports: readonly Transport[];

  /** Whether fallback to secondary transports is enabled. */
  readonly fallbackEnabled: boolean;

  private readonly hooks: MultiTransportHooks;
  private connected = false;

  constructor(config: MultiTransportConfig, hooks: MultiTransportHooks = {}) {
    const primary = config.transports[0];
    if (!primary) {
      throw new Error('At least one transport is required');
    }

    this.primary = primary;
    this.transports = config.transports;
    this.fallbackEnabled = config.fallbackEnabled ?? true;
    this.hooks = hooks;
    this.name = `multi(${this.transports.map((t) => t.name).join(',')})`;
    this.capabilities = mergeCapabilities(this.transports);
  }

  async connect(): Promise<void> {
    // Connect all transports in parallel
    await Promise.all(this.transports.map((t) => t.connect()));
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    // Disconnect all transports in parallel
    await Promise.all(this.transports.map((t) => t.disconnect()));
    this.connected = false;
  }

  isConnected(): boolean {
    // Consider connected if primary is connected
    return this.connected && this.primary.isConnected();
  }

  /**
   * Registers a callback to fire each time the transport successfully (re)connects
   * @param callback - The callback to fire when the transport successfully (re)connects
   * @returns A function to unsubscribe from the callback
   */
  onConnected(callback: () => void): () => void {
    // This adds the callback on all transports that support it, and returns functions to unsubscribe from all transports
    const unsubFunctions = this.transports
      .map((t) => t.onConnected?.(callback))
      .filter(Boolean) as (() => void)[];

    // Return a function to unsubscribe from all transports
    return () => {
      for (const unsub of unsubFunctions) {
        unsub();
      }
    };
  }

  async applyTopology(topology: Topology): Promise<void> {
    // Apply topology to all transports so they're all ready
    await Promise.all(this.transports.map((t) => t.applyTopology(topology)));
  }

  async send(
    queue: string,
    envelope: Envelope,
    options?: SendOptions,
  ): Promise<Transport['name']> {
    // Determine which transport to use
    const selectedTransport = await this.selectTransport();

    // If fallback is disabled, only use selected transport
    if (!this.fallbackEnabled) {
      return selectedTransport.send(queue, envelope, options);
    }

    // Build transport order: selected first, then others
    const transportOrder = this.getTransportOrder(selectedTransport);

    const errors: Error[] = [];

    for (let i = 0; i < transportOrder.length; i++) {
      const transport = transportOrder[i];
      if (!transport) continue;
      try {
        return await transport.send(queue, envelope, options);
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        errors.push(err);

        // Notify about fallback before trying next transport
        const nextTransport = transportOrder[i + 1];
        if (nextTransport && this.hooks.onEnqueueFallback) {
          this.hooks.onEnqueueFallback({
            envelope,
            queue,
            failedTransport: transport.name,
            nextTransport: nextTransport.name,
            error: err,
          });
        }
        // Continue to next transport
      }
    }

    // All transports failed
    throw new AllTransportsFailedError(queue, errors);
  }

  /**
   * Selects the transport to use based on the getDesiredBackend hook.
   */
  private async selectTransport(): Promise<Transport> {
    if (!this.hooks.getDesiredBackend) {
      return this.primary;
    }

    try {
      const desiredBackend = await this.hooks.getDesiredBackend();
      if (!desiredBackend) {
        return this.primary;
      }

      const selected = this.transports.find((t) => t.name === desiredBackend);
      if (selected) {
        return selected;
      }

      // Desired backend not found, use primary
      return this.primary;
    } catch {
      // Hook threw, use primary
      return this.primary;
    }
  }

  /**
   * Returns transports in order, with the selected transport first.
   */
  private getTransportOrder(selected: Transport): readonly Transport[] {
    if (selected === this.primary) {
      return this.transports;
    }

    // Put selected first, then the rest (excluding selected)
    return [selected, ...this.transports.filter((t) => t !== selected)];
  }

  async subscribe(
    queue: string,
    handler: MessageHandler,
    options?: SubscribeOptions,
  ): Promise<Subscription> {
    // Subscribe on ALL transports so messages are processed regardless of
    // which transport they were enqueued to
    const subscriptions = await Promise.all(
      this.transports.map((transport) => {
        // Wrap handler to tag receipts with source transport
        const wrappedHandler: MessageHandler = (envelope, receipt) => {
          const taggedReceipt = Object.assign(receipt, {
            [SOURCE_TRANSPORT]: transport,
          });
          return handler(envelope, taggedReceipt);
        };
        return transport.subscribe(queue, wrappedHandler, options);
      }),
    );

    return {
      unsubscribe: async () => {
        await Promise.all(subscriptions.map((s) => s.unsubscribe()));
      },
      get isActive() {
        return subscriptions.some((s) => s.isActive);
      },
    };
  }

  async complete(receipt: MessageReceipt): Promise<void> {
    const transport = getSourceTransport(receipt) ?? this.primary;
    return transport.complete(receipt);
  }

  async sendToDeadLetter(
    receipt: MessageReceipt,
    dlqName: string,
    envelope: Envelope,
    reason: string,
  ): Promise<void> {
    const transport = getSourceTransport(receipt) ?? this.primary;
    if (transport.sendToDeadLetter) {
      return transport.sendToDeadLetter(receipt, dlqName, envelope, reason);
    }
    throw new Error('Transport does not support dead letter routing');
  }
}
