/**
 * Transport-agnostic topology definition.
 * Matador owns the topology; transports translate and apply it.
 */
export interface Topology {
  /** Namespace prefix for all queues */
  readonly namespace: string;

  /** Work queues for processing events */
  readonly queues: readonly QueueDefinition[];

  /** Dead-letter queue configuration */
  readonly deadLetter: DeadLetterConfig;

  /** Retry queue configuration */
  readonly retry: RetryConfig;

  /**
   * Optional overrides for how broker resource names are derived.
   * Intended for migrations: lets an adopter keep pre-existing queue and
   * exchange names (e.g. from Matador v1) so a rolling deploy keeps using
   * the broker resources already in place.
   */
  readonly naming?: TopologyNaming | undefined;
}

/**
 * Strategy for deriving broker resource names.
 *
 * Each builder receives the namespace as an explicit argument and returns the
 * final name, so the namespace is always an input to the strategy — it is
 * never applied separately on top of the result. Provide only the builders you
 * want to override; the rest keep Matador's defaults:
 *  - work queue: `${namespace}.${queueName}`
 *  - main exchange: `${namespace}.exchange`
 *  - dead-letter exchange: `${namespace}.dlx`
 *  - delayed exchange: `${namespace}.delayed`
 *
 * Retry queues and DLQs have no builder of their own: they are derived from the
 * work-queue name (`<queue>.retry`, `<queue>.unhandled`, `<queue>.undeliverable`),
 * so overriding `queue` carries your prefix into them automatically.
 *
 * Intended for migrations: an adopter can keep pre-existing names (e.g. from
 * Matador v1) so a rolling deploy keeps using the broker resources already in
 * place instead of draining or dual-consuming.
 *
 * @example Keep Matador v1 names (`matador.{namespace}.{queue}`):
 * ```typescript
 * TopologyBuilder.create()
 *   .withNamespace('myapp')
 *   .withNaming({
 *     queue: (ns, q) => `matador.${ns}.${q}`,
 *     mainExchange: (ns) => `matador.${ns}`,
 *     dlxExchange: (ns) => `matador.${ns}.dlx-undeliverable`,
 *     dlxExchangeType: 'topic',
 *     delayedExchange: (ns) => `matador.${ns}.delayed`,
 *   })
 * ```
 */
export interface TopologyNaming {
  /**
   * Builds the qualified work-queue name from the namespace and queue name.
   * Retry queues and DLQs are derived from the result automatically
   * (`<queue>.retry`, `<queue>.unhandled`, `<queue>.undeliverable`).
   * Default builds `${namespace}.${queueName}`.
   */
  readonly queue?:
    | ((namespace: string, queueName: string) => string)
    | undefined;

  /** Builds the main exchange name. Default builds `${namespace}.exchange`. */
  readonly mainExchange?: ((namespace: string) => string) | undefined;

  /** Builds the dead-letter exchange name. Default builds `${namespace}.dlx`. */
  readonly dlxExchange?: ((namespace: string) => string) | undefined;

  /**
   * Exchange type used when asserting the dead-letter exchange.
   * Useful when an existing DLX was declared as `topic` — asserting it as
   * `direct` would fail with PRECONDITION_FAILED. Literal (non-wildcard)
   * binding keys behave identically on a topic exchange.
   * Default: `'direct'`.
   */
  readonly dlxExchangeType?: 'direct' | 'topic' | undefined;

  /** Builds the delayed-message exchange name. Default builds `${namespace}.delayed`. */
  readonly delayedExchange?: ((namespace: string) => string) | undefined;
}

/**
 * Individual queue definition.
 */
export interface QueueDefinition {
  /** Queue name (will be prefixed with namespace unless exact: true) */
  readonly name: string;

  /** Concurrency for this queue */
  readonly concurrency?: number | undefined;

  /** Consumer timeout in milliseconds */
  readonly consumerTimeout?: number | undefined;

  /** Enable priority support if transport allows */
  readonly priorities?: boolean | undefined;

  /**
   * When true, the queue name is used exactly as provided without any
   * modification. The namespace prefix will NOT be added, and no other
   * transformations will be applied. Use this for referencing external
   * queues that are not managed by Matador.
   */
  readonly exact?: boolean | undefined;

  /** Transport-specific queue options */
  readonly transport?: TransportQueueOptions | undefined;
}

/**
 * Transport-specific queue options.
 * Each transport can define its own options under its transport name key.
 */
export interface TransportQueueOptions {
  /** RabbitMQ-specific queue options */
  readonly rabbitmq?: RabbitMQQueueDefinition | undefined;
}

/**
 * RabbitMQ-specific queue definition options.
 */
export interface RabbitMQQueueDefinition {
  /**
   * Exact RabbitMQ queue assertion options.
   * When provided, these options completely replace all auto-computed defaults
   * (durable, x-queue-type, x-dead-letter-exchange, etc.).
   */
  readonly options?: RabbitMQQueueOptions | undefined;
}

/**
 * RabbitMQ queue assertion options.
 * Maps to amqplib's Options.AssertQueue.
 */
export interface RabbitMQQueueOptions {
  /** Queue survives broker restart */
  readonly durable?: boolean | undefined;

  /** Queue is deleted when last consumer unsubscribes */
  readonly autoDelete?: boolean | undefined;

  /** Queue can only be used by the declaring connection */
  readonly exclusive?: boolean | undefined;

  /** Exchange to which dead-lettered messages are sent */
  readonly deadLetterExchange?: string | undefined;

  /** Routing key for dead-lettered messages */
  readonly deadLetterRoutingKey?: string | undefined;

  /** Message TTL in milliseconds */
  readonly messageTtl?: number | undefined;

  /** Queue expires after this many milliseconds of non-use */
  readonly expires?: number | undefined;

  /** Maximum number of messages in the queue */
  readonly maxLength?: number | undefined;

  /** Maximum priority level (0-255) */
  readonly maxPriority?: number | undefined;

  /** Additional x-* arguments for RabbitMQ */
  readonly arguments?: Record<string, unknown> | undefined;
}

/**
 * Dead-letter queue configuration.
 */
export interface DeadLetterConfig {
  /** Unhandled events (schema mismatch) queue */
  readonly unhandled: DeadLetterQueueConfig;

  /** Undeliverable events (permanent failures) queue */
  readonly undeliverable: DeadLetterQueueConfig;
}

/**
 * Configuration for a specific dead-letter queue.
 */
export interface DeadLetterQueueConfig {
  /** Whether this DLQ is enabled */
  readonly enabled: boolean;

  /** Maximum number of messages in the DLQ */
  readonly maxLength?: number | undefined;
}

/**
 * Retry queue configuration.
 */
export interface RetryConfig {
  /** Enable retry queue with delay */
  readonly enabled: boolean;

  /** Default retry delay in milliseconds */
  readonly defaultDelayMs: number;

  /** Maximum retry delay in milliseconds */
  readonly maxDelayMs: number;
}

/**
 * Gets the fully qualified work-queue name.
 * An optional naming strategy may override how the name is derived.
 */
export function getQualifiedQueueName(
  namespace: string,
  queueName: string,
  naming?: TopologyNaming,
): string {
  return naming?.queue?.(namespace, queueName) ?? `${namespace}.${queueName}`;
}

/**
 * Gets the dead-letter queue name for a given queue.
 */
export function getDeadLetterQueueName(
  namespace: string,
  queueName: string,
  dlqType: 'unhandled' | 'undeliverable',
  naming?: TopologyNaming,
): string {
  return `${getQualifiedQueueName(namespace, queueName, naming)}.${dlqType}`;
}

/**
 * Gets the retry queue name for a given queue.
 */
export function getRetryQueueName(
  namespace: string,
  queueName: string,
  naming?: TopologyNaming,
): string {
  return `${getQualifiedQueueName(namespace, queueName, naming)}.retry`;
}

/**
 * Resolves the actual queue name for a given queue definition.
 * When exact: true, returns name as-is. Otherwise, returns the
 * namespace-qualified name (honoring any naming overrides).
 */
export function resolveQueueName(
  namespace: string,
  queueDef: QueueDefinition,
  naming?: TopologyNaming,
): string {
  if (queueDef.exact) {
    return queueDef.name;
  }
  return getQualifiedQueueName(namespace, queueDef.name, naming);
}

/**
 * Finds the queue definition a local queue reference points at.
 */
export function findQueueDefinition(
  topology: Topology,
  queueName: string,
): QueueDefinition | undefined {
  return topology.queues.find((q) => q.name === queueName);
}

/**
 * Resolves a local queue reference (a subscriber's `targetQueue` or a
 * `consumeFrom` entry) to the transport-level queue name.
 *
 * A reference matching an exact queue's name resolves to that name as-is;
 * everything else is namespace-qualified (honoring any naming overrides).
 */
export function resolveTargetQueueName(
  topology: Topology,
  queueName: string,
): string {
  const def = findQueueDefinition(topology, queueName);
  if (def?.exact) {
    return def.name;
  }
  return getQualifiedQueueName(topology.namespace, queueName, topology.naming);
}
