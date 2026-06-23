import type { HasDescription } from '../errors/index.js';
import type {
  DeadLetterConfig,
  QueueDefinition,
  RetryConfig,
  Topology,
  TopologyNaming,
} from './types.js';

/**
 * Options for adding a queue.
 * Excludes 'name' as that is provided as the first argument to addQueue.
 */
export type QueueOptions = Omit<QueueDefinition, 'name'>;

/**
 * Type guard to check if the argument is a QueueDefinition object.
 */
function isQueueDefinition(
  arg: string | QueueDefinition,
): arg is QueueDefinition {
  return typeof arg === 'object' && arg !== null && 'name' in arg;
}

/**
 * Error thrown when topology validation fails.
 */
export class TopologyValidationError extends Error implements HasDescription {
  readonly description =
    'The topology configuration is invalid. Check the issues array for ' +
    'specific validation failures such as missing namespace, invalid queue ' +
    'names, or conflicting settings. This error occurs during Matador ' +
    'initialization and must be fixed in the configuration.';

  constructor(
    message: string,
    public readonly issues: readonly string[],
  ) {
    super(message);
    this.name = 'TopologyValidationError';
  }
}

/**
 * Fluent builder for creating Topology configurations.
 */
export class TopologyBuilder {
  /**
   * Creates a new TopologyBuilder instance.
   */
  static create(): TopologyBuilder {
    return new TopologyBuilder();
  }

  private namespace = '';
  private queues: QueueDefinition[] = [];
  private deadLetter: DeadLetterConfig = {
    unhandled: { enabled: true },
    undeliverable: { enabled: true },
  };
  private retry: RetryConfig = {
    enabled: true,
    defaultDelayMs: 1000,
    maxDelayMs: 300000, // 5 minutes
  };
  private naming: TopologyNaming | undefined;
  private prefix: string | null = 'matador';

  /**
   * Sets the namespace prefix for all queues.
   */
  withNamespace(namespace: string): this {
    this.namespace = namespace;
    return this;
  }

  /**
   * Sets the prefix prepended to every default-derived broker resource name
   * (e.g. `matador.{namespace}.{queue}`), so Matador-managed queues and
   * exchanges are identifiable. Pass `null` to disable prefixing.
   *
   * Only applies to default names — a {@link withNaming} override fully owns
   * its output and is never prefixed.
   *
   * @default 'matador'
   * @example
   * TopologyBuilder.create().withNamespace('myapp')            // matador.myapp.events
   * TopologyBuilder.create().withNamespace('myapp').withPrefix('acme')  // acme.myapp.events
   * TopologyBuilder.create().withNamespace('myapp').withPrefix(null)    // myapp.events
   */
  withPrefix(prefix: string | null): this {
    this.prefix = prefix;
    return this;
  }

  /**
   * Overrides how broker resource names are derived.
   * Use during migrations to keep pre-existing queue/exchange names so a
   * rolling deploy keeps routing through the resources already declared on
   * the broker.
   * @see TopologyNaming
   */
  withNaming(naming: TopologyNaming): this {
    this.naming = naming;
    return this;
  }

  /**
   * Adds a queue to the topology.
   * @param definition - A complete QueueDefinition object
   */
  addQueue(definition: QueueDefinition): this;
  /**
   * Adds a queue to the topology.
   * @param name - Queue name
   * @param options - Queue options
   */
  addQueue(name: string, options?: QueueOptions): this;
  addQueue(
    nameOrDefinition: string | QueueDefinition,
    options: QueueOptions = {},
  ): this {
    if (isQueueDefinition(nameOrDefinition)) {
      this.queues.push(nameOrDefinition);
    } else {
      this.queues.push({ name: nameOrDefinition, ...options });
    }
    return this;
  }

  /**
   * Alias for addQueue().
   * @param definition - A complete QueueDefinition object
   */
  queue(definition: QueueDefinition): this;
  /**
   * Alias for addQueue().
   * @param name - Queue name
   * @param options - Queue options
   */
  queue(name: string, options?: QueueOptions): this;
  queue(
    nameOrDefinition: string | QueueDefinition,
    options: QueueOptions = {},
  ): this {
    if (isQueueDefinition(nameOrDefinition)) {
      return this.addQueue(nameOrDefinition);
    }
    return this.addQueue(nameOrDefinition, options);
  }

  /**
   * Configures dead-letter queue settings.
   */
  withDeadLetter(config: Partial<DeadLetterConfig>): this {
    this.deadLetter = {
      unhandled: config.unhandled ?? this.deadLetter.unhandled,
      undeliverable: config.undeliverable ?? this.deadLetter.undeliverable,
    };
    return this;
  }

  /**
   * Configures retry settings.
   */
  withRetry(config: Partial<RetryConfig>): this {
    this.retry = {
      enabled: config.enabled ?? this.retry.enabled,
      defaultDelayMs: config.defaultDelayMs ?? this.retry.defaultDelayMs,
      maxDelayMs: config.maxDelayMs ?? this.retry.maxDelayMs,
    };
    return this;
  }

  /**
   * Disables retry functionality.
   */
  withoutRetry(): this {
    this.retry = { ...this.retry, enabled: false };
    return this;
  }

  /**
   * Disables dead-letter queues.
   */
  withoutDeadLetter(): this {
    this.deadLetter = {
      unhandled: { enabled: false },
      undeliverable: { enabled: false },
    };
    return this;
  }

  /**
   * Validates the topology configuration.
   */
  validate(): readonly string[] {
    return [
      ...validateNamespace(this.namespace),
      ...validatePrefix(this.prefix),
      ...validateQueues(this.queues),
      ...validateRetry(this.retry),
      ...validateNaming(this.naming),
    ];
  }

  /**
   * Builds the topology configuration.
   * @throws TopologyValidationError if validation fails
   */
  build(): Topology {
    const issues = this.validate();
    if (issues.length > 0) {
      throw new TopologyValidationError(
        `Invalid topology: ${issues.join('; ')}`,
        issues,
      );
    }

    return {
      namespace: this.namespace,
      queues: [...this.queues],
      deadLetter: this.deadLetter,
      retry: this.retry,
      naming: this.naming,
      prefix: this.prefix,
    };
  }
}

const IDENTIFIER_PATTERN = /^[a-zA-Z][a-zA-Z0-9_-]*$/;

function validateNamespace(namespace: string): string[] {
  if (!namespace || namespace.trim() === '') {
    return ['Namespace is required'];
  }
  if (!IDENTIFIER_PATTERN.test(namespace)) {
    return [
      'Namespace must start with a letter and contain only alphanumeric characters, underscores, and hyphens',
    ];
  }
  return [];
}

function validatePrefix(prefix: string | null): string[] {
  // null explicitly disables prefixing.
  if (prefix === null) {
    return [];
  }
  if (prefix.trim() === '') {
    return ['Prefix must be a non-empty string, or null to disable prefixing'];
  }
  if (!IDENTIFIER_PATTERN.test(prefix)) {
    return [
      'Prefix must start with a letter and contain only alphanumeric characters, underscores, and hyphens',
    ];
  }
  return [];
}

function validateQueueName(
  queue: QueueDefinition,
  seen: Set<string>,
): string[] {
  if (!queue.name || queue.name.trim() === '') {
    return ['Queue name cannot be empty'];
  }
  if (!queue.exact && !IDENTIFIER_PATTERN.test(queue.name)) {
    return [
      `Queue name "${queue.name}" must start with a letter and contain only alphanumeric characters, underscores, and hyphens`,
    ];
  }
  if (seen.has(queue.name)) {
    return [`Duplicate queue name: "${queue.name}"`];
  }
  seen.add(queue.name);
  return [];
}

function validateQueueLimits(queue: QueueDefinition): string[] {
  const issues: string[] = [];
  if (queue.concurrency !== undefined && queue.concurrency < 1) {
    issues.push(`Queue "${queue.name}" concurrency must be at least 1`);
  }
  if (queue.consumerTimeout !== undefined && queue.consumerTimeout < 0) {
    issues.push(`Queue "${queue.name}" consumer timeout must be non-negative`);
  }
  return issues;
}

function validateQueues(queues: readonly QueueDefinition[]): string[] {
  const issues: string[] = [];
  if (queues.length === 0) {
    issues.push('At least one queue is required');
  }
  const seen = new Set<string>();
  for (const queue of queues) {
    issues.push(...validateQueueName(queue, seen));
    issues.push(...validateQueueLimits(queue));
  }
  return issues;
}

function validateNaming(naming: TopologyNaming | undefined): string[] {
  if (!naming) return [];
  const issues: string[] = [];
  for (const field of [
    'queue',
    'mainExchange',
    'dlxExchange',
    'delayedExchange',
  ] as const) {
    const value = naming[field];
    if (value !== undefined && typeof value !== 'function') {
      issues.push(`Naming override "${field}" must be a function`);
    }
  }
  if (
    naming.dlxExchangeType !== undefined &&
    naming.dlxExchangeType !== 'direct' &&
    naming.dlxExchangeType !== 'topic'
  ) {
    issues.push(
      `Naming override "dlxExchangeType" must be 'direct' or 'topic'`,
    );
  }
  return issues;
}

function validateRetry(retry: RetryConfig): string[] {
  if (!retry.enabled) return [];
  const issues: string[] = [];
  if (retry.defaultDelayMs < 0) {
    issues.push('Default retry delay must be non-negative');
  }
  if (retry.maxDelayMs < retry.defaultDelayMs) {
    issues.push(
      'Max retry delay must be greater than or equal to default delay',
    );
  }
  return issues;
}
