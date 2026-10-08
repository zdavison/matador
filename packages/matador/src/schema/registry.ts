import type { HasDescription } from '../errors/index.js';
import type {
  AnySubscriber,
  EventClass,
  MatadorEvent,
  Subscriber,
  SubscriberDefinition,
} from '../types/index.js';
import { isSubscriber } from '../types/index.js';
import type {
  RegisterOptions,
  SchemaEntry,
  SchemaIssue,
  SchemaValidationResult,
} from './types.js';

/**
 * Error thrown when schema operations fail.
 */
export class SchemaError extends Error implements HasDescription {
  readonly description =
    'A schema operation failed, such as registering a duplicate event or ' +
    'conflicting alias. This is a configuration error that should be fixed ' +
    'during development. Review the event and subscriber registrations to ' +
    'resolve the conflict.';

  constructor(message: string) {
    super(message);
    this.name = 'SchemaError';
  }
}

/**
 * Registry for managing event-subscriber relationships.
 */
export class SchemaRegistry {
  private readonly entries = new Map<string, SchemaEntry>();
  private readonly aliases = new Map<string, string>();

  /**
   * Registers an event class with its subscribers.
   */
  register<T>(
    eventClass: EventClass<T>,
    subscribers: readonly AnySubscriber[],
    options: RegisterOptions = {},
  ): void {
    const key = eventClass.key;

    if (this.entries.has(key) && !options.override) {
      throw new SchemaError(`Event "${key}" is already registered`);
    }

    this.entries.set(key, {
      eventClass,
      subscribers,
    } as SchemaEntry);

    // Register aliases
    if (eventClass.aliases) {
      for (const alias of eventClass.aliases) {
        if (
          this.aliases.has(alias) &&
          this.aliases.get(alias) !== key &&
          !options.override
        ) {
          throw new SchemaError(
            `Alias "${alias}" is already registered for event "${this.aliases.get(alias)}"`,
          );
        }
        this.aliases.set(alias, key);
      }
    }
  }

  /**
   * Gets the event class for a given key.
   */
  getEventClass(key: string): EventClass | undefined {
    const entry = this.entries.get(key);
    if (entry) return entry.eventClass;

    // Check aliases
    const aliasKey = this.aliases.get(key);
    if (aliasKey) {
      return this.entries.get(aliasKey)?.eventClass;
    }

    return undefined;
  }

  /**
   * Gets all subscribers for an event key.
   */
  getSubscribers(eventKey: string): readonly AnySubscriber[] {
    const entry = this.getEntry(eventKey);
    return entry?.subscribers ?? [];
  }

  /**
   * Gets a specific subscriber by event key and subscriber name.
   */
  getSubscriber(
    eventKey: string,
    subscriberName: string,
  ): AnySubscriber | undefined {
    const subscribers = this.getSubscribers(eventKey);
    return subscribers.find((s) => s.name === subscriberName);
  }

  /**
   * Gets a subscriber definition for the pipeline.
   */
  getSubscriberDefinition(
    eventKey: string,
    subscriberName: string,
  ): SubscriberDefinition | undefined {
    const subscriber = this.getSubscriber(eventKey, subscriberName);
    if (!subscriber) return undefined;

    const def: SubscriberDefinition = {
      name: subscriber.name,
      idempotent: subscriber.idempotent ?? 'unknown',
      importance: subscriber.importance ?? 'should-investigate',
      ...('description' in subscriber &&
        subscriber.description !== undefined && {
          description: subscriber.description,
        }),
    };

    if (subscriber.targetQueue !== undefined) {
      (def as { targetQueue?: string }).targetQueue = subscriber.targetQueue;
    }

    if (subscriber.track !== undefined) {
      (def as { track?: boolean }).track = subscriber.track;
    }

    return def;
  }

  /**
   * Gets a subscriber with callback for execution.
   */
  getExecutableSubscriber(
    eventKey: string,
    subscriberName: string,
  ): Subscriber<MatadorEvent<unknown>> | undefined {
    const subscriber = this.getSubscriber(eventKey, subscriberName);
    if (!subscriber || !isSubscriber(subscriber)) return undefined;
    return subscriber;
  }

  /**
   * Gets the event class by alias.
   */
  getEventByAlias(alias: string): EventClass | undefined {
    const key = this.aliases.get(alias);
    if (!key) return undefined;
    return this.entries.get(key)?.eventClass;
  }

  /**
   * Checks if an event key is registered.
   */
  hasEvent(key: string): boolean {
    return this.entries.has(key) || this.aliases.has(key);
  }

  /**
   * Gets all registered event keys.
   */
  getEventKeys(): readonly string[] {
    return [...this.entries.keys()];
  }

  /**
   * Validates the schema for issues.
   */
  validate(): SchemaValidationResult {
    const issues: SchemaIssue[] = [];

    for (const [key, entry] of this.entries) {
      // Check for duplicate subscriber names
      const names = new Set<string>();
      for (const subscriber of entry.subscribers) {
        if (names.has(subscriber.name)) {
          issues.push({
            severity: 'error',
            eventKey: key,
            message: `Duplicate subscriber name: "${subscriber.name}"`,
          });
        }
        names.add(subscriber.name);
      }

      // Check for subscribers without callbacks (stubs in consuming instance)
      const stubs = entry.subscribers.filter((s) => !isSubscriber(s));
      if (stubs.length === entry.subscribers.length && stubs.length > 0) {
        issues.push({
          severity: 'warning',
          eventKey: key,
          message: 'All subscribers are stubs, no local handlers',
        });
      }
    }

    return {
      valid: issues.filter((i) => i.severity === 'error').length === 0,
      issues,
    };
  }

  /**
   * Clears all registered entries.
   */
  clear(): void {
    this.entries.clear();
    this.aliases.clear();
  }

  private getEntry(eventKey: string): SchemaEntry | undefined {
    const entry = this.entries.get(eventKey);
    if (entry) return entry;

    const aliasKey = this.aliases.get(eventKey);
    if (aliasKey) {
      return this.entries.get(aliasKey);
    }

    return undefined;
  }
}
