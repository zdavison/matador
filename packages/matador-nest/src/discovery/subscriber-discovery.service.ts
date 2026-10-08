import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import {
  type AnySubscriber,
  type CallbackContext,
  type Envelope,
  type EventClass,
  type MatadorSchema,
  type ResumableCallbackContext,
  createSubscriber,
} from '@zdavison/matador';
import { MATADOR_EVENT_HANDLERS } from '../constants.js';
import type {
  MatadorEventHandlerMetadata,
  MatadorModuleOptions,
} from '../types.js';

/**
 * Schema entry tuple type for building the schema.
 */
type SchemaEntryTuple = [EventClass<unknown>, readonly AnySubscriber[]];

/**
 * Mutable schema type for building during discovery.
 */
type MutableSchema = Record<string, SchemaEntryTuple>;

/**
 * Service that discovers all @OnMatadorEvent decorated methods in the application
 * and builds a Matador schema from them.
 */
@Injectable()
export class SubscriberDiscoveryService implements OnModuleInit {
  private schema: MutableSchema = {};
  private discovered = false;

  constructor(
    @Inject(DiscoveryService)
    private readonly discoveryService: DiscoveryService,
  ) {}

  onModuleInit(): void {
    this.discoverSubscribers();
  }

  /**
   * Discovers all @OnMatadorEvent decorated methods and builds the schema.
   */
  private discoverSubscribers(): void {
    if (this.discovered) {
      return;
    }

    const providers = this.discoveryService.getProviders();

    for (const wrapper of providers) {
      const { instance } = wrapper;
      if (!instance || typeof instance !== 'object') {
        continue;
      }

      const handlers: MatadorEventHandlerMetadata[] | undefined =
        Reflect.getMetadata(MATADOR_EVENT_HANDLERS, instance.constructor);

      if (!handlers || handlers.length === 0) {
        continue;
      }

      for (const handler of handlers) {
        this.registerHandler(instance, handler);
      }
    }

    this.discovered = true;
  }

  /**
   * Registers a single handler method as a subscriber.
   */
  private registerHandler(
    instance: object,
    metadata: MatadorEventHandlerMetadata,
  ): void {
    const { eventClass, options, methodName } = metadata;
    const className = instance.constructor.name;

    // Auto-generate name if not provided: ClassName.methodName
    const subscriberName = options.name ?? `${className}.${methodName}`;

    // Determine if this is a resumable subscriber
    const isResumable = options.idempotent === 'resumable';

    // Create the callback that calls the instance method
    // biome-ignore lint/suspicious/noExplicitAny: Instance methods may have various signatures
    const boundMethod = (instance as Record<string, any>)[methodName].bind(
      instance,
    );

    // Create a subscriber that calls the instance method.
    // The method gets the context, and its return value is the subscriber result.
    const subscriber: AnySubscriber = isResumable
      ? createSubscriber({
          name: subscriberName,
          description: options.description,
          idempotent: 'resumable',
          importance: options.importance,
          targetQueue: options.targetQueue,
          enabled: options.enabled,
          track: options.track,
          callback: async (
            envelope: Envelope<unknown>,
            context: ResumableCallbackContext,
          ) => {
            return await boundMethod(envelope, context);
          },
        })
      : createSubscriber({
          name: subscriberName,
          description: options.description,
          idempotent: options.idempotent,
          importance: options.importance,
          targetQueue: options.targetQueue,
          enabled: options.enabled,
          track: options.track,
          callback: async (
            envelope: Envelope<unknown>,
            context: CallbackContext,
          ) => {
            return await boundMethod(envelope, context);
          },
        });

    // Add to schema
    this.addToSchema(eventClass, subscriber);
  }

  /**
   * Adds a subscriber to the schema for an event.
   */
  private addToSchema(
    eventClass: EventClass<unknown>,
    subscriber: AnySubscriber,
  ): void {
    const eventKey = eventClass.key;
    const existing = this.schema[eventKey];

    if (existing) {
      // Add subscriber to existing event entry
      const [existingEventClass, existingSubscribers] = existing;
      this.schema[eventKey] = [
        existingEventClass,
        [...existingSubscribers, subscriber],
      ];
    } else {
      // Create new entry
      this.schema[eventKey] = [eventClass, [subscriber]];
    }
  }

  /**
   * Gets the discovered schema.
   * Must be called after onModuleInit has run.
   */
  getSchema(): MatadorSchema {
    return this.schema;
  }

  /**
   * Gets the schema merged with additional events from options.
   */
  getMergedSchema(options: MatadorModuleOptions): MatadorSchema {
    const mergedSchema: MutableSchema = { ...this.schema };

    if (options.additionalEvents) {
      for (const [eventClass, subscribers] of options.additionalEvents) {
        const existing = mergedSchema[eventClass.key];
        if (existing) {
          const [existingEventClass, existingSubscribers] = existing;
          mergedSchema[eventClass.key] = [
            existingEventClass,
            [...existingSubscribers, ...subscribers],
          ];
        } else {
          mergedSchema[eventClass.key] = [eventClass, subscribers];
        }
      }
    }

    return mergedSchema;
  }
}
