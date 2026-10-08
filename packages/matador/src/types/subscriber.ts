import type {
  JsonSerializable,
  SubscriberContext,
} from '../checkpoint/index.js';
import type { Idempotency, Importance } from './common.js';
import type { Dispatcher } from './dispatcher.js';
import type { Envelope } from './envelope.js';
import type { MatadorEvent } from './event.js';

/**
 * Context passed to subscriber callbacks.
 * Provides access to the Matador instance for sending additional events.
 */
export interface CallbackContext {
  /** Matador dispatcher for sending additional events from within a subscriber */
  readonly matador: Dispatcher;

  /**
   * Report progress. Returns at once and does not wait for the store.
   * Does nothing if the subscriber does not have track: true.
   * @param percent - A number from 0 to 100. Values outside the range are clamped.
   * @param details - Optional small JSON value, for example counters.
   */
  progress(percent: number, details?: JsonSerializable): void;
}

/**
 * Helper type to get the envelope type for a subscriber callback.
 * Extracts the data type from a MatadorEvent and wraps it in an Envelope.
 *
 * @example
 * async callback(envelope: EnvelopeOf<MyEvent>) {
 *   console.log(envelope.data.someField); // Type-safe access
 * }
 */
export type EnvelopeOf<T extends MatadorEvent> = Envelope<T['data']>;

/**
 * Callback function executed when an event is received (standard subscribers).
 * Receives the full envelope containing id, data, and docket, plus a context
 * with access to the matador instance for sending additional events.
 *
 * The callback may optionally return a value, which will be included in the
 * onWorkerSuccess hook context for logging or observability purposes.
 */
export type StandardCallback<T = unknown> = (
  envelope: Envelope<T>,
  context: CallbackContext,
) => Promise<unknown> | unknown | undefined;

/**
 * Context for resumable subscriber callbacks.
 * Combines checkpoint operations (io, all) with matador access.
 */
export type ResumableCallbackContext = SubscriberContext & CallbackContext;

/**
 * Callback function for resumable subscribers.
 * Receives the envelope and a context with io() for checkpointed operations
 * and matador for sending additional events.
 *
 * The callback may optionally return a value, which will be included in the
 * onWorkerSuccess hook context for logging or observability purposes.
 */
export type ResumableCallback<T = unknown> = (
  envelope: Envelope<T>,
  context: ResumableCallbackContext,
) => Promise<unknown> | unknown | undefined;

/**
 * Callback function executed when an event is received.
 * @deprecated Use StandardCallback or ResumableCallback instead.
 */
export type SubscriberCallback<T = unknown> = StandardCallback<T>;

/**
 * Base configuration options shared by all subscriber types.
 */
export interface BaseSubscriberOptions {
  /** Human-readable description of what this subscriber does */
  readonly description: string;

  /** Route this subscriber's events to a specific queue */
  readonly targetQueue?: string | undefined;

  /** Importance level for monitoring and alerting */
  readonly importance?: Importance | undefined;

  /** Feature flag function to conditionally enable/disable the subscriber */
  readonly enabled?: (() => boolean | Promise<boolean>) | undefined;

  /**
   * If true, Matador keeps a status record for each send to this subscriber.
   * Requires a statusStore in the Matador config.
   */
  readonly track?: boolean | undefined;
}

/**
 * Options for standard (non-resumable) subscribers.
 */
export interface StandardSubscriberOptions extends BaseSubscriberOptions {
  /** Idempotency declaration for retry handling (non-resumable) */
  readonly idempotent?: 'yes' | 'no' | 'unknown' | undefined;
}

/**
 * Options for resumable subscribers that use io() for checkpointed operations.
 */
export interface ResumableSubscriberOptions extends BaseSubscriberOptions {
  /** Must be 'resumable' to enable checkpoint-based idempotency */
  readonly idempotent: 'resumable';
}

/**
 * Configuration options for a subscriber.
 * Discriminated union based on idempotent value.
 */
export type SubscriberOptions =
  | StandardSubscriberOptions
  | ResumableSubscriberOptions;

/**
 * Standard subscriber definition with standard callback.
 */
export interface StandardSubscriber<T extends MatadorEvent>
  extends StandardSubscriberOptions {
  /** Human-readable name for the subscriber */
  readonly name: string;

  /** Callback function to execute when event is received */
  readonly callback: StandardCallback<T['data']>;
}

/**
 * Resumable subscriber definition with resumable callback.
 */
export interface ResumableSubscriber<T extends MatadorEvent>
  extends ResumableSubscriberOptions {
  /** Human-readable name for the subscriber */
  readonly name: string;

  /** Callback function with SubscriberContext for checkpointed operations */
  readonly callback: ResumableCallback<T['data']>;
}

/**
 * Full subscriber definition with callback (either standard or resumable).
 */
export type Subscriber<T extends MatadorEvent> =
  | StandardSubscriber<T>
  | ResumableSubscriber<T>;

/**
 * Subscriber stub for multi-codebase scenarios where subscriber implementation
 * is in a remote service. Declares the subscriber contract without providing
 * the callback.
 */
export interface SubscriberStub
  extends Omit<StandardSubscriberOptions, 'description'> {
  /** Human-readable name for the subscriber */
  readonly name: string;

  /** Indicates this is a stub without implementation */
  readonly isStub: true;
}

/**
 * Union type for any subscriber definition (full or stub).
 * This is the type-erased version for use in collections and schema.
 * Uses `any` because Subscriber<T> is contravariant in T (callback parameter),
 * making it impossible to assign Subscriber<SpecificEvent> to Subscriber<MatadorEvent<unknown>>.
 */
// biome-ignore lint/suspicious/noExplicitAny: Required for variance compatibility in heterogeneous collections
export type AnySubscriber = Subscriber<MatadorEvent<any>> | SubscriberStub;

/**
 * Type guard to check if a subscriber is a stub.
 */
export function isSubscriberStub(
  subscriber: AnySubscriber,
): subscriber is SubscriberStub {
  return 'isStub' in subscriber && subscriber.isStub === true;
}

/**
 * Type guard to check if a subscriber has a callback implementation.
 */
export function isSubscriber(
  subscriber: AnySubscriber,
  // biome-ignore lint/suspicious/noExplicitAny: Required for variance compatibility
): subscriber is Subscriber<MatadorEvent<any>> {
  return 'callback' in subscriber && typeof subscriber.callback === 'function';
}

/**
 * Type guard to check if a subscriber is resumable.
 */
export function isResumableSubscriber(
  subscriber: AnySubscriber,
  // biome-ignore lint/suspicious/noExplicitAny: Required for variance compatibility
): subscriber is ResumableSubscriber<MatadorEvent<any>> {
  return isSubscriber(subscriber) && subscriber.idempotent === 'resumable';
}

/**
 * Type guard to check if a subscriber is a standard (non-resumable) subscriber.
 */
export function isStandardSubscriber(
  subscriber: AnySubscriber,
  // biome-ignore lint/suspicious/noExplicitAny: Required for variance compatibility
): subscriber is StandardSubscriber<MatadorEvent<any>> {
  return isSubscriber(subscriber) && subscriber.idempotent !== 'resumable';
}

/**
 * Input options for createSubscriber with standard callback.
 */
export interface CreateStandardSubscriberInput<T extends MatadorEvent> {
  readonly name: string;
  readonly description: string;
  readonly callback: StandardCallback<T['data']>;
  readonly idempotent?: 'yes' | 'no' | 'unknown' | undefined;
  readonly importance?: Importance | undefined;
  readonly targetQueue?: string | undefined;
  readonly enabled?: (() => boolean | Promise<boolean>) | undefined;
  readonly track?: boolean | undefined;
}

/**
 * Input options for createSubscriber with resumable callback.
 */
export interface CreateResumableSubscriberInput<T extends MatadorEvent> {
  readonly name: string;
  readonly description: string;
  readonly callback: ResumableCallback<T['data']>;
  readonly idempotent: 'resumable';
  readonly importance?: Importance | undefined;
  readonly targetQueue?: string | undefined;
  readonly enabled?: (() => boolean | Promise<boolean>) | undefined;
  readonly track?: boolean | undefined;
}

/**
 * Input options for createSubscriber (discriminated union).
 */
export type CreateSubscriberInput<T extends MatadorEvent> =
  | CreateStandardSubscriberInput<T>
  | CreateResumableSubscriberInput<T>;

/**
 * Creates a subscriber definition.
 *
 * @example Standard subscriber
 * ```typescript
 * const subscriber = createSubscriber<MyEvent>({
 *   name: 'my-subscriber',
 *   description: 'Handles MyEvent by logging the data',
 *   callback: async (envelope) => {
 *     console.log(envelope.data);
 *   },
 * });
 * ```
 *
 * @example Resumable subscriber with io()
 * ```typescript
 * const subscriber = createSubscriber<MyEvent>({
 *   name: 'my-resumable-subscriber',
 *   description: 'Processes MyEvent with checkpoint-based idempotency',
 *   idempotent: 'resumable',
 *   callback: async (envelope, { io }) => {
 *     await io('step-1', () => doSomething());
 *   },
 * });
 * ```
 */
export function createSubscriber<T extends MatadorEvent>(
  input: CreateSubscriberInput<T>,
): Subscriber<T> {
  const base = {
    name: input.name,
    description: input.description,
    callback: input.callback,
    importance: input.importance ?? 'should-investigate',
    ...(input.targetQueue !== undefined && {
      targetQueue: input.targetQueue,
    }),
    ...(input.enabled !== undefined && { enabled: input.enabled }),
    ...(input.track !== undefined && { track: input.track }),
  };

  if (input.idempotent === 'resumable') {
    return {
      ...base,
      idempotent: 'resumable',
      callback: input.callback,
    } as ResumableSubscriber<T>;
  }

  return {
    ...base,
    idempotent: input.idempotent ?? 'unknown',
    callback: input.callback,
  } as StandardSubscriber<T>;
}

/**
 * Input options for createSubscriberStub.
 */
export interface CreateSubscriberStubInput {
  readonly name: string;
  readonly idempotent?: 'yes' | 'no' | 'unknown' | undefined;
  readonly importance?: Importance | undefined;
  readonly targetQueue?: string | undefined;
  readonly enabled?: (() => boolean | Promise<boolean>) | undefined;
  readonly track?: boolean | undefined;
}

/**
 * Creates a subscriber stub for remote implementations.
 *
 * @example
 * ```typescript
 * const stub = createSubscriberStub({
 *   name: 'remote-analytics',
 *   targetQueue: 'analytics-worker',
 * });
 * ```
 */
export function createSubscriberStub(
  input: CreateSubscriberStubInput,
): SubscriberStub {
  return {
    name: input.name,
    isStub: true,
    idempotent: input.idempotent ?? 'unknown',
    importance: input.importance ?? 'should-investigate',
    ...(input.targetQueue !== undefined && {
      targetQueue: input.targetQueue,
    }),
    ...(input.enabled !== undefined && { enabled: input.enabled }),
    ...(input.track !== undefined && { track: input.track }),
  };
}

/**
 * Definition interface used by the pipeline (excludes event class reference).
 */
export interface SubscriberDefinition {
  readonly name: string;
  readonly description?: string | undefined;
  readonly idempotent: Idempotency;
  readonly importance: Importance;
  readonly targetQueue?: string | undefined;
  readonly track?: boolean | undefined;
}
