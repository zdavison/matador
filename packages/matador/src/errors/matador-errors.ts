/**
 * Base class for all Matador errors.
 * All errors have a unique name that appears in monitoring tools (e.g., DataDog)
 * and a description explaining the error and actions to resolve it.
 *
 * The `name` property is automatically set to the class name for proper
 * identification in error monitoring and logging systems.
 */
export abstract class MatadorError extends Error {
  /**
   * Error class name (e.g., "NotStartedError", "TransportNotConnectedError").
   * This is preserved during serialization for error monitoring tools.
   */
  declare readonly name: string;

  /**
   * Human-readable description explaining what went wrong and
   * what ACTION the user should take to resolve the issue.
   */
  abstract readonly description: string;

  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    // Ensure name is preserved when serialized
    Object.defineProperty(this, 'name', {
      value: this.constructor.name,
      enumerable: true,
      configurable: false,
      writable: false,
    });
  }

  /**
   * Returns a serializable representation for logging/monitoring.
   */
  toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      message: this.message,
      description: this.description,
      stack: this.stack,
    };
  }
}

// ============================================================================
// Lifecycle Errors
// ============================================================================

/**
 * Thrown when attempting to use Matador before calling start().
 */
export class NotStartedError extends MatadorError {
  readonly description =
    'Matador has not been started. ' +
    'ACTION: Call matador.start() before dispatching events or performing other operations. ' +
    'Ensure start() completes successfully before using other methods.';

  constructor(operation = 'operation') {
    super(
      `Cannot perform ${operation}: Matador has not been started. Call start() first.`,
    );
  }
}

/**
 * Thrown when attempting to dispatch events during shutdown.
 */
export class ShutdownInProgressError extends MatadorError {
  readonly description =
    'Matador is shutting down and will not accept new events. ' +
    'ACTION: Do not dispatch events after calling shutdown(). ' +
    'If you need to send events, do so before initiating shutdown. ' +
    'Consider implementing a pre-shutdown event flush if needed.';

  constructor() {
    super('Cannot dispatch events: Matador is shutting down.');
  }
}

// ============================================================================
// Transport Errors
// ============================================================================

/**
 * Thrown when a transport operation is attempted but the transport is not connected.
 */
export class TransportNotConnectedError extends MatadorError {
  readonly description =
    'The transport is not connected to the message broker. ' +
    'ACTION: Ensure the transport is connected by calling transport.connect() or matador.start(). ' +
    'Check that the broker (e.g., RabbitMQ) is running and accessible. ' +
    'Verify connection settings (URL, credentials, network access).';

  constructor(
    public readonly transportName: string,
    operation = 'operation',
  ) {
    super(
      `Cannot perform ${operation}: Transport "${transportName}" is not connected. Ensure connect() was called and the broker is accessible.`,
    );
  }
}

/**
 * Thrown when the transport has been closed (during shutdown).
 */
export class TransportClosedError extends MatadorError {
  readonly description =
    'The transport has been closed and will not accept new operations. ' +
    'ACTION: This typically occurs during application shutdown. ' +
    'If unexpected, check for early shutdown triggers. ' +
    'Events sent after transport closure will be lost.';

  constructor(public readonly transportName: string) {
    super(
      `Transport "${transportName}" has been closed and will not accept new operations.`,
    );
  }
}

/**
 * Thrown when all transports in a fallback chain fail.
 */
export class AllTransportsFailedError extends MatadorError {
  readonly description =
    'All transports failed to send the message. ' +
    'ACTION: Check the health of all configured transports (primary and fallbacks). ' +
    'Review the errors array for specific failure reasons. ' +
    'Ensure at least one transport is properly configured and reachable. ' +
    'Consider adding a LocalTransport as a last-resort fallback.';

  constructor(
    public readonly queue: string,
    public readonly errors: readonly Error[],
  ) {
    super(
      `All transports failed to send message to queue "${queue}". ` +
        `Errors: ${errors.map((e) => e.message).join('; ')}`,
    );
  }
}

/**
 * Thrown when sending a message to the transport fails.
 */
export class TransportSendError extends MatadorError {
  readonly description =
    'Failed to send a message through the transport. ' +
    'ACTION: Check the underlying error for details. Common causes: ' +
    '(1) Transport disconnected during send, ' +
    '(2) Network issues between application and broker, ' +
    '(3) Broker rejected the message (size, permissions, queue limits). ' +
    'The message was NOT delivered and should be retried or logged.';

  constructor(
    public readonly queue: string,
    public readonly cause: Error,
  ) {
    super(`Failed to send message to queue "${queue}": ${cause.message}`);
  }

  override toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      queue: this.queue,
      cause: {
        name: this.cause.name,
        message: this.cause.message,
      },
    };
  }
}

/**
 * Thrown when send() completes but one or more subscriber publishes failed.
 * The `errors` array contains per-failure details: subscriber name, target queue, and underlying error.
 */
export class SomeSendError extends MatadorError {
  readonly description =
    'One or more subscriber messages could not be published during send(). ' +
    'ACTION: Inspect the `errors` array for per-failure details. ' +
    'Each entry contains the subscriber name, target queue, and underlying error.';

  constructor(
    public readonly eventKey: string,
    public readonly errors: ReadonlyArray<{
      subscriberName: string;
      queue: string;
      error: Error;
    }>,
  ) {
    const summary = errors
      .map((e) => `${e.subscriberName} (${e.queue}): ${e.error.message}`)
      .join('; ');
    super(
      `send("${eventKey}") failed for ${errors.length} subscriber(s): ${summary}`,
    );
  }
}

/**
 * Thrown when delayed messages are requested but the plugin is not available.
 */
export class DelayedMessagesNotSupportedError extends MatadorError {
  readonly description =
    'Delayed messages were requested but the transport does not support them. ' +
    'ACTION: For RabbitMQ, install the rabbitmq_delayed_message_exchange plugin. ' +
    'Run: rabbitmq-plugins enable rabbitmq_delayed_message_exchange ' +
    'Then restart RabbitMQ and reconnect. ' +
    'Alternatively, remove delayMs from your event options if delays are not required.';

  constructor(public readonly transportName: string) {
    super(
      'Delayed messages require the RabbitMQ delayed message exchange plugin. ' +
        'Install rabbitmq_delayed_message_exchange or remove delayMs from event options.',
    );
  }
}

// ============================================================================
// Schema & Configuration Errors
// ============================================================================

/**
 * Thrown when an event is not registered in the schema.
 */
export class EventNotRegisteredError extends MatadorError {
  readonly description =
    'The event type is not registered in the schema. ' +
    'ACTION: Register the event using matador.register(EventClass, subscribers) before dispatching. ' +
    'If this occurs during message consumption, it may indicate schema drift between services. ' +
    'Ensure all services have matching schema registrations for shared events.';

  constructor(public readonly eventKey: string) {
    super(
      `Event "${eventKey}" is not registered in schema. Register it using matador.register(EventClass, subscribers).`,
    );
  }
}

/**
 * Thrown when a subscriber is not found for an event.
 */
export class SubscriberNotRegisteredError extends MatadorError {
  readonly description =
    'The subscriber is not registered for this event in the schema. ' +
    'ACTION: Ensure the subscriber is included in the registration for this event. ' +
    'This may occur if: (1) The subscriber was removed from the schema but messages still exist, ' +
    '(2) Schema drift between producer and consumer services, ' +
    '(3) A deployment is in progress with different schema versions. ' +
    'Check the dead-letter queue for affected messages.';

  constructor(
    public readonly subscriberName: string,
    public readonly eventKey?: string,
  ) {
    super(
      `Subscriber "${subscriberName}" is not registered${eventKey ? ` for event "${eventKey}"` : ''}. Check schema registration.`,
    );
  }
}

/**
 * Thrown when no subscribers exist for an event during fanout.
 */
export class NoSubscribersExistError extends MatadorError {
  readonly description =
    'The event has no subscribers registered. ' +
    'ACTION: Register at least one subscriber for this event type. ' +
    'If subscribers were intentionally removed, consider also removing the event dispatch. ' +
    'Events without subscribers are not useful and may indicate configuration issues.';

  constructor(public readonly eventKey: string) {
    super(
      `No subscribers registered for event "${eventKey}". Add subscribers using matador.register(EventClass, [subscriber1, subscriber2]).`,
    );
  }
}

/**
 * Thrown when the schema configuration is invalid.
 */
export class InvalidSchemaError extends MatadorError {
  readonly description =
    'The schema configuration is invalid. ' +
    'ACTION: Review the schema registration for issues. Common problems: ' +
    '(1) Duplicate subscriber names for the same event, ' +
    '(2) Missing required fields on event class (key, description), ' +
    '(3) Invalid alias configuration. ' +
    'Check the cause property for specific details.';

  constructor(
    message: string,
    public readonly cause?: string,
  ) {
    super(`Invalid schema: ${message}${cause ? `. Cause: ${cause}` : ''}`);
  }
}

/**
 * Thrown when a subscriber is a stub but is being processed locally.
 */
export class SubscriberIsStubError extends MatadorError {
  readonly description =
    'A SubscriberStub was registered in a consuming schema. ' +
    'ACTION: SubscriberStubs should only be used in producer schemas to declare ' +
    'that a subscriber exists in another service. ' +
    'In the consumer service, provide a full Subscriber with a callback function. ' +
    'Remove the stub from the consumer schema and add the actual implementation.';

  constructor(public readonly subscriberName: string) {
    super(
      `Subscriber "${subscriberName}" is a stub and cannot be processed locally. Replace with a full Subscriber implementation in the consumer schema.`,
    );
  }
}

/**
 * Thrown when a LocalTransport tries to process a stub subscriber.
 */
export class LocalTransportCannotProcessStubError extends MatadorError {
  readonly description =
    'The LocalTransport cannot process events for SubscriberStubs. ' +
    'ACTION: SubscriberStubs represent remote implementations that only RabbitMQ can route. ' +
    'If using LocalTransport for testing, provide mock implementations instead of stubs. ' +
    'For production fallback scenarios, be aware that stub-targeted events will be dropped.';

  constructor(public readonly subscriberName: string) {
    super(
      `LocalTransport cannot process stub subscriber "${subscriberName}". Stub subscribers require a distributed transport like RabbitMQ.`,
    );
  }
}

// ============================================================================
// Queue Errors
// ============================================================================

/**
 * Thrown when a subscriber's `targetQueue` or a `consumeFrom` entry references
 * a queue that was never declared in the topology.
 */
export class UnknownQueueReferenceError extends MatadorError {
  readonly description =
    'A queue was referenced that is not declared in the topology. ' +
    'Matador only routes to queues it knows about; an unknown reference would ' +
    'otherwise be silently namespace-qualified and published to a queue nobody ' +
    'consumes, losing the message. ' +
    'ACTION: Declare the queue on the topology before referencing it. ' +
    'For a Matador-owned queue use `.addQueue(name)`; for a foreign queue owned ' +
    'by another service use `.addQueue({ name, exact: true })` so it is routed ' +
    'to verbatim.';

  constructor(public readonly queueName: string) {
    super(
      `Queue reference "${queueName}" is not declared in the topology. Add it via .addQueue(name), or .addQueue({ name, exact: true }) for a foreign queue.`,
    );
  }
}

/**
 * Thrown when a queue is not found or not created.
 */
export class QueueNotFoundError extends MatadorError {
  readonly description =
    'The specified queue does not exist or has not been created. ' +
    'ACTION: Ensure the queue is defined in the topology configuration. ' +
    'Call transport.applyTopology() or matador.start() to create queues. ' +
    'Check that the queue name matches the topology definition.';

  constructor(public readonly queueName: string) {
    super(
      `Queue "${queueName}" not found. Ensure it is defined in topology and applyTopology() was called.`,
    );
  }
}

// ============================================================================
// Event Validation Errors
// ============================================================================

/**
 * Thrown when an event is invalid or malformed.
 */
export class InvalidEventError extends MatadorError {
  readonly description =
    'The event is invalid or missing required fields. ' +
    'ACTION: Ensure the event has all required properties. ' +
    'Common issues: missing targetSubscriber during processing, ' +
    'null/undefined data when the event type requires data, ' +
    'malformed event structure from codec decode failure.';

  constructor(
    message: string,
    public readonly cause?: string,
  ) {
    super(`Invalid event: ${message}${cause ? `. Cause: ${cause}` : ''}`);
  }
}

// ============================================================================
// Message Processing Errors
// ============================================================================

/**
 * Thrown when a message has been redelivered too many times (poison message).
 */
export class MessageMaybePoisonedError extends MatadorError {
  readonly description =
    'A message was redelivered multiple times without successful processing. ' +
    'This usually indicates the message causes a crash or timeout during processing. ' +
    'ACTION: (1) Check application logs for errors/crashes during message processing, ' +
    '(2) Inspect the message in the dead-letter queue for malformed data, ' +
    '(3) Review the subscriber code for bugs that cause crashes, ' +
    '(4) Consider increasing processing timeout if the operation is legitimately slow. ' +
    'This message will NOT be retried to prevent crash loops.';

  constructor(
    public readonly eventId: string,
    public readonly deliveryCount: number,
    public readonly maxDeliveries: number,
  ) {
    super(
      `Message "${eventId}" delivered ${deliveryCount} times (max: ${maxDeliveries}). Possible poison message - will not be retried.`,
    );
  }
}

/**
 * Thrown when a non-idempotent message cannot be retried after redelivery.
 */
export class IdempotentMessageCannotRetryError extends MatadorError {
  readonly description =
    'A non-idempotent subscriber received a redelivered message. ' +
    'Retrying would risk duplicate side effects (e.g., double payments, duplicate emails). ' +
    'ACTION: (1) Mark the subscriber as idempotent if it safely handles duplicates, ' +
    '(2) Implement idempotency keys in the subscriber logic, ' +
    '(3) Manually inspect and replay the message from the dead-letter queue after verification. ' +
    'The message will be sent to the dead-letter queue for manual review.';

  constructor(
    public readonly eventId: string,
    public readonly subscriberName: string,
  ) {
    super(
      `Non-idempotent subscriber "${subscriberName}" cannot retry redelivered message "${eventId}". Mark subscriber as idempotent='yes' or implement idempotency handling.`,
    );
  }
}

// ============================================================================
// Timeout Errors
// ============================================================================

/**
 * Thrown when an operation times out.
 */
export class TimeoutError extends MatadorError {
  readonly description =
    'An operation timed out before completing. ' +
    'ACTION: (1) Increase the timeout if the operation legitimately needs more time, ' +
    '(2) Optimize the operation to complete faster, ' +
    '(3) Check for deadlocks or blocking operations, ' +
    '(4) Verify external service dependencies are responsive.';

  constructor(
    public readonly operation: string,
    public readonly timeoutMs: number,
  ) {
    super(`Operation "${operation}" timed out after ${timeoutMs}ms.`);
  }
}

// ============================================================================
// Type Guards
// ============================================================================

export function isMatadorError(error: unknown): error is MatadorError {
  return error instanceof MatadorError;
}

export function isNotStartedError(error: unknown): error is NotStartedError {
  return error instanceof NotStartedError;
}

export function isTransportNotConnectedError(
  error: unknown,
): error is TransportNotConnectedError {
  return error instanceof TransportNotConnectedError;
}

export function isEventNotRegisteredError(
  error: unknown,
): error is EventNotRegisteredError {
  return error instanceof EventNotRegisteredError;
}

export function isSubscriberNotRegisteredError(
  error: unknown,
): error is SubscriberNotRegisteredError {
  return error instanceof SubscriberNotRegisteredError;
}

export function isUnknownQueueReferenceError(
  error: unknown,
): error is UnknownQueueReferenceError {
  return error instanceof UnknownQueueReferenceError;
}

export function isMessageMaybePoisonedError(
  error: unknown,
): error is MessageMaybePoisonedError {
  return error instanceof MessageMaybePoisonedError;
}

export function isIdempotentMessageCannotRetryError(
  error: unknown,
): error is IdempotentMessageCannotRetryError {
  return error instanceof IdempotentMessageCannotRetryError;
}
