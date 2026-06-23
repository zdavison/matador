export type { HasDescription } from './has-description.js';
export { hasDescription } from './has-description.js';

export {
  assertEvent,
  DontRetry,
  DoRetry,
  EventAssertionError,
  isAssertionError,
  isDontRetry,
  isDoRetry,
  RetryControlError,
} from './retry-errors.js';

export {
  // Base class
  MatadorError,
  isMatadorError,
  // Lifecycle errors
  NotStartedError,
  isNotStartedError,
  ShutdownInProgressError,
  // Transport errors
  TransportNotConnectedError,
  isTransportNotConnectedError,
  TransportClosedError,
  TransportSendError,
  SomeSendError,
  AllTransportsFailedError,
  DelayedMessagesNotSupportedError,
  // Schema & configuration errors
  EventNotRegisteredError,
  isEventNotRegisteredError,
  SubscriberNotRegisteredError,
  isSubscriberNotRegisteredError,
  NoSubscribersExistError,
  InvalidSchemaError,
  SubscriberIsStubError,
  LocalTransportCannotProcessStubError,
  // Queue errors
  QueueNotFoundError,
  UnknownQueueReferenceError,
  isUnknownQueueReferenceError,
  // Event validation errors
  InvalidEventError,
  // Message processing errors
  MessageMaybePoisonedError,
  isMessageMaybePoisonedError,
  IdempotentMessageCannotRetryError,
  isIdempotentMessageCannotRetryError,
  // Timeout errors
  TimeoutError,
} from './matador-errors.js';

export {
  // Checkpoint errors
  CheckpointStoreError,
  DuplicateIoKeyError,
  isCheckpointStoreError,
  isDuplicateIoKeyError,
} from './checkpoint-errors.js';
