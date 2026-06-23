// Core
export type {
  FanoutConfig,
  HandlersState,
  MatadorConfig,
  SendError,
  SendResult,
  ShutdownConfig,
  ShutdownState,
} from './core/index.js';
export {
  defaultShutdownConfig,
  FanoutEngine,
  Matador,
  ShutdownManager,
} from './core/index.js';

// Types
export type {
  AnySubscriber,
  BaseSubscriberOptions,
  CallbackContext,
  CreateEnvelopeOptions,
  DeliveryMode,
  Dispatcher,
  Docket,
  Envelope,
  EnvelopeOf,
  Event,
  EventClass,
  EventData,
  EventKey,
  EventOptions,
  EventStatic,
  Idempotency,
  Importance,
  JsonRecord,
  ResumableCallback,
  ResumableCallbackContext,
  StandardCallback,
  Subscriber,
  SubscriberCallback,
  SubscriberDefinition,
  SubscriberOptions,
  SubscriberStub,
  ValidationError,
  ValidationResult,
} from './types/index.js';
export {
  MatadorEvent,
  createDummyEnvelope,
  createEnvelope,
  createSubscriber,
  createSubscriberStub,
  invalidResult,
  isSubscriber,
  isSubscriberStub,
  validResult,
} from './types/index.js';

// Transport
export type {
  ConnectFn,
  ConnectionManagerConfig,
  ConnectionState,
  DisconnectFn,
  MessageHandler,
  MessageReceipt,
  RabbitMQSendOptions,
  RabbitMQSubscribeOptions,
  RabbitMQTransportConfig,
  SendOptions,
  StateChangeCallback,
  SubscribeOptions,
  Subscription,
  Transport,
  TransportCapabilities,
  TransportSendOptions,
  TransportSubscribeOptions,
} from './transport/index.js';
export {
  ConnectionManager,
  defaultConnectionConfig,
  hasNativeDeadLetter,
  LocalTransport,
  RabbitMQTransport,
  MultiTransport,
  type MultiTransportConfig,
  type MultiTransportHooks,
  supportsDeliveryMode,
  supportsDelayedMessages,
} from './transport/index.js';

// Topology
export type {
  DeadLetterConfig,
  DeadLetterQueueConfig,
  QueueDefinition,
  QueueOptions,
  RabbitMQQueueDefinition,
  RabbitMQQueueOptions,
  RetryConfig,
  Topology,
  TopologyNaming,
  TransportQueueOptions,
} from './topology/index.js';
export {
  applyPrefix,
  findQueueDefinition,
  getDeadLetterQueueName,
  getQualifiedQueueName,
  getRetryQueueName,
  resolveQueueName,
  resolveTargetQueueName,
  TopologyBuilder,
  TopologyValidationError,
} from './topology/index.js';

// Codec
export type {
  Codec,
  EncodedMessage,
  HeaderAwareCodec,
} from './codec/index.js';
export { CodecDecodeError, JsonCodec, RabbitMQCodec } from './codec/index.js';

// Schema
export type {
  MatadorSchema,
  RegisterOptions,
  RuntimeSchemaEntryTuple,
  SchemaEntry,
  SchemaEntryTuple,
  SchemaIssue,
  SchemaPlugin,
  SchemaValidationResult,
} from './schema/index.js';
export {
  bind,
  installPlugins,
  isSchemaEntryTuple,
  SchemaError,
  SchemaRegistry,
} from './schema/index.js';

// Retry
export type {
  RetryContext,
  RetryDecision,
  RetryPolicy,
  StandardRetryPolicyConfig,
} from './retry/index.js';
export {
  defaultRetryConfig,
  StandardRetryPolicy,
} from './retry/index.js';

// Hooks
export type {
  DecodeErrorContext,
  EnqueueErrorContext,
  EnqueueSuccessContext,
  Logger,
  MatadorHooks,
  WorkerErrorContext,
  WorkerExecuteFn,
  WorkerSuccessContext,
} from './hooks/index.js';
export { consoleLogger, SafeHooks } from './hooks/index.js';

// Pipeline
export type { PipelineConfig, ProcessResult } from './pipeline/index.js';
export { ProcessingPipeline } from './pipeline/index.js';

// Errors
export type { HasDescription } from './errors/index.js';
export {
  // Retry control errors
  assertEvent,
  DontRetry,
  DoRetry,
  EventAssertionError,
  RetryControlError,
  isAssertionError,
  isDontRetry,
  isDoRetry,
  // Matador errors with descriptions
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
  // Event validation errors
  InvalidEventError,
  // Message processing errors
  MessageMaybePoisonedError,
  isMessageMaybePoisonedError,
  IdempotentMessageCannotRetryError,
  isIdempotentMessageCannotRetryError,
  // Timeout errors
  TimeoutError,
  // Utility
  hasDescription,
} from './errors/index.js';

// Checkpoint (Resumable Subscribers)
export type {
  Checkpoint,
  CheckpointClearedContext,
  CheckpointHitContext,
  CheckpointLoadedContext,
  CheckpointMissContext,
  CheckpointStore,
  JsonSerializable,
  ResumableContextConfig,
  ResumableContextHooks,
  SubscriberContext,
} from './checkpoint/index.js';
export {
  CheckpointStoreError,
  DuplicateIoKeyError,
  MemoryCheckpointStore,
  NoOpCheckpointStore,
  ResumableContext,
  isCheckpointStoreError,
  isDuplicateIoKeyError,
} from './checkpoint/index.js';
