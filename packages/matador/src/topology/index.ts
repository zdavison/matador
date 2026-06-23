export type {
  DeadLetterConfig,
  DeadLetterQueueConfig,
  QueueDefinition,
  RabbitMQQueueDefinition,
  RabbitMQQueueOptions,
  RetryConfig,
  Topology,
  TopologyNaming,
  TransportQueueOptions,
} from './types.js';
export {
  applyPrefix,
  findQueueDefinition,
  getDeadLetterQueueName,
  getQualifiedQueueName,
  getRetryQueueName,
  resolveQueueName,
  resolveTargetQueueName,
} from './types.js';

export type { QueueOptions } from './builder.js';
export { TopologyBuilder, TopologyValidationError } from './builder.js';
