import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'bun:test';
import {
  RabbitMQContainer,
  type StartedRabbitMQContainer,
} from '@testcontainers/rabbitmq';
import amqplib from 'amqplib';
import {
  type Envelope,
  Matador,
  MatadorEvent,
  type MatadorSchema,
  createEnvelope,
  createSubscriber,
} from '../../src/index.js';
import { StandardRetryPolicy } from '../../src/retry/index.js';
import type { QueueDefinition, Topology } from '../../src/topology/types.js';
import type { Subscription } from '../../src/transport/index.js';
import { RabbitMQTransport } from '../../src/transport/rabbitmq/rabbitmq-transport.js';
import {
  createTestEnvelope,
  createTestTopology,
} from './transport-compliance.e2e.test.js';

// Skip tests if docker is not available
const SKIP_E2E = process.env.SKIP_E2E_TESTS === 'true';

describe.skipIf(SKIP_E2E)('RabbitMQ Transport E2E', () => {
  let container: StartedRabbitMQContainer;
  let connectionUrl: string;

  beforeAll(async () => {
    // Start RabbitMQ container
    container = await new RabbitMQContainer('rabbitmq:3.13-management')
      .withExposedPorts(5672, 15672)
      .start();

    connectionUrl = container.getAmqpUrl();
    console.log(`RabbitMQ container started at ${connectionUrl}`);
  }, 120_000);

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
  });

  describe('RabbitMQ-specific features', () => {
    let transport: RabbitMQTransport;
    let subscriptions: Subscription[] = [];

    beforeEach(async () => {
      transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: 'matador-e2e-test',
        quorumQueues: false, // Use classic queues for faster tests
        defaultPrefetch: 5,
      });
      await transport.connect();
      await transport.applyTopology(createTestTopology(`test-${Date.now()}`));
      subscriptions = [];
    });

    afterEach(async () => {
      for (const sub of subscriptions) {
        if (sub.isActive) {
          await sub.unsubscribe();
        }
      }
      if (transport.isConnected()) {
        await transport.disconnect();
      }
    });

    it('should report correct capabilities', () => {
      expect(transport.name).toBe('rabbitmq');
      expect(transport.capabilities.deliveryModes).toContain('at-least-once');
      expect(transport.capabilities.deadLetterRouting).toBe('native');
      expect(transport.capabilities.attemptTracking).toBe(true);
      expect(transport.capabilities.concurrencyModel).toBe('prefetch');
      expect(transport.capabilities.priorities).toBe(true);
      // delayedMessages depends on plugin availability
    });

    it('should handle message priority', async () => {
      const topology = createTestTopology(`priority-${Date.now()}`);
      (topology.queues as unknown as QueueDefinition[])[0] = {
        ...topology.queues[0],
        priorities: true,
      };

      await transport.applyTopology(topology);
      const queueName = `${topology.namespace}.events`;

      const receivedOrder: number[] = [];

      const subscription = await transport.subscribe(
        queueName,
        async (env, receipt) => {
          receivedOrder.push((env.data as { priority: number }).priority);
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subscription);

      // Send messages with different priorities
      await transport.send(
        queueName,
        createTestEnvelope({ eventKey: 'priority.1' }),
        { priority: 1 },
      );
      await transport.send(
        queueName,
        createTestEnvelope({ eventKey: 'priority.5' }),
        { priority: 5 },
      );
      await transport.send(
        queueName,
        createTestEnvelope({ eventKey: 'priority.10' }),
        { priority: 10 },
      );

      // Wait for all messages
      await waitFor(() => receivedOrder.length >= 3, 5000);

      // Priority is not strictly guaranteed in RabbitMQ,
      // but higher priority messages should generally come first
      expect(receivedOrder.length).toBe(3);
    });

    it('should track attempt number in headers', async () => {
      const topology = createTestTopology(`attempts-${Date.now()}`);
      await transport.applyTopology(topology);
      const queueName = `${topology.namespace}.events`;

      let attemptNumber = 0;

      const subscription = await transport.subscribe(
        queueName,
        async (_env, receipt) => {
          attemptNumber = receipt.attemptNumber;
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subscription);

      await transport.send(queueName, createTestEnvelope());

      await waitFor(() => attemptNumber > 0, 5000);

      expect(attemptNumber).toBe(1);
    });

    it('should handle prefetch/concurrency per queue', async () => {
      const topology = createTestTopology(`prefetch-${Date.now()}`);
      await transport.applyTopology(topology);
      const queueName = `${topology.namespace}.events`;

      const processing = new Set<string>();
      let maxConcurrent = 0;

      const subscription = await transport.subscribe(
        queueName,
        async (env, receipt) => {
          processing.add(env.id);
          maxConcurrent = Math.max(maxConcurrent, processing.size);

          // Simulate processing time
          await new Promise((resolve) => setTimeout(resolve, 100));

          processing.delete(env.id);
          await transport.complete(receipt);
        },
        { concurrency: 3 },
      );
      subscriptions.push(subscription);

      // Send more messages than prefetch allows
      for (let i = 0; i < 10; i++) {
        await transport.send(queueName, createTestEnvelope({ id: `msg-${i}` }));
      }

      // Wait for all messages to be processed
      await waitFor(() => processing.size === 0, 10000);

      // Should never exceed prefetch
      expect(maxConcurrent).toBeLessThanOrEqual(3);
    });

    it('should throw when sending delayed message without plugin', async () => {
      // Standard RabbitMQ container doesn't have delayed message plugin
      const topology = createTestTopology(`delay-${Date.now()}`);
      await transport.applyTopology(topology);
      const queueName = `${topology.namespace}.events`;

      // Should throw because plugin is not available
      await expect(
        transport.send(queueName, createTestEnvelope(), { delay: 1000 }),
      ).rejects.toThrow('delayed message exchange plugin');
    });

    it('should report delayedMessages capability based on plugin', () => {
      // Standard RabbitMQ without plugin should report false
      expect(transport.capabilities.delayedMessages).toBe(false);
    });
  });

  describe('reconnection behavior', () => {
    it('should handle disconnect and reconnect', async () => {
      const transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: 'matador-reconnect-test',
        connection: {
          maxReconnectAttempts: 3,
          initialReconnectDelay: 100,
        },
      });

      await transport.connect();
      expect(transport.isConnected()).toBe(true);

      await transport.disconnect();
      expect(transport.isConnected()).toBe(false);

      // Reconnect
      await transport.connect();
      expect(transport.isConnected()).toBe(true);

      await transport.disconnect();
    });
  });

  describe('exact queue definitions with transport options', () => {
    let transport: RabbitMQTransport;
    const subscriptions: Subscription[] = [];

    beforeEach(async () => {
      transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: 'matador-exact-queue-test',
        quorumQueues: false,
        defaultPrefetch: 5,
      });
      await transport.connect();
    });

    afterEach(async () => {
      for (const sub of subscriptions) {
        if (sub.isActive) {
          await sub.unsubscribe();
        }
      }
      if (transport.isConnected()) {
        await transport.disconnect();
      }
    });

    it('should apply exact RabbitMQ options when asserting queue', async () => {
      const namespace = `exact-opts-${Date.now()}`;
      const exactQueueName = `${namespace}.shared.custom-queue`;

      const topology: Topology = {
        namespace,
        queues: [
          {
            name: exactQueueName,
            exact: true,
            transport: {
              rabbitmq: {
                options: {
                  durable: true,
                  arguments: {
                    'x-max-length': 1000,
                    'x-message-ttl': 60000,
                  },
                },
              },
            },
          },
        ],
        deadLetter: {
          unhandled: { enabled: false },
          undeliverable: { enabled: false },
        },
        retry: {
          enabled: true,
          defaultDelayMs: 1000,
          maxDelayMs: 30000,
        },
      };

      // Should not throw - queue options should be applied correctly
      await transport.applyTopology(topology);

      // Verify queue works by sending and receiving a message
      let receivedMessage = false;
      const subscription = await transport.subscribe(
        exactQueueName,
        async (_env, receipt) => {
          receivedMessage = true;
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subscription);

      await transport.send(exactQueueName, createTestEnvelope());
      await waitFor(() => receivedMessage, 5000);

      expect(receivedMessage).toBe(true);
    });

    it('should NOT create retry queue for exact queue (cross-namespace safe)', async () => {
      const namespace = `exact-retry-${Date.now()}`;
      const exactQueueName = `${namespace}.shared.retry-queue`;

      const topology: Topology = {
        namespace,
        queues: [
          {
            name: exactQueueName,
            exact: true,
            transport: {
              rabbitmq: {
                options: {
                  durable: true,
                },
              },
            },
          },
        ],
        deadLetter: {
          unhandled: { enabled: false },
          undeliverable: { enabled: false },
        },
        retry: {
          enabled: true,
          defaultDelayMs: 1000,
          maxDelayMs: 30000,
        },
      };

      // Apply topology - should create the main queue but skip the retry sibling.
      await transport.applyTopology(topology);

      // Main queue still works
      let receivedCount = 0;
      const subscription = await transport.subscribe(
        exactQueueName,
        async (_env, receipt) => {
          receivedCount++;
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subscription);

      await transport.send(exactQueueName, createTestEnvelope());
      await waitFor(() => receivedCount === 1, 5000);

      expect(receivedCount).toBe(1);

      // Retry queue should not have been declared. checkQueue is passive and
      // throws NOT_FOUND if the queue is absent. We run it on a fresh channel
      // because passive failures close the channel.
      const probeConn = await amqplib.connect(connectionUrl);
      const probeChannel = await probeConn.createChannel();
      probeChannel.on('error', () => {});
      let retryQueueAbsent = false;
      try {
        await probeChannel.checkQueue(`${exactQueueName}.retry`);
      } catch (err) {
        retryQueueAbsent =
          err instanceof Error && /NOT_FOUND/i.test(err.message);
      }
      try {
        await probeConn.close();
      } catch {
        /* channel may already be closed by the passive failure */
      }
      expect(retryQueueAbsent).toBe(true);
    });

    it('should not conflict when two namespaces declare the same exact queue', async () => {
      // The whole reason we skip retry-queue assertion for exact:true queues:
      // if two processes with different `topology.namespace` values both
      // asserted `<exactQueue>.retry`, they would race to set conflicting
      // `x-dead-letter-exchange` args (each derives DLX from its own
      // namespace), causing PRECONDITION_FAILED 406 forever.
      const exactQueueName = `matador.shared.cross-ns-${Date.now()}`;

      const makeTopology = (namespace: string): Topology => ({
        namespace,
        queues: [
          {
            name: exactQueueName,
            exact: true,
            transport: { rabbitmq: { options: { durable: true } } },
          },
        ],
        deadLetter: {
          unhandled: { enabled: false },
          undeliverable: { enabled: false },
        },
        retry: { enabled: true, defaultDelayMs: 1000, maxDelayMs: 30000 },
      });

      // First namespace asserts. Then a second namespace, sharing the broker,
      // asserts the same exact queue. With the retry-queue guard, neither call
      // should throw.
      await transport.applyTopology(makeTopology(`ns-a-${Date.now()}`));
      await transport.applyTopology(makeTopology(`ns-b-${Date.now()}`));

      // Main queue still functions.
      let receivedMessage = false;
      const subscription = await transport.subscribe(
        exactQueueName,
        async (_env, receipt) => {
          receivedMessage = true;
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subscription);

      await transport.send(exactQueueName, createTestEnvelope());
      await waitFor(() => receivedMessage, 5000);

      expect(receivedMessage).toBe(true);
    });

    it('should use custom dead letter exchange from exact options', async () => {
      const namespace = `exact-dlx-${Date.now()}`;
      const exactQueueName = `${namespace}.shared.dlx-queue`;
      const customDlxExchange = `${namespace}.custom-dlx`;

      // First create the custom DLX exchange manually
      // (In real usage, this would be managed externally)

      const topology: Topology = {
        namespace,
        queues: [
          {
            name: exactQueueName,
            exact: true,
            transport: {
              rabbitmq: {
                options: {
                  durable: true,
                  deadLetterExchange: customDlxExchange,
                },
              },
            },
          },
        ],
        deadLetter: {
          unhandled: { enabled: false },
          undeliverable: { enabled: false },
        },
        retry: {
          enabled: false,
          defaultDelayMs: 1000,
          maxDelayMs: 30000,
        },
      };

      // Should not throw - custom DLX should be set
      await transport.applyTopology(topology);

      // Verify queue works
      let receivedMessage = false;
      const subscription = await transport.subscribe(
        exactQueueName,
        async (_env, receipt) => {
          receivedMessage = true;
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subscription);

      await transport.send(exactQueueName, createTestEnvelope());
      await waitFor(() => receivedMessage, 5000);

      expect(receivedMessage).toBe(true);
    });

    it('should apply RabbitMQ options without exact mode', async () => {
      const namespace = `opts-no-exact-${Date.now()}`;

      const topology: Topology = {
        namespace,
        queues: [
          {
            name: 'custom-options-queue',
            // exact: false (default) - namespace prefix will be added
            transport: {
              rabbitmq: {
                options: {
                  durable: true,
                  arguments: {
                    'x-max-length': 500,
                  },
                },
              },
            },
          },
        ],
        deadLetter: {
          unhandled: { enabled: false },
          undeliverable: { enabled: false },
        },
        retry: {
          enabled: true,
          defaultDelayMs: 1000,
          maxDelayMs: 30000,
        },
      };

      await transport.applyTopology(topology);

      // Queue name should have namespace prefix since exact: false
      const queueName = `${namespace}.custom-options-queue`;

      let receivedMessage = false;
      const subscription = await transport.subscribe(
        queueName,
        async (_env, receipt) => {
          receivedMessage = true;
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subscription);

      await transport.send(queueName, createTestEnvelope());
      await waitFor(() => receivedMessage, 5000);

      expect(receivedMessage).toBe(true);
    });

    it('should allow queue name with dots when exact: true', async () => {
      const namespace = `exact-dots-${Date.now()}`;
      const exactQueueName = 'matador.shared.id-platform.events';

      const topology: Topology = {
        namespace,
        queues: [
          {
            name: exactQueueName,
            exact: true,
            transport: {
              rabbitmq: {
                options: {
                  durable: true,
                },
              },
            },
          },
        ],
        deadLetter: {
          unhandled: { enabled: false },
          undeliverable: { enabled: false },
        },
        retry: {
          enabled: true,
          defaultDelayMs: 1000,
          maxDelayMs: 30000,
        },
      };

      await transport.applyTopology(topology);

      // Verify queue with dots in name works
      let receivedMessage = false;
      const subscription = await transport.subscribe(
        exactQueueName,
        async (_env, receipt) => {
          receivedMessage = true;
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subscription);

      await transport.send(exactQueueName, createTestEnvelope());
      await waitFor(() => receivedMessage, 5000);

      expect(receivedMessage).toBe(true);
    });
  });

  describe('channel per queue isolation', () => {
    let transport: RabbitMQTransport;
    const subscriptions: Subscription[] = [];

    beforeEach(async () => {
      transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: 'matador-isolation-test',
        quorumQueues: false,
        defaultPrefetch: 2,
      });
      await transport.connect();
    });

    afterEach(async () => {
      for (const sub of subscriptions) {
        if (sub.isActive) {
          await sub.unsubscribe();
        }
      }
      if (transport.isConnected()) {
        await transport.disconnect();
      }
    });

    it('should have independent concurrency per queue', async () => {
      const topology = createTestTopology(`isolation-${Date.now()}`);
      await transport.applyTopology(topology);

      const queue1 = `${topology.namespace}.events`;
      const queue2 = `${topology.namespace}.notifications`;

      const queue1Processing = new Set<string>();
      const queue2Processing = new Set<string>();
      let queue1MaxConcurrent = 0;
      let queue2MaxConcurrent = 0;

      // Subscribe to both queues with different concurrency
      const sub1 = await transport.subscribe(
        queue1,
        async (env, receipt) => {
          queue1Processing.add(env.id);
          queue1MaxConcurrent = Math.max(
            queue1MaxConcurrent,
            queue1Processing.size,
          );
          await new Promise((resolve) => setTimeout(resolve, 50));
          queue1Processing.delete(env.id);
          await transport.complete(receipt);
        },
        { concurrency: 2 },
      );
      subscriptions.push(sub1);

      const sub2 = await transport.subscribe(
        queue2,
        async (env, receipt) => {
          queue2Processing.add(env.id);
          queue2MaxConcurrent = Math.max(
            queue2MaxConcurrent,
            queue2Processing.size,
          );
          await new Promise((resolve) => setTimeout(resolve, 50));
          queue2Processing.delete(env.id);
          await transport.complete(receipt);
        },
        { concurrency: 3 },
      );
      subscriptions.push(sub2);

      // Send messages to both queues
      for (let i = 0; i < 6; i++) {
        await transport.send(queue1, createTestEnvelope({ id: `q1-${i}` }));
        await transport.send(queue2, createTestEnvelope({ id: `q2-${i}` }));
      }

      // Wait for all to complete
      await waitFor(
        () => queue1Processing.size === 0 && queue2Processing.size === 0,
        10000,
      );

      // Each queue should respect its own prefetch
      expect(queue1MaxConcurrent).toBeLessThanOrEqual(2);
      expect(queue2MaxConcurrent).toBeLessThanOrEqual(3);
    });
  });

  describe('single active consumer', () => {
    let transport: RabbitMQTransport;
    let subscriptions: Subscription[] = [];

    beforeEach(async () => {
      transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: 'matador-sac-test',
        quorumQueues: false,
        defaultPrefetch: 1,
      });
      await transport.connect();
    });

    afterEach(async () => {
      for (const sub of subscriptions) {
        if (sub.isActive) {
          await sub.unsubscribe();
        }
      }
      subscriptions = [];
      if (transport.isConnected()) {
        await transport.disconnect();
      }
    });

    it('routes messages to only one of several consumers, failing over when it disconnects', async () => {
      const topology = createTestTopology(`sac-${Date.now()}`);
      (topology.queues as unknown as QueueDefinition[])[0] = {
        ...topology.queues[0],
        singleActiveConsumer: true,
      };
      await transport.applyTopology(topology);
      const queueName = `${topology.namespace}.events`;

      const receivedByA: string[] = [];
      const receivedByB: string[] = [];

      const subA = await transport.subscribe(
        queueName,
        async (env, receipt) => {
          receivedByA.push(env.id);
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subA);

      const subB = await transport.subscribe(
        queueName,
        async (env, receipt) => {
          receivedByB.push(env.id);
          await transport.complete(receipt);
        },
      );
      subscriptions.push(subB);

      for (let i = 0; i < 5; i++) {
        await transport.send(queueName, createTestEnvelope({ id: `sac-${i}` }));
      }

      await waitFor(() => receivedByA.length + receivedByB.length >= 5, 5000);

      // Only the (first-subscribed) active consumer should receive messages
      // while both remain subscribed.
      expect(receivedByA.length).toBe(5);
      expect(receivedByB.length).toBe(0);

      // Disconnecting the active consumer should fail over to the other one.
      await subA.unsubscribe();
      subscriptions = subscriptions.filter((s) => s !== subA);

      for (let i = 5; i < 10; i++) {
        await transport.send(queueName, createTestEnvelope({ id: `sac-${i}` }));
      }

      await waitFor(() => receivedByB.length >= 5, 5000);
      expect(receivedByB.length).toBe(5);
    });
  });

  describe('subscriber idempotency', () => {
    let matador: Matador | undefined;

    afterEach(async () => {
      if (matador) {
        await matador.shutdown().catch(() => {});
        matador = undefined;
      }
    });

    class IdempotencyTestEvent extends MatadorEvent {
      static readonly key = 'idempotency-test.event';
      static readonly description =
        'Event used to exercise idempotent retry behavior';
      constructor(public data: { id: string }) {
        super();
      }
    }

    /**
     * Pulls and nack-requeues one message from a throwaway channel, simulating a worker
     * crashing before it acks. RabbitMQ marks the next delivery of that message
     * `redelivered: true` which is checked by StandardRetryPolicy's idempotency rule
     */
    async function forceNativeRedelivery(queueName: string): Promise<void> {
      const conn = await amqplib.connect(connectionUrl);
      const channel = await conn.createChannel();
      try {
        const msg = await channel.get(queueName, { noAck: false });
        if (!msg) {
          throw new Error(`Expected a message on '${queueName}'`);
        }
        channel.nack(msg, false, true);
      } finally {
        await channel.close();
        await conn.close();
      }
    }

    /**
     * Polls a queue with a throwaway channel until a message appears (acking it),
     * or returns null after timeoutMs. Used to observe where the real pipeline
     * routed a message (source queue vs. dead-letter queue) without racing a
     * long-lived consumer against the assertions.
     */
    async function pollForMessage(
      queueName: string,
      timeoutMs: number,
    ): Promise<Pick<Envelope, 'data'> | null> {
      const conn = await amqplib.connect(connectionUrl);
      const channel = await conn.createChannel();
      try {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
          const msg = await channel.get(queueName, { noAck: true });
          if (msg) {
            return {
              data: JSON.parse(msg.content.toString()),
            };
          }
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
        return null;
      } finally {
        await channel.close();
        await conn.close();
      }
    }

    it('dead-letters a non-idempotent subscriber after an exception', async () => {
      const namespace = `idempotent-no-${Date.now()}`;
      const topology = createTestTopology(namespace);
      const queueName = `${namespace}.events`;
      const dlqQueueName = `${queueName}.undeliverable`;

      let callCount = 0;
      const subscriber = createSubscriber<IdempotencyTestEvent>({
        name: 'non-idempotent-subscriber',
        description: 'Cannot safely handle a duplicate delivery',
        idempotent: 'no',
        callback: async () => {
          callCount++;
          throw new Error('Simulated processing failure');
        },
      });

      const schema: MatadorSchema = {
        [IdempotencyTestEvent.key]: [IdempotencyTestEvent, [subscriber]],
      };

      const transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: `idempotent-no-${Date.now()}`,
        quorumQueues: false,
        defaultPrefetch: 1,
      });

      matador = new Matador({
        transport,
        topology,
        schema,
        consumeFrom: ['events'],
        retryPolicy: new StandardRetryPolicy({ baseDelay: 0, maxDelay: 0 }),
      });
      await matador.start();

      const event = new IdempotencyTestEvent({
        id: 'idempotent-no',
      });

      const result = await matador.send(event);

      // Wait for processing
      await matador.waitForIdle(5000);

      expect(result.subscribersSent).toBe(1);
      expect(result.errors).toHaveLength(0);
      expect(callCount).toBe(1);

      const dlqEnvelope = (await pollForMessage(dlqQueueName, 5000)) as Pick<
        Envelope<IdempotencyTestEvent>,
        'data'
      >;
      expect(dlqEnvelope?.data.data.id).toBe('idempotent-no');
    });

    it('dead-letters a non-idempotent subscriber after a crash', async () => {
      const namespace = `idempotent-no-${Date.now()}`;
      const topology = createTestTopology(namespace);
      const queueName = `${namespace}.events`;
      const dlqQueueName = `${queueName}.undeliverable`;

      let callCount = 0;
      const subscriber = createSubscriber<IdempotencyTestEvent>({
        name: 'non-idempotent-subscriber',
        description: 'Cannot safely handle a duplicate delivery',
        idempotent: 'no',
        callback: async () => {
          callCount++;
          throw new Error('Execution should not reach this point');
        },
      });

      const schema: MatadorSchema = {
        [IdempotencyTestEvent.key]: [IdempotencyTestEvent, [subscriber]],
      };

      const transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: `idempotent-no-${Date.now()}`,
        quorumQueues: false,
        defaultPrefetch: 1,
      });

      // Pre-seed the queue and force a native redelivery *before* Matador starts
      // consuming, so the very first delivery its real pipeline sees already
      // carries the broker's redelivered:true flag.
      await transport.connect();
      await transport.applyTopology(topology);
      await transport.send(
        queueName,
        createEnvelope({
          eventKey: IdempotencyTestEvent.key,
          targetSubscriber: subscriber.name,
          data: { id: 'idempotent-no' },
          importance: 'should-investigate',
        }),
      );
      await forceNativeRedelivery(queueName);

      matador = new Matador({
        transport,
        topology,
        schema,
        consumeFrom: ['events'],
        retryPolicy: new StandardRetryPolicy({ baseDelay: 0, maxDelay: 0 }),
      });
      await matador.start();

      const dlqEnvelope = (await pollForMessage(dlqQueueName, 5000)) as Pick<
        Envelope<IdempotencyTestEvent>,
        'data'
      >;
      expect(dlqEnvelope?.data.data.id).toBe('idempotent-no');
      expect(callCount).toBe(0);
    });

    it('retries an idempotent subscriber after an exception', async () => {
      const namespace = `idempotent-yes-${Date.now()}`;
      const topology = createTestTopology(namespace);
      const queueName = `${namespace}.events`;

      let callCount = 0;
      let success = false;
      const subscriber = createSubscriber<IdempotencyTestEvent>({
        name: 'idempotent-subscriber',
        description: 'Can safely handle a duplicate delivery',
        idempotent: 'yes',
        callback: async () => {
          callCount++;
          if (callCount === 1) {
            throw new Error('Simulated processing failure');
          }
          success = true;
        },
      });

      const schema: MatadorSchema = {
        [IdempotencyTestEvent.key]: [IdempotencyTestEvent, [subscriber]],
      };

      const transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: `idempotent-yes-${Date.now()}`,
        quorumQueues: false,
        defaultPrefetch: 1,
      });

      matador = new Matador({
        transport,
        topology,
        schema,
        consumeFrom: ['events'],
        retryPolicy: new StandardRetryPolicy({ baseDelay: 0, maxDelay: 0 }),
      });
      await matador.start();

      const event = new IdempotencyTestEvent({
        id: 'idempotent-yes',
      });

      const result = await matador.send(event);

      // Wait for processing
      await matador.waitForIdle(10_000);

      expect(result.subscribersSent).toBe(1);
      expect(result.errors).toHaveLength(0);
      expect(callCount).toBe(2);
      expect(success).toBeTruthy();
    });

    it('retries an idempotent subscriber after a crash', async () => {
      const namespace = `idempotent-yes-${Date.now()}`;
      const topology = createTestTopology(namespace);
      const queueName = `${namespace}.events`;

      let callCount = 0;
      let success = false;
      const subscriber = createSubscriber<IdempotencyTestEvent>({
        name: 'idempotent-subscriber',
        description: 'Can safely handle a duplicate delivery',
        idempotent: 'yes',
        callback: async () => {
          callCount++;
          success = true;
        },
      });

      const schema: MatadorSchema = {
        [IdempotencyTestEvent.key]: [IdempotencyTestEvent, [subscriber]],
      };

      const transport = new RabbitMQTransport({
        url: connectionUrl,
        connectionName: `idempotent-yes-${Date.now()}`,
        quorumQueues: false,
        defaultPrefetch: 1,
      });

      // Pre-seed the queue and force a native redelivery *before* Matador starts
      // consuming, so the very first delivery its real pipeline sees already
      // carries the broker's redelivered:true flag.
      await transport.connect();
      await transport.applyTopology(topology);
      await transport.send(
        queueName,
        createEnvelope({
          eventKey: IdempotencyTestEvent.key,
          targetSubscriber: subscriber.name,
          data: { id: 'idempotent-yes' },
          importance: 'should-investigate',
        }),
      );
      await forceNativeRedelivery(queueName);

      matador = new Matador({
        transport,
        topology,
        schema,
        consumeFrom: ['events'],
        retryPolicy: new StandardRetryPolicy({ baseDelay: 0, maxDelay: 0 }),
      });
      await matador.start();

      // Wait for processing
      await matador.waitForIdle(10_000);

      expect(callCount).toBe(1);
      expect(success).toBeTruthy();
    });
  });
});

/**
 * Waits for a condition to be true, with timeout.
 */
async function waitFor(
  condition: () => boolean,
  timeoutMs: number,
  intervalMs = 50,
): Promise<void> {
  const startTime = Date.now();
  while (!condition()) {
    if (Date.now() - startTime > timeoutMs) {
      throw new Error(`Timeout waiting for condition after ${timeoutMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}
