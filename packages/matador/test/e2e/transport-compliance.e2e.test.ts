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
import type { Topology } from '../../src/topology/types.js';
import type { Subscription, Transport } from '../../src/transport/index.js';
import { LocalTransport } from '../../src/transport/local/local-transport.js';
import { RabbitMQTransport } from '../../src/transport/rabbitmq/rabbitmq-transport.js';
import { createEnvelope } from '../../src/types/index.js';

/**
 * Creates a test topology for transport compliance tests.
 */
export function createTestTopology(namespace = 'test'): Topology {
  return {
    namespace,
    queues: [{ name: 'events' }, { name: 'notifications' }],
    deadLetter: {
      unhandled: { enabled: true },
      undeliverable: { enabled: true },
    },
    retry: {
      enabled: true,
      defaultDelayMs: 1000,
      maxDelayMs: 30000,
    },
  };
}

/**
 * Creates a test envelope.
 */
export function createTestEnvelope(overrides?: {
  id?: string;
  eventKey?: string;
}) {
  return createEnvelope({
    id: overrides?.id,
    eventKey: overrides?.eventKey ?? 'test.event',
    targetSubscriber: 'test-subscriber',
    data: { test: 'data', timestamp: Date.now() },
    importance: 'should-investigate',
  });
}

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

// Skip E2E tests (RabbitMQ) if flag is set
const SKIP_E2E = process.env.SKIP_E2E_TESTS === 'true';

// RabbitMQ container for E2E tests
let rabbitContainer: StartedRabbitMQContainer | null = null;
let rabbitConnectionUrl: string | null = null;

// Start RabbitMQ container before all tests (if not skipping E2E)
beforeAll(async () => {
  if (!SKIP_E2E) {
    rabbitContainer = await new RabbitMQContainer('rabbitmq:3.13-management')
      .withExposedPorts(5672, 15672)
      .start();
    rabbitConnectionUrl = rabbitContainer.getAmqpUrl();
    console.log(`RabbitMQ container started at ${rabbitConnectionUrl}`);
  }
}, 120_000);

afterAll(async () => {
  if (rabbitContainer) {
    await rabbitContainer.stop();
  }
});

// Transport factories for test.each
type TransportFactory = {
  name: string;
  create: () => Transport;
  cleanup?: (transport: Transport) => void;
  skip?: boolean;
};

const transportFactories: TransportFactory[] = [
  {
    name: 'LocalTransport',
    create: () => new LocalTransport(),
    cleanup: (transport) => (transport as LocalTransport).clear(),
    skip: false,
  },
  {
    name: 'RabbitMQTransport',
    create: () => {
      if (!rabbitConnectionUrl) {
        throw new Error('RabbitMQ container not started');
      }
      return new RabbitMQTransport({
        url: rabbitConnectionUrl,
        connectionName: 'matador-compliance-test',
        quorumQueues: false, // Use classic queues for faster tests
        defaultPrefetch: 10,
      });
    },
    cleanup: undefined,
    skip: SKIP_E2E,
  },
];

describe.each(transportFactories)(
  '$name Transport Compliance',
  ({ name, create, cleanup, skip }) => {
    // Use unique namespace per transport to avoid conflicts
    const getNamespace = () => `${name.toLowerCase()}-${Date.now()}`;

    describe.skipIf(skip ?? false)(`${name} tests`, () => {
      let transport: Transport;
      let subscriptions: Subscription[] = [];

      beforeEach(async () => {
        transport = create();
        subscriptions = [];
      });

      afterEach(async () => {
        // Clean up subscriptions
        for (const sub of subscriptions) {
          if (sub.isActive) {
            await sub.unsubscribe();
          }
        }
        subscriptions = [];

        // Disconnect transport
        if (transport.isConnected()) {
          await transport.disconnect();
        }

        // Additional cleanup
        if (cleanup) {
          cleanup(transport);
        }
      });

      describe('connection lifecycle', () => {
        it('should start disconnected', () => {
          expect(transport.isConnected()).toBe(false);
        });

        it('should connect successfully', async () => {
          await transport.connect();
          expect(transport.isConnected()).toBe(true);
        });

        it('should disconnect successfully', async () => {
          await transport.connect();
          await transport.disconnect();
          expect(transport.isConnected()).toBe(false);
        });

        it('should handle multiple connect calls idempotently', async () => {
          await transport.connect();
          await transport.connect();
          expect(transport.isConnected()).toBe(true);
        });

        it('should handle multiple disconnect calls idempotently', async () => {
          await transport.connect();
          await transport.disconnect();
          await transport.disconnect();
          expect(transport.isConnected()).toBe(false);
        });
      });

      describe('capabilities', () => {
        it('should have a name', () => {
          expect(transport.name).toBeTruthy();
          expect(typeof transport.name).toBe('string');
        });

        it('should report capabilities', () => {
          expect(transport.capabilities).toBeTruthy();
          expect(Array.isArray(transport.capabilities.deliveryModes)).toBe(
            true,
          );
          expect(transport.capabilities.deliveryModes.length).toBeGreaterThan(
            0,
          );
        });

        it('should report at least one delivery mode', () => {
          const { deliveryModes } = transport.capabilities;
          const validModes = ['at-least-once', 'at-most-once'];
          expect(deliveryModes.some((m) => validModes.includes(m))).toBe(true);
        });
      });

      describe('topology', () => {
        it('should apply basic topology without error', async () => {
          await transport.connect();
          const topology = createTestTopology(getNamespace());

          await expect(
            transport.applyTopology(topology),
          ).resolves.toBeUndefined();
        });

        it('should throw when applying topology while disconnected', async () => {
          const topology = createTestTopology(getNamespace());

          await expect(transport.applyTopology(topology)).rejects.toThrow();
        });
      });

      describe('send and receive', () => {
        let namespace: string;
        let queueName: string;

        beforeEach(async () => {
          namespace = getNamespace();
          queueName = `${namespace}.events`;
          await transport.connect();
          await transport.applyTopology(createTestTopology(namespace));
        });

        it('should send a message without error', async () => {
          const envelope = createTestEnvelope();

          if (name === 'LocalTransport') {
            // Unlike a real broker, LocalTransport can't durably hold a
            // message for a consumer that doesn't exist in this process yet
            // (there's nowhere else for it to live), so it requires an
            // active subscriber at send time.
            const subscription = await transport.subscribe(
              queueName,
              async () => {},
            );
            subscriptions.push(subscription);
          }

          await expect(transport.send(queueName, envelope)).resolves.toBe(
            transport.name,
          );
        });

        it('should receive sent messages via subscription', async () => {
          const envelope = createTestEnvelope();
          const received: unknown[] = [];

          const subscription = await transport.subscribe(
            queueName,
            async (env, receipt) => {
              received.push(env);
              await transport.complete(receipt);
            },
          );
          subscriptions.push(subscription);

          await transport.send(queueName, envelope);

          // Wait for message delivery
          await waitFor(() => received.length > 0, 5000);

          expect(received.length).toBe(1);
          expect((received[0] as { id: string }).id).toBe(envelope.id);
        });

        it('should preserve message payload', async () => {
          const envelope = createTestEnvelope();
          const received: unknown[] = [];

          const subscription = await transport.subscribe(
            queueName,
            async (env, receipt) => {
              received.push(env);
              await transport.complete(receipt);
            },
          );
          subscriptions.push(subscription);

          await transport.send(queueName, envelope);

          await waitFor(() => received.length > 0, 5000);

          const receivedEnvelope = received[0] as typeof envelope;
          expect(receivedEnvelope.data).toEqual(envelope.data);
          expect(receivedEnvelope.docket.eventKey).toBe(
            envelope.docket.eventKey,
          );
          expect(receivedEnvelope.docket.targetSubscriber).toBe(
            envelope.docket.targetSubscriber,
          );
        });

        it('should throw when sending while disconnected', async () => {
          await transport.disconnect();
          const envelope = createTestEnvelope();

          await expect(transport.send(queueName, envelope)).rejects.toThrow();
        });
      });

      describe('subscriptions', () => {
        let namespace: string;
        let queueName: string;

        beforeEach(async () => {
          namespace = getNamespace();
          queueName = `${namespace}.events`;
          await transport.connect();
          await transport.applyTopology(createTestTopology(namespace));
        });

        it('should create an active subscription', async () => {
          const subscription = await transport.subscribe(
            queueName,
            async () => {},
          );
          subscriptions.push(subscription);

          expect(subscription.isActive).toBe(true);
        });

        it('should deactivate subscription after unsubscribe', async () => {
          const subscription = await transport.subscribe(
            queueName,
            async () => {},
          );
          subscriptions.push(subscription);

          await subscription.unsubscribe();

          expect(subscription.isActive).toBe(false);
        });

        it('should stop receiving messages after unsubscribe', async () => {
          const received: unknown[] = [];

          const subscription = await transport.subscribe(
            queueName,
            async (env, receipt) => {
              received.push(env);
              await transport.complete(receipt);
            },
          );

          await subscription.unsubscribe();

          // Send message after unsubscribe
          if (name === 'LocalTransport') {
            // With no subscriber left in this process, LocalTransport has
            // nowhere to durably hold the message (unlike a real broker), so
            // it rejects the send instead of silently accepting a message
            // nobody will ever receive.
            await expect(
              transport.send(queueName, createTestEnvelope()),
            ).rejects.toThrow();
          } else {
            await transport.send(queueName, createTestEnvelope());
          }

          // Wait a bit to ensure no message is received
          await new Promise((resolve) => setTimeout(resolve, 500));

          expect(received.length).toBe(0);
        });

        it('should throw when subscribing while disconnected', async () => {
          await transport.disconnect();

          await expect(
            transport.subscribe(queueName, async () => {}),
          ).rejects.toThrow();
        });
      });

      describe('message completion', () => {
        let namespace: string;
        let queueName: string;

        beforeEach(async () => {
          namespace = getNamespace();
          queueName = `${namespace}.events`;
          await transport.connect();
          await transport.applyTopology(createTestTopology(namespace));
        });

        it('should complete messages successfully', async () => {
          const envelope = createTestEnvelope();
          let completed = false;

          const subscription = await transport.subscribe(
            queueName,
            async (_env, receipt) => {
              await transport.complete(receipt);
              completed = true;
            },
          );
          subscriptions.push(subscription);

          await transport.send(queueName, envelope);

          await waitFor(() => completed, 5000);

          expect(completed).toBe(true);
        });
      });

      describe('dead letter queue', () => {
        let namespace: string;
        let queueName: string;

        beforeEach(async () => {
          namespace = getNamespace();
          queueName = `${namespace}.events`;
          await transport.connect();
          await transport.applyTopology(createTestTopology(namespace));
        });

        it('should send messages to dead letter queue', async () => {
          // Only test if transport supports sendToDeadLetter
          if (!transport.sendToDeadLetter) {
            return;
          }

          const envelope = createTestEnvelope();
          let dlqReceived = false;

          // Subscribe to DLQ
          const dlqSub = await transport.subscribe(
            `${queueName}.undeliverable`,
            async (_env, receipt) => {
              dlqReceived = true;
              await transport.complete(receipt);
            },
          );
          subscriptions.push(dlqSub);

          // Subscribe to main queue and send to DLQ
          const mainSub = await transport.subscribe(
            queueName,
            async (_env, receipt) => {
              await transport.sendToDeadLetter?.(
                receipt,
                'undeliverable',
                envelope,
                'Test error',
              );
            },
          );
          subscriptions.push(mainSub);

          await transport.send(queueName, envelope);

          await waitFor(() => dlqReceived, 5000);

          expect(dlqReceived).toBe(true);
        });
      });

      describe('message isolation', () => {
        let namespace: string;

        beforeEach(async () => {
          namespace = getNamespace();
          await transport.connect();
          await transport.applyTopology(createTestTopology(namespace));
        });

        it('should isolate messages between queues', async () => {
          const queue1 = `${namespace}.events`;
          const queue2 = `${namespace}.notifications`;

          const receivedQueue1: unknown[] = [];
          const receivedQueue2: unknown[] = [];

          const sub1 = await transport.subscribe(
            queue1,
            async (env, receipt) => {
              receivedQueue1.push(env);
              await transport.complete(receipt);
            },
          );
          subscriptions.push(sub1);

          const sub2 = await transport.subscribe(
            queue2,
            async (env, receipt) => {
              receivedQueue2.push(env);
              await transport.complete(receipt);
            },
          );
          subscriptions.push(sub2);

          // Send to queue1 only
          await transport.send(
            queue1,
            createTestEnvelope({ eventKey: 'queue1.event' }),
          );

          await waitFor(() => receivedQueue1.length > 0, 5000);

          expect(receivedQueue1.length).toBe(1);
          expect(receivedQueue2.length).toBe(0);
        });
      });
    });
  },
);
