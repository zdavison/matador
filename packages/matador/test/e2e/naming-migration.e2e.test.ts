import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import {
  RabbitMQContainer,
  type StartedRabbitMQContainer,
} from '@testcontainers/rabbitmq';
import amqplib from 'amqplib';
import { TopologyBuilder } from '../../src/topology/builder.js';
import { resolveTargetQueueName } from '../../src/topology/types.js';
import { RabbitMQTransport } from '../../src/transport/rabbitmq/rabbitmq-transport.js';
import { createTestEnvelope } from './transport-compliance.e2e.test.js';

// Skip tests if docker is not available
const SKIP_E2E = process.env.SKIP_E2E_TESTS === 'true';

/**
 * Simulates a broker that already has Matador v1 resources declared, then
 * verifies a v3 transport configured with naming overrides can:
 *  1. re-assert the same resources without PRECONDITION_FAILED, and
 *  2. deliver messages into the pre-existing v1-named queues so a rolling
 *     deploy keeps using the resources already declared on the broker.
 */
describe.skipIf(SKIP_E2E)('Naming migration E2E (v1-compatible names)', () => {
  let container: StartedRabbitMQContainer;
  let connectionUrl: string;
  let transport: RabbitMQTransport | undefined;

  const NS = 'myapp';
  const V1_EXCHANGE = `matador.${NS}`;
  const V1_DLX = `matador.${NS}.dlx-undeliverable`;
  const V1_QUEUE = `matador.${NS}.general`;
  const V1_REMOTE_QUEUE = 'matador.shared.id-platform';
  const V1_REMOTE_DLX = 'matador.shared.dlx-undeliverable';

  beforeAll(async () => {
    container = await new RabbitMQContainer('rabbitmq:3.13-management')
      .withExposedPorts(5672, 15672)
      .start();
    connectionUrl = container.getAmqpUrl();

    // Declare resources exactly the way Matador v1 did.
    const connection = await amqplib.connect(connectionUrl);
    const channel = await connection.createChannel();
    await channel.assertExchange(V1_EXCHANGE, 'direct', { durable: true });
    await channel.assertExchange(V1_DLX, 'topic', { durable: true });
    await channel.assertQueue(V1_QUEUE, {
      durable: true,
      deadLetterExchange: V1_DLX,
      arguments: { 'x-queue-type': 'quorum' },
    });
    await channel.bindQueue(V1_QUEUE, V1_EXCHANGE, V1_QUEUE);
    // Remote (cross-namespace) queue owned by another service.
    await channel.assertExchange(V1_REMOTE_DLX, 'topic', { durable: true });
    await channel.assertQueue(V1_REMOTE_QUEUE, {
      durable: true,
      deadLetterExchange: V1_REMOTE_DLX,
      arguments: { 'x-queue-type': 'quorum' },
    });
    await channel.close();
    await connection.close();
  }, 120_000);

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
  });

  afterEach(async () => {
    if (transport?.isConnected()) {
      await transport.disconnect();
    }
    transport = undefined;
  });

  function createV1CompatTopology() {
    return TopologyBuilder.create()
      .withNamespace(NS)
      .withNaming({
        queue: (ns, q) => `matador.${ns}.${q}`,
        mainExchange: (ns) => `matador.${ns}`,
        dlxExchange: (ns) => `matador.${ns}.dlx-undeliverable`,
        dlxExchangeType: 'topic',
        delayedExchange: (ns) => `matador.${ns}.delayed`,
      })
      .addQueue('general')
      .addQueue({
        name: V1_REMOTE_QUEUE,
        exact: true,
        transport: {
          rabbitmq: {
            options: {
              durable: true,
              deadLetterExchange: V1_REMOTE_DLX,
              arguments: { 'x-queue-type': 'quorum' },
            },
          },
        },
      })
      .build();
  }

  it('re-asserts pre-existing v1 resources without PRECONDITION_FAILED', async () => {
    transport = new RabbitMQTransport({
      url: connectionUrl,
      connectionName: 'naming-migration-e2e',
    });
    await transport.connect();

    // Throws PRECONDITION_FAILED if any assert mismatches the v1 resources.
    await transport.applyTopology(createV1CompatTopology());

    expect(transport.isConnected()).toBe(true);
  });

  it('delivers to the pre-existing v1-named work queue', async () => {
    const topology = createV1CompatTopology();
    transport = new RabbitMQTransport({
      url: connectionUrl,
      connectionName: 'naming-migration-e2e',
    });
    await transport.connect();
    await transport.applyTopology(topology);

    const qualified = resolveTargetQueueName(topology, 'general');
    expect(qualified).toBe(V1_QUEUE);

    const envelope = createTestEnvelope();
    await transport.send(qualified, envelope);

    // Read the message back off the raw v1 queue name.
    const connection = await amqplib.connect(connectionUrl);
    const channel = await connection.createChannel();
    const msg = await channel.get(V1_QUEUE, { noAck: true });
    await channel.close();
    await connection.close();

    expect(msg).not.toBe(false);
  });

  it('declares dead-letter and retry queues under the migration prefix', async () => {
    const topology = createV1CompatTopology();
    transport = new RabbitMQTransport({
      url: connectionUrl,
      connectionName: 'naming-migration-e2e',
    });
    await transport.connect();
    await transport.applyTopology(topology);

    // DLQ and retry names inherit the migration prefix from the `queue`
    // builder, so they stay identifiable as Matador-managed resources.
    // `checkQueue` resolves only if the queue exists (and would throw and kill
    // the channel otherwise), so each check runs on its own channel.
    const expectedQueues = [
      `matador.${NS}.general.retry`,
      `matador.${NS}.general.unhandled`,
      `matador.${NS}.general.undeliverable`,
    ];
    const connection = await amqplib.connect(connectionUrl);
    for (const queueName of expectedQueues) {
      const channel = await connection.createChannel();
      const info = await channel.checkQueue(queueName);
      expect(info.queue).toBe(queueName);
      await channel.close();
    }
    await connection.close();
  });

  it('routes an exact queue referenced by its full name as-is', async () => {
    const topology = createV1CompatTopology();
    transport = new RabbitMQTransport({
      url: connectionUrl,
      connectionName: 'naming-migration-e2e',
    });
    await transport.connect();
    await transport.applyTopology(topology);

    const qualified = resolveTargetQueueName(topology, V1_REMOTE_QUEUE);
    expect(qualified).toBe(V1_REMOTE_QUEUE);

    const envelope = createTestEnvelope();
    await transport.send(qualified, envelope);

    const connection = await amqplib.connect(connectionUrl);
    const channel = await connection.createChannel();
    const msg = await channel.get(V1_REMOTE_QUEUE, { noAck: true });
    await channel.close();
    await connection.close();

    expect(msg).not.toBe(false);
  });
});
