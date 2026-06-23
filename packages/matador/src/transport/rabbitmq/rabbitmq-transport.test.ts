import { describe, expect, it, mock } from 'bun:test';
import type { Logger } from '../../hooks/index.js';
import { TopologyBuilder } from '../../topology/index.js';
import type { QueueDefinition, Topology } from '../../topology/index.js';
import { RabbitMQTransport, redactAmqpUrl } from './rabbitmq-transport.js';

describe('redactAmqpUrl', () => {
  it('should redact username and password with 4 asterisks', () => {
    const url = 'amqp://myuser:mypassword@localhost:5672';
    const redacted = redactAmqpUrl(url);
    expect(redacted).toBe('amqp://****:****@localhost:5672');
  });

  it('should redact credentials in amqps URLs', () => {
    const url = 'amqps://admin:secret123@rabbitmq.example.com:5671';
    const redacted = redactAmqpUrl(url);
    expect(redacted).toBe('amqps://****:****@rabbitmq.example.com:5671');
  });

  it('should redact long credentials to exactly 4 asterisks', () => {
    const url =
      'amqp://verylongusername:verylongpassword@rabbitmq-cluster.svc.local:5672';
    const redacted = redactAmqpUrl(url);
    expect(redacted).toBe('amqp://****:****@rabbitmq-cluster.svc.local:5672');
  });

  it('should redact short credentials to exactly 4 asterisks', () => {
    const url = 'amqp://a:b@host:5672';
    const redacted = redactAmqpUrl(url);
    expect(redacted).toBe('amqp://****:****@host:5672');
  });

  it('should preserve vhost in URL', () => {
    const url = 'amqp://user:pass@host:5672/myvhost';
    const redacted = redactAmqpUrl(url);
    expect(redacted).toBe('amqp://****:****@host:5672/myvhost');
  });

  it('should not modify URL without credentials', () => {
    const url = 'amqp://localhost:5672';
    const redacted = redactAmqpUrl(url);
    expect(redacted).toBe('amqp://localhost:5672');
  });

  it('should not modify URL with only username (no password)', () => {
    const url = 'amqp://guest@localhost:5672';
    const redacted = redactAmqpUrl(url);
    expect(redacted).toBe('amqp://guest@localhost:5672');
  });

  it('should handle URL-encoded credentials', () => {
    // URL-encoded @ in password: p%40ssword
    const url = 'amqp://user:p%40ssword@host:5672';
    const redacted = redactAmqpUrl(url);
    expect(redacted).toBe('amqp://****:****@host:5672');
  });
});

describe('RabbitMQTransport', () => {
  describe('connection logging', () => {
    it('should log redacted connection URL when connecting', async () => {
      const mockLogger: Logger = {
        debug: mock(() => {}),
        info: mock(() => {}),
        warn: mock(() => {}),
        error: mock(() => {}),
      };

      const transport = new RabbitMQTransport({
        url: 'amqp://testuser:testpass@localhost:5672',
        connectionName: 'test-connection',
        logger: mockLogger,
        connection: {
          maxReconnectAttempts: 1, // Only try once to avoid long retries
          initialReconnectDelay: 10,
        },
      });

      // Attempt to connect - it will fail since there's no RabbitMQ server
      // but the log should still be emitted before the connection attempt
      try {
        await transport.connect();
      } catch {
        // Expected to fail - no RabbitMQ server running
      }

      // Verify the log was called with the redacted URL
      expect(mockLogger.info).toHaveBeenCalledWith(
        "[Matador] \u23F3 Connecting to RabbitMQ at 'amqp://****:****@localhost:5672'.",
      );
    });

    it('should log connection URL as-is when no credentials provided', async () => {
      const mockLogger: Logger = {
        debug: mock(() => {}),
        info: mock(() => {}),
        warn: mock(() => {}),
        error: mock(() => {}),
      };

      const transport = new RabbitMQTransport({
        url: 'amqp://localhost:5672',
        connectionName: 'test-connection',
        logger: mockLogger,
        connection: {
          maxReconnectAttempts: 1,
          initialReconnectDelay: 10,
        },
      });

      try {
        await transport.connect();
      } catch {
        // Expected to fail
      }

      expect(mockLogger.info).toHaveBeenCalledWith(
        "[Matador] \u23F3 Connecting to RabbitMQ at 'amqp://localhost:5672'.",
      );
    });
  });
});

describe('dead-letter and retry queue naming under withNaming', () => {
  // A v1-style naming strategy: a migrating deployment keeps Matador v1's
  // `matador.{ns}.{queue}` names so DLQ/retry resources stay identifiable.
  const NS = 'myapp';
  const v1Naming = {
    queue: (ns: string, q: string) => `matador.${ns}.${q}`,
    mainExchange: (ns: string) => `matador.${ns}`,
    dlxExchange: (ns: string) => `matador.${ns}.dlx-undeliverable`,
    dlxExchangeType: 'topic' as const,
  };

  function buildTopology(): Topology {
    return TopologyBuilder.create()
      .withNamespace(NS)
      .withNaming(v1Naming)
      .addQueue('events')
      .build();
  }

  interface AssertQueueCall {
    name: string;
    options: unknown;
  }
  interface BindQueueCall {
    queue: string;
    exchange: string;
    routingKey: string;
  }

  function createRecordingChannel() {
    const assertQueueCalls: AssertQueueCall[] = [];
    const bindQueueCalls: BindQueueCall[] = [];
    const channel = {
      assertQueue: (name: string, options: unknown) => {
        assertQueueCalls.push({ name, options });
        return Promise.resolve({ queue: name });
      },
      bindQueue: (queue: string, exchange: string, routingKey: string) => {
        bindQueueCalls.push({ queue, exchange, routingKey });
        return Promise.resolve({});
      },
    };
    return { channel, assertQueueCalls, bindQueueCalls };
  }

  function createTransport() {
    return new RabbitMQTransport({
      url: 'amqp://localhost:5672',
      connectionName: 'test',
    });
  }

  it('derives the retry queue name through the naming strategy', async () => {
    const transport = createTransport();
    const topology = buildTopology();
    const queueDef = topology.queues[0] as QueueDefinition;
    const { channel, assertQueueCalls, bindQueueCalls } =
      createRecordingChannel();

    await (
      transport as unknown as {
        assertRetryQueue(
          channel: unknown,
          topology: Topology,
          queueDef: QueueDefinition,
        ): Promise<void>;
      }
    ).assertRetryQueue(channel, topology, queueDef);

    expect(assertQueueCalls.map((c) => c.name)).toEqual([
      'matador.myapp.events.retry',
    ]);
    // Retry queue dead-letters back to the (overridden) main exchange using the
    // resolved work-queue name as the routing key.
    const retryOptions = assertQueueCalls[0]?.options as {
      arguments: Record<string, unknown>;
    };
    expect(retryOptions.arguments['x-dead-letter-exchange']).toBe(
      'matador.myapp',
    );
    expect(retryOptions.arguments['x-dead-letter-routing-key']).toBe(
      'matador.myapp.events',
    );
    expect(bindQueueCalls).toEqual([
      {
        queue: 'matador.myapp.events.retry',
        exchange: 'matador.myapp',
        routingKey: 'matador.myapp.events.retry',
      },
    ]);
  });

  it('derives DLQ names through the naming strategy and binds them to the overridden DLX', async () => {
    const transport = createTransport();
    const topology = buildTopology();
    const { channel, assertQueueCalls, bindQueueCalls } =
      createRecordingChannel();

    const assertDeadLetterQueues = (
      transport as unknown as {
        assertDeadLetterQueues(
          channel: unknown,
          topology: Topology,
          dlqType: 'unhandled' | 'undeliverable',
        ): Promise<void>;
      }
    ).assertDeadLetterQueues.bind(transport);

    await assertDeadLetterQueues(channel, topology, 'unhandled');
    await assertDeadLetterQueues(channel, topology, 'undeliverable');

    expect(assertQueueCalls.map((c) => c.name)).toEqual([
      'matador.myapp.events.unhandled',
      'matador.myapp.events.undeliverable',
    ]);
    expect(bindQueueCalls).toEqual([
      {
        queue: 'matador.myapp.events.unhandled',
        exchange: 'matador.myapp.dlx-undeliverable',
        routingKey: 'matador.myapp.events.unhandled',
      },
      {
        queue: 'matador.myapp.events.undeliverable',
        exchange: 'matador.myapp.dlx-undeliverable',
        routingKey: 'matador.myapp.events.undeliverable',
      },
    ]);
  });
});

describe('confirmPublish', () => {
  type PublishCallback = (err: Error | null) => void;

  function createTransport(publishTimeoutMs?: number) {
    return new RabbitMQTransport({
      url: 'amqp://localhost:5672',
      connectionName: 'test',
      ...(publishTimeoutMs !== undefined ? { publishTimeoutMs } : {}),
    });
  }

  function callConfirmPublish(
    transport: RabbitMQTransport,
    publish: (cb: PublishCallback) => void,
  ): Promise<void> {
    const fakeChannel = {
      publish: (
        _exchange: string,
        _routingKey: string,
        _buffer: Buffer,
        _options: unknown,
        cb: PublishCallback,
      ) => {
        publish(cb);
        return true;
      },
    };
    return (
      transport as unknown as {
        confirmPublish(
          channel: unknown,
          exchange: string,
          routingKey: string,
          buffer: Buffer,
          options: unknown,
        ): Promise<void>;
      }
    ).confirmPublish(fakeChannel, 'ex', 'queue', Buffer.from('x'), {});
  }

  it('resolves when the broker confirms the publish', async () => {
    const transport = createTransport();
    await callConfirmPublish(transport, (cb) => cb(null));
  });

  it('rejects with TransportSendError when the broker nacks', async () => {
    const transport = createTransport();
    await expect(
      callConfirmPublish(transport, (cb) => cb(new Error('nacked'))),
    ).rejects.toMatchObject({
      name: 'TransportSendError',
      queue: 'queue',
    });
  });

  it('rejects when the broker never confirms within the timeout', async () => {
    const transport = createTransport(50);
    await expect(
      callConfirmPublish(transport, () => {
        // Broker never calls back.
      }),
    ).rejects.toMatchObject({ name: 'TransportSendError' });
  });

  it('ignores a late confirm after the timeout already rejected', async () => {
    const transport = createTransport(50);
    let lateCallback: PublishCallback | undefined;
    await expect(
      callConfirmPublish(transport, (cb) => {
        lateCallback = cb;
      }),
    ).rejects.toMatchObject({ name: 'TransportSendError' });
    // Late confirm must not throw or double-settle.
    lateCallback?.(null);
  });
});
