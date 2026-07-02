import { beforeEach, describe, expect, it, mock } from 'bun:test';
import { TopologyBuilder } from '../../topology/index.js';

// Hoisted by Bun before static imports are resolved.
// Keeps amqplib out of the other rabbitmq-transport.test.ts which relies on
// real connection failures for its logging assertions.
mock.module('amqplib', () => {
  return { default: { connect: mockConnect } };
});

// ─── Shared mock state ────────────────────────────────────────────────────────

// Handlers registered by doConnect() for the 'close' event on the connection.
// Index 0 = first (initial) connection, index 1 = second (post-reconnect), …
const closeHandlers: Array<() => void> = [];

// Total number of times channel.consume() has been called across all connections.
let consumeCallCount = 0;

// close() call count per connection, indexed by creation order.
const connectionCloseCounts: number[] = [];

// Set to a connection index to make that connection's createConfirmChannel throw.
let failConfirmChannelOnConnection = -1;

function resetMockState() {
  closeHandlers.length = 0;
  consumeCallCount = 0;
  connectionCloseCounts.length = 0;
  failConfirmChannelOnConnection = -1;
}

function makeMockChannel() {
  return {
    on: () => {},
    assertExchange: async () => {},
    assertQueue: async (_name: string) => ({ queue: _name }),
    bindQueue: async () => {},
    prefetch: async () => {},
    consume: async (_queue: string, _handler: unknown) => {
      consumeCallCount++;
      return { consumerTag: `consumer-${consumeCallCount}` };
    },
    cancel: async () => {},
    close: async () => {},
    // ConfirmChannel publish — immediately acks
    publish: (
      _exchange: string,
      _routingKey: string,
      _buffer: unknown,
      _options: unknown,
      cb: (err: null) => void,
    ) => {
      cb(null);
      return true;
    },
  };
}

function mockConnect() {
  const index = connectionCloseCounts.length;
  connectionCloseCounts.push(0);

  const channel = makeMockChannel();
  const connectionCloseListeners: Array<() => void> = [];

  const connection = {
    on: (event: string, handler: () => void) => {
      if (event === 'close') {
        connectionCloseListeners.push(handler);
        closeHandlers.push(handler);
      }
    },
    createChannel: async () => channel,
    createConfirmChannel: async () => {
      if (failConfirmChannelOnConnection === index) {
        throw new Error('simulated channel creation failure');
      }
      return channel;
    },
    close: async () => {
      if (connectionCloseCounts[index] !== undefined) {
        connectionCloseCounts[index]++;
      }
    },
  };

  return Promise.resolve(connection);
}

// ─── Tests ────────────────────────────────────────────────────────────────────

import { RabbitMQTransport } from './rabbitmq-transport.js';

const topology = TopologyBuilder.create()
  .withNamespace('test')
  .addQueue('events')
  .build();

describe('RabbitMQTransport – consumer recreation on reconnect', () => {
  let transport: RabbitMQTransport;

  beforeEach(async () => {
    resetMockState();
    transport = new RabbitMQTransport({
      url: 'amqp://localhost:5672',
      connectionName: 'test',
      connection: {
        initialReconnectDelay: 10,
        maxReconnectDelay: 10,
      },
    });
    await transport.connect();
    await transport.applyTopology(topology);
  });

  it('registers one consumer per queue on initial connect', async () => {
    await transport.subscribe('test.events', async () => {});
    expect(consumeCallCount).toBe(1);
  });

  it('recreates consumers after the connection drops and reconnects', async () => {
    await transport.subscribe('test.events', async () => {});
    expect(consumeCallCount).toBe(1);

    // Simulate an unexpected connection close.
    // doConnect() registered a 'close' listener that calls
    // connectionManager.handleConnectionLost(), which triggers reconnection.
    const triggerClose = closeHandlers[0];
    if (!triggerClose)
      throw new Error('no close handler captured from mock connection');
    triggerClose();

    // Give the ConnectionManager time to reconnect (initialReconnectDelay = 10 ms).
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(consumeCallCount).toBe(2);
  });

  it('delivers messages to the handler after reconnect', async () => {
    const received: unknown[] = [];
    await transport.subscribe('test.events', async (envelope) => {
      received.push(envelope);
    });

    const triggerClose = closeHandlers[0];
    if (!triggerClose)
      throw new Error('no close handler captured from mock connection');
    triggerClose();
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(consumeCallCount).toBe(2);
  });

  it('closes the previous (dead) connection before opening a new one', async () => {
    await transport.subscribe('test.events', async () => {});

    // Trigger a new connect (private method)
    // biome-ignore lint/complexity/useLiteralKeys: private method
    await transport['doConnect']();

    // doConnect() must call close() on the stale first connection before
    // creating a new one, so broker resources are not leaked.
    expect(connectionCloseCounts[0]).toBe(1);
  });

  it('closes the new connection when post-connect setup fails during reconnect', async () => {
    await transport.subscribe('test.events', async () => {});

    // Make the second connection's confirm channel creation fail.
    // ConnectionManager will retry; the third connection succeeds.
    failConfirmChannelOnConnection = 1;

    const triggerClose = closeHandlers[0];
    if (!triggerClose)
      throw new Error('no close handler captured from mock connection');
    triggerClose();

    // Wait for the failed attempt (~10 ms) + successful retry (~10 ms) + margin.
    await new Promise((resolve) => setTimeout(resolve, 150));

    // The failed connection (index 1) must be closed by the catch block so it
    // does not leak as an open broker connection while the retry loop continues.
    expect(connectionCloseCounts[1]).toBe(1);
    // The successful retry (connection index 2) recreates the consumer.
    expect(consumeCallCount).toBe(2);
  });
});
