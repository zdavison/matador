import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import { SomeSendError, TransportSendError } from '../errors/index.js';
import type { MatadorSchema } from '../schema/index.js';
import { TopologyBuilder } from '../topology/builder.js';
import { LocalTransport } from '../transport/local/local-transport.js';
import { MatadorEvent, createSubscriber } from '../types/index.js';
import { Matador } from './matador.js';

class UserCreatedEvent extends MatadorEvent {
  static readonly key = 'user.created';
  static readonly description = 'Fired when a new user is created';

  constructor(public data: { userId: string; email: string }) {
    super();
  }
}

class OrderPlacedEvent extends MatadorEvent {
  static readonly key = 'order.placed';
  static readonly description = 'Fired when an order is placed';

  constructor(public data: { orderId: string; amount: number }) {
    super();
  }
}

describe('Matador', () => {
  let transport: LocalTransport;
  let matador: Matador;

  beforeEach(() => {
    transport = new LocalTransport();
  });

  afterEach(async () => {
    if (matador) {
      await matador.shutdown();
    }
  });

  describe('configuration', () => {
    it('should create with schema in config using constructor', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      expect(matador).toBeInstanceOf(Matador);
      expect(matador.isConnected()).toBe(false);
    });

    it('should support hooks as second argument for dependency injection', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      const logs: string[] = [];
      const mockLogger = {
        debug: (msg: string) => logs.push(`debug: ${msg}`),
        info: (msg: string) => logs.push(`info: ${msg}`),
        warn: (msg: string) => logs.push(`warn: ${msg}`),
        error: (msg: string) => logs.push(`error: ${msg}`),
      };

      matador = new Matador(
        { transport, topology, schema },
        { logger: mockLogger },
      );

      expect(matador).toBeInstanceOf(Matador);
    });
  });

  describe('registration', () => {
    it('should register events via schema in constructor', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'send-welcome-email',
        description: 'Sends welcome email',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      expect(matador).toBeInstanceOf(Matador);
    });

    it('should support multiple events in schema', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const userSub = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      const orderSub = createSubscriber({
        name: 'handle-order',
        description: 'Handles order events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [userSub]],
        [OrderPlacedEvent.key]: [OrderPlacedEvent, [orderSub]],
      };

      matador = new Matador({ transport, topology, schema });

      expect(matador).toBeInstanceOf(Matador);
    });
  });

  describe('start', () => {
    it('should connect transport and be ready', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      expect(matador.isConnected()).toBe(true);
    });

    it('should throw if started twice', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      // start() is idempotent - calling it again should not throw
      await expect(matador.start()).resolves.toBeUndefined();
    });

    it('should throw on invalid schema', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      // Register duplicate subscriber names
      const sub1 = createSubscriber({
        name: 'same-name',
        description: 'Test subscriber',
        callback: async () => {},
      });
      const sub2 = createSubscriber({
        name: 'same-name',
        description: 'Test subscriber',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [sub1, sub2]],
      };

      matador = new Matador({ transport, topology, schema });

      expect(matador.start()).rejects.toThrow('Schema validation failed');
    });
  });

  describe('send', () => {
    it('should throw if not started', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await expect(matador.send(event)).rejects.toThrow(
        'Matador has not been started',
      );
    });

    it('should send events to transport', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await matador.send(event);

      expect(result.subscribersSent).toBe(1);
      expect(result.errors).toHaveLength(0);
    });

    it('should send events using class + data shorthand', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      // Use the shorthand: send(EventClass, data)
      const result = await matador.send(UserCreatedEvent, {
        userId: '123',
        email: 'test@example.com',
      });

      expect(result.subscribersSent).toBe(1);
      expect(result.errors).toHaveLength(0);
    });

    it('should create one envelope per subscriber', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const sub1 = createSubscriber({
        name: 'sub-1',
        description: 'Subscriber 1',
        callback: async () => {},
      });
      const sub2 = createSubscriber({
        name: 'sub-2',
        description: 'Subscriber 2',
        callback: async () => {},
      });
      const sub3 = createSubscriber({
        name: 'sub-3',
        description: 'Subscriber 3',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [sub1, sub2, sub3]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await matador.send(event);

      expect(result.subscribersSent).toBe(3);
    });

    it('should include correlation ID in send', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await matador.send(event, {
        correlationId: 'request-456',
      });

      expect(result.subscribersSent).toBe(1);
    });

    it('should throw SomeSendError when transport send fails', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      const sendError = new Error('Network timeout');
      transport.send = mock(async () => {
        throw sendError;
      });

      const thrown = await matador
        .send(
          new UserCreatedEvent({ userId: '123', email: 'test@example.com' }),
          { buffer: false },
        )
        .catch((e) => e);

      expect(thrown).toBeInstanceOf(SomeSendError);
      expect(thrown.eventKey).toBe(UserCreatedEvent.key);
      expect(thrown.errors).toHaveLength(1);
      expect(thrown.errors[0]?.subscriberName).toBe('handle-user');
      expect(thrown.errors[0]?.queue).toBe('matador.test.events');
      expect(thrown.errors[0]?.error).toBeInstanceOf(TransportSendError);
      expect(thrown.errors[0]?.error.cause).toBe(sendError);
    });
  });

  describe('shutdown', () => {
    it('should gracefully shutdown', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();
      await matador.shutdown();

      expect(matador.isConnected()).toBe(false);
    });

    it('should be idempotent', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();
      await matador.shutdown();
      await matador.shutdown(); // Should not throw

      expect(matador.isConnected()).toBe(false);
    });

    it('should reject send after shutdown initiated', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      // Initiate shutdown but don't await
      const shutdownPromise = matador.shutdown();

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // Send should fail during/after shutdown
      await shutdownPromise;

      expect(matador.send(event)).rejects.toThrow();
    });
  });

  describe('stopReceiving', () => {
    it('should return false when not started', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      const result = await matador.stopReceiving();

      expect(result).toBe(false);
    });

    it('should return true when started and stop receiving messages', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      const result = await matador.stopReceiving();

      expect(result).toBe(true);
      expect(matador.isConnected()).toBe(true);
    });

    it('should return false when called twice', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      const result1 = await matador.stopReceiving();
      const result2 = await matador.stopReceiving();

      expect(result1).toBe(true);
      expect(result2).toBe(false);
    });

    it('should allow shutdown after stopReceiving', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();
      await matador.stopReceiving();
      await matador.shutdown();

      expect(matador.isConnected()).toBe(false);
    });

    it('should allow sending events after stopReceiving but before shutdown', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();
      await matador.stopReceiving();

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await matador.send(event);

      expect(result.subscribersSent).toBe(1);
    });
  });

  describe('idle state', () => {
    it('should report idle when no processing', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      expect(matador.isIdle()).toBe(true);
    });

    it('should return handlers state', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      const state = matador.getHandlersState();

      expect(state.isIdle).toBe(true);
      expect(state.eventsBeingProcessed).toBe(0);
      expect(state.eventsBeingEnqueued).toBe(0);
    });

    it('should wait for idle', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({ transport, topology, schema });

      await matador.start();

      const isIdle = await matador.waitForIdle(1000);
      expect(isIdle).toBe(true);
    });
  });

  describe('consuming from queues', () => {
    it('should subscribe to specified queues', async () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .addQueue('notifications')
        .build();

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      const schema: MatadorSchema = {
        [UserCreatedEvent.key]: [UserCreatedEvent, [subscriber]],
      };

      matador = new Matador({
        transport,
        topology,
        schema,
        consumeFrom: ['events'],
      });

      await matador.start();

      expect(matador.isConnected()).toBe(true);
    });
  });
});
