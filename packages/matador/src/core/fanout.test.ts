import { beforeEach, describe, expect, it, mock } from 'bun:test';
import { TransportSendError } from '../errors/index.js';
import { SafeHooks } from '../hooks/index.js';
import type { MatadorHooks } from '../hooks/index.js';
import { SchemaRegistry } from '../schema/index.js';
import { TopologyBuilder, resolveTargetQueueName } from '../topology/index.js';
import { LocalTransport, MultiTransport } from '../transport/index.js';
import type { Transport } from '../transport/index.js';
import {
  MatadorEvent,
  createSubscriber,
  createSubscriberStub,
} from '../types/index.js';
import type { Envelope, EventOptions } from '../types/index.js';
import { FanoutEngine } from './fanout.js';

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

class UserCreatedEventWithMetadata extends MatadorEvent {
  static readonly key = 'user.created.with-metadata';
  static readonly description =
    'Fired when a new user is created (with metadata)';

  constructor(
    public data: { userId: string; email: string },
    public override metadata?: { source: string; version: number },
  ) {
    super();
  }
}

const testTopology = TopologyBuilder.create()
  .withNamespace('test')
  .addQueue('events')
  .addQueue('notifications')
  .addQueue('queue-1')
  .addQueue('queue-2')
  .build();

describe('FanoutEngine', () => {
  let transport: Transport;
  let schema: SchemaRegistry;
  let hooks: SafeHooks;
  let fanout: FanoutEngine;

  beforeEach(() => {
    schema = new SchemaRegistry();

    transport = {
      name: 'mock',
      capabilities: {
        deliveryModes: ['at-least-once'],
        delayedMessages: true,
        deadLetterRouting: 'native',
        attemptTracking: false,
        concurrencyModel: 'prefetch',
        ordering: 'none',
        priorities: false,
      },
      isConnected: () => true,
      connect: mock(async () => {}),
      disconnect: mock(async () => {}),
      send: mock(async () => 'mock'),
      subscribe: mock(async () => ({
        unsubscribe: async () => {},
        isActive: true,
      })),
      applyTopology: mock(async () => {}),
      complete: mock(async () => {}),
    };

    hooks = new SafeHooks();

    fanout = new FanoutEngine({
      transport,
      schema,
      hooks,
      topology: testTopology,
      defaultQueue: 'events',
    });
  });

  describe('creation', () => {
    it('should create instance via static factory', () => {
      expect(fanout).toBeInstanceOf(FanoutEngine);
    });

    it('should create instance via constructor', () => {
      const instance = new FanoutEngine({
        transport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });
      expect(instance).toBeInstanceOf(FanoutEngine);
    });

    it('should initialize with zero events being enqueued', () => {
      expect(fanout.eventsBeingEnqueuedCount).toBe(0);
    });
  });

  describe('send() with single subscriber', () => {
    it('should send event to one subscriber', async () => {
      const subscriber = createSubscriber({
        name: 'send-welcome-email',
        description: 'Sends welcome email',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.eventKey).toBe('user.created');
      expect(result.subscribersSent).toBe(1);
      expect(result.subscribersSkipped).toBe(0);
      expect(result.errors).toHaveLength(0);
      expect(transport.send).toHaveBeenCalledTimes(1);
    });

    it('should use default queue when subscriber has no targetQueue', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      expect(transport.send).toHaveBeenCalledWith(
        'matador.test.events',
        expect.any(Object),
        undefined,
      );
    });

    it('should use subscriber targetQueue when specified', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
        targetQueue: 'notifications',
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      expect(transport.send).toHaveBeenCalledWith(
        'matador.test.notifications',
        expect.any(Object),
        undefined,
      );
    });

    it('should create envelope with correct structure', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      const sendCall = (transport.send as ReturnType<typeof mock>).mock
        .calls[0];
      if (!sendCall) throw new Error('expected transport.send to be called');
      const envelope = sendCall[1] as Envelope;

      expect(envelope.docket.eventKey).toBe('user.created');
      expect(envelope.docket.eventDescription).toBe(
        'Fired when a new user is created',
      );
      expect(envelope.docket.targetSubscriber).toBe('handle-user');
      expect(envelope.data).toEqual({
        userId: '123',
        email: 'test@example.com',
      });
      expect(envelope.id).toBeDefined();
      expect(envelope.docket.importance).toBe('should-investigate');
    });

    it('should include eventDescription from event class', async () => {
      const subscriber = createSubscriber({
        name: 'order-handler',
        description: 'Handles order events',
        callback: async () => {},
      });

      schema.register(OrderPlacedEvent, [subscriber]);

      const event = new OrderPlacedEvent({
        orderId: 'ord_123',
        amount: 99.99,
      });

      await fanout.send(OrderPlacedEvent, event);

      const sendCall = (transport.send as ReturnType<typeof mock>).mock
        .calls[0];
      if (!sendCall) throw new Error('expected transport.send to be called');
      const envelope = sendCall[1] as Envelope;

      expect(envelope.docket.eventKey).toBe('order.placed');
      expect(envelope.docket.eventDescription).toBe(
        'Fired when an order is placed',
      );
    });
  });

  describe('send() with multiple subscribers', () => {
    it('should send to all subscribers', async () => {
      const sub1 = createSubscriber({
        name: 'subscriber-1',
        description: 'Subscriber 1',
        callback: async () => {},
      });
      const sub2 = createSubscriber({
        name: 'subscriber-2',
        description: 'Subscriber 2',
        callback: async () => {},
      });
      const sub3 = createSubscriber({
        name: 'subscriber-3',
        description: 'Subscriber 3',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [sub1, sub2, sub3]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(3);
      expect(result.subscribersSkipped).toBe(0);
      expect(result.errors).toHaveLength(0);
      expect(transport.send).toHaveBeenCalledTimes(3);
    });

    it('should create separate envelope for each subscriber', async () => {
      const sub1 = createSubscriber({
        name: 'subscriber-1',
        description: 'Subscriber 1',
        callback: async () => {},
      });
      const sub2 = createSubscriber({
        name: 'subscriber-2',
        description: 'Subscriber 2',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [sub1, sub2]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      const calls = (transport.send as ReturnType<typeof mock>).mock.calls;
      const envelope1 = calls[0]?.[1] as Envelope;
      const envelope2 = calls[1]?.[1] as Envelope;

      expect(envelope1.id).not.toBe(envelope2.id);
      expect(envelope1.docket.targetSubscriber).toBe('subscriber-1');
      expect(envelope2.docket.targetSubscriber).toBe('subscriber-2');
      expect(envelope1.data).toEqual(envelope2.data);
    });

    it('should route to different queues based on targetQueue', async () => {
      const sub1 = createSubscriber({
        name: 'subscriber-1',
        description: 'Subscriber 1',
        callback: async () => {},
        targetQueue: 'queue-1',
      });
      const sub2 = createSubscriber({
        name: 'subscriber-2',
        description: 'Subscriber 2',
        callback: async () => {},
        targetQueue: 'queue-2',
      });

      schema.register(UserCreatedEvent, [sub1, sub2]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      const calls = (transport.send as ReturnType<typeof mock>).mock.calls;
      expect(calls[0]?.[0]).toBe('matador.test.queue-1');
      expect(calls[1]?.[0]).toBe('matador.test.queue-2');
    });

    it('should send to no subscribers when event is not registered', async () => {
      const event = new OrderPlacedEvent({
        orderId: '456',
        amount: 99.99,
      });

      const result = await fanout.send(OrderPlacedEvent, event);

      expect(result.subscribersSent).toBe(0);
      expect(result.subscribersSkipped).toBe(0);
      expect(result.errors).toHaveLength(0);
      expect(transport.send).not.toHaveBeenCalled();
    });
  });

  describe('metadata merging', () => {
    it('should include event metadata in envelope', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEventWithMetadata, [subscriber]);

      const event = new UserCreatedEventWithMetadata(
        { userId: '123', email: 'test@example.com' },
        { source: 'api', version: 1 },
      );

      await fanout.send(UserCreatedEventWithMetadata, event);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.metadata).toEqual({ source: 'api', version: 1 });
    });

    it('should include options metadata in envelope', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const options: EventOptions = {
        metadata: { requestId: 'req-123' },
      };

      await fanout.send(UserCreatedEvent, event, options);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.metadata).toEqual({ requestId: 'req-123' });
    });

    it('should merge event and options metadata with options taking precedence', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEventWithMetadata, [subscriber]);

      const event = new UserCreatedEventWithMetadata(
        { userId: '123', email: 'test@example.com' },
        { source: 'api', version: 1 },
      );

      const options: EventOptions = {
        metadata: { version: 2, requestId: 'req-123' },
      };

      await fanout.send(UserCreatedEventWithMetadata, event, options);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.metadata).toEqual({
        source: 'api',
        version: 2,
        requestId: 'req-123',
      });
    });

    it('should include universal metadata from hooks', async () => {
      const hooksWithMetadata: MatadorHooks = {
        loadUniversalMetadata: async () => ({
          environment: 'test',
          hostname: 'test-host',
        }),
      };
      const hooksInstance = new SafeHooks(hooksWithMetadata);

      const fanoutWithHooks = new FanoutEngine({
        transport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithHooks.send(UserCreatedEvent, event);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.metadata).toEqual({
        environment: 'test',
        hostname: 'test-host',
      });
    });

    it('should have empty metadata when no metadata provided', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.metadata).toEqual({});
    });
  });

  describe('filtering disabled subscribers', () => {
    it('should send to enabled subscriber', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
        enabled: () => true,
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(1);
      expect(result.subscribersSkipped).toBe(0);
    });

    it('should skip disabled subscriber', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
        enabled: () => false,
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(0);
      expect(result.subscribersSkipped).toBe(1);
      expect(transport.send).not.toHaveBeenCalled();
    });

    it('should filter some subscribers and send to others', async () => {
      const sub1 = createSubscriber({
        name: 'enabled-sub',
        description: 'Enabled subscriber',
        callback: async () => {},
        enabled: () => true,
      });
      const sub2 = createSubscriber({
        name: 'disabled-sub',
        description: 'Disabled subscriber',
        callback: async () => {},
        enabled: () => false,
      });
      const sub3 = createSubscriber({
        name: 'always-enabled-sub',
        description: 'Always enabled subscriber',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [sub1, sub2, sub3]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(2);
      expect(result.subscribersSkipped).toBe(1);
      expect(transport.send).toHaveBeenCalledTimes(2);
    });

    it('should support async enabled function', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
        enabled: async () => {
          await new Promise((resolve) => setTimeout(resolve, 10));
          return true;
        },
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(1);
      expect(result.subscribersSkipped).toBe(0);
    });

    it('should treat subscriber as enabled if enabled() throws error', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
        enabled: () => {
          throw new Error('Feature flag service down');
        },
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(1);
      expect(result.subscribersSkipped).toBe(0);
    });

    it('should treat subscriber as enabled if no enabled hook provided', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(1);
      expect(result.subscribersSkipped).toBe(0);
    });

    it('should work with subscriber stubs', async () => {
      const stub = createSubscriberStub({
        name: 'remote-subscriber',
        enabled: () => false,
      });

      schema.register(UserCreatedEvent, [stub]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanout.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(0);
      expect(result.subscribersSkipped).toBe(1);
    });
  });

  describe('error handling', () => {
    it('should capture error when transport.send fails and buffer is disabled', async () => {
      const sendError = new Error('Network timeout');
      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw sendError;
        }),
      };

      const fanoutWithFailingTransport = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanoutWithFailingTransport.send(
        UserCreatedEvent,
        event,
        { buffer: false },
      );

      expect(result.subscribersSent).toBe(0);
      expect(result.subscribersSkipped).toBe(0);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]?.subscriberName).toBe('handle-user');
      expect(result.errors[0]?.queue).toBe('matador.test.events');
      expect(result.errors[0]?.error).toBeInstanceOf(TransportSendError);
    });

    it('should continue sending to other subscribers after one fails', async () => {
      const failingTransport: Transport = {
        ...transport,
        send: mock(async (queue: string) => {
          if (queue === 'matador.test.queue-1') {
            throw new Error('Queue 1 failed');
          }
          return 'mock';
        }),
      };

      const fanoutWithFailingTransport = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const sub1 = createSubscriber({
        name: 'subscriber-1',
        description: 'Subscriber 1',
        callback: async () => {},
        targetQueue: 'queue-1',
      });
      const sub2 = createSubscriber({
        name: 'subscriber-2',
        description: 'Subscriber 2',
        callback: async () => {},
        targetQueue: 'queue-2',
      });

      schema.register(UserCreatedEvent, [sub1, sub2]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanoutWithFailingTransport.send(
        UserCreatedEvent,
        event,
        { buffer: false },
      );

      expect(result.subscribersSent).toBe(1);
      expect(result.subscribersSkipped).toBe(0);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]?.subscriberName).toBe('subscriber-1');
    });

    it('should wrap non-Error throws as Error in TransportSendError', async () => {
      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw 'string error';
        }),
      };

      const fanoutWithFailingTransport = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanoutWithFailingTransport.send(
        UserCreatedEvent,
        event,
        { buffer: false },
      );

      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]?.error).toBeInstanceOf(TransportSendError);
      expect(result.errors[0]?.error.message).toContain('matador.test.events');
    });
  });

  describe('eventsBeingEnqueuedCount tracking', () => {
    it('should increment count during send', async () => {
      let countDuringSend = 0;
      // biome-ignore lint/style/useConst: Variable must be let to be assigned after closure definition
      let fanoutWithTracking: FanoutEngine;

      const trackingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          countDuringSend = fanoutWithTracking.eventsBeingEnqueuedCount;
          return 'mock';
        }),
      };

      fanoutWithTracking = new FanoutEngine({
        transport: trackingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      expect(fanoutWithTracking.eventsBeingEnqueuedCount).toBe(0);
      await fanoutWithTracking.send(UserCreatedEvent, event);

      expect(countDuringSend).toBe(1);
    });

    it('should decrement count after send completes', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      expect(fanout.eventsBeingEnqueuedCount).toBe(0);
      await fanout.send(UserCreatedEvent, event);
      expect(fanout.eventsBeingEnqueuedCount).toBe(0);
    });

    it('should decrement count even when send fails', async () => {
      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Send failed');
        }),
      };

      const fanoutWithFailingTransport = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      expect(fanoutWithFailingTransport.eventsBeingEnqueuedCount).toBe(0);
      await fanoutWithFailingTransport.send(UserCreatedEvent, event);
      expect(fanoutWithFailingTransport.eventsBeingEnqueuedCount).toBe(0);
    });

    it('should track multiple sequential sends to subscribers', async () => {
      const sub1 = createSubscriber({
        name: 'subscriber-1',
        description: 'Subscriber 1',
        callback: async () => {},
      });
      const sub2 = createSubscriber({
        name: 'subscriber-2',
        description: 'Subscriber 2',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [sub1, sub2]);

      let maxCount = 0;
      const counts: number[] = [];
      // biome-ignore lint/style/useConst: Variable must be let to be assigned after closure definition
      let fanoutWithTracking: FanoutEngine;

      const trackingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          const current = fanoutWithTracking.eventsBeingEnqueuedCount;
          counts.push(current);
          maxCount = Math.max(maxCount, current);
          await new Promise((resolve) => setTimeout(resolve, 10));
          return 'mock';
        }),
      };

      fanoutWithTracking = new FanoutEngine({
        transport: trackingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithTracking.send(UserCreatedEvent, event);

      expect(maxCount).toBe(1);
      expect(counts).toEqual([1, 1]);
      expect(fanoutWithTracking.eventsBeingEnqueuedCount).toBe(0);
    });
  });

  describe('correlation ID propagation', () => {
    it('should include correlation ID in envelope when provided', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const options: EventOptions = {
        correlationId: 'corr-123',
      };

      await fanout.send(UserCreatedEvent, event, options);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.correlationId).toBe('corr-123');
    });

    it('should propagate same correlation ID to all subscribers', async () => {
      const sub1 = createSubscriber({
        name: 'subscriber-1',
        description: 'Subscriber 1',
        callback: async () => {},
      });
      const sub2 = createSubscriber({
        name: 'subscriber-2',
        description: 'Subscriber 2',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [sub1, sub2]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const options: EventOptions = {
        correlationId: 'corr-456',
      };

      await fanout.send(UserCreatedEvent, event, options);

      const calls = (transport.send as ReturnType<typeof mock>).mock.calls;
      const envelope1 = calls[0]?.[1] as Envelope;
      const envelope2 = calls[1]?.[1] as Envelope;

      expect(envelope1.docket.correlationId).toBe('corr-456');
      expect(envelope2.docket.correlationId).toBe('corr-456');
    });

    it('should have undefined correlation ID when not provided', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.correlationId).toBeUndefined();
    });
  });

  describe('hook invocation', () => {
    it('should call onEnqueueSuccess after successful send', async () => {
      const onEnqueueSuccess = mock(async () => {});
      const hooksWithSuccess: MatadorHooks = {
        onEnqueueSuccess,
      };
      const hooksInstance = new SafeHooks(hooksWithSuccess);

      const fanoutWithHooks = new FanoutEngine({
        transport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithHooks.send(UserCreatedEvent, event);

      expect(onEnqueueSuccess).toHaveBeenCalledTimes(1);
      expect(onEnqueueSuccess).toHaveBeenCalledWith({
        envelope: expect.any(Object),
        queue: 'matador.test.events',
        transport: 'mock',
      });

      const calls = onEnqueueSuccess.mock.calls as unknown as Array<
        [{ envelope: Envelope; queue: string }]
      >;
      const firstCall = calls[0];
      if (!firstCall) throw new Error('expected onEnqueueSuccess to be called');
      const callArgs = firstCall[0];
      expect(callArgs.envelope.docket.eventKey).toBe('user.created');
      expect(callArgs.envelope.docket.targetSubscriber).toBe('handle-user');
    });

    it('should call onEnqueueError after failed send', async () => {
      const onEnqueueError = mock(async () => {});
      const hooksWithError: MatadorHooks = {
        onEnqueueError,
      };
      const hooksInstance = new SafeHooks(hooksWithError);

      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Send failed');
        }),
      };

      const fanoutWithHooks = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithHooks.send(UserCreatedEvent, event, {
        throwOnBufferedFailure: true,
      });

      expect(onEnqueueError).toHaveBeenCalledTimes(1);
      expect(onEnqueueError).toHaveBeenCalledWith({
        envelope: expect.any(Object),
        error: expect.any(TransportSendError),
        transport: 'mock',
      });

      const calls = onEnqueueError.mock.calls as unknown as Array<
        [{ envelope: Envelope; error: TransportSendError }]
      >;
      const firstCall = calls[0];
      if (!firstCall) throw new Error('expected onEnqueueError to be called');
      const callArgs = firstCall[0];
      expect(callArgs.envelope.docket.eventKey).toBe('user.created');
      expect(callArgs.error).toBeInstanceOf(TransportSendError);
    });

    it('should call hooks for each subscriber', async () => {
      const onEnqueueSuccess = mock(async () => {});
      const hooksWithSuccess: MatadorHooks = {
        onEnqueueSuccess,
      };
      const hooksInstance = new SafeHooks(hooksWithSuccess);

      const fanoutWithHooks = new FanoutEngine({
        transport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const sub1 = createSubscriber({
        name: 'subscriber-1',
        description: 'Subscriber 1',
        callback: async () => {},
      });
      const sub2 = createSubscriber({
        name: 'subscriber-2',
        description: 'Subscriber 2',
        callback: async () => {},
      });
      const sub3 = createSubscriber({
        name: 'subscriber-3',
        description: 'Subscriber 3',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [sub1, sub2, sub3]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithHooks.send(UserCreatedEvent, event);

      expect(onEnqueueSuccess).toHaveBeenCalledTimes(3);
    });

    it('should not call onEnqueueSuccess when subscriber is disabled', async () => {
      const onEnqueueSuccess = mock(async () => {});
      const hooksWithSuccess: MatadorHooks = {
        onEnqueueSuccess,
      };
      const hooksInstance = new SafeHooks(hooksWithSuccess);

      const fanoutWithHooks = new FanoutEngine({
        transport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
        enabled: () => false,
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithHooks.send(UserCreatedEvent, event);

      expect(onEnqueueSuccess).not.toHaveBeenCalled();
    });

    it('should continue processing even if hook throws error', async () => {
      const onEnqueueSuccess = mock(async () => {
        throw new Error('Hook error');
      });
      const hooksWithSuccess: MatadorHooks = {
        onEnqueueSuccess,
      };
      const hooksInstance = new SafeHooks(hooksWithSuccess);

      const fanoutWithHooks = new FanoutEngine({
        transport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanoutWithHooks.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(1);
      expect(result.errors).toHaveLength(0);
      expect(onEnqueueSuccess).toHaveBeenCalledTimes(1);
    });
  });

  describe('delay options', () => {
    it('should pass delay option to transport', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const options: EventOptions = {
        delayMs: 5000,
      };

      await fanout.send(UserCreatedEvent, event, options);

      expect(transport.send).toHaveBeenCalledWith(
        'matador.test.events',
        expect.any(Object),
        { delay: 5000 },
      );
    });

    it('should include delay in envelope', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const options: EventOptions = {
        delayMs: 3000,
      };

      await fanout.send(UserCreatedEvent, event, options);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.scheduledFor).toBeDefined();
      if (envelope.docket.scheduledFor) {
        const scheduledTime = new Date(envelope.docket.scheduledFor).getTime();
        const now = Date.now();
        expect(scheduledTime).toBeGreaterThan(now);
        expect(scheduledTime).toBeLessThan(now + 4000);
      }
    });

    it('should not pass delay option when delayMs is undefined', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      expect(transport.send).toHaveBeenCalledWith(
        'matador.test.events',
        expect.any(Object),
        undefined,
      );
    });
  });

  describe('subscriber importance', () => {
    it('should use subscriber importance when specified', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
        importance: 'must-investigate',
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.importance).toBe('must-investigate');
    });

    it('should default to should-investigate when importance not specified', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanout.send(UserCreatedEvent, event);

      const envelope = (transport.send as ReturnType<typeof mock>).mock
        .calls[0]?.[1] as Envelope;
      expect(envelope.docket.importance).toBe('should-investigate');
    });
  });

  describe('retry buffering', () => {
    it('should buffer a failed send and not return it as an error', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Connection lost');
        }),
      };

      const fanoutWithFailingTransport = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });
      const result = await fanoutWithFailingTransport.send(
        UserCreatedEvent,
        event,
      );

      expect(result.errors).toHaveLength(0);
      expect(result.subscribersSent).toBe(0);
    });

    it('should buffer stub failures too', async () => {
      const stub = createSubscriberStub({ name: 'remote-service' });

      schema.register(UserCreatedEvent, [stub]);

      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Connection lost');
        }),
      };

      const fanoutWithFailingTransport = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });
      const result = await fanoutWithFailingTransport.send(
        UserCreatedEvent,
        event,
      );

      expect(result.errors).toHaveLength(0);
      expect(result.subscribersSent).toBe(0);
    });

    it('should buffer instead of reporting success when a stub subscriber has no local subscriber', async () => {
      // LocalTransport rejects sends to a queue with no active subscriber in
      // this process. A stub subscriber's real implementation lives in another
      // service, so this queue will never have one here — the send must fail
      // and be buffered, not reported as delivered.
      const stub = createSubscriberStub({ name: 'remote-service' });

      schema.register(UserCreatedEvent, [stub]);

      const onEnqueueSuccess = mock(async () => {});
      const hooksInstance = new SafeHooks({ onEnqueueSuccess });

      const localTransport = new LocalTransport();
      await localTransport.connect();
      await localTransport.applyTopology(testTopology);

      const fanoutWithLocal = new FanoutEngine({
        transport: localTransport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
        // Fast interval so the test doesn't have to wait on the 30s default
        // to prove the message is actually held for retry, not dropped.
        retryIntervalMs: 10,
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });
      const result = await fanoutWithLocal.send(UserCreatedEvent, event);

      expect(result.errors).toHaveLength(0);
      expect(result.subscribersSent).toBe(0);
      expect(onEnqueueSuccess).not.toHaveBeenCalled();

      // The message never reached LocalTransport's own storage (enqueue()
      // throws before storing), so it can only still exist in-memory if
      // FanoutEngine actually buffered it. Prove that by attaching a
      // subscriber and letting the periodic flush retry the send — if the
      // message had been dropped instead of buffered, nothing would arrive.
      const received: Envelope[] = [];
      await localTransport.subscribe(
        resolveTargetQueueName(testTopology, 'events'),
        async (envelope) => {
          received.push(envelope);
        },
      );

      await new Promise((resolve) => setTimeout(resolve, 30));

      expect(received).toHaveLength(1);
      expect(onEnqueueSuccess).toHaveBeenCalledTimes(1);

      fanoutWithLocal.dispose();
    });

    it('should flush buffered messages when onConnected fires', async () => {
      let connectedCallback: (() => void) | undefined;

      const failThenSucceed = mock(async () => {
        throw new Error('Connection lost');
      });

      const transportWithReconnect: Transport = {
        ...transport,
        send: failThenSucceed,
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const fanoutWithReconnect = new FanoutEngine({
        transport: transportWithReconnect,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // First send — transport fails, message should be buffered
      await fanoutWithReconnect.send(UserCreatedEvent, event);
      expect(failThenSucceed).toHaveBeenCalledTimes(1);

      // Simulate reconnect — now send succeeds
      (
        transportWithReconnect.send as ReturnType<typeof mock>
      ).mockImplementation(async () => 'mock');

      expect(connectedCallback).toBeDefined();
      connectedCallback?.();

      // Give the async flush a chance to run
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(transportWithReconnect.send).toHaveBeenCalledTimes(2);
    });

    it('should re-buffer on flush failure and retry on next reconnect', async () => {
      let connectedCallback: (() => void) | undefined;

      const transportWithReconnect: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Still down');
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const fanoutWithReconnect = new FanoutEngine({
        transport: transportWithReconnect,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithReconnect.send(UserCreatedEvent, event);
      expect(transportWithReconnect.send).toHaveBeenCalledTimes(1);

      // Simulate reconnect — flush fails, item should be re-buffered
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));

      // send was attempted again during the flush
      expect(transportWithReconnect.send).toHaveBeenCalledTimes(2);

      // Simulate a second reconnect — should retry again
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(transportWithReconnect.send).toHaveBeenCalledTimes(3);
    });

    it('should not buffer when transport works fine', async () => {
      let connectedCallback: (() => void) | undefined;

      const successTransport: Transport = {
        ...transport,
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const fanoutWithReconnect = new FanoutEngine({
        transport: successTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });
      const result = await fanoutWithReconnect.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(1);
      expect(result.errors).toHaveLength(0);

      // onConnected fires — nothing buffered so flush is a no-op
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));

      // Only one call total (the original send — not a spurious re-send)
      expect(successTransport.send).toHaveBeenCalledTimes(1);
    });

    it('should report error in result when buffer is false', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });

      schema.register(UserCreatedEvent, [subscriber]);

      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Connection lost');
        }),
      };

      const fanoutWithFailingTransport = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });
      const result = await fanoutWithFailingTransport.send(
        UserCreatedEvent,
        event,
        { buffer: false },
      );

      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]?.subscriberName).toBe('handle-user');
      expect(result.errors[0]?.error).toBeInstanceOf(TransportSendError);
    });

    it('should both buffer and report error when reportBufferedFailure is true', async () => {
      let connectedCallback: (() => void) | undefined;

      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Connection lost');
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const fanoutWithReconnect = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });
      const result = await fanoutWithReconnect.send(UserCreatedEvent, event, {
        throwOnBufferedFailure: true,
      });

      // Error is reported immediately
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]?.subscriberName).toBe('handle-user');
      // But it was also buffered — it should retry on reconnect
      (failingTransport.send as ReturnType<typeof mock>).mockImplementation(
        async () => 'mock',
      );
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(failingTransport.send).toHaveBeenCalledTimes(2);
    });

    it('should unsubscribe the onConnected listener when dispose() is called', () => {
      const unsubscribe = mock(() => {});

      const transportWithOnConnected: Transport = {
        ...transport,
        onConnected: mock((_cb) => unsubscribe),
      };

      const fanout = new FanoutEngine({
        transport: transportWithOnConnected,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      expect(unsubscribe).not.toHaveBeenCalled();
      fanout.dispose();
      expect(unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('should drop and report error when retry buffer is full', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Connection lost');
        }),
      };

      const fanoutWithSmallBuffer = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
        maxRetryBufferSize: 1,
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // First send fills the buffer
      const first = await fanoutWithSmallBuffer.send(UserCreatedEvent, event);
      expect(first.errors).toHaveLength(0);

      // Second send hits the full buffer — must report an error
      const second = await fanoutWithSmallBuffer.send(UserCreatedEvent, event);
      expect(second.errors).toHaveLength(1);
      expect(second.errors[0]?.error).toBeInstanceOf(TransportSendError);
    });

    it('should report buffer-full drop via onEnqueueError on initial send', async () => {
      const onEnqueueError = mock(async () => {});
      const hooksInstance = new SafeHooks({
        onEnqueueError,
      } satisfies MatadorHooks);

      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Connection lost');
        }),
      };

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const fanoutWithSmallBuffer = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
        maxRetryBufferSize: 1,
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // First send fills the buffer — no error reported to caller or hook
      const first = await fanoutWithSmallBuffer.send(UserCreatedEvent, event);
      expect(first.errors).toHaveLength(0);
      expect(onEnqueueError).not.toHaveBeenCalled();

      // Second send finds the buffer full — onEnqueueError fires and error is returned
      const second = await fanoutWithSmallBuffer.send(UserCreatedEvent, event);
      expect(second.errors).toHaveLength(1);
      expect(onEnqueueError).toHaveBeenCalledTimes(1);
    });

    it('should not buffer when a fallback transport succeeds after the primary fails', async () => {
      const rabbitMock: Transport = {
        ...transport,
        name: 'rabbitmq',
        send: mock(async () => {
          throw new Error('RabbitMQ down');
        }),
      };
      const localMock: Transport = {
        ...transport,
        name: 'local',
        send: mock(async () => 'local'),
      };

      const multiTransport = new MultiTransport({
        transports: [rabbitMock, localMock],
      });

      const fanoutWithMulti = new FanoutEngine({
        transport: multiTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanoutWithMulti.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(1);
      expect(result.errors).toHaveLength(0);
      expect(rabbitMock.send).toHaveBeenCalledTimes(1);
      expect(localMock.send).toHaveBeenCalledTimes(1);
      expect(fanoutWithMulti.eventsBeingEnqueuedCount).toBe(0);
    });

    it('should buffer (not report success) when a real MultiTransport falls back to local for a stub subscriber', async () => {
      const stub = createSubscriberStub({ name: 'remote-service' });
      schema.register(UserCreatedEvent, [stub]);

      const rabbitMock: Transport = {
        ...transport,
        name: 'rabbitmq',
        send: mock(async () => {
          throw new Error('RabbitMQ down');
        }),
      };
      const localTransport = new LocalTransport();
      await localTransport.connect();
      await localTransport.applyTopology(testTopology);

      const multiTransport = new MultiTransport({
        transports: [rabbitMock, localTransport],
      });

      const onEnqueueSuccess = mock(async () => {});
      const hooksInstance = new SafeHooks({ onEnqueueSuccess });

      const fanoutWithMulti = new FanoutEngine({
        transport: multiTransport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
        // Fast interval so the test doesn't have to wait on the 30s default
        // to prove the message is actually held for retry, not dropped.
        retryIntervalMs: 10,
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const result = await fanoutWithMulti.send(UserCreatedEvent, event);

      expect(result.subscribersSent).toBe(0);
      expect(result.errors).toHaveLength(0);
      expect(onEnqueueSuccess).not.toHaveBeenCalled();
      expect(rabbitMock.send).toHaveBeenCalledTimes(1);

      // Neither rabbitmq nor local ever stored the message durably (rabbit
      // threw, local's enqueue() throws with no subscriber), so it can only
      // still be in memory if FanoutEngine actually buffered it. Prove that
      // by attaching a local subscriber and letting the periodic flush retry.
      const received: Envelope[] = [];
      await localTransport.subscribe(
        resolveTargetQueueName(testTopology, 'events'),
        async (envelope) => {
          received.push(envelope);
        },
      );

      await new Promise((resolve) => setTimeout(resolve, 30));

      expect(received).toHaveLength(1);
      expect(onEnqueueSuccess).toHaveBeenCalledTimes(1);

      fanoutWithMulti.dispose();
    });

    it('should re-buffer a stub message that falls back to local again on flush, then deliver once the primary recovers', async () => {
      const stub = createSubscriberStub({ name: 'remote-service' });
      schema.register(UserCreatedEvent, [stub]);

      let rabbitUp = false;
      let connectedCallback: (() => void) | undefined;

      const rabbitMock: Transport = {
        ...transport,
        name: 'rabbitmq',
        send: mock(async () => {
          if (!rabbitUp) throw new Error('RabbitMQ down');
          return 'rabbitmq';
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };
      const localTransport = new LocalTransport();
      await localTransport.connect();
      await localTransport.applyTopology(testTopology);

      const multiTransport = new MultiTransport({
        transports: [rabbitMock, localTransport],
      });

      const onEnqueueSuccess = mock(async () => {});
      const hooksInstance = new SafeHooks({ onEnqueueSuccess });

      const fanoutWithMulti = new FanoutEngine({
        transport: multiTransport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // Initial send: rabbit down, falls back to local, buffered (not delivered).
      await fanoutWithMulti.send(UserCreatedEvent, event);
      expect(onEnqueueSuccess).not.toHaveBeenCalled();

      // Reconnect fires while rabbit is still down — flush falls back to local
      // again and must re-buffer, not report success.
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(onEnqueueSuccess).not.toHaveBeenCalled();

      // Rabbit recovers — next flush should deliver for real via rabbitmq.
      rabbitUp = true;
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(onEnqueueSuccess).toHaveBeenCalledTimes(1);
      expect(onEnqueueSuccess).toHaveBeenCalledWith(
        expect.objectContaining({ transport: 'rabbitmq' }),
      );
    });

    it('should retry via the periodic interval even without a reconnect event', async () => {
      // Covers the case where the transport stays connected the whole time
      // but an individual publish keeps failing — onConnected never fires
      // again for that, so only the interval can recover it.
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      let shouldFail = true;
      const flakyTransport: Transport = {
        ...transport,
        send: mock(async () => {
          if (shouldFail) throw new Error('Publish nacked');
          return 'mock';
        }),
        // Deliberately no onConnected — the transport never disconnects.
      };

      const fanoutWithInterval = new FanoutEngine({
        transport: flakyTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
        retryIntervalMs: 20,
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithInterval.send(UserCreatedEvent, event);
      expect(flakyTransport.send).toHaveBeenCalledTimes(1);

      shouldFail = false;
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(flakyTransport.send).toHaveBeenCalledTimes(2);

      fanoutWithInterval.dispose();
    });

    it('should stop retrying the rest of the buffer after 10 consecutive failures in a flush pass', async () => {
      let connectedCallback: (() => void) | undefined;
      let shouldFail = true;

      const flakyTransport: Transport = {
        ...transport,
        send: mock(async () => {
          if (shouldFail) throw new Error('Broker rejecting everything');
          return 'mock';
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const fanoutWithReconnect = new FanoutEngine({
        transport: flakyTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // 12 sends buffered; flush bails after the 10th consecutive failure,
      // leaving 2 untried.
      for (let i = 0; i < 12; i++) {
        await fanoutWithReconnect.send(UserCreatedEvent, event);
      }
      expect(flakyTransport.send).toHaveBeenCalledTimes(12);

      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(flakyTransport.send).toHaveBeenCalledTimes(22);

      // Recovery: next flush delivers all 12 re-buffered/untried items.
      shouldFail = false;
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(flakyTransport.send).toHaveBeenCalledTimes(34);
    });

    it('should respect a custom maxConsecutiveFlushFailures threshold', async () => {
      let connectedCallback: (() => void) | undefined;

      const flakyTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Broker rejecting everything');
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const fanoutWithCustomThreshold = new FanoutEngine({
        transport: flakyTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
        maxConsecutiveFlushFailures: 3,
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // 5 sends buffered; flush should bail after the 3rd consecutive
      // failure, leaving 2 untried, instead of the default threshold of 10.
      for (let i = 0; i < 5; i++) {
        await fanoutWithCustomThreshold.send(UserCreatedEvent, event);
      }
      expect(flakyTransport.send).toHaveBeenCalledTimes(5);

      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(flakyTransport.send).toHaveBeenCalledTimes(8);
    });

    it('should never bail out of a flush pass when maxConsecutiveFlushFailures is disabled (0)', async () => {
      let connectedCallback: (() => void) | undefined;

      const flakyTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Broker rejecting everything');
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const fanoutWithBreakerDisabled = new FanoutEngine({
        transport: flakyTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
        maxConsecutiveFlushFailures: 0,
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // 15 sends buffered, well past the default breaker threshold of 10;
      // with the breaker disabled every single one should still be attempted.
      for (let i = 0; i < 15; i++) {
        await fanoutWithBreakerDisabled.send(UserCreatedEvent, event);
      }
      expect(flakyTransport.send).toHaveBeenCalledTimes(15);

      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(flakyTransport.send).toHaveBeenCalledTimes(30);
    });

    it('should not trip the failure breaker on isolated failures interspersed with successes', async () => {
      let connectedCallback: (() => void) | undefined;
      let phase: 'buffering' | 'flushing' = 'buffering';
      let flushCallIndex = 0;

      const onEnqueueSuccess = mock(async () => {});
      const hooksInstance = new SafeHooks({ onEnqueueSuccess });

      const mostlyHealthyTransport: Transport = {
        ...transport,
        send: mock(async () => {
          if (phase === 'buffering') {
            throw new Error('Initially down');
          }
          flushCallIndex++;
          // Every 3rd flushed item fails — isolated, never two in a row.
          if (flushCallIndex % 3 === 0) throw new Error('Occasional nack');
          return 'mock';
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const fanoutWithReconnect = new FanoutEngine({
        transport: mostlyHealthyTransport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
      });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      for (let i = 0; i < 9; i++) {
        await fanoutWithReconnect.send(UserCreatedEvent, event);
      }
      expect(mostlyHealthyTransport.send).toHaveBeenCalledTimes(9);

      // Every 3rd flushed item fails, but never two in a row, so the
      // breaker (threshold 10) never trips and all 9 get attempted.
      phase = 'flushing';
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(mostlyHealthyTransport.send).toHaveBeenCalledTimes(9 + 9);
      expect(onEnqueueSuccess).toHaveBeenCalledTimes(9 - 3);
    });

    it('should drop untried messages that no longer fit once maxRetryBufferSize is hit by concurrent buffering', async () => {
      let connectedCallback: (() => void) | undefined;
      let phase: 'buffering' | 'flushing' = 'buffering';
      let flushCallIndex = 0;
      let triggeredConcurrentBuffering = false;
      let fanoutWithReconnect: FanoutEngine;

      const onEnqueueError = mock(async () => {});
      const hooksInstance = new SafeHooks({ onEnqueueError });

      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      const flakyTransport: Transport = {
        ...transport,
        send: mock(async () => {
          if (phase === 'buffering') {
            throw new Error('Initially down');
          }
          flushCallIndex++;
          // Simulate a concurrent send() buffering more items mid-flush.
          if (flushCallIndex === 5 && !triggeredConcurrentBuffering) {
            triggeredConcurrentBuffering = true;
            for (let i = 0; i < 4; i++) {
              await fanoutWithReconnect.send(UserCreatedEvent, event);
            }
          }
          throw new Error('Still down');
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      fanoutWithReconnect = new FanoutEngine({
        transport: flakyTransport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
        maxRetryBufferSize: 13,
      });

      // 11 sends buffered under the cap of 13 — nothing dropped yet.
      for (let i = 0; i < 11; i++) {
        await fanoutWithReconnect.send(UserCreatedEvent, event);
      }
      expect(flakyTransport.send).toHaveBeenCalledTimes(11);

      // Flush: 10 consecutive failures trip the breaker, leaving the 11th
      // item untried. The 4 concurrently-buffered items (injected mid-pass)
      // fill the buffer to its cap of 13 by the time the streak trips, so
      // both the 10th flush item (existing buffer-full path) and the
      // untried 11th item (rebufferUntried's buffer-full path) get dropped.
      phase = 'flushing';
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));

      // 11 initial + 10 flush-loop attempts + 4 injected.
      expect(flakyTransport.send).toHaveBeenCalledTimes(11 + 10 + 4);
      expect(onEnqueueError).toHaveBeenCalledTimes(2);
    });

    it('should stop the periodic retry timer after dispose()', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Connection lost');
        }),
      };

      const fanoutWithInterval = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks,
        topology: testTopology,
        defaultQueue: 'events',
        retryIntervalMs: 20,
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      await fanoutWithInterval.send(UserCreatedEvent, event);
      expect(failingTransport.send).toHaveBeenCalledTimes(1);

      fanoutWithInterval.dispose();

      await new Promise((resolve) => setTimeout(resolve, 50));

      // No further flush attempts after dispose, despite the buffered message.
      expect(failingTransport.send).toHaveBeenCalledTimes(1);
    });

    it('should drop a message after exceeding maxRetryAttempts instead of retrying forever', async () => {
      const subscriber = createSubscriber({
        name: 'handle-user',
        description: 'Handles user events',
        callback: async () => {},
      });
      schema.register(UserCreatedEvent, [subscriber]);

      let connectedCallback: (() => void) | undefined;
      const failingTransport: Transport = {
        ...transport,
        send: mock(async () => {
          throw new Error('Still down');
        }),
        onConnected: (cb) => {
          connectedCallback = cb;
          return () => {};
        },
      };

      const onEnqueueError = mock(async () => {});
      const hooksInstance = new SafeHooks({
        onEnqueueError,
      } satisfies MatadorHooks);

      const fanoutWithMaxAttempts = new FanoutEngine({
        transport: failingTransport,
        schema,
        hooks: hooksInstance,
        topology: testTopology,
        defaultQueue: 'events',
        maxRetryAttempts: 2,
      });

      const event = new UserCreatedEvent({
        userId: '123',
        email: 'test@example.com',
      });

      // Initial send fails and buffers (attempts: 0).
      await fanoutWithMaxAttempts.send(UserCreatedEvent, event);
      expect(onEnqueueError).not.toHaveBeenCalled();

      // First flush fails: attempts becomes 1, still under the cap, re-buffered.
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(onEnqueueError).not.toHaveBeenCalled();

      // Second flush fails: attempts becomes 2, meets the cap, dropped for good.
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(onEnqueueError).toHaveBeenCalledTimes(1);

      // A further reconnect must not retry it again — it's gone.
      connectedCallback?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(onEnqueueError).toHaveBeenCalledTimes(1);
      // 1 initial + 2 flush attempts = 3 total send() calls; no 4th.
      expect(failingTransport.send).toHaveBeenCalledTimes(3);
    });
  });
});
