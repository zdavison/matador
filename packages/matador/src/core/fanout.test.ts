import { beforeEach, describe, expect, it, mock } from 'bun:test';
import { TransportSendError } from '../errors/index.js';
import { SafeHooks } from '../hooks/index.js';
import type { MatadorHooks } from '../hooks/index.js';
import { SchemaRegistry } from '../schema/index.js';
import { TopologyBuilder } from '../topology/index.js';
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
    it('should capture error when transport.send fails', async () => {
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

      await fanoutWithHooks.send(UserCreatedEvent, event);

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
});
