import { beforeEach, describe, expect, it, mock } from 'bun:test';
import type { TransportFallbackContext } from '../../hooks/index.js';
import type { Topology } from '../../topology/types.js';
import { createEnvelope } from '../../types/index.js';
import { LocalTransport } from '../local/local-transport.js';
import type { Transport } from '../transport.js';
import { MultiTransport } from './multi-transport.js';

describe('MultiTransport', () => {
  let primary: LocalTransport;
  let secondary: LocalTransport;
  let transport: MultiTransport;

  beforeEach(() => {
    primary = new LocalTransport();
    secondary = new LocalTransport();
    transport = new MultiTransport({
      transports: [primary, secondary],
    });
  });

  describe('constructor', () => {
    it('should throw if no transports provided', () => {
      expect(() => new MultiTransport({ transports: [] })).toThrow(
        'At least one transport is required',
      );
    });

    it('should set name based on transport names', () => {
      expect(transport.name).toBe('multi(local,local)');
    });

    it('should use primary transport capabilities', () => {
      expect(transport.capabilities).toBe(primary.capabilities);
    });

    it('should expose primary transport', () => {
      expect(transport.primary).toBe(primary);
    });

    it('should expose all transports', () => {
      expect(transport.transports).toEqual([primary, secondary]);
    });

    it('should default fallbackEnabled to true', () => {
      expect(transport.fallbackEnabled).toBe(true);
    });

    it('should allow disabling fallback', () => {
      const noFallback = new MultiTransport({
        transports: [primary, secondary],
        fallbackEnabled: false,
      });
      expect(noFallback.fallbackEnabled).toBe(false);
    });
  });

  describe('connection', () => {
    it('should start disconnected', () => {
      expect(transport.isConnected()).toBe(false);
    });

    it('should connect all transports', async () => {
      await transport.connect();

      expect(transport.isConnected()).toBe(true);
      expect(primary.isConnected()).toBe(true);
      expect(secondary.isConnected()).toBe(true);
    });

    it('should disconnect all transports', async () => {
      await transport.connect();
      await transport.disconnect();

      expect(transport.isConnected()).toBe(false);
      expect(primary.isConnected()).toBe(false);
      expect(secondary.isConnected()).toBe(false);
    });
  });

  describe('topology', () => {
    it('should apply topology to all transports', async () => {
      const topology: Topology = {
        namespace: 'test',
        queues: [{ name: 'events' }],
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

      await transport.connect();
      await transport.applyTopology(topology);

      // Both transports should have the queue
      expect(primary.getQueueSize('test.events')).toBe(0);
      expect(secondary.getQueueSize('test.events')).toBe(0);
    });
  });

  describe('send - primary success', () => {
    beforeEach(async () => {
      await transport.connect();
    });

    it('should send to primary transport when it succeeds', async () => {
      const envelope = createTestEnvelope();
      await transport.send('test-queue', envelope);

      expect(primary.getQueueSize('test-queue')).toBe(1);
      expect(secondary.getQueueSize('test-queue')).toBe(0);
    });

    it('should not call onEnqueueFallback when primary succeeds', async () => {
      const onEnqueueFallback = mock(() => {});
      const transportWithCallback = new MultiTransport(
        { transports: [primary, secondary] },
        { onEnqueueFallback },
      );
      await transportWithCallback.connect();

      const envelope = createTestEnvelope();
      await transportWithCallback.send('test-queue', envelope);

      expect(onEnqueueFallback).not.toHaveBeenCalled();
    });
  });

  describe('send - fallback', () => {
    beforeEach(async () => {
      await transport.connect();
    });

    it('should fallback when primary fails', async () => {
      // Make primary fail
      await primary.disconnect();

      const envelope = createTestEnvelope();
      await transport.send('test-queue', envelope);

      expect(secondary.getQueueSize('test-queue')).toBe(1);
    });

    it('should call onEnqueueFallback when fallback is used', async () => {
      const fallbackContexts: TransportFallbackContext[] = [];
      const transportWithCallback = new MultiTransport(
        { transports: [primary, secondary] },
        { onEnqueueFallback: (ctx) => fallbackContexts.push(ctx) },
      );
      await transportWithCallback.connect();

      // Make primary fail
      await primary.disconnect();

      const envelope = createTestEnvelope();
      await transportWithCallback.send('test-queue', envelope);

      expect(fallbackContexts).toHaveLength(1);
      const ctx = fallbackContexts[0];
      if (!ctx) throw new Error('expected fallback context');
      expect(ctx.failedTransport).toBe('local');
      expect(ctx.nextTransport).toBe('local');
      expect(ctx.queue).toBe('test-queue');
      expect(ctx.envelope).toBe(envelope);
      expect(ctx.error.message).toContain('is not connected');
    });

    it('should throw when all transports fail', async () => {
      await primary.disconnect();
      await secondary.disconnect();

      const envelope = createTestEnvelope();
      await expect(transport.send('test-queue', envelope)).rejects.toThrow(
        'All transports failed',
      );
    });

    it('should try transports in order', async () => {
      const third = new LocalTransport();
      const multiTransport = new MultiTransport({
        transports: [primary, secondary, third],
      });
      await multiTransport.connect();

      // Make primary and secondary fail
      await primary.disconnect();
      await secondary.disconnect();

      const envelope = createTestEnvelope();
      await multiTransport.send('test-queue', envelope);

      expect(third.getQueueSize('test-queue')).toBe(1);
    });

    it('should not fallback when fallbackEnabled is false', async () => {
      const noFallback = new MultiTransport({
        transports: [primary, secondary],
        fallbackEnabled: false,
      });
      await noFallback.connect();

      // Make primary fail
      await primary.disconnect();

      const envelope = createTestEnvelope();
      await expect(noFallback.send('test-queue', envelope)).rejects.toThrow(
        'is not connected',
      );

      // Secondary should not have received the message
      expect(secondary.getQueueSize('test-queue')).toBe(0);
    });

    it('should send to primary when fallbackEnabled is false and primary succeeds', async () => {
      const noFallback = new MultiTransport({
        transports: [primary, secondary],
        fallbackEnabled: false,
      });
      await noFallback.connect();

      const envelope = createTestEnvelope();
      await noFallback.send('test-queue', envelope);

      expect(primary.getQueueSize('test-queue')).toBe(1);
      expect(secondary.getQueueSize('test-queue')).toBe(0);
    });
  });

  describe('subscribe', () => {
    beforeEach(async () => {
      await transport.connect();
    });

    it('should receive messages from primary transport', async () => {
      const receivedMessages: unknown[] = [];

      await transport.subscribe('test-queue', async (envelope) => {
        receivedMessages.push(envelope);
      });

      // Send directly to primary (simulating normal operation)
      await primary.send('test-queue', createTestEnvelope());

      expect(receivedMessages).toHaveLength(1);
    });

    it('should receive messages that fell back to secondary transport', async () => {
      const receivedMessages: unknown[] = [];

      // Subscribe through MultiTransport
      await transport.subscribe('test-queue', async (envelope) => {
        receivedMessages.push(envelope);
      });

      // Make primary fail for sends
      await primary.disconnect();

      // Send through MultiTransport - should fall back to secondary
      const envelope = createTestEnvelope();
      await transport.send('test-queue', envelope);

      // Message went to secondary transport, subscriber should still receive it
      expect(receivedMessages).toHaveLength(1);
      expect((receivedMessages[0] as { id: string }).id).toBe(envelope.id);
    });

    it('should leave sub-transports subscribed when pauseForShutdown is a no-op', async () => {
      const receivedMessages: unknown[] = [];
      const subscription = await transport.subscribe(
        'test-queue',
        async (envelope) => {
          receivedMessages.push(envelope);
        },
      );

      // Both primary and secondary here are LocalTransport, whose
      // pauseForShutdown is a no-op — pausing the aggregate must leave both
      // underlying subscriptions untouched.
      await subscription.pauseForShutdown?.();

      await primary.send('test-queue', createTestEnvelope());

      expect(receivedMessages).toHaveLength(1);
    });

    it('should call pauseForShutdown on sub-transports that implement it', async () => {
      const externalPauseForShutdown = mock(async () => {});
      const externalMock: Transport = {
        name: 'rabbitmq',
        capabilities: {
          deliveryModes: ['at-least-once'],
          delayedMessages: false,
          deadLetterRouting: 'native',
          attemptTracking: true,
          concurrencyModel: 'prefetch',
          ordering: 'none',
          priorities: false,
        },
        isConnected: () => true,
        connect: mock(async () => {}),
        disconnect: mock(async () => {}),
        send: mock(async () => 'rabbitmq'),
        subscribe: mock(async () => ({
          unsubscribe: mock(async () => {}),
          pauseForShutdown: externalPauseForShutdown,
          isActive: true,
        })),
        applyTopology: mock(async () => {}),
        complete: mock(async () => {}),
      };

      const multiWithExternal = new MultiTransport({
        transports: [externalMock, primary],
      });
      await multiWithExternal.connect();

      const subscription = await multiWithExternal.subscribe(
        'test-queue',
        async () => {},
      );

      await subscription.pauseForShutdown?.();

      expect(externalPauseForShutdown).toHaveBeenCalledTimes(1);
    });

    it('should fall back to unsubscribe for sub-transports without pauseForShutdown', async () => {
      const externalUnsubscribe = mock(async () => {});
      const externalMock: Transport = {
        name: 'rabbitmq',
        capabilities: {
          deliveryModes: ['at-least-once'],
          delayedMessages: false,
          deadLetterRouting: 'native',
          attemptTracking: true,
          concurrencyModel: 'prefetch',
          ordering: 'none',
          priorities: false,
        },
        isConnected: () => true,
        connect: mock(async () => {}),
        disconnect: mock(async () => {}),
        send: mock(async () => 'rabbitmq'),
        subscribe: mock(async () => ({
          unsubscribe: externalUnsubscribe,
          isActive: true,
        })),
        applyTopology: mock(async () => {}),
        complete: mock(async () => {}),
      };

      const multiWithExternal = new MultiTransport({
        transports: [externalMock, primary],
      });
      await multiWithExternal.connect();

      const subscription = await multiWithExternal.subscribe(
        'test-queue',
        async () => {},
      );

      await subscription.pauseForShutdown?.();

      expect(externalUnsubscribe).toHaveBeenCalledTimes(1);
    });
  });

  describe('complete', () => {
    beforeEach(async () => {
      await transport.connect();
    });

    it('should complete using primary transport', async () => {
      await primary.send('test-queue', createTestEnvelope());

      const received = await primary.receiveOne('test-queue');
      if (!received) throw new Error('expected received message');

      await transport.complete(received.receipt);

      expect(primary.getCompleted()).toHaveLength(1);
    });
  });

  describe('sendToDeadLetter', () => {
    beforeEach(async () => {
      await transport.connect();
    });

    it('should send to dead letter using primary transport', async () => {
      const envelope = createTestEnvelope();
      await primary.send('test-queue', envelope);

      const received = await primary.receiveOne('test-queue');
      if (!received) throw new Error('expected received message');

      await transport.sendToDeadLetter(
        received.receipt,
        'dlq',
        envelope,
        'test error',
      );

      expect(primary.getQueueSize('test-queue')).toBe(0);
      expect(primary.getQueueSize('test-queue.dlq')).toBe(1);
    });
  });
});

function createTestEnvelope() {
  return createEnvelope({
    eventKey: 'test.event',
    targetSubscriber: 'test-subscriber',
    data: { test: 'data' },
    importance: 'should-investigate',
  });
}
