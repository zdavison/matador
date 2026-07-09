import { beforeEach, describe, expect, it } from 'bun:test';
import { LocalTransportNoActiveSubscriberError } from '../../errors/index.js';
import type { Topology } from '../../topology/types.js';
import { createEnvelope } from '../../types/index.js';
import { LocalTransport } from './local-transport.js';

describe('LocalTransport', () => {
  let transport: LocalTransport;

  beforeEach(() => {
    transport = new LocalTransport();
  });

  describe('connection', () => {
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
  });

  describe('capabilities', () => {
    it('should report correct capabilities', () => {
      expect(transport.name).toBe('local');
      expect(transport.capabilities.deliveryModes).toContain('at-least-once');
      expect(transport.capabilities.delayedMessages).toBe(true);
      expect(transport.capabilities.deadLetterRouting).toBe('manual');
    });
  });

  describe('send and receive', () => {
    beforeEach(async () => {
      await transport.connect();
      // A no-op subscriber makes 'test-queue' deliverable; LocalTransport now
      // rejects sends to queues with no active subscriber in this process.
      await transport.subscribe('test-queue', async () => {});
    });

    it('should throw when sending while disconnected', async () => {
      await transport.disconnect();
      const envelope = createTestEnvelope();

      await expect(transport.send('test-queue', envelope)).rejects.toThrow(
        'is not connected',
      );
    });

    it('should throw when sending to a queue with no active subscriber', async () => {
      const envelope = createTestEnvelope();

      await expect(
        transport.send('unsubscribed-queue', envelope),
      ).rejects.toThrow(LocalTransportNoActiveSubscriberError);
    });

    it('should send and receive a message', async () => {
      const envelope = createTestEnvelope();
      await transport.send('test-queue', envelope);

      const received = await transport.receiveOne('test-queue');
      expect(received).not.toBeNull();
      expect(received?.envelope.id).toBe(envelope.id);
      expect(received?.envelope.data).toEqual({ test: 'data' });
    });

    it('should track queue size correctly', async () => {
      expect(transport.getQueueSize('test-queue')).toBe(0);

      await transport.send('test-queue', createTestEnvelope());
      expect(transport.getQueueSize('test-queue')).toBe(1);

      await transport.send('test-queue', createTestEnvelope());
      expect(transport.getQueueSize('test-queue')).toBe(2);
    });

    it('should complete messages and remove from queue', async () => {
      const envelope = createTestEnvelope();
      await transport.send('test-queue', envelope);

      const received = await transport.receiveOne('test-queue');
      if (!received) throw new Error('expected received message');

      await transport.complete(received.receipt);

      expect(transport.getQueueSize('test-queue')).toBe(0);
      expect(transport.getCompleted()).toHaveLength(1);
    });

    it('should return null when queue is empty', async () => {
      const received = await transport.receiveOne('empty-queue');
      expect(received).toBeNull();
    });
  });

  describe('subscriptions', () => {
    beforeEach(async () => {
      await transport.connect();
    });

    it('should throw when subscribing while disconnected', async () => {
      await transport.disconnect();

      await expect(
        transport.subscribe('test-queue', async () => {}),
      ).rejects.toThrow('is not connected');
    });

    it('should deliver messages to subscribers', async () => {
      const receivedMessages: unknown[] = [];

      await transport.subscribe('test-queue', async (envelope) => {
        receivedMessages.push(envelope);
      });

      const envelope = createTestEnvelope();
      await transport.send('test-queue', envelope);

      expect(receivedMessages).toHaveLength(1);
      expect((receivedMessages[0] as { id: string }).id).toBe(envelope.id);
    });

    it('should redeliver pending (uncompleted) messages to a newly attached subscriber', async () => {
      const firstSubscriberMessages: unknown[] = [];
      const firstSubscription = await transport.subscribe(
        'test-queue',
        async (envelope) => {
          firstSubscriberMessages.push(envelope);
        },
      );

      const envelope = createTestEnvelope();
      await transport.send('test-queue', envelope);
      expect(firstSubscriberMessages).toHaveLength(1);

      // First subscriber never completed the message before going away.
      await firstSubscription.unsubscribe();

      const secondSubscriberMessages: unknown[] = [];
      await transport.subscribe('test-queue', async (env) => {
        secondSubscriberMessages.push(env);
      });

      expect(secondSubscriberMessages).toHaveLength(1);
    });

    it('should stop delivering after unsubscribe', async () => {
      const receivedMessages: unknown[] = [];

      const subscription = await transport.subscribe(
        'test-queue',
        async (envelope) => {
          receivedMessages.push(envelope);
        },
      );

      await transport.send('test-queue', createTestEnvelope());
      expect(receivedMessages).toHaveLength(1);

      await subscription.unsubscribe();
      expect(subscription.isActive).toBe(false);
    });
  });

  describe('dead letter queue', () => {
    beforeEach(async () => {
      await transport.connect();
      await transport.subscribe('test-queue', async () => {});
    });

    it('should send messages to dead letter queue', async () => {
      const envelope = createTestEnvelope();
      await transport.send('test-queue', envelope);

      const received = await transport.receiveOne('test-queue');
      if (!received) throw new Error('expected received message');

      await transport.sendToDeadLetter(
        received.receipt,
        'dlq',
        envelope,
        'test error',
      );

      expect(transport.getQueueSize('test-queue')).toBe(0);
      expect(transport.getQueueSize('test-queue.dlq')).toBe(1);
    });
  });

  describe('topology', () => {
    it('should apply topology and create queues', async () => {
      const topology: Topology = {
        namespace: 'test',
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

      await transport.connect();
      await transport.applyTopology(topology);

      // Queues should be created
      expect(transport.getQueueSize('test.events')).toBe(0);
      expect(transport.getQueueSize('test.notifications')).toBe(0);
    });
  });

  describe('clear', () => {
    it('should reset all state', async () => {
      await transport.connect();
      await transport.subscribe('test-queue', async () => {});
      await transport.send('test-queue', createTestEnvelope());

      const received = await transport.receiveOne('test-queue');
      if (!received) throw new Error('expected received message');
      await transport.complete(received.receipt);

      transport.clear();

      expect(transport.getQueueSize('test-queue')).toBe(0);
      expect(transport.getCompleted()).toHaveLength(0);
    });
  });

  describe('delayed messages', () => {
    beforeEach(async () => {
      await transport.connect();
      await transport.subscribe('test-queue', async () => {});
    });

    it('should return immediately and delay message delivery', async () => {
      const envelope = createTestEnvelope();

      // Send returns immediately (non-blocking)
      await transport.send('test-queue', envelope, {
        delay: 100,
      });

      // Message should not be in queue immediately
      expect(transport.getQueueSize('test-queue')).toBe(0);

      // Wait for the delay to pass
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Message should now be in queue
      expect(transport.getQueueSize('test-queue')).toBe(1);
    });

    it('should cancel delayed messages on disconnect', async () => {
      const envelope = createTestEnvelope();

      await transport.send('test-queue', envelope, {
        delay: 100,
      });

      // Disconnect before delay expires
      await transport.disconnect();

      // Wait past the original delay
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Message should never have been delivered
      expect(transport.getQueueSize('test-queue')).toBe(0);
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
