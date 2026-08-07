import { describe, expect, it } from 'bun:test';
import {
  DoRetry,
  DontRetry,
  EventAssertionError,
} from '../errors/retry-errors.js';
import type { MessageReceipt } from '../transport/transport.js';
import { createEnvelope } from '../types/envelope.js';
import type { SubscriberDefinition } from '../types/subscriber.js';
import type { RetryContext, RetryDecision } from './policy.js';
import { StandardRetryPolicy } from './standard-policy.js';

describe('StandardRetryPolicy', () => {
  describe('shouldRetry', () => {
    it('should dead-letter on EventAssertionError', () => {
      const policy = new StandardRetryPolicy();
      const envelope = createEnvelope({
        eventKey: 'test.event',
        targetSubscriber: 'test-subscriber',
        data: { test: 'data' },
        importance: 'should-investigate',
      });
      const context = createContext(
        new EventAssertionError(envelope, 'Invalid event'),
      );

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('dead-letter');
      assertDeadLetter(decision);
      expect(decision.queue).toBe('undeliverable');
      expect(decision.reason).toContain('assertion error');
    });

    it('should dead-letter on DontRetry error', () => {
      const policy = new StandardRetryPolicy();
      const context = createContext(new DontRetry('Business rule violation'));

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('dead-letter');
      assertDeadLetter(decision);
      expect(decision.queue).toBe('undeliverable');
      expect(decision.reason).toContain('Business rule violation');
    });

    it('should retry on DoRetry error if under max attempts', () => {
      const policy = new StandardRetryPolicy({ maxAttempts: 3 });
      const context = createContext(new DoRetry('Temporary failure'), {
        attemptNumber: 1,
      });

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('retry');
      assertRetry(decision);
      expect(decision.delay).toBeGreaterThan(0);
    });

    it('should dead-letter on DoRetry error if max attempts exceeded', () => {
      const policy = new StandardRetryPolicy({ maxAttempts: 3 });
      const context = createContext(new DoRetry('Temporary failure'), {
        attemptNumber: 3,
      });

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('dead-letter');
      assertDeadLetter(decision);
      expect(decision.reason).toContain('max attempts exceeded');
    });

    it('should dead-letter when max attempts exceeded', () => {
      const policy = new StandardRetryPolicy({ maxAttempts: 3 });
      const context = createContext(new Error('Generic error'), {
        attemptNumber: 3,
      });

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('dead-letter');
      assertDeadLetter(decision);
      expect(decision.queue).toBe('undeliverable');
      expect(decision.reason).toContain('max attempts exceeded (3)');
    });

    it('should dead-letter non-idempotent subscriber on redelivery', () => {
      const policy = new StandardRetryPolicy();
      const context = createContext(
        new Error('Some error'),
        { attemptNumber: 1, redelivered: true },
        { idempotent: 'no' },
      );

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('dead-letter');
      assertDeadLetter(decision);
      expect(decision.reason).toContain('Non-idempotent subscriber');
    });

    it('should retry idempotent subscriber on redelivery', () => {
      const policy = new StandardRetryPolicy();
      const context = createContext(
        new Error('Some error'),
        { attemptNumber: 1, redelivered: true },
        { idempotent: 'yes' },
      );

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('retry');
    });

    it('should dead-letter unknown idempotency subscriber on redelivery (unknown == no)', () => {
      const policy = new StandardRetryPolicy();
      const context = createContext(
        new Error('Some error'),
        { attemptNumber: 1, redelivered: true },
        { idempotent: 'unknown' },
      );

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('dead-letter');
      assertDeadLetter(decision);
      expect(decision.reason).toContain('Non-idempotent subscriber');
    });

    it('should retry resumable subscriber on redelivery', () => {
      const policy = new StandardRetryPolicy();
      const context = createContext(
        new Error('Some error'),
        { attemptNumber: 1, redelivered: true },
        { idempotent: 'resumable' },
      );

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('retry');
    });

    it('should retry generic errors with backoff', () => {
      const policy = new StandardRetryPolicy({ maxAttempts: 5 });
      const context = createContext(new Error('Generic error'), {
        attemptNumber: 1,
      });

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('retry');
      assertRetry(decision);
      expect(decision.delay).toBeDefined();
    });

    it('should dead-letter as poisoned when delivery count reaches the threshold', () => {
      const policy = new StandardRetryPolicy({ maxDeliveries: 5 });
      const context = createContext(new Error('Generic error'), {
        attemptNumber: 1,
        deliveryCount: 5,
      });

      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('dead-letter');
      assertDeadLetter(decision);
      expect(decision.queue).toBe('undeliverable');
      expect(decision.reason).toContain('Possible poison message');
    });

    it('should take priority over other rules once poisoned', () => {
      // Even a DontRetry-worthy error should still report as poisoned once
      // delivery count is at/above the threshold, since poison detection
      // runs first.
      const policy = new StandardRetryPolicy({ maxDeliveries: 5 });
      const context = createContext(new DoRetry('Temporary failure'), {
        attemptNumber: 1,
        deliveryCount: 5,
      });

      const decision = policy.shouldRetry(context);

      assertDeadLetter(decision);
      expect(decision.reason).toContain('Possible poison message');
    });
  });

  describe('shouldProcess', () => {
    it('should return pass when delivery count is under the threshold', () => {
      const policy = new StandardRetryPolicy({ maxDeliveries: 5 });
      const { envelope, receipt } = createProcessContext({
        deliveryCount: 4,
      });

      const decision = policy.shouldProcess({ envelope, receipt });

      expect(decision.action).toBe('process');
    });

    it('should dead-letter when delivery count is at/above the threshold, without an error', () => {
      const policy = new StandardRetryPolicy({ maxDeliveries: 5 });
      const { envelope, receipt } = createProcessContext({
        deliveryCount: 5,
      });

      const decision = policy.shouldProcess({ envelope, receipt });

      expect(decision.action).toBe('dead-letter');
      if (decision.action !== 'dead-letter') {
        throw new Error(`Expected dead-letter action, got ${decision.action}`);
      }
      expect(decision.queue).toBe('undeliverable');
      expect(decision.reason).toContain('Possible poison message');
    });

    it('should agree with shouldRetry on the same poisoned receipt', () => {
      // shouldProcess and shouldRetry share the same poison-detection logic, so
      // a message flagged by one must be flagged the same way by the other.
      const policy = new StandardRetryPolicy({ maxDeliveries: 5 });
      const receiptOverrides = { attemptNumber: 1, deliveryCount: 5 };
      const context = createContext(new Error('Some error'), receiptOverrides);

      const processDecision = policy.shouldProcess({
        envelope: context.envelope,
        receipt: context.receipt,
      });
      const shouldRetryDecision = policy.shouldRetry(context);

      expect(processDecision as RetryDecision).toEqual(shouldRetryDecision);
    });
  });

  describe('getDelay', () => {
    it('should calculate exponential backoff', () => {
      const policy = new StandardRetryPolicy({
        baseDelay: 1000,
        backoffMultiplier: 2,
      });

      const delay1 = policy.getDelay(
        createContext(new Error(), { attemptNumber: 1 }),
      );
      const delay2 = policy.getDelay(
        createContext(new Error(), { attemptNumber: 2 }),
      );
      const delay3 = policy.getDelay(
        createContext(new Error(), { attemptNumber: 3 }),
      );

      expect(delay1).toBe(1000); // 1000 * 2^0
      expect(delay2).toBe(2000); // 1000 * 2^1
      expect(delay3).toBe(4000); // 1000 * 2^2
    });

    it('should cap delay at maxDelay', () => {
      const policy = new StandardRetryPolicy({
        baseDelay: 1000,
        backoffMultiplier: 10,
        maxDelay: 5000,
      });

      const delay = policy.getDelay(
        createContext(new Error(), { attemptNumber: 5 }),
      );

      expect(delay).toBe(5000);
    });
  });

  describe('configuration', () => {
    it('should use default configuration', () => {
      const policy = new StandardRetryPolicy();
      const context = createContext(new Error(), { attemptNumber: 1 });

      const decision = policy.shouldRetry(context);

      // Default maxAttempts is 3, so should retry on attempt 1
      expect(decision.action).toBe('retry');
    });

    it('should accept custom configuration', () => {
      const policy = new StandardRetryPolicy({
        maxAttempts: 1,
        baseDelay: 500,
        maxDelay: 1000,
        backoffMultiplier: 1.5,
      });

      // With maxAttempts: 1, first attempt should dead-letter
      const context = createContext(new Error(), { attemptNumber: 1 });
      const decision = policy.shouldRetry(context);

      expect(decision.action).toBe('dead-letter');
    });

    it('should merge partial configuration with defaults', () => {
      const policy = new StandardRetryPolicy({ maxAttempts: 10 });
      // Use attemptNumber: 5 but deliveryCount: 3 to avoid poison detection
      const context = createContext(new Error(), {
        attemptNumber: 5,
        deliveryCount: 3,
      });

      // Should still retry because we increased maxAttempts
      const decision = policy.shouldRetry(context);
      expect(decision.action).toBe('retry');
    });
  });
});

function assertDeadLetter(decision: RetryDecision): asserts decision is {
  action: 'dead-letter';
  queue: string;
  reason: string;
} {
  if (decision.action !== 'dead-letter') {
    throw new Error(`Expected dead-letter action, got ${decision.action}`);
  }
}

function assertRetry(
  decision: RetryDecision,
): asserts decision is { action: 'retry'; delay: number } {
  if (decision.action !== 'retry') {
    throw new Error(`Expected retry action, got ${decision.action}`);
  }
}

function createContext(
  error: Error,
  receiptOverrides: Partial<MessageReceipt> = {},
  subscriberOverrides: Partial<SubscriberDefinition> = {},
): RetryContext {
  const receipt: MessageReceipt = {
    handle: {},
    redelivered: false,
    attemptNumber: 1,
    deliveryCount:
      receiptOverrides.deliveryCount ?? receiptOverrides.attemptNumber ?? 1,
    sourceQueue: 'test-queue',
    sourceTransport: 'mock',
    ...receiptOverrides,
  };

  const subscriber: SubscriberDefinition = {
    name: 'test-subscriber',
    description: 'Test subscriber',
    idempotent: 'yes',
    importance: 'should-investigate',
    ...subscriberOverrides,
  };

  const envelope = createEnvelope({
    eventKey: 'test.event',
    targetSubscriber: 'test-subscriber',
    data: { test: 'data' },
    importance: 'should-investigate',
  });

  return {
    envelope,
    error,
    receipt,
    subscriber,
  };
}

function createProcessContext(receiptOverrides: Partial<MessageReceipt> = {}): {
  envelope: RetryContext['envelope'];
  receipt: MessageReceipt;
} {
  const { envelope, receipt } = createContext(
    new Error('unused'),
    receiptOverrides,
  );
  return { envelope, receipt };
}
