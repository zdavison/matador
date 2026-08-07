import {
  IdempotentMessageCannotRetryError,
  MessageMaybePoisonedError,
  isAssertionError,
  isDoRetry,
  isDontRetry,
} from '../errors/index.js';
import type { MessageReceipt } from '../transport/index.js';
import type { Envelope } from '../types/index.js';
import type {
  PrecheckContext,
  PrecheckDecision,
  RetryContext,
  RetryDecision,
  RetryPolicy,
} from './policy.js';

/**
 * Configuration for the standard retry policy.
 */
export interface StandardRetryPolicyConfig {
  /** Maximum number of attempts before dead-lettering */
  readonly maxAttempts: number;

  /** Base delay between retries in milliseconds */
  readonly baseDelay: number;

  /** Maximum delay between retries in milliseconds */
  readonly maxDelay: number;

  /** Multiplier for exponential backoff */
  readonly backoffMultiplier: number;

  /**
   * Maximum native delivery count before considering message poisoned.
   * This prevents crash loops from messages that crash the worker.
   * Poison messages are sent directly to the dead-letter queue.
   * Default: 5
   */
  readonly maxDeliveries: number;
}

/**
 * Default configuration values.
 */
export const defaultRetryConfig: StandardRetryPolicyConfig = {
  maxAttempts: 3,
  baseDelay: 1000,
  maxDelay: 300000, // 5 minutes
  backoffMultiplier: 2,
  maxDeliveries: 5,
};

/**
 * Standard retry policy implementing Matador v1 behavior.
 *
 * Decision logic (in priority order):
 * 1. Poison message → dead-letter (prevent crash loops)
 * 2. EventAssertionError → dead-letter (never retry)
 * 3. DontRetry → dead-letter (explicit no-retry)
 * 4. DoRetry → retry if under max attempts
 * 5. Max attempts exceeded → dead-letter
 * 6. Non-idempotent subscriber on redelivery → dead-letter
 * 7. Default → retry with exponential backoff
 */
export class StandardRetryPolicy implements RetryPolicy {
  private readonly config: StandardRetryPolicyConfig;

  constructor(config: Partial<StandardRetryPolicyConfig> = {}) {
    this.config = { ...defaultRetryConfig, ...config };
  }

  /**
   * Check if the message should be dead-lettered or discarded before the subscriber callback is invoked
   *
   * Uses the same delivery-count threshold as `shouldRetry`'s
   * poison check, so an already-poisoned message is dead-lettered without ever running the callback again.
   *
   * @param context - The precheck context.
   * @returns A 'pass' decision if the message is not poisoned, a poison decision otherwise.
   */
  precheck(context: PrecheckContext): PrecheckDecision {
    return this.checkPoisoned(context.envelope, context.receipt);
  }

  shouldRetry(context: RetryContext): RetryDecision {
    const { envelope, error, subscriber, receipt } = context;
    const errorMessage = error.message;

    // 1. Poison message detection - prevent crash loops
    const poisonDecision = this.checkPoisoned(envelope, receipt);

    // If the message is poisoned, return the poison decision
    if (poisonDecision.action !== 'pass') {
      return poisonDecision;
    }

    // 2. Assertion errors never retry
    if (isAssertionError(error)) {
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: `assertion error: ${errorMessage}`,
      };
    }

    // 3. Explicit no-retry
    if (isDontRetry(error)) {
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: errorMessage,
      };
    }

    // 4. Explicit retry request
    if (isDoRetry(error)) {
      if (receipt.attemptNumber >= this.config.maxAttempts) {
        return {
          action: 'dead-letter',
          queue: 'undeliverable',
          reason: `max attempts exceeded (${this.config.maxAttempts}) with forced retry`,
        };
      }
      return {
        action: 'retry',
        delay: this.getDelay(context),
      };
    }

    // 5. Max attempts exceeded
    if (receipt.attemptNumber >= this.config.maxAttempts) {
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: `max attempts exceeded (${this.config.maxAttempts})`,
      };
    }

    // 6. Non-idempotent subscriber on redelivery
    // 'no' and 'unknown' are treated the same (safer default)
    if (
      receipt.redelivered &&
      (subscriber.idempotent === 'no' || subscriber.idempotent === 'unknown')
    ) {
      const idempotentError = new IdempotentMessageCannotRetryError(
        envelope.id,
        subscriber.name,
      );
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: idempotentError.message,
      };
    }

    // 7. Default: retry with backoff
    return {
      action: 'retry',
      delay: this.getDelay(context),
    };
  }

  getDelay(context: RetryContext): number {
    const attempt = context.receipt.attemptNumber;
    const delay =
      this.config.baseDelay * this.config.backoffMultiplier ** (attempt - 1);
    return Math.min(delay, this.config.maxDelay);
  }

  /**
   * Checks the native delivery count against the poison threshold
   *
   * @param envelope - The message envelope.
   * @param receipt - The message receipt.
   * @returns A dead-letter decision if the message is poisoned, a pass decision otherwise.
   */
  private checkPoisoned(
    envelope: Envelope,
    receipt: MessageReceipt,
  ): PrecheckDecision {
    if (receipt.deliveryCount >= this.config.maxDeliveries) {
      const poisonError = new MessageMaybePoisonedError(
        envelope.id,
        receipt.deliveryCount,
        this.config.maxDeliveries,
      );
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: poisonError.message,
      };
    }
    return { action: 'pass' };
  }
}
