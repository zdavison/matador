import {
  IdempotentMessageCannotRetryError,
  MessageMaybePoisonedError,
  isAssertionError,
  isDoRetry,
  isDontRetry,
} from '../errors/index.js';
import type { MessageReceipt } from '../transport/index.js';
import type { Envelope, SubscriberDefinition } from '../types/index.js';
import type {
  ProcessContext,
  ProcessDecision,
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
 * 6. Non-idempotent subscriber → dead-letter
 * 7. Default → retry with exponential backoff
 */
export class StandardRetryPolicy implements RetryPolicy {
  private readonly config: StandardRetryPolicyConfig;

  constructor(config: Partial<StandardRetryPolicyConfig> = {}) {
    this.config = { ...defaultRetryConfig, ...config };
  }

  /**
   * Check if the message should be dead-lettered before the subscriber callback is invoked.
   * This is needed when a subscriber crashes and the `shouldRetry` policy is never executed.
   *
   * Uses the same delivery-count threshold as `shouldRetry` poison check, so an
   * already-poisoned message is dead-lettered without ever running the callback again.
   *
   * Checks the subscriber idempotency setting to prevent processing when a previous attempt crashed.
   *
   * @param context - The shouldProcess context.
   * @returns A 'process' decision if subscriber processing should be attempted; a dead-letter decision otherwise.
   */
  shouldProcess(context: ProcessContext): ProcessDecision {
    // Poison message detection - prevent crash loops
    const poisonError = this.checkPoisoned(context.envelope, context.receipt);

    // If the message is poisoned, dead-letter it
    if (poisonError) {
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: poisonError.message,
      };
    }

    // Non-idempotent subscriber on redelivery
    const idempotencyError = this.checkIdempotency(
      context.envelope,
      context.receipt,
      context.subscriber,
    );

    if (idempotencyError) {
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: idempotencyError.message,
      };
    }

    // Allow processing to continue
    return { action: 'process' };
  }

  shouldRetry(context: RetryContext): RetryDecision {
    const { envelope, error, subscriber, receipt } = context;
    const errorMessage = error.message;

    // 1. Poison message detection - prevent crash loops
    const poisonError = this.checkPoisoned(envelope, receipt);

    // If the message is poisoned, dead-letter it
    if (poisonError) {
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: poisonError.message,
      };
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

    // 6. Non-idempotent subscriber
    const idempotencyError = this.checkIdempotency(
      envelope,
      receipt,
      subscriber,
      error,
    );

    if (idempotencyError) {
      return {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: idempotencyError.message,
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
   * @returns A MessageMaybePoisonedError if the message is poisoned; null otherwise
   */
  private checkPoisoned(
    envelope: Envelope,
    receipt: MessageReceipt,
  ): MessageMaybePoisonedError | null {
    if (receipt.deliveryCount >= this.config.maxDeliveries) {
      return new MessageMaybePoisonedError(
        envelope.id,
        receipt.deliveryCount,
        this.config.maxDeliveries,
      );
    }
    return null;
  }

  /**
   * Checks whether the subscriber allows retries based on its idempotency setting
   *
   * @param envelope - The message envelope.
   * @param receipt - The message receipt.
   * @param subscriber - The subscriber definition
   * @param error - The processing error, if it exists
   * @returns A IdempotentMessageCannotRetryError if the message is poisoned; null otherwise
   */
  private checkIdempotency(
    envelope: Envelope,
    receipt: MessageReceipt,
    subscriber: SubscriberDefinition,
    error?: Error,
  ): IdempotentMessageCannotRetryError | null {
    // 'no' and 'unknown' are treated the same (safer default)
    if (
      (error || receipt.redelivered) &&
      (subscriber.idempotent === 'no' || subscriber.idempotent === 'unknown')
    ) {
      return new IdempotentMessageCannotRetryError(
        envelope.id,
        subscriber.name,
      );
    }
    return null;
  }
}
