import type { MessageReceipt } from '../transport/index.js';
import type { Envelope, SubscriberDefinition } from '../types/index.js';

/**
 * Context provided to retry policy for decision making.
 */
export interface RetryContext {
  /** The message envelope */
  readonly envelope: Envelope;

  /** The error that caused the failure */
  readonly error: Error;

  /** The subscriber definition */
  readonly subscriber: SubscriberDefinition;

  /** Message receipt with delivery information */
  readonly receipt: MessageReceipt;
}

/**
 * Decision returned by retry policy.
 */
export type RetryDecision =
  | { readonly action: 'retry'; readonly delay: number }
  | {
      readonly action: 'dead-letter';
      readonly queue: string;
      readonly reason: string;
    }
  | { readonly action: 'discard'; readonly reason: string };

/**
 * Decision for a poisoned message
 */
export type PoisonedMessageDecision =
  | { action: 'not-poisoned' }
  | Extract<RetryDecision, { action: 'dead-letter' | 'discard' }>;

/**
 * Decision a pre-execution check can return
 */
export type PrecheckDecision =
  | { action: 'pass' }
  | Extract<RetryDecision, { action: 'dead-letter' | 'discard' }>;

/**
 * Context provided to retry policy for a pre-execution check, i.e. before the
 * subscriber callback has run and before any error exists.
 */
export interface PrecheckContext {
  /** The message envelope */
  readonly envelope: Envelope;

  /** Message receipt with delivery information */
  readonly receipt: MessageReceipt;
}

/**
 * Interface for retry policies.
 */
export interface RetryPolicy {
  /**
   * Check if the message should be dead-lettered or discarded before the subscriber callback is invoked
   *
   * Return a 'pass' decision to proceed with normal processing, otherwise return a decision to handle the message accordingly (e.g. dead-letter or discard).
   */
  precheck(context: PrecheckContext): PrecheckDecision;

  /**
   * Determines what to do with a failed message.
   */
  shouldRetry(context: RetryContext): RetryDecision;

  /**
   * Calculates the delay for a retry attempt.
   */
  getDelay(context: RetryContext): number;
}
