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
 * Decision a pre-execution check can return. Narrower than `RetryDecision`:
 * with no error yet and the callback not yet run, a precheck can only skip
 * execution outright (dead-letter or discard) — never schedule a delayed
 * first attempt.
 */
export type PrecheckDecision = Extract<
  RetryDecision,
  { action: 'dead-letter' | 'discard' }
>;

/**
 * Interface for retry policies.
 */
export interface RetryPolicy {
  /**
   * Determines what to do with a failed message.
   */
  shouldRetry(context: RetryContext): RetryDecision;

  /**
   * Calculates the delay for a retry attempt.
   */
  getDelay(context: RetryContext): number;

  /**
   * Optional pre-execution check
   * It runs before the subscriber callback is invoked, based on delivery metadata alone.
   * It is used to determine if the message is poisoned and should be dead-lettered or discarded.
   *
   * Return `undefined` to proceed with normal processing, otherwise return a decision to handle the message accordingly.
   */
  precheck?(context: PrecheckContext): PrecheckDecision | undefined;
}
