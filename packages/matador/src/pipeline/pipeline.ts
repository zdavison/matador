import type { Checkpoint, CheckpointStore } from '../checkpoint/index.js';
import { NoOpCheckpointStore, ResumableContext } from '../checkpoint/index.js';
import type { Codec } from '../codec/index.js';
import { CodecDecodeError } from '../codec/index.js';
import {
  SubscriberIsStubError,
  SubscriberNotRegisteredError,
} from '../errors/index.js';
import type { SafeHooks } from '../hooks/index.js';
import type { RetryDecision, RetryPolicy } from '../retry/index.js';
import type { SchemaRegistry } from '../schema/index.js';
import type { MessageReceipt, Transport } from '../transport/index.js';
import type {
  CallbackContext,
  Dispatcher,
  Envelope,
  MatadorEvent,
  ResumableCallbackContext,
  Subscriber,
  SubscriberDefinition,
} from '../types/index.js';
import { isResumableSubscriber } from '../types/index.js';

/**
 * Configuration for the processing pipeline.
 */
export interface PipelineConfig {
  readonly transport: Transport;
  readonly schema: SchemaRegistry;
  readonly codec: Codec;
  readonly retryPolicy: RetryPolicy;
  readonly hooks: SafeHooks;
  /** Optional checkpoint store for resumable subscribers */
  readonly checkpointStore?: CheckpointStore | undefined;
  /** Dispatcher (matador) for sending events from subscriber callbacks */
  readonly dispatcher: Dispatcher;
}

/**
 * Result of pipeline processing.
 */
export interface ProcessResult {
  readonly success: boolean;
  readonly envelope?: Envelope | undefined;
  readonly subscriber?: SubscriberDefinition | undefined;
  readonly error?: Error | undefined;
  readonly decision?: RetryDecision | undefined;
  readonly durationMs: number;
}

/**
 * Processing pipeline for incoming messages.
 *
 * Handles the complete message lifecycle:
 * 1. Decode envelope from raw bytes
 * 2. Lookup subscriber from schema
 * 3. Pre-execution poison check (retry policy, before running the callback)
 * 4. Execute subscriber callback with hooks
 * 5. Handle success
 * 6. Handle failure with retry policy
 */
export class ProcessingPipeline {
  private readonly transport: Transport;
  private readonly schema: SchemaRegistry;
  private readonly codec: Codec;
  private readonly retryPolicy: RetryPolicy;
  private readonly hooks: SafeHooks;
  private readonly checkpointStore: CheckpointStore;
  private readonly dispatcher: Dispatcher;

  constructor(config: PipelineConfig) {
    this.transport = config.transport;
    this.schema = config.schema;
    this.codec = config.codec;
    this.retryPolicy = config.retryPolicy;
    this.hooks = config.hooks;
    this.checkpointStore = config.checkpointStore ?? new NoOpCheckpointStore();
    this.dispatcher = config.dispatcher;
  }

  /**
   * Processes a raw message from the transport.
   */
  async process(
    rawMessage: Uint8Array,
    receipt: MessageReceipt,
  ): Promise<ProcessResult> {
    const startTime = performance.now();

    // 1. Decode envelope
    let envelope: Envelope;
    try {
      envelope = this.codec.decode(rawMessage);
    } catch (error) {
      const decodeError =
        error instanceof CodecDecodeError
          ? error
          : new CodecDecodeError('Unknown decode error', error);

      await this.transport.complete(receipt);
      await this.hooks.onDecodeError({
        error: decodeError,
        rawMessage,
        sourceQueue: receipt.sourceQueue,
        transport: receipt.sourceTransport,
      });

      return {
        success: false,
        error: decodeError,
        durationMs: performance.now() - startTime,
      };
    }

    // 2. Lookup subscriber from schema
    const subscriberDef = this.schema.getSubscriberDefinition(
      envelope.docket.eventKey,
      envelope.docket.targetSubscriber,
    );

    if (!subscriberDef) {
      const error = new SubscriberNotRegisteredError(
        envelope.docket.targetSubscriber,
        envelope.docket.eventKey,
      );

      // Retry unhandled messages (likely deployment timing issue)
      const decision = this.getUnhandledRetryDecision(envelope, receipt, error);
      await this.handleRetryDecision(receipt, envelope, decision);

      return {
        success: false,
        envelope,
        error,
        decision,
        durationMs: performance.now() - startTime,
      };
    }

    // Get executable subscriber
    const subscriber = this.schema.getExecutableSubscriber(
      envelope.docket.eventKey,
      envelope.docket.targetSubscriber,
    );

    if (!subscriber) {
      // Subscriber is a stub (remote implementation)
      const error = new SubscriberIsStubError(envelope.docket.targetSubscriber);

      // Retry stub messages (likely deployment timing issue)
      const decision = this.getUnhandledRetryDecision(envelope, receipt, error);
      await this.handleRetryDecision(receipt, envelope, decision);

      return {
        success: false,
        envelope,
        subscriber: subscriberDef,
        error,
        decision,
        durationMs: performance.now() - startTime,
      };
    }

    // 3. Pre-execution poison check, based on delivery metadata alone.
    //
    // Catches a message that's already known-poisoned (e.g. delivery count at/above the threshold)
    // before running the callback
    const precheckResult = await this.runPrecheck(
      envelope,
      subscriberDef,
      subscriber,
      receipt,
      startTime,
    );
    if (precheckResult) {
      return precheckResult;
    }

    // 4. Execute subscriber callback with hooks
    let result: unknown;
    let error: Error | undefined;
    let context: ResumableContext | undefined;

    // For resumable subscribers, load existing checkpoint and create context
    const isResumable = isResumableSubscriber(subscriber);
    let existingCheckpoint: Checkpoint | undefined;

    if (isResumable) {
      existingCheckpoint = await this.checkpointStore.get(envelope.id);

      if (existingCheckpoint) {
        await this.hooks.onCheckpointLoaded?.({
          envelope,
          subscriber: subscriberDef,
          checkpoint: existingCheckpoint,
          cachedSteps: Object.keys(existingCheckpoint.completedSteps).length,
        });
      }

      context = new ResumableContext({
        store: this.checkpointStore,
        envelope,
        subscriber: subscriberDef,
        existingCheckpoint,
        hooks: {
          onCheckpointHit: (ctx) => this.hooks.onCheckpointHit?.(ctx),
          onCheckpointMiss: (ctx) => this.hooks.onCheckpointMiss?.(ctx),
        },
      });
    }

    // Create base callback context with matador dispatcher
    const callbackContext: CallbackContext = { matador: this.dispatcher };

    await this.hooks.onWorkerWrap(envelope, subscriberDef, async () => {
      await this.hooks.onWorkerBeforeProcess(envelope, subscriberDef);

      try {
        if (isResumable && context) {
          // Resumable subscriber: pass combined context (checkpoint + matador)
          // Cast needed because TypeScript can't narrow the union type based on isResumable
          const resumableCallback = subscriber.callback as (
            envelope: Envelope,
            context: ResumableCallbackContext,
          ) => Promise<void> | void;
          // Create combined context by binding class methods and adding matador
          const fullContext: ResumableCallbackContext = {
            io: context.io.bind(context),
            all: context.all.bind(context),
            attempt: context.attempt,
            isRetry: context.isRetry,
            matador: this.dispatcher,
          };
          result = await resumableCallback(envelope, fullContext);
        } else {
          // Standard subscriber: pass envelope and callback context
          const standardCallback = subscriber.callback as (
            envelope: Envelope,
            context: CallbackContext,
          ) => Promise<void> | void;
          result = await standardCallback(envelope, callbackContext);
        }
      } catch (e) {
        error = e instanceof Error ? e : new Error(String(e));
      }
    });

    const durationMs = performance.now() - startTime;

    // 5. Handle success
    if (!error) {
      // Clear checkpoint on success for resumable subscribers
      if (context) {
        await context.clear();
        await this.hooks.onCheckpointCleared?.({
          envelope,
          subscriber: subscriberDef,
          reason: 'success',
        });
      }

      await this.transport.complete(receipt);
      await this.hooks.onWorkerSuccess({
        envelope,
        subscriber: subscriberDef,
        result,
        durationMs,
        transport: receipt.sourceTransport,
      });

      return {
        success: true,
        envelope,
        subscriber: subscriberDef,
        durationMs,
      };
    }

    // 6. Handle failure - consult retry policy
    const decision = this.retryPolicy.shouldRetry({
      envelope,
      error,
      subscriber: subscriberDef,
      receipt,
    });

    // Update envelope with error info
    envelope.docket.lastError = error.message;
    envelope.docket.firstError ??= error.message;

    // Clear checkpoint on dead-letter (terminal state)
    if (decision.action === 'dead-letter' && context) {
      await context.clear();
      await this.hooks.onCheckpointCleared?.({
        envelope,
        subscriber: subscriberDef,
        reason: 'dead-letter',
      });
    }

    await this.handleRetryDecision(receipt, envelope, decision);

    await this.hooks.onWorkerError({
      envelope,
      subscriber: subscriberDef,
      error,
      durationMs,
      decision,
      transport: receipt.sourceTransport,
    });

    return {
      success: false,
      envelope,
      subscriber: subscriberDef,
      error,
      decision,
      durationMs,
    };
  }

  /**
   * Runs the retry policy's optional pre-execution poison check.
   * @param envelope - The message envelope
   * @param subscriberDef - The subscriber definition
   * @param subscriber - The subscriber
   * @param receipt - The message receipt
   * @param startTime - The start time of the processing
   * @returns A terminal `ProcessResult` if the message should be skipped (dead-lettered/discarded) without ever invoking the callback, or `undefined` to proceed with normal processing.
   */
  private async runPrecheck(
    envelope: Envelope,
    subscriberDef: SubscriberDefinition,
    subscriber: Subscriber<MatadorEvent<unknown>>,
    receipt: MessageReceipt,
    startTime: number,
  ): Promise<ProcessResult | undefined> {
    if (!this.retryPolicy.precheck) {
      return undefined;
    }

    const decision = this.retryPolicy.precheck({ envelope, receipt });

    // If no decision, the message is not poisoned and we can proceed with normal processing
   if (!decision) {
      return undefined;
    }

    // If a decision is returned, the message is poisoned and we need to handle it accordingly

    const error = new Error(decision.reason);

    envelope.docket.lastError = error.message;
    envelope.docket.firstError ??= error.message;

    // Clear any checkpoint left over from a prior (crashed) attempt
    if (
      decision.action === 'dead-letter' &&
      isResumableSubscriber(subscriber)
    ) {
      await this.checkpointStore.delete(envelope.id);
      await this.hooks.onCheckpointCleared?.({
        envelope,
        subscriber: subscriberDef,
        reason: 'dead-letter',
      });
    }

    await this.handleRetryDecision(receipt, envelope, decision);

    const durationMs = performance.now() - startTime;

    await this.hooks.onWorkerError({
      envelope,
      subscriber: subscriberDef,
      error,
      durationMs,
      decision,
      transport: receipt.sourceTransport,
    });

    return {
      success: false,
      envelope,
      subscriber: subscriberDef,
      error,
      decision,
      durationMs,
    };
  }

  private async handleRetryDecision(
    receipt: MessageReceipt,
    envelope: Envelope,
    decision: RetryDecision,
  ): Promise<void> {
    switch (decision.action) {
      case 'retry': {
        // Increment attempts and schedule retry
        envelope.docket.attempts++;
        envelope.docket.scheduledFor = new Date(
          Date.now() + decision.delay,
        ).toISOString();

        await this.transport.send(receipt.sourceQueue, envelope, {
          delay: decision.delay,
        });
        await this.transport.complete(receipt);
        break;
      }

      case 'dead-letter': {
        await this.sendToDeadLetter(
          receipt,
          envelope,
          decision.queue,
          decision.reason,
        );
        break;
      }

      case 'discard': {
        await this.transport.complete(receipt);
        break;
      }
    }
  }

  private async sendToDeadLetter(
    receipt: MessageReceipt,
    envelope: Envelope,
    dlqName: string,
    reason: string,
  ): Promise<void> {
    envelope.docket.originalQueue ??= receipt.sourceQueue;

    if (this.transport.sendToDeadLetter) {
      await this.transport.sendToDeadLetter(receipt, dlqName, envelope, reason);
    } else {
      // Manual: send to DLQ then complete original
      const fullDlqName = `${receipt.sourceQueue}.${dlqName}`;
      await this.transport.send(fullDlqName, envelope);
      await this.transport.complete(receipt);
    }
  }

  /**
   * Gets retry decision for unhandled messages (subscriber not found).
   * Uses same retry policy logic but sends to 'unhandled' DLQ after max attempts.
   */
  private getUnhandledRetryDecision(
    envelope: Envelope,
    receipt: MessageReceipt,
    error: Error,
  ): RetryDecision {
    // Create a synthetic subscriber definition for retry policy
    const syntheticSubscriber: SubscriberDefinition = {
      name: envelope.docket.targetSubscriber,
      description: 'Synthetic subscriber for unhandled message retry',
      idempotent: 'yes', // Safe to retry unhandled messages
      importance: envelope.docket.importance ?? 'should-investigate',
    };

    const decision = this.retryPolicy.shouldRetry({
      envelope,
      error,
      subscriber: syntheticSubscriber,
      receipt,
    });

    // Override dead-letter queue to 'unhandled' instead of 'undeliverable'
    if (decision.action === 'dead-letter') {
      return {
        action: 'dead-letter',
        queue: 'unhandled',
        reason: decision.reason,
      };
    }

    return decision;
  }
}
