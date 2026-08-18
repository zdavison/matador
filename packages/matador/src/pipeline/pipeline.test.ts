import { describe, expect, it, mock } from 'bun:test';
import type { Codec } from '../codec/index.js';
import { CodecDecodeError } from '../codec/index.js';
import {
  SubscriberIsStubError,
  SubscriberNotRegisteredError,
} from '../errors/index.js';
import type { SafeHooks } from '../hooks/index.js';
import type {
  ProcessDecision,
  RetryDecision,
  RetryPolicy,
} from '../retry/index.js';
import { StandardRetryPolicy } from '../retry/index.js';
import type { SchemaRegistry } from '../schema/index.js';
import type { MessageReceipt, Transport } from '../transport/index.js';
import type {
  Dispatcher,
  Envelope,
  SubscriberDefinition,
} from '../types/index.js';
import { createEnvelope } from '../types/index.js';
import type { PipelineConfig } from './pipeline.js';
import { ProcessingPipeline } from './pipeline.js';

describe('ProcessingPipeline', () => {
  describe('creation', () => {
    it('should create via static factory method', () => {
      const config = createMockConfig();
      const pipeline = new ProcessingPipeline(config);

      expect(pipeline).toBeInstanceOf(ProcessingPipeline);
    });

    it('should create via constructor', () => {
      const config = createMockConfig();
      const pipeline = new ProcessingPipeline(config);

      expect(pipeline).toBeInstanceOf(ProcessingPipeline);
    });
  });

  describe('decode operation', () => {
    it('should decode raw message via codec', async () => {
      const envelope = createTestEnvelope();
      const rawMessage = new TextEncoder().encode(JSON.stringify(envelope));

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {}),
          })),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(rawMessage, receipt);

      expect(config.codec.decode).toHaveBeenCalledWith(rawMessage);
    });

    it('should handle decode errors and complete message', async () => {
      const rawMessage = new Uint8Array([1, 2, 3]);
      const decodeError = new CodecDecodeError('Invalid JSON');

      const completeMock = mock(async () => {});
      const onDecodeErrorMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => {
            throw decodeError;
          }),
        },
        transport: {
          complete: completeMock,
        },
        hooks: {
          onDecodeError: onDecodeErrorMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'test-queue' });

      const result = await pipeline.process(rawMessage, receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBeInstanceOf(CodecDecodeError);
      expect(completeMock).toHaveBeenCalledWith(receipt);
      expect(onDecodeErrorMock).toHaveBeenCalledWith({
        error: decodeError,
        rawMessage,
        sourceQueue: 'test-queue',
        transport: 'mock',
      });
    });

    it('should wrap non-CodecDecodeError exceptions', async () => {
      const rawMessage = new Uint8Array([1, 2, 3]);
      const originalError = new Error('Something went wrong');

      const completeMock = mock(async () => {});
      const onDecodeErrorMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => {
            throw originalError;
          }),
        },
        transport: {
          complete: completeMock,
        },
        hooks: {
          onDecodeError: onDecodeErrorMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(rawMessage, receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBeInstanceOf(CodecDecodeError);
      expect((result.error as CodecDecodeError).message).toBe(
        'Unknown decode error',
      );
      expect((result.error as CodecDecodeError).cause).toBe(originalError);
      expect(completeMock).toHaveBeenCalled();
      expect(onDecodeErrorMock).toHaveBeenCalled();
    });
  });

  describe('subscriber lookup', () => {
    it('should lookup subscriber from registry', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const getSubscriberDefinitionMock = mock(() => subscriberDef);
      const getExecutableSubscriberMock = mock(() => ({
        name: 'test-subscriber',
        description: 'Test subscriber',
        idempotent: 'unknown' as const,
        callback: mock(async () => {}),
      }));

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: getSubscriberDefinitionMock,
          getExecutableSubscriber: getExecutableSubscriberMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(getSubscriberDefinitionMock).toHaveBeenCalledWith(
        envelope.docket.eventKey,
        envelope.docket.targetSubscriber,
      );
      expect(getExecutableSubscriberMock).toHaveBeenCalledWith(
        envelope.docket.eventKey,
        envelope.docket.targetSubscriber,
      );
    });

    it('should retry missing subscriber definition (deployment timing)', async () => {
      const envelope = createTestEnvelope();
      const sendMock = mock(async () => 'mock-id');
      const completeMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => undefined),
        },
        transport: {
          send: sendMock,
          complete: completeMock,
        },
        retryPolicy: new StandardRetryPolicy(),
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBeInstanceOf(SubscriberNotRegisteredError);
      expect(result.envelope).toBeDefined();
      expect(result.decision?.action).toBe('retry');
      expect(sendMock).toHaveBeenCalled();
      expect(completeMock).toHaveBeenCalled();
    });

    it('should dead-letter missing subscriber after max attempts', async () => {
      const envelope = createTestEnvelope();
      const sendToDeadLetterMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => undefined),
        },
        transport: {
          sendToDeadLetter: sendToDeadLetterMock,
          complete: mock(async () => {}),
        },
        retryPolicy: new StandardRetryPolicy(),
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ attemptNumber: 3 }); // At max attempts

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBeInstanceOf(SubscriberNotRegisteredError);
      expect(result.decision?.action).toBe('dead-letter');
      expect(sendToDeadLetterMock).toHaveBeenCalled();
    });

    it('should retry subscriber stub (deployment timing)', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const sendMock = mock(async () => 'mock-id');
      const completeMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => undefined),
        },
        transport: {
          send: sendMock,
          complete: completeMock,
        },
        retryPolicy: new StandardRetryPolicy(),
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBeInstanceOf(SubscriberIsStubError);
      expect(result.subscriber).toEqual(subscriberDef);
      expect(result.decision?.action).toBe('retry');
      expect(sendMock).toHaveBeenCalled();
      expect(completeMock).toHaveBeenCalled();
    });

    it('should dead-letter subscriber stub after max attempts', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const sendToDeadLetterMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => undefined),
        },
        transport: {
          sendToDeadLetter: sendToDeadLetterMock,
          complete: mock(async () => {}),
        },
        retryPolicy: new StandardRetryPolicy(),
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ attemptNumber: 3 }); // At max attempts

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBeInstanceOf(SubscriberIsStubError);
      expect(result.decision?.action).toBe('dead-letter');
      expect(sendToDeadLetterMock).toHaveBeenCalled();
    });
  });

  describe('callback execution', () => {
    it('should execute subscriber callback successfully', async () => {
      const envelope = createTestEnvelope();
      const callbackMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: callbackMock,
          })),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(true);
      expect(callbackMock).toHaveBeenCalledTimes(1);
      // Callback receives envelope as first argument and context (with matador) as second
      const calls = callbackMock.mock.calls as unknown as [
        [Envelope, { matador: Dispatcher }],
      ];
      expect(calls[0][0]).toEqual(envelope);
      expect(calls[0][1]).toHaveProperty('matador');
    });

    it('should handle callback throwing error', async () => {
      const envelope = createTestEnvelope();
      const error = new Error('Processing failed');
      const callbackMock = mock(async () => {
        throw error;
      });

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: callbackMock,
          })),
        },
        retryPolicy: {
          shouldRetry: mock(() => ({
            action: 'dead-letter' as const,
            queue: 'undeliverable',
            reason: 'error',
          })),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBe(error);
    });

    it('should wrap non-Error exceptions as Error', async () => {
      const envelope = createTestEnvelope();
      const callbackMock = mock(async () => {
        throw 'string error';
      });

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: callbackMock,
          })),
        },
        retryPolicy: {
          shouldRetry: mock(() => ({
            action: 'discard' as const,
            reason: 'error',
          })),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toBe('string error');
    });
  });

  describe('hook invocation', () => {
    it('should call onWorkerWrap with execute function', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const callbackMock = mock(async () => {});
      let executeCalled = false;

      const onWorkerWrapMock = mock(
        async (
          _envelope: Envelope,
          _subscriber: SubscriberDefinition,
          execute: () => Promise<void>,
        ) => {
          await execute();
          executeCalled = true;
        },
      );

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: callbackMock,
          })),
        },
        hooks: {
          onWorkerWrap: onWorkerWrapMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(onWorkerWrapMock).toHaveBeenCalledWith(
        envelope,
        subscriberDef,
        expect.any(Function),
      );
      expect(executeCalled).toBe(true);
      expect(callbackMock).toHaveBeenCalled();
    });

    it('should call onWorkerBeforeProcess before callback', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const callOrder: string[] = [];

      const onWorkerBeforeProcessMock = mock(async () => {
        callOrder.push('before');
      });
      const callbackMock = mock(async () => {
        callOrder.push('callback');
      });

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: callbackMock,
          })),
        },
        hooks: {
          onWorkerBeforeProcess: onWorkerBeforeProcessMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(onWorkerBeforeProcessMock).toHaveBeenCalledWith(
        envelope,
        subscriberDef,
      );
      expect(callOrder).toEqual(['before', 'callback']);
    });

    it('should call onWorkerSuccess after successful callback', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const onWorkerSuccessMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {}),
          })),
        },
        hooks: {
          onWorkerSuccess: onWorkerSuccessMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(onWorkerSuccessMock).toHaveBeenCalledWith({
        envelope,
        subscriber: subscriberDef,
        result: undefined,
        durationMs: expect.any(Number),
        transport: 'mock',
      });
    });

    it('should pass subscriber return value to onWorkerSuccess', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const onWorkerSuccessMock = mock(async () => {});
      const returnValue = { processed: true, count: 42 };

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => returnValue),
          })),
        },
        hooks: {
          onWorkerSuccess: onWorkerSuccessMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(onWorkerSuccessMock).toHaveBeenCalledWith({
        envelope,
        subscriber: subscriberDef,
        result: returnValue,
        durationMs: expect.any(Number),
        transport: 'mock',
      });
    });

    it('should call onWorkerError after failed callback', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const error = new Error('Processing failed');
      const decision: RetryDecision = {
        action: 'retry' as const,
        delay: 1000,
      };
      const onWorkerErrorMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw error;
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(() => decision),
        },
        hooks: {
          onWorkerError: onWorkerErrorMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(onWorkerErrorMock).toHaveBeenCalledWith({
        envelope,
        subscriber: subscriberDef,
        error,
        durationMs: expect.any(Number),
        decision,
        transport: 'mock',
      });
    });
  });

  describe('message completion', () => {
    it('should complete message after successful processing', async () => {
      const envelope = createTestEnvelope();
      const completeMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {}),
          })),
        },
        transport: {
          complete: completeMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(completeMock).toHaveBeenCalledWith(receipt);
    });

    it('should not complete message before handling retry decision', async () => {
      const envelope = createTestEnvelope();
      const completeMock = mock(async () => {});
      const sendMock = mock(async () => 'mock');
      let completeCalledAfterSend = false;

      sendMock.mockImplementation(async () => {
        if (completeMock.mock.calls.length > 0) {
          completeCalledAfterSend = false;
        } else {
          completeCalledAfterSend = true;
        }
        return 'mock';
      });

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('Test error');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'retry' as const,
              delay: 1000,
            }),
          ),
        },
        transport: {
          complete: completeMock,
          send: sendMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'test-queue' });

      await pipeline.process(new Uint8Array(), receipt);

      expect(sendMock).toHaveBeenCalled();
      expect(completeMock).toHaveBeenCalled();
      expect(completeCalledAfterSend).toBe(true);
    });
  });

  describe('retry decision handling', () => {
    it('should consult retry policy on error', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const error = new Error('Test error');
      const shouldRetryMock = mock(
        (): RetryDecision => ({
          action: 'retry' as const,
          delay: 1000,
        }),
      );

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw error;
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: shouldRetryMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(shouldRetryMock).toHaveBeenCalledWith({
        envelope,
        error,
        subscriber: subscriberDef,
        receipt,
      });
    });

    it('should handle retry decision by sending to queue with delay', async () => {
      const envelope = createTestEnvelope();
      const sendMock = mock(async () => 'mock');
      const completeMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('Test error');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'retry' as const,
              delay: 2000,
            }),
          ),
        },
        transport: {
          send: sendMock,
          complete: completeMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'test-queue' });

      await pipeline.process(new Uint8Array(), receipt);

      expect(sendMock).toHaveBeenCalledWith(
        'test-queue',
        expect.objectContaining({
          id: envelope.id,
          docket: expect.objectContaining({
            attempts: 2,
            scheduledFor: expect.any(String),
          }),
        }),
        { delay: 2000 },
      );
      expect(completeMock).toHaveBeenCalledWith(receipt);
    });

    it('should update envelope error fields on retry', async () => {
      const envelope = createTestEnvelope();
      const sendMock = mock(async () => 'mock');

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('Test error message');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'retry' as const,
              delay: 1000,
            }),
          ),
        },
        transport: {
          send: sendMock,
          complete: mock(async () => {}),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'test-queue' });

      await pipeline.process(new Uint8Array(), receipt);

      const calls = sendMock.mock.calls as unknown as Array<[string, Envelope]>;
      const firstCall = calls[0];
      if (!firstCall) throw new Error('expected sendMock to be called');
      const sentEnvelope = firstCall[1];
      expect(sentEnvelope.docket.lastError).toBe('Test error message');
      expect(sentEnvelope.docket.firstError).toBe('Test error message');
    });

    it('should handle dead-letter decision', async () => {
      const envelope = createTestEnvelope();
      const sendToDeadLetterMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('Fatal error');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'dead-letter' as const,
              queue: 'undeliverable',
              reason: 'Max attempts exceeded',
            }),
          ),
        },
        transport: {
          sendToDeadLetter: sendToDeadLetterMock,
          complete: mock(async () => {}),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'test-queue' });

      await pipeline.process(new Uint8Array(), receipt);

      expect(sendToDeadLetterMock).toHaveBeenCalledWith(
        receipt,
        'undeliverable',
        envelope,
        'Max attempts exceeded',
      );
    });

    it('should handle dead-letter manually when transport lacks support', async () => {
      const envelope = createTestEnvelope();
      const sendMock = mock(async () => 'mock');
      const completeMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('Fatal error');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'dead-letter' as const,
              queue: 'undeliverable',
              reason: 'Max attempts exceeded',
            }),
          ),
        },
        transport: {
          send: sendMock,
          complete: completeMock,
        },
      });
      // Remove sendToDeadLetter after merging
      (config.transport as { sendToDeadLetter?: unknown }).sendToDeadLetter =
        undefined;

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'test-queue' });

      await pipeline.process(new Uint8Array(), receipt);

      expect(sendMock).toHaveBeenCalledWith(
        'test-queue.undeliverable',
        envelope,
      );
      expect(completeMock).toHaveBeenCalledWith(receipt);
    });

    it('should set originalQueue when sending to dead-letter', async () => {
      const envelope = createTestEnvelope();
      const sendToDeadLetterMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('Fatal error');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'dead-letter' as const,
              queue: 'undeliverable',
              reason: 'Max attempts exceeded',
            }),
          ),
        },
        transport: {
          sendToDeadLetter: sendToDeadLetterMock,
          complete: mock(async () => {}),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'original-queue' });

      await pipeline.process(new Uint8Array(), receipt);

      const calls = sendToDeadLetterMock.mock.calls as unknown as Array<
        [MessageReceipt, string, Envelope, string]
      >;
      const firstCall = calls[0];
      if (!firstCall) throw new Error('expected sendToDeadLetter to be called');
      const sentEnvelope = firstCall[2];
      expect(sentEnvelope.docket.originalQueue).toBe('original-queue');
    });

    it('should handle discard decision', async () => {
      const envelope = createTestEnvelope();
      const completeMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('Discard me');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'discard' as const,
              reason: 'Discarding message',
            }),
          ),
        },
        transport: {
          complete: completeMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      await pipeline.process(new Uint8Array(), receipt);

      expect(completeMock).toHaveBeenCalledWith(receipt);
    });
  });

  describe('pre-processing check', () => {
    it('should call retryPolicy.shouldProcess before invoking the callback', async () => {
      const envelope = createTestEnvelope();
      const callbackMock = mock(async () => {});
      const shouldProcessMock = mock(
        (): ProcessDecision => ({ action: 'process' as const }),
      );

      const subscriberDef = createSubscriberDefinition();

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: callbackMock,
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({ action: 'discard' as const, reason: '' }),
          ),
          getDelay: mock(() => 1000),
          shouldProcess: shouldProcessMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ deliveryCount: 1 });

      await pipeline.process(new Uint8Array(), receipt);

      expect(shouldProcessMock).toHaveBeenCalledWith({
        envelope,
        subscriber: subscriberDef,
        receipt,
      });
      expect(callbackMock).toHaveBeenCalled();
    });

    it('should dead-letter without invoking the callback when shouldProcess flags the message as poisoned', async () => {
      const envelope = createTestEnvelope();
      const callbackMock = mock(async () => {});
      const sendToDeadLetterMock = mock(async () => {});
      const onWorkerErrorMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: callbackMock,
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({ action: 'discard' as const, reason: '' }),
          ),
          getDelay: mock(() => 1000),
          shouldProcess: mock(
            (): ProcessDecision => ({
              action: 'dead-letter' as const,
              queue: 'undeliverable',
              reason:
                'Message delivered 5 times (max: 5). Possible poison message.',
            }),
          ),
        },
        transport: {
          sendToDeadLetter: sendToDeadLetterMock,
        },
        hooks: {
          onWorkerError: onWorkerErrorMock,
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ deliveryCount: 5 });

      const result = await pipeline.process(new Uint8Array(), receipt);

      // The callback must never run for an already-poisoned message.
      expect(callbackMock).not.toHaveBeenCalled();

      expect(sendToDeadLetterMock).toHaveBeenCalledWith(
        receipt,
        'undeliverable',
        expect.objectContaining({ id: envelope.id }),
        expect.stringContaining('Possible poison message'),
      );

      expect(onWorkerErrorMock).toHaveBeenCalledWith(
        expect.objectContaining({
          envelope,
          error: expect.any(Error),
          decision: expect.objectContaining({ action: 'dead-letter' }),
        }),
      );

      expect(result.success).toBe(false);
      expect(result.decision).toEqual(
        expect.objectContaining({ action: 'dead-letter' }),
      );
    });

    it('should clear the checkpoint before dead-lettering a poisoned resumable subscriber', async () => {
      const envelope = createTestEnvelope();
      const callbackMock = mock(async () => {});
      const deleteMock = mock(async () => {});

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'resumable' as const,
            callback: callbackMock,
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({ action: 'discard' as const, reason: '' }),
          ),
          getDelay: mock(() => 1000),
          shouldProcess: mock(
            (): ProcessDecision => ({
              action: 'dead-letter' as const,
              queue: 'undeliverable',
              reason: 'poisoned',
            }),
          ),
        },
      });
      const pipelineConfig = {
        ...config,
        checkpointStore: {
          get: mock(async () => undefined),
          set: mock(async () => {}),
          delete: deleteMock,
        },
      };

      const pipeline = new ProcessingPipeline(pipelineConfig);
      const receipt = createReceipt({ deliveryCount: 5 });

      await pipeline.process(new Uint8Array(), receipt);

      expect(callbackMock).not.toHaveBeenCalled();
      expect(deleteMock).toHaveBeenCalledWith(envelope.id);
    });
  });

  describe('process result', () => {
    it('should return success result with duration', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {}),
          })),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(true);
      expect(result.envelope).toEqual(envelope);
      expect(result.subscriber).toEqual(subscriberDef);
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
      expect(result.error).toBeUndefined();
      expect(result.decision).toBeUndefined();
    });

    it('should return failure result with error and decision', async () => {
      const envelope = createTestEnvelope();
      const subscriberDef = createSubscriberDefinition();
      const error = new Error('Processing failed');
      const decision: RetryDecision = { action: 'retry' as const, delay: 1000 };

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => subscriberDef),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw error;
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock((): RetryDecision => decision),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(false);
      expect(result.envelope).toEqual(envelope);
      expect(result.subscriber).toEqual(subscriberDef);
      expect(result.error).toBe(error);
      expect(result.decision).toEqual(decision);
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('should return result with only error on decode failure', async () => {
      const error = new CodecDecodeError('Invalid message');

      const config = createMockConfig({
        codec: {
          decode: mock(() => {
            throw error;
          }),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt();

      const result = await pipeline.process(new Uint8Array(), receipt);

      expect(result.success).toBe(false);
      expect(result.error).toBe(error);
      expect(result.envelope).toBeUndefined();
      expect(result.subscriber).toBeUndefined();
      expect(result.decision).toBeUndefined();
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  describe('envelope error tracking', () => {
    it('should set firstError on first failure', async () => {
      const envelope = createTestEnvelope();
      const sendMock = mock(async () => 'mock');

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('First failure');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'retry' as const,
              delay: 1000,
            }),
          ),
        },
        transport: {
          send: sendMock,
          complete: mock(async () => {}),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'test-queue' });

      await pipeline.process(new Uint8Array(), receipt);

      const calls = sendMock.mock.calls as unknown as Array<[string, Envelope]>;
      const firstCall = calls[0];
      if (!firstCall) throw new Error('expected sendMock to be called');
      const sentEnvelope = firstCall[1];
      expect(sentEnvelope.docket.firstError).toBe('First failure');
      expect(sentEnvelope.docket.lastError).toBe('First failure');
    });

    it('should preserve firstError on subsequent failures', async () => {
      const envelope = createTestEnvelope();
      envelope.docket.firstError = 'Original error';
      const sendMock = mock(async () => 'mock');

      const config = createMockConfig({
        codec: {
          decode: mock(() => envelope),
        },
        schema: {
          getSubscriberDefinition: mock(() => createSubscriberDefinition()),
          getExecutableSubscriber: mock(() => ({
            name: 'test-subscriber',
            description: 'Test subscriber',
            idempotent: 'unknown' as const,
            callback: mock(async () => {
              throw new Error('Second failure');
            }),
          })),
        },
        retryPolicy: {
          shouldRetry: mock(
            (): RetryDecision => ({
              action: 'retry' as const,
              delay: 1000,
            }),
          ),
        },
        transport: {
          send: sendMock,
          complete: mock(async () => {}),
        },
      });

      const pipeline = new ProcessingPipeline(config);
      const receipt = createReceipt({ sourceQueue: 'test-queue' });

      await pipeline.process(new Uint8Array(), receipt);

      const calls = sendMock.mock.calls as unknown as Array<[string, Envelope]>;
      const firstCall = calls[0];
      if (!firstCall) throw new Error('expected sendMock to be called');
      const sentEnvelope = firstCall[1];
      expect(sentEnvelope.docket.firstError).toBe('Original error');
      expect(sentEnvelope.docket.lastError).toBe('Second failure');
    });
  });
});

function createMockConfig(
  overrides: Partial<{
    transport: Partial<Transport>;
    schema: Partial<SchemaRegistry>;
    codec: Partial<Codec>;
    retryPolicy: RetryPolicy | Partial<RetryPolicy>;
    hooks: Partial<SafeHooks>;
    dispatcher: Partial<Dispatcher>;
  }> = {},
): PipelineConfig {
  const defaultTransport: Transport = {
    name: 'mock',
    capabilities: {
      deliveryModes: ['at-least-once'],
      delayedMessages: true,
      deadLetterRouting: 'manual',
      attemptTracking: true,
      concurrencyModel: 'none',
      ordering: 'none',
      priorities: false,
    },
    connect: mock(async () => {}),
    disconnect: mock(async () => {}),
    isConnected: mock(() => true),
    applyTopology: mock(async () => {}),
    send: mock(async () => 'mock'),
    subscribe: mock(async () => ({
      unsubscribe: async () => {},
      isActive: true,
    })),
    complete: mock(async () => {}),
    sendToDeadLetter: mock(async () => {}),
    ...overrides.transport,
  };

  const defaultSchema: SchemaRegistry = {
    getSubscriberDefinition: mock(() => undefined),
    getExecutableSubscriber: mock(() => undefined),
  } as unknown as SchemaRegistry;

  const defaultCodec: Codec = {
    encode: mock(() => new Uint8Array()),
    decode: mock(() => createTestEnvelope()),
    contentType: 'application/json',
    ...overrides.codec,
  };

  // If retryPolicy is a full implementation (has both methods), use it directly
  // Otherwise, merge with defaults
  const isFullRetryPolicy = (
    p: RetryPolicy | Partial<RetryPolicy> | undefined,
  ): p is RetryPolicy => {
    return (
      p !== undefined &&
      'shouldRetry' in p &&
      'getDelay' in p &&
      typeof p.shouldRetry === 'function' &&
      typeof p.getDelay === 'function'
    );
  };

  const defaultRetryPolicy: RetryPolicy = isFullRetryPolicy(
    overrides.retryPolicy,
  )
    ? overrides.retryPolicy
    : {
        shouldProcess: mock(
          (): ProcessDecision => ({
            action: 'process' as const,
          }),
        ),
        shouldRetry: mock(
          (): RetryDecision => ({
            action: 'discard' as const,
            reason: 'default',
          }),
        ),
        getDelay: mock(() => 1000),
        ...overrides.retryPolicy,
      };

  const defaultHooks: SafeHooks = {
    onWorkerWrap: async (
      _envelope: Envelope,
      _subscriber: SubscriberDefinition,
      execute: () => Promise<void>,
    ) => {
      await execute();
    },
    onWorkerBeforeProcess: async () => {},
    onWorkerSuccess: async () => {},
    onWorkerError: async () => {},
    onDecodeError: async () => {},
    ...overrides.hooks,
  } as SafeHooks;

  const defaultDispatcher: Dispatcher = {
    send: mock(async () => ({
      eventKey: 'test.event',
      subscribersSent: 0,
      subscribersSkipped: 0,
      errors: [],
    })),
    ...overrides.dispatcher,
  } as Dispatcher;

  return {
    transport: defaultTransport,
    schema: { ...defaultSchema, ...overrides.schema } as SchemaRegistry,
    codec: defaultCodec,
    retryPolicy: defaultRetryPolicy,
    hooks: defaultHooks,
    dispatcher: defaultDispatcher,
  };
}

function createTestEnvelope(): Envelope {
  return createEnvelope({
    eventKey: 'test.event',
    targetSubscriber: 'test-subscriber',
    data: { test: 'data' },
    importance: 'should-investigate',
  });
}

function createReceipt(
  overrides: Partial<MessageReceipt> = {},
): MessageReceipt {
  return {
    handle: {},
    redelivered: false,
    attemptNumber: 1,
    deliveryCount: 1,
    sourceQueue: 'test-queue',
    sourceTransport: 'mock',
    ...overrides,
  };
}

function createSubscriberDefinition(
  overrides: Partial<SubscriberDefinition> = {},
): SubscriberDefinition {
  return {
    name: 'test-subscriber',
    description: 'Test subscriber',
    idempotent: 'unknown',
    importance: 'should-investigate',
    ...overrides,
  };
}
