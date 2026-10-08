import { describe, expect, it, mock } from 'bun:test';
import { SafeHooks } from '../hooks/index.js';
import type { Logger, StatusStoreErrorContext } from '../hooks/index.js';
import { createEnvelope } from '../types/index.js';
import type { Envelope } from '../types/index.js';
import { MemoryStatusStore } from './stores/memory.js';
import { StatusTracker } from './tracker.js';
import {
  type StatusRecord,
  type StatusStore,
  type StatusStoreConfig,
  resolveStatusStoreConfig,
} from './types.js';

const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

class FailingStore implements StatusStore {
  readonly config: StatusStoreConfig = resolveStatusStoreConfig();
  async getAll(): Promise<undefined> {
    throw new Error('store down');
  }
  async get(): Promise<undefined> {
    throw new Error('store down');
  }
  async set(): Promise<void> {
    throw new Error('store down');
  }
  async delete(): Promise<void> {
    throw new Error('store down');
  }
}

function envelopeFor(
  subscriber: string,
  data: unknown = { userId: 'u-1' },
  trackingId = 't-1',
): Envelope {
  return createEnvelope({
    eventKey: 'job.requested',
    targetSubscriber: subscriber,
    importance: 'should-investigate',
    data,
    trackingId,
  });
}

function setup(
  store: StatusStore = new MemoryStatusStore({ progressIntervalMs: 0 }),
) {
  const onStatusStoreError = mock((_context: StatusStoreErrorContext) => {});
  const warn = mock((_message: string, ..._args: unknown[]) => {});
  const hooks = new SafeHooks({
    logger: { ...silentLogger, warn },
    onStatusStoreError,
  });
  return {
    store,
    tracker: new StatusTracker(store, hooks),
    onStatusStoreError,
    warn,
  };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('StatusTracker', () => {
  describe('createQueued', () => {
    it('writes a queued record for each envelope', async () => {
      const { store, tracker } = setup();
      await tracker.createQueued([envelopeFor('a'), envelopeFor('b')]);
      const all = await store.getAll('t-1');
      expect(Object.keys(all ?? {}).sort()).toEqual(['a', 'b']);
      expect(all?.a?.status).toBe('queued');
      expect(all?.a?.envelope.data).toEqual({ userId: 'u-1' });
    });

    it('throws RecordTooLargeError and writes nothing if any record is too large', async () => {
      const { store, tracker } = setup(
        new MemoryStatusStore({ maxRecordBytes: 1024 }),
      );
      const small = envelopeFor('a');
      const large = envelopeFor('b', { blob: 'x'.repeat(2000) });
      await expect(tracker.createQueued([small, large])).rejects.toMatchObject({
        name: 'RecordTooLargeError',
        subscriberName: 'b',
        maxBytes: 1024,
      });
      expect(await store.getAll('t-1')).toBeUndefined();
    });

    it('wraps a store failure in StatusStoreError', async () => {
      const { tracker } = setup(new FailingStore());
      await expect(
        tracker.createQueued([envelopeFor('a')]),
      ).rejects.toMatchObject({
        name: 'StatusStoreError',
        operation: 'create',
        trackingId: 't-1',
      });
    });
  });

  describe('transitions', () => {
    it('markActive writes active', async () => {
      const { store, tracker } = setup();
      await tracker.markActive(envelopeFor('a'));
      expect((await store.get('t-1', 'a'))?.status).toBe('active');
    });

    it('markCompleted writes completed with the result', async () => {
      const { store, tracker } = setup();
      await tracker.markCompleted(envelopeFor('a'), {
        s3Key: 'exports/1.csv',
        rows: 3,
      });
      const record = await store.get('t-1', 'a');
      expect(record?.status).toBe('completed');
      expect(record?.result).toEqual({ s3Key: 'exports/1.csv', rows: 3 });
      expect(record?.error).toBeUndefined();
    });

    it('markCompleted writes no result field for an undefined return value', async () => {
      const { store, tracker } = setup();
      await tracker.markCompleted(envelopeFor('a'), undefined);
      const record = await store.get('t-1', 'a');
      expect(record?.status).toBe('completed');
      expect(record && 'result' in record).toBe(false);
    });

    it('markCompleted keeps the last progress value', async () => {
      const { store, tracker } = setup();
      const envelope = envelopeFor('a');
      const reporter = tracker.startProgress(envelope);
      reporter.report(80, { done: 8 });
      await tracker.markCompleted(envelope, undefined, reporter);
      expect((await store.get('t-1', 'a'))?.progress).toEqual({
        percent: 80,
        details: { done: 8 },
      });
    });

    it('fails with RecordTooLargeError when the result is too large', async () => {
      const { store, tracker } = setup(
        new MemoryStatusStore({ maxRecordBytes: 2048 }),
      );
      await tracker.markCompleted(envelopeFor('a'), { text: 'x'.repeat(4000) });
      const record = await store.get('t-1', 'a');
      expect(record?.status).toBe('failed');
      expect(record?.error?.name).toBe('RecordTooLargeError');
      expect(record && 'result' in record).toBe(false);
    });

    it('fails with ResultNotSerializableError for a BigInt or a function', async () => {
      const { store, tracker } = setup();
      await tracker.markCompleted(envelopeFor('a', {}, 't-big'), 10n);
      await tracker.markCompleted(envelopeFor('a', {}, 't-fn'), () => 'x');
      for (const trackingId of ['t-big', 't-fn']) {
        const record = await store.get(trackingId, 'a');
        expect(record?.status).toBe('failed');
        expect(record?.error?.name).toBe('ResultNotSerializableError');
      }
    });

    it('markFailure writes retrying for a retry decision', async () => {
      const { store, tracker } = setup();
      await tracker.markFailure(envelopeFor('a'), new Error('timeout'), {
        action: 'retry',
        delay: 100,
      });
      const record = await store.get('t-1', 'a');
      expect(record?.status).toBe('retrying');
      expect(record?.error).toEqual({ name: 'Error', message: 'timeout' });
    });

    it('markFailure writes failed for dead-letter and discard decisions', async () => {
      const { store, tracker } = setup();
      await tracker.markFailure(
        envelopeFor('a', {}, 't-dl'),
        new Error('boom'),
        {
          action: 'dead-letter',
          queue: 'undeliverable',
          reason: 'boom',
        },
      );
      await tracker.markFailure(
        envelopeFor('a', {}, 't-dc'),
        new Error('gone'),
        {
          action: 'discard',
          reason: 'gone',
        },
      );
      expect((await store.get('t-dl', 'a'))?.status).toBe('failed');
      expect((await store.get('t-dc', 'a'))?.status).toBe('failed');
      expect((await store.get('t-dc', 'a'))?.error?.message).toBe('gone');
    });

    it('markPublishFailed writes failed with the publish error', async () => {
      const { store, tracker } = setup();
      await tracker.markPublishFailed(
        envelopeFor('a'),
        new Error('broker down'),
      );
      const record = await store.get('t-1', 'a');
      expect(record?.status).toBe('failed');
      expect(record?.error?.message).toBe('broker down');
    });
  });

  describe('store failures after send', () => {
    it('reports an update failure to the hook and does not throw', async () => {
      const { tracker, onStatusStoreError } = setup(new FailingStore());
      await tracker.markActive(envelopeFor('a'));
      await tracker.markCompleted(envelopeFor('a'), { ok: true });
      expect(onStatusStoreError).toHaveBeenCalledTimes(2);
      expect(onStatusStoreError.mock.calls[0]).toEqual([
        expect.objectContaining({
          trackingId: 't-1',
          subscriberName: 'a',
          operation: 'update',
        }),
      ]);
    });

    it('drops a progress value that makes the record too large', async () => {
      const store = new MemoryStatusStore({
        maxRecordBytes: 2048,
        progressIntervalMs: 0,
      });
      const { tracker, warn, onStatusStoreError } = setup(store);
      const envelope = envelopeFor('a');
      await tracker.markActive(envelope);
      const reporter = tracker.startProgress(envelope);
      reporter.report(50, { text: 'x'.repeat(4000) });
      await settle();
      await reporter.stop();
      const record = await store.get('t-1', 'a');
      expect(record?.status).toBe('active');
      expect(record?.progress).toBeUndefined();
      expect(warn).toHaveBeenCalled();
      expect(onStatusStoreError).not.toHaveBeenCalled();
    });

    it('logs a warning for a progress value that is not a finite number', () => {
      const { tracker, warn } = setup();
      tracker.startProgress(envelopeFor('a')).report(Number.NaN);
      expect(warn).toHaveBeenCalled();
    });
  });

  describe('read', () => {
    it('reads all records or one record', async () => {
      const { tracker } = setup();
      await tracker.createQueued([envelopeFor('a'), envelopeFor('b')]);
      const all = await tracker.read('t-1');
      const one = await tracker.read('t-1', 'b');
      expect(Object.keys(all ?? {}).sort()).toEqual(['a', 'b']);
      expect((one as StatusRecord | undefined)?.status).toBe('queued');
    });

    it('throws StatusStoreError and calls the hook when the store fails', async () => {
      const { tracker, onStatusStoreError } = setup(new FailingStore());
      await expect(tracker.read('t-1')).rejects.toMatchObject({
        name: 'StatusStoreError',
        operation: 'read',
      });
      expect(onStatusStoreError).toHaveBeenCalledTimes(1);
    });
  });
});
