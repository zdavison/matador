import { beforeEach, describe, expect, it } from 'bun:test';
import { createEnvelope } from '../../types/index.js';
import type { StatusRecord, StatusStore, TrackingStatus } from '../types.js';

function makeRecord(
  subscriberName: string,
  status: TrackingStatus = 'queued',
  updatedAt = '2026-01-01T00:00:00.000Z',
): StatusRecord {
  return {
    status,
    envelope: createEnvelope({
      eventKey: 'contract.event',
      targetSubscriber: subscriberName,
      importance: 'should-investigate',
      data: { userId: 'user-1' },
    }),
    updatedAt,
  };
}

/**
 * Shared behavior tests for any StatusStore implementation.
 * Each test uses a new random tracking ID, so stores that keep data
 * between tests (for example Redis) do not interfere.
 */
export function describeStatusStoreContract(
  name: string,
  createStore: () => StatusStore | Promise<StatusStore>,
): void {
  describe(`${name} (StatusStore contract)`, () => {
    let store: StatusStore;

    beforeEach(async () => {
      store = await createStore();
    });

    it('exposes a resolved config', () => {
      expect(store.config.maxRecordBytes).toBeGreaterThan(0);
      expect(store.config.progressIntervalMs).toBeGreaterThanOrEqual(0);
    });

    it('returns undefined for an unknown tracking ID', async () => {
      const trackingId = crypto.randomUUID();
      expect(await store.getAll(trackingId)).toBeUndefined();
      expect(await store.get(trackingId, 'sub-a')).toBeUndefined();
    });

    it('writes and reads one record', async () => {
      const trackingId = crypto.randomUUID();
      const record = makeRecord('sub-a', 'active');
      await store.set(trackingId, 'sub-a', record);
      expect(await store.get(trackingId, 'sub-a')).toEqual(record);
    });

    it('keeps one record for each subscriber under one tracking ID', async () => {
      const trackingId = crypto.randomUUID();
      const a = makeRecord('sub-a', 'active');
      const b = makeRecord('sub-b', 'completed');
      await store.set(trackingId, 'sub-a', a);
      await store.set(trackingId, 'sub-b', b);
      expect(await store.getAll(trackingId)).toEqual({
        'sub-a': a,
        'sub-b': b,
      });
    });

    it('replaces a record with the same tracking ID and subscriber name', async () => {
      const trackingId = crypto.randomUUID();
      await store.set(trackingId, 'sub-a', makeRecord('sub-a', 'queued'));
      const latest = makeRecord('sub-a', 'completed');
      await store.set(trackingId, 'sub-a', latest);
      expect(await store.get(trackingId, 'sub-a')).toEqual(latest);
    });

    it('deletes all records for a tracking ID', async () => {
      const trackingId = crypto.randomUUID();
      await store.set(trackingId, 'sub-a', makeRecord('sub-a'));
      await store.set(trackingId, 'sub-b', makeRecord('sub-b'));
      await store.delete(trackingId);
      expect(await store.getAll(trackingId)).toBeUndefined();
    });

    it('is not affected by later changes to the written record', async () => {
      const trackingId = crypto.randomUUID();
      const record = makeRecord('sub-a', 'active');
      await store.set(trackingId, 'sub-a', record);
      record.envelope.docket.attempts = 99;
      const stored = await store.get(trackingId, 'sub-a');
      expect(stored?.envelope.docket.attempts).toBe(1);
    });
  });
}
