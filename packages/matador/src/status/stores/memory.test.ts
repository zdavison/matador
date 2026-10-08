import { describe, expect, it } from 'bun:test';
import { createEnvelope } from '../../types/index.js';
import type { StatusRecord } from '../types.js';
import { MemoryStatusStore } from './memory.js';
import { describeStatusStoreContract } from './status-store-contract.js';

describeStatusStoreContract('MemoryStatusStore', () => new MemoryStatusStore());

function recordAt(updatedAt: string): StatusRecord {
  return {
    status: 'completed',
    envelope: createEnvelope({
      eventKey: 'memory.event',
      targetSubscriber: 'sub-a',
      importance: 'should-investigate',
      data: {},
    }),
    updatedAt,
  };
}

describe('MemoryStatusStore', () => {
  it('takes config options', () => {
    const store = new MemoryStatusStore({ maxRecordBytes: 2048 });
    expect(store.config).toEqual({
      maxRecordBytes: 2048,
      progressIntervalMs: 1_000,
    });
  });

  it('cleanup deletes records older than the date and returns the count', async () => {
    const store = new MemoryStatusStore();
    await store.set('old', 'sub-a', recordAt('2026-01-01T00:00:00.000Z'));
    await store.set('old', 'sub-b', recordAt('2026-03-01T00:00:00.000Z'));
    await store.set('new', 'sub-a', recordAt('2026-03-01T00:00:00.000Z'));

    const count = await store.cleanup(new Date('2026-02-01T00:00:00.000Z'));

    expect(count).toBe(1);
    expect(await store.get('old', 'sub-a')).toBeUndefined();
    expect(await store.get('old', 'sub-b')).toBeDefined();
    expect(await store.get('new', 'sub-a')).toBeDefined();
  });

  it('cleanup removes a tracking ID when its last record is deleted', async () => {
    const store = new MemoryStatusStore();
    await store.set('old', 'sub-a', recordAt('2026-01-01T00:00:00.000Z'));
    await store.cleanup(new Date('2026-02-01T00:00:00.000Z'));
    expect(await store.getAll('old')).toBeUndefined();
  });
});
