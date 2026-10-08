import { describe, expect, it } from 'bun:test';
import { createEnvelope } from '@zdavison/matador';
import { describeStatusStoreContract } from '../../matador/src/status/stores/status-store-contract.js';
import {
  type RedisStatusClient,
  type RedisStatusMulti,
  RedisStatusStore,
} from './redis-status-store.js';

/** In-memory fake of the small Redis client interface. */
class FakeRedis implements RedisStatusClient {
  readonly hashes = new Map<string, Map<string, string>>();
  readonly ttls = new Map<string, number>();

  async hGet(key: string, field: string): Promise<string | undefined> {
    return this.hashes.get(key)?.get(field);
  }

  async hGetAll(key: string): Promise<Record<string, string>> {
    return Object.fromEntries(this.hashes.get(key) ?? []);
  }

  async del(key: string): Promise<number> {
    this.ttls.delete(key);
    return this.hashes.delete(key) ? 1 : 0;
  }

  multi(): RedisStatusMulti {
    const ops: (() => void)[] = [];
    const chain: RedisStatusMulti = {
      hSet: (key, field, value) => {
        ops.push(() => {
          let hash = this.hashes.get(key);
          if (!hash) {
            hash = new Map();
            this.hashes.set(key, hash);
          }
          hash.set(field, value);
        });
        return chain;
      },
      pExpire: (key, milliseconds) => {
        ops.push(() => {
          this.ttls.set(key, milliseconds);
        });
        return chain;
      },
      exec: async () => {
        for (const op of ops) op();
        return [];
      },
    };
    return chain;
  }
}

describeStatusStoreContract(
  'RedisStatusStore (fake client)',
  () => new RedisStatusStore({ client: new FakeRedis(), ttlMs: 60_000 }),
);

function record() {
  return {
    status: 'queued' as const,
    envelope: createEnvelope({
      eventKey: 'job.requested',
      targetSubscriber: 'runner',
      importance: 'should-investigate' as const,
      data: {},
    }),
    updatedAt: '2026-01-01T00:00:00.000Z',
  };
}

describe('RedisStatusStore', () => {
  it('stores one hash for each tracking ID under the key prefix', async () => {
    const client = new FakeRedis();
    const store = new RedisStatusStore({
      client,
      ttlMs: 60_000,
      keyPrefix: 'app:',
    });
    await store.set('t-1', 'runner', record());
    expect([...client.hashes.keys()]).toEqual(['app:t-1']);
    expect([...(client.hashes.get('app:t-1')?.keys() ?? [])]).toEqual([
      'runner',
    ]);
  });

  it('uses matador:status: as the default key prefix', async () => {
    const client = new FakeRedis();
    const store = new RedisStatusStore({ client, ttlMs: 60_000 });
    await store.set('t-1', 'runner', record());
    expect([...client.hashes.keys()]).toEqual(['matador:status:t-1']);
  });

  it('sets the TTL on each write', async () => {
    const client = new FakeRedis();
    const store = new RedisStatusStore({ client, ttlMs: 90_000 });
    await store.set('t-1', 'runner', record());
    expect(client.ttls.get('matador:status:t-1')).toBe(90_000);
  });

  it('takes the status store settings', () => {
    const store = new RedisStatusStore({
      client: new FakeRedis(),
      ttlMs: 60_000,
      maxRecordBytes: 4096,
    });
    expect(store.config).toEqual({
      maxRecordBytes: 4096,
      progressIntervalMs: 1_000,
    });
  });

  it('rejects a ttlMs that is not a positive integer', () => {
    for (const ttlMs of [0, -1, 1.5, Number.NaN]) {
      expect(
        () => new RedisStatusStore({ client: new FakeRedis(), ttlMs }),
      ).toThrow(RangeError);
    }
  });

  it('does not implement cleanup, because Redis TTL replaces it', () => {
    const store = new RedisStatusStore({
      client: new FakeRedis(),
      ttlMs: 60_000,
    });
    expect('cleanup' in store).toBe(false);
  });
});
