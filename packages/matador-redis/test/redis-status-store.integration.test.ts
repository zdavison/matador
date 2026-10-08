import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import {
  RedisContainer,
  type StartedRedisContainer,
} from '@testcontainers/redis';
import { createEnvelope } from '@zdavison/matador';
import { createClient } from 'redis';
import { describeStatusStoreContract } from '../../matador/src/status/stores/status-store-contract.js';
import { RedisStatusStore } from '../src/index.js';

// Skip tests if docker is not available
const SKIP_E2E = process.env.SKIP_E2E_TESTS === 'true';

describe.skipIf(SKIP_E2E)('RedisStatusStore with a real Redis', () => {
  let container: StartedRedisContainer;
  let client: ReturnType<typeof createClient>;

  beforeAll(async () => {
    container = await new RedisContainer('redis:7-alpine').start();
    client = createClient({ url: container.getConnectionUrl() });
    await client.connect();
  }, 120_000);

  afterAll(async () => {
    await client?.quit();
    await container?.stop();
  });

  describeStatusStoreContract(
    'RedisStatusStore (redis v4 client)',
    () => new RedisStatusStore({ client, ttlMs: 60_000 }),
  );

  it('sets a TTL on the hash and refreshes it on each write', async () => {
    const store = new RedisStatusStore({ client, ttlMs: 60_000 });
    const trackingId = crypto.randomUUID();
    const record = {
      status: 'queued' as const,
      envelope: createEnvelope({
        eventKey: 'job.requested',
        targetSubscriber: 'runner',
        importance: 'should-investigate' as const,
        data: {},
      }),
      updatedAt: new Date().toISOString(),
    };

    await store.set(trackingId, 'runner', record);
    const ttl = await client.pTTL(`matador:status:${trackingId}`);

    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(60_000);
  });
});
