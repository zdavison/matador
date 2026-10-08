import {
  type StatusRecord,
  type StatusStore,
  type StatusStoreConfig,
  resolveStatusStoreConfig,
} from '@zdavison/matador';

/**
 * The part of a Redis MULTI chain that the store uses.
 * A `redis` v4 (node-redis) client fits this interface.
 */
export interface RedisStatusMulti {
  hSet(key: string, field: string, value: string): RedisStatusMulti;
  pExpire(key: string, milliseconds: number): RedisStatusMulti;
  exec(): Promise<unknown>;
}

/**
 * The part of a Redis client that the store uses.
 * A `redis` v4 (node-redis) client fits this interface. For other clients,
 * for example ioredis, write a small adapter.
 */
export interface RedisStatusClient {
  hGet(key: string, field: string): Promise<string | null | undefined>;
  hGetAll(key: string): Promise<Record<string, string>>;
  del(key: string): Promise<number>;
  multi(): RedisStatusMulti;
}

export interface RedisStatusStoreOptions extends Partial<StatusStoreConfig> {
  /** A connected Redis client */
  readonly client: RedisStatusClient;
  /** Retention of each tracking ID, in milliseconds. Each write refreshes it. Required. */
  readonly ttlMs: number;
  /** Prefix for the Redis keys. Default: 'matador:status:' */
  readonly keyPrefix?: string | undefined;
}

export const DEFAULT_KEY_PREFIX = 'matador:status:';

/**
 * Status store on Redis.
 *
 * Keeps one Redis hash for each tracking ID: one field for each subscriber name,
 * and the value is the JSON record. Each write refreshes the TTL of the hash.
 * Redis TTL replaces cleanup().
 */
export class RedisStatusStore implements StatusStore {
  readonly config: StatusStoreConfig;
  private readonly client: RedisStatusClient;
  private readonly ttlMs: number;
  private readonly keyPrefix: string;

  constructor(options: RedisStatusStoreOptions) {
    const { client, ttlMs, keyPrefix, ...config } = options;
    if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
      throw new RangeError(
        `RedisStatusStore: ttlMs must be a positive integer number of milliseconds. Got ${ttlMs}.`,
      );
    }
    this.config = resolveStatusStoreConfig(config);
    this.client = client;
    this.ttlMs = ttlMs;
    this.keyPrefix = keyPrefix ?? DEFAULT_KEY_PREFIX;
  }

  async getAll(
    trackingId: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | undefined> {
    const hash = await this.client.hGetAll(this.key(trackingId));
    const entries = Object.entries(hash);
    if (entries.length === 0) {
      return undefined;
    }
    return Object.fromEntries(
      entries.map(([name, value]) => [name, JSON.parse(value) as StatusRecord]),
    );
  }

  async get(
    trackingId: string,
    subscriberName: string,
  ): Promise<StatusRecord | undefined> {
    const value = await this.client.hGet(this.key(trackingId), subscriberName);
    if (value === null || value === undefined) {
      return undefined;
    }
    return JSON.parse(value) as StatusRecord;
  }

  async set(
    trackingId: string,
    subscriberName: string,
    record: StatusRecord,
  ): Promise<void> {
    const key = this.key(trackingId);
    await this.client
      .multi()
      .hSet(key, subscriberName, JSON.stringify(record))
      .pExpire(key, this.ttlMs)
      .exec();
  }

  async delete(trackingId: string): Promise<void> {
    await this.client.del(this.key(trackingId));
  }

  private key(trackingId: string): string {
    return `${this.keyPrefix}${trackingId}`;
  }
}
