import {
  type StatusRecord,
  type StatusStore,
  type StatusStoreConfig,
  resolveStatusStoreConfig,
} from '../types.js';

/**
 * Copies a record through JSON, so the memory store behaves like a
 * serializing store (for example Redis): later changes to the input do not
 * show, and undefined fields are dropped.
 */
function cloneRecord(record: StatusRecord): StatusRecord {
  return JSON.parse(JSON.stringify(record)) as StatusRecord;
}

/**
 * In-memory status store for tests and local development.
 * Records are lost when the process restarts.
 */
export class MemoryStatusStore implements StatusStore {
  readonly config: StatusStoreConfig;
  private readonly records = new Map<string, Map<string, StatusRecord>>();

  constructor(options: Partial<StatusStoreConfig> = {}) {
    this.config = resolveStatusStoreConfig(options);
  }

  async getAll(
    trackingId: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | undefined> {
    const bySubscriber = this.records.get(trackingId);
    if (!bySubscriber || bySubscriber.size === 0) {
      return undefined;
    }
    return Object.fromEntries(
      [...bySubscriber].map(([name, record]) => [name, cloneRecord(record)]),
    );
  }

  async get(
    trackingId: string,
    subscriberName: string,
  ): Promise<StatusRecord | undefined> {
    const record = this.records.get(trackingId)?.get(subscriberName);
    return record === undefined ? undefined : cloneRecord(record);
  }

  async set(
    trackingId: string,
    subscriberName: string,
    record: StatusRecord,
  ): Promise<void> {
    let bySubscriber = this.records.get(trackingId);
    if (!bySubscriber) {
      bySubscriber = new Map();
      this.records.set(trackingId, bySubscriber);
    }
    bySubscriber.set(subscriberName, cloneRecord(record));
  }

  async delete(trackingId: string): Promise<void> {
    this.records.delete(trackingId);
  }

  async cleanup(olderThan: Date): Promise<number> {
    const limit = olderThan.getTime();
    let count = 0;
    for (const [trackingId, bySubscriber] of this.records) {
      for (const [subscriberName, record] of bySubscriber) {
        if (Date.parse(record.updatedAt) < limit) {
          bySubscriber.delete(subscriberName);
          count++;
        }
      }
      if (bySubscriber.size === 0) {
        this.records.delete(trackingId);
      }
    }
    return count;
  }
}
