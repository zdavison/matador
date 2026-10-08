import { describe, expect, it } from 'bun:test';
import {
  DEFAULT_MAX_RECORD_BYTES,
  DEFAULT_PROGRESS_INTERVAL_MS,
  resolveStatusStoreConfig,
} from './types.js';

describe('resolveStatusStoreConfig', () => {
  it('uses the defaults when no options are given', () => {
    expect(resolveStatusStoreConfig()).toEqual({
      maxRecordBytes: 65_536,
      progressIntervalMs: 1_000,
    });
    expect(DEFAULT_MAX_RECORD_BYTES).toBe(65_536);
    expect(DEFAULT_PROGRESS_INTERVAL_MS).toBe(1_000);
  });

  it('keeps the values that the caller gives', () => {
    expect(
      resolveStatusStoreConfig({ maxRecordBytes: 1024, progressIntervalMs: 0 }),
    ).toEqual({ maxRecordBytes: 1024, progressIntervalMs: 0 });
  });

  it('fills only the missing values', () => {
    expect(resolveStatusStoreConfig({ progressIntervalMs: 250 })).toEqual({
      maxRecordBytes: 65_536,
      progressIntervalMs: 250,
    });
  });
});
