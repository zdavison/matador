import { describe, expect, it } from 'bun:test';
import {
  MatadorError,
  RecordTooLargeError,
  ResultNotSerializableError,
  StatusStoreError,
  StatusStoreRequiredError,
} from './index.js';

describe('status errors', () => {
  it('StatusStoreRequiredError names the context', () => {
    const error = new StatusStoreRequiredError(
      "subscriber 'ocr' has track: true",
    );
    expect(error).toBeInstanceOf(MatadorError);
    expect(error.name).toBe('StatusStoreRequiredError');
    expect(error.message).toContain("subscriber 'ocr' has track: true");
    expect(error.description).toContain('ACTION:');
  });

  it('RecordTooLargeError gives the size and the limit', () => {
    const error = new RecordTooLargeError('t-1', 'ocr', 70_000, 65_536);
    expect(error.name).toBe('RecordTooLargeError');
    expect(error.trackingId).toBe('t-1');
    expect(error.subscriberName).toBe('ocr');
    expect(error.sizeBytes).toBe(70_000);
    expect(error.maxBytes).toBe(65_536);
    expect(error.message).toContain('70000');
    expect(error.message).toContain('65536');
    expect(error.description).toContain('blob storage');
  });

  it('ResultNotSerializableError wraps the cause', () => {
    const cause = new TypeError('Do not know how to serialize a BigInt');
    const error = new ResultNotSerializableError('ocr', cause);
    expect(error.name).toBe('ResultNotSerializableError');
    expect(error.cause).toBe(cause);
    expect(error.message).toContain('ocr');
    expect(error.message).toContain('BigInt');
  });

  it('StatusStoreError gives the operation and wraps the cause', () => {
    const cause = new Error('ECONNREFUSED');
    const error = new StatusStoreError('create', 't-1', cause);
    expect(error.name).toBe('StatusStoreError');
    expect(error.operation).toBe('create');
    expect(error.trackingId).toBe('t-1');
    expect(error.cause).toBe(cause);
    expect(error.message).toContain('ECONNREFUSED');
  });
});
