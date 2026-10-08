import { MatadorError } from './matador-errors.js';

/**
 * Status store operations, as reported in errors and in the onStatusStoreError hook.
 */
export type StatusStoreOperation = 'create' | 'update' | 'progress' | 'read';

/**
 * Thrown when status tracking is used but Matador has no statusStore.
 */
export class StatusStoreRequiredError extends MatadorError {
  readonly description =
    'A subscriber has track: true, or getStatus() was called, but Matador has no statusStore. ' +
    'ACTION: Pass a statusStore in the Matador config, for example MemoryStatusStore in tests ' +
    'or RedisStatusStore from @zdavison/matador-redis. Each process that sends to or consumes ' +
    'a tracked subscriber needs a store that points to the same data.';

  constructor(public readonly context: string) {
    super(`A status store is required: ${context}.`);
  }
}

/**
 * Thrown when a status record is larger than the store's maxRecordBytes.
 */
export class RecordTooLargeError extends MatadorError {
  readonly description =
    'A status record is larger than the maxRecordBytes limit of the status store. ' +
    'The record holds the whole envelope, the progress details and the result. ' +
    'ACTION: Do not put large data, for example files or documents, in the event payload, ' +
    'the progress details or the subscriber return value. Write the data to blob storage, ' +
    'for example S3, and put a pointer, for example the object key, in the payload or the result.';

  constructor(
    public readonly trackingId: string,
    public readonly subscriberName: string,
    public readonly sizeBytes: number,
    public readonly maxBytes: number,
  ) {
    super(
      `The status record for tracking ID "${trackingId}" and subscriber "${subscriberName}" is ${sizeBytes} bytes. The limit is ${maxBytes} bytes.`,
    );
  }
}

/**
 * Thrown when a tracked subscriber returns a value that JSON cannot hold.
 */
export class ResultNotSerializableError extends MatadorError {
  readonly description =
    'A tracked subscriber returned a value that JSON.stringify cannot serialize, ' +
    'for example a BigInt, a function or an object with a circular reference. ' +
    'ACTION: Return a JSON-serializable value from the subscriber callback, or return nothing.';

  constructor(
    public readonly subscriberName: string,
    public readonly cause: Error,
  ) {
    super(
      `The result of subscriber "${subscriberName}" is not JSON-serializable: ${cause.message}`,
    );
  }
}

/**
 * Thrown when a status store operation fails.
 */
export class StatusStoreError extends MatadorError {
  readonly description =
    'A status store operation failed. ' +
    'ACTION: Check the storage system behind the status store, for example Redis, for connectivity issues.';

  constructor(
    public readonly operation: StatusStoreOperation,
    public readonly trackingId: string,
    public readonly cause: Error,
  ) {
    super(
      `Status store ${operation} failed for tracking ID "${trackingId}": ${cause.message}`,
    );
  }
}
