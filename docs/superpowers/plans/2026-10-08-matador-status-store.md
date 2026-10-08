# Matador Status Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional, pluggable status store to Matador, so that an app can poll the status, progress and result of a message that it sent.

**Architecture:** A new `status/` module in core holds the record types, the `StatusStore` interface, a `MemoryStatusStore`, a `ProgressReporter` and a `StatusTracker`. Matador makes a `StatusTracker` only if `statusStore` is set. `FanoutEngine` and `ProcessingPipeline` call the tracker at each lifecycle step. A new package, `@zdavison/matador-redis`, gives `RedisStatusStore`. `matador-nest` passes the store through and exposes `getStatus()`.

**Tech Stack:** TypeScript 5 (strict, `exactOptionalPropertyTypes`, `noUncheckedIndexedAccess`), Bun test runner, Biome, tsup, Testcontainers, `redis` v4 (`node-redis`) for tests only.

**Spec:** `docs/superpowers/specs/2026-10-08-matador-status-store-design.md`. Read it before you start. The plan argues from the spec.

## Global Constraints

- If no `statusStore` is configured, Matador behaves as in v4.2.0. No tracked code path runs.
- The store does not depend on a transport.
- Matador does not read `envelope.data` or `docket.metadata` for logic.
- `maxRecordBytes` default: `65536` (64 KB), measured as the UTF-8 byte length of `JSON.stringify(record)`.
- `progressIntervalMs` default: `1000`.
- The settings belong to the store (`store.config`). `MatadorConfig` gets only `statusStore`.
- The store interface has plain `get`/`set`. No atomic operations in v1. No concurrency keys in v1.
- Core Matador has no Redis dependency.
- All new errors extend `MatadorError` and have a `description` that ends with an `ACTION:` sentence.
- ESM imports use `.js` extensions. No `any`. Biome style: 2-space indent, single quotes, semicolons.
- `@zdavison/matador-nest` and `@zdavison/matador-redis` import `@zdavison/matador` from its built `dist`. After a core change, run `bun run --filter '@zdavison/matador' build` before you test those packages.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

These conditions are implied by the spec, but no spec test covers them. Each one has a test in the task that owns the code.

1. **A retry writes `retrying` before the redelivery writes `active`.** With `LocalTransport` and a short retry delay, the next delivery can start at once. If `retrying` lands after `active`, the record shows `retrying` while the job runs. Test: Task 7, "writes retrying before the next delivery writes active".
2. **A buffered send that Matador drops later becomes `failed`.** A send that the retry buffer drops after `maxRetryAttempts` must not stay `queued` forever. Test: Task 6, "marks a buffered send failed when the buffer drops it".
3. **`RedisStatusStore` with `ttlMs` of 0, a negative value or a fraction.** `PEXPIRE key 0` deletes the key at once, so every record disappears. The constructor must throw a `RangeError`. Test: Task 9, "rejects a ttlMs that is not a positive integer".
4. **`ctx.progress(NaN)` or `ctx.progress(Infinity)`.** Clamping does not fix `NaN`. Matador ignores the call and logs a warning. Test: Task 4, "ignores a percent that is not a finite number".
5. **A callback that returns a value that JSON cannot hold.** A `BigInt` makes `JSON.stringify` throw. A function serializes to `undefined`. Both must give `failed` with `ResultNotSerializableError`, not a crash or an empty `completed`. Test: Task 5, "fails with ResultNotSerializableError for a BigInt or a function".

---

## File Structure

**Core (`packages/matador`)**

| File | Responsibility |
|---|---|
| Create `src/status/types.ts` | `TrackingStatus`, `StatusRecord`, `StatusStore`, `StatusStoreConfig`, defaults, `resolveStatusStoreConfig` |
| Create `src/status/progress.ts` | `ProgressReporter`: progress coalescing for one execution |
| Create `src/status/tracker.ts` | `StatusTracker`: the only unit that writes records |
| Create `src/status/stores/memory.ts` | `MemoryStatusStore` |
| Create `src/status/stores/status-store-contract.ts` | Shared contract test suite for any `StatusStore` |
| Create `src/status/index.ts` | Status module exports |
| Create `src/errors/status-errors.ts` | `StatusStoreRequiredError`, `RecordTooLargeError`, `ResultNotSerializableError`, `StatusStoreError` |
| Modify `src/errors/index.ts` | Export the status errors |
| Modify `src/types/envelope.ts` | `docket.trackingId` |
| Modify `src/types/event.ts` | `EventOptions.trackingId` |
| Modify `src/types/subscriber.ts` | `track` option, `CallbackContext.progress` |
| Modify `src/schema/registry.ts` | Copy `track` into `SubscriberDefinition` |
| Modify `src/codec/rabbitmq-codec.ts` | `x-matador-tracking-id` header |
| Modify `src/hooks/types.ts`, `src/hooks/safe-hooks.ts`, `src/hooks/index.ts` | `onStatusStoreError` hook |
| Modify `src/core/fanout.ts` | Tracking ID, `queued` records, publish failures, `SendResult.trackingId` |
| Modify `src/pipeline/pipeline.ts` | `active`, `completed`, `retrying`, `failed`, `ctx.progress` |
| Modify `src/core/matador.ts` | `statusStore` config, registration check, `getStatus()` |
| Modify `src/index.ts` | Public exports |
| Modify `tsconfig.json` | Exclude `*-contract.ts` from the build |
| Modify `README.md` (repo root) | "Status tracking" section |

**NestJS (`packages/matador-nest`)**: modify `src/discovery/subscriber-discovery.service.ts` and `src/services/matador.service.ts`.

**Redis (`packages/matador-redis`, new)**: `package.json`, `tsconfig.json`, `tsup.config.ts`, `README.md`, `src/index.ts`, `src/redis-status-store.ts`, `src/redis-status-store.test.ts`, `test/redis-status-store.integration.test.ts`. Modify the root `package.json` build script.

---

### Task 1: Status types, store config and errors

**Files:**
- Create: `packages/matador/src/status/types.ts`
- Create: `packages/matador/src/errors/status-errors.ts`
- Modify: `packages/matador/src/errors/index.ts`
- Test: `packages/matador/src/status/types.test.ts`
- Test: `packages/matador/src/errors/status-errors.test.ts`

**Interfaces:**
- Consumes: `JsonSerializable` from `src/checkpoint/index.js`, `Envelope` from `src/types/index.js`, `MatadorError` from `src/errors/matador-errors.js`.
- Produces:
  - `type TrackingStatus = 'queued' | 'active' | 'retrying' | 'completed' | 'failed'`
  - `interface StatusProgress { percent: number; details?: JsonSerializable | undefined }`
  - `interface StatusRecordError { name: string; message: string }`
  - `interface StatusRecord { status; progress?; result?; error?; envelope: Envelope; updatedAt: string }`
  - `interface StatusStoreConfig { maxRecordBytes: number; progressIntervalMs: number }`
  - `interface StatusStore { config; getAll(trackingId); get(trackingId, subscriberName); set(trackingId, subscriberName, record); delete(trackingId); cleanup?(olderThan) }`
  - `DEFAULT_MAX_RECORD_BYTES = 65_536`, `DEFAULT_PROGRESS_INTERVAL_MS = 1_000`
  - `resolveStatusStoreConfig(options?: Partial<StatusStoreConfig>): StatusStoreConfig`
  - `StatusStoreRequiredError(context: string)`
  - `RecordTooLargeError(trackingId, subscriberName, sizeBytes, maxBytes)`
  - `ResultNotSerializableError(subscriberName, cause: Error)`
  - `StatusStoreError(operation: StatusStoreOperation, trackingId, cause: Error)`
  - `type StatusStoreOperation = 'create' | 'update' | 'progress' | 'read'`

- [ ] **Step 1: Write the failing tests**

`packages/matador/src/status/types.test.ts`:

```ts
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
```

`packages/matador/src/errors/status-errors.test.ts`:

```ts
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
    const error = new StatusStoreRequiredError("subscriber 'ocr' has track: true");
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
```

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `cd packages/matador && bun test src/status/types.test.ts src/errors/status-errors.test.ts`
Expected: FAIL. The modules `./types.js` and the new error exports do not exist.

- [ ] **Step 3: Write the types**

`packages/matador/src/status/types.ts`:

```ts
import type { JsonSerializable } from '../checkpoint/index.js';
import type { Envelope } from '../types/index.js';

/**
 * Lifecycle status of one tracked subscriber for one send.
 * 'completed' and 'failed' are terminal.
 */
export type TrackingStatus =
  | 'queued'
  | 'active'
  | 'retrying'
  | 'completed'
  | 'failed';

/**
 * Progress reported by a subscriber with ctx.progress().
 */
export interface StatusProgress {
  /** A number from 0 to 100 */
  readonly percent: number;
  /** Optional small JSON value, for example counters */
  readonly details?: JsonSerializable | undefined;
}

/**
 * Error summary kept in a status record. Has no stack trace.
 */
export interface StatusRecordError {
  readonly name: string;
  readonly message: string;
}

/**
 * Status of one tracked subscriber for one send.
 *
 * The record does not copy values that the envelope holds:
 * the tracking ID is docket.trackingId, the subscriber is docket.targetSubscriber,
 * the creation time is docket.createdAt and the attempt count is docket.attempts.
 */
export interface StatusRecord {
  readonly status: TrackingStatus;
  readonly progress?: StatusProgress | undefined;
  /** Set only when status is 'completed'. */
  readonly result?: JsonSerializable | undefined;
  /** Set when status is 'retrying' or 'failed'. Covers errors that are not in the docket. */
  readonly error?: StatusRecordError | undefined;
  /** The latest envelope, including data and docket. */
  readonly envelope: Envelope;
  /** ISO 8601 */
  readonly updatedAt: string;
}

/**
 * Settings that each store implements. Stores take these in their constructor options.
 * The tracker enforces them, so no store reimplements the size check or the coalescing.
 */
export interface StatusStoreConfig {
  /** Limit on the JSON-serialized record, in bytes. */
  readonly maxRecordBytes: number;
  /** Minimum time between two progress writes for one record, in milliseconds. */
  readonly progressIntervalMs: number;
}

export const DEFAULT_MAX_RECORD_BYTES = 65_536;
export const DEFAULT_PROGRESS_INTERVAL_MS = 1_000;

/**
 * Fills the defaults. Each store calls this in its constructor.
 */
export function resolveStatusStoreConfig(
  options: Partial<StatusStoreConfig> = {},
): StatusStoreConfig {
  return {
    maxRecordBytes: options.maxRecordBytes ?? DEFAULT_MAX_RECORD_BYTES,
    progressIntervalMs:
      options.progressIntervalMs ?? DEFAULT_PROGRESS_INTERVAL_MS,
  };
}

/**
 * Persistence for status records, keyed by tracking ID and subscriber name.
 * Writes are plain replacements. v1 has no atomic operations.
 */
export interface StatusStore {
  /** The resolved settings. The tracker reads these values. */
  readonly config: StatusStoreConfig;

  /** Returns all records for one tracking ID, keyed by subscriber name. */
  getAll(
    trackingId: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | undefined>;

  /** Returns one record. */
  get(
    trackingId: string,
    subscriberName: string,
  ): Promise<StatusRecord | undefined>;

  /** Writes the record. Replaces any record with the same tracking ID and subscriber name. */
  set(
    trackingId: string,
    subscriberName: string,
    record: StatusRecord,
  ): Promise<void>;

  /** Deletes all records for one tracking ID. */
  delete(trackingId: string): Promise<void>;

  /** Optional: deletes records with updatedAt before olderThan. Returns the count. */
  cleanup?(olderThan: Date): Promise<number>;
}
```

- [ ] **Step 4: Write the errors**

`packages/matador/src/errors/status-errors.ts`:

```ts
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
```

- [ ] **Step 5: Export the errors**

Append to `packages/matador/src/errors/index.ts`:

```ts
export type { StatusStoreOperation } from './status-errors.js';
export {
  // Status errors
  RecordTooLargeError,
  ResultNotSerializableError,
  StatusStoreError,
  StatusStoreRequiredError,
} from './status-errors.js';
```

- [ ] **Step 6: Run the tests to make sure they pass**

Run: `cd packages/matador && bun test src/status/types.test.ts src/errors/status-errors.test.ts && bun run typecheck`
Expected: PASS, and no type errors.

- [ ] **Step 7: Commit**

```bash
git add packages/matador/src/status/types.ts packages/matador/src/status/types.test.ts packages/matador/src/errors/status-errors.ts packages/matador/src/errors/status-errors.test.ts packages/matador/src/errors/index.ts
git commit -m "feat(status): add status record types, store config and errors

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: MemoryStatusStore and the shared store contract

**Files:**
- Create: `packages/matador/src/status/stores/status-store-contract.ts`
- Create: `packages/matador/src/status/stores/memory.ts`
- Modify: `packages/matador/tsconfig.json`
- Test: `packages/matador/src/status/stores/memory.test.ts`

**Interfaces:**
- Consumes: `StatusRecord`, `StatusStore`, `StatusStoreConfig`, `resolveStatusStoreConfig` from Task 1. `createEnvelope` from `src/types/index.js`.
- Produces:
  - `describeStatusStoreContract(name: string, createStore: () => StatusStore | Promise<StatusStore>): void`. Task 9 imports it by relative path.
  - `class MemoryStatusStore implements StatusStore`, `constructor(options?: Partial<StatusStoreConfig>)`, with `cleanup(olderThan: Date): Promise<number>`.

The contract file imports `bun:test`. It must not go into the build. So exclude it in `tsconfig.json`. `bun test` does not run it directly, because its name does not end in `.test.ts`.

- [ ] **Step 1: Exclude contract files from the build**

In `packages/matador/tsconfig.json`, change the `exclude` line to:

```json
  "exclude": ["node_modules", "dist", "test", "**/*.test.ts", "**/*-contract.ts"]
```

- [ ] **Step 2: Write the contract suite**

`packages/matador/src/status/stores/status-store-contract.ts`:

```ts
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
      expect(await store.getAll(trackingId)).toEqual({ 'sub-a': a, 'sub-b': b });
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
```

- [ ] **Step 3: Write the failing test**

`packages/matador/src/status/stores/memory.test.ts`:

```ts
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
    expect(store.config).toEqual({ maxRecordBytes: 2048, progressIntervalMs: 1_000 });
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
```

- [ ] **Step 4: Run the test to make sure it fails**

Run: `cd packages/matador && bun test src/status/stores/memory.test.ts`
Expected: FAIL. `./memory.js` does not exist.

- [ ] **Step 5: Write the memory store**

`packages/matador/src/status/stores/memory.ts`:

```ts
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
```

- [ ] **Step 6: Run the tests to make sure they pass**

Run: `cd packages/matador && bun test src/status/stores/memory.test.ts && bun run typecheck`
Expected: PASS. The contract suite shows 7 tests, and the `MemoryStatusStore` block shows 3 tests.

- [ ] **Step 7: Commit**

```bash
git add packages/matador/tsconfig.json packages/matador/src/status/stores
git commit -m "feat(status): add MemoryStatusStore and the shared store contract suite

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Tracking ID and `track` plumbing

**Files:**
- Modify: `packages/matador/src/types/envelope.ts` (`Docket`, `DocketCreateFields`, `createEnvelope`)
- Modify: `packages/matador/src/types/event.ts` (`EventOptions`)
- Modify: `packages/matador/src/types/subscriber.ts` (`BaseSubscriberOptions`, create inputs, `createSubscriber`, `createSubscriberStub`, `SubscriberDefinition`)
- Modify: `packages/matador/src/schema/registry.ts:114-135` (`getSubscriberDefinition`)
- Modify: `packages/matador/src/codec/rabbitmq-codec.ts` (header map, `encode`, `decodeV2`)
- Test: `packages/matador/src/status/plumbing.test.ts`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `Docket.trackingId?: string | undefined`
  - `CreateEnvelopeOptions.trackingId?: string | undefined`
  - `EventOptions.trackingId?: string | undefined`
  - `BaseSubscriberOptions.track?: boolean | undefined` (so `Subscriber` and `SubscriberStub` have it)
  - `SubscriberDefinition.track?: boolean | undefined`
  - RabbitMQ header `x-matador-tracking-id`

- [ ] **Step 1: Write the failing tests**

`packages/matador/src/status/plumbing.test.ts`:

```ts
import { describe, expect, it } from 'bun:test';
import { JsonCodec, RabbitMQCodec } from '../codec/index.js';
import { SchemaRegistry } from '../schema/index.js';
import {
  MatadorEvent,
  createEnvelope,
  createSubscriber,
  createSubscriberStub,
} from '../types/index.js';

class JobEvent extends MatadorEvent {
  static readonly key = 'job.requested';
  static readonly description = 'A job was requested';

  constructor(public data: { userId: string }) {
    super();
  }
}

function trackedEnvelope(trackingId?: string) {
  return createEnvelope({
    eventKey: JobEvent.key,
    targetSubscriber: 'job-runner',
    importance: 'should-investigate',
    data: { userId: 'u-1' },
    trackingId,
  });
}

describe('tracking plumbing', () => {
  it('createEnvelope sets docket.trackingId only when given', () => {
    expect(trackedEnvelope('t-1').docket.trackingId).toBe('t-1');
    expect('trackingId' in trackedEnvelope().docket).toBe(false);
  });

  it('RabbitMQCodec keeps the tracking ID through encode and decode', () => {
    const codec = new RabbitMQCodec();
    const encoded = codec.encode(trackedEnvelope('t-1'));
    expect(encoded.headers['x-matador-tracking-id']).toBe('t-1');
    const decoded = codec.decode(encoded.body, encoded.headers);
    expect(decoded.docket.trackingId).toBe('t-1');
  });

  it('RabbitMQCodec sends no tracking header for an untracked envelope', () => {
    const codec = new RabbitMQCodec();
    const encoded = codec.encode(trackedEnvelope());
    expect('x-matador-tracking-id' in encoded.headers).toBe(false);
    const decoded = codec.decode(encoded.body, encoded.headers);
    expect(decoded.docket.trackingId).toBeUndefined();
  });

  it('JsonCodec keeps the tracking ID through encode and decode', () => {
    const codec = new JsonCodec();
    const decoded = codec.decode(codec.encode(trackedEnvelope('t-1')));
    expect(decoded.docket.trackingId).toBe('t-1');
  });

  it('createSubscriber and createSubscriberStub keep track', () => {
    const tracked = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      track: true,
      callback: async () => {},
    });
    const untracked = createSubscriber<JobEvent>({
      name: 'audit',
      description: 'Audits the job',
      callback: async () => {},
    });
    const stub = createSubscriberStub({ name: 'remote-runner', track: true });

    expect(tracked.track).toBe(true);
    expect('track' in untracked).toBe(false);
    expect(stub.track).toBe(true);
  });

  it('SchemaRegistry copies track into the subscriber definition', () => {
    const registry = new SchemaRegistry();
    registry.register(JobEvent, [
      createSubscriber<JobEvent>({
        name: 'job-runner',
        description: 'Runs the job',
        track: true,
        callback: async () => {},
      }),
    ]);
    expect(
      registry.getSubscriberDefinition(JobEvent.key, 'job-runner')?.track,
    ).toBe(true);
  });
});
```

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `cd packages/matador && bun test src/status/plumbing.test.ts`
Expected: FAIL. `trackingId` and `track` are not set, and TypeScript in the test reports unknown properties.

- [ ] **Step 3: Add `trackingId` to the envelope**

In `packages/matador/src/types/envelope.ts`, add this field to `Docket`, after `correlationId`:

```ts
  /**
   * Tracking ID for status records. Set only on envelopes for subscribers with track: true.
   * One send has one tracking ID for all of its tracked subscribers.
   */
  readonly trackingId?: string | undefined;
```

Add `'trackingId'` to `DocketCreateFields`:

```ts
type DocketCreateFields = Pick<
  Docket,
  | 'eventKey'
  | 'eventDescription'
  | 'targetSubscriber'
  | 'importance'
  | 'correlationId'
  | 'trackingId'
>;
```

In `createEnvelope`, after the `correlationId` spread, add:

```ts
      ...(options.trackingId !== undefined && {
        trackingId: options.trackingId,
      }),
```

- [ ] **Step 4: Add `trackingId` to the send options**

In `packages/matador/src/types/event.ts`, add this field to `EventOptions`, after `correlationId`:

```ts
  /**
   * ID for the status records of this send.
   * Used only if the event has a subscriber with track: true.
   * If not set, Matador makes a UUID v4. The caller makes sure that a given ID is unique.
   */
  readonly trackingId?: string | undefined;
```

- [ ] **Step 5: Add `track` to subscribers**

In `packages/matador/src/types/subscriber.ts`:

Add to `BaseSubscriberOptions`, after `enabled`:

```ts
  /**
   * If true, Matador keeps a status record for each send to this subscriber.
   * Requires a statusStore in the Matador config.
   */
  readonly track?: boolean | undefined;
```

Add the same field (`readonly track?: boolean | undefined;`) to `CreateStandardSubscriberInput`, `CreateResumableSubscriberInput` and `CreateSubscriberStubInput`, after `enabled`.

In `createSubscriber`, add to the `base` object, after the `enabled` spread:

```ts
    ...(input.track !== undefined && { track: input.track }),
```

In `createSubscriberStub`, add to the returned object, after the `enabled` spread:

```ts
    ...(input.track !== undefined && { track: input.track }),
```

Add to `SubscriberDefinition`:

```ts
  readonly track?: boolean | undefined;
```

- [ ] **Step 6: Copy `track` in the registry**

In `packages/matador/src/schema/registry.ts`, in `getSubscriberDefinition`, add after the `targetQueue` block:

```ts
    if (subscriber.track !== undefined) {
      (def as { track?: boolean }).track = subscriber.track;
    }
```

- [ ] **Step 7: Add the RabbitMQ header**

In `packages/matador/src/codec/rabbitmq-codec.ts`:

Add to the `HEADERS` object, after `CORRELATION_ID`:

```ts
  TRACKING_ID: 'x-matador-tracking-id',
```

In `encode`, after the `correlationId` block:

```ts
    if (docket.trackingId !== undefined) {
      headers[HEADERS.TRACKING_ID] = docket.trackingId;
    }
```

In `decodeV2`, after `const correlationId = ...`:

```ts
    const trackingId = this.optionalStringHeader(headers, HEADERS.TRACKING_ID);
```

and in the `docket` object, after the `correlationId` spread:

```ts
      ...(trackingId !== undefined && { trackingId }),
```

- [ ] **Step 8: Run the tests to make sure they pass**

Run: `cd packages/matador && bun test src/status/plumbing.test.ts src/codec src/schema src/types && bun run typecheck`
Expected: PASS. The existing codec, schema and type tests also pass.

- [ ] **Step 9: Commit**

```bash
git add packages/matador/src/types packages/matador/src/schema/registry.ts packages/matador/src/codec/rabbitmq-codec.ts packages/matador/src/status/plumbing.test.ts
git commit -m "feat(status): add docket.trackingId, the trackingId send option and subscriber track

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: ProgressReporter and `ctx.progress`

**Files:**
- Create: `packages/matador/src/status/progress.ts`
- Modify: `packages/matador/src/types/subscriber.ts` (`CallbackContext`)
- Modify: `packages/matador/src/pipeline/pipeline.ts:211-231` (both callback contexts get a `progress` that does nothing; Task 7 connects it)
- Test: `packages/matador/src/status/progress.test.ts`

**Interfaces:**
- Consumes: `StatusProgress` from Task 1, `JsonSerializable` from `src/checkpoint/index.js`.
- Produces:
  - `interface ProgressScheduler { now(): number; schedule(fn: () => void, delayMs: number): () => void }`
  - `const realScheduler: ProgressScheduler`
  - `class ProgressReporter`:
    - `constructor(write: (progress: StatusProgress) => Promise<void>, intervalMs: number, onInvalid: (percent: number) => void, scheduler?: ProgressScheduler)`
    - `report(percent: number, details?: JsonSerializable): void`
    - `stop(): Promise<void>`
    - `get current(): StatusProgress | undefined`
  - `CallbackContext.progress(percent: number, details?: JsonSerializable): void`

The reporter writes the first value at once. After that, it writes the latest value at most once in each `intervalMs`, with only one write in flight. `stop()` drops the pending value, cancels the timer and waits for the write in flight.

- [ ] **Step 1: Write the failing tests**

`packages/matador/src/status/progress.test.ts`:

```ts
import { describe, expect, it } from 'bun:test';
import { ProgressReporter, type ProgressScheduler } from './progress.js';
import type { StatusProgress } from './types.js';

class ManualScheduler implements ProgressScheduler {
  time = 0;
  private tasks: { at: number; fn: () => void; cancelled: boolean }[] = [];

  now(): number {
    return this.time;
  }

  schedule(fn: () => void, delayMs: number): () => void {
    const task = { at: this.time + delayMs, fn, cancelled: false };
    this.tasks.push(task);
    return () => {
      task.cancelled = true;
    };
  }

  advance(ms: number): void {
    this.time += ms;
    const due = this.tasks.filter((t) => !t.cancelled && t.at <= this.time);
    this.tasks = this.tasks.filter((t) => !due.includes(t));
    for (const task of due) task.fn();
  }
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

function setup(intervalMs = 1000) {
  const scheduler = new ManualScheduler();
  const writes: StatusProgress[] = [];
  const invalid: number[] = [];
  const reporter = new ProgressReporter(
    async (progress) => {
      writes.push(progress);
    },
    intervalMs,
    (percent) => invalid.push(percent),
    scheduler,
  );
  return { scheduler, writes, invalid, reporter };
}

describe('ProgressReporter', () => {
  it('writes the first report at once', async () => {
    const { writes, reporter } = setup();
    reporter.report(10);
    await settle();
    expect(writes).toEqual([{ percent: 10 }]);
  });

  it('coalesces reports inside the interval and writes the latest value', async () => {
    const { scheduler, writes, reporter } = setup();
    reporter.report(10);
    reporter.report(20);
    reporter.report(30, { done: 3 });
    await settle();
    expect(writes).toEqual([{ percent: 10 }]);

    scheduler.advance(1000);
    await settle();
    expect(writes).toEqual([{ percent: 10 }, { percent: 30, details: { done: 3 } }]);
  });

  it('keeps only one write in flight', async () => {
    const scheduler = new ManualScheduler();
    const writes: StatusProgress[] = [];
    let release: () => void = () => {};
    const reporter = new ProgressReporter(
      (progress) => {
        writes.push(progress);
        return new Promise<void>((resolve) => {
          release = resolve;
        });
      },
      0,
      () => {},
      scheduler,
    );

    reporter.report(10);
    reporter.report(20);
    reporter.report(30);
    await settle();
    expect(writes).toEqual([{ percent: 10 }]);

    release();
    await settle();
    expect(writes).toEqual([{ percent: 10 }, { percent: 30 }]);
  });

  it('stop drops the pending value and waits for the write in flight', async () => {
    const scheduler = new ManualScheduler();
    const writes: StatusProgress[] = [];
    let release: () => void = () => {};
    const reporter = new ProgressReporter(
      (progress) => {
        writes.push(progress);
        return new Promise<void>((resolve) => {
          release = resolve;
        });
      },
      1000,
      () => {},
      scheduler,
    );

    reporter.report(10);
    reporter.report(50);
    let stopped = false;
    const stopping = reporter.stop().then(() => {
      stopped = true;
    });
    await settle();
    expect(stopped).toBe(false);

    release();
    await stopping;
    scheduler.advance(5000);
    await settle();
    expect(writes).toEqual([{ percent: 10 }]);
    expect(reporter.current).toEqual({ percent: 50 });
  });

  it('ignores reports after stop', async () => {
    const { writes, reporter } = setup();
    await reporter.stop();
    reporter.report(10);
    await settle();
    expect(writes).toEqual([]);
    expect(reporter.current).toBeUndefined();
  });

  it('clamps percent to the range 0 to 100', async () => {
    const { scheduler, writes, reporter } = setup(0);
    reporter.report(150);
    await settle();
    scheduler.advance(0);
    reporter.report(-5);
    await settle();
    expect(writes).toEqual([{ percent: 100 }, { percent: 0 }]);
  });

  it('ignores a percent that is not a finite number', async () => {
    const { writes, invalid, reporter } = setup();
    reporter.report(Number.NaN);
    reporter.report(Number.POSITIVE_INFINITY);
    await settle();
    expect(writes).toEqual([]);
    expect(invalid).toEqual([Number.NaN, Number.POSITIVE_INFINITY]);
    expect(reporter.current).toBeUndefined();
  });

  it('does not stop on a write that rejects', async () => {
    const scheduler = new ManualScheduler();
    let calls = 0;
    const reporter = new ProgressReporter(
      async () => {
        calls++;
        throw new Error('store down');
      },
      0,
      () => {},
      scheduler,
    );
    reporter.report(10);
    await settle();
    reporter.report(20);
    await settle();
    expect(calls).toBe(2);
  });
});
```

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `cd packages/matador && bun test src/status/progress.test.ts`
Expected: FAIL. `./progress.js` does not exist.

- [ ] **Step 3: Write the reporter**

`packages/matador/src/status/progress.ts`:

```ts
import type { JsonSerializable } from '../checkpoint/index.js';
import type { StatusProgress } from './types.js';

/**
 * Time source and timer for ProgressReporter. Tests replace it.
 */
export interface ProgressScheduler {
  now(): number;
  /** Runs fn after delayMs. Returns a function that cancels the timer. */
  schedule(fn: () => void, delayMs: number): () => void;
}

export const realScheduler: ProgressScheduler = {
  now: () => Date.now(),
  schedule: (fn, delayMs) => {
    const timer = setTimeout(fn, delayMs);
    timer.unref?.();
    return () => clearTimeout(timer);
  },
};

/**
 * Coalesces progress reports for one execution of a tracked subscriber.
 *
 * - The first report is written at once.
 * - After that, the latest value is written at most once in each interval.
 * - Only one write is in flight at a time.
 * - stop() drops the pending value and waits for the write in flight.
 */
export class ProgressReporter {
  private latest: StatusProgress | undefined;
  private pending: StatusProgress | undefined;
  private inFlight: Promise<void> | undefined;
  private cancelTimer: (() => void) | undefined;
  private lastWriteAt = Number.NEGATIVE_INFINITY;
  private stopped = false;

  constructor(
    private readonly write: (progress: StatusProgress) => Promise<void>,
    private readonly intervalMs: number,
    private readonly onInvalid: (percent: number) => void,
    private readonly scheduler: ProgressScheduler = realScheduler,
  ) {}

  /** The latest reported value, written or not. */
  get current(): StatusProgress | undefined {
    return this.latest;
  }

  report(percent: number, details?: JsonSerializable): void {
    if (this.stopped) {
      return;
    }
    if (!Number.isFinite(percent)) {
      this.onInvalid(percent);
      return;
    }
    const clamped = Math.min(100, Math.max(0, percent));
    const progress: StatusProgress =
      details === undefined
        ? { percent: clamped }
        : { percent: clamped, details };
    this.latest = progress;
    this.pending = progress;
    this.flush();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.pending = undefined;
    this.cancelTimer?.();
    this.cancelTimer = undefined;
    await this.inFlight;
  }

  private flush(): void {
    if (
      this.stopped ||
      this.pending === undefined ||
      this.inFlight !== undefined ||
      this.cancelTimer !== undefined
    ) {
      return;
    }

    const waitMs = this.lastWriteAt + this.intervalMs - this.scheduler.now();
    if (waitMs > 0) {
      this.cancelTimer = this.scheduler.schedule(() => {
        this.cancelTimer = undefined;
        this.flush();
      }, waitMs);
      return;
    }

    const value = this.pending;
    this.pending = undefined;
    this.lastWriteAt = this.scheduler.now();
    this.inFlight = this.write(value)
      .catch(() => undefined)
      .finally(() => {
        this.inFlight = undefined;
        this.flush();
      });
  }
}
```

- [ ] **Step 4: Add `progress` to the callback context**

In `packages/matador/src/types/subscriber.ts`, add the import at the top:

```ts
import type { JsonSerializable, SubscriberContext } from '../checkpoint/index.js';
```

(replace the existing `import type { SubscriberContext } from '../checkpoint/index.js';`), and add to `CallbackContext`:

```ts
  /**
   * Report progress. Returns at once and does not wait for the store.
   * Does nothing if the subscriber does not have track: true.
   * @param percent - A number from 0 to 100. Values outside the range are clamped.
   * @param details - Optional small JSON value, for example counters.
   */
  progress(percent: number, details?: JsonSerializable): void;
```

In `packages/matador/src/pipeline/pipeline.ts`, in `process()`, change the callback context to:

```ts
    // Create base callback context with matador dispatcher
    const report = (_percent: number, _details?: JsonSerializable): void => {};
    const callbackContext: CallbackContext = {
      matador: this.dispatcher,
      progress: report,
    };
```

and add `progress: report,` to the `fullContext` object. Change the checkpoint import to:

```ts
import type {
  Checkpoint,
  CheckpointStore,
  JsonSerializable,
} from '../checkpoint/index.js';
```

- [ ] **Step 5: Run the tests to make sure they pass**

Run: `cd packages/matador && bun test src/status/progress.test.ts src/pipeline && bun run typecheck`
Expected: PASS. If `typecheck` reports other places that build a `CallbackContext` by hand, add `progress: () => {}` there.

- [ ] **Step 6: Commit**

```bash
git add packages/matador/src/status/progress.ts packages/matador/src/status/progress.test.ts packages/matador/src/types/subscriber.ts packages/matador/src/pipeline/pipeline.ts
git commit -m "feat(status): add ProgressReporter and ctx.progress

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: StatusTracker and the `onStatusStoreError` hook

**Files:**
- Create: `packages/matador/src/status/tracker.ts`
- Create: `packages/matador/src/status/index.ts`
- Modify: `packages/matador/src/hooks/types.ts`, `packages/matador/src/hooks/safe-hooks.ts`, `packages/matador/src/hooks/index.ts`
- Test: `packages/matador/src/status/tracker.test.ts`

**Interfaces:**
- Consumes: Task 1 types and errors, Task 2 `MemoryStatusStore`, Task 3 `docket.trackingId`, Task 4 `ProgressReporter`, `ProgressScheduler`, `realScheduler`. `SafeHooks` from `src/hooks/index.js`. `RetryDecision` from `src/retry/index.js`.
- Produces:
  - `isTrackedEnvelope(envelope: Envelope): boolean` (true if `docket.trackingId` is set)
  - `class StatusTracker`:
    - `constructor(store: StatusStore, hooks: SafeHooks, scheduler?: ProgressScheduler)`
    - `createQueued(envelopes: readonly Envelope[]): Promise<void>`. Throws `RecordTooLargeError` or `StatusStoreError`. Writes nothing if any record is too large.
    - `markPublishFailed(envelope: Envelope, error: Error): Promise<void>`. Never throws.
    - `markActive(envelope: Envelope): Promise<void>`. Never throws.
    - `startProgress(envelope: Envelope): ProgressReporter`
    - `markCompleted(envelope: Envelope, result: unknown, reporter?: ProgressReporter): Promise<void>`. Never throws.
    - `markFailure(envelope: Envelope, error: Error, decision: RetryDecision, reporter?: ProgressReporter): Promise<void>`. Never throws.
    - `read(trackingId: string): Promise<Readonly<Record<string, StatusRecord>> | undefined>` and `read(trackingId: string, subscriberName: string): Promise<StatusRecord | undefined>`. Throws `StatusStoreError`.
  - `interface StatusStoreErrorContext { trackingId: string; subscriberName?: string | undefined; operation: StatusStoreOperation; error: Error }`
  - `MatadorHooks.onStatusStoreError?(context: StatusStoreErrorContext): void | Promise<void>`
  - `SafeHooks.onStatusStoreError(context: StatusStoreErrorContext): Promise<void>`

`subscriberName` in the hook context is optional, because a `read` of all records has no subscriber name.

- [ ] **Step 1: Write the failing tests**

`packages/matador/src/status/tracker.test.ts`:

```ts
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

function setup(store: StatusStore = new MemoryStatusStore({ progressIntervalMs: 0 })) {
  const onStatusStoreError = mock((_context: StatusStoreErrorContext) => {});
  const warn = mock((_message: string, ..._args: unknown[]) => {});
  const hooks = new SafeHooks({
    logger: { ...silentLogger, warn },
    onStatusStoreError,
  });
  return { store, tracker: new StatusTracker(store, hooks), onStatusStoreError, warn };
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
      const { store, tracker } = setup(new MemoryStatusStore({ maxRecordBytes: 1024 }));
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
      await expect(tracker.createQueued([envelopeFor('a')])).rejects.toMatchObject({
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
      await tracker.markCompleted(envelopeFor('a'), { s3Key: 'exports/1.csv', rows: 3 });
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
      const { store, tracker } = setup(new MemoryStatusStore({ maxRecordBytes: 2048 }));
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
      await tracker.markFailure(envelopeFor('a', {}, 't-dl'), new Error('boom'), {
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: 'boom',
      });
      await tracker.markFailure(envelopeFor('a', {}, 't-dc'), new Error('gone'), {
        action: 'discard',
        reason: 'gone',
      });
      expect((await store.get('t-dl', 'a'))?.status).toBe('failed');
      expect((await store.get('t-dc', 'a'))?.status).toBe('failed');
      expect((await store.get('t-dc', 'a'))?.error?.message).toBe('gone');
    });

    it('markPublishFailed writes failed with the publish error', async () => {
      const { store, tracker } = setup();
      await tracker.markPublishFailed(envelopeFor('a'), new Error('broker down'));
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
      const store = new MemoryStatusStore({ maxRecordBytes: 2048, progressIntervalMs: 0 });
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
```

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `cd packages/matador && bun test src/status/tracker.test.ts`
Expected: FAIL. `./tracker.js` does not exist, and `SafeHooks` has no `onStatusStoreError`.

- [ ] **Step 3: Add the hook**

In `packages/matador/src/hooks/types.ts`, add the import:

```ts
import type { StatusStoreOperation } from '../errors/index.js';
```

Add the context type after `WorkerErrorContext`:

```ts
/**
 * Context for the status store error hook.
 */
export interface StatusStoreErrorContext {
  readonly trackingId: string;
  /** Not set for a read of all records of a tracking ID */
  readonly subscriberName?: string | undefined;
  readonly operation: StatusStoreOperation;
  readonly error: Error;
}
```

Add to `MatadorHooks`, after `onCheckpointCleared`:

```ts
  // === Status Hooks (Tracked Subscribers) ===

  /**
   * Called when a status store write or read fails, or when a record is too large.
   * Message handling does not change: the status record is for observation only.
   */
  onStatusStoreError?(context: StatusStoreErrorContext): void | Promise<void>;
```

In `packages/matador/src/hooks/safe-hooks.ts`, add `type StatusStoreErrorContext,` to the import from `./types.js`, and add this method after `onCheckpointCleared`:

```ts
  async onStatusStoreError(context: StatusStoreErrorContext): Promise<void> {
    await this.safeCall('onStatusStoreError', () =>
      this.hooks.onStatusStoreError?.(context),
    );
  }
```

In `packages/matador/src/hooks/index.ts`, add `StatusStoreErrorContext,` to the type export list.

- [ ] **Step 4: Write the tracker**

`packages/matador/src/status/tracker.ts`:

```ts
import type { JsonSerializable } from '../checkpoint/index.js';
import {
  RecordTooLargeError,
  ResultNotSerializableError,
  StatusStoreError,
} from '../errors/index.js';
import type { SafeHooks } from '../hooks/index.js';
import type { RetryDecision } from '../retry/index.js';
import type { Envelope } from '../types/index.js';
import {
  ProgressReporter,
  type ProgressScheduler,
  realScheduler,
} from './progress.js';
import type {
  StatusProgress,
  StatusRecord,
  StatusRecordError,
  StatusStore,
  TrackingStatus,
} from './types.js';

type RecordFields = {
  readonly progress?: StatusProgress | undefined;
  readonly result?: JsonSerializable | undefined;
  readonly error?: StatusRecordError | undefined;
};

/**
 * True if the envelope belongs to a tracked subscriber.
 * Fanout sets docket.trackingId only on those envelopes.
 */
export function isTrackedEnvelope(envelope: Envelope): boolean {
  return envelope.docket.trackingId !== undefined;
}

function trackingIdOf(envelope: Envelope): string {
  const trackingId = envelope.docket.trackingId;
  if (trackingId === undefined) {
    throw new Error(
      `Envelope ${envelope.id} has no tracking ID. Call isTrackedEnvelope() first.`,
    );
  }
  return trackingId;
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

function toRecordError(error: Error): StatusRecordError {
  return { name: error.name, message: error.message };
}

function recordSize(record: StatusRecord): number {
  return new TextEncoder().encode(JSON.stringify(record)).byteLength;
}

/**
 * Converts a callback return value to JSON.
 * Throws if JSON.stringify throws, or if the value has no JSON form.
 */
function toJsonResult(result: unknown): JsonSerializable | undefined {
  if (result === undefined) {
    return undefined;
  }
  const text = JSON.stringify(result);
  if (text === undefined) {
    throw new TypeError(`A value of type ${typeof result} has no JSON form`);
  }
  return JSON.parse(text) as JsonSerializable;
}

/**
 * The only unit that writes status records.
 * Matador makes a tracker only if a statusStore is configured.
 */
export class StatusTracker {
  constructor(
    private readonly store: StatusStore,
    private readonly hooks: SafeHooks,
    private readonly scheduler: ProgressScheduler = realScheduler,
  ) {}

  /**
   * Writes the 'queued' records before the publish.
   * Checks all sizes before the first write.
   * @throws RecordTooLargeError if any record is too large
   * @throws StatusStoreError if the store fails
   */
  async createQueued(envelopes: readonly Envelope[]): Promise<void> {
    const writes = envelopes.map((envelope) => {
      const trackingId = trackingIdOf(envelope);
      const subscriberName = envelope.docket.targetSubscriber;
      const record = this.buildRecord(envelope, 'queued', {});
      this.assertSize(trackingId, subscriberName, record);
      return { trackingId, subscriberName, record };
    });

    try {
      await Promise.all(
        writes.map((w) => this.store.set(w.trackingId, w.subscriberName, w.record)),
      );
    } catch (error) {
      const trackingId = writes[0]?.trackingId ?? 'unknown';
      throw new StatusStoreError('create', trackingId, toError(error));
    }
  }

  async markPublishFailed(envelope: Envelope, error: Error): Promise<void> {
    await this.write('update', envelope, 'failed', {
      error: toRecordError(error),
    });
  }

  async markActive(envelope: Envelope): Promise<void> {
    await this.write('update', envelope, 'active', {});
  }

  startProgress(envelope: Envelope): ProgressReporter {
    return new ProgressReporter(
      (progress) => this.write('progress', envelope, 'active', { progress }),
      this.store.config.progressIntervalMs,
      (percent) =>
        this.hooks.logger.warn(
          `[Matador] 🟡 Ignored progress value ${percent} for tracking ID '${envelope.docket.trackingId}'. The percent must be a finite number.`,
        ),
      this.scheduler,
    );
  }

  async markCompleted(
    envelope: Envelope,
    result: unknown,
    reporter?: ProgressReporter,
  ): Promise<void> {
    await reporter?.stop();
    const progress = reporter?.current;
    const trackingId = trackingIdOf(envelope);
    const subscriberName = envelope.docket.targetSubscriber;

    let json: JsonSerializable | undefined;
    try {
      json = toJsonResult(result);
    } catch (error) {
      const cause = new ResultNotSerializableError(subscriberName, toError(error));
      await this.write('update', envelope, 'failed', {
        progress,
        error: toRecordError(cause),
      });
      return;
    }

    const record = this.buildRecord(envelope, 'completed', { progress, result: json });
    const size = recordSize(record);
    const maxBytes = this.store.config.maxRecordBytes;
    if (size > maxBytes) {
      const cause = new RecordTooLargeError(trackingId, subscriberName, size, maxBytes);
      await this.write('update', envelope, 'failed', {
        progress,
        error: toRecordError(cause),
      });
      return;
    }

    await this.write('update', envelope, 'completed', { progress, result: json });
  }

  async markFailure(
    envelope: Envelope,
    error: Error,
    decision: RetryDecision,
    reporter?: ProgressReporter,
  ): Promise<void> {
    await reporter?.stop();
    const status: TrackingStatus =
      decision.action === 'retry' ? 'retrying' : 'failed';
    await this.write('update', envelope, status, {
      progress: reporter?.current,
      error: toRecordError(error),
    });
  }

  read(trackingId: string): Promise<Readonly<Record<string, StatusRecord>> | undefined>;
  read(trackingId: string, subscriberName: string): Promise<StatusRecord | undefined>;
  async read(
    trackingId: string,
    subscriberName?: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | StatusRecord | undefined> {
    try {
      return subscriberName === undefined
        ? await this.store.getAll(trackingId)
        : await this.store.get(trackingId, subscriberName);
    } catch (e) {
      const error = toError(e);
      await this.hooks.onStatusStoreError({
        trackingId,
        subscriberName,
        operation: 'read',
        error,
      });
      throw new StatusStoreError('read', trackingId, error);
    }
  }

  private buildRecord(
    envelope: Envelope,
    status: TrackingStatus,
    fields: RecordFields,
  ): StatusRecord {
    return {
      status,
      ...(fields.progress !== undefined && { progress: fields.progress }),
      ...(fields.result !== undefined && { result: fields.result }),
      ...(fields.error !== undefined && { error: fields.error }),
      envelope,
      updatedAt: new Date().toISOString(),
    };
  }

  private assertSize(
    trackingId: string,
    subscriberName: string,
    record: StatusRecord,
  ): void {
    const size = recordSize(record);
    const maxBytes = this.store.config.maxRecordBytes;
    if (size > maxBytes) {
      throw new RecordTooLargeError(trackingId, subscriberName, size, maxBytes);
    }
  }

  /**
   * Writes a record after the send. Never throws: a status store problem
   * must not change message handling.
   */
  private async write(
    operation: 'update' | 'progress',
    envelope: Envelope,
    status: TrackingStatus,
    fields: RecordFields,
  ): Promise<void> {
    const trackingId = trackingIdOf(envelope);
    const subscriberName = envelope.docket.targetSubscriber;
    const record = this.buildRecord(envelope, status, fields);

    const size = recordSize(record);
    const maxBytes = this.store.config.maxRecordBytes;
    if (size > maxBytes) {
      const error = new RecordTooLargeError(trackingId, subscriberName, size, maxBytes);
      if (operation === 'progress') {
        this.hooks.logger.warn(
          `[Matador] 🟡 Dropped a progress value for tracking ID '${trackingId}'. ${error.message}`,
        );
        return;
      }
      await this.hooks.onStatusStoreError({ trackingId, subscriberName, operation, error });
      return;
    }

    try {
      await this.store.set(trackingId, subscriberName, record);
    } catch (e) {
      const error = toError(e);
      this.hooks.logger.error(
        `[Matador] 🔴 Status store ${operation} failed for tracking ID '${trackingId}'.`,
        error,
      );
      await this.hooks.onStatusStoreError({ trackingId, subscriberName, operation, error });
    }
  }
}
```

- [ ] **Step 5: Write the module index**

`packages/matador/src/status/index.ts`:

```ts
export type {
  StatusProgress,
  StatusRecord,
  StatusRecordError,
  StatusStore,
  StatusStoreConfig,
  TrackingStatus,
} from './types.js';
export {
  DEFAULT_MAX_RECORD_BYTES,
  DEFAULT_PROGRESS_INTERVAL_MS,
  resolveStatusStoreConfig,
} from './types.js';
export type { ProgressScheduler } from './progress.js';
export { ProgressReporter, realScheduler } from './progress.js';
export { StatusTracker, isTrackedEnvelope } from './tracker.js';
export { MemoryStatusStore } from './stores/memory.js';
```

- [ ] **Step 6: Run the tests to make sure they pass**

Run: `cd packages/matador && bun test src/status && bun run typecheck`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/matador/src/status packages/matador/src/hooks
git commit -m "feat(status): add StatusTracker and the onStatusStoreError hook

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Matador config, fanout tracking and `getStatus`

**Files:**
- Modify: `packages/matador/src/core/fanout.ts` (`FanoutConfig`, `SendResult`, `send`, `rebufferUntried`, `handleFlushFailure`)
- Modify: `packages/matador/src/core/matador.ts` (`MatadorConfig`, constructor, `register`, `registerSchema`, new `getStatus`)
- Modify: `packages/matador/src/index.ts`
- Test: `packages/matador/src/core/status-tracking.test.ts`

**Interfaces:**
- Consumes: `StatusTracker`, `isTrackedEnvelope`, `MemoryStatusStore`, `StatusStore`, `StatusRecord` (Task 5). `StatusStoreRequiredError` (Task 1). `track` and `trackingId` (Task 3).
- Produces:
  - `FanoutConfig.statusTracker?: StatusTracker | undefined`
  - `SendResult.trackingId?: string | undefined`
  - `MatadorConfig.statusStore?: StatusStore | undefined`
  - `Matador.getStatus(trackingId: string): Promise<Readonly<Record<string, StatusRecord>> | undefined>`
  - `Matador.getStatus(trackingId: string, subscriberName: string): Promise<StatusRecord | undefined>`
  - Public exports from `@zdavison/matador`: the status types, `MemoryStatusStore`, `resolveStatusStoreConfig`, `DEFAULT_MAX_RECORD_BYTES`, `DEFAULT_PROGRESS_INTERVAL_MS`, the four status errors, `StatusStoreOperation`, `StatusStoreErrorContext`.
  - Task 7 uses the private field `Matador.statusTracker` to pass the tracker to the pipeline.

- [ ] **Step 1: Write the failing tests**

`packages/matador/src/core/status-tracking.test.ts`:

```ts
import { afterEach, describe, expect, it } from 'bun:test';
import {
  RecordTooLargeError,
  SomeSendError,
  StatusStoreRequiredError,
} from '../errors/index.js';
import type { Logger } from '../hooks/index.js';
import type { MatadorSchema } from '../schema/index.js';
import { MemoryStatusStore } from '../status/index.js';
import type { StatusRecord } from '../status/index.js';
import { TopologyBuilder } from '../topology/builder.js';
import { LocalTransport } from '../transport/index.js';
import { MatadorEvent, createSubscriber } from '../types/index.js';
import { Matador } from './matador.js';

class JobEvent extends MatadorEvent {
  static readonly key = 'job.requested';
  static readonly description = 'A job was requested';

  constructor(public data: { userId: string; blob?: string }) {
    super();
  }
}

const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

const topology = TopologyBuilder.create()
  .withNamespace('status')
  .addQueue('events')
  .build();

function trackedRunner(callback: () => unknown = async () => undefined) {
  return createSubscriber<JobEvent>({
    name: 'job-runner',
    description: 'Runs the job',
    track: true,
    callback,
  });
}

function audit() {
  return createSubscriber<JobEvent>({
    name: 'audit',
    description: 'Audits the job',
    callback: async () => {},
  });
}

async function waitFor(condition: () => Promise<boolean>, timeoutMs = 2000) {
  const start = Date.now();
  while (!(await condition())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`Timeout after ${timeoutMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

describe('status tracking: configuration and send', () => {
  let matador: Matador | undefined;

  afterEach(async () => {
    await matador?.shutdown();
    matador = undefined;
  });

  it('throws StatusStoreRequiredError for a tracked subscriber with no store', () => {
    const schema: MatadorSchema = { [JobEvent.key]: [JobEvent, [trackedRunner()]] };
    expect(
      () => new Matador({ transport: new LocalTransport(), topology, schema }),
    ).toThrow(StatusStoreRequiredError);
  });

  it('throws StatusStoreRequiredError from register() with no store', () => {
    matador = new Matador({ transport: new LocalTransport(), topology, schema: {} });
    expect(() => matador?.register(JobEvent, [trackedRunner()])).toThrow(
      StatusStoreRequiredError,
    );
  });

  it('throws StatusStoreRequiredError from getStatus() with no store', async () => {
    matador = new Matador({ transport: new LocalTransport(), topology, schema: {} });
    await expect(matador.getStatus('t-1')).rejects.toBeInstanceOf(
      StatusStoreRequiredError,
    );
  });

  it('returns a trackingId and writes queued before the publish', async () => {
    const store = new MemoryStatusStore();
    const transport = new LocalTransport(silentLogger);
    const seenAtPublish: (StatusRecord | undefined)[] = [];
    const send = transport.send.bind(transport);
    transport.send = async (queue, envelope, options) => {
      if (envelope.docket.trackingId !== undefined) {
        seenAtPublish.push(
          await store.get(envelope.docket.trackingId, envelope.docket.targetSubscriber),
        );
      }
      return send(queue, envelope, options);
    };

    matador = new Matador(
      {
        transport,
        topology,
        schema: { [JobEvent.key]: [JobEvent, [trackedRunner()]] },
        consumeFrom: ['events'],
        statusStore: store,
      },
      { logger: silentLogger },
    );
    await matador.start();

    const result = await matador.send(JobEvent, { userId: 'u-1' });

    expect(result.trackingId).toBeString();
    expect(seenAtPublish).toHaveLength(1);
    expect(seenAtPublish[0]?.status).toBe('queued');
    expect(seenAtPublish[0]?.envelope.docket.trackingId).toBe(result.trackingId);
  });

  it('uses a trackingId that the caller gives', async () => {
    const store = new MemoryStatusStore();
    matador = new Matador(
      {
        transport: new LocalTransport(silentLogger),
        topology,
        schema: { [JobEvent.key]: [JobEvent, [trackedRunner()]] },
        consumeFrom: ['events'],
        statusStore: store,
      },
      { logger: silentLogger },
    );
    await matador.start();

    const result = await matador.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-42' });

    expect(result.trackingId).toBe('job-42');
    expect(await matador.getStatus('job-42', 'job-runner')).toBeDefined();
  });

  it('ignores a caller trackingId when no subscriber is tracked', async () => {
    const store = new MemoryStatusStore();
    matador = new Matador(
      {
        transport: new LocalTransport(silentLogger),
        topology,
        schema: { [JobEvent.key]: [JobEvent, [audit()]] },
        consumeFrom: ['events'],
        statusStore: store,
      },
      { logger: silentLogger },
    );
    await matador.start();

    const result = await matador.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-42' });

    expect(result.trackingId).toBeUndefined();
    expect(await store.getAll('job-42')).toBeUndefined();
  });

  it('tracks only the subscribers with track: true', async () => {
    const store = new MemoryStatusStore();
    matador = new Matador(
      {
        transport: new LocalTransport(silentLogger),
        topology,
        schema: { [JobEvent.key]: [JobEvent, [trackedRunner(), audit()]] },
        consumeFrom: ['events'],
        statusStore: store,
      },
      { logger: silentLogger },
    );
    await matador.start();

    const result = await matador.send(JobEvent, { userId: 'u-1' });

    const all = await matador.getStatus(result.trackingId ?? '');
    expect(Object.keys(all ?? {})).toEqual(['job-runner']);
  });

  it('throws RecordTooLargeError and publishes nothing for a large payload', async () => {
    const store = new MemoryStatusStore({ maxRecordBytes: 1024 });
    const transport = new LocalTransport(silentLogger);
    let publishes = 0;
    const send = transport.send.bind(transport);
    transport.send = async (queue, envelope, options) => {
      publishes++;
      return send(queue, envelope, options);
    };

    matador = new Matador(
      {
        transport,
        topology,
        schema: { [JobEvent.key]: [JobEvent, [trackedRunner(), audit()]] },
        consumeFrom: ['events'],
        statusStore: store,
      },
      { logger: silentLogger },
    );
    await matador.start();

    await expect(
      matador.send(JobEvent, { userId: 'u-1', blob: 'x'.repeat(2000) }),
    ).rejects.toBeInstanceOf(RecordTooLargeError);
    expect(publishes).toBe(0);
  });

  it('marks the record failed when the publish fails and the send is not buffered', async () => {
    const store = new MemoryStatusStore();
    matador = new Matador(
      {
        transport: new LocalTransport(silentLogger),
        topology,
        schema: { [JobEvent.key]: [JobEvent, [trackedRunner()]] },
        statusStore: store,
      },
      { logger: silentLogger },
    );
    await matador.start();

    let error: unknown;
    try {
      await matador.send(JobEvent, { userId: 'u-1' }, { buffer: false, trackingId: 'job-1' });
    } catch (e) {
      error = e;
    }

    expect(error).toBeInstanceOf(SomeSendError);
    const record = await matador.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('failed');
    expect(record?.error?.name).toBe('TransportSendError');
  });

  it('keeps a buffered send queued', async () => {
    const store = new MemoryStatusStore();
    matador = new Matador(
      {
        transport: new LocalTransport(silentLogger),
        topology,
        schema: { [JobEvent.key]: [JobEvent, [trackedRunner()]] },
        statusStore: store,
        retryIntervalMs: 0,
      },
      { logger: silentLogger },
    );
    await matador.start();

    await matador.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });

    expect((await matador.getStatus('job-1', 'job-runner'))?.status).toBe('queued');
  });

  it('marks a buffered send failed when the buffer drops it', async () => {
    const store = new MemoryStatusStore();
    matador = new Matador(
      {
        transport: new LocalTransport(silentLogger),
        topology,
        schema: { [JobEvent.key]: [JobEvent, [trackedRunner()]] },
        statusStore: store,
        maxRetryAttempts: 1,
        retryIntervalMs: 20,
      },
      { logger: silentLogger },
    );
    await matador.start();

    await matador.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });

    await waitFor(
      async () => (await matador?.getStatus('job-1', 'job-runner'))?.status === 'failed',
    );
  });
});
```

The publish-failure tests use a `LocalTransport` with no consumer. `LocalTransport.send` then throws `LocalTransportNoActiveSubscriberError`, and fanout wraps it in `TransportSendError`.

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `cd packages/matador && bun test src/core/status-tracking.test.ts`
Expected: FAIL. `statusStore` is not a config field, and `getStatus` does not exist.

- [ ] **Step 3: Change the fanout engine**

In `packages/matador/src/core/fanout.ts`:

Change the imports:

```ts
import { StatusStoreRequiredError, TransportSendError } from '../errors/index.js';
import type { SafeHooks } from '../hooks/index.js';
import type { SchemaRegistry } from '../schema/index.js';
import { type StatusTracker, isTrackedEnvelope } from '../status/index.js';
```

Add to `FanoutConfig`:

```ts
  /** Status tracker. Set only if Matador has a statusStore. */
  readonly statusTracker?: StatusTracker | undefined;
```

Add to `SendResult`:

```ts
  /** Set only if the send has one or more tracked subscribers. */
  readonly trackingId?: string | undefined;
```

Add the field `private readonly statusTracker: StatusTracker | undefined;` to the class, and set it in the constructor: `this.statusTracker = config.statusTracker;`.

Replace the body of `send()` from `const errors: SendError[] = [];` down to the start of the `this.enqueuingCount++;` line, so that the method first resolves the enabled subscribers, then builds all envelopes, then writes the `queued` records, and only then publishes:

```ts
    const errors: SendError[] = [];
    let sent = 0;
    let skipped = 0;

    // Load universal metadata
    const universalMetadata = await this.hooks.loadUniversalMetadata();

    // Merge event.metadata with options.metadata (options takes precedence)
    const mergedMetadata =
      event.metadata || options.metadata
        ? { ...event.metadata, ...options.metadata }
        : undefined;

    // 1. Resolve the enabled subscribers
    const targets: AnySubscriber[] = [];
    for (const subscriber of subscribers) {
      if (await this.isSubscriberEnabled(subscriber)) {
        targets.push(subscriber);
      } else {
        skipped++;
      }
    }

    // 2. One tracking ID for all tracked subscribers of this send
    const hasTracked = targets.some((s) => s.track === true);
    if (hasTracked && !this.statusTracker) {
      throw new StatusStoreRequiredError(
        `event '${eventKey}' has a subscriber with track: true`,
      );
    }
    const trackingId = hasTracked
      ? (options.trackingId ?? crypto.randomUUID())
      : undefined;

    // 3. Build the envelopes
    const prepared = targets.map((subscriber) => {
      const targetQueue = subscriber.targetQueue ?? this.defaultQueue;
      const qualifiedQueue = resolveTargetQueueName(this.topology, targetQueue);
      const envelope = createEnvelope({
        eventKey,
        eventDescription: eventClass.description,
        targetSubscriber: subscriber.name,
        data: event.data,
        importance: subscriber.importance ?? 'should-investigate',
        correlationId: options.correlationId,
        trackingId: subscriber.track === true ? trackingId : undefined,
        metadata: mergedMetadata,
        universalMetadata,
        delayMs: options.delayMs,
      });
      return { subscriber, qualifiedQueue, envelope };
    });

    // 4. Write the queued records before any publish
    if (this.statusTracker) {
      const tracked = prepared
        .map((p) => p.envelope)
        .filter(isTrackedEnvelope);
      if (tracked.length > 0) {
        await this.statusTracker.createQueued(tracked);
      }
    }

    // 5. Publish
    for (const { subscriber, qualifiedQueue, envelope } of prepared) {
      // Send to transport
      const sendOptions: SendOptions | undefined =
        options.delayMs !== undefined ? { delay: options.delayMs } : undefined;

```

Keep the rest of the loop body (`this.enqueuingCount++; try { ... } finally { ... }`) as it is. In the `else` branch of the `catch` (the send is not buffered), add after the `onEnqueueError` call:

```ts
          await this.markPublishFailed(envelope, err);
```

Change the `return` of `send()` to:

```ts
    return {
      eventKey,
      subscribersSent: sent,
      subscribersSkipped: skipped,
      errors,
      ...(trackingId !== undefined && { trackingId }),
    };
```

In `rebufferUntried`, in the `for (const item of dropped)` loop, after the `onEnqueueError` call, add:

```ts
      await this.markPublishFailed(item.envelope, err);
```

In `handleFlushFailure`, after the final `onEnqueueError` call, add:

```ts
    await this.markPublishFailed(item.envelope, err);
```

Add this private method before `isSubscriberEnabled`:

```ts
  /**
   * Marks a tracked envelope failed when Matador gives up on its publish.
   */
  private async markPublishFailed(envelope: Envelope, error: Error): Promise<void> {
    if (this.statusTracker && isTrackedEnvelope(envelope)) {
      await this.statusTracker.markPublishFailed(envelope, error);
    }
  }
```

Make sure that `AnySubscriber` and `Envelope` are in the type import from `../types/index.js` at the top of the file.

- [ ] **Step 4: Change Matador**

In `packages/matador/src/core/matador.ts`:

Add imports:

```ts
import {
  InvalidSchemaError,
  NotStartedError,
  ShutdownInProgressError,
  SomeSendError,
  StatusStoreRequiredError,
} from '../errors/index.js';
import {
  type StatusRecord,
  type StatusStore,
  StatusTracker,
} from '../status/index.js';
```

Add to `MatadorConfig`, after `checkpointStore`:

```ts
  /**
   * Status store for subscribers with track: true.
   * Required if the schema has a tracked subscriber. Every process that sends to
   * or consumes a tracked subscriber needs a store that points to the same data.
   * The store owns its settings (maxRecordBytes, progressIntervalMs).
   */
  readonly statusStore?: StatusStore | undefined;
```

Add the field `private readonly statusTracker: StatusTracker | undefined;`. In the constructor, set it before `this.registerSchema(config.schema);`:

```ts
    this.statusTracker = config.statusStore
      ? new StatusTracker(config.statusStore, this.hooks)
      : undefined;
```

Pass `statusTracker: this.statusTracker,` in the `FanoutEngine` config.

Add this private method, and call `this.assertTrackingSupported(subscribers);` as the first line of `register()` and before each `this.schema.register(...)` call in `registerSchema()`:

```ts
  /**
   * Throws if a subscriber has track: true and Matador has no status store.
   */
  private assertTrackingSupported(subscribers: readonly AnySubscriber[]): void {
    if (this.statusTracker) {
      return;
    }
    const tracked = subscribers.find((s) => s.track === true);
    if (tracked) {
      throw new StatusStoreRequiredError(
        `subscriber '${tracked.name}' has track: true`,
      );
    }
  }
```

Add `getStatus` after `send()`:

```ts
  /**
   * Reads the status records of a tracked send.
   *
   * @param trackingId - The tracking ID from SendResult.trackingId or from EventOptions.trackingId
   * @returns A map from subscriber name to record, or undefined if no record exists
   * @throws StatusStoreRequiredError if Matador has no statusStore
   */
  getStatus(
    trackingId: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | undefined>;
  /**
   * Reads the status record of one tracked subscriber.
   */
  getStatus(
    trackingId: string,
    subscriberName: string,
  ): Promise<StatusRecord | undefined>;
  async getStatus(
    trackingId: string,
    subscriberName?: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | StatusRecord | undefined> {
    if (!this.statusTracker) {
      throw new StatusStoreRequiredError('getStatus() was called');
    }
    return subscriberName === undefined
      ? this.statusTracker.read(trackingId)
      : this.statusTracker.read(trackingId, subscriberName);
  }
```

- [ ] **Step 5: Export the public API**

In `packages/matador/src/index.ts`, add a section after the `// Core` exports:

```ts
// Status
export type {
  StatusProgress,
  StatusRecord,
  StatusRecordError,
  StatusStore,
  StatusStoreConfig,
  TrackingStatus,
} from './status/index.js';
export {
  DEFAULT_MAX_RECORD_BYTES,
  DEFAULT_PROGRESS_INTERVAL_MS,
  MemoryStatusStore,
  resolveStatusStoreConfig,
} from './status/index.js';
```

Find the errors export block in `src/index.ts` and add `RecordTooLargeError`, `ResultNotSerializableError`, `StatusStoreError`, `StatusStoreRequiredError` to it, and `StatusStoreOperation` to its type exports. Find the hooks type export block and add `StatusStoreErrorContext`.

- [ ] **Step 6: Run the tests to make sure they pass**

Run: `cd packages/matador && bun test src/core && bun run typecheck && bun run lint`
Expected: PASS. The existing `fanout.test.ts` and `matador.test.ts` tests also pass with no change.

- [ ] **Step 7: Commit**

```bash
git add packages/matador/src/core packages/matador/src/index.ts
git commit -m "feat(status): track sends in fanout and add Matador.getStatus

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Pipeline status writes, `ctx.progress` and docs

**Files:**
- Modify: `packages/matador/src/pipeline/pipeline.ts` (`PipelineConfig`, `process`, `handleShouldProcess`, `handleFailure`)
- Modify: `packages/matador/src/core/matador.ts` (pass the tracker to the pipeline)
- Modify: `README.md` (repo root)
- Test: `packages/matador/test/integration/status-tracking.integration.test.ts`

**Interfaces:**
- Consumes: `StatusTracker`, `ProgressReporter`, `isTrackedEnvelope` (Task 5). `Matador.getStatus`, `MatadorConfig.statusStore` (Task 6).
- Produces: `PipelineConfig.statusTracker?: StatusTracker | undefined`. The full lifecycle of spec section 6.1.

Write order in the pipeline:

1. `active` before the callback runs.
2. `completed` after `transport.complete(receipt)`.
3. `retrying` or `failed` after `docket.lastError` is set, and before `handleRetryDecision`. The retry publishes the next delivery, so `retrying` must land first.

- [ ] **Step 1: Write the failing tests**

`packages/matador/test/integration/status-tracking.integration.test.ts`:

```ts
import { afterEach, describe, expect, it, mock } from 'bun:test';
import {
  DoRetry,
  DontRetry,
  LocalTransport,
  type Logger,
  Matador,
  MatadorEvent,
  type MatadorSchema,
  MemoryStatusStore,
  type RetryPolicy,
  type StatusRecord,
  type StatusStore,
  StandardRetryPolicy,
  TopologyBuilder,
  type TrackingStatus,
  createSubscriber,
} from '../../src/index.js';

class JobEvent extends MatadorEvent {
  static readonly key = 'job.requested';
  static readonly description = 'A job was requested';

  constructor(public data: { userId: string }) {
    super();
  }
}

const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

/** Memory store that records the status of each write, in order. */
class RecordingStore extends MemoryStatusStore {
  readonly writes: TrackingStatus[] = [];

  override async set(trackingId: string, subscriberName: string, record: StatusRecord) {
    this.writes.push(record.status);
    await super.set(trackingId, subscriberName, record);
  }
}

/** Memory store that fails every write of the given statuses. */
class FlakyStore extends MemoryStatusStore {
  constructor(private readonly failOn: readonly TrackingStatus[]) {
    super({ progressIntervalMs: 0 });
  }

  override async set(trackingId: string, subscriberName: string, record: StatusRecord) {
    if (this.failOn.includes(record.status)) {
      throw new Error('store down');
    }
    await super.set(trackingId, subscriberName, record);
  }
}

async function waitFor(condition: () => Promise<boolean> | boolean, timeoutMs = 3000) {
  const start = Date.now();
  while (!(await condition())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`Timeout after ${timeoutMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

function gate() {
  let open: () => void = () => {};
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, opened };
}

describe('status tracking: lifecycle', () => {
  let matador: Matador | undefined;

  afterEach(async () => {
    await matador?.shutdown();
    matador = undefined;
  });

  async function start(
    schema: MatadorSchema,
    store: StatusStore,
    options: { retry?: boolean; retryPolicy?: RetryPolicy; hooks?: object } = {},
  ): Promise<Matador> {
    const builder = TopologyBuilder.create().withNamespace('status-int').addQueue('events');
    const topology = options.retry
      ? builder.withRetry({ enabled: true, defaultDelayMs: 1, maxDelayMs: 5 }).build()
      : builder.build();
    matador = new Matador(
      {
        transport: new LocalTransport(silentLogger),
        topology,
        schema,
        consumeFrom: ['events'],
        statusStore: store,
        retryPolicy: options.retryPolicy,
      },
      { logger: silentLogger, ...options.hooks },
    );
    await matador.start();
    return matador;
  }

  it('shows active while the callback runs and completed with the result after', async () => {
    const running = gate();
    const store = new MemoryStatusStore();
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      track: true,
      callback: async () => {
        await running.opened;
        return { s3Key: 'exports/1.csv' };
      },
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [subscriber]] }, store);

    const sending = m.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await waitFor(async () => (await m.getStatus('job-1', 'job-runner'))?.status === 'active');
    running.open();
    await sending;
    await m.waitForIdle(2000);

    const record = await m.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('completed');
    expect(record?.result).toEqual({ s3Key: 'exports/1.csv' });
    expect(record?.envelope.data).toEqual({ userId: 'u-1' });
  });

  it('shows progress while the callback runs and keeps it after completion', async () => {
    const running = gate();
    const store = new MemoryStatusStore({ progressIntervalMs: 0 });
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      track: true,
      callback: async (_envelope, ctx) => {
        ctx.progress(40, { itemsCompleted: 4, totalItems: 10 });
        await running.opened;
      },
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [subscriber]] }, store);

    const sending = m.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await waitFor(
      async () => (await m.getStatus('job-1', 'job-runner'))?.progress?.percent === 40,
    );
    running.open();
    await sending;
    await m.waitForIdle(2000);

    const record = await m.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('completed');
    expect(record?.progress).toEqual({
      percent: 40,
      details: { itemsCompleted: 4, totalItems: 10 },
    });
  });

  it('writes retrying before the next delivery writes active', async () => {
    const store = new RecordingStore();
    let attempts = 0;
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      idempotent: 'yes',
      track: true,
      callback: async () => {
        attempts++;
        if (attempts === 1) {
          throw new DoRetry('temporary');
        }
      },
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [subscriber]] }, store, {
      retry: true,
    });

    await m.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await waitFor(() => store.writes.includes('completed'), 5000);

    expect(store.writes).toEqual(['queued', 'active', 'retrying', 'active', 'completed']);
  });

  it('writes failed with the error for DontRetry', async () => {
    const store = new MemoryStatusStore();
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      track: true,
      callback: async () => {
        throw new DontRetry('bad input');
      },
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [subscriber]] }, store);

    await m.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await m.waitForIdle(2000);

    const record = await m.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('failed');
    expect(record?.error?.message).toBe('bad input');
    expect(record?.envelope.docket.lastError).toBe('bad input');
  });

  it('writes failed when shouldProcess rejects the message before the callback runs', async () => {
    const store = new MemoryStatusStore();
    const callback = mock(async () => {});
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      track: true,
      callback,
    });
    const standard = new StandardRetryPolicy();
    const rejectAll: RetryPolicy = {
      shouldProcess: () => ({
        action: 'dead-letter',
        queue: 'undeliverable',
        reason: 'redelivered message for idempotent: no',
      }),
      shouldRetry: (context) => standard.shouldRetry(context),
      getDelay: (context) => standard.getDelay(context),
    };
    const m = await start({ [JobEvent.key]: [JobEvent, [subscriber]] }, store, {
      retryPolicy: rejectAll,
    });

    await m.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await m.waitForIdle(2000);

    expect(callback).not.toHaveBeenCalled();
    const record = await m.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('failed');
    expect(record?.error?.message).toBe('redelivered message for idempotent: no');
  });

  it('fails with RecordTooLargeError for a large result and does not retry', async () => {
    const store = new MemoryStatusStore({ maxRecordBytes: 2048 });
    let calls = 0;
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      idempotent: 'yes',
      track: true,
      callback: async () => {
        calls++;
        return { markdown: 'x'.repeat(4000) };
      },
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [subscriber]] }, store, {
      retry: true,
    });

    await m.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await m.waitForIdle(2000);

    expect(calls).toBe(1);
    const record = await m.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('failed');
    expect(record?.error?.name).toBe('RecordTooLargeError');
  });

  it('does not change message handling when the store fails after the send', async () => {
    const store = new FlakyStore(['active', 'completed']);
    const onStatusStoreError = mock(() => {});
    let calls = 0;
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      idempotent: 'yes',
      track: true,
      callback: async () => {
        calls++;
      },
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [subscriber]] }, store, {
      retry: true,
      hooks: { onStatusStoreError },
    });

    await m.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await m.waitForIdle(2000);

    expect(calls).toBe(1);
    expect(onStatusStoreError).toHaveBeenCalledTimes(2);
    expect((await m.getStatus('job-1', 'job-runner'))?.status).toBe('queued');
  });

  it('keeps one record for each of two tracked subscribers', async () => {
    const store = new MemoryStatusStore();
    const first = createSubscriber<JobEvent>({
      name: 'ocr',
      description: 'Runs OCR',
      track: true,
      callback: async () => ({ pages: 3 }),
    });
    const second = createSubscriber<JobEvent>({
      name: 'thumbnail',
      description: 'Makes a thumbnail',
      track: true,
      callback: async () => {
        throw new DontRetry('no image');
      },
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [first, second]] }, store);

    const result = await m.send(JobEvent, { userId: 'u-1' });
    await m.waitForIdle(2000);

    const all = await m.getStatus(result.trackingId ?? '');
    expect(all?.ocr?.status).toBe('completed');
    expect(all?.ocr?.result).toEqual({ pages: 3 });
    expect(all?.thumbnail?.status).toBe('failed');
  });

  it('gives untracked subscribers a progress function that does nothing', async () => {
    const store = new MemoryStatusStore({ progressIntervalMs: 0 });
    const subscriber = createSubscriber<JobEvent>({
      name: 'audit',
      description: 'Audits the job',
      callback: async (_envelope, ctx) => {
        ctx.progress(50);
      },
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [subscriber]] }, store);

    const result = await m.send(JobEvent, { userId: 'u-1' });
    await m.waitForIdle(2000);

    expect(result.trackingId).toBeUndefined();
  });
});
```

If `src/index.ts` does not export `StandardRetryPolicy` or `RetryPolicy`, import them from `../../src/retry/index.js`.

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `cd packages/matador && bun test test/integration/status-tracking.integration.test.ts`
Expected: FAIL. The records stay `queued`, because the pipeline does not write status yet.

- [ ] **Step 3: Connect the tracker in the pipeline**

In `packages/matador/src/pipeline/pipeline.ts`:

Add the import:

```ts
import {
  type ProgressReporter,
  type StatusTracker,
  isTrackedEnvelope,
} from '../status/index.js';
```

Add to `PipelineConfig`:

```ts
  /** Status tracker. Set only if Matador has a statusStore. */
  readonly statusTracker?: StatusTracker | undefined;
```

Add the field `private readonly statusTracker: StatusTracker | undefined;` and set it in the constructor: `this.statusTracker = config.statusTracker;`.

In `process()`, replace the Task 4 placeholder lines

```ts
    const report = (_percent: number, _details?: JsonSerializable): void => {};
```

with:

```ts
    // Status tracking: only for envelopes that fanout gave a tracking ID
    const tracker =
      this.statusTracker && isTrackedEnvelope(envelope)
        ? this.statusTracker
        : undefined;
    const reporter = tracker?.startProgress(envelope);
    const report = (percent: number, details?: JsonSerializable): void => {
      reporter?.report(percent, details);
    };
    await tracker?.markActive(envelope);
```

In the success branch, after `await this.transport.complete(receipt);`, add:

```ts
      await tracker?.markCompleted(envelope, result, reporter);
```

In the failure branch at the end of `process()`, pass the reporter:

```ts
    return await this.handleFailure(
      error,
      decision,
      envelope,
      subscriberDef,
      isResumable,
      receipt,
      durationMs,
      reporter,
    );
```

Add a last parameter to `handleFailure`, and write the status before `handleRetryDecision`:

```ts
  private async handleFailure(
    error: Error,
    decision: RetryDecision,
    envelope: Envelope,
    subscriberDef: SubscriberDefinition,
    isResumable: boolean,
    receipt: MessageReceipt,
    durationMs: number,
    reporter?: ProgressReporter,
  ): Promise<ProcessResult> {
    // Update envelope with error info
    envelope.docket.lastError = error.message;
    envelope.docket.firstError ??= error.message;

    // Write retrying or failed before the retry publishes the next delivery
    if (this.statusTracker && isTrackedEnvelope(envelope)) {
      await this.statusTracker.markFailure(envelope, error, decision, reporter);
    }
```

Keep the rest of `handleFailure` as it is. Update its JSDoc with `@param reporter - The progress reporter of this delivery, if the envelope is tracked`. `handleShouldProcess` calls `handleFailure` with no reporter, so a rejected redelivery also gets `failed`.

- [ ] **Step 4: Pass the tracker from Matador**

In `packages/matador/src/core/matador.ts`, the pipeline is made before `this.statusTracker` is used by fanout. Move the `this.statusTracker = ...` assignment above `new ProcessingPipeline(...)` if it is not there already, and add to the pipeline config:

```ts
      statusTracker: this.statusTracker,
```

- [ ] **Step 5: Run the tests to make sure they pass**

Run: `cd packages/matador && bun test test/integration/status-tracking.integration.test.ts src && bun run typecheck && bun run lint`
Expected: PASS. All existing unit tests pass with no change, which shows that a Matador with no store behaves as before.

- [ ] **Step 6: Document the feature**

In `README.md` at the repo root, add a section after the `### \`ResumableSubscriber\`` section (before `### Ordering and concurrency guarantees`):

````markdown
### Status tracking

RabbitMQ cannot give the status of one message. Matador can keep the status in a pluggable store, so that an app can poll the status, the progress and the result of a job.

1. Give Matador a store. Use `MemoryStatusStore` in tests, and `RedisStatusStore` from `@zdavison/matador-redis` in production.
2. Set `track: true` on the subscriber.
3. Read the `trackingId` from the send result, or give your own.
4. Poll with `matador.getStatus(trackingId)`.

```typescript
const matador = new Matador({ transport, topology, schema, statusStore: new MemoryStatusStore() });

const ocr = createSubscriber<OcrRequestedEvent>({
  name: 'ocr',
  description: 'Runs OCR on an uploaded file',
  track: true,
  callback: async (envelope, ctx) => {
    ctx.progress(10);
    const markdown = await runOcr(envelope.data.fileKey);
    const resultKey = await uploadToS3(markdown);
    ctx.progress(100);
    return { resultKey }; // The return value is the result. Keep it small.
  },
});

const { trackingId } = await matador.send(OcrRequestedEvent, { fileKey, userId });
const record = await matador.getStatus(trackingId!, 'ocr');
// record.status: 'queued' | 'active' | 'retrying' | 'completed' | 'failed'
// record.progress, record.result, record.error, record.envelope
```

Rules:

- The record holds the whole envelope. Check access in your app, for example compare `record.envelope.data.userId` with the current user.
- The store limits each record to `maxRecordBytes` (default 64 KB). Write large data, for example files, to blob storage and return a pointer.
- `ctx.progress()` returns at once. The store writes the latest value at most once in each `progressIntervalMs` (default 1000 ms).
- A store failure after the send does not change message handling. Matador calls the `onStatusStoreError` hook.
- v1 has no atomic writes. If two processes handle the same message, the record can show the wrong final status.
````

- [ ] **Step 7: Commit**

```bash
git add packages/matador/src/pipeline/pipeline.ts packages/matador/src/core/matador.ts packages/matador/test/integration/status-tracking.integration.test.ts README.md
git commit -m "feat(status): write status and progress from the pipeline

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: matador-nest support

**Files:**
- Modify: `packages/matador-nest/src/discovery/subscriber-discovery.service.ts:97-122`
- Modify: `packages/matador-nest/src/services/matador.service.ts` (constructor config, new `getStatus`)
- Test: `packages/matador-nest/test/status.test.ts`

**Interfaces:**
- Consumes: `track` on `BaseSubscriberOptions` (Task 3), `MatadorConfig.statusStore`, `Matador.getStatus`, `MemoryStatusStore`, `StatusRecord` (Task 6), `CallbackContext.progress` (Task 4).
- Produces:
  - `@OnMatadorEvent(Event, { track: true })` makes a tracked subscriber.
  - The decorated method gets `(envelope, context)`, and its return value is the result.
  - `MatadorService.getStatus(trackingId)` and `MatadorService.getStatus(trackingId, subscriberName)`, with the same return types as `Matador.getStatus`.

Today the discovery service drops the method return value and does not pass the context to standard subscribers. Without this fix, a Nest subscriber cannot return a result or call `ctx.progress()`.

- [ ] **Step 1: Build core**

Run: `bun run --filter '@zdavison/matador' build`
Expected: the build succeeds. `matador-nest` imports `@zdavison/matador` from `dist`.

- [ ] **Step 2: Write the failing tests**

`packages/matador-nest/test/status.test.ts`:

```ts
import { describe, expect, it } from 'bun:test';
import 'reflect-metadata';
import type { DiscoveryService } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import {
  type CallbackContext,
  type Envelope,
  MatadorEvent,
  MemoryStatusStore,
} from '@zdavison/matador';
import { OnMatadorEvent } from '../src/decorators/on-matador-event.decorator.js';
import { SubscriberDiscoveryService } from '../src/discovery/subscriber-discovery.service.js';
import { MatadorService } from '../src/services/matador.service.js';
import { MatadorTestingModule } from '../src/testing/matador-testing.module.js';

class JobEvent extends MatadorEvent<{ userId: string }> {
  static readonly key = 'job.requested';
  static readonly description = 'A job was requested';

  constructor(public readonly data: { userId: string }) {
    super();
  }
}

function discover(instance: object) {
  const discovery = new SubscriberDiscoveryService({
    getProviders: () => [{ instance }],
  } as unknown as DiscoveryService);
  discovery.onModuleInit();
  const [, subscribers] = discovery.getSchema()[JobEvent.key] as [
    unknown,
    {
      track?: boolean;
      callback: (envelope: unknown, context: unknown) => Promise<unknown>;
    }[],
  ];
  const subscriber = subscribers[0];
  if (!subscriber) throw new Error('no subscriber discovered');
  return subscriber;
}

describe('matador-nest status tracking', () => {
  it('carries track: true from the decorator', () => {
    class JobService {
      @OnMatadorEvent(JobEvent, { description: 'Runs the job', track: true })
      async run(_envelope: Envelope<JobEvent['data']>) {}
    }
    expect(discover(new JobService()).track).toBe(true);
  });

  it('passes the context and returns the method result', async () => {
    let received: unknown;
    class JobService {
      @OnMatadorEvent(JobEvent, { description: 'Runs the job', track: true })
      async run(_envelope: Envelope<JobEvent['data']>, context: CallbackContext) {
        received = context;
        return { s3Key: 'k' };
      }
    }
    const context = { marker: true };
    const result = await discover(new JobService()).callback({}, context);
    expect(received).toBe(context);
    expect(result).toEqual({ s3Key: 'k' });
  });

  it('passes the context and returns the result for a resumable subscriber', async () => {
    let received: unknown;
    class JobService {
      @OnMatadorEvent(JobEvent, {
        description: 'Runs the job',
        idempotent: 'resumable',
        track: true,
      })
      async run(_envelope: Envelope<JobEvent['data']>, context: CallbackContext) {
        received = context;
        return 7;
      }
    }
    const context = { marker: true };
    const result = await discover(new JobService()).callback({}, context);
    expect(received).toBe(context);
    expect(result).toBe(7);
  });

  it('MatadorService.getStatus reads from the configured store', async () => {
    const statusStore = new MemoryStatusStore();
    await statusStore.set('job-1', 'runner', {
      status: 'active',
      envelope: {
        id: 'e-1',
        data: { userId: 'u-1' },
        docket: {
          eventKey: JobEvent.key,
          targetSubscriber: 'runner',
          attempts: 1,
          createdAt: '2026-01-01T00:00:00.000Z',
          importance: 'should-investigate',
          trackingId: 'job-1',
        },
      },
      updatedAt: '2026-01-01T00:00:00.000Z',
    });

    const moduleRef = await Test.createTestingModule({
      imports: [MatadorTestingModule.forTest({ autoStart: false, statusStore })],
    }).compile();
    await moduleRef.init();
    const service = moduleRef.get(MatadorService);

    expect((await service.getStatus('job-1', 'runner'))?.status).toBe('active');
    expect(Object.keys((await service.getStatus('job-1')) ?? {})).toEqual(['runner']);

    await moduleRef.close();
  });
});
```

- [ ] **Step 3: Run the tests to make sure they fail**

Run: `cd packages/matador-nest && bun test test/status.test.ts`
Expected: FAIL. `track` is not carried, the context and the result are dropped, and `getStatus` does not exist.

- [ ] **Step 4: Change the discovery service**

In `packages/matador-nest/src/discovery/subscriber-discovery.service.ts`, replace the `const subscriber: AnySubscriber = isResumable ? ... : ...;` expression with:

```ts
    // Create a subscriber that calls the instance method.
    // The method gets the context, and its return value is the subscriber result.
    const subscriber: AnySubscriber = isResumable
      ? createSubscriber({
          name: subscriberName,
          description: options.description,
          idempotent: 'resumable',
          importance: options.importance,
          targetQueue: options.targetQueue,
          enabled: options.enabled,
          track: options.track,
          callback: async (
            envelope: Envelope<unknown>,
            context: ResumableCallbackContext,
          ) => {
            return await boundMethod(envelope, context);
          },
        })
      : createSubscriber({
          name: subscriberName,
          description: options.description,
          idempotent: options.idempotent,
          importance: options.importance,
          targetQueue: options.targetQueue,
          enabled: options.enabled,
          track: options.track,
          callback: async (
            envelope: Envelope<unknown>,
            context: CallbackContext,
          ) => {
            return await boundMethod(envelope, context);
          },
        });
```

Change the type import from `@zdavison/matador` in that file: remove `SubscriberContext` if nothing else uses it, and add `CallbackContext` and `ResumableCallbackContext`.

- [ ] **Step 5: Change the service**

In `packages/matador-nest/src/services/matador.service.ts`, add `statusStore: this.options.statusStore,` to the `new Matador({...})` config, after `checkpointStore`. Add `StatusRecord` to the type import from `@zdavison/matador`. Add after `send()`:

```ts
  /**
   * Reads the status records of a tracked send.
   * @returns A map from subscriber name to record, or undefined if no record exists
   * @throws StatusStoreRequiredError if no statusStore is configured
   */
  getStatus(
    trackingId: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | undefined>;
  /**
   * Reads the status record of one tracked subscriber.
   */
  getStatus(
    trackingId: string,
    subscriberName: string,
  ): Promise<StatusRecord | undefined>;
  async getStatus(
    trackingId: string,
    subscriberName?: string,
  ): Promise<Readonly<Record<string, StatusRecord>> | StatusRecord | undefined> {
    return subscriberName === undefined
      ? this.matador.getStatus(trackingId)
      : this.matador.getStatus(trackingId, subscriberName);
  }
```

- [ ] **Step 6: Run the tests to make sure they pass**

Run: `cd packages/matador-nest && bun test && bun run typecheck && bun run lint`
Expected: PASS, including the existing discovery, decorator and module tests.

- [ ] **Step 7: Commit**

```bash
git add packages/matador-nest/src packages/matador-nest/test/status.test.ts
git commit -m "feat(nest): support tracked subscribers and MatadorService.getStatus

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: `@zdavison/matador-redis` package

**Files:**
- Create: `packages/matador-redis/package.json`
- Create: `packages/matador-redis/tsconfig.json`
- Create: `packages/matador-redis/tsup.config.ts`
- Create: `packages/matador-redis/README.md`
- Create: `packages/matador-redis/src/index.ts`
- Create: `packages/matador-redis/src/redis-status-store.ts`
- Test: `packages/matador-redis/src/redis-status-store.test.ts`
- Test: `packages/matador-redis/test/redis-status-store.integration.test.ts`
- Modify: `package.json` (repo root, `build` script)
- Modify: `bun.lock` (by `bun install`)

**Interfaces:**
- Consumes: `StatusRecord`, `StatusStore`, `StatusStoreConfig`, `resolveStatusStoreConfig` from `@zdavison/matador` (Task 6 exports). `describeStatusStoreContract` from `../../matador/src/status/stores/status-store-contract.js` (Task 2).
- Produces:
  - `interface RedisStatusClient { hGet; hGetAll; del; multi }` and `interface RedisStatusMulti { hSet; pExpire; exec }`. A `redis` v4 client fits this interface with no adapter. This was checked with `redis@4.7.1` and `tsc --strict --exactOptionalPropertyTypes`.
  - `interface RedisStatusStoreOptions extends Partial<StatusStoreConfig> { client: RedisStatusClient; ttlMs: number; keyPrefix?: string | undefined }`
  - `DEFAULT_KEY_PREFIX = 'matador:status:'`
  - `class RedisStatusStore implements StatusStore`

The store has no runtime dependency on `redis`. It uses only the client interface. `redis` is a dev dependency for the tests.

- [ ] **Step 1: Build core**

Run: `bun run --filter '@zdavison/matador' build`
Expected: the build succeeds.

- [ ] **Step 2: Write the package files**

`packages/matador-redis/package.json`:

```json
{
  "name": "@zdavison/matador-redis",
  "version": "4.2.0",
  "description": "Redis status store for the Matador event processing library",
  "type": "module",
  "main": "./dist/index.cjs",
  "module": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "import": {
        "types": "./dist/index.d.ts",
        "default": "./dist/index.js"
      },
      "require": {
        "types": "./dist/index.d.cts",
        "default": "./dist/index.cjs"
      }
    }
  },
  "scripts": {
    "build": "rm -rf dist *.tsbuildinfo && tsup && tsc --build && cp dist/index.d.ts dist/index.d.cts",
    "test": "bun test",
    "test:watch": "bun test --watch",
    "lint": "bunx biome check .",
    "lint:fix": "bunx biome check . --fix",
    "typecheck": "tsc --noEmit",
    "clean": "rm -rf dist"
  },
  "peerDependencies": {
    "@zdavison/matador": "^4.2.0"
  },
  "devDependencies": {
    "@testcontainers/redis": "^10.16.0",
    "@zdavison/matador": "4.2.0",
    "redis": "^4.6.5",
    "testcontainers": "^10.16.0",
    "tsup": "^8.3.5"
  },
  "engines": {
    "node": ">=18.0.0"
  },
  "keywords": ["matador", "redis", "status", "events", "queue"],
  "license": "MIT",
  "files": ["dist"],
  "repository": {
    "type": "git",
    "url": "https://github.com/meetsmore/matador.git"
  }
}
```

`packages/matador-redis/tsconfig.json`:

```json
{
  "compilerOptions": {
    "composite": true,
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "declaration": true,
    "declarationMap": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "noEmit": false,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,
    "exactOptionalPropertyTypes": true,
    "noUncheckedIndexedAccess": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "verbatimModuleSyntax": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist", "test", "**/*.test.ts"],
  "references": [{ "path": "../matador" }]
}
```

`packages/matador-redis/tsup.config.ts`:

```ts
import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['cjs', 'esm'],
  dts: false,
  splitting: false,
  sourcemap: true,
  clean: false,
  outDir: 'dist',
  target: 'node18',
  treeshake: true,
  external: ['@zdavison/matador'],
});
```

In the root `package.json`, change the `build` script to build the Redis package after core:

```json
    "build": "bun run --filter '@zdavison/matador' build && bun run --filter '@zdavison/matador-nest' build && bun run --filter '@zdavison/matador-redis' build && bun run --filter 'matador-nest-example' build",
```

Run: `bun install`
Expected: `bun.lock` changes and the new dev dependencies install.

- [ ] **Step 3: Write the failing unit test**

`packages/matador-redis/src/redis-status-store.test.ts`:

```ts
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
    const store = new RedisStatusStore({ client, ttlMs: 60_000, keyPrefix: 'app:' });
    await store.set('t-1', 'runner', record());
    expect([...client.hashes.keys()]).toEqual(['app:t-1']);
    expect([...(client.hashes.get('app:t-1')?.keys() ?? [])]).toEqual(['runner']);
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
    expect(store.config).toEqual({ maxRecordBytes: 4096, progressIntervalMs: 1_000 });
  });

  it('rejects a ttlMs that is not a positive integer', () => {
    for (const ttlMs of [0, -1, 1.5, Number.NaN]) {
      expect(() => new RedisStatusStore({ client: new FakeRedis(), ttlMs })).toThrow(
        RangeError,
      );
    }
  });

  it('does not implement cleanup, because Redis TTL replaces it', () => {
    const store = new RedisStatusStore({ client: new FakeRedis(), ttlMs: 60_000 });
    expect('cleanup' in store).toBe(false);
  });
});
```

- [ ] **Step 4: Run the test to make sure it fails**

Run: `cd packages/matador-redis && bun test src`
Expected: FAIL. `./redis-status-store.js` does not exist.

- [ ] **Step 5: Write the store**

`packages/matador-redis/src/redis-status-store.ts`:

```ts
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
```

`packages/matador-redis/src/index.ts`:

```ts
export type {
  RedisStatusClient,
  RedisStatusMulti,
  RedisStatusStoreOptions,
} from './redis-status-store.js';
export { DEFAULT_KEY_PREFIX, RedisStatusStore } from './redis-status-store.js';
```

- [ ] **Step 6: Run the unit test to make sure it passes**

Run: `cd packages/matador-redis && bun test src && bun run typecheck`
Expected: PASS. The contract suite shows 7 tests, and the `RedisStatusStore` block shows 6 tests.

- [ ] **Step 7: Write the integration test**

`packages/matador-redis/test/redis-status-store.integration.test.ts`:

```ts
import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
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
```

- [ ] **Step 8: Run the integration test**

Run: `cd packages/matador-redis && bun test test`
Expected: PASS when Docker runs. The contract suite shows 7 tests, plus the TTL test. If Docker does not run, set `SKIP_E2E_TESTS=true`, and the suite is skipped.

- [ ] **Step 9: Write the package README**

`packages/matador-redis/README.md`:

````markdown
# @zdavison/matador-redis

A Redis status store for [Matador](https://github.com/zdavison/matador) status tracking.

## Install

```bash
bun add @zdavison/matador-redis
```

## Use

```typescript
import { createClient } from 'redis';
import { Matador } from '@zdavison/matador';
import { RedisStatusStore } from '@zdavison/matador-redis';

const client = createClient({ url: process.env.REDIS_URL });
await client.connect();

const matador = new Matador({
  transport,
  topology,
  schema,
  statusStore: new RedisStatusStore({
    client,
    ttlMs: 24 * 60 * 60 * 1000, // Keep each tracking ID for one day after its last write
  }),
});
```

## Options

| Option | Required | Default | Meaning |
|---|---|---|---|
| `client` | Yes | | A connected `redis` v4 client. For other clients, write an adapter for `RedisStatusClient`. |
| `ttlMs` | Yes | | Retention of each tracking ID, in milliseconds. Each write refreshes it. |
| `keyPrefix` | No | `matador:status:` | Prefix for the Redis keys |
| `maxRecordBytes` | No | `65536` | Limit on each JSON record, in bytes |
| `progressIntervalMs` | No | `1000` | Minimum time between two progress writes for one record |

## Data layout

The store keeps one Redis hash for each tracking ID, at `<keyPrefix><trackingId>`. Each field is a subscriber name, and each value is a JSON status record.
````

- [ ] **Step 10: Run the full verification**

Run each command from the repo root:

```bash
bun run build
bun run typecheck
bun run lint
bun run test
bun run --filter '@zdavison/matador' test:integration
```

Expected: all commands succeed. If Docker does not run, the Redis and RabbitMQ container suites need `SKIP_E2E_TESTS=true`, and you must report that they did not run.

- [ ] **Step 11: Commit**

```bash
git add package.json bun.lock packages/matador-redis
git commit -m "feat(redis): add @zdavison/matador-redis with RedisStatusStore

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
