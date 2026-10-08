# Matador status store: design

Date: 2026-10-08. Status: draft for review. Branch: `feat/status-store`.

## 1. Goal

Matador gets an optional, pluggable status store. With the store, an app polls the status, progress and result of a message that it sent. Apps then stop building their own job-status records for each feature.

### 1.1 Success criteria

- The OCR feature (meetsone PR #27189) and `async-operations` can replace their own status records with the Matador store. The migration is a later, separate task.
- If no store is configured, Matador behaves as it does in v4.2.0. No tracked code path runs.
- The store does not depend on a transport.
- The design makes large data in the store hard. Large data goes to blob storage, for example S3. The record keeps only a pointer.

### 1.2 Why Matador needs a store

RabbitMQ cannot give the status of one message. AMQP has no operation to find a message by ID. The broker reports only queue-level counts. After the ack, the message is gone. So each job system that supports polling keeps the status outside the broker. Celery calls this store a "result backend".

## 2. Scope

### 2.1 In scope for v1

- A `StatusStore` interface and a `MemoryStatusStore` in `@zdavison/matador`.
- A `RedisStatusStore` in a new package, `@zdavison/matador-redis`.
- `track: true` on a subscriber definition.
- A `trackingId` for each send, given by the caller or made by Matador.
- Status writes by Matador for the full lifecycle.
- `ctx.progress(percent, details?)` in the subscriber context.
- The callback return value as the result.
- A size limit, `maxRecordBytes`, on the whole record.
- `matador.getStatus()` and `MatadorService.getStatus()` in `matador-nest`.

### 2.2 Out of scope for v1

- **Concurrency limits**, for example "one active job for each user". Apps keep their own locks.
- **Atomic writes.** The store has plain reads and writes. Section 8 gives the known race.
- **Ownership checks.** Matador has no owner concept. The app reads the record and checks the envelope data itself.
- **A combined status for an event.** The app calculates a combined status from the records for each subscriber.
- **Cancellation.** `async-operations` has a `cancelled` status. v1 has no cancel operation.
- **Migration of meetsone.**

### 2.3 Rules

- Matador never uses `docket.metadata` or `envelope.data` for logic. Metadata is for observability and debugging only.
- Matador does not interpret any app field in the record.

## 3. Concepts

| Term | Meaning |
|---|---|
| Tracked subscriber | A subscriber with `track: true` in its definition |
| Tracked send | A `send()` of an event that has one or more tracked subscribers |
| `trackingId` | The ID of one tracked send. One ID covers all tracked subscribers of that send. |
| Status record | The data for one `trackingId` and one tracked subscriber |
| Terminal status | `completed` or `failed` |

## 4. Public API

### 4.1 Configuration

```ts
interface MatadorConfig {
  // existing fields ...
  readonly statusStore?: StatusStore | undefined;
  readonly statusTracking?: {
    /** Limit on the JSON-serialized record, in bytes. Default: 65536 (64 KB). */
    readonly maxRecordBytes?: number | undefined;
    /** Minimum time between two progress writes for one record. Default: 1000. */
    readonly progressIntervalMs?: number | undefined;
  } | undefined;
}
```

If the schema has a tracked subscriber and `statusStore` is not set, the `Matador` constructor throws `StatusStoreRequiredError`. Both the dispatcher process and the consumer process need the store. The dispatcher writes the first record, and the consumer writes the later records.

### 4.2 Subscriber definition

```ts
interface BaseSubscriberOptions {
  // existing fields ...
  /** If true, Matador keeps a status record for each send to this subscriber. */
  readonly track?: boolean | undefined;
}
```

`@OnMatadorEvent` options extend `BaseSubscriberOptions`, so `@OnMatadorEvent({ track: true })` works with no other change.

### 4.3 Send

```ts
interface EventOptions {
  // existing fields ...
  /** ID for the status records of this send. If not set, Matador makes a UUID v4. */
  readonly trackingId?: string | undefined;
}

interface SendResult {
  // existing fields ...
  /** Set only if the send has one or more tracked subscribers. */
  readonly trackingId?: string | undefined;
}
```

- If the send has no tracked subscriber, Matador ignores `trackingId`, and `SendResult.trackingId` is `undefined`.
- The caller makes sure that a caller-supplied `trackingId` is unique. If two sends use the same ID, the second send replaces the records of the first send.
- Matador adds the ID to the envelope as `docket.trackingId`. The consumer reads the ID from the docket. This is an optional, additive field in the envelope format.

### 4.4 Subscriber context

```ts
interface CallbackContext {
  // existing fields ...
  /**
   * Report progress. Returns at once and does not wait for the store.
   * Does nothing if the subscriber is not tracked.
   * @param percent - A number from 0 to 100.
   * @param details - Optional small JSON value, for example counters.
   */
  progress(percent: number, details?: JsonSerializable): void;
}
```

`ResumableCallbackContext` is `SubscriberContext & CallbackContext`, so resumable subscribers also get `progress`.

### 4.5 Result

The callback return value is the result. For a tracked subscriber, the return value must be `JsonSerializable` or `undefined`. Matador checks this at runtime with `JSON.stringify`. If the value cannot be serialized, Matador writes `failed` with `ResultNotSerializableError`.

The design does not let the record hold large data. If a result is large, the subscriber writes the data to blob storage and returns a pointer, for example `{ s3Key: '...' }`.

### 4.6 Read

```ts
class Matador {
  getStatus(trackingId: string): Promise<Readonly<Record<string, StatusRecord>> | undefined>;
  getStatus(trackingId: string, subscriberName: string): Promise<StatusRecord | undefined>;
}
```

- The first form returns a map from subscriber name to record. It returns `undefined` if no record exists.
- The second form returns one record.
- If no store is configured, `getStatus` throws `StatusStoreRequiredError`.
- The app checks access itself. For example, it compares `record.envelope.data.userId` with the current user.

### 4.7 NestJS

- `MatadorModule.forRoot({ statusStore, statusTracking })` passes both values to Matador.
- `MatadorService.getStatus(...)` has the same overloads as `Matador.getStatus`.
- `SendResult.trackingId` reaches Nest apps through the existing `MatadorService.send()`.

## 5. The status record

```ts
type TrackingStatus = 'queued' | 'active' | 'retrying' | 'completed' | 'failed';

interface StatusRecord {
  readonly trackingId: string;
  readonly subscriberName: string;
  readonly status: TrackingStatus;
  /** The attempt number of the latest delivery (1-based). */
  readonly attempts: number;
  readonly progress?: {
    readonly percent: number;
    readonly details?: JsonSerializable | undefined;
  } | undefined;
  /** Set only when status is 'completed'. */
  readonly result?: JsonSerializable | undefined;
  /** Set when status is 'retrying' or 'failed'. */
  readonly error?: { readonly name: string; readonly message: string } | undefined;
  /** The latest envelope, including data and docket. */
  readonly envelope: Envelope;
  readonly createdAt: string; // ISO 8601
  readonly updatedAt: string; // ISO 8601
}
```

- Matador writes the whole record on each status change. The record always has the latest envelope, so `docket.attempts`, `firstError` and `lastError` are current.
- `error` has no stack trace. This keeps the record small.

### 5.1 Size limit

Matador serializes the record with `JSON.stringify` and measures the UTF-8 byte length before each write.

| Write | If the record is larger than `maxRecordBytes` |
|---|---|
| The first record, in `send()` | `send()` throws `RecordTooLargeError` before Matador publishes any envelope. |
| The `completed` record | Matador writes `failed` with `RecordTooLargeError` and no result. The message is acked. The message is not retried, because the callback has already run. |
| A progress write | Matador drops that progress value and logs a warning. The job continues. |

The `description` of `RecordTooLargeError` tells the developer to write large data to blob storage and to put a pointer in the payload or the result.

## 6. Lifecycle

### 6.1 Transitions

The pipeline has a `RetryDecision` with three actions: `retry`, `dead-letter` and `discard`. The tracker maps the events to statuses:

| When | New status |
|---|---|
| `send()`, before the publish | `queued` |
| The publish fails and Matador does not buffer the send | `failed`, with the publish error |
| Before the callback runs | `active` |
| The callback returns | `completed`, with `result` |
| The callback throws, and the decision is `retry` | `retrying`, with `error` |
| The decision is `dead-letter` or `discard` | `failed`, with `error` |
| `shouldProcess` rejects a redelivery (`idempotent: 'no'` or `'unknown'`) | `failed`, with `error` |

### 6.2 Notes

- Matador writes `queued` before the publish. So the subscriber always finds a record, even if the subscriber runs at once.
- A buffered send stays `queued` until the publish succeeds. `queued` means "accepted by Matador". It does not mean "in the broker".
- `discard` maps to `failed`. A discarded message did not run to completion, so the app must not show the message as done. `error.message` holds the discard reason.
- A message for a tracked subscriber with no `docket.trackingId` is not tracked. This happens for a message that a v4.2.0 dispatcher sent.

### 6.3 Progress

- `ctx.progress()` stores the latest value in memory and returns at once.
- Matador writes the latest value at most once in each `progressIntervalMs`. Only one progress write is in flight at a time for each record.
- When the callback finishes, Matador drops the pending progress value. The terminal write follows. If a progress write is in flight, Matador waits for that write before the terminal write.
- `percent` outside 0 to 100 is clamped to that range.

## 7. Components

### 7.1 `StatusStore` interface (core)

```ts
interface StatusStore {
  /** Returns all records for one trackingId, keyed by subscriber name. */
  getAll(trackingId: string): Promise<Readonly<Record<string, StatusRecord>> | undefined>;
  get(trackingId: string, subscriberName: string): Promise<StatusRecord | undefined>;
  /** Writes the record. Replaces any record with the same trackingId and subscriber name. */
  set(record: StatusRecord): Promise<void>;
  /** Deletes all records for one trackingId. */
  delete(trackingId: string): Promise<void>;
  /** Optional: deletes records with updatedAt before olderThan. Returns the count. */
  cleanup?(olderThan: Date): Promise<number>;
}
```

The interface follows the `CheckpointStore` pattern. It has no atomic operations in v1.

### 7.2 `MemoryStatusStore` (core)

A `Map` from `trackingId` to a `Map` from subscriber name to record. It implements `cleanup`. It is for tests and local development.

### 7.3 `RedisStatusStore` (`@zdavison/matador-redis`)

- One Redis hash for each `trackingId`: key `<prefix><trackingId>`, one field for each subscriber name, the value is the JSON record.
- `set` runs `HSET` and `PEXPIRE` in one `MULTI`. Each write refreshes the TTL.
- `getAll` runs `HGETALL`. `get` runs `HGET`. `delete` runs `DEL`.
- Redis TTL replaces `cleanup`, so the Redis store does not implement `cleanup`.
- Constructor options:
  - `client`: a Redis client. The store uses a small client interface that matches `redis` v4 (`node-redis`). meetsone uses `redis` `^4.6.5`.
  - `ttlMs`: required. The store does not choose a retention period for the app.
  - `keyPrefix`: optional. Default: `matador:status:`.
- `redis` is a peer dependency of the package. Core Matador has no Redis dependency.

### 7.4 `StatusTracker` (core, internal)

Matador makes a `StatusTracker` only if `statusStore` is set. The tracker is the only unit that writes records. It owns:

- the creation of the first records;
- the transition rules in section 6.1;
- the size check in section 5.1;
- the progress coalescing in section 6.3;
- the error handling in section 8.

These units call the tracker:

| Caller | Call |
|---|---|
| `FanoutEngine.send()` | Create the `queued` records before the publish. Write `failed` if the publish fails and Matador does not buffer the send. |
| `ProcessingPipeline` | Write `active`, `completed`, `retrying` and `failed`. Give the `progress` function for the callback context. |
| `Matador.getStatus()` | Read records. |

If the tracker does not exist, these call sites do nothing new.

## 8. Error handling

### 8.1 Store errors

| Where | Behavior |
|---|---|
| The first write, in `send()` | `send()` throws `StatusStoreError` and publishes nothing. A job that the app cannot poll must not start. |
| `active`, `retrying` or a terminal write, in the consumer | Matador logs the error and calls the new hook `onStatusStoreError`. Message handling does not change: the ack, retry and dead-letter steps run as they do today. |
| A progress write | Matador logs the error and calls `onStatusStoreError`. The job continues. |

The status record is for observation. A store outage must not make a job retry or fail. So after a store outage, a record can show an old status. The record then expires through the store TTL.

### 8.2 Known race in v1

With plain `get` and `set`, two processes can write the same record. For example:

1. Worker 1 stalls.
2. The broker redelivers the message to worker 2.
3. Worker 2 writes `completed`.
4. Worker 1 then writes `retrying` or `failed`.

The record then shows the wrong final status. In one process, the tracker writes nothing after a terminal status, so the race needs two processes. A later version can add a conditional write to the store interface.

### 8.3 New errors

All errors extend `MatadorError` and have a `description`.

| Error | Cause |
|---|---|
| `StatusStoreRequiredError` | A tracked subscriber or `getStatus` with no `statusStore` |
| `RecordTooLargeError` | A record larger than `maxRecordBytes` |
| `ResultNotSerializableError` | A tracked callback returned a value that `JSON.stringify` cannot serialize |
| `StatusStoreError` | The store threw during the first write in `send()`. Wraps the cause. |

### 8.4 New hook

```ts
onStatusStoreError?(context: {
  readonly trackingId: string;
  readonly subscriberName: string;
  readonly operation: 'create' | 'update' | 'progress' | 'read';
  readonly error: Error;
}): void | Promise<void>;
```

## 9. Testing

- **`StatusTracker` unit tests** with `MemoryStatusStore`: each transition in section 6.1, the size limit, the progress coalescing, and each row of section 8.1 with a store that throws.
- **A shared store contract test suite.** The same tests run against `MemoryStatusStore` and `RedisStatusStore`.
- **End-to-end tests with `LocalTransport`:** a tracked send that completes, a send that retries and then completes, a send that dead-letters, a fanout with two tracked subscribers, and a send with no tracked subscriber.
- **No-store test:** the existing test suite passes with no change when no store is configured.
- **Integration tests** for `RedisStatusStore` with Testcontainers Redis. They run with `bun run test:integration`, as the RabbitMQ tests do.
- **`matador-nest` tests:** `getStatus` through `MatadorService`, and `track: true` through `@OnMatadorEvent`.

## 10. Open decisions

These defaults were chosen by Claude during the spec write-up. They were not discussed. Please confirm or change them.

1. **`StatusStoreRequiredError` at startup.** A tracked subscriber with no store makes the constructor throw. The other option is a warning, and the subscriber is then not tracked.
2. **A store error in `send()` throws.** The other option is to publish with no record and report the error in `SendResult.errors`.
3. **A store error in the consumer does not change message handling.** The record can show an old status after an outage.
4. **`discard` maps to `failed`.** The other option is a separate status, `discarded`.
5. **A result that is too large gives `failed`, but the message is acked and not retried.**
6. **`progress` exists on the context of all subscribers** and does nothing if the subscriber is not tracked. The other option is `progress` only on the type of a tracked subscriber.
7. **The result type is checked at runtime only.** The type system does not require `JsonSerializable` for a tracked callback.
8. **`RedisStatusStore` requires `ttlMs`,** with no default.
9. **The Redis store targets the `redis` v4 client interface.** `ioredis` users write a small adapter.
10. **A duplicate caller-supplied `trackingId` replaces the old records.**
