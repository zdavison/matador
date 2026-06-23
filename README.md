# Matador

![image](./assets/logo-small.png)

An opinionated, batteries-included framework for using event transports (e.g. `RabbitMQ`) with a lot of useful conventions built in.

## Table of Contents

- [Vision](#vision)
- [History](#history)
- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
  - [Define a MatadorEvent](#define-a-matadorevent)
  - [Define a Subscriber](#define-a-subscriber)
  - [Define a Schema](#define-a-schema)
  - [Instantiate Matador and send events](#instantiate-matador-and-send-events)
- [Concepts](#concepts)
  - [MatadorEvent](#matadorevent)
  - [Subscriber](#subscriber)
  - [Schema](#schema)
  - [Envelope](#envelope)
  - [Docket](#docket)
  - [Fanout](#fanout)
  - [Transport](#transport)
  - [Topology](#topology)
  - [Codec](#codec)
  - [Config](#config)
  - [Hooks](#hooks)
  - [idempotent](#idempotent)
  - [ResumableSubscriber](#resumablesubscriber)
- [Why it works this way](#why-it-works-this-way)
  - [Sending one message will result in a unique message per subscriber](#sending-one-message-will-result-in-a-unique-message-per-subscriber)
  - [You are working in a monorepo](#you-are-working-in-a-monorepo)
  - [You want at-least-once delivery](#you-want-at-least-once-delivery)
- [Logging](#logging)
- [Errors](#errors)
- [Other features](#other-features)
  - [universalMetadata](#universalmetadata)
  - [Schema plugins](#schema-plugins)
  - [DoRetry & DontRetry](#doretry--dontretry)
  - [LocalTransport](#localtransport)
  - [MultiTransport](#multitransport)
  - [enabled() hook for Subscriber](#enabled-hook-for-subscriber)
  - [importance](#importance)
  - [Delayed messages](#delayed-messages)
- [CLI](#cli)

# Vision

Matador aims to provide a ready-to-use framework for dispatching messages and moving work off your API servers onto workers.
It is an _opinionated_ library that may-or-may not suit your expectations about how to work with queues.

You can use it to very quickly set up a framework for your events, with battle-tested conventions, edge-case coverage, and observability baked in.

It works best under the following conditions.

- You want the queue topology to be created and managed for you.
- You are working in a monorepo, where you can easily share code.
- You want a 1:N fanout strategy, where each _subscriber_ receives a unique copy of each event.
- You want `at-least-once` delivery semantics.

These conditions are explained later.

# History

Matador has been used at [MeetsMore Inc](https://meetsmore.com/) for over 2 years to publish, consume, and observe 1,000,000+ successful events per day.

This version of Matador is Matador V2, and was re-written from the ground up using our learnings over that 2 year period.

# Features

- Conventional types for `Event` and `Subscriber`
- Map an event to a list of subscribers that will consume it.
- Each subscriber receives a unique copy of each event that can be individually retried and re-queued.
- Automatically create and manage your queue topology.
- Automatic reconnection when connection drops.
- Async hooks for changing behaviour at runtime.
- Async lifecycle hooks for plugging into your observability platform.
- Required fields enforcing good, observable documentation practices.
- `idempotency` declaration for subscribers, allowing you to indicate if events can be retried safely or not.
- `metadata` separated from event payloads, to clearly separate data used for logic, and data that is just used for debugging.
- Gracefully wait for pending enqueues and subscribers to complete work before shutting down.
- Fallback to a different message broker if enqueuing fails.
- Local broker for executing code locally (useful for fallback).
- Retry control flow errors (`DoRetry` and `DontRetry`), so subscribers can manually dictate retry logic.
- Clear, documented, actionable errors for all error cases.
- Poisoned message detection.
- Resumable subscribers.

# Architecture

```
                              DISPATCH PATH (Producer)
┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  matador.send(Event, data)                                                   │
│          │                                                                   │
│          ▼                                                                   │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                  │
│  │    SCHEMA    │────▶│    FANOUT    │────▶│    CODEC     │                  │
│  │              │     │              │     │              │                  │
│  │ Event → Subs │     │  1 Event →   │     │ Envelope →   │                  │
│  │   mapping    │     │ N Envelopes  │     │    bytes     │                  │
│  └──────────────┘     └──────────────┘     └──────┬───────┘                  │
│                                                   │                          │
│                                                   ▼                          │
│                                           ┌──────────────┐                   │
│                                           │  TRANSPORT   │───────▶ Broker    │
│                                           └──────────────┘                   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

                              CONSUME PATH (Consumer)
┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  Broker ─────▶ TRANSPORT                                                     │
│                    │                                                         │
│                    ▼                                                         │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │                            PIPELINE                                   │   │
│  │                                                                       │   │
│  │  ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐       │   │
│  │  │ CODEC  │──▶│ SCHEMA │──▶│ RETRY  │──▶│CALLBACK│──▶│ ACK /  │       │   │
│  │  │        │   │        │   │ POLICY │   │        │   │ NACK   │       │   │
│  │  │ bytes→ │   │ lookup │   │        │   │execute │   │        │       │   │
│  │  │Envelope│   │  sub   │   │ check  │   │  sub   │   │ result │       │   │
│  │  └────────┘   └────────┘   └────────┘   └────────┘   └────────┘       │   │
│  │                                                                       │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

                            SUPPORTING COMPONENTS
┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   TOPOLOGY   │  │    HOOKS     │  │   SHUTDOWN   │  │  CHECKPOINT  │      │
│  │              │  │              │  │   MANAGER    │  │    STORE     │      │
│  │ Queue config │  │  Lifecycle   │  │   Graceful   │  │  io() cache  │      │
│  │ & structure  │  │  callbacks   │  │    drain     │  │  (optional)  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

COMPONENT RESPONSIBILITIES
─────────────────────────────────────────────────────────────────────────────────
SCHEMA         Maps EventKey → [EventClass, Subscriber[]]
FANOUT         Creates unique envelope per subscriber (1:N distribution)
CODEC          Serializes/deserializes envelopes (JSON by default)
TRANSPORT      Broker abstraction (RabbitMQ, Local, Multi)
PIPELINE       Message processing: decode → validate → execute → ack/nack
RETRY POLICY   Determines retry behavior based on attempts & idempotency
TOPOLOGY       Defines queue structure (main, retry, dead-letter queues)
HOOKS          Lifecycle callbacks for observability & dynamic config
SHUTDOWN       Manages graceful shutdown with in-flight message draining
CHECKPOINT     Persists io() results for resumable subscribers
```

# Getting Started

### Define a MatadorEvent

```ts
export class UserLoggedInEvent extends MatadorEvent {
  static readonly key = 'user.logged-in';                   // The unique event name (used to route events).
  static readonly description = 'Fired when a user logs.';  // A description of when the event is triggered.

  constructor(
    public data: {                                          // The data payload used by business logic.
      userId: string;
      username: string;
    },
    public metadata: {                                      // Additional data helpful for logging or debugging.
      loginMethod: 'email' | 'social'
    }
  ) {
    super();
  }
}
```

### Define a Subscriber

```ts
const detectLoginFraud: Subscriber<UserLoggedInEvent> = {
  name: 'detect-login-fraud',                                           // Unique subscriber name.
  description: 'Send an email if unusual login behaviour is detected.'  // A description of what the subscriber does.
  idempotent: 'no'                                                      // Dictates if the operation can be safely retried.
  targetQueue: 'compliance-jobs-worker'                                 // The queue this subscriber consumes from.
  callback: async (event: EnvelopeOf<UserLoggedIn>) => { /** process event */ }
}
```

### Define a Schema

```ts
const myMatadorSchema = MatadorSchema = {
  [UserLoggedIn.key]: [UserLoggedIn, bind([ detectLoginFraud, logEventToBigQuery ])]
}
```

### Instantiate Matador and send events

```ts
// Create topology
const topology = TopologyBuilder.create()
  .withNamespace('my-app')
  .addQueue('events')
  .build();

// Create transport
const transport = new RabbitMQTransport({
  url: 'amqp://localhost',
  connectionName: 'my-app',
});

// Create Matador instance
const matador = new Matador({
  transport,
  topology,
  schema: myMatadorSchema,
  consumeFrom: ['events'],  // Queues to consume from (optional, empty = producer only)
});

await matador.start();
```

```ts
await matador.send(UserLoggedInEvent, { userId: '123', username: 'john' });
```

# Concepts

### `MatadorEvent`

A definition of an event. You extend `MatadorEvent` in order to create the _template_ for your event.

### `Subscriber`

A subscriber defines the logic that will execute when an event is consumed.
It also defines details about the event that are used to _fanout_ the event.

```ts
/**
 * Standard subscriber definition with standard callback.
 */
export interface StandardSubscriber<T extends MatadorEvent> {
  /** Human-readable name for the subscriber */
  readonly name: string;

  /** A description of what the event does. */
  readonly description: string

  /** Idempotency declaration for retry handling (non-resumable) */
  readonly idempotent?: 'yes' | 'no' | 'unknown' | undefined;

  /** Route this subscriber's events to a specific queue */
  readonly targetQueue?: string | undefined;

  /** Importance level for monitoring and alerting */
  readonly importance?: Importance | undefined;

  /** Feature flag function to conditionally enable/disable the subscriber */
  readonly enabled?: (() => boolean | Promise<boolean>) | undefined;

  /** Callback function to execute when event is received */
  readonly callback: StandardCallback<T['data']>;
}
```

If you want to reference a subscriber which is defined in another codebase, you can use `SubscriberStub`:

```ts
/**
 * Subscriber stub for multi-codebase scenarios where subscriber implementation
 * is in a remote service. Declares the subscriber contract without providing
 * the callback.
 */
export interface SubscriberStub extends StandardSubscriberOptions {
  /** Human-readable name for the subscriber */
  readonly name: string;

  /** Indicates this is a stub without implementation */
  readonly isStub: true;
}
```

### `Schema`

Defines the mapping of `Events` to `Subscribers`.
```ts
const myMatadorSchema = MatadorSchema = {
  // user.logged-in -> UserLoggedInEvent (class) -> subscribers
  [UserLoggedInEvent.key]: [UserLoggedInEvent, bind([ detectLoginFraud, logEventToBigQuery ])]
}
```

### `Envelope`

Combines both your event data + metadata with a `Docket`.
Subscribers receive an `EnvelopeOf<MyEvent>`, which is an envelope with a `data` and `metadata` field typed according to your `MatadorEvent` subclass.

```ts
/**
 * Message envelope containing the event data and routing/observability metadata.
 * This is the transport-agnostic message format used throughout Matador.
 */
export interface Envelope<T = unknown> {
  /** Unique message ID (UUID v4) */
  readonly id: string;

  /** The event data */
  readonly data: T;

  /** Routing, processing state, and observability metadata */
  readonly docket: Docket;
}

/**
 * Helper type to get the envelope type for a subscriber callback.
 * Extracts the data type from a MatadorEvent and wraps it in an Envelope.
 *
 * @example
 * async callback(envelope: EnvelopeOf<MyEvent>) {
 *   console.log(envelope.data.someField); // Type-safe access
 * }
 */
export type EnvelopeOf<T extends MatadorEvent> = Envelope<T['data']>;
```

### `Docket`

> A docket is a commercial document accompanying shipped goods, detailing its contents.

Contains all metadata about an `Envelope`.

| Property            | Documentation                                                                                                                                                                                                                                          |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Routing**         |                                                                                                                                                                                                                                                        |
| `eventKey`          | Event key for routing                                                                                                                                                                                                                                  |
| `eventDescription`  | Human-readable description of the event (for observability/logging)                                                                                                                                                                                    |
| `targetSubscriber`  | Target subscriber name for 1:1 routing                                                                                                                                                                                                                 |
| `originalQueue`     | Original queue before any dead-letter routing                                                                                                                                                                                                          |
| `scheduledFor`      | Scheduled processing time for delayed messages (ISO 8601 string)                                                                                                                                                                                       |
| **Processing State**|                                                                                                                                                                                                                                                        |
| `attempts`          | Attempt counter managed by Matador (1-based). Incremented on each retry. Used when transport doesn't track attempts.                                                                                                                                   |
| `createdAt`         | When the envelope was first created (ISO 8601 string)                                                                                                                                                                                                  |
| `firstError`        | Error message from first failure (for debugging)                                                                                                                                                                                                       |
| `lastError`         | Error message from most recent failure                                                                                                                                                                                                                 |
| **Observability**   |                                                                                                                                                                                                                                                        |
| `importance`        | Importance level for monitoring                                                                                                                                                                                                                        |
| `correlationId`     | Correlation ID for request tracing                                                                                                                                                                                                                     |
| `metadata`          | Custom metadata provided by the application, includes `universalMetadata` |

### `Fanout`

The practice of taking a single published event and creating `N` concrete events, where `N` is the number of subscribers mapped to that event in your `MatadorSchema`

### `Transport`

A representation of a message broker or event backend. e.g `RabbitMQ`, `BullMQ`, `Temporal`, etc.
Currently, only `RabbitMQ` is supported.

The RabbitMQ transport publishes on a **confirm channel**: `send()` resolves
only after the broker acknowledges the message, and rejects with
`TransportSendError` on a broker nack or when no confirm arrives within
`publishTimeoutMs` (default: 5000ms). Callers can await delivery
confirmation at the call site (or rely on `MultiTransport` fallback) rather
than treating publishes as fire-and-forget.

### `Topology`

The structure of queues, exchanges, topics, etc that **Matador** will create and manage.
For RabbitMQ, for example, the topology refers to the exchanges, queues, and bindings of those entities.
**Matador** creates and manages topology for you.

There is a generic `Topology` definition in Matador, which you can use to describe the queues you want in a limited fashion.

Each `Transport` then translates that generic `Topology` into the concrete queues, exchanges, etc.

The topology that **Matador** creates looks like this by default.

![image](./assets/matador-rabbitmq-configuration-simple.drawio.png)

#### Retry Queue

When events fail, they are pushed into the retry queue. They will then be re-delivered after their TTL expires.

#### Unhandled Queue

In **Matador**, _unhandled_ refers to a message that was consumed by a worker but is not in your `MatadorSchema`.
This can happen during deployments, where your publisher has already been deployed, but your consumer has not, and therefore your consumer doesn't know about the event yet.
Since the most common reason for this (99.99% of the time in our experience) is simply deployment timing, _unhandled_ events are automatically retried using the same retry policy as failed messages. After max attempts are exhausted, they are sent to the unhandled dead-letter queue for inspection.

#### Undeliverable Queue

Undeliverable is a conventional term for messages that could not be successfully processed.
After a configured amount of retries (default: 3 attempts), a message will be sent to the undeliverable queue and left there for inspection.
By default, this queue is not cleared by **Matador**. You can optionally configure a `maxLength` on the dead-letter queue via the `DeadLetterQueueConfig`.

### `Codec`

Handles the _encoding_ and _decoding_ of events that are published and consumed.
By default, `JSONCodec` is used, which serializes and de-serializes events using JSON.

You can create other `Codec`s if you wish (for example a `MessagePackCodec`).

When using `RabbitMQ`, `RabbitMQCodec` is used, which still uses `JSONCodec` internally, but uses AMQP headers to store `Docket` information.

### `Config`

**Matador** is configured via the `MatadorConfig` interface:

| Property          | Required | Description                                                                                              |
| ----------------- | -------- | -------------------------------------------------------------------------------------------------------- |
| `transport`       | Yes      | Transport for message delivery (e.g., `RabbitMQTransport`, `LocalTransport`, `MultiTransport`)           |
| `topology`        | Yes      | Topology configuration defining queues, dead-letter, and retry settings                                  |
| `schema`          | Yes      | Event schema mapping event keys to event classes and subscribers                                         |
| `consumeFrom`     | No       | Array of queue names to consume from (empty = producer only mode)                                        |
| `codec`           | No       | Custom codec for serialization (defaults to `JsonCodec`)                                                 |
| `retryPolicy`     | No       | Custom retry policy (defaults to `StandardRetryPolicy` with 3 max attempts)                              |
| `shutdownConfig`  | No       | Shutdown configuration (drain timeout, etc.)                                                             |
| `checkpointStore` | No       | Checkpoint store for resumable subscribers (required for persisting `io()` results across retries)       |

**Topology** is configured via `TopologyBuilder`:

| Property       | Default | Description                                              |
| -------------- | ------- | -------------------------------------------------------- |
| `namespace`    | -       | Prefix for all queue names (required)                    |
| `queues`       | -       | Array of queue definitions (at least one required)       |
| `deadLetter`   | enabled | Dead-letter queue configuration (unhandled/undeliverable)|
| `retry`        | enabled | Retry queue configuration (defaultDelayMs: 1000ms, maxDelayMs: 5 minutes) |

**Queue Definition** options:

| Property          | Default | Description                                                |
| ----------------- | ------- | ---------------------------------------------------------- |
| `name`            | -       | Queue name (required)                                      |
| `concurrency`     | -       | Number of concurrent consumers for this queue              |
| `consumerTimeout` | -       | Consumer timeout in milliseconds                           |
| `priorities`      | false   | Enable priority support if transport allows                |
| `exact`           | false   | See [exact-queues](#exact-queues).  |

**Retry Policy** defaults (`StandardRetryPolicy`):

| Property           | Default  | Description                                              |
| ------------------ | -------- | -------------------------------------------------------- |
| `maxAttempts`      | 3        | Maximum retry attempts before dead-lettering             |
| `baseDelay`        | 1000ms   | Base delay between retries                               |
| `maxDelay`         | 300000ms | Maximum delay (5 minutes)                                |
| `backoffMultiplier`| 2        | Multiplier for exponential backoff                       |
| `maxDeliveries`    | 5        | Max native delivery count before poison detection        |

**Checkpoint Store** options (for resumable subscribers):

| Store                  | Description                                                                            |
| ---------------------- | -------------------------------------------------------------------------------------- |
| `MemoryCheckpointStore`| In-memory store for testing/development. Checkpoints are lost on process restart.      |
| `NoOpCheckpointStore`  | No-op store (default). `io()` works but doesn't persist across retries.                |

For production, implement `CheckpointStore` interface with Redis or another persistent store.

### `Hooks`

Hooks are passed as the second argument to the `Matador` constructor and provide lifecycle callbacks for observability and dynamic configuration.

**Lifecycle Hooks:**

| Hook                     | Description                                                                           |
| ------------------------ | ------------------------------------------------------------------------------------- |
| `logger`                 | Logger instance for internal Matador logging (defaults to console)                    |
| `onEnqueueSuccess`       | Called when an event is successfully enqueued                                         |
| `onTransportFallback`    | Called when transport fallback occurs (MultiTransport with fallbackEnabled)           |
| `onEnqueueError`         | Called when enqueue fails completely                                                  |
| `onWorkerWrap`           | Wraps entire worker processing (for APM context like DataDog, NewRelic)               |
| `onWorkerBeforeProcess`  | Called before processing begins                                                       |
| `onWorkerSuccess`        | Called after successful processing                                                    |
| `onWorkerError`          | Called after processing error                                                         |
| `onDecodeError`          | Called when message decoding fails                                                    |
| `onConnectionStateChange`| Called when transport connection state changes                                        |

**Dynamic Configuration Hooks:**

| Hook                     | Description                                                                           |
| ------------------------ | ------------------------------------------------------------------------------------- |
| `loadUniversalMetadata`  | Loads metadata to add to all envelopes (e.g., correlationId from AsyncLocalStorage)   |
| `getRetryDelay`          | Dynamic retry delay lookup                                                            |
| `getAttempts`            | Dynamic max attempts lookup                                                           |
| `getMaxDeliveries`       | Dynamic max deliveries (poison threshold) lookup                                      |

**Checkpoint Hooks (Resumable Subscribers):**

| Hook                     | Description                                                                           |
| ------------------------ | ------------------------------------------------------------------------------------- |
| `onCheckpointLoaded`     | Called when a checkpoint is loaded for a retry                                        |
| `onCheckpointHit`        | Called when an `io()` operation uses a cached value                                   |
| `onCheckpointMiss`       | Called when an `io()` operation executes (cache miss)                                 |
| `onCheckpointCleared`    | Called when a checkpoint is cleared (success or dead-letter)                          |

### `idempotent`

> In programming, an idempotent operation is one that can be performed multiple times with the same result as if it were done only once.

In **Matador**, `idempotent` has a very close meaning, but ultimately just means that an event is _allowed_ to be retried automatically by **Matador**.

| Value       | Meaning                                                            | Retry Behavior                     |
| ----------- | ------------------------------------------------------------------ | ---------------------------------- |
| `'yes'`     | Subscriber is manually idempotent (safe to retry)                  | Retries allowed                    |
| `'no'`      | Subscriber is NOT idempotent (unsafe to retry)                     | Dead-letter on redelivery          |
| `'unknown'` | Idempotency not determined (default)                               | Dead-letter on redelivery          |
| `'resumable'` | Subscriber uses `io()` for checkpoint-based idempotency          | Retries allowed, checkpoint loaded |

### `ResumableSubscriber`

> ⚠️ This feature is experimental and not tested in production. 

Matador supports durable / resumable subscribers.

This allows subscribers that fail to resume where they left off, allowing you to retry subscribers that otherwise would not be idempotent.

This requires a persistent `CheckpointStore` to be configured (e.g., Redis, database, etc.).

When using `idempotent: 'resumable'`, you can use the `io` function argument to wrap side-effects, which will be cached when successful.

On subsequent retries, those successful `io` calls will return the cached value, instead of executing again.

```ts
import { MemoryCheckpointStore } from '@zdavison/matador';

// Configure Matador with a checkpoint store
const matador = new Matador({
  transport,
  topology,
  schema,
  consumeFrom: ['events'],
  checkpointStore: new MemoryCheckpointStore(), // Use RedisCheckpointStore in production
});

// Define a resumable subscriber
const processOrder: Subscriber<OrderPlacedEvent> = {
  name: 'process-order',
  idempotent: 'resumable',  // Enables the io() function
  callback: async (envelope, { io }) => {
    const { orderId, userId, amount } = envelope.data;

    // Step 1: Reserve inventory (cached on retry)
    const reservation = await io('reserve-inventory', async () => {
      return await inventoryService.reserve(orderId);
    });

    // Step 2: Charge payment (cached on retry)
    const payment = await io('charge-payment', async () => {
      return await paymentService.charge(userId, amount);
    });

    // Step 3: Send confirmation email (cached on retry)
    await io('send-confirmation', async () => {
      await emailService.send(userId, {
        subject: 'Order Confirmed',
        reservationId: reservation.id,
        transactionId: payment.transactionId,
      });
    });
  },
};
```

If the subscriber crashes after charging the payment but before sending the email:
- On retry, `reserve-inventory` returns the cached reservation (no duplicate reservation)
- On retry, `charge-payment` returns the cached payment (no double charge)
- `send-confirmation` executes fresh (no cache entry exists)

You can also use `all()` to execute multiple `io()` operations in parallel:

```ts
const enrichUserData: Subscriber<UserCreatedEvent> = {
  name: 'enrich-user-data',
  idempotent: 'resumable',
  callback: async (envelope, { io, all }) => {
    const { userId } = envelope.data;

    // Fetch multiple data sources in parallel (each with a unique key)
    const [profile, preferences, history] = await all([
      ['fetch-profile', async () => await profileService.get(userId)],
      ['fetch-preferences', async () => await prefsService.get(userId)],
      ['fetch-history', async () => await historyService.get(userId)],
    ]);

    // Use the enriched data
    await io('save-enrichment', async () => {
      await enrichmentService.save({ userId, profile, preferences, history });
    });
  },
};
```

Each `io()` key must be unique within the subscriber and stable across retries. Use descriptive names like `'fetch-user'`, `'send-email'`, or `'charge-payment'`.

# Why it works this way

Since **Matador** is opinionated, we should explain the rationale behind our choices.

### Sending one message will result in a unique message _per subscriber_.

When you send an event in **Matador**, there is a unique copy of that event sent with different `Docket` details.

This is automatic, and referred to as _fanout_ within **Matador**.

We chose this model because while it results in `N` real events per published event, it allows each real event to be retried and managed individually. A single failed subscriber does not affect any others or result in any additional cognitive complexity about the system.

This however does have the impact of requiring the publisher to know about the subscriber. 
This is easy in a monorepo environment, but for remote subscribers, you can use `SubscriberStub`.

### You are working in a monorepo

While it is possible to use **Matador** outside of a monorepo environment, it is designed to work in a situation where you can easily share code between your applications.
This is because both the _publisher_ and the _consumer_ need to know about the event _schema_ (e.g. which events map to which subscribers).

You can either share the code for your `MatadorSchema`, or you can make your workers simply be different instances of the same codebase (with different configuration).

```ts
// Example: Dynamic configuration based on environment
const matador = new Matador(
  {
    transport,
    topology,
    schema: myMatadorSchema,
    // Only consume in worker mode
    consumeFrom: process.env.WORKER_MODE === 'true' ? ['events'] : [],
  },
  {
    // Dynamic hooks can use runtime config
    getAttempts: async (envelope) => {
      // High-importance events get more retries
      if (envelope.docket.importance === 'must-investigate') {
        return 5;
      }
      return 3;
    },
  }
);
```

### You want a conventional queue topology to be managed for you

Because making queue topology choices can be difficult, we decided to create a reasonable, good-for-most-use-cases design that covers the basic needs.

In our org, we have been using this topology for 2+ years, and it's suited most of our needs. We create new queues, but the 'infrastructure queues' (e.g. dead-letter, retry, etc), we don't need to touch.

So that you can get up and running quickly, **Matador** makes a lot of assumptions about how events should be routed and retried, and you're buying into those conventions by using **Matador**.

If you want precise control over your queue topology, **Matador** may not be suitable for you.

### You want `at-least-once` delivery.

There are two options for delivery in an event system.

- `at-least-once`: Every message is guaranteed to be delivered once, but may deliver more times in some edge-cases.
- `at-most-once`: Every message is guaranteed to be delivered at most once, but may not be delivered at all in some edge cases.

Any system that promises `exactly-once` is lying to you, because there are always timeout scenarios in a distributed system that mean that there may still be a very small possibility of a message being either delivered multiple times (`at-least-once`) or not at all (`at-most-once`).

The choice essentially comes down to when you `ack` (acknowledge) a message.

- **Before processing**: `at-most-once`. If the worker crashes after ack but before completing, the message is lost.
- **After processing**: `at-least-once`. If the worker crashes after completing but before ack, the message may be redelivered.

**Matador** uses `at-least-once` delivery by acknowledging messages only after successful processing.

# Logging

**Matador** uses a pluggable logger interface. You can provide your own logger via the `logger` hook:

```ts
const matador = new Matador(config, {
  logger: {
    debug: (msg, ...args) => myLogger.debug(msg, ...args),
    info: (msg, ...args) => myLogger.info(msg, ...args),
    warn: (msg, ...args) => myLogger.warn(msg, ...args),
    error: (msg, ...args) => myLogger.error(msg, ...args),
  },
});
```

### Log Messages by Level

**Debug:**

| Message                                                   | Context                                        |
| --------------------------------------------------------- | ---------------------------------------------- |
| `[Matador] 🔌 Delayed message exchange plugin detected`   | RabbitMQ delayed message plugin is available   |

**Info:**

| Message                                                                       | Context                                            |
| ----------------------------------------------------------------------------- | -------------------------------------------------- |
| `[Matador] 🟢 matador-nest initialized.`                                      | MatadorService initialized (matador-nest)          |
| `[Matador] 🟢 Started`                                                        | Matador started consuming (matador-nest)           |
| `[Matador] ⏳ Graceful shutdown initiated, draining in-flight messages`       | Shutdown started, waiting for messages to complete |
| `[Matador] 🟢 Shutdown complete`                                              | Matador shutdown finished (matador-nest)           |

**Warn:**

| Message                                                                          | Context                                                    |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `[Matador] 🟡 RabbitMQ delayed message exchange plugin not available...`         | RabbitMQ plugin not installed, delayed messages disabled   |
| `[Matador] 🟡 Shutdown timeout reached with N events still processing`           | Graceful shutdown timed out with pending events            |
| `[Matador] 🟡 Shutdown timeout reached after Nms, some messages may not...`      | Shutdown timeout in matador-nest                           |
| `[Matador] 🟡 Hook onWorkerWrap threw an error`                                  | The `onWorkerWrap` hook threw an exception                 |
| `[Matador] 🟡 Hook loadUniversalMetadata threw an error`                         | The `loadUniversalMetadata` hook threw an exception        |
| `[Matador] 🟡 Hook getRetryDelay threw an error`                                 | The `getRetryDelay` hook threw an exception                |
| `[Matador] 🟡 Hook getAttempts threw an error`                                   | The `getAttempts` hook threw an exception                  |
| `[Matador] 🟡 Hook getMaxDeliveries threw an error`                              | The `getMaxDeliveries` hook threw an exception             |
| `[Matador] 🟡 Hook {hookName} threw an error`                                    | Any other lifecycle hook threw an exception                |

**Error:**

| Message                                                                          | Context                                                    |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `[Matador] 🔴 RabbitMQ connection error`                                         | RabbitMQ connection encountered an error                   |
| `[Matador] 🔴 RabbitMQ publish channel error`                                    | RabbitMQ publish channel encountered an error              |
| `[Matador] 🔴 Handler error in message processing`                               | Subscriber callback threw an unhandled exception           |
| `[Matador] 🔴 Failed to enqueue delayed message`                                 | LocalTransport failed to enqueue a delayed message         |

For application-level logging (event processing, success/failure tracking), use the lifecycle hooks (`onWorkerSuccess`, `onWorkerError`, `onEnqueueSuccess`, etc.).

# Errors

All errors in **Matador** extend `MatadorError` and include a `description` field explaining the cause and recommended action.

| Error                             | Description                                                                                |
| --------------------------------- | ------------------------------------------------------------------------------------------ |
| `NotStartedError`                 | Matador has not been started. Call `matador.start()` first.                                |
| `ShutdownInProgressError`         | Matador is shutting down and will not accept new events.                                   |
| `TransportNotConnectedError`      | Transport is not connected to the message broker.                                          |
| `TransportClosedError`            | Transport has been closed (during shutdown).                                               |
| `AllTransportsFailedError`        | All transports in a fallback chain failed.                                                 |
| `SomeSendError`                | One or more subscriber publishes failed during `send()`. Contains an `errors` array with per-failure details (`subscriberName`, `queue`, `error`). |
| `TransportSendError`              | Failed to send a single message through the transport. Wrapped inside `SomeSendError.errors[n].error` as the underlying cause.                                             |
| `DelayedMessagesNotSupportedError`| Delayed messages requested but transport doesn't support them.                             |
| `EventNotRegisteredError`         | Event type is not registered in the schema.                                                |
| `SubscriberNotRegisteredError`    | Subscriber is not registered for this event.                                               |
| `NoSubscribersExistError`         | Event has no subscribers registered.                                                       |
| `InvalidSchemaError`              | Schema configuration is invalid.                                                           |
| `SubscriberIsStubError`           | A SubscriberStub was registered in a consuming schema.                                     |
| `LocalTransportCannotProcessStubError` | LocalTransport cannot process events for SubscriberStubs.                             |
| `QueueNotFoundError`              | Queue does not exist or has not been created.                                              |
| `InvalidEventError`               | Event is invalid or missing required fields.                                               |
| `MessageMaybePoisonedError`       | Message redelivered too many times (possible poison message).                              |
| `IdempotentMessageCannotRetryError` | Non-idempotent subscriber received a redelivered message.                                |
| `TimeoutError`                    | Operation timed out before completing.                                                     |

**Checkpoint Errors** (resumable subscribers):

| Error                   | Description                                                                                      |
| ----------------------- | ------------------------------------------------------------------------------------------------ |
| `DuplicateIoKeyError`   | Same `io()` key used multiple times in a single subscriber execution. Keys must be unique.       |
| `CheckpointStoreError`  | Checkpoint store operation failed (get/set/delete). Check underlying storage connectivity.       |

**Retry Control Errors** (thrown by subscribers to control retry behavior):

| Error                | Description                                                                                    |
| -------------------- | ---------------------------------------------------------------------------------------------- |
| `DoRetry`            | Forces retry regardless of subscriber idempotency setting.                                     |
| `DontRetry`          | Prevents retry regardless of subscriber idempotency setting. Message goes to dead-letter.      |
| `EventAssertionError`| Assertion error that should never be retried. Goes directly to dead-letter queue.              |

# Other features

### `universalMetadata`

It is often the case that you want every event to have a consistent set of metadata.
**Matador** provides a hook `loadUniversalMetadata` which you can use to do this.

You can combine this with `asyncLocalStorage` to set session based metadata on your events.

Here is a real world example:

```ts
loadUniversalMetadata: () => {
  const store = asyncLocalStorage?.getStore()
  return {
    timestamp: new Date(),
    correlationId: store?.correlationId || null,
    userId: store?.userId || null,
  }
},
```

### Schema plugins

Sometimes, you want to run a subscriber on every event in your _schema_.
Instead of defining the subscriber mapping for every event, you can use `installPlugins` to add global subscribers:

```ts
import { installPlugins } from '@zdavison/matador';

const baseSchema: MatadorSchema = {
  [UserCreatedEvent.key]: [UserCreatedEvent, [sendWelcomeEmail]],
  [OrderPlacedEvent.key]: [OrderPlacedEvent, [processOrder]],
  [ChatMessageSentEvent.key]: [ChatMessageSentEvent, [notifyRecipient]],
};

const myMatadorSchema = installPlugins(baseSchema, [
  {
    subscriber: logToBigQuery,
    exclusions: [ChatMessageSentEvent.key], // Don't log chat messages
  }
]);
```

Each plugin defines a `subscriber` and optional `exclusions` array of event keys to skip.

### `DoRetry` & `DontRetry`

Sometimes, your subscribers may want to explicitly control if they should be retried or not.
This is useful in cases where a message is _sometimes_ idempotent, but in certain cases (e.g. error scenarios) it is not.

```ts
import { DoRetry, DontRetry } from '@zdavison/matador';

const processPayment: Subscriber<PaymentRequestedEvent> = {
  name: 'process-payment',
  idempotent: 'no', // Default: don't retry
  callback: async (envelope) => {
    try {
      await paymentGateway.charge(envelope.data.amount);
    } catch (error) {
      if (error.code === 'TEMPORARY_FAILURE') {
        // Gateway is temporarily unavailable, safe to retry
        throw new DoRetry('Payment gateway temporarily unavailable');
      }
      if (error.code === 'CARD_DECLINED') {
        // Permanent failure, don't retry
        throw new DontRetry('Card was declined');
      }
      throw error; // Default behavior based on idempotent setting
    }
  },
};
```

### `LocalTransport`

Matador includes a `LocalTransport`, which is an in-memory transport that will simply process events on the same machine.
This allows you to develop locally without having a message broker, then simply switch to using an actual message broker when you deploy.

### `MultiTransport`

`MultiTransport` allows you to declare a group of transports, and then switch between them at runtime.

You can also use configure it with `fallbackEnabled` (default: `true`), and if enqueuing a message fails, it can fallback to another queue system (in declared order).

You can combine this with `LocalTransport` to make messages that fail to enqueue execute locally, providing resilience against your message broker being unavailable or timing out.

```ts
import { MultiTransport, RabbitMQTransport, LocalTransport } from '@zdavison/matador';

const rabbitTransport = new RabbitMQTransport({
  url: 'amqp://localhost',
  connectionName: 'my-app',
});
const localTransport = new LocalTransport();

const transport = new MultiTransport(
  {
    transports: [rabbitTransport, localTransport],
    fallbackEnabled: true, // Default: true
  },
  {
    // Optional: dynamically select backend based on feature flags
    getDesiredBackend: async () => {
      if (process.env.SANDBOX === 'true') return 'local';
      const useLocal = await featureFlags.isEnabled('use-local-transport');
      return useLocal ? 'local' : 'rabbitmq';
    },
    // Optional: log when fallback occurs
    onEnqueueFallback: (ctx) => {
      console.warn(`Fallback from ${ctx.failedTransport} to ${ctx.nextTransport}: ${ctx.error.message}`);
    },
  }
);

const matador = new Matador({
  transport,
  topology,
  schema,
  consumeFrom: ['events'],
});
```

### `enabled()` hook for `Subscriber`

Sometimes, you want to enable / disable subscribers at runtime using feature flags.

Each subscriber accepts an `enabled()` callback, which is invoked before **publishing** an event for that subscriber. If `enabled()` returns `false`, the message is not sent for that subscriber (it's skipped during fanout). If the check fails with an error, the subscriber is treated as enabled.

```ts
const sendWelcomeEmail: Subscriber<UserCreatedEvent> = {
  name: 'send-welcome-email',
  idempotent: 'yes',
  enabled: async () => {
    // Check feature flag
    return await featureFlags.isEnabled('welcome-email-v2');
  },
  callback: async (envelope) => {
    await emailService.sendWelcome(envelope.data.email);
  },
};

// Sync enabled check is also supported
const legacySubscriber: Subscriber<UserCreatedEvent> = {
  name: 'legacy-handler',
  enabled: () => process.env.ENABLE_LEGACY === 'true',
  callback: async (envelope) => { /* ... */ },
};
```

### `importance`

`importance` is an optional field that allows you to note how critical a subscriber failure is.
This is useful for setting up alerts in your observability platform.

For example, if an analytics event fails, it is likely unimportant, but if a payment related subscriber fails, it warrants investigation and should trigger an alert.

Available importance levels:
- `'can-ignore'` - Failures are not critical (e.g., analytics, logging)
- `'should-investigate'` - Failures should be reviewed (default)
- `'must-investigate'` - Failures are critical and require immediate attention (e.g., payments)

```ts
const trackAnalytics: Subscriber<UserCreatedEvent> = {
  name: 'track-analytics',
  importance: 'can-ignore', // Don't alert on failures
  callback: async (envelope) => {
    await analytics.track('user_created', envelope.data);
  },
};

const processPayment: Subscriber<PaymentRequestedEvent> = {
  name: 'process-payment',
  importance: 'must-investigate', // Critical - alert immediately on failures
  callback: async (envelope) => {
    await paymentGateway.charge(envelope.data);
  },
};

// Use in hooks for alerting
const matador = new Matador(config, {
  onWorkerError: (ctx) => {
    if (ctx.subscriber.importance === 'must-investigate') {
      alerting.critical(`Subscriber ${ctx.subscriber.name} failed: ${ctx.error.message}`);
    }
  },
});
```

### Delayed messages

> Delayed messages implementation is dependent on the `Transport` used.

You can optionally delay messages in Matador. This will cause their delivery to be delayed.

When using `RabbitMQTransport`, this is implemented using the [delayed-message-exchange plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange), and you should ensure it is installed in your RabbitMQ instance first.

> Note: Distributed event systems do not have precise delivery timings. The consumption of your event will be dependent on your throughput, and you should not depend on delayed messages for accurate timings.

```ts
// Delay message by 5 minutes
await matador.send(ReminderEvent, { userId: '123', message: 'Hello!' }, {
  delayMs: 5 * 60 * 1000, // 5 minutes
});

// Delay can also be used with correlationId and metadata
await matador.send(ScheduledNotificationEvent, notificationData, {
  delayMs: 60000, // 1 minute
  correlationId: 'notification-123',
  metadata: { scheduledBy: 'cron-job' },
});
```

#### Resource name prefix

By default, Matador prefixes every broker resource it creates with `matador`, so its queues and exchanges are identifiable on a shared broker:

```typescript
TopologyBuilder.create()
  .withNamespace('myapp')
  .addQueue('events')
  .build()
// → matador.myapp.events, matador.myapp.exchange, matador.myapp.dlx,
//   matador.myapp.events.retry, matador.myapp.events.unhandled, …
```

Change the prefix, or disable it entirely, with `withPrefix()`:

```typescript
.withPrefix('acme')  // acme.myapp.events
.withPrefix(null)    // myapp.events  (no prefix)
```

The prefix applies only to default-derived names. A `withNaming()` override (below) fully owns its output and is never prefixed. `exact: true` queues are never prefixed.

> **BREAKING:** default resource names are now `matador`-prefixed. A v3 deployment created before this change used unprefixed names (`{namespace}.{queue}`); add `.withPrefix(null)` to keep them and avoid declaring a new set of queues.

#### Migrations from v1

To adopt v3 from v1 without renaming, override how names are derived with `withNaming()`. Each builder receives the namespace from `withNamespace()` as an argument and returns the final name — so the namespace is always an explicit input to the strategy, never applied separately on top of the result:

```typescript
TopologyBuilder.create()
  .withNamespace('myapp')
  .withNaming({
    queue: (ns, q) => `matador.${ns}.${q}`,                 // queues: matador.myapp.{queue}
    mainExchange: (ns) => `matador.${ns}`,                  // v1 main exchange
    dlxExchange: (ns) => `matador.${ns}.dlx-undeliverable`, // v1 dead-letter exchange…
    dlxExchangeType: 'topic',                               // …which v1 declared as topic
    delayedExchange: (ns) => `matador.${ns}.delayed`,
  })
  .addQueue('general')
  .build()
```

Retry queues and dead-letter queues are derived from the work-queue name your `queue` builder returns. All builders are optional; omit any to keep the default (`matador.${namespace}.…`, or `${namespace}.…` when the prefix is disabled via `withPrefix(null)`).

#### Exact queues

Setting `exact: true` marks a queue as owned by another service. When set:

- The queue name is used exactly as written without namespace prefix.
- Matador still asserts the queue and binds it to this namespace's main exchange, so `send()` can route to it.
- Matador does **not** create the resources it normally manages alongside a queue: 
  no `.retry`, `.unhandled`, `.undeliverable` queues, no `x-queue-type: quorum` argument.
- Pass `transport.rabbitmq.options` mirroring the owner's declaration. Without it, the auto-computed defaults use *your*
  namespace's dead-letter exchange, which won't match the existing queue thus `PRECONDITION_FAILED`.
- To target or consume it, reference it by its full exact name in
  `targetQueue` / `consumeFrom` — it resolves as-is, without the
  namespace prefix.

# CLI

Matador provides a CLI utility for quick local testing of your Matador configuration.

```bash
# Send an event using config and event files
bunx matador send <config-file> <event-file> [options]

# Send a test event defined in the config file
bunx matador send-test-event <config-file> [options]
```

**Options:**
- `--help, -h` - Show help message
- `--dry-run` - Validate config and event without dispatching
- `--timeout` - Timeout in milliseconds for processing (default: 5000)
- `--verbose` - Show verbose output including all hook logs

**Config file** should export:
- `schema: MatadorSchema` - Map of event keys to [EventClass, Subscribers[]]
- `topology?: Topology` - Optional topology (defaults to simple 'events' queue)
- `hooks?: MatadorHooks` - Optional hooks
- `testEvent?: { eventKey, data, before?, options? }` - Test event for `send-test-event` command

**Event file** should export:
- `eventKey: string` - The key of the event to dispatch
- `data: unknown` - The event data payload
- `before?: unknown` - Optional 'before' data for change events
- `options?: EventOptions` - Optional dispatch options (correlationId, metadata, delayMs)

**Examples:**
```bash
bunx matador send ./my-config.ts ./test-event.ts --verbose
bunx matador send-test-event ./my-config.ts
```

# Thanks

Thank you to the following MeetsMore engineers who worked on our internal V1 version of Matador.

Matador V2 would not exist without your contributions.

- https://github.com/GuillaumeDeconinck
- https://github.com/mm-derek
- https://github.com/colachg
- https://github.com/richardmarchant
