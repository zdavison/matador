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

const client = createClient({
  url: process.env.REDIS_URL,
  disableOfflineQueue: true, // Fail fast during an outage
  socket: { connectTimeout: 5000 },
});
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

## Client settings for an outage

Create the `redis` client with `disableOfflineQueue: true` and a `socket.connectTimeout`. By default, node-redis v4 queues each command while the client is disconnected. The status writes then wait until Redis comes back. The consumer waits for these writes, so it stops to ack messages. With `disableOfflineQueue: true`, each command fails at once. Matador then logs the error and calls the `onStatusStoreError` hook, and message handling continues.

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
