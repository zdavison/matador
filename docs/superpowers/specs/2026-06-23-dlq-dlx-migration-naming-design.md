# Migration-safe DLQ/DLX naming (Option A1)

**Date:** 2026-06-23
**Branch:** `meetsmore/v3-compat` (PR #3)
**Status:** Approved design, pending implementation plan

## Problem

PR #3 added `TopologyBuilder.withNaming(...)` so a v3 deployment can adopt the
broker resource names a Matador v1 deployment already declared, letting a rolling
deploy re-assert and reuse existing exchanges/queues instead of creating a parallel
set.

`withNaming` currently exposes builders for the work-queue name (`queue`), the main
exchange (`mainExchange`), the dead-letter exchange (`dlxExchange` + `dlxExchangeType`),
and the delayed exchange (`delayedExchange`). The **dead-letter exchange name is
explicitly controllable**, but the **dead-letter queue (DLQ) and retry queue names are
not** — they are derived as `<resolved-work-queue>.unhandled`, `.undeliverable`, and
`.retry`.

Because they derive from the resolved work-queue name, overriding `queue` to
`matador.${ns}.${q}` *already* makes the DLQ/retry names carry the `matador` prefix
(`matador.{ns}.{queue}.unhandled`, etc.). However:

1. The RabbitMQ transport builds these names with **inline string concatenation**
   (`rabbitmq-transport.ts:752` and `:788`) rather than calling the naming-aware
   helpers `getRetryQueueName` / `getDeadLetterQueueName`. They produce the correct
   result today only because the work-queue name passed in is already resolved. This
   is a latent **drift risk**: a future change to the helper (or to how the inline
   name is built) would silently desynchronise the publish side from the
   assert/bind side.
2. There is **no test** asserting that DLQ and retry queue names carry the migration
   prefix. The e2e migration test (`naming-migration.e2e.test.ts`) asserts work-queue
   and remote-queue routing only; it never inspects the dead-letter resource names.

## Scope decision

This is **Option A1** from brainstorming:

- **Option A (chosen over B):** name-only migration. v3 keeps its own dead-letter
  *shape* (per-work-queue DLQs, a single DLX distinguished by routing key). We do not
  reproduce v1's shared per-namespace DLQs or its three separate DLX exchanges.
  Migrating deployments get matador-prefixed names but these are **new** resources;
  v1's shared `dlq-*` queues are not reused.
- **Option A1 (chosen over A2):** DLQ and retry queue names **inherit** the prefix
  from the `queue` builder. We do **not** add independent `deadLetterQueue` /
  `retryQueue` builder hooks to `TopologyNaming`.

## Goal

Guarantee — and prove with tests — that DLQ and retry queue names carry the migration
prefix by routing every DLQ/retry name construction in the transport through the
single naming-aware source of truth, with no new public API.

## Changes

### 1. Transport refactor (behavior-preserving; removes drift risk)

File: `packages/matador/src/transport/rabbitmq/rabbitmq-transport.ts`

- `assertRetryQueue`: replace the inline `` `${workQueueName}.retry` `` (line ~752)
  with `getRetryQueueName(topology.namespace, queueDef.name, topology.naming)`. This
  requires passing the `QueueDefinition` (or its `name`) into `assertRetryQueue`
  instead of the pre-resolved `workQueueName` string, so the method can call the
  helper itself. The resolved work-queue name is still needed for the retry queue's
  `x-dead-letter-routing-key`, so the method continues to resolve it (via
  `resolveQueueName`) for that argument.
- `assertDeadLetterQueues`: replace the inline `` `${workQueueName}.${dlqType}` ``
  (line ~788) with
  `getDeadLetterQueueName(topology.namespace, queueDef.name, dlqType, topology.naming)`.
- `sendToDeadLetter` (line ~407): **no change.** It derives the DLQ routing key from
  `receipt.sourceQueue` (already the resolved/prefixed name) plus the
  `unhandled`/`undeliverable` suffix, which matches the binding key established in
  `assertDeadLetterQueues`. A test will lock this behavior.

Net effect: every DLQ/retry name in the transport flows through the same helpers the
unit tests already cover. No output changes for any existing configuration.

### 2. Tests (the primary deliverable)

Per TDD, the meaningful failing test is a **drift guard** — the current inline code
produces the right string, so a test that merely checks the output string would pass
today. The test must instead pin the behavior so a future divergence fails:

- **Transport unit test** (`rabbitmq-transport.test.ts`): with a stubbed/spied channel
  and a topology built with a v1-style `withNaming`, assert that `channel.assertQueue`
  is called with the exact prefixed names:
  - `matador.{ns}.{queue}.retry`
  - `matador.{ns}.{queue}.unhandled`
  - `matador.{ns}.{queue}.undeliverable`
  and that each DLQ is bound to the overridden DLX name.
- **e2e migration test** (`naming-migration.e2e.test.ts`): after connecting with the
  v1-style naming, passively declare (`checkQueue`) the three dead-letter resources to
  confirm they exist under the migration prefix, and confirm DLQ binding to the
  overridden DLX.
- **`sendToDeadLetter` test**: assert the publish routing key equals the bound DLQ
  name under a naming override, so the publish path and the assert/bind path stay
  consistent.

### 3. Documentation

- Update the `withNaming` JSDoc (`topology/types.ts`, the `TopologyNaming` interface
  and its example) to state explicitly that DLQ and retry queue names are **derived
  from the `queue` builder result** (`<queue>.retry`, `<queue>.unhandled`,
  `<queue>.undeliverable`), so overriding `queue` carries the prefix into them
  automatically. There is intentionally no separate builder for them.
- Mirror this note in the README section that documents `withNaming`.

## Non-goals

- No new `TopologyNaming` fields (no `deadLetterQueue` / `retryQueue` builders).
- No change to v3's per-queue DLQ shape or single-DLX model (that was Option B).
- No change to the suffix terms (`.retry`, `.unhandled`, `.undeliverable`).
- No reuse of v1's shared `dlq-*` queues; migrating deployments create new,
  prefixed per-queue dead-letter resources.

## Acceptance criteria

1. `assertRetryQueue` and `assertDeadLetterQueues` construct names exclusively via
   `getRetryQueueName` / `getDeadLetterQueueName` (no inline suffix concatenation).
2. A transport-level test fails if those name constructions stop honoring
   `topology.naming`.
3. The e2e migration test asserts the existence and DLX binding of the prefixed
   `retry`, `unhandled`, and `undeliverable` resources.
4. `withNaming` documentation states that DLQ/retry names inherit from the `queue`
   builder.
5. `bun run typecheck`, `bun run lint`, and `bun test` pass.
