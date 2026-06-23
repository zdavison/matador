# Default `matador` resource-name prefix (configurable)

**Date:** 2026-06-23
**Branch:** `meetsmore/v3-default-prefix` (stacked on `meetsmore/v3-dlq-naming-tests` → `meetsmore/v3-compat`)
**Status:** Approved design, pending implementation plan

## Problem

Matador v1 prefixed every broker resource it created with `matador` (e.g.
`matador.{namespace}.{queue}`), so operators could tell at a glance which queues
and exchanges were Matador-managed. Matador v3's default naming dropped the prefix
(`{namespace}.{queue}`), and the only way to get it back is to hand-write a full
`withNaming` strategy.

We want v3 to **default** to a `matador` prefix again — identifiable out of the box —
while letting it be changed to another string or disabled entirely.

## Decisions (from brainstorming)

- **Default flip, no compatibility guard.** The default changes from
  `{ns}.{queue}` to `matador.{ns}.{queue}`. This changes the broker topology for any
  v3 deployment not using `withNaming`, but the only existing v3 deployment already
  uses a `matador` prefix, so a guard is unnecessary. Documented as a breaking change.
- **API: `.withGlobalPrefix(value: string | null)`** on `TopologyBuilder`. Default
  `'matador'`. A string sets the prefix; `null` disables prefixing.
- **Prefix applies to default name builders only.** If a `withNaming` builder is
  supplied for a resource, that builder fully owns its output and the prefix is **not**
  prepended — consistent with the existing "namespace is an input to the strategy,
  never applied on top of the result" philosophy.
- **v3 suffixes are kept.** Prefixing yields `matador.{ns}.exchange`,
  `matador.{ns}.dlx`, etc. — "matador-prefixed v3", not byte-identical to v1. Exact
  v1 names remain the job of `withNaming`.

## Design

### Data model

Add `prefix: string | null` to the `Topology` type (and the builder). The builder
initialises it to `'matador'`.

### Builder API

```ts
TopologyBuilder.create()
  .withNamespace('myapp')   // matador.myapp.events  (default)
  .withGlobalPrefix('acme')       // acme.myapp.events
  .withGlobalPrefix(null)         // myapp.events          (no prefix)
```

`withGlobalPrefix(value: string | null): this` stores the value. Never calling it leaves
the default `'matador'`.

### Prefix application

A small helper centralises the rule:

```ts
function applyPrefix(prefix: string | null, name: string): string {
  return prefix == null ? name : `${prefix}.${name}`;
}
```

It wraps the **default** branch of each naming function. Resulting defaults with
prefix `matador`:

| Resource       | Name                                   |
|----------------|----------------------------------------|
| work queue     | `matador.{ns}.{queue}`                 |
| main exchange  | `matador.{ns}.exchange`                |
| dlx exchange   | `matador.{ns}.dlx`                     |
| delayed exch.  | `matador.{ns}.delayed`                 |
| retry queue    | `matador.{ns}.{queue}.retry`           |
| unhandled DLQ  | `matador.{ns}.{queue}.unhandled`       |
| undeliverable  | `matador.{ns}.{queue}.undeliverable`   |

DLQ and retry names are derived from the qualified work-queue name (per the prior
change), so they inherit the prefix automatically with no extra wiring.

`exact: true` queues are unaffected — no namespace, no prefix — as today.

### Threading the prefix

- `resolveTargetQueueName(topology, name)` (used by `fanout` and consumer setup in
  `matador.ts`) reads `topology.prefix` internally — **no signature change**.
- `getQualifiedQueueName`, `getDeadLetterQueueName`, `getRetryQueueName`,
  `resolveQueueName` gain a `prefix: string | null` parameter. Their callers in the
  RabbitMQ transport and local transport pass `topology.prefix`.
- The transport's exchange-name methods already receive `topology`, so they read
  `topology.prefix` directly.

### Composition with `withNaming`

Each naming function keeps its current shape: `naming?.<builder>?.(...) ?? <default>`.
The prefix is applied **only inside `<default>`** via `applyPrefix`. So an explicit
`withNaming` builder bypasses the prefix entirely; a default does not.

### Validation

`validate()` gains `validatePrefix(prefix)`:
- `null` → valid (no prefix).
- non-empty string matching `^[a-zA-Z][a-zA-Z0-9_-]*$` → valid.
- empty string or non-matching string → validation error instructing the use of
  `null` for no prefix.

### Documentation

- README topology/`withNaming` section: document the default `matador` prefix,
  `.withGlobalPrefix()`, and `.withGlobalPrefix(null)`.
- JSDoc on `withGlobalPrefix` and the `Topology.prefix` field, including `@default 'matador'`.
- A short "BREAKING: default resource names are now `matador`-prefixed; use
  `.withGlobalPrefix(null)` to keep the previous unprefixed names" note in the changelog /
  README migration section.

## Testing (TDD)

RED-first unit tests (topology builder + naming helpers):

1. Default topology (no `.withGlobalPrefix`) qualifies a queue as `matador.{ns}.{queue}`.
2. `.withGlobalPrefix('acme')` qualifies as `acme.{ns}.{queue}`.
3. `.withGlobalPrefix(null)` qualifies as `{ns}.{queue}` (no prefix).
4. Prefix reaches the default main / dlx / delayed exchange names.
5. DLQ and retry names inherit the prefix (`matador.{ns}.{queue}.unhandled`, `.retry`).
6. A `withNaming.queue` (and exchange) override is **not** prefixed even when a
   non-null prefix is set.
7. Invalid prefix (`''`, `'1abc'`, `'a b'`) is rejected by `validate()`/`build()`.

Confirm the existing `naming-migration.e2e.test.ts` still passes unchanged (it
overrides every builder via `withNaming`, so the prefix never applies).

## Non-goals

- No change to the `withNaming` override mechanism.
- No per-resource prefix configuration.
- No v1-byte-identical default (still `withNaming`'s job).

## Acceptance criteria

1. Default builds produce `matador`-prefixed names for all default resources.
2. `.withGlobalPrefix(str)` and `.withGlobalPrefix(null)` change/disable the prefix.
3. `withNaming` overrides are never prefixed.
4. Invalid prefixes fail validation with a clear message.
5. Docs state the new default and the breaking change.
6. `bun run typecheck`, `bun run lint`, `bun test`, and the e2e migration test pass.
