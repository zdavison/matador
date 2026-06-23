import { describe, expect, it } from 'bun:test';
import { TopologyBuilder, TopologyValidationError } from './builder.js';
import {
  type TopologyNaming,
  findQueueDefinition,
  getDeadLetterQueueName,
  getQualifiedQueueName,
  getRetryQueueName,
  resolveQueueName,
  resolveTargetQueueName,
} from './types.js';

describe('TopologyBuilder', () => {
  describe('withNamespace', () => {
    it('should set the namespace', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('myapp')
        .addQueue('events')
        .build();

      expect(topology.namespace).toBe('myapp');
    });

    it('should reject empty namespace', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('')
        .addQueue('events');

      expect(() => builder.build()).toThrow(TopologyValidationError);
    });

    it('should reject namespace starting with number', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('123app')
        .addQueue('events');

      expect(() => builder.build()).toThrow(TopologyValidationError);
    });

    it('should allow hyphens and underscores in namespace', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('my-app_v2')
        .addQueue('events')
        .build();

      expect(topology.namespace).toBe('my-app_v2');
    });
  });

  describe('addQueue', () => {
    it('should add a queue with defaults', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      expect(topology.queues).toHaveLength(1);
      expect(topology.queues[0]?.name).toBe('events');
    });

    it('should add queue with options', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events', {
          concurrency: 5,
          consumerTimeout: 30000,
          priorities: true,
        })
        .build();

      expect(topology.queues[0]?.concurrency).toBe(5);
      expect(topology.queues[0]?.consumerTimeout).toBe(30000);
      expect(topology.queues[0]?.priorities).toBe(true);
    });

    it('should add multiple queues', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .addQueue('notifications')
        .addQueue('analytics')
        .build();

      expect(topology.queues).toHaveLength(3);
    });

    it('should reject duplicate queue names', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .addQueue('events');

      expect(() => builder.build()).toThrow('Duplicate queue name');
    });

    it('should reject empty queue name', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('');

      expect(() => builder.build()).toThrow('Queue name cannot be empty');
    });

    it('should reject queue names starting with number', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('123queue');

      expect(() => builder.build()).toThrow('must start with a letter');
    });

    it('should reject invalid concurrency', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events', { concurrency: 0 });

      expect(() => builder.build()).toThrow('concurrency must be at least 1');
    });

    it('should reject negative consumer timeout', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events', { consumerTimeout: -1 });

      expect(() => builder.build()).toThrow(
        'consumer timeout must be non-negative',
      );
    });
  });

  describe('withDeadLetter', () => {
    it('should configure dead letter settings', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .withDeadLetter({
          unhandled: { enabled: false },
          undeliverable: { enabled: true },
        })
        .build();

      expect(topology.deadLetter.unhandled.enabled).toBe(false);
      expect(topology.deadLetter.undeliverable.enabled).toBe(true);
    });

    it('should merge with defaults', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .withDeadLetter({ unhandled: { enabled: false } })
        .build();

      // undeliverable should keep default
      expect(topology.deadLetter.unhandled.enabled).toBe(false);
      expect(topology.deadLetter.undeliverable.enabled).toBe(true);
    });
  });

  describe('withRetry', () => {
    it('should configure retry settings', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .withRetry({
          enabled: true,
          defaultDelayMs: 5000,
          maxDelayMs: 600000,
        })
        .build();

      expect(topology.retry.enabled).toBe(true);
      expect(topology.retry.defaultDelayMs).toBe(5000);
      expect(topology.retry.maxDelayMs).toBe(600000);
    });

    it('should reject negative default delay', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .withRetry({ defaultDelayMs: -1 });

      expect(() => builder.build()).toThrow('delay must be non-negative');
    });

    it('should reject max delay less than default', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .withRetry({
          defaultDelayMs: 10000,
          maxDelayMs: 5000,
        });

      expect(() => builder.build()).toThrow(
        'Max retry delay must be greater than or equal to default',
      );
    });
  });

  describe('withoutRetry', () => {
    it('should disable retry', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .withoutRetry()
        .build();

      expect(topology.retry.enabled).toBe(false);
    });
  });

  describe('withoutDeadLetter', () => {
    it('should disable dead letter queues', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .withoutDeadLetter()
        .build();

      expect(topology.deadLetter.unhandled.enabled).toBe(false);
      expect(topology.deadLetter.undeliverable.enabled).toBe(false);
    });
  });

  describe('validate', () => {
    it('should return issues without throwing', () => {
      const builder = TopologyBuilder.create();
      const issues = builder.validate();

      expect(issues.length).toBeGreaterThan(0);
      expect(issues).toContain('Namespace is required');
      expect(issues).toContain('At least one queue is required');
    });

    it('should return empty array for valid topology', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events');

      const issues = builder.validate();
      expect(issues).toHaveLength(0);
    });
  });

  describe('build', () => {
    it('should throw TopologyValidationError with issues', () => {
      const builder = TopologyBuilder.create();

      try {
        builder.build();
        expect(true).toBe(false); // Should not reach here
      } catch (error) {
        expect(error).toBeInstanceOf(TopologyValidationError);
        expect((error as TopologyValidationError).issues).toContain(
          'Namespace is required',
        );
      }
    });

    it('should return immutable topology', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('events')
        .build();

      // Verify structure
      expect(topology.namespace).toBe('test');
      expect(topology.queues).toHaveLength(1);
      expect(topology.deadLetter.unhandled.enabled).toBe(true);
      expect(topology.retry.enabled).toBe(true);
    });
  });

  describe('exact queue option', () => {
    it('should mark queue as exact (external)', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('external-queue', { exact: true })
        .build();

      expect(topology.queues[0]?.exact).toBe(true);
    });

    it('should allow dots in queue name when exact: true', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('matador.shared.id-platform', { exact: true })
        .build();

      expect(topology.queues[0]?.name).toBe('matador.shared.id-platform');
      expect(topology.queues[0]?.exact).toBe(true);
    });

    it('should reject dots in queue name when exact: false', () => {
      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('invalid.queue.name');

      expect(() => builder.build()).toThrow('must start with a letter');
    });

    it('should allow transport-specific RabbitMQ options with exact queue', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue('matador.shared.id-platform', {
          exact: true,
          transport: {
            rabbitmq: {
              options: {
                durable: true,
                deadLetterExchange: 'matador.shared.dlx-undeliverable',
                arguments: {
                  'x-queue-type': 'quorum',
                },
              },
            },
          },
        })
        .build();

      expect(topology.queues[0]?.name).toBe('matador.shared.id-platform');
      expect(topology.queues[0]?.exact).toBe(true);
      expect(topology.queues[0]?.transport?.rabbitmq?.options?.durable).toBe(
        true,
      );
      expect(
        topology.queues[0]?.transport?.rabbitmq?.options?.deadLetterExchange,
      ).toBe('matador.shared.dlx-undeliverable');
      expect(
        topology.queues[0]?.transport?.rabbitmq?.options?.arguments?.[
          'x-queue-type'
        ],
      ).toBe('quorum');
    });
  });

  describe('addQueue with QueueDefinition object', () => {
    it('should accept a QueueDefinition object', () => {
      const queueDef = {
        name: 'events',
        concurrency: 5,
        consumerTimeout: 30000,
      };

      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue(queueDef)
        .build();

      expect(topology.queues).toHaveLength(1);
      expect(topology.queues[0]?.name).toBe('events');
      expect(topology.queues[0]?.concurrency).toBe(5);
      expect(topology.queues[0]?.consumerTimeout).toBe(30000);
    });

    it('should allow reusing queue definitions across builders', () => {
      const sharedQueue = {
        name: 'shared-queue',
        concurrency: 10,
        exact: true,
      };

      const topology1 = TopologyBuilder.create()
        .withNamespace('app1')
        .addQueue(sharedQueue)
        .build();

      const topology2 = TopologyBuilder.create()
        .withNamespace('app2')
        .addQueue(sharedQueue)
        .build();

      expect(topology1.queues[0]).toEqual(sharedQueue);
      expect(topology2.queues[0]).toEqual(sharedQueue);
    });

    it('should work with queue() alias', () => {
      const queueDef = {
        name: 'analytics',
        priorities: true,
      };

      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .queue(queueDef)
        .build();

      expect(topology.queues[0]?.name).toBe('analytics');
      expect(topology.queues[0]?.priorities).toBe(true);
    });

    it('should mix QueueDefinition objects with name+options style', () => {
      const reusableQueue = {
        name: 'shared',
        concurrency: 3,
      };

      const topology = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue(reusableQueue)
        .addQueue('local', { concurrency: 1 })
        .build();

      expect(topology.queues).toHaveLength(2);
      expect(topology.queues[0]?.name).toBe('shared');
      expect(topology.queues[1]?.name).toBe('local');
    });

    it('should validate QueueDefinition objects the same way', () => {
      const invalidQueue = {
        name: '123invalid',
        concurrency: 5,
      };

      const builder = TopologyBuilder.create()
        .withNamespace('test')
        .addQueue(invalidQueue);

      expect(() => builder.build()).toThrow('must start with a letter');
    });
  });
});

describe('resolveQueueName', () => {
  it('should return namespace.name for regular queues', () => {
    const queueDef = { name: 'events' };
    expect(resolveQueueName('myapp', queueDef)).toBe('myapp.events');
  });

  it('should return name as-is when exact: true', () => {
    const queueDef = { name: 'matador.shared.id-platform', exact: true };
    expect(resolveQueueName('myapp', queueDef)).toBe(
      'matador.shared.id-platform',
    );
  });

  it('should return namespace.name when exact: false', () => {
    const queueDef = { name: 'events', exact: false };
    expect(resolveQueueName('myapp', queueDef)).toBe('myapp.events');
  });

  it('should work with full QueueDefinition including transport options', () => {
    const queueDef = {
      name: 'matador.shared.id-platform',
      exact: true,
      transport: {
        rabbitmq: {
          options: {
            durable: true,
            deadLetterExchange: 'matador.shared.dlx-undeliverable',
          },
        },
      },
    };
    expect(resolveQueueName('myapp', queueDef)).toBe(
      'matador.shared.id-platform',
    );
  });
});

describe('TopologyNaming', () => {
  const v1Naming = {
    queue: (ns: string, q: string) => `matador.${ns}.${q}`,
    mainExchange: (ns: string) => `matador.${ns}`,
    dlxExchange: (ns: string) => `matador.${ns}.dlx-undeliverable`,
    dlxExchangeType: 'topic' as const,
    delayedExchange: (ns: string) => `matador.${ns}.delayed`,
  };

  describe('builder validation', () => {
    it('should accept a valid naming configuration', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('myapp')
        .withNaming(v1Naming)
        .addQueue('events')
        .build();

      expect(topology.naming?.queue?.('myapp', 'events')).toBe(
        'matador.myapp.events',
      );
      expect(topology.naming?.mainExchange?.('myapp')).toBe('matador.myapp');
    });

    it('should accept partial naming overrides', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('myapp')
        .withNaming({ queue: (ns, q) => `legacy.matador.${ns}.${q}` })
        .addQueue('events')
        .build();

      expect(topology.naming?.queue?.('myapp', 'events')).toBe(
        'legacy.matador.myapp.events',
      );
    });

    it('should reject a non-function naming override', () => {
      expect(() =>
        TopologyBuilder.create()
          .withNamespace('myapp')
          .withNaming({ queue: 'matador.' } as unknown as TopologyNaming)
          .addQueue('events')
          .build(),
      ).toThrow(TopologyValidationError);
    });

    it('should reject an invalid dlxExchangeType', () => {
      expect(() =>
        TopologyBuilder.create()
          .withNamespace('myapp')
          .withNaming({
            dlxExchangeType: 'fanout',
          } as unknown as TopologyNaming)
          .addQueue('events')
          .build(),
      ).toThrow(TopologyValidationError);
    });
  });

  describe('name resolution with naming overrides', () => {
    const topology = TopologyBuilder.create()
      .withNamespace('myapp')
      .withNaming({ queue: (ns, q) => `matador.${ns}.${q}` })
      .addQueue('events')
      .addQueue({
        name: 'matador.shared.id-platform',
        exact: true,
      })
      .build();

    it('should build qualified queue names via the strategy', () => {
      expect(getQualifiedQueueName('myapp', 'events', topology.naming)).toBe(
        'matador.myapp.events',
      );
    });

    it('should build resolved queue definitions via the strategy', () => {
      const queueDef = { name: 'events' };
      expect(resolveQueueName('myapp', queueDef, topology.naming)).toBe(
        'matador.myapp.events',
      );
    });

    it('should not transform exact queue definitions', () => {
      const queueDef = { name: 'matador.shared.id-platform', exact: true };
      expect(resolveQueueName('myapp', queueDef, topology.naming)).toBe(
        'matador.shared.id-platform',
      );
    });

    it('should resolve a target queue by exact name as-is', () => {
      expect(
        resolveTargetQueueName(topology, 'matador.shared.id-platform'),
      ).toBe('matador.shared.id-platform');
    });

    it('should build non-exact target queues via the strategy', () => {
      expect(resolveTargetQueueName(topology, 'events')).toBe(
        'matador.myapp.events',
      );
    });

    it('should derive retry and DLQ names from the strategy name', () => {
      expect(getRetryQueueName('myapp', 'events', topology.naming)).toBe(
        'matador.myapp.events.retry',
      );
      expect(
        getDeadLetterQueueName('myapp', 'events', 'unhandled', topology.naming),
      ).toBe('matador.myapp.events.unhandled');
    });

    it('should find queue definitions by name', () => {
      expect(findQueueDefinition(topology, 'events')?.name).toBe('events');
      expect(
        findQueueDefinition(topology, 'matador.shared.id-platform')?.name,
      ).toBe('matador.shared.id-platform');
      expect(findQueueDefinition(topology, 'missing')).toBeUndefined();
    });
  });
});

describe('resource name prefix', () => {
  describe('default', () => {
    it('prefixes resource names with "matador" by default', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('myapp')
        .addQueue('events')
        .build();

      expect(topology.prefix).toBe('matador');
      expect(resolveTargetQueueName(topology, 'events')).toBe(
        'matador.myapp.events',
      );
    });

    it('inherits the default prefix into retry and DLQ names', () => {
      expect(getRetryQueueName('myapp', 'events', undefined, 'matador')).toBe(
        'matador.myapp.events.retry',
      );
      expect(
        getDeadLetterQueueName(
          'myapp',
          'events',
          'unhandled',
          undefined,
          'matador',
        ),
      ).toBe('matador.myapp.events.unhandled');
    });
  });

  describe('withPrefix', () => {
    it('uses a custom prefix string', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('myapp')
        .withPrefix('acme')
        .addQueue('events')
        .build();

      expect(topology.prefix).toBe('acme');
      expect(resolveTargetQueueName(topology, 'events')).toBe(
        'acme.myapp.events',
      );
    });

    it('disables prefixing when set to null', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('myapp')
        .withPrefix(null)
        .addQueue('events')
        .build();

      expect(topology.prefix).toBeNull();
      expect(resolveTargetQueueName(topology, 'events')).toBe('myapp.events');
    });
  });

  describe('helper-level prefixing', () => {
    it('applies a non-null prefix to the default qualified name', () => {
      expect(getQualifiedQueueName('myapp', 'events', undefined, 'acme')).toBe(
        'acme.myapp.events',
      );
    });

    it('omits the prefix when null or undefined', () => {
      expect(getQualifiedQueueName('myapp', 'events', undefined, null)).toBe(
        'myapp.events',
      );
      expect(
        getQualifiedQueueName('myapp', 'events', undefined, undefined),
      ).toBe('myapp.events');
    });
  });

  describe('composition with withNaming', () => {
    it('does not prefix names produced by a withNaming override', () => {
      const topology = TopologyBuilder.create()
        .withNamespace('myapp')
        .withPrefix('matador')
        .withNaming({ queue: (_ns, q) => `legacy-${q}` })
        .addQueue('events')
        .build();

      // The override fully owns the name; the prefix is not prepended.
      expect(resolveTargetQueueName(topology, 'events')).toBe('legacy-events');
    });
  });

  describe('validation', () => {
    it('rejects an empty-string prefix', () => {
      expect(() =>
        TopologyBuilder.create()
          .withNamespace('myapp')
          .withPrefix('')
          .addQueue('events')
          .build(),
      ).toThrow(TopologyValidationError);
    });

    it('rejects a prefix that is not a valid identifier', () => {
      expect(() =>
        TopologyBuilder.create()
          .withNamespace('myapp')
          .withPrefix('1bad')
          .addQueue('events')
          .build(),
      ).toThrow(TopologyValidationError);
      expect(() =>
        TopologyBuilder.create()
          .withNamespace('myapp')
          .withPrefix('has space')
          .addQueue('events')
          .build(),
      ).toThrow(TopologyValidationError);
    });

    it('accepts null (no prefix) and valid identifier prefixes', () => {
      expect(() =>
        TopologyBuilder.create()
          .withNamespace('myapp')
          .withPrefix(null)
          .addQueue('events')
          .build(),
      ).not.toThrow();
      expect(() =>
        TopologyBuilder.create()
          .withNamespace('myapp')
          .withPrefix('acme_co-1')
          .addQueue('events')
          .build(),
      ).not.toThrow();
    });
  });
});
