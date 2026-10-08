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
      async run(
        _envelope: Envelope<JobEvent['data']>,
        context: CallbackContext,
      ) {
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
      async run(
        _envelope: Envelope<JobEvent['data']>,
        context: CallbackContext,
      ) {
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
      imports: [
        MatadorTestingModule.forTest({ autoStart: false, statusStore }),
      ],
    }).compile();
    await moduleRef.init();
    const service = moduleRef.get(MatadorService);

    expect((await service.getStatus('job-1', 'runner'))?.status).toBe('active');
    expect(Object.keys((await service.getStatus('job-1')) ?? {})).toEqual([
      'runner',
    ]);

    await moduleRef.close();
  });
});
