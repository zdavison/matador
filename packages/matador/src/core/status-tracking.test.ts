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
    const schema: MatadorSchema = {
      [JobEvent.key]: [JobEvent, [trackedRunner()]],
    };
    expect(
      () => new Matador({ transport: new LocalTransport(), topology, schema }),
    ).toThrow(StatusStoreRequiredError);
  });

  it('throws StatusStoreRequiredError from register() with no store', () => {
    matador = new Matador({
      transport: new LocalTransport(),
      topology,
      schema: {},
    });
    expect(() => matador?.register(JobEvent, [trackedRunner()])).toThrow(
      StatusStoreRequiredError,
    );
  });

  it('throws StatusStoreRequiredError from getStatus() with no store', async () => {
    matador = new Matador({
      transport: new LocalTransport(),
      topology,
      schema: {},
    });
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
          await store.get(
            envelope.docket.trackingId,
            envelope.docket.targetSubscriber,
          ),
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
    expect(seenAtPublish[0]?.envelope.docket.trackingId).toBe(
      result.trackingId,
    );
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

    const result = await matador.send(
      JobEvent,
      { userId: 'u-1' },
      { trackingId: 'job-42' },
    );

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

    const result = await matador.send(
      JobEvent,
      { userId: 'u-1' },
      { trackingId: 'job-42' },
    );

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
      await matador.send(
        JobEvent,
        { userId: 'u-1' },
        { buffer: false, trackingId: 'job-1' },
      );
    } catch (e) {
      error = e;
    }

    expect(error).toBeInstanceOf(SomeSendError);
    const record = await matador.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('failed');
    expect(record?.error?.name).toBe('TransportSendError');
  });

  it('puts the tracking ID on SomeSendError for a generated ID', async () => {
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
      await matador.send(JobEvent, { userId: 'u-1' }, { buffer: false });
    } catch (e) {
      error = e;
    }

    expect(error).toBeInstanceOf(SomeSendError);
    const trackingId = (error as SomeSendError).trackingId;
    expect(typeof trackingId).toBe('string');
    const records = await matador.getStatus(trackingId as string);
    expect(records?.['job-runner']?.status).toBe('failed');
  });

  it('puts the caller tracking ID on SomeSendError (event instance path)', async () => {
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
      await matador.send(new JobEvent({ userId: 'u-1' }), {
        buffer: false,
        trackingId: 'job-2',
      });
    } catch (e) {
      error = e;
    }

    expect((error as SomeSendError).trackingId).toBe('job-2');
    expect(await matador.getStatus('job-2')).toBeDefined();
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

    expect((await matador.getStatus('job-1', 'job-runner'))?.status).toBe(
      'queued',
    );
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
      async () =>
        (await matador?.getStatus('job-1', 'job-runner'))?.status === 'failed',
    );
  });
});
