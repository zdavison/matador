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
  StandardRetryPolicy,
  type StatusRecord,
  type StatusStore,
  TopologyBuilder,
  type TrackingStatus,
  createSubscriber,
  createSubscriberStub,
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

  override async set(
    trackingId: string,
    subscriberName: string,
    record: StatusRecord,
  ) {
    this.writes.push(record.status);
    await super.set(trackingId, subscriberName, record);
  }
}

/** Memory store that fails every write of the given statuses. */
class FlakyStore extends MemoryStatusStore {
  constructor(private readonly failOn: readonly TrackingStatus[]) {
    super({ progressIntervalMs: 0 });
  }

  override async set(
    trackingId: string,
    subscriberName: string,
    record: StatusRecord,
  ) {
    if (this.failOn.includes(record.status)) {
      throw new Error('store down');
    }
    await super.set(trackingId, subscriberName, record);
  }
}

async function waitFor(
  condition: () => Promise<boolean> | boolean,
  timeoutMs = 3000,
) {
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
    options: {
      retry?: boolean;
      retryPolicy?: RetryPolicy;
      hooks?: object;
    } = {},
  ): Promise<Matador> {
    const builder = TopologyBuilder.create()
      .withNamespace('status-int')
      .addQueue('events');
    const topology = options.retry
      ? builder
          .withRetry({ enabled: true, defaultDelayMs: 1, maxDelayMs: 5 })
          .build()
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

    const sending = m.send(
      JobEvent,
      { userId: 'u-1' },
      { trackingId: 'job-1' },
    );
    await waitFor(
      async () =>
        (await m.getStatus('job-1', 'job-runner'))?.status === 'active',
    );
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

    const sending = m.send(
      JobEvent,
      { userId: 'u-1' },
      { trackingId: 'job-1' },
    );
    await waitFor(
      async () =>
        (await m.getStatus('job-1', 'job-runner'))?.progress?.percent === 40,
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

    expect(store.writes).toEqual([
      'queued',
      'active',
      'retrying',
      'active',
      'completed',
    ]);
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
    expect(record?.error?.message).toBe(
      'redelivered message for idempotent: no',
    );
  });

  it('moves a tracked stub subscriber from queued to retrying to failed', async () => {
    const store = new RecordingStore();
    const stub = createSubscriberStub({
      name: 'job-runner',
      idempotent: 'yes',
      track: true,
    });
    const m = await start({ [JobEvent.key]: [JobEvent, [stub]] }, store, {
      retry: true,
    });

    await m.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await waitFor(() => store.writes.includes('failed'), 5000);

    expect(store.writes[0]).toBe('queued');
    expect(store.writes).toContain('retrying');
    expect(store.writes.at(-1)).toBe('failed');
    const record = await m.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('failed');
    expect(record?.error?.name).toBe('SubscriberIsStubError');
  });

  it('moves a tracked message for an unregistered subscriber from queued to failed', async () => {
    const store = new RecordingStore();
    const transport = new LocalTransport(silentLogger);
    const topology = TopologyBuilder.create()
      .withNamespace('status-int')
      .addQueue('events')
      .withRetry({ enabled: true, defaultDelayMs: 1, maxDelayMs: 5 })
      .build();
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      idempotent: 'yes',
      track: true,
      callback: async () => {},
    });
    const dispatcher = new Matador(
      {
        transport,
        topology,
        schema: { [JobEvent.key]: [JobEvent, [subscriber]] },
        statusStore: store,
      },
      { logger: silentLogger },
    );
    matador = new Matador(
      {
        transport,
        topology,
        schema: { [JobEvent.key]: [JobEvent, []] },
        consumeFrom: ['events'],
        statusStore: store,
      },
      { logger: silentLogger },
    );
    await dispatcher.start();
    await matador.start();

    await dispatcher.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await waitFor(() => store.writes.includes('failed'), 5000);

    expect(store.writes).toContain('retrying');
    const record = await matador.getStatus('job-1', 'job-runner');
    expect(record?.status).toBe('failed');
    expect(record?.error?.name).toBe('SubscriberNotRegisteredError');
    await dispatcher.shutdown();
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
    const m = await start(
      { [JobEvent.key]: [JobEvent, [first, second]] },
      store,
    );

    const result = await m.send(JobEvent, { userId: 'u-1' });
    await m.waitForIdle(2000);

    const all = await m.getStatus(result.trackingId ?? '');
    expect(all?.ocr?.status).toBe('completed');
    expect(all?.ocr?.result).toEqual({ pages: 3 });
    expect(all?.thumbnail?.status).toBe('failed');
  });

  it('drops pending progress when process exits by a throw', async () => {
    class FullRecordStore extends MemoryStatusStore {
      readonly received: StatusRecord[] = [];

      override async set(
        trackingId: string,
        subscriberName: string,
        record: StatusRecord,
      ) {
        this.received.push(structuredClone(record));
        await super.set(trackingId, subscriberName, record);
      }
    }

    class ThrowingCompleteTransport extends LocalTransport {
      private thrown = false;

      override async complete(
        receipt: Parameters<LocalTransport['complete']>[0],
      ) {
        await super.complete(receipt);
        if (!this.thrown) {
          this.thrown = true;
          throw new Error('complete failed');
        }
      }
    }

    const store = new FullRecordStore({ progressIntervalMs: 50 });
    const subscriber = createSubscriber<JobEvent>({
      name: 'job-runner',
      description: 'Runs the job',
      track: true,
      callback: async (_envelope, ctx) => {
        ctx.progress(10);
        ctx.progress(20);
      },
    });
    matador = new Matador(
      {
        transport: new ThrowingCompleteTransport(silentLogger),
        topology: TopologyBuilder.create()
          .withNamespace('status-int')
          .addQueue('events')
          .build(),
        schema: { [JobEvent.key]: [JobEvent, [subscriber]] },
        consumeFrom: ['events'],
        statusStore: store,
      },
      { logger: silentLogger },
    );
    await matador.start();

    await matador.send(JobEvent, { userId: 'u-1' }, { trackingId: 'job-1' });
    await matador.waitForIdle(2000);
    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(
      store.received.some((record) => record.progress?.percent === 10),
    ).toBe(true);
    expect(
      store.received.some((record) => record.progress?.percent === 20),
    ).toBe(false);
  });

  it('gives untracked subscribers a progress function that does nothing', async () => {
    const store = new RecordingStore({ progressIntervalMs: 0 });
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
    expect(store.writes).toEqual([]);
  });
});
