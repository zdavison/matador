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
