import { describe, expect, it } from 'bun:test';
import { EventEmitter } from 'node:events';
import type { ConfirmChannel, Options } from 'amqplib';
import { RabbitMQPublisher } from './rabbitmq-publisher.js';

function createPublisher(writes: boolean[] = [], timeoutMs = 1000) {
  const calls: Array<{
    options: Options.Publish;
    confirm: (error: Error | null) => void;
  }> = [];
  const channel = Object.assign(new EventEmitter(), {
    publish: (
      _exchange: string,
      _routingKey: string,
      _content: Buffer,
      options: Options.Publish,
      confirm: (error: Error | null) => void,
    ) => {
      calls.push({ options, confirm });
      return writes.shift() ?? true;
    },
  });
  const publisher = new RabbitMQPublisher(
    channel as unknown as ConfirmChannel,
    timeoutMs,
  );
  return {
    calls,
    channel,
    publish(routingKey: string) {
      return publisher.publish('exchange', routingKey, Buffer.from('body'), {
        mandatory: true,
        messageId: 'same-envelope',
        persistent: true,
      });
    },
  };
}

describe('RabbitMQPublisher', () => {
  it('matches returns and out-of-order confirms to individual publishes', async () => {
    const { calls, channel, publish } = createPublisher();
    const results = Promise.allSettled([
      publish('no-route'),
      publish('confirmed'),
      publish('nacked'),
    ]);

    const [returned, confirmed, nacked] = calls;
    expect(returned?.options.messageId).toBe(confirmed?.options.messageId);
    expect(returned?.options.correlationId).not.toBe(
      confirmed?.options.correlationId,
    );
    expect(confirmed?.options.persistent).toBe(true);
    confirmed?.confirm(null);
    channel.emit('return', { properties: returned?.options });
    returned?.confirm(null);
    nacked?.confirm(new Error('broker nack'));

    expect(await results).toEqual([
      {
        status: 'rejected',
        reason: expect.objectContaining({
          message: expect.stringContaining('unroutable publish'),
        }),
      },
      { status: 'fulfilled', value: undefined },
      {
        status: 'rejected',
        reason: expect.objectContaining({
          name: 'TransportSendError',
          queue: 'nacked',
          message: expect.stringContaining('broker nack'),
        }),
      },
    ]);
  });

  it('pauses each time the channel reports backpressure and resumes on drain', async () => {
    const { calls, channel, publish } = createPublisher([false, false]);
    const results = Promise.all([
      publish('first'),
      publish('second'),
      publish('third'),
    ]);

    expect(calls).toHaveLength(1);
    calls[0]?.confirm(null);
    expect(calls).toHaveLength(1);
    channel.emit('drain');
    expect(calls).toHaveLength(2);
    channel.emit('drain');
    expect(calls).toHaveLength(3);
    calls[2]?.confirm(null);
    calls[1]?.confirm(null);

    await expect(results).resolves.toEqual([undefined, undefined, undefined]);
  });

  it('rejects in-flight and queued publishes on close without replay', async () => {
    const { calls, channel, publish } = createPublisher([false]);
    const results = Promise.allSettled([
      publish('in-flight'),
      publish('queued'),
    ]);
    channel.emit('close');
    channel.emit('drain');

    expect(calls).toHaveLength(1);
    expect((await results).map((result) => result.status)).toEqual([
      'rejected',
      'rejected',
    ]);
    await expect(publish('closed')).rejects.toThrow('publish channel closed');
  });

  it('does not publish a queued request after its timeout', async () => {
    const { calls, channel, publish } = createPublisher([false], 20);
    const results = await Promise.allSettled([
      publish('in-flight'),
      publish('expired'),
    ]);
    for (const result of results) {
      expect(result).toMatchObject({
        status: 'rejected',
        reason: {
          name: 'TransportSendError',
          message: expect.stringContaining('Publish not confirmed'),
        },
      });
    }

    calls[0]?.confirm(null);
    channel.emit('drain');
    expect(calls).toHaveLength(1);
    const recovered = publish('recovered');
    calls[1]?.confirm(null);
    await expect(recovered).resolves.toBeUndefined();
  });
});
