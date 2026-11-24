import type Redis from 'ioredis';
import {DEFAULT_GATEWAY_ALERT_STREAM} from '../domain/contracts';
import type {DeliveryPublisher, GatewayOutboundEvent} from '../domain/types';

/** Publishes outbound alert events to the Redis Stream consumed by WebSocket Gateway instances. */
export class RedisStreamDeliveryPublisher implements DeliveryPublisher {
  constructor(
    private readonly redis: Redis,
    readonly stream: string = DEFAULT_GATEWAY_ALERT_STREAM,
    readonly maxlen: number = 10_000,
  ) {}

  async publish(eventKey: string, envelope: GatewayOutboundEvent): Promise<string> {
    const res = await this.redis.xadd(
      this.stream,
      'MAXLEN',
      '~',
      this.maxlen,
      '*',
      'eventKey',
      eventKey,
      'event',
      JSON.stringify(envelope),
    );
    return res ?? '';
  }
}
