import type Redis from 'ioredis';
import {gatewayChannel} from '../domain/contracts';
import type {DeliveryPublisher, GatewayFrame} from '../domain/types';

/** Legacy pubsub publisher: publishes to the single Gateway instance holding the vehicle's socket. */
export class RedisPubSubDeliveryPublisher {
  constructor(private readonly redis: Redis) {}

  async publish(gatewayId: string, frame: GatewayFrame): Promise<number> {
    return this.redis.publish(gatewayChannel(gatewayId), JSON.stringify(frame));
  }
}
