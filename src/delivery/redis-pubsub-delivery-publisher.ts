import type Redis from 'ioredis';
import {gatewayChannel} from '../domain/contracts';
import type {DeliveryPublisher, GatewayFrame} from '../domain/types';

/** Publishes to the single Gateway instance holding the vehicle's socket. */
export class RedisPubSubDeliveryPublisher implements DeliveryPublisher {
  constructor(private readonly redis: Redis) {}

  async publish(gatewayId: string, frame: GatewayFrame): Promise<number> {
    return this.redis.publish(gatewayChannel(gatewayId), JSON.stringify(frame));
  }
}
