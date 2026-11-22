import type Redis from 'ioredis';
import {routeDeliveredKey} from '../domain/contracts';
import type {RouteVersionStore} from '../domain/types';

const ROUTE_VERSION_TTL_MS = 24 * 60 * 60 * 1000;
const ADVANCE_SCRIPT = `
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if tonumber(ARGV[1]) > current then redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]) return 1 end
return 0`;

/** Last route version delivered per dispatch; it only moves forward. */
export class RedisRouteVersionStore implements RouteVersionStore {
  constructor(private readonly redis: Redis) {}

  async isLatest(dispatchId: string, routeVersion: number): Promise<boolean> {
    return routeVersion > Number(await this.redis.get(routeDeliveredKey(dispatchId)) ?? 0);
  }

  async markDelivered(dispatchId: string, routeVersion: number): Promise<void> {
    await this.redis.eval(ADVANCE_SCRIPT, 1, routeDeliveredKey(dispatchId), routeVersion, ROUTE_VERSION_TTL_MS);
  }
}
