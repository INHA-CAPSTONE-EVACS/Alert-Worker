import type Redis from 'ioredis';
import {sessionKey} from '../domain/contracts';
import type {SessionInfo, SessionLocator} from '../domain/types';

const nonBlank = (value: unknown): value is string => typeof value === 'string' && value.trim().length > 0;

/** Reads the Gateway-owned ws:vehicle:{vehicleId} record. Unusable records count as no session. */
export class RedisSessionLocator implements SessionLocator {
  constructor(private readonly redis: Redis) {}

  async findGateway(vehicleId: string): Promise<SessionInfo | null> {
    const raw = await this.redis.get(sessionKey(vehicleId));
    if (!raw) return null;
    let value: Record<string, unknown>;
    try {
      value = JSON.parse(raw) as Record<string, unknown>;
    } catch {
      return null;
    }
    if (typeof value !== 'object' || value === null) return null;
    if (!nonBlank(value.gatewayId) || !nonBlank(value.sessionId)) return null;
    return {
      vehicleId: nonBlank(value.vehicleId) ? value.vehicleId : vehicleId,
      gatewayId: value.gatewayId,
      sessionId: value.sessionId,
      connectedAt: typeof value.connectedAt === 'string' ? value.connectedAt : '',
    };
  }
}
