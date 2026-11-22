import type Redis from 'ioredis';
import {DEDUP_TTL_MS, dedupKey} from '../domain/contracts';
import type {Deduplicator} from '../domain/types';

/** Suppresses re-sending the same guidance to the same dispatch/vehicle within the TTL window. */
export class AlertDeduplicator implements Deduplicator {
  constructor(private readonly redis: Redis, private readonly ttlMs: number = DEDUP_TTL_MS) {}

  async shouldSend(dispatchId: string, vehicleId: string, fingerprint: string): Promise<boolean> {
    return await this.redis.get(dedupKey(dispatchId, vehicleId)) !== fingerprint;
  }

  async remember(dispatchId: string, vehicleId: string, fingerprint: string): Promise<void> {
    await this.redis.set(dedupKey(dispatchId, vehicleId), fingerprint, 'PX', this.ttlMs);
  }

  async forget(dispatchId: string, vehicleId: string): Promise<void> {
    await this.redis.del(dedupKey(dispatchId, vehicleId));
  }
}
