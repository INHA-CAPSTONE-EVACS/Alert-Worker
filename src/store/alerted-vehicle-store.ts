import type Redis from 'ioredis';
import {ALERTED_VEHICLES_TTL_MS} from '../domain/contracts';
import type {AlertedVehicleStore} from '../domain/types';

/**
 * Set read by the Control API Server's RedisAlertRepository (`GET /control/emergencies/{id}/alerted-vehicles`).
 * A Set counts each vehicle once however many alerts it acknowledged; the TTL outlives any emergency.
 */
export class RedisAlertedVehicleStore implements AlertedVehicleStore {
  constructor(private readonly redis: Redis, private readonly keyPattern: string) {}

  async add(emergencyId: string, vehicleId: string): Promise<void> {
    const key = this.keyPattern.replace('%s', emergencyId);
    await this.redis.multi().sadd(key, vehicleId).pexpire(key, ALERTED_VEHICLES_TTL_MS).exec();
  }
}
