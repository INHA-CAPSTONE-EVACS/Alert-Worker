import type Redis from 'ioredis';
import {
  ALERT_EXPIRY_KEY, STATE_RETENTION_MS, activeAlertKey, alertStateKey,
} from '../domain/contracts';
import {type AlertState, type AlertStateStore, type AlertStatus, isTerminal} from '../domain/types';

// Writes unless a strictly higher alertVersion is stored, then maintains the active and expiry indexes.
const SAVE_SCRIPT = `
local current = redis.call('GET', KEYS[1])
if current then
  local stored = cjson.decode(current)
  if tonumber(stored.alertVersion) > tonumber(ARGV[2]) then return 0 end
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
if ARGV[7] == '1' then
  if redis.call('GET', KEYS[2]) == ARGV[4] then redis.call('DEL', KEYS[2]) end
else
  redis.call('SET', KEYS[2], ARGV[4], 'PX', ARGV[3])
end
if ARGV[6] == '1' then
  redis.call('ZADD', KEYS[3], ARGV[5], ARGV[4])
else
  redis.call('ZREM', KEYS[3], ARGV[4])
end
return 1`;

// Compare-and-set on status (and optionally version) so racing writers cannot overwrite each other.
const TRANSITION_SCRIPT = `
local current = redis.call('GET', KEYS[1])
if not current then return false end
local stored = cjson.decode(current)
local allowed = false
for _, status in ipairs(cjson.decode(ARGV[1])) do
  if stored.status == status then allowed = true end
end
if not allowed then return false end
if ARGV[4] ~= '' and tonumber(stored.alertVersion) ~= tonumber(ARGV[4]) then return false end
for field, value in pairs(cjson.decode(ARGV[3])) do stored[field] = value end
stored.status = ARGV[2]
local encoded = cjson.encode(stored)
redis.call('SET', KEYS[1], encoded, 'KEEPTTL')
if ARGV[6] == '1' then
  redis.call('ZREM', KEYS[3], ARGV[5])
  if redis.call('GET', KEYS[2]) == ARGV[5] then redis.call('DEL', KEYS[2]) end
end
return encoded`;

const SCHEDULED: readonly AlertStatus[] = ['SENT', 'ACKNOWLEDGED'];

/** Current alert state in Redis (alert:{alertId}:state) with active and expiry indexes. */
export class AlertStateRepository implements AlertStateStore {
  constructor(private readonly redis: Redis, private readonly now: () => number = Date.now) {}

  async findById(alertId: string): Promise<AlertState | null> {
    const raw = await this.redis.get(alertStateKey(alertId));
    return raw ? JSON.parse(raw) as AlertState : null;
  }

  async findActive(dispatchId: string, vehicleId: string): Promise<AlertState | null> {
    const alertId = await this.redis.get(activeAlertKey(dispatchId, vehicleId));
    if (!alertId) return null;
    const state = await this.findById(alertId);
    return state && !isTerminal(state.status) ? state : null;
  }

  async save(state: AlertState): Promise<boolean> {
    const expiresAtMs = Date.parse(state.expiresAt);
    const ttlMs = Math.max(expiresAtMs - this.now(), 0) + STATE_RETENTION_MS;
    const result = await this.redis.eval(SAVE_SCRIPT, 3, alertStateKey(state.alertId),
      activeAlertKey(state.dispatchId, state.targetVehicleId), ALERT_EXPIRY_KEY,
      JSON.stringify(state), state.alertVersion, ttlMs, state.alertId, expiresAtMs,
      SCHEDULED.includes(state.status) ? '1' : '0', isTerminal(state.status) ? '1' : '0');
    return result === 1;
  }

  async transition(alertId: string, from: readonly AlertStatus[], to: AlertStatus, patch: Partial<AlertState>,
    expectedVersion?: number): Promise<AlertState | null> {
    // Index keys derive from immutable fields, so reading them outside the script is safe.
    const current = await this.findById(alertId);
    if (!current) return null;
    const result = await this.redis.eval(TRANSITION_SCRIPT, 3, alertStateKey(alertId),
      activeAlertKey(current.dispatchId, current.targetVehicleId), ALERT_EXPIRY_KEY,
      JSON.stringify(from), to, JSON.stringify(patch), expectedVersion ?? '', alertId,
      isTerminal(to) ? '1' : '0') as string | null;
    return result ? JSON.parse(result) as AlertState : null;
  }

  async dueForExpiry(nowMs: number, limit: number): Promise<string[]> {
    return this.redis.zrangebyscore(ALERT_EXPIRY_KEY, '-inf', nowMs, 'LIMIT', 0, limit);
  }

  async unschedule(alertId: string): Promise<void> {
    await this.redis.zrem(ALERT_EXPIRY_KEY, alertId);
  }
}
