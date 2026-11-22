import type Redis from 'ioredis';
import {
  ALERT_EXPIRY_KEY, STATE_RETENTION_MS, activeAlertKey, alertStateKey,
} from '../domain/contracts';
import {type AlertState, type AlertStateStore, type AlertStatus, isTerminal} from '../domain/types';

const LIVE_LUA = "local function live(s) return s == 'PENDING' or s == 'SENT' or s == 'ACKNOWLEDGED' end";
const SCHEDULED_LUA = "local function scheduled(s) return s == 'SENT' or s == 'ACKNOWLEDGED' end";

// Version fence plus episode fence, then maintains the active and expiry indexes.
// ARGV: 1 json, 2 alertVersion, 3 ttlMs, 4 alertId, 5 expiresAtMs, 6 status, 7 alertLogId, 8 expectedEpisode
const SAVE_SCRIPT = `${LIVE_LUA}
${SCHEDULED_LUA}
local current = redis.call('GET', KEYS[1])
local stored = current and cjson.decode(current) or nil
if stored and tonumber(stored.alertVersion) > tonumber(ARGV[2]) then return 0 end
local liveEpisode = (stored and live(stored.status)) and stored.alertLogId or ''
if ARGV[8] == '' then
  if liveEpisode ~= '' and liveEpisode ~= ARGV[7] then return 0 end
elseif liveEpisode ~= ARGV[8] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
if live(ARGV[6]) then
  redis.call('SET', KEYS[2], ARGV[4], 'PX', ARGV[3])
elseif redis.call('GET', KEYS[2]) == ARGV[4] then
  redis.call('DEL', KEYS[2])
end
if scheduled(ARGV[6]) then
  redis.call('ZADD', KEYS[3], ARGV[5], ARGV[4])
else
  redis.call('ZREM', KEYS[3], ARGV[4])
end
return 1`;

// Compare-and-set on status (and optionally version) so racing writers cannot overwrite each other.
// ARGV: 1 from[], 2 to, 3 patch, 4 expectedVersion|'', 5 alertId, 6 ttlMs, 7 expiresAtMs
const TRANSITION_SCRIPT = `${LIVE_LUA}
${SCHEDULED_LUA}
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
redis.call('SET', KEYS[1], encoded, 'PX', ARGV[6])
if scheduled(ARGV[2]) then
  redis.call('ZADD', KEYS[3], ARGV[7], ARGV[5])
else
  redis.call('ZREM', KEYS[3], ARGV[5])
end
if not live(ARGV[2]) and redis.call('GET', KEYS[2]) == ARGV[5] then redis.call('DEL', KEYS[2]) end
return encoded`;

// Removes an expiry entry only when the alert can no longer expire (missing or not SENT/ACKNOWLEDGED).
const UNSCHEDULE_SCRIPT = `${SCHEDULED_LUA}
local current = redis.call('GET', KEYS[1])
if current and scheduled(cjson.decode(current).status) then return 0 end
return redis.call('ZREM', KEYS[2], ARGV[1])`;

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

  async save(state: AlertState, expectedEpisode: string | null): Promise<boolean> {
    const expiresAtMs = Date.parse(state.expiresAt);
    const result = await this.redis.eval(SAVE_SCRIPT, 3, alertStateKey(state.alertId),
      activeAlertKey(state.dispatchId, state.targetVehicleId), ALERT_EXPIRY_KEY,
      JSON.stringify(state), state.alertVersion, this.ttlMs(expiresAtMs), state.alertId, expiresAtMs,
      state.status, state.alertLogId, expectedEpisode ?? '');
    return result === 1;
  }

  async transition(alertId: string, from: readonly AlertStatus[], to: AlertStatus, patch: Partial<AlertState>,
    expectedVersion?: number): Promise<AlertState | null> {
    // Index keys derive from immutable fields, so reading them outside the script is safe.
    const current = await this.findById(alertId);
    if (!current) return null;
    const expiresAtMs = Date.parse(patch.expiresAt ?? current.expiresAt);
    const result = await this.redis.eval(TRANSITION_SCRIPT, 3, alertStateKey(alertId),
      activeAlertKey(current.dispatchId, current.targetVehicleId), ALERT_EXPIRY_KEY,
      JSON.stringify(from), to, JSON.stringify(patch), expectedVersion ?? '', alertId, this.ttlMs(expiresAtMs),
      expiresAtMs) as string | null;
    return result ? JSON.parse(result) as AlertState : null;
  }

  async dueForExpiry(nowMs: number, limit: number): Promise<string[]> {
    return this.redis.zrangebyscore(ALERT_EXPIRY_KEY, '-inf', nowMs, 'LIMIT', 0, limit);
  }

  async unschedule(alertId: string): Promise<void> {
    await this.redis.eval(UNSCHEDULE_SCRIPT, 2, alertStateKey(alertId), ALERT_EXPIRY_KEY, alertId);
  }

  private ttlMs(expiresAtMs: number): number {
    return Math.max(expiresAtMs - this.now(), 0) + STATE_RETENTION_MS;
  }
}
