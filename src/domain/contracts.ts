// Redis protocol shared with Emergency Worker (producer) and the Gateway (delivery).
export const ALERT_STREAM = 'stream:alert:event';
export const DLQ_STREAM = 'stream:alert-worker:dlq';
export const CONSUMER_GROUP = 'alert-worker';
export const EVENT_FIELD = 'event';
export const DEDUP_TTL_MS = 10_000;
export const STATE_RETENTION_MS = 300_000;
export const ALERT_EXPIRY_KEY = 'alert:expiry';

export const sessionKey = (vehicleId: string) => `ws:vehicle:${vehicleId}`;
export const alertStateKey = (alertId: string) => `alert:${alertId}:state`;
export const activeAlertKey = (dispatchId: string, vehicleId: string) =>
  `alert:active:${dispatchId}:${vehicleId}`;
export const dedupKey = (dispatchId: string, vehicleId: string) => `alert:dedup:${dispatchId}:${vehicleId}`;
export const routeDeliveredKey = (dispatchId: string) => `route:delivered:${dispatchId}`;
export const gatewayChannel = (gatewayId: string) => `channel:gateway:${gatewayId}`;

export const YIELD_ALERT_REQUESTED = 'YIELD_ALERT_REQUESTED';
export const YIELD_ALERT_CLEAR_REQUESTED = 'YIELD_ALERT_CLEAR_REQUESTED';
export const ROUTE_UPDATE_REQUESTED = 'ROUTE_UPDATE_REQUESTED';
export const YIELD_ALERT_ACKNOWLEDGED = 'YIELD_ALERT_ACKNOWLEDGED';
export const ALERT_EVENT_TYPES = [YIELD_ALERT_REQUESTED, YIELD_ALERT_CLEAR_REQUESTED, ROUTE_UPDATE_REQUESTED,
  YIELD_ALERT_ACKNOWLEDGED] as const;
export type AlertEventType = typeof ALERT_EVENT_TYPES[number];

export const APPROACH_DIRECTIONS = ['REAR', 'REAR_LEFT', 'REAR_RIGHT', 'LEFT', 'RIGHT'] as const;
export type ApproachDirection = typeof APPROACH_DIRECTIONS[number];
export const RECOMMENDED_ACTIONS = ['YIELD_WHEN_SAFE', 'MOVE_RIGHT_WHEN_SAFE', 'CLEAR_INTERSECTION_FIRST'] as const;
export type RecommendedAction = typeof RECOMMENDED_ACTIONS[number];
export const YIELD_REASONS = ['SAME_ROUTE_AHEAD', 'MOVING_SLOW', 'BLOCK_SECTION'] as const;
export type YieldReason = typeof YIELD_REASONS[number];
export const REQUEST_CLEAR_REASONS = ['EMERGENCY_PASSED', 'ROUTE_CHANGED', 'NO_LONGER_RELEVANT',
  'EMERGENCY_COMPLETED'] as const;
/** EXPIRED is produced only by this worker's expiration scheduler. */
export const CLEAR_REASONS = [...REQUEST_CLEAR_REASONS, 'EXPIRED'] as const;
export type ClearReason = typeof CLEAR_REASONS[number];

export interface Coordinates {latitude: number; longitude: number}

export interface EventEnvelope<TType extends string, TPayload> {
  eventId: string;
  eventType: TType;
  schemaVersion: 1;
  occurredAt: string;
  producer: string;
  payload: TPayload;
}

export interface YieldAlertRequestedPayload {
  alertId: string;
  emergencyId: string;
  dispatchId: string;
  emergencyVehicleId: string;
  targetVehicleId: string;
  approachDirection: ApproachDirection;
  estimatedArrivalSeconds: number;
  recommendedAction: RecommendedAction;
  reason: YieldReason;
  decisionVersion: number;
  expiresAt: string;
}

export interface YieldAlertClearRequestedPayload {
  alertId: string;
  dispatchId: string;
  targetVehicleId: string;
  reason: ClearReason;
}

export interface RouteUpdateRequestedPayload {
  dispatchId: string;
  targetVehicleId: string;
  routeVersion: number;
  destination: Coordinates;
  waypoints: Coordinates[];
  congestedSegments: string[];
  estimatedTravelSeconds: number;
}

export interface YieldAlertAcknowledgedPayload {
  alertId: string;
  vehicleId: string;
  alertVersion: number;
  receivedAt: string;
}

export type AlertStreamEvent =
  | EventEnvelope<typeof YIELD_ALERT_REQUESTED, YieldAlertRequestedPayload>
  | EventEnvelope<typeof YIELD_ALERT_CLEAR_REQUESTED, YieldAlertClearRequestedPayload>
  | EventEnvelope<typeof ROUTE_UPDATE_REQUESTED, RouteUpdateRequestedPayload>
  | EventEnvelope<typeof YIELD_ALERT_ACKNOWLEDGED, YieldAlertAcknowledgedPayload>;

export class ContractError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ContractError';
  }
}

type Fields = Record<string, unknown>;
const isObject = (value: unknown): value is Fields =>
  typeof value === 'object' && value !== null && !Array.isArray(value);
const isText = (value: unknown): value is string => typeof value === 'string' && value.trim().length > 0;
const isInteger = (value: unknown, min = 0): boolean =>
  Number.isSafeInteger(value) && (value as number) >= min;
const isMember = (value: unknown, values: readonly string[]): boolean =>
  typeof value === 'string' && values.includes(value);
const texts = (value: Fields, fields: readonly string[]) => fields.every(field => isText(value[field]));

/** UTC ISO 8601 with a Z suffix, matching the Emergency Worker contract. */
export function isTimestamp(value: unknown): value is string {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(value)) {
    return false;
  }
  const ms = Date.parse(value);
  return Number.isFinite(ms) && new Date(ms).toISOString().slice(0, 19) === value.slice(0, 19);
}

const isPoint = (value: unknown): boolean => isObject(value)
  && typeof value.latitude === 'number' && Number.isFinite(value.latitude) && Math.abs(value.latitude) <= 90
  && typeof value.longitude === 'number' && Number.isFinite(value.longitude) && Math.abs(value.longitude) <= 180;

function isValidPayload(eventType: unknown, p: Fields): boolean {
  switch (eventType) {
    case YIELD_ALERT_REQUESTED:
      return texts(p, ['alertId', 'emergencyId', 'dispatchId', 'emergencyVehicleId', 'targetVehicleId'])
        && isMember(p.approachDirection, APPROACH_DIRECTIONS)
        && isMember(p.recommendedAction, RECOMMENDED_ACTIONS) && isMember(p.reason, YIELD_REASONS)
        && isInteger(p.estimatedArrivalSeconds) && isInteger(p.decisionVersion, 1) && isTimestamp(p.expiresAt);
    case YIELD_ALERT_CLEAR_REQUESTED:
      return texts(p, ['alertId', 'dispatchId', 'targetVehicleId']) && isMember(p.reason, REQUEST_CLEAR_REASONS);
    case ROUTE_UPDATE_REQUESTED:
      return texts(p, ['dispatchId', 'targetVehicleId']) && isInteger(p.routeVersion, 1) && isPoint(p.destination)
        && Array.isArray(p.waypoints) && p.waypoints.length > 0 && p.waypoints.every(isPoint)
        && Array.isArray(p.congestedSegments) && p.congestedSegments.every(isText)
        && isInteger(p.estimatedTravelSeconds);
    case YIELD_ALERT_ACKNOWLEDGED:
      return texts(p, ['alertId', 'vehicleId']) && isInteger(p.alertVersion, 1) && isTimestamp(p.receivedAt);
    default:
      return false;
  }
}

/** Validates required fields only; unknown extra fields are tolerated for forward compatibility. */
export function parseEventEnvelope(value: unknown): AlertStreamEvent {
  if (!isObject(value) || !texts(value, ['eventId', 'eventType', 'producer']) || value.schemaVersion !== 1
    || !isTimestamp(value.occurredAt) || !isObject(value.payload)) {
    throw new ContractError('Invalid event envelope');
  }
  if (!isValidPayload(value.eventType, value.payload)) {
    throw new ContractError(`Invalid ${String(value.eventType)} payload`);
  }
  return value as unknown as AlertStreamEvent;
}

/** Reads the JSON envelope stored under the `event` field of a stream entry. */
export function parseStreamEvent(fields: readonly string[]): AlertStreamEvent {
  if (fields.length % 2 !== 0) throw new ContractError('Unpaired stream fields');
  let raw: string | undefined;
  for (let i = 0; i < fields.length; i += 2) {
    if (fields[i] !== EVENT_FIELD) continue;
    if (raw !== undefined) throw new ContractError('Duplicate event field');
    raw = fields[i + 1];
  }
  if (raw === undefined) throw new ContractError('Missing event field');
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new ContractError('Invalid event JSON');
  }
  return parseEventEnvelope(value);
}
