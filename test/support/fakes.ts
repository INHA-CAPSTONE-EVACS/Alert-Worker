import type {
  AlertDeliveryLog, AlertDeliveryLogStore, AlertState, AlertStateStore, AlertStatus, Deduplicator,
  DeliveryPublisher, GatewayFrame, LogStatusExtra, RouteVersionStore, SessionInfo, SessionLocator,
} from '../../src/domain/types';
import {isTerminal} from '../../src/domain/types';

/** In-memory AlertStateStore with the same version and status guards as the Redis implementation. */
export class FakeStates implements AlertStateStore {
  readonly states = new Map<string, AlertState>();
  readonly active = new Map<string, string>();

  get(alertId: string): AlertState | undefined { return this.states.get(alertId); }

  async findById(alertId: string) { return this.states.get(alertId) ?? null; }

  async findActive(dispatchId: string, vehicleId: string) {
    const id = this.active.get(`${dispatchId}:${vehicleId}`);
    const state = id ? this.states.get(id) : undefined;
    return state && !isTerminal(state.status) ? state : null;
  }

  async save(state: AlertState) {
    const current = this.states.get(state.alertId);
    if (current && current.alertVersion > state.alertVersion) return false;
    this.states.set(state.alertId, {...state});
    const key = `${state.dispatchId}:${state.targetVehicleId}`;
    if (isTerminal(state.status)) {
      if (this.active.get(key) === state.alertId) this.active.delete(key);
    } else {
      this.active.set(key, state.alertId);
    }
    return true;
  }

  async transition(alertId: string, from: readonly AlertStatus[], to: AlertStatus, patch: Partial<AlertState>,
    expectedVersion?: number) {
    const current = this.states.get(alertId);
    if (!current || !from.includes(current.status)) return null;
    if (expectedVersion !== undefined && current.alertVersion !== expectedVersion) return null;
    const next = {...current, ...patch, status: to};
    this.states.set(alertId, next);
    const key = `${next.dispatchId}:${next.targetVehicleId}`;
    if (isTerminal(to) && this.active.get(key) === alertId) this.active.delete(key);
    return next;
  }

  async dueForExpiry(nowMs: number) {
    return [...this.states.values()]
      .filter(state => (state.status === 'SENT' || state.status === 'ACKNOWLEDGED')
        && Date.parse(state.expiresAt) <= nowMs)
      .map(state => state.alertId);
  }
}

export class FakeDedup implements Deduplicator {
  readonly records = new Map<string, string>();
  async shouldSend(dispatchId: string, vehicleId: string, fingerprint: string) {
    return this.records.get(`${dispatchId}:${vehicleId}`) !== fingerprint;
  }
  async remember(dispatchId: string, vehicleId: string, fingerprint: string) {
    this.records.set(`${dispatchId}:${vehicleId}`, fingerprint);
  }
  async forget(dispatchId: string, vehicleId: string) {
    this.records.delete(`${dispatchId}:${vehicleId}`);
  }
}

export class FakeSessions implements SessionLocator {
  readonly sessions = new Map<string, SessionInfo>();
  connect(vehicleId: string, gatewayId = 'gw-1', sessionId = 's-1') {
    this.sessions.set(vehicleId, {vehicleId, gatewayId, sessionId, connectedAt: '2026-10-12T07:59:00.000Z'});
  }
  async findGateway(vehicleId: string) { return this.sessions.get(vehicleId) ?? null; }
}

export class FakePublisher implements DeliveryPublisher {
  readonly frames: {gatewayId: string; frame: GatewayFrame}[] = [];
  receivers = 1;
  async publish(gatewayId: string, frame: GatewayFrame) {
    this.frames.push({gatewayId, frame});
    return this.receivers;
  }
}

export class FakeLogs implements AlertDeliveryLogStore {
  readonly rows = new Map<string, AlertDeliveryLog>();
  insertCalls = 0;
  readonly updates: {alertLogId: string; status: AlertStatus; at: string; extra?: LogStatusExtra}[] = [];
  async insert(log: AlertDeliveryLog) {
    this.insertCalls++;
    if (!this.rows.has(log.alertLogId)) this.rows.set(log.alertLogId, {...log});
  }
  async updateStatus(alertLogId: string, status: AlertStatus, at: string, extra?: LogStatusExtra) {
    this.updates.push({alertLogId, status, at, extra});
    const row = this.rows.get(alertLogId);
    if (row) this.rows.set(alertLogId, {...row, alertStatus: status});
  }
  async findByDispatchId(dispatchId: string) {
    return [...this.rows.values()].filter(row => row.dispatchId === dispatchId);
  }
}

export class FakeRoutes implements RouteVersionStore {
  readonly delivered = new Map<string, number>();
  async isLatest(dispatchId: string, routeVersion: number) {
    return routeVersion > (this.delivered.get(dispatchId) ?? 0);
  }
  async markDelivered(dispatchId: string, routeVersion: number) {
    if (routeVersion > (this.delivered.get(dispatchId) ?? 0)) this.delivered.set(dispatchId, routeVersion);
  }
}
