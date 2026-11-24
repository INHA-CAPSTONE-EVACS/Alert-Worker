import type {
  AlertDeliveryLog, AlertDeliveryLogStore, AlertState, AlertStateStore, AlertStatus, Deduplicator,
  DeliveryPublisher, GatewayFrame, GatewayOutboundEvent, LogStatusExtra, RouteVersionStore, SessionInfo, SessionLocator,
  AlertedVehicleStore,
} from '../../src/domain/types';
import {isTerminal} from '../../src/domain/types';

/** In-memory AlertStateStore with the same version and status guards as the Redis implementation. */
export class FakeStates implements AlertStateStore {
  readonly states = new Map<string, AlertState>();
  readonly active = new Map<string, string>();
  readonly unscheduled: string[] = [];

  get(alertId: string): AlertState | undefined { return this.states.get(alertId); }

  async findById(alertId: string) { return this.states.get(alertId) ?? null; }

  async findActive(dispatchId: string, vehicleId: string) {
    const id = this.active.get(`${dispatchId}:${vehicleId}`);
    const state = id ? this.states.get(id) : undefined;
    return state && !isTerminal(state.status) ? state : null;
  }

  async save(state: AlertState, expectedEpisode: string | null) {
    const current = this.states.get(state.alertId);
    if (current && current.alertVersion > state.alertVersion) return false;
    const live = current && !isTerminal(current.status) ? current : undefined;
    if (expectedEpisode === null ? live !== undefined && live.alertLogId !== state.alertLogId
      : live?.alertLogId !== expectedEpisode) return false;
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

  async unschedule(alertId: string) {
    const current = this.states.get(alertId);
    if (current && (current.status === 'SENT' || current.status === 'ACKNOWLEDGED')) return;
    this.unscheduled.push(alertId);
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
  readonly events: {eventKey: string; envelope: GatewayOutboundEvent}[] = [];
  readonly frames: {gatewayId: string; frame: GatewayFrame}[] = [];
  receivers = 1;
  shouldFail = false;

  async publish(eventKey: string, envelope: GatewayOutboundEvent): Promise<string | number> {
    if (this.shouldFail || this.receivers === 0) return 0;
    this.events.push({eventKey, envelope});
    const payload = (envelope.payload ?? {}) as Record<string, unknown>;
    const targetVehicleId = (payload.targetVehicleId ?? payload.vehicleId ?? '') as string;
    this.frames.push({
      gatewayId: 'gateway',
      frame: {
        targetVehicleId,
        sessionId: '',
        message: {
          type: envelope.eventType as any,
          eventType: envelope.eventType as any,
          ...payload,
        } as any,
      },
    });
    return '1-0';
  }
}

const ENDED: readonly AlertStatus[] = ['CLEARED', 'EXPIRED', 'FAILED'];

/** Mirrors the SQL guards: inserts are idempotent and an ended row never goes back to a live status. */
export class FakeLogs implements AlertDeliveryLogStore {
  readonly rows = new Map<string, AlertDeliveryLog>();
  insertCalls = 0;
  /** Number of upcoming writes (insert or updateStatus) that throw, simulating a MySQL outage. */
  failNext = 0;
  private maybeFail() {
    if (this.failNext > 0) {
      this.failNext--;
      throw new Error('database unavailable');
    }
  }
  readonly updates: {alertLogId: string; status: AlertStatus; at: string; extra?: LogStatusExtra}[] = [];
  async insert(log: AlertDeliveryLog) {
    this.maybeFail();
    this.insertCalls++;
    if (!this.rows.has(log.alertLogId)) this.rows.set(log.alertLogId, {...log});
  }
  async updateStatus(alertLogId: string, status: AlertStatus, at: string, extra?: LogStatusExtra) {
    this.maybeFail();
    this.updates.push({alertLogId, status, at, extra});
    const row = this.rows.get(alertLogId);
    if (!row || (ENDED.includes(row.alertStatus) && !ENDED.includes(status))) return;
    this.rows.set(alertLogId, {...row, alertStatus: status, alertVersion: extra?.alertVersion ?? row.alertVersion,
      acknowledgedAt: status === 'ACKNOWLEDGED' ? at : row.acknowledgedAt,
      clearedAt: ENDED.includes(status) ? at : row.clearedAt, clearReason: extra?.clearReason ?? row.clearReason});
  }
  async findByDispatchId(dispatchId: string) {
    return [...this.rows.values()].filter(row => row.dispatchId === dispatchId);
  }
}

export class FakeAlerted implements AlertedVehicleStore {
  readonly sets = new Map<string, Set<string>>();
  async add(emergencyId: string, vehicleId: string) {
    this.sets.set(emergencyId, (this.sets.get(emergencyId) ?? new Set()).add(vehicleId));
  }
  members(emergencyId: string) {
    return [...(this.sets.get(emergencyId) ?? [])].sort();
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
