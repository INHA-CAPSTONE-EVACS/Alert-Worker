import {randomUUID} from 'node:crypto';
import type {YieldAlertClearRequestedPayload, YieldAlertRequestedPayload} from '../domain/contracts';
import {
  type AlertDeliveryLog, type AlertDeliveryLogStore, type AlertState, type AlertStateStore, type Deduplicator,
  type DeliveryResult, type EventMeta, type LogStatusExtra, isTerminal,
} from '../domain/types';
import type {AlertDeliverer} from '../delivery/alert-deliverer';
import type {AlertMessageFactory} from './alert-message-factory';

export interface YieldAlertServiceDeps {
  states: AlertStateStore;
  dedup: Deduplicator;
  deliverer: AlertDeliverer;
  logs: AlertDeliveryLogStore;
  factory: AlertMessageFactory;
  clock: () => Date;
}

/** How the history row is written once delivery settles. */
type LogMode = 'insert' | 'update' | 'sync';

const MAX_FENCE_RETRIES = 3;

/** Guidance content that, when unchanged, is not re-sent within the dedup window. */
export const fingerprint = (p: YieldAlertRequestedPayload) => `${p.recommendedAction}|${p.approachDirection}`;

export const sentExtra = (p: YieldAlertRequestedPayload): LogStatusExtra =>
  ({alertVersion: p.decisionVersion, estimatedArrivalSeconds: p.estimatedArrivalSeconds, expiresAt: p.expiresAt});

export function newLog(state: AlertState, p: YieldAlertRequestedPayload): AlertDeliveryLog {
  return {
    alertLogId: state.alertLogId, alertId: p.alertId, alertVersion: p.decisionVersion, dispatchId: p.dispatchId,
    targetVehicleId: p.targetVehicleId, selectionReason: p.reason,
    estimatedArrivalSeconds: p.estimatedArrivalSeconds, alertStatus: state.status,
    firstSentAt: state.status === 'SENT' ? state.lastSentAt : null, acknowledgedAt: null, clearedAt: null,
    expiresAt: p.expiresAt, clearReason: null,
  };
}

/** The moment the stored status took effect, used when re-applying history. */
function statusTime(state: AlertState): string {
  switch (state.status) {
    case 'ACKNOWLEDGED': return state.acknowledgedAt ?? state.decidedAt;
    case 'CLEARED':
    case 'EXPIRED': return state.clearedAt ?? state.decidedAt;
    default: return state.lastSentAt ?? state.decidedAt;
  }
}

/**
 * Controls issuing, updating and clearing yield alerts.
 *
 * Every Redis transition happens before the PostgreSQL write, and a redelivered stream entry whose effect is
 * already in Redis re-applies the idempotent history write. That way a PostgreSQL outage delays history but
 * never loses it or causes a second client message.
 */
export class YieldAlertService {
  constructor(private readonly deps: YieldAlertServiceDeps) {}

  async handleRequested(p: YieldAlertRequestedPayload, meta: EventMeta = this.defaultMeta()): Promise<DeliveryResult> {
    for (let attempt = 0; attempt < MAX_FENCE_RETRIES; attempt++) {
      const result = await this.tryRequested(p, meta);
      if (result) return result;
    }
    throw new Error(`Alert ${p.alertId} kept changing concurrently`);
  }

  async handleClearRequested(p: YieldAlertClearRequestedPayload,
    meta: EventMeta = this.defaultMeta()): Promise<DeliveryResult> {
    const {states, dedup, deliverer, logs, factory, clock} = this.deps;
    const result = (outcome: DeliveryResult['outcome']): DeliveryResult => ({outcome, alertId: p.alertId});
    const state = await states.findById(p.alertId);
    if (!state) return result('NO_ACTIVE_ALERT');
    // alertId is stable per (dispatch, vehicle): a clear older than the current decision belongs to an earlier
    // episode (typically a redelivered entry) and must not end this one.
    if (Date.parse(meta.occurredAt) < Date.parse(state.decidedAt)) return result('STALE');
    if (isTerminal(state.status)) {
      if (state.status === 'CLEARED') {
        await logs.updateStatus(state.alertLogId, 'CLEARED', statusTime(state), {clearReason: p.reason});
      }
      return result('NO_ACTIVE_ALERT');
    }

    const outcome = await deliverer.deliver(state.targetVehicleId,
      factory.createCleared(p.alertId, state.targetVehicleId, p.reason));
    const at = clock().toISOString();
    // The alert ends even if the vehicle is offline: it no longer needs to yield.
    const cleared = await states.transition(p.alertId, ['PENDING', 'SENT', 'ACKNOWLEDGED'], 'CLEARED',
      {clearedAt: at, refresh: null}, state.alertVersion);
    if (!cleared) return result('STALE');
    await dedup.forget(state.dispatchId, state.targetVehicleId);
    await logs.updateStatus(state.alertLogId, 'CLEARED', at, {clearReason: p.reason});
    return result(outcome);
  }

  /** True when the request carries an older decision than the stored state. */
  isStale(p: YieldAlertRequestedPayload, state: AlertState): boolean {
    return p.decisionVersion < state.alertVersion;
  }

  /** Returns null when an episode fence was lost and the request must be re-evaluated. */
  private async tryRequested(p: YieldAlertRequestedPayload, meta: EventMeta): Promise<DeliveryResult | null> {
    const {states, dedup, clock} = this.deps;
    const result = (outcome: DeliveryResult['outcome']): DeliveryResult => ({outcome, alertId: p.alertId});

    // Ordering checks come before the expiry check so a late redelivery can still repair history.
    const existing = await states.findById(p.alertId);
    if (existing && this.isStale(p, existing)) return result('STALE');
    if (existing && existing.alertVersion === p.decisionVersion) {
      // A PENDING state means the previous attempt reserved but never finished sending.
      if (existing.status === 'PENDING') return this.send(existing, p, 'sync');
      await this.syncLog(existing, p);
      return result('DUPLICATE');
    }
    if (Date.parse(p.expiresAt) <= clock().getTime()) return result('EXPIRED');

    const live = existing && !isTerminal(existing.status) ? existing : null;
    if (live && live.status !== 'PENDING'
      && !await dedup.shouldSend(p.dispatchId, p.targetVehicleId, fingerprint(p))) {
      // Keep the newest decision so the sweep can re-issue it instead of letting the old version lapse early.
      const kept = await states.transition(p.alertId, [live.status], live.status, {refresh: p}, live.alertVersion);
      return kept ? result('SUPPRESSED') : null;
    }

    // Reserve before publishing: the sweep only expires SENT/ACKNOWLEDGED, so it cannot end this send midway.
    const reserved: AlertState = {
      alertId: p.alertId,
      alertLogId: live?.alertLogId ?? randomUUID(),
      dispatchId: p.dispatchId,
      emergencyId: p.emergencyId,
      targetVehicleId: p.targetVehicleId,
      status: 'PENDING',
      alertVersion: p.decisionVersion,
      recommendedAction: p.recommendedAction,
      approachDirection: p.approachDirection,
      lastSentAt: live?.lastSentAt ?? null,
      acknowledgedAt: null,
      clearedAt: null,
      expiresAt: p.expiresAt,
      decidedAt: meta.occurredAt,
      refresh: null,
    };
    if (!await states.save(reserved, live?.alertLogId ?? null)) return null;
    return this.send(reserved, p, !live ? 'insert' : live.status === 'PENDING' ? 'sync' : 'update');
  }

  private async send(reserved: AlertState, p: YieldAlertRequestedPayload, mode: LogMode): Promise<DeliveryResult> {
    const {states, dedup, deliverer, logs, factory, clock} = this.deps;
    const outcome = await deliverer.deliver(p.targetVehicleId, factory.createIssued(p));
    const sent = outcome === 'SENT';
    const at = clock().toISOString();
    const settled = await states.transition(p.alertId, ['PENDING'], sent ? 'SENT' : 'FAILED',
      {lastSentAt: sent ? at : reserved.lastSentAt, clearedAt: sent ? null : at}, p.decisionVersion);
    if (!settled) return {outcome: 'STALE', alertId: p.alertId};
    if (sent) await dedup.remember(p.dispatchId, p.targetVehicleId, fingerprint(p));
    if (mode === 'insert') await logs.insert(newLog(settled, p));
    else if (mode === 'update') await logs.updateStatus(settled.alertLogId, settled.status, at, sent ? sentExtra(p) : {});
    else await this.syncLog(settled, p);
    return {outcome, alertId: p.alertId};
  }

  /** Re-applies the history implied by the stored state; every write here is idempotent. */
  private async syncLog(state: AlertState, p: YieldAlertRequestedPayload): Promise<void> {
    const {logs} = this.deps;
    await logs.insert(newLog(state, p));
    await logs.updateStatus(state.alertLogId, state.status, statusTime(state),
      state.status === 'SENT' ? sentExtra(p) : {});
  }

  private defaultMeta(): EventMeta {
    return {eventId: '', occurredAt: this.deps.clock().toISOString()};
  }
}
