import {randomUUID} from 'node:crypto';
import type {YieldAlertClearRequestedPayload, YieldAlertRequestedPayload} from '../domain/contracts';
import {
  type AlertDeliveryLog, type AlertDeliveryLogStore, type AlertState, type AlertStateStore, type Deduplicator,
  type DeliveryResult, isTerminal,
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

/** Guidance content that, when unchanged, is not re-sent within the dedup window. */
const fingerprint = (p: YieldAlertRequestedPayload) => `${p.recommendedAction}|${p.approachDirection}`;

/** Controls issuing, updating and clearing yield alerts. */
export class YieldAlertService {
  constructor(private readonly deps: YieldAlertServiceDeps) {}

  async handleRequested(p: YieldAlertRequestedPayload): Promise<DeliveryResult> {
    const {states, dedup, deliverer, logs, factory, clock} = this.deps;
    const now = clock();
    const result = (outcome: DeliveryResult['outcome']): DeliveryResult => ({outcome, alertId: p.alertId});
    if (Date.parse(p.expiresAt) <= now.getTime()) return result('EXPIRED');

    const existing = await states.findById(p.alertId);
    if (existing && this.isStale(p, existing)) return result('STALE');
    if (existing && existing.alertVersion === p.decisionVersion) {
      // Redelivered stream entry: the message already went out; only make sure history was written.
      await logs.insert(this.newLog(existing.alertLogId, p, existing.status, existing.lastSentAt));
      return result('DUPLICATE');
    }
    const active = existing && !isTerminal(existing.status) ? existing : null;
    if (active && !await dedup.shouldSend(p.dispatchId, p.targetVehicleId, fingerprint(p))) {
      return result('SUPPRESSED');
    }

    const outcome = await deliverer.deliver(p.targetVehicleId, factory.createIssued(p));
    const sent = outcome === 'SENT';
    const at = now.toISOString();
    const state: AlertState = {
      alertId: p.alertId,
      alertLogId: active?.alertLogId ?? randomUUID(),
      dispatchId: p.dispatchId,
      emergencyId: p.emergencyId,
      targetVehicleId: p.targetVehicleId,
      status: sent ? 'SENT' : 'FAILED',
      alertVersion: p.decisionVersion,
      recommendedAction: p.recommendedAction,
      approachDirection: p.approachDirection,
      lastSentAt: sent ? at : active?.lastSentAt ?? null,
      acknowledgedAt: null,
      clearedAt: null,
      expiresAt: p.expiresAt,
    };
    if (!await states.save(state)) return result('STALE');
    if (sent) await dedup.remember(p.dispatchId, p.targetVehicleId, fingerprint(p));

    if (active) {
      await logs.updateStatus(state.alertLogId, state.status, at, sent
        ? {alertVersion: p.decisionVersion, estimatedArrivalSeconds: p.estimatedArrivalSeconds, expiresAt: p.expiresAt}
        : {});
    } else {
      await logs.insert(this.newLog(state.alertLogId, p, state.status, sent ? at : null));
    }
    return result(outcome);
  }

  async handleClearRequested(p: YieldAlertClearRequestedPayload): Promise<DeliveryResult> {
    const {states, dedup, deliverer, logs, factory, clock} = this.deps;
    const active = await states.findById(p.alertId);
    if (!active || isTerminal(active.status)) return {outcome: 'NO_ACTIVE_ALERT', alertId: p.alertId};

    const outcome = await deliverer.deliver(active.targetVehicleId,
      factory.createCleared(p.alertId, active.targetVehicleId, p.reason));
    const at = clock().toISOString();
    // The alert ends even if the vehicle is offline: it no longer needs to yield.
    const cleared = await states.transition(p.alertId, ['PENDING', 'SENT', 'ACKNOWLEDGED'], 'CLEARED',
      {clearedAt: at});
    if (!cleared) return {outcome: 'NO_ACTIVE_ALERT', alertId: p.alertId};
    await dedup.forget(active.dispatchId, active.targetVehicleId);
    await logs.updateStatus(active.alertLogId, 'CLEARED', at, {clearReason: p.reason});
    return {outcome, alertId: p.alertId};
  }

  /** True when the request carries an older decision than the stored state. */
  isStale(p: YieldAlertRequestedPayload, state: AlertState): boolean {
    return p.decisionVersion < state.alertVersion;
  }

  private newLog(alertLogId: string, p: YieldAlertRequestedPayload, status: AlertState['status'],
    firstSentAt: string | null): AlertDeliveryLog {
    return {
      alertLogId, alertId: p.alertId, alertVersion: p.decisionVersion, dispatchId: p.dispatchId,
      targetVehicleId: p.targetVehicleId, selectionReason: p.reason,
      estimatedArrivalSeconds: p.estimatedArrivalSeconds, alertStatus: status, firstSentAt, acknowledgedAt: null,
      clearedAt: null, expiresAt: p.expiresAt, clearReason: null,
    };
  }
}
