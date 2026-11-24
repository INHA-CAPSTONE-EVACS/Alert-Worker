import type {AlertDeliverer} from '../delivery/alert-deliverer';
import type {AlertDeliveryLogStore, AlertState, AlertStateStore, Deduplicator} from '../domain/types';
import {describeError, logger} from '../infra/logger';
import type {AlertMessageFactory} from './alert-message-factory';
import {fingerprint, sentExtra} from './yield-alert-service';

export interface AlertExpirationSchedulerDeps {
  states: AlertStateStore;
  dedup: Deduplicator;
  deliverer: AlertDeliverer;
  logs: AlertDeliveryLogStore;
  factory: AlertMessageFactory;
  clock: () => Date;
}

const SWEEP_BATCH = 100;
/** A suppressed newer decision is re-issued this long before the current version lapses on the client. */
export const REFRESH_LEAD_MS = 2000;

/**
 * Closes alerts whose validity ended without a clear request, or extends them with the newest decision that
 * dedup suppressed.
 */
export class AlertExpirationScheduler {
  private timer?: ReturnType<typeof setInterval>;
  private sweeping = false;

  constructor(private readonly deps: AlertExpirationSchedulerDeps) {}

  start(intervalMs: number): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      // Skip a tick rather than overlap a slow sweep.
      if (this.sweeping) return;
      this.sweeping = true;
      this.sweepExpired(this.deps.clock())
        .catch(error => logger.error('expiry sweep failed', {error: describeError(error)}))
        .finally(() => { this.sweeping = false; });
    }, intervalMs);
  }

  /** Returns the number of alerts moved to EXPIRED. */
  async sweepExpired(now: Date): Promise<number> {
    const {states} = this.deps;
    const nowMs = now.getTime();
    let expired = 0;
    for (const alertId of await states.dueForExpiry(nowMs + REFRESH_LEAD_MS, SWEEP_BATCH)) {
      try {
        const state = await states.findById(alertId);
        if (!state || (state.status !== 'SENT' && state.status !== 'ACKNOWLEDGED')) {
          await states.unschedule(alertId);
          continue;
        }
        const refresh = state.refresh;
        if (refresh && Date.parse(refresh.expiresAt) > Math.max(Date.parse(state.expiresAt), nowMs)) {
          await this.reissue(state, now);
          continue;
        }
        if (Date.parse(state.expiresAt) > nowMs) continue;
        if (await this.expire(state, now)) expired++;
      } catch (error) {
        logger.error('alert expiry failed', {alertId, error: describeError(error)});
      }
    }
    return expired;
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  private async expire(state: AlertState, now: Date): Promise<boolean> {
    const {states, dedup, deliverer, logs, factory} = this.deps;
    const at = now.toISOString();
    // The version fence makes a concurrent update or clear win; a failed fence leaves the newer schedule alone.
    const expired = await states.transition(state.alertId, ['SENT', 'ACKNOWLEDGED'], 'EXPIRED',
      {clearedAt: at, refresh: null}, state.alertVersion);
    if (!expired) {
      await states.unschedule(state.alertId);
      return false;
    }
    // The alert is out of the expiry index now, so each follow-up step must run even if another fails.
    await this.attempt(state.alertId, 'deliver', () => deliverer.deliver(state.targetVehicleId,
      factory.createCleared(state.alertId, state.targetVehicleId, 'EXPIRED', state.dispatchId)));
    await this.attempt(state.alertId, 'dedup', () => dedup.forget(state.dispatchId, state.targetVehicleId));
    await this.attempt(state.alertId, 'history', () =>
      logs.updateStatus(state.alertLogId, 'EXPIRED', at, {clearReason: 'EXPIRED'}));
    return true;
  }

  private async reissue(state: AlertState, now: Date): Promise<void> {
    const {states, dedup, deliverer, logs, factory} = this.deps;
    const p = state.refresh!;
    const at = now.toISOString();
    const updated = await states.transition(state.alertId, [state.status], 'SENT', {
      alertVersion: p.decisionVersion, expiresAt: p.expiresAt, recommendedAction: p.recommendedAction,
      approachDirection: p.approachDirection, lastSentAt: at, acknowledgedAt: null, refresh: null,
    }, state.alertVersion);
    if (!updated) return;
    await deliverer.deliver(p.targetVehicleId, factory.createIssued(p));
    await this.attempt(state.alertId, 'dedup', () => dedup.remember(p.dispatchId, p.targetVehicleId, fingerprint(p)));
    await this.attempt(state.alertId, 'history', () => logs.updateStatus(state.alertLogId, 'SENT', at, sentExtra(p)));
  }

  private async attempt(alertId: string, step: string, action: () => Promise<unknown>): Promise<void> {
    try {
      await action();
    } catch (error) {
      logger.error('alert expiry step failed', {alertId, step, error: describeError(error)});
    }
  }
}
