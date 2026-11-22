import type {AlertDeliverer} from '../delivery/alert-deliverer';
import type {AlertDeliveryLogStore, AlertStateStore, Deduplicator} from '../domain/types';
import {describeError, logger} from '../infra/logger';
import type {AlertMessageFactory} from './alert-message-factory';

export interface AlertExpirationSchedulerDeps {
  states: AlertStateStore;
  dedup: Deduplicator;
  deliverer: AlertDeliverer;
  logs: AlertDeliveryLogStore;
  factory: AlertMessageFactory;
  clock: () => Date;
}

const SWEEP_BATCH = 100;

/** Closes alerts whose validity ended without a clear request. */
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

  async sweepExpired(now: Date): Promise<number> {
    const {states, dedup, deliverer, logs, factory} = this.deps;
    const at = now.toISOString();
    let expired = 0;
    for (const alertId of await states.dueForExpiry(now.getTime(), SWEEP_BATCH)) {
      try {
        // Transition first: a clear or ACK that already moved the alert wins the race.
        const state = await states.transition(alertId, ['SENT', 'ACKNOWLEDGED'], 'EXPIRED', {clearedAt: at});
        if (!state) {
          await states.unschedule(alertId);
          continue;
        }
        await dedup.forget(state.dispatchId, state.targetVehicleId);
        await logs.updateStatus(state.alertLogId, 'EXPIRED', at, {clearReason: 'EXPIRED'});
        await deliverer.deliver(state.targetVehicleId,
          factory.createCleared(state.alertId, state.targetVehicleId, 'EXPIRED'));
        expired++;
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
}
