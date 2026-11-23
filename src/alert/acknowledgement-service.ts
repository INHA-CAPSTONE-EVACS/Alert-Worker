import type {YieldAlertAcknowledgedPayload} from '../domain/contracts';
import type {AlertDeliveryLogStore, AlertedVehicleStore, AlertState, AlertStateStore} from '../domain/types';

export interface AcknowledgementServiceDeps {
  states: AlertStateStore;
  logs: AlertDeliveryLogStore;
  alerted: AlertedVehicleStore;
}

/** Applies client receipts; only an ACK for the alert version currently in force counts. */
export class AcknowledgementService {
  constructor(private readonly deps: AcknowledgementServiceDeps) {}

  async handleAck(p: YieldAlertAcknowledgedPayload): Promise<void> {
    const {states, logs, alerted} = this.deps;
    const current = await states.findById(p.alertId);
    if (!current || current.targetVehicleId !== p.vehicleId || !this.isCurrentVersion(current, p.alertVersion)) {
      return;
    }
    if (current.status === 'ACKNOWLEDGED') {
      // Redelivered ACK: the state already reflects it, so only re-apply the idempotent history write.
      await logs.updateStatus(current.alertLogId, 'ACKNOWLEDGED', current.acknowledgedAt ?? p.receivedAt);
      await alerted.add(current.emergencyId, current.targetVehicleId);
      return;
    }
    // The version fence protects against an update re-sent between the read and this write.
    const acknowledged = await states.transition(p.alertId, ['SENT'], 'ACKNOWLEDGED',
      {acknowledgedAt: p.receivedAt}, p.alertVersion);
    if (!acknowledged) return;
    await logs.updateStatus(acknowledged.alertLogId, 'ACKNOWLEDGED', p.receivedAt);
    // After the history write, so a redelivery that repairs history also repairs the dashboard set.
    await alerted.add(acknowledged.emergencyId, acknowledged.targetVehicleId);
  }

  isCurrentVersion(state: AlertState, alertVersion: number): boolean {
    return state.alertVersion === alertVersion;
  }
}
