import type {YieldAlertAcknowledgedPayload} from '../domain/contracts';
import type {AlertDeliveryLogStore, AlertState, AlertStateStore} from '../domain/types';

export interface AcknowledgementServiceDeps {
  states: AlertStateStore;
  logs: AlertDeliveryLogStore;
}

/** Applies client receipts; only an ACK for the alert version currently in force counts. */
export class AcknowledgementService {
  constructor(private readonly deps: AcknowledgementServiceDeps) {}

  async handleAck(p: YieldAlertAcknowledgedPayload): Promise<void> {
    const {states, logs} = this.deps;
    const current = await states.findById(p.alertId);
    if (!current || current.targetVehicleId !== p.vehicleId || !this.isCurrentVersion(current, p.alertVersion)) {
      return;
    }
    // The version fence protects against an update re-sent between the read and this write.
    const acknowledged = await states.transition(p.alertId, ['SENT'], 'ACKNOWLEDGED',
      {acknowledgedAt: p.receivedAt}, p.alertVersion);
    if (acknowledged) await logs.updateStatus(acknowledged.alertLogId, 'ACKNOWLEDGED', p.receivedAt);
  }

  isCurrentVersion(state: AlertState, alertVersion: number): boolean {
    return state.alertVersion === alertVersion;
  }
}
