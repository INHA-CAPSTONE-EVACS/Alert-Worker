import type {YieldAlertRequestedPayload} from '../../src/domain/contracts';
import type {AlertState, GatewayFrame} from '../../src/domain/types';

export const NOW = '2026-10-12T08:00:00.000Z';
export const NOW_MS = Date.parse(NOW);

export const state = (overrides: Partial<AlertState> = {}): AlertState => ({
  alertId: 'ALT-001', alertLogId: '00000000-0000-4000-8000-000000000001', dispatchId: 'DSP-001',
  emergencyId: 'EMG-001', targetVehicleId: 'VEH-001', status: 'SENT', alertVersion: 3,
  recommendedAction: 'YIELD_WHEN_SAFE', approachDirection: 'REAR', lastSentAt: NOW, acknowledgedAt: null,
  clearedAt: null, expiresAt: '2026-10-12T08:00:20.000Z', ...overrides,
});

export const requested = (overrides: Partial<YieldAlertRequestedPayload> = {}): YieldAlertRequestedPayload => ({
  alertId: 'ALT-001', emergencyId: 'EMG-001', dispatchId: 'DSP-001', emergencyVehicleId: 'AMB-001',
  targetVehicleId: 'VEH-001', approachDirection: 'REAR', estimatedArrivalSeconds: 15,
  recommendedAction: 'YIELD_WHEN_SAFE', reason: 'SAME_ROUTE_AHEAD', decisionVersion: 3,
  expiresAt: '2026-10-12T08:00:20.000Z', ...overrides,
});

export const frame = (targetVehicleId = 'VEH-001'): GatewayFrame => ({
  targetVehicleId, sessionId: 's-1',
  message: {type: 'YIELD_ALERT_CLEARED', eventType: 'YIELD_ALERT_CLEARED', alertId: 'ALT-001', targetVehicleId,
    reason: 'EMERGENCY_PASSED'},
});
