import type {
  ApproachDirection, ClearReason, RecommendedAction, RouteUpdateRequestedPayload, YieldAlertRequestedPayload,
} from '../domain/contracts';
import type {
  EmergencyRouteUpdatedMessage, YieldAlertClearedMessage, YieldAlertIssuedMessage,
} from '../domain/types';

const DIRECTION_TEXT: Record<ApproachDirection, string> = {
  REAR: '후방에서',
  REAR_LEFT: '좌측 후방에서',
  REAR_RIGHT: '우측 후방에서',
  LEFT: '좌측에서',
  RIGHT: '우측에서',
};

const ACTION_TEXT: Record<RecommendedAction, string> = {
  YIELD_WHEN_SAFE: '안전한 상황에서 양보해 주세요.',
  MOVE_RIGHT_WHEN_SAFE: '안전할 때 우측으로 이동해 주세요.',
  CLEAR_INTERSECTION_FIRST: '교차로를 먼저 통과한 뒤 안전하게 양보해 주세요.',
};

/** Converts internal request events into client-facing messages. */
export class AlertMessageFactory {
  createIssued(p: YieldAlertRequestedPayload): YieldAlertIssuedMessage {
    return {
      type: 'YIELD_ALERT_ISSUED',
      eventType: 'YIELD_ALERT_ISSUED',
      alertId: p.alertId,
      emergencyId: p.emergencyId,
      dispatchId: p.dispatchId,
      targetVehicleId: p.targetVehicleId,
      approachDirection: p.approachDirection,
      estimatedArrivalSeconds: p.estimatedArrivalSeconds,
      recommendedAction: p.recommendedAction,
      message: this.buildText(p.approachDirection, p.recommendedAction),
      alertVersion: p.decisionVersion,
      expiresAt: p.expiresAt,
    };
  }

  createCleared(alertId: string, targetVehicleId: string, reason: ClearReason): YieldAlertClearedMessage {
    return {type: 'YIELD_ALERT_CLEARED', eventType: 'YIELD_ALERT_CLEARED', alertId, targetVehicleId, reason};
  }

  createRouteUpdated(p: RouteUpdateRequestedPayload): EmergencyRouteUpdatedMessage {
    return {
      type: 'EMERGENCY_ROUTE_UPDATED',
      eventType: 'EMERGENCY_ROUTE_UPDATED',
      dispatchId: p.dispatchId,
      targetVehicleId: p.targetVehicleId,
      routeVersion: p.routeVersion,
      destination: p.destination,
      waypoints: p.waypoints,
      congestedSegments: p.congestedSegments,
      estimatedTravelSeconds: p.estimatedTravelSeconds,
    };
  }

  buildText(direction: ApproachDirection, action: RecommendedAction): string {
    return `${DIRECTION_TEXT[direction]} 긴급차량이 접근하고 있습니다. ${ACTION_TEXT[action]}`;
  }
}
