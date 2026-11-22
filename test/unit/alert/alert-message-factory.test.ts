import {AlertMessageFactory} from '../../../src/alert/alert-message-factory';
import {
  APPROACH_DIRECTIONS, RECOMMENDED_ACTIONS, type RouteUpdateRequestedPayload, type YieldAlertRequestedPayload,
} from '../../../src/domain/contracts';

const factory = new AlertMessageFactory();
const requested: YieldAlertRequestedPayload = {alertId: 'ALT-001', emergencyId: 'EMG-001', dispatchId: 'DSP-001',
  emergencyVehicleId: 'AMB-001', targetVehicleId: 'VEH-001', approachDirection: 'REAR', estimatedArrivalSeconds: 15,
  recommendedAction: 'YIELD_WHEN_SAFE', reason: 'SAME_ROUTE_AHEAD', decisionVersion: 3,
  expiresAt: '2026-10-12T08:00:20.000Z'};

test('issued message matches design example', () => {
  expect(factory.createIssued(requested)).toEqual({
    type: 'YIELD_ALERT_ISSUED', eventType: 'YIELD_ALERT_ISSUED', alertId: 'ALT-001', emergencyId: 'EMG-001',
    dispatchId: 'DSP-001', targetVehicleId: 'VEH-001', approachDirection: 'REAR', estimatedArrivalSeconds: 15,
    recommendedAction: 'YIELD_WHEN_SAFE',
    message: '후방에서 긴급차량이 접근하고 있습니다. 안전한 상황에서 양보해 주세요.',
    alertVersion: 3, expiresAt: '2026-10-12T08:00:20.000Z'});
});

test('every direction and action combination yields distinct Korean text', () => {
  const texts = new Set<string>();
  for (const direction of APPROACH_DIRECTIONS) {
    for (const action of RECOMMENDED_ACTIONS) {
      const text = factory.buildText(direction, action);
      expect(text).toMatch(/^.+에서 긴급차량이 접근하고 있습니다\. .+\.$/);
      texts.add(text);
    }
  }
  expect(texts.size).toBe(APPROACH_DIRECTIONS.length * RECOMMENDED_ACTIONS.length);
});

test('cleared message carries reason', () => {
  expect(factory.createCleared('ALT-001', 'VEH-001', 'EMERGENCY_PASSED')).toEqual({
    type: 'YIELD_ALERT_CLEARED', eventType: 'YIELD_ALERT_CLEARED', alertId: 'ALT-001',
    targetVehicleId: 'VEH-001', reason: 'EMERGENCY_PASSED'});
});

test('route updated carries waypoints, congestedSegments, estimatedTravelSeconds', () => {
  const route: RouteUpdateRequestedPayload = {dispatchId: 'DSP-001', targetVehicleId: 'AMB-001', routeVersion: 3,
    destination: {latitude: 37.451, longitude: 126.654},
    waypoints: [{latitude: 37.449, longitude: 126.65}, {latitude: 37.451, longitude: 126.654}],
    congestedSegments: ['EDGE-101'], estimatedTravelSeconds: 180};
  expect(factory.createRouteUpdated(route)).toEqual({
    type: 'EMERGENCY_ROUTE_UPDATED', eventType: 'EMERGENCY_ROUTE_UPDATED', ...route});
});
