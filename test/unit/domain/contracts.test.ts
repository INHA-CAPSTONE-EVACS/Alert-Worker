import {ContractError, parseStreamEvent} from '../../../src/domain/contracts';

const envelope = (eventType: string, payload: object) =>
  ['eventKey', 'k1', 'event', JSON.stringify({eventId: 'e1', eventType, schemaVersion: 1,
    occurredAt: '2026-10-12T08:00:00.000Z', producer: 'emergency-worker', payload})];
const requested = {alertId: 'ALT-001', emergencyId: 'EMG-001', dispatchId: 'DSP-001', emergencyVehicleId: 'AMB-001',
  targetVehicleId: 'VEH-001', approachDirection: 'REAR', estimatedArrivalSeconds: 15,
  recommendedAction: 'YIELD_WHEN_SAFE', reason: 'SAME_ROUTE_AHEAD', decisionVersion: 3,
  expiresAt: '2026-10-12T08:00:20.000Z'};
const route = {dispatchId: 'DSP-001', targetVehicleId: 'AMB-001', routeVersion: 3,
  destination: {latitude: 37.451, longitude: 126.654},
  waypoints: [{latitude: 37.449, longitude: 126.65}, {latitude: 37.451, longitude: 126.654}],
  congestedSegments: ['EDGE-101'], estimatedTravelSeconds: 180};
const ack = {alertId: 'ALT-001', vehicleId: 'VEH-001', alertVersion: 3, receivedAt: '2026-10-12T08:00:01.200Z'};

test('parses YIELD_ALERT_REQUESTED from event field', () => {
  const event = parseStreamEvent(envelope('YIELD_ALERT_REQUESTED', requested));
  expect(event.eventType).toBe('YIELD_ALERT_REQUESTED');
  expect(event.payload).toEqual(requested);
});

test('accepts unknown extra payload fields', () => {
  expect(() => parseStreamEvent(envelope('YIELD_ALERT_REQUESTED', {...requested, extra: 1}))).not.toThrow();
});

test('parses ROUTE_UPDATE_REQUESTED with congestedSegments', () => {
  expect(parseStreamEvent(envelope('ROUTE_UPDATE_REQUESTED', route)).payload).toEqual(route);
});

test.each(['EMERGENCY_PASSED', 'ROUTE_CHANGED', 'NO_LONGER_RELEVANT', 'EMERGENCY_COMPLETED'])(
  'parses YIELD_ALERT_CLEAR_REQUESTED with reason %s', reason => {
    const payload = {alertId: 'ALT-001', dispatchId: 'DSP-001', targetVehicleId: 'VEH-001', reason};
    expect(parseStreamEvent(envelope('YIELD_ALERT_CLEAR_REQUESTED', payload)).payload).toEqual(payload);
  });

test('parses YIELD_ALERT_ACKNOWLEDGED', () => {
  expect(parseStreamEvent(envelope('YIELD_ALERT_ACKNOWLEDGED', ack)).payload).toEqual(ack);
});

test.each<[string, string[]]>([
  ['missing event field', ['payload', '{}']],
  ['bad json', ['event', '{']],
  ['unpaired fields', ['event']],
  ['unknown approachDirection', envelope('YIELD_ALERT_REQUESTED', {...requested, approachDirection: 'FRONT'})],
  ['decisionVersion 0', envelope('YIELD_ALERT_REQUESTED', {...requested, decisionVersion: 0})],
  ['invalid expiresAt', envelope('YIELD_ALERT_REQUESTED', {...requested, expiresAt: 'tomorrow'})],
  ['unsupported eventType', envelope('VEHICLE_STATE_UPDATED', {})],
  ['ack without alertVersion', envelope('YIELD_ALERT_ACKNOWLEDGED',
    {alertId: 'A', vehicleId: 'V', receivedAt: '2026-10-12T08:00:01.200Z'})],
  ['clear with unknown reason', envelope('YIELD_ALERT_CLEAR_REQUESTED',
    {alertId: 'A', dispatchId: 'D', targetVehicleId: 'V', reason: 'EXPIRED'})],
  ['route with empty waypoints', envelope('ROUTE_UPDATE_REQUESTED', {...route, waypoints: []})],
])('rejects %s', (_, fields) => {
  expect(() => parseStreamEvent(fields)).toThrow(ContractError);
});
