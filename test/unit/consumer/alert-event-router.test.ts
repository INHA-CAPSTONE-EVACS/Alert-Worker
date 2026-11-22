import {AlertEventRouter} from '../../../src/consumer/alert-event-router';
import type {AlertStreamEvent} from '../../../src/domain/contracts';

const calls: string[] = [];
const router = new AlertEventRouter({
  yieldAlerts: {
    handleRequested: async () => { calls.push('requested'); return {outcome: 'SENT'}; },
    handleClearRequested: async () => { calls.push('clear'); return {outcome: 'SENT'}; },
  },
  routes: {handleRouteUpdate: async () => { calls.push('route'); return {outcome: 'SENT'}; }},
  acks: {handleAck: async () => { calls.push('ack'); }},
});

const event = (eventType: string) => ({eventId: 'e', eventType, schemaVersion: 1,
  occurredAt: '2026-10-12T08:00:00.000Z', producer: 'p', payload: {}}) as unknown as AlertStreamEvent;

beforeEach(() => { calls.length = 0; });

test.each([
  ['YIELD_ALERT_REQUESTED', 'requested'],
  ['YIELD_ALERT_CLEAR_REQUESTED', 'clear'],
  ['ROUTE_UPDATE_REQUESTED', 'route'],
  ['YIELD_ALERT_ACKNOWLEDGED', 'ack'],
])('%s goes to its service', async (eventType, expected) => {
  await router.route(event(eventType));
  expect(calls).toEqual([expected]);
});

test('isSupported recognises only alert event types', () => {
  expect(router.isSupported('YIELD_ALERT_REQUESTED')).toBe(true);
  expect(router.isSupported('VEHICLE_STATE_UPDATED')).toBe(false);
});

test('unsupported event type throws', async () => {
  await expect(router.route(event('VEHICLE_STATE_UPDATED'))).rejects.toThrow('Unsupported');
});
