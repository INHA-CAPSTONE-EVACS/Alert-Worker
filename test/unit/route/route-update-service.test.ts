import {AlertMessageFactory} from '../../../src/alert/alert-message-factory';
import {AlertDeliverer} from '../../../src/delivery/alert-deliverer';
import type {RouteUpdateRequestedPayload} from '../../../src/domain/contracts';
import {RouteUpdateService} from '../../../src/route/route-update-service';
import {FakePublisher, FakeRoutes, FakeSessions} from '../../support/fakes';

let sessions: FakeSessions;
let publisher: FakePublisher;
let routes: FakeRoutes;
let svc: RouteUpdateService;
const route = (routeVersion: number): RouteUpdateRequestedPayload => ({dispatchId: 'DSP-001',
  targetVehicleId: 'AMB-001', routeVersion, destination: {latitude: 37.451, longitude: 126.654},
  waypoints: [{latitude: 37.449, longitude: 126.65}], congestedSegments: [], estimatedTravelSeconds: 180});

beforeEach(() => {
  sessions = new FakeSessions();
  publisher = new FakePublisher();
  routes = new FakeRoutes();
  svc = new RouteUpdateService({routes, deliverer: new AlertDeliverer(sessions, publisher),
    factory: new AlertMessageFactory()});
  sessions.connect('AMB-001', 'gw-2', 's-9');
});

test('route update sends EMERGENCY_ROUTE_UPDATED to emergency vehicle', async () => {
  expect((await svc.handleRouteUpdate(route(3))).outcome).toBe('SENT');
  expect(publisher.events).toEqual([{
    eventKey: 'DSP-001:route:3:EMERGENCY_ROUTE_UPDATED:AMB-001',
    envelope: expect.objectContaining({
      eventType: 'EMERGENCY_ROUTE_UPDATED',
      payload: expect.objectContaining({routeVersion: 3, targetVehicleId: 'AMB-001'}),
    }),
  }]);
  expect(routes.delivered.get('DSP-001')).toBe(3);
});

test('older or equal routeVersion is STALE', async () => {
  await svc.handleRouteUpdate(route(3));
  expect((await svc.handleRouteUpdate(route(2))).outcome).toBe('STALE');
  expect((await svc.handleRouteUpdate(route(3))).outcome).toBe('STALE');
  expect(publisher.events).toHaveLength(1);
});

test('undelivered route does not advance version', async () => {
  publisher.shouldFail = true;
  expect((await svc.handleRouteUpdate(route(3))).outcome).toBe('FAILED');
  expect(routes.delivered.has('DSP-001')).toBe(false);
  publisher.shouldFail = false;
  expect((await svc.handleRouteUpdate(route(3))).outcome).toBe('SENT');
});
