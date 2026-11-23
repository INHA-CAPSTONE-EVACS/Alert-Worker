import {randomUUID} from 'node:crypto';
import type {AlertDeliveryLog} from '../../../src/domain/types';
import {createMysqlPool} from '../../../src/infra/mysql';
import {MysqlAlertDeliveryLogRepository} from '../../../src/store/alert-delivery-log-repository';
import {TEST_DATABASE_URL, uniqueId} from '../../support/redis';

const pool = createMysqlPool(TEST_DATABASE_URL);
const repo = new MysqlAlertDeliveryLogRepository(pool);
afterAll(() => pool.end());

const log = (overrides: Partial<AlertDeliveryLog> = {}): AlertDeliveryLog => ({
  alertLogId: randomUUID(), alertId: 'ALT-001', alertVersion: 3, dispatchId: uniqueId('D'),
  targetVehicleId: 'EVACS_CAR_000000', selectionReason: 'SAME_ROUTE_AHEAD', estimatedArrivalSeconds: 15,
  alertStatus: 'SENT', firstSentAt: '2026-10-12T08:00:00.000Z', acknowledgedAt: null, clearedAt: null,
  expiresAt: '2026-10-12T08:00:20.000Z', clearReason: null, ...overrides,
});

test('insert is idempotent by alert_log_id', async () => {
  const row = log();
  await repo.insert(row);
  await repo.insert(row);
  expect(await repo.findByDispatchId(row.dispatchId)).toEqual([row]);
});

test('ACKNOWLEDGED sets acknowledged_at', async () => {
  const row = log();
  await repo.insert(row);
  await repo.updateStatus(row.alertLogId, 'ACKNOWLEDGED', '2026-10-12T08:00:01.200Z');
  expect((await repo.findByDispatchId(row.dispatchId))[0]).toMatchObject({
    alertStatus: 'ACKNOWLEDGED', acknowledgedAt: '2026-10-12T08:00:01.200Z', clearedAt: null});
});

test.each(['CLEARED', 'EXPIRED'] as const)('%s sets cleared_at and clear_reason', async status => {
  const row = log();
  await repo.insert(row);
  await repo.updateStatus(row.alertLogId, status, '2026-10-12T08:00:05.000Z', {clearReason: 'EMERGENCY_PASSED'});
  expect((await repo.findByDispatchId(row.dispatchId))[0]).toMatchObject({
    alertStatus: status, clearedAt: '2026-10-12T08:00:05.000Z', clearReason: 'EMERGENCY_PASSED'});
});

test('SENT update refreshes version, eta and expiry but keeps first_sent_at', async () => {
  const row = log();
  await repo.insert(row);
  await repo.updateStatus(row.alertLogId, 'SENT', '2026-10-12T08:00:04.000Z',
    {alertVersion: 5, estimatedArrivalSeconds: 9, expiresAt: '2026-10-12T08:00:24.000Z'});
  expect((await repo.findByDispatchId(row.dispatchId))[0]).toEqual({...row, alertVersion: 5,
    estimatedArrivalSeconds: 9, expiresAt: '2026-10-12T08:00:24.000Z'});
});

test('findByDispatchId orders by first_sent_at', async () => {
  const dispatchId = uniqueId('D');
  const later = log({dispatchId, firstSentAt: '2026-10-12T08:00:05.000Z'});
  const earlier = log({dispatchId, firstSentAt: '2026-10-12T08:00:01.000Z'});
  await repo.insert(later);
  await repo.insert(earlier);
  expect((await repo.findByDispatchId(dispatchId)).map(row => row.alertLogId))
    .toEqual([earlier.alertLogId, later.alertLogId]);
});

test.each(['SENT', 'ACKNOWLEDGED'] as const)('an ended row is never moved back to %s', async status => {
  const row = log();
  await repo.insert(row);
  await repo.updateStatus(row.alertLogId, 'EXPIRED', '2026-10-12T08:00:20.000Z', {clearReason: 'EXPIRED'});
  await repo.updateStatus(row.alertLogId, status, '2026-10-12T08:00:21.000Z', {alertVersion: 9});
  expect((await repo.findByDispatchId(row.dispatchId))[0]).toMatchObject({alertStatus: 'EXPIRED', alertVersion: 3,
    acknowledgedAt: null});
});
