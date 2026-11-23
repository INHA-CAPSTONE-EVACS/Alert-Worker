import type {Pool, RowDataPacket} from 'mysql2/promise';
import type {
  AlertDeliveryLog, AlertDeliveryLogStore, AlertStatus, LogStatusExtra,
} from '../domain/types';

interface Row extends RowDataPacket {
  alert_log_id: string;
  alert_id: string;
  alert_version: number;
  dispatch_id: string;
  target_vehicle_id: string;
  selection_reason: string;
  estimated_arrival_seconds: number | null;
  alert_status: AlertStatus;
  first_sent_at: Date | null;
  acknowledged_at: Date | null;
  cleared_at: Date | null;
  expires_at: Date | null;
  clear_reason: string | null;
}

const iso = (value: Date | null) => value ? value.toISOString() : null;
const date = (value: string | null | undefined) => value ? new Date(value) : null;

/** alert_delivery_log persistence. All writes are safe to repeat on stream redelivery. */
export class MysqlAlertDeliveryLogRepository implements AlertDeliveryLogStore {
  constructor(private readonly pool: Pool) {}

  async insert(log: AlertDeliveryLog): Promise<void> {
    // The no-op update keeps a redelivered insert idempotent without INSERT IGNORE swallowing CHECK failures.
    await this.pool.query(
      `INSERT INTO alert_delivery_log (alert_log_id, alert_id, alert_version, dispatch_id, target_vehicle_id,
         selection_reason, estimated_arrival_seconds, alert_status, first_sent_at, acknowledged_at, cleared_at,
         expires_at, clear_reason)
       VALUES (:alertLogId, :alertId, :alertVersion, :dispatchId, :targetVehicleId, :selectionReason,
         :estimatedArrivalSeconds, :alertStatus, :firstSentAt, :acknowledgedAt, :clearedAt, :expiresAt, :clearReason)
       ON DUPLICATE KEY UPDATE alert_log_id = alert_log_id`,
      {...log, firstSentAt: date(log.firstSentAt), acknowledgedAt: date(log.acknowledgedAt),
        clearedAt: date(log.clearedAt), expiresAt: date(log.expiresAt)});
  }

  async updateStatus(alertLogId: string, status: AlertStatus, at: string, extra: LogStatusExtra = {}): Promise<void> {
    await this.pool.query(
      `UPDATE alert_delivery_log SET
         alert_status = :status,
         acknowledged_at = IF(:acknowledged, :at, acknowledged_at),
         cleared_at = IF(:ended, :at, cleared_at),
         first_sent_at = COALESCE(first_sent_at, IF(:status = 'SENT', :at, NULL)),
         alert_version = COALESCE(:alertVersion, alert_version),
         estimated_arrival_seconds = COALESCE(:estimatedArrivalSeconds, estimated_arrival_seconds),
         expires_at = COALESCE(:expiresAt, expires_at),
         clear_reason = COALESCE(:clearReason, clear_reason)
       WHERE alert_log_id = :alertLogId
         -- An ended episode is never revived by a late or redelivered write.
         AND NOT (alert_status IN ('CLEARED', 'EXPIRED', 'FAILED')
                  AND :status NOT IN ('CLEARED', 'EXPIRED', 'FAILED'))`,
      {alertLogId, status, at: new Date(at), acknowledged: status === 'ACKNOWLEDGED',
        ended: status === 'CLEARED' || status === 'EXPIRED' || status === 'FAILED',
        alertVersion: extra.alertVersion ?? null, estimatedArrivalSeconds: extra.estimatedArrivalSeconds ?? null,
        expiresAt: date(extra.expiresAt), clearReason: extra.clearReason ?? null});
  }

  async findByDispatchId(dispatchId: string): Promise<AlertDeliveryLog[]> {
    // MySQL has no NULLS LAST; `IS NULL` sorts unsent rows after sent ones.
    const [rows] = await this.pool.query<Row[]>(
      `SELECT * FROM alert_delivery_log WHERE dispatch_id = :dispatchId
       ORDER BY first_sent_at IS NULL, first_sent_at, alert_log_id`, {dispatchId});
    return rows.map(row => ({
      alertLogId: row.alert_log_id,
      alertId: row.alert_id,
      alertVersion: row.alert_version,
      dispatchId: row.dispatch_id,
      targetVehicleId: row.target_vehicle_id,
      selectionReason: row.selection_reason,
      estimatedArrivalSeconds: row.estimated_arrival_seconds,
      alertStatus: row.alert_status,
      firstSentAt: iso(row.first_sent_at),
      acknowledgedAt: iso(row.acknowledged_at),
      clearedAt: iso(row.cleared_at),
      expiresAt: iso(row.expires_at),
      clearReason: row.clear_reason,
    }));
  }

  async ping(): Promise<void> {
    await this.pool.query({sql: 'SELECT 1 FROM alert_delivery_log LIMIT 0', timeout: 1000});
  }
}
