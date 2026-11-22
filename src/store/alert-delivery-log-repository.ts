import type {Pool} from 'pg';
import type {
  AlertDeliveryLog, AlertDeliveryLogStore, AlertStatus, LogStatusExtra,
} from '../domain/types';

interface Row {
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

/** alert_delivery_log persistence. All writes are safe to repeat on stream redelivery. */
export class PgAlertDeliveryLogRepository implements AlertDeliveryLogStore {
  constructor(private readonly pool: Pool) {}

  async insert(log: AlertDeliveryLog): Promise<void> {
    await this.pool.query(
      `INSERT INTO alert_delivery_log (alert_log_id, alert_id, alert_version, dispatch_id, target_vehicle_id,
         selection_reason, estimated_arrival_seconds, alert_status, first_sent_at, acknowledged_at, cleared_at,
         expires_at, clear_reason)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       ON CONFLICT (alert_log_id) DO NOTHING`,
      [log.alertLogId, log.alertId, log.alertVersion, log.dispatchId, log.targetVehicleId, log.selectionReason,
        log.estimatedArrivalSeconds, log.alertStatus, log.firstSentAt, log.acknowledgedAt, log.clearedAt,
        log.expiresAt, log.clearReason]);
  }

  async updateStatus(alertLogId: string, status: AlertStatus, at: string, extra: LogStatusExtra = {}): Promise<void> {
    const acknowledged = status === 'ACKNOWLEDGED';
    const ended = status === 'CLEARED' || status === 'EXPIRED' || status === 'FAILED';
    await this.pool.query(
      `UPDATE alert_delivery_log SET
         alert_status = $2::varchar,
         acknowledged_at = CASE WHEN $3::boolean THEN $5::timestamptz ELSE acknowledged_at END,
         cleared_at = CASE WHEN $4::boolean THEN $5::timestamptz ELSE cleared_at END,
         first_sent_at = COALESCE(first_sent_at, CASE WHEN $2::varchar = 'SENT' THEN $5::timestamptz END),
         alert_version = COALESCE($6::int, alert_version),
         estimated_arrival_seconds = COALESCE($7::int, estimated_arrival_seconds),
         expires_at = COALESCE($8::timestamptz, expires_at),
         clear_reason = COALESCE($9::varchar, clear_reason)
       WHERE alert_log_id = $1
         -- An ended episode is never revived by a late or redelivered write.
         AND NOT (alert_status IN ('CLEARED', 'EXPIRED', 'FAILED')
                  AND $2::varchar NOT IN ('CLEARED', 'EXPIRED', 'FAILED'))`,
      [alertLogId, status, acknowledged, ended, at, extra.alertVersion ?? null,
        extra.estimatedArrivalSeconds ?? null, extra.expiresAt ?? null, extra.clearReason ?? null]);
  }

  async findByDispatchId(dispatchId: string): Promise<AlertDeliveryLog[]> {
    const result = await this.pool.query<Row>(
      'SELECT * FROM alert_delivery_log WHERE dispatch_id = $1 ORDER BY first_sent_at NULLS LAST, alert_log_id',
      [dispatchId]);
    return result.rows.map(row => ({
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
    await this.pool.query('SELECT 1 FROM alert_delivery_log LIMIT 0');
  }
}
