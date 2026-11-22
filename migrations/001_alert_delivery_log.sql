BEGIN;

-- One row per alert episode. Emergency Worker keeps alertId stable per (dispatch, vehicle), so an alert that was
-- cleared and later re-issued under the same alert_id starts a new row (new alert_log_id).
-- IDs are VARCHAR rather than UUID: SUMO vehicle IDs look like EVACS_CAR_000000 and no dispatch table exists yet.
CREATE TABLE IF NOT EXISTS alert_delivery_log (
  alert_log_id UUID PRIMARY KEY,
  alert_id VARCHAR(128) NOT NULL,
  alert_version INT NOT NULL CHECK (alert_version > 0),
  dispatch_id VARCHAR(128) NOT NULL,
  target_vehicle_id VARCHAR(128) NOT NULL,
  selection_reason VARCHAR NOT NULL,
  estimated_arrival_seconds INT NULL,
  alert_status VARCHAR NOT NULL
    CHECK (alert_status IN ('PENDING', 'SENT', 'ACKNOWLEDGED', 'CLEARED', 'EXPIRED', 'FAILED')),
  first_sent_at TIMESTAMPTZ NULL,
  acknowledged_at TIMESTAMPTZ NULL,
  cleared_at TIMESTAMPTZ NULL,
  expires_at TIMESTAMPTZ NULL,
  clear_reason VARCHAR NULL
);
CREATE INDEX IF NOT EXISTS alert_delivery_log_dispatch ON alert_delivery_log(dispatch_id);
CREATE INDEX IF NOT EXISTS alert_delivery_log_alert ON alert_delivery_log(alert_id);

COMMIT;
