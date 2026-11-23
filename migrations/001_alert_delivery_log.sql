-- Alert delivery history (MySQL 8.4), in the Control API Server's `evacs` database.
-- Types follow the API Server's conventions: UUID -> CHAR(36), timestamps -> DATETIME(3) in UTC, enum -> VARCHAR + CHECK.
--
-- One row per alert episode. Emergency Worker keeps alertId stable per (dispatch, vehicle), so an alert that was
-- cleared and later re-issued under the same alert_id starts a new row (new alert_log_id).
-- IDs are VARCHAR without FKs: SUMO vehicle IDs look like EVACS_CAR_000000, and under the API contract the dispatch
-- ID is the emergency ID of the API Server's `emergency` table, which this worker does not own.
CREATE TABLE IF NOT EXISTS alert_delivery_log (
  alert_log_id CHAR(36) NOT NULL,
  alert_id VARCHAR(128) NOT NULL,
  alert_version INT NOT NULL,
  dispatch_id VARCHAR(128) NOT NULL,
  target_vehicle_id VARCHAR(128) NOT NULL,
  selection_reason VARCHAR(64) NOT NULL,
  estimated_arrival_seconds INT NULL,
  alert_status VARCHAR(16) NOT NULL,
  first_sent_at DATETIME(3) NULL,
  acknowledged_at DATETIME(3) NULL,
  cleared_at DATETIME(3) NULL,
  expires_at DATETIME(3) NULL,
  clear_reason VARCHAR(64) NULL,
  PRIMARY KEY (alert_log_id),
  KEY alert_delivery_log_dispatch (dispatch_id),
  KEY alert_delivery_log_alert (alert_id),
  CONSTRAINT alert_delivery_log_version_check CHECK (alert_version > 0),
  CONSTRAINT alert_delivery_log_status_check
    CHECK (alert_status IN ('PENDING', 'SENT', 'ACKNOWLEDGED', 'CLEARED', 'EXPIRED', 'FAILED'))
) ENGINE = InnoDB;
