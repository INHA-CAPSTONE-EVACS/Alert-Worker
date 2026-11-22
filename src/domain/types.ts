import type {
  ApproachDirection, ClearReason, Coordinates, RecommendedAction,
} from './contracts';

export const ALERT_STATUSES = ['PENDING', 'SENT', 'ACKNOWLEDGED', 'CLEARED', 'EXPIRED', 'FAILED'] as const;
export type AlertStatus = typeof ALERT_STATUSES[number];
export const TERMINAL_STATUSES: readonly AlertStatus[] = ['CLEARED', 'EXPIRED', 'FAILED'];
export const isTerminal = (status: AlertStatus) => TERMINAL_STATUSES.includes(status);

/** Current alert state stored at alert:{alertId}:state. */
export interface AlertState {
  alertId: string;
  /** alert_delivery_log row of the current episode. */
  alertLogId: string;
  dispatchId: string;
  emergencyId: string;
  targetVehicleId: string;
  status: AlertStatus;
  alertVersion: number;
  recommendedAction: RecommendedAction;
  approachDirection: ApproachDirection;
  lastSentAt: string | null;
  acknowledgedAt: string | null;
  clearedAt: string | null;
  expiresAt: string;
}

export interface YieldAlertIssuedMessage {
  type: 'YIELD_ALERT_ISSUED';
  eventType: 'YIELD_ALERT_ISSUED';
  alertId: string;
  emergencyId: string;
  dispatchId: string;
  targetVehicleId: string;
  approachDirection: ApproachDirection;
  estimatedArrivalSeconds: number;
  recommendedAction: RecommendedAction;
  message: string;
  alertVersion: number;
  expiresAt: string;
}

export interface YieldAlertClearedMessage {
  type: 'YIELD_ALERT_CLEARED';
  eventType: 'YIELD_ALERT_CLEARED';
  alertId: string;
  targetVehicleId: string;
  reason: ClearReason;
}

export interface EmergencyRouteUpdatedMessage {
  type: 'EMERGENCY_ROUTE_UPDATED';
  eventType: 'EMERGENCY_ROUTE_UPDATED';
  dispatchId: string;
  targetVehicleId: string;
  routeVersion: number;
  destination: Coordinates;
  waypoints: Coordinates[];
  congestedSegments: string[];
  estimatedTravelSeconds: number;
}

export type ClientMessage = YieldAlertIssuedMessage | YieldAlertClearedMessage | EmergencyRouteUpdatedMessage;

/** Frame published on channel:gateway:{gatewayId}. */
export interface GatewayFrame {
  targetVehicleId: string;
  sessionId: string;
  message: ClientMessage;
}

/** Gateway-owned connection record at ws:vehicle:{vehicleId}. */
export interface SessionInfo {
  vehicleId: string;
  gatewayId: string;
  sessionId: string;
  connectedAt: string;
}

export type DeliveryOutcome = 'SENT' | 'NO_SESSION' | 'NO_SUBSCRIBER' | 'SUPPRESSED' | 'STALE' | 'DUPLICATE'
  | 'EXPIRED' | 'NO_ACTIVE_ALERT';
export type SendOutcome = Extract<DeliveryOutcome, 'SENT' | 'NO_SESSION' | 'NO_SUBSCRIBER'>;

export interface DeliveryResult {
  outcome: DeliveryOutcome;
  alertId?: string;
}

export interface AlertDeliveryLog {
  alertLogId: string;
  alertId: string;
  alertVersion: number;
  dispatchId: string;
  targetVehicleId: string;
  selectionReason: string;
  estimatedArrivalSeconds: number | null;
  alertStatus: AlertStatus;
  firstSentAt: string | null;
  acknowledgedAt: string | null;
  clearedAt: string | null;
  expiresAt: string | null;
  clearReason: string | null;
}

export interface LogStatusExtra {
  alertVersion?: number;
  estimatedArrivalSeconds?: number;
  expiresAt?: string;
  clearReason?: string;
}

export interface AlertStateStore {
  findById(alertId: string): Promise<AlertState | null>;
  findActive(dispatchId: string, vehicleId: string): Promise<AlertState | null>;
  /** Writes unless a higher alertVersion is stored; false means a newer state already exists. */
  save(state: AlertState): Promise<boolean>;
  /** Applies patch only if the status is in `from` (and the version matches); null otherwise. */
  transition(alertId: string, from: readonly AlertStatus[], to: AlertStatus, patch: Partial<AlertState>,
    expectedVersion?: number): Promise<AlertState | null>;
  dueForExpiry(nowMs: number, limit: number): Promise<string[]>;
}

export interface Deduplicator {
  shouldSend(dispatchId: string, vehicleId: string, fingerprint: string): Promise<boolean>;
  remember(dispatchId: string, vehicleId: string, fingerprint: string): Promise<void>;
  forget(dispatchId: string, vehicleId: string): Promise<void>;
}

export interface SessionLocator {
  findGateway(vehicleId: string): Promise<SessionInfo | null>;
}

export interface DeliveryPublisher {
  /** Returns the number of Gateway subscribers that received the frame. */
  publish(gatewayId: string, frame: GatewayFrame): Promise<number>;
}

export interface AlertDeliveryLogStore {
  insert(log: AlertDeliveryLog): Promise<void>;
  updateStatus(alertLogId: string, status: AlertStatus, at: string, extra?: LogStatusExtra): Promise<void>;
  findByDispatchId(dispatchId: string): Promise<AlertDeliveryLog[]>;
}

export interface RouteVersionStore {
  isLatest(dispatchId: string, routeVersion: number): Promise<boolean>;
  markDelivered(dispatchId: string, routeVersion: number): Promise<void>;
}
