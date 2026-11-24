import type {
  ClientMessage, DeliveryPublisher, GatewayOutboundEvent, SendOutcome, SessionLocator,
} from '../domain/types';

/** Delivers alert messages to the Gateway-consumed Redis Stream. */
export class AlertDeliverer {
  private readonly publisher: DeliveryPublisher;
  private readonly clock: () => Date;

  constructor(publisher: DeliveryPublisher, clock?: () => Date);
  constructor(sessions: SessionLocator | unknown, publisher: DeliveryPublisher, clock?: () => Date);
  constructor(
    arg1: DeliveryPublisher | SessionLocator | unknown,
    arg2?: DeliveryPublisher | (() => Date),
    arg3?: () => Date,
  ) {
    if (arg2 && typeof (arg2 as DeliveryPublisher).publish === 'function') {
      this.publisher = arg2 as DeliveryPublisher;
      this.clock = arg3 ?? (() => new Date());
    } else {
      this.publisher = arg1 as DeliveryPublisher;
      this.clock = (typeof arg2 === 'function' ? arg2 : undefined) ?? (() => new Date());
    }
  }

  async deliver(vehicleId: string, message: ClientMessage): Promise<SendOutcome> {
    const occurredAt = this.clock().toISOString();
    const envelope = this.buildEnvelope(vehicleId, message, occurredAt);
    const eventKey = envelope.eventId;
    const res = await this.publisher.publish(eventKey, envelope);
    if (res === 0) return 'FAILED';
    return 'SENT';
  }

  private buildEnvelope(vehicleId: string, message: ClientMessage, occurredAt: string): GatewayOutboundEvent {
    switch (message.type) {
      case 'YIELD_ALERT_ISSUED': {
        const eventId = `${message.dispatchId}:${message.alertId}:${message.alertVersion}:YIELD_ALERT_ISSUED:${vehicleId}`;
        return {
          eventId,
          eventType: 'YIELD_ALERT_ISSUED',
          schemaVersion: 1,
          occurredAt,
          producer: 'alert-worker',
          payload: {
            alertId: message.alertId,
            alertVersion: message.alertVersion,
            emergencyId: message.emergencyId,
            dispatchId: message.dispatchId,
            vehicleId,
            targetVehicleId: vehicleId,
            approachDirection: message.approachDirection,
            estimatedArrivalSeconds: message.estimatedArrivalSeconds,
            recommendedAction: message.recommendedAction,
            message: message.message,
            expiresAt: message.expiresAt,
            issuedAt: occurredAt,
            details: {
              emergencyId: message.emergencyId,
              dispatchId: message.dispatchId,
              approachDirection: message.approachDirection,
              estimatedArrivalSeconds: message.estimatedArrivalSeconds,
              recommendedAction: message.recommendedAction,
              message: message.message,
            },
          },
        };
      }
      case 'YIELD_ALERT_CLEARED': {
        const dispatchPrefix = message.dispatchId || 'dispatch';
        const eventId = `${dispatchPrefix}:${message.alertId}:clear:YIELD_ALERT_CLEARED:${vehicleId}`;
        return {
          eventId,
          eventType: 'YIELD_ALERT_CLEARED',
          schemaVersion: 1,
          occurredAt,
          producer: 'alert-worker',
          payload: {
            alertId: message.alertId,
            dispatchId: message.dispatchId ?? '',
            vehicleId,
            targetVehicleId: vehicleId,
            reason: message.reason,
          },
        };
      }
      case 'EMERGENCY_ROUTE_UPDATED': {
        const eventId = `${message.dispatchId}:route:${message.routeVersion}:EMERGENCY_ROUTE_UPDATED:${vehicleId}`;
        return {
          eventId,
          eventType: 'EMERGENCY_ROUTE_UPDATED',
          schemaVersion: 1,
          occurredAt,
          producer: 'alert-worker',
          payload: {
            dispatchId: message.dispatchId,
            vehicleId,
            targetVehicleId: vehicleId,
            routeVersion: message.routeVersion,
            destination: message.destination,
            waypoints: message.waypoints,
            congestedSegments: message.congestedSegments,
            estimatedTravelSeconds: message.estimatedTravelSeconds,
            details: {
              destination: message.destination,
              waypoints: message.waypoints,
              congestedSegments: message.congestedSegments,
              estimatedTravelSeconds: message.estimatedTravelSeconds,
            },
          },
        };
      }
    }
  }
}
