import type {AcknowledgementService} from '../alert/acknowledgement-service';
import type {YieldAlertService} from '../alert/yield-alert-service';
import {
  ALERT_EVENT_TYPES, ROUTE_UPDATE_REQUESTED, YIELD_ALERT_ACKNOWLEDGED, YIELD_ALERT_CLEAR_REQUESTED,
  YIELD_ALERT_REQUESTED, type AlertStreamEvent,
} from '../domain/contracts';
import {logger} from '../infra/logger';
import type {EventMeta} from '../domain/types';
import type {RouteUpdateService} from '../route/route-update-service';

const meta = (event: AlertStreamEvent): EventMeta => ({eventId: event.eventId, occurredAt: event.occurredAt});

export interface AlertServices {
  yieldAlerts: Pick<YieldAlertService, 'handleRequested' | 'handleClearRequested'>;
  routes: Pick<RouteUpdateService, 'handleRouteUpdate'>;
  acks: Pick<AcknowledgementService, 'handleAck'>;
}

export interface EventRouter {
  route(event: AlertStreamEvent): Promise<void>;
}

/** Dispatches each stream event to the service owning its event type. */
export class AlertEventRouter implements EventRouter {
  constructor(private readonly services: AlertServices) {}

  async route(event: AlertStreamEvent): Promise<void> {
    switch (event.eventType) {
      case YIELD_ALERT_REQUESTED: {
        const result = await this.services.yieldAlerts.handleRequested(event.payload, meta(event));
        logger.info('yield alert requested', {eventId: event.eventId, ...result,
          vehicleId: event.payload.targetVehicleId, decisionVersion: event.payload.decisionVersion});
        return;
      }
      case YIELD_ALERT_CLEAR_REQUESTED: {
        const result = await this.services.yieldAlerts.handleClearRequested(event.payload, meta(event));
        logger.info('yield alert clear requested', {eventId: event.eventId, ...result, reason: event.payload.reason});
        return;
      }
      case ROUTE_UPDATE_REQUESTED: {
        const result = await this.services.routes.handleRouteUpdate(event.payload);
        logger.info('route update requested', {eventId: event.eventId, ...result,
          dispatchId: event.payload.dispatchId, routeVersion: event.payload.routeVersion});
        return;
      }
      case YIELD_ALERT_ACKNOWLEDGED:
        await this.services.acks.handleAck(event.payload);
        logger.info('yield alert acknowledged', {eventId: event.eventId, alertId: event.payload.alertId,
          alertVersion: event.payload.alertVersion});
        return;
      default:
        throw new Error(`Unsupported event type: ${String((event as {eventType: unknown}).eventType)}`);
    }
  }

  isSupported(eventType: string): boolean {
    return (ALERT_EVENT_TYPES as readonly string[]).includes(eventType);
  }
}
