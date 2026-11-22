import type {ClientMessage, DeliveryPublisher, SendOutcome, SessionLocator} from '../domain/types';

/** Finds the vehicle's Gateway and publishes the message on that Gateway's channel only. */
export class AlertDeliverer {
  constructor(private readonly sessions: SessionLocator, private readonly publisher: DeliveryPublisher) {}

  async deliver(vehicleId: string, message: ClientMessage): Promise<SendOutcome> {
    const session = await this.sessions.findGateway(vehicleId);
    if (!session) return 'NO_SESSION';
    const receivers = await this.publisher.publish(session.gatewayId,
      {targetVehicleId: vehicleId, sessionId: session.sessionId, message});
    // Zero receivers means no Gateway instance is listening; reporting SENT would mislead the dashboard.
    return receivers > 0 ? 'SENT' : 'NO_SUBSCRIBER';
  }
}
