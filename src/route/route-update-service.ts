import type {AlertMessageFactory} from '../alert/alert-message-factory';
import type {AlertDeliverer} from '../delivery/alert-deliverer';
import type {RouteUpdateRequestedPayload} from '../domain/contracts';
import type {DeliveryResult, RouteVersionStore} from '../domain/types';

export interface RouteUpdateServiceDeps {
  routes: RouteVersionStore;
  deliverer: AlertDeliverer;
  factory: AlertMessageFactory;
}

/** Delivers the route computed by Emergency Worker to the emergency vehicle's terminal. */
export class RouteUpdateService {
  constructor(private readonly deps: RouteUpdateServiceDeps) {}

  async handleRouteUpdate(p: RouteUpdateRequestedPayload): Promise<DeliveryResult> {
    const {routes, deliverer, factory} = this.deps;
    if (!await this.isLatestVersion(p.dispatchId, p.routeVersion)) return {outcome: 'STALE'};
    const outcome = await deliverer.deliver(p.targetVehicleId, factory.createRouteUpdated(p));
    // Only a delivered version counts, so a terminal that was offline can still receive it later.
    if (outcome === 'SENT') await routes.markDelivered(p.dispatchId, p.routeVersion);
    return {outcome};
  }

  isLatestVersion(dispatchId: string, routeVersion: number): Promise<boolean> {
    return this.deps.routes.isLatest(dispatchId, routeVersion);
  }
}
