import type Redis from 'ioredis';
import type {Pool} from 'pg';
import {AcknowledgementService} from './alert/acknowledgement-service';
import {AlertDeduplicator} from './alert/alert-deduplicator';
import {AlertExpirationScheduler} from './alert/alert-expiration-scheduler';
import {AlertMessageFactory} from './alert/alert-message-factory';
import {YieldAlertService} from './alert/yield-alert-service';
import type {WorkerConfig} from './config/config';
import {AlertEventRouter} from './consumer/alert-event-router';
import {AlertEventSubscriber} from './consumer/alert-event-subscriber';
import {AlertDeliverer} from './delivery/alert-deliverer';
import {RedisPubSubDeliveryPublisher} from './delivery/redis-pubsub-delivery-publisher';
import {RedisSessionLocator} from './delivery/session-locator';
import {HealthServer, type Readiness} from './infra/health-server';
import {logger} from './infra/logger';
import {createPool} from './infra/postgres';
import {createRedis} from './infra/redis';
import {RouteUpdateService} from './route/route-update-service';
import {PgAlertDeliveryLogRepository} from './store/alert-delivery-log-repository';
import {AlertStateRepository} from './store/alert-state-repository';
import {RedisRouteVersionStore} from './store/route-version-store';

const CHECK_TIMEOUT_MS = 1000;

const passes = async (check: () => Promise<unknown>): Promise<boolean> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([check(), new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('timeout')), CHECK_TIMEOUT_MS);
    })]);
    return true;
  } catch {
    return false;
  } finally {
    if (timer) clearTimeout(timer);
  }
};

/** Composition root: wires adapters, services, the stream subscriber, the expiry sweep and health checks. */
export class Worker {
  private port = 0;

  private constructor(
    private readonly config: WorkerConfig,
    readonly redis: Redis,
    private readonly pool: Pool,
    private readonly logs: PgAlertDeliveryLogRepository,
    private readonly subscriber: AlertEventSubscriber,
    private readonly scheduler: AlertExpirationScheduler,
    private readonly health: HealthServer,
  ) {}

  static create(config: WorkerConfig): Worker {
    const redis = createRedis(config.redisUrl);
    const pool = createPool(config.databaseUrl);
    const clock = () => new Date();
    const states = new AlertStateRepository(redis);
    const dedup = new AlertDeduplicator(redis, config.dedupTtlMs);
    const logs = new PgAlertDeliveryLogRepository(pool);
    const factory = new AlertMessageFactory();
    const deliverer = new AlertDeliverer(new RedisSessionLocator(redis), new RedisPubSubDeliveryPublisher(redis));
    const router = new AlertEventRouter({
      yieldAlerts: new YieldAlertService({states, dedup, deliverer, logs, factory, clock}),
      routes: new RouteUpdateService({routes: new RedisRouteVersionStore(redis), deliverer, factory}),
      acks: new AcknowledgementService({states, logs}),
    });
    const subscriber = new AlertEventSubscriber(redis, config, router);
    const scheduler = new AlertExpirationScheduler({states, dedup, deliverer, logs, factory, clock});
    let worker: Worker | undefined;
    const health = new HealthServer(() => worker!.readiness());
    worker = new Worker(config, redis, pool, logs, subscriber, scheduler, health);
    return worker;
  }

  get healthPort(): number {
    return this.port;
  }

  async start(): Promise<void> {
    await this.logs.ping();
    await this.subscriber.start();
    this.scheduler.start(this.config.expirySweepMs);
    this.port = await this.health.listen(this.config.healthPort);
    logger.info('alert worker started', {healthPort: this.port, stream: this.config.alertStream,
      group: this.config.consumerGroup, consumer: this.config.consumerName});
  }

  async stop(): Promise<void> {
    await this.health.close();
    this.scheduler.stop();
    await this.subscriber.stop();
    if (this.redis.status !== 'end') {
      const ended = new Promise(resolve => this.redis.once('end', resolve));
      this.redis.disconnect();
      await ended;
    }
    await this.pool.end().catch(() => undefined);
    logger.info('alert worker stopped');
  }

  async readiness(): Promise<Readiness> {
    const checks = {
      redis: this.redis.status === 'ready' && await passes(() => this.redis.ping()),
      postgres: await passes(() => this.logs.ping()),
      subscriber: this.subscriber.ready,
    };
    return {ready: Object.values(checks).every(Boolean), checks};
  }
}
