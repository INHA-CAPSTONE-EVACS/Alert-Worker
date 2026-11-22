import type Redis from 'ioredis';
import type {WorkerConfig} from '../config/config';
import {ContractError, EVENT_FIELD, parseStreamEvent} from '../domain/contracts';
import {describeError, logger} from '../infra/logger';
import type {EventRouter} from './alert-event-router';

type Entry = [id: string, fields: string[] | null];
type SubscriberConfig = Pick<WorkerConfig, 'alertStream' | 'dlqStream' | 'consumerGroup' | 'consumerName'
  | 'readCount' | 'blockMs' | 'pendingMinIdleMs' | 'maxDeliveries' | 'dlqMaxlen' | 'shutdownDrainTimeoutMs'>;

const RETRY_DELAY_MS = 1000;

/**
 * Consumer-group reader for stream:alert:event. Entries are XACKed only after successful handling,
 * so transient failures stay pending and are reclaimed with XAUTOCLAIM.
 */
export class AlertEventSubscriber {
  private blocking?: Redis;
  private loop?: Promise<void>;
  private running = false;
  private connected = false;
  private reclaimCursor = '0-0';
  lastError: unknown;

  constructor(private readonly redis: Redis, private readonly config: SubscriberConfig,
    private readonly router: EventRouter) {}

  get ready(): boolean {
    return this.running && this.connected;
  }

  async start(): Promise<void> {
    if (this.running) return;
    const {alertStream, consumerGroup, blockMs} = this.config;
    try {
      // '$': a fresh deployment must not replay alerts that were requested before it existed.
      await this.redis.xgroup('CREATE', alertStream, consumerGroup, '$', 'MKSTREAM');
    } catch (error) {
      if (!describeError(error).includes('BUSYGROUP')) throw error;
    }
    this.blocking = this.redis.duplicate({commandTimeout: blockMs + 2000});
    this.blocking.on('error', (error: Error) => {
      this.lastError = error;
      this.connected = false;
    });
    this.running = true;
    this.connected = true;
    this.loop = this.run();
    logger.info('alert stream subscription started', {stream: alertStream, group: consumerGroup});
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.connected = false;
    // Disconnecting interrupts the blocking XREADGROUP without touching the command connection.
    this.blocking?.disconnect();
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([this.loop, new Promise<void>(resolve => {
      timer = setTimeout(resolve, this.config.shutdownDrainTimeoutMs);
    })]);
    if (timer) clearTimeout(timer);
    this.loop = undefined;
    logger.info('alert stream subscription stopped');
  }

  async onMessage(id: string, fields: string[]): Promise<void> {
    let event;
    try {
      event = parseStreamEvent(fields);
    } catch (error) {
      if (!(error instanceof ContractError)) throw error;
      // Redelivery cannot fix a malformed entry; park it for inspection.
      await this.deadLetter(id, error.message, fields);
      return;
    }
    try {
      await this.router.route(event);
      await this.acknowledge(id);
    } catch (error) {
      logger.error('alert event handling failed; left pending', {entryId: id, eventType: event.eventType,
        error: describeError(error)});
    }
  }

  async acknowledge(entryId: string): Promise<void> {
    await this.redis.xack(this.config.alertStream, this.config.consumerGroup, entryId);
  }

  async reclaimPending(idleMs: number): Promise<void> {
    const {alertStream, consumerGroup, consumerName, readCount, maxDeliveries} = this.config;
    const [next, entries] = await this.redis.xautoclaim(alertStream, consumerGroup, consumerName, idleMs,
      this.reclaimCursor, 'COUNT', readCount) as [string, Entry[]];
    this.reclaimCursor = next;
    for (const [id, fields] of entries) {
      if (!this.running) return;
      if (!fields) continue;
      const pending = await this.redis.xpending(alertStream, consumerGroup, 'IDLE', 0, id, id, 1) as
        [string, string, number, number][];
      const deliveries = pending[0]?.[3] ?? 0;
      if (deliveries > maxDeliveries) {
        await this.deadLetter(id, `Exceeded ${maxDeliveries} deliveries`, fields);
        continue;
      }
      await this.onMessage(id, fields);
    }
  }

  private async run(): Promise<void> {
    const {alertStream, consumerGroup, consumerName, readCount, blockMs, pendingMinIdleMs} = this.config;
    while (this.running) {
      try {
        await this.reclaimPending(pendingMinIdleMs);
        if (!this.running) break;
        const response = await this.blocking!.xreadgroup('GROUP', consumerGroup, consumerName, 'COUNT', readCount,
          'BLOCK', blockMs, 'STREAMS', alertStream, '>') as [string, Entry[]][] | null;
        this.connected = true;
        for (const [, entries] of response ?? []) {
          for (const [id, fields] of entries) {
            if (fields) await this.onMessage(id, fields);
          }
        }
      } catch (error) {
        if (!this.running) break;
        this.lastError = error;
        this.connected = false;
        logger.error('alert stream read failed', {error: describeError(error)});
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
      }
    }
  }

  private async deadLetter(id: string, reason: string, fields: string[]): Promise<void> {
    const eventIndex = fields.indexOf(EVENT_FIELD);
    const raw = eventIndex >= 0 && eventIndex % 2 === 0 ? fields[eventIndex + 1] ?? '' : JSON.stringify(fields);
    await this.redis.xadd(this.config.dlqStream, 'MAXLEN', '~', this.config.dlqMaxlen, '*',
      'sourceId', id, 'reason', reason, 'event', raw);
    await this.acknowledge(id);
    logger.warn('alert event dead-lettered', {entryId: id, reason});
  }
}
