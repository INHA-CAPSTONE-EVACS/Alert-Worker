import {hostname} from 'node:os';
import {
  ALERT_STREAM, ALERTED_VEHICLES_KEY_PATTERN, CONSUMER_GROUP, DEDUP_TTL_MS, DEFAULT_GATEWAY_ALERT_STREAM, DLQ_STREAM,
} from '../domain/contracts';

export interface WorkerConfig {
  redisUrl: string;
  databaseUrl: string;
  healthPort: number;
  alertStream: string;
  dlqStream: string;
  gatewayAlertStream: string;
  gatewayAlertStreamMaxlen: number;
  consumerGroup: string;
  consumerName: string;
  readCount: number;
  blockMs: number;
  pendingMinIdleMs: number;
  maxDeliveries: number;
  expirySweepMs: number;
  dedupTtlMs: number;
  dlqMaxlen: number;
  shutdownDrainTimeoutMs: number;
  /** Control API Server alerted-vehicle Set key; must contain %s for the emergency ID. */
  alertedKeyPattern: string;
}

export interface ConfigIssue {variable: string; message: string}

export class ConfigError extends Error {
  constructor(readonly issues: readonly ConfigIssue[]) {
    super('Invalid configuration: ' + issues.map(issue => `${issue.variable} ${issue.message}`).join('; '));
    this.name = 'ConfigError';
  }
}

type Env = Readonly<Record<string, string | undefined>>;

/** Blank values use defaults; every invalid setting is reported in one ConfigError. */
export function loadConfig(env: Env = process.env): WorkerConfig {
  const issues: ConfigIssue[] = [];
  const read = (name: string) => env[name]?.trim() || undefined;
  const integer = (name: string, fallback: number, min = 1, max = Number.MAX_SAFE_INTEGER) => {
    const raw = read(name);
    if (raw === undefined) return fallback;
    const value = Number(raw);
    if (!/^-?\d+$/.test(raw) || !Number.isSafeInteger(value) || value < min || value > max) {
      issues.push({variable: name, message: `must be an integer between ${min} and ${max}`});
      return fallback;
    }
    return value;
  };
  const databaseUrl = read('DATABASE_URL');
  if (!databaseUrl) issues.push({variable: 'DATABASE_URL', message: 'is required'});
  const config: WorkerConfig = {
    redisUrl: read('REDIS_URL') ?? 'redis://localhost:6401',
    databaseUrl: databaseUrl ?? '',
    healthPort: integer('HEALTH_PORT', 18083, 0, 65535),
    alertStream: read('ALERT_STREAM') ?? ALERT_STREAM,
    dlqStream: read('DLQ_STREAM') ?? DLQ_STREAM,
    gatewayAlertStream: read('GATEWAY_ALERT_STREAM') ?? read('EVACS_ALERT_STREAM') ?? DEFAULT_GATEWAY_ALERT_STREAM,
    gatewayAlertStreamMaxlen: integer('GATEWAY_ALERT_STREAM_MAXLEN', 10_000),
    consumerGroup: read('CONSUMER_GROUP') ?? CONSUMER_GROUP,
    consumerName: read('CONSUMER_NAME') ?? read('HOSTNAME') ?? hostname(),
    readCount: integer('READ_COUNT', 10),
    blockMs: integer('BLOCK_MS', 1000),
    pendingMinIdleMs: integer('PENDING_MIN_IDLE_MS', 30_000),
    maxDeliveries: integer('MAX_DELIVERIES', 5),
    expirySweepMs: integer('EXPIRY_SWEEP_MS', 1000),
    dedupTtlMs: integer('DEDUP_TTL_MS', DEDUP_TTL_MS),
    dlqMaxlen: integer('DLQ_MAXLEN', 10_000),
    shutdownDrainTimeoutMs: integer('SHUTDOWN_DRAIN_TIMEOUT_MS', 5000),
    alertedKeyPattern: read('EVACS_ALERT_KEY_PATTERN') ?? ALERTED_VEHICLES_KEY_PATTERN,
  };
  if ((config.alertedKeyPattern.match(/%s/g) ?? []).length !== 1) {
    issues.push({variable: 'EVACS_ALERT_KEY_PATTERN', message: 'must contain exactly one %s'});
  }
  if (issues.length) throw new ConfigError(issues);
  return config;
}
