import {ConfigError, loadConfig} from '../../../src/config/config';

const DATABASE_URL = 'mysql://evacs:evacs@127.0.0.1:33306/evacs';

test('applies defaults', () => {
  const config = loadConfig({DATABASE_URL, HOSTNAME: 'host-a'});
  expect(config).toMatchObject({
    redisUrl: 'redis://localhost:6401', databaseUrl: DATABASE_URL, healthPort: 18083,
    alertStream: 'stream:alert:event', dlqStream: 'stream:alert-worker:dlq', consumerGroup: 'alert-worker',
    gatewayAlertStream: 'evacs:stream:yield-alert', gatewayAlertStreamMaxlen: 10000,
    consumerName: 'host-a', readCount: 10, blockMs: 1000, pendingMinIdleMs: 30000, maxDeliveries: 5,
    expirySweepMs: 1000, dedupTtlMs: 10000,
  });
});

test('reports every invalid setting together', () => {
  try {
    loadConfig({BLOCK_MS: 'abc'});
    throw new Error('expected ConfigError');
  } catch (error) {
    expect(error).toBeInstanceOf(ConfigError);
    expect((error as ConfigError).issues.map(issue => issue.variable).sort()).toEqual(['BLOCK_MS', 'DATABASE_URL']);
  }
});

test('blank values fall back to defaults', () => {
  expect(loadConfig({DATABASE_URL, ALERT_STREAM: '  '}).alertStream).toBe('stream:alert:event');
});
