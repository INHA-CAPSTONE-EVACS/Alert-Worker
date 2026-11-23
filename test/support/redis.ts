import {randomUUID} from 'node:crypto';
import Redis from 'ioredis';

export const TEST_REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6401';
export const TEST_DATABASE_URL = process.env.DATABASE_URL ?? 'mysql://evacs:evacs@127.0.0.1:33306/evacs';

export const uniqueId = (label: string) => `${label}-${randomUUID().slice(0, 8)}`;

/**
 * Redis client whose keys live under a random prefix so tests never touch the running sandbox's data.
 * Channel names are not prefixed by ioredis, so channel-based tests must also use unique gateway ids.
 */
export function uniqueRedis(): {redis: Redis; prefix: string; close: () => Promise<void>} {
  const prefix = `${uniqueId('test')}:`;
  const redis = new Redis(TEST_REDIS_URL, {keyPrefix: prefix, maxRetriesPerRequest: 1});
  return {
    redis,
    prefix,
    close: async () => {
      const raw = new Redis(TEST_REDIS_URL);
      const keys = await raw.keys(`${prefix}*`);
      if (keys.length) await raw.del(...keys);
      raw.disconnect();
      redis.disconnect();
    },
  };
}
