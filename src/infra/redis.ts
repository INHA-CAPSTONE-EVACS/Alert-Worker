import Redis, {type RedisOptions} from 'ioredis';

/** Commands, the blocking stream reader and publishing use separate connections. */
export function createRedis(redisUrl: string, options: RedisOptions = {}): Redis {
  return new Redis(redisUrl, {maxRetriesPerRequest: 1, ...options});
}
