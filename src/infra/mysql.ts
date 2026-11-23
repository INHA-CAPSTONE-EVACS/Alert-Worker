import {createPool, type Pool} from 'mysql2/promise';

/** Timestamps are stored as UTC DATETIME(3); the driver converts Date values in UTC. */
export function createMysqlPool(databaseUrl: string): Pool {
  return createPool({uri: databaseUrl, connectionLimit: 4, connectTimeout: 2000, timezone: 'Z', namedPlaceholders: true});
}
