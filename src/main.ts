import {ConfigError, loadConfig} from './config/config';
import {describeError, logger} from './infra/logger';
import {Worker} from './worker';

async function main(): Promise<void> {
  let worker: Worker;
  try {
    worker = Worker.create(loadConfig());
  } catch (error) {
    if (error instanceof ConfigError) {
      logger.error('invalid configuration', {issues: error.issues});
      process.exit(1);
    }
    throw error;
  }
  let stopping = false;
  const shutdown = (signal: string) => {
    if (stopping) return;
    stopping = true;
    logger.info('shutdown requested', {signal});
    worker.stop().then(() => process.exit(0), error => {
      logger.error('shutdown failed', {error: describeError(error)});
      process.exit(1);
    });
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  await worker.start();
}

main().catch(error => {
  logger.error('alert worker failed to start', {error: describeError(error)});
  process.exit(1);
});
