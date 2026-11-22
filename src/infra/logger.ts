type Level = 'debug' | 'info' | 'warn' | 'error';

/** One JSON object per line so container logs stay greppable. */
export const logger = {
  debug: (message: string, fields: Record<string, unknown> = {}) => write('debug', message, fields),
  info: (message: string, fields: Record<string, unknown> = {}) => write('info', message, fields),
  warn: (message: string, fields: Record<string, unknown> = {}) => write('warn', message, fields),
  error: (message: string, fields: Record<string, unknown> = {}) => write('error', message, fields),
};

function write(level: Level, message: string, fields: Record<string, unknown>): void {
  if (level === 'debug' && process.env.LOG_LEVEL !== 'debug') return;
  if (process.env.LOG_SILENT === 'true') return;
  const line = JSON.stringify({level, time: new Date().toISOString(), service: 'alert-worker', message, ...fields});
  (level === 'error' || level === 'warn' ? process.stderr : process.stdout).write(line + '\n');
}

export const describeError = (error: unknown) => error instanceof Error ? error.message : String(error);
