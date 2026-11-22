import {createServer, type Server} from 'node:http';
import type {AddressInfo} from 'node:net';

export interface Readiness {
  ready: boolean;
  checks: Record<string, boolean>;
}

/** /livez reports the process; /readyz reports dependencies and the stream subscription. */
export class HealthServer {
  private readonly server: Server;

  constructor(private readonly readiness: () => Promise<Readiness>) {
    this.server = createServer((request, response) => {
      const reply = (status: number, body: unknown) => {
        response.writeHead(status, {'content-type': 'application/json'});
        response.end(JSON.stringify(body));
      };
      if (request.url === '/livez') return reply(200, {live: true});
      if (request.url === '/readyz') {
        this.readiness()
          .then(result => reply(result.ready ? 200 : 503, result))
          .catch(() => reply(503, {ready: false, checks: {}}));
        return;
      }
      reply(404, {error: 'not found'});
    });
  }

  listen(port: number): Promise<number> {
    return new Promise((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(port, () => resolve((this.server.address() as AddressInfo).port));
    });
  }

  close(): Promise<void> {
    this.server.closeAllConnections();
    return new Promise(resolve => this.server.close(() => resolve()));
  }
}
