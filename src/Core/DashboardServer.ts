import type { ClusterManager } from './ClusterManager.ts';

export interface DashboardOptions {
    port?: number;
    password?: string;
}

export class DashboardServer {
    public manager?: ClusterManager;
    public port: number;
    private password?: string;

    constructor(options: DashboardOptions = {}) {
        this.port = options.port || 3001;
        this.password = options.password;
    }

    public build(manager: ClusterManager) {
        this.manager = manager;
        this.start();
    }

    public start() {
        Bun.serve({
            port: this.port,
            fetch: async (req) => {
                const url = new URL(req.url);

                // Simple Auth
                if (this.password && req.headers.get('Authorization') !== this.password) {
                    return new Response('Unauthorized', { status: 401 });
                }

                // Stats Endpoint
                if (url.pathname === '/stats' && req.method === 'GET') {
                    const stats = await this.getStats();
                    return Response.json(stats);
                }

                // Restart Endpoint
                if (url.pathname === '/restart' && req.method === 'POST') {
                    const body = (await req.json()) as any;
                    const clusterId = body.clusterId;

                    if (clusterId !== undefined) {
                        const cluster = this.manager?.clusters.get(clusterId);
                        if (!cluster) return new Response('Cluster not found', { status: 404 });
                        cluster.respawn();
                        return new Response(`Restarting Cluster ${clusterId}`);
                    } else {
                        this.manager?.respawnAll();
                        return new Response('Restarting all clusters');
                    }
                }

                // Maintenance Endpoint
                if (url.pathname === '/maintenance' && req.method === 'POST') {
                    const body = (await req.json()) as any;
                    const enable = body.enable;
                    const reason = body.reason || 'Maintenance via API';

                    if (enable) {
                        this.manager?.triggerMaintenance(reason);
                    } else {
                        this.manager?.triggerMaintenance();
                    }
                    return new Response(`Maintenance ${enable ? 'enabled' : 'disabled'}`);
                }

                return new Response('Not Found', { status: 404 });
            },
        });

        this.manager?._debug(`[Dashboard] Server started on port ${this.port}`);
    }

    private async getStats() {
        const clusterStats = await this.manager?.fetchClientValues('cluster.info');
        return {
            totalClusters: this.manager?.totalClusters,
            activeClusters: this.manager?.clusters.size,
            memoryUsage: process.memoryUsage(),
            clusters: clusterStats,
        };
    }
}
