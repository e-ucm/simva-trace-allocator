import http from 'node:http';

/**
 * @typedef HealthServerOptions
 * @property {import('./compactor.js').Compactor} [compactor]
 * @property {import('./kafka.js').KafkaClient} [kafka]
 */

/**
 * Builds the payload served by the health endpoint.
 *
 * The report is a liveness view of the worker: it is `ok` while the process is
 * up and not shutting down, and it exposes the compactor/kafka state so a
 * deeper check can be made without parsing logs.
 *
 * @param {HealthServerOptions} opts
 * @returns {{service: string, status: string, uptime: number, timestamp: string, compactor?: object, kafka?: object}}
 */
export function buildHealthReport({ compactor, kafka } = {}) {
    const report = {
        service: 'simva-trace-allocator',
        status: compactor?.shouldExit === true ? 'shutting-down' : 'ok',
        uptime: process.uptime(),
        timestamp: new Date().toISOString()
    };
    if (compactor) {
        report.compactor = {
            processing: compactor.status?.processing === true,
            current: compactor.status?.current,
            total: compactor.status?.total,
            startTime: compactor.status?.startTime ?? null,
            elapsedTime: compactor.elapsedTime
        };
    }
    if (kafka) {
        report.kafka = { connected: kafka.connected === true };
    }
    return report;
}

/**
 * Creates (but does not start) the HTTP server exposing the health endpoint.
 *
 * `GET /health` (alias `/healthz`) answers `200` with a JSON report while the
 * worker is alive, `503` once it starts shutting down, `405` for other
 * methods and `404` for any other path.
 *
 * @param {HealthServerOptions} options
 * @returns {http.Server}
 */
export function createHealthServer(options = {}) {
    return http.createServer((req, res) => {
        const { pathname } = new URL(req.url ?? '/', 'http://localhost');
        if (pathname !== '/health' && pathname !== '/healthz') {
            send(res, 404, { service: 'simva-trace-allocator', status: 'not-found', path: pathname }, req.method);
            return;
        }
        if (req.method !== 'GET' && req.method !== 'HEAD') {
            res.setHeader('Allow', 'GET, HEAD');
            send(res, 405, { service: 'simva-trace-allocator', status: 'method-not-allowed' }, req.method);
            return;
        }
        const report = buildHealthReport(options);
        send(res, report.status === 'ok' ? 200 : 503, report, req.method);
    });
}

/**
 * @param {http.ServerResponse} res
 * @param {number} statusCode
 * @param {object} payload
 * @param {string} [method]
 */
function send(res, statusCode, payload, method) {
    const body = JSON.stringify(payload);
    res.writeHead(statusCode, {
        'content-type': 'application/json; charset=utf-8',
        'content-length': Buffer.byteLength(body),
        'cache-control': 'no-store'
    });
    res.end(method === 'HEAD' ? undefined : body);
}
