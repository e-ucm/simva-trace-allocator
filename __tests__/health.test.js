import test from 'node:test';
import assert from 'node:assert/strict';

import { buildHealthReport, createHealthServer } from '../src/health.js';

function fakeCompactor(overrides = {}) {
  return {
    shouldExit: false,
    status: { processing: false, current: -1, total: -1 },
    elapsedTime: 'Not running compactor',
    ...overrides
  };
}

/**
 * Runs `fn` against a health server bound to a free port.
 * @param {import('../src/health.js').HealthServerOptions} options
 * @param {(base: string) => Promise<void>} fn
 */
async function withServer(options, fn) {
  const server = createHealthServer(options);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    await fn(`http://127.0.0.1:${server.address().port}`);
  } finally {
    server.closeAllConnections?.();
    await new Promise((resolve) => server.close(resolve));
  }
}

test('buildHealthReport reports an idle worker as ok', () => {
  const report = buildHealthReport({ compactor: fakeCompactor() });
  assert.equal(report.service, 'simva-trace-allocator');
  assert.equal(report.status, 'ok');
  assert.equal(typeof report.uptime, 'number');
  assert.ok(!Number.isNaN(Date.parse(report.timestamp)));
  assert.deepEqual(report.compactor, {
    processing: false,
    current: -1,
    total: -1,
    startTime: null,
    elapsedTime: 'Not running compactor'
  });
});

test('buildHealthReport exposes the kafka connection state', () => {
  const report = buildHealthReport({ kafka: { connected: true } });
  assert.deepEqual(report.kafka, { connected: true });
  assert.equal(buildHealthReport({ kafka: {} }).kafka.connected, false);
  assert.equal(buildHealthReport({}).kafka, undefined);
});

test('buildHealthReport reports a worker on its way out as shutting-down', () => {
  const report = buildHealthReport({ compactor: fakeCompactor({ shouldExit: true }) });
  assert.equal(report.status, 'shutting-down');
});

test('GET /health answers 200 with the report', async () => {
  await withServer({ compactor: fakeCompactor(), kafka: { connected: true } }, async (base) => {
    const res = await fetch(`${base}/health`);
    assert.equal(res.status, 200);
    assert.match(res.headers.get('content-type'), /application\/json/);
    const body = await res.json();
    assert.equal(body.status, 'ok');
    assert.equal(body.compactor.processing, false);
    assert.equal(body.kafka.connected, true);
  });
});

test('/healthz is accepted as an alias', async () => {
  await withServer({}, async (base) => {
    const res = await fetch(`${base}/healthz`);
    assert.equal(res.status, 200);
  });
});

test('HEAD /health answers 200 without a body', async () => {
  await withServer({}, async (base) => {
    const res = await fetch(`${base}/health`, { method: 'HEAD' });
    assert.equal(res.status, 200);
    assert.equal(await res.text(), '');
  });
});

test('GET /health answers 503 while the worker is shutting down', async () => {
  await withServer({ compactor: fakeCompactor({ shouldExit: true }) }, async (base) => {
    const res = await fetch(`${base}/health`);
    assert.equal(res.status, 503);
    const body = await res.json();
    assert.equal(body.status, 'shutting-down');
  });
});

test('unknown paths answer 404', async () => {
  await withServer({}, async (base) => {
    const res = await fetch(`${base}/metrics`);
    assert.equal(res.status, 404);
  });
});

test('methods other than GET/HEAD answer 405', async () => {
  await withServer({}, async (base) => {
    const res = await fetch(`${base}/health`, { method: 'POST' });
    assert.equal(res.status, 405);
    assert.equal(res.headers.get('allow'), 'GET, HEAD');
  });
});
