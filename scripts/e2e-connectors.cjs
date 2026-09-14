#!/usr/bin/env node
/* Real production browser/API/compiler with the explicitly labeled SQLite remote
 * surrogate in connector-test-server.py. No vendor or model accuracy claim. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {execFileSync} = require('node:child_process');
const {createRequire} = require('node:module');
const root = path.resolve(__dirname, '..');
const {chromium} = createRequire(path.join(root, 'frontend/package.json'))('playwright');
const base = process.env.AIDA_CONNECTOR_BROWSER_URL || 'http://127.0.0.1:38147';
const directory = path.join(root, 'artifacts/connector-browser');
const report = {started_at: new Date().toISOString(), remote: 'explicit SQLite remote-session surrogate', production_browser: true, real_vendor_connection: false, model_calls: 0, steps: [], browser_errors: []};
async function step(name, fn) {await fn(); report.steps.push(name); process.stdout.write(`PASS ${name}\n`);}
async function until(page, predicate) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const data = await (await page.request.get(`${base}/api/v1/connections`)).json();
    if (predicate(data.connections)) return;
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  throw new Error('Connection status did not reach the expected state');
}
(async () => {
  const browser = await chromium.launch({headless: true, ...(process.env.AIDA_BROWSER_CHANNEL ? {channel: process.env.AIDA_BROWSER_CHANNEL} : {})});
  const page = await browser.newPage({viewport: {width: 1440, height: 1000}});
  page.on('pageerror', error => report.browser_errors.push(error.message));
  page.on('response', async response => {
    if (response.url().endsWith('/api/v1/query')) {
      try {report.model_calls += (await response.json()).meta?.model_calls || 0;} catch {}
    }
  });
  try {
    const health = await page.request.get(`${base}/api/v1/health`);
    assert(health.ok() && (await health.json()).status === 'healthy', 'Start the isolated AIDA test services first');
    await step('Open database connection form', async () => {
      await page.goto(base);
      await page.getByRole('button', {name: 'Data catalog', exact: true}).click();
      await page.getByRole('button', {name: 'Connect database', exact: true}).click();
      await page.getByLabel('Connection name', {exact: true}).fill('Browser inventory');
      await page.getByLabel('Database host', {exact: true}).fill('127.0.0.1');
      await page.getByLabel('Database name', {exact: true}).fill('synthetic');
      await page.getByLabel('Database schema', {exact: true}).fill('main');
      await page.getByLabel('Database username', {exact: true}).fill('test_reader');
      await page.getByLabel('Database password', {exact: true}).fill('disposable-test-secret');
      await page.getByLabel('Verify TLS certificate and hostname', {exact: false}).uncheck();
      await page.getByRole('button', {name: 'Test connection and inspect', exact: true}).click();
      await page.getByLabel('Copy stock.quantity', {exact: true}).waitFor();
      assert(!(await page.locator('body').innerText()).includes('synthetic@example.test'));
    });
    await step('Copy only selected columns and schedule refresh', async () => {
      for (const field of ['item_id', 'category', 'quantity']) await page.getByLabel(`Copy stock.${field}`, {exact: true}).check();
      assert(!(await page.getByLabel('Copy stock.customer_email', {exact: true}).isChecked()));
      await page.getByLabel('Initial refresh schedule', {exact: true}).selectOption('hourly');
      await page.getByRole('button', {name: 'Create reporting snapshot', exact: true}).click();
      await page.getByRole('button', {name: 'Open snapshot / approve catalog', exact: true}).waitFor({timeout: 30000});
      await page.getByRole('button', {name: 'Open snapshot / approve catalog', exact: true}).click();
      await page.getByLabel('Measure 1 aggregation', {exact: true}).selectOption('SUM');
      await page.getByLabel('Measure 1 column', {exact: true}).selectOption('quantity');
      await page.getByLabel('Measure 1 label', {exact: true}).fill('Inventory');
      await page.getByLabel('Measure 1 definition', {exact: true}).fill('Sum of inventory quantities');
      await page.getByRole('button', {name: 'Save catalog and explore', exact: true}).click();
      await page.getByTestId('result-panel').waitFor({timeout: 30000});
      await page.getByRole('button', {name: 'Save to dashboard', exact: true}).click();
      await page.getByRole('button', {name: /^Dashboards/}).click();
      await page.getByTestId('dashboard-card').waitFor();
    });
    let sourceId;
    await step('Manual refresh picks up changed data and preserves saved catalog', async () => {
      const before = await (await page.request.get(`${base}/api/v1/connections`)).json();
      sourceId = before.connections[0].source_id;
      const query = {source_id: sourceId, plan: {metric: 'inventory'}};
      assert.deepEqual((await (await page.request.post(`${base}/api/v1/query`, {data: query})).json()).data, [{value: 19}]);
      execFileSync(path.join(root, '.venv/Scripts/python.exe'), ['-c', "import sqlite3; c=sqlite3.connect('artifacts/connector-browser/remote.sqlite'); c.execute('UPDATE stock SET quantity=50'); c.commit(); c.close()"], {cwd: root});
      await page.getByRole('button', {name: 'Data catalog', exact: true}).click();
      const last = before.connections[0].last_success;
      await page.getByRole('button', {name: 'Refresh now', exact: true}).click();
      await until(page, connections => connections[0]?.status === 'succeeded' && connections[0]?.last_success !== last);
      const result = await (await page.request.post(`${base}/api/v1/query`, {data: query})).json();
      assert.deepEqual(result.data, [{value: 100}]);
      assert(result.meta.snapshot_updated_at);
      await page.getByRole('button', {name: /^Dashboards/}).click();
      const refresh = page.waitForResponse(response => response.url().endsWith('/api/v1/query'));
      await page.getByRole('button', {name: 'Refresh dashboard', exact: true}).click();
      assert.deepEqual((await (await refresh).json()).data, [{value: 100}]);
    });
    await step('Pause schedule and disconnect while retaining the snapshot', async () => {
      await page.getByRole('button', {name: 'Data catalog', exact: true}).click();
      await page.getByLabel('Refresh schedule for Browser inventory', {exact: true}).selectOption('manual');
      await until(page, connections => connections[0]?.schedule === 'manual');
      await page.screenshot({path: path.join(directory, 'connections.png'), fullPage: true});
      await page.getByRole('button', {name: 'Disconnect and remove credentials', exact: true}).click();
      await until(page, connections => connections.length === 0);
      assert((await (await page.request.get(`${base}/api/v1/catalog?source_id=${sourceId}`)).json()).metrics.length);
      assert.deepEqual(report.browser_errors, []);
      assert.equal(report.model_calls, 0);
    });
    report.passed = true;
  } catch (error) {report.passed = false; report.error = error.message; await page.screenshot({path: path.join(directory, 'failure.png'), fullPage: true}); throw error;}
  finally {report.finished_at = new Date().toISOString(); fs.writeFileSync(path.join(directory, 'report.json'), JSON.stringify(report, null, 2)); await browser.close();}
})().catch(error => {process.stderr.write(`${error.stack}\n`); process.exitCode = 1;});
