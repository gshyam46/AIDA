#!/usr/bin/env node
/* Run against real local services: node scripts/e2e.cjs
 * Start a fresh backend first to verify the initial uncached language call.
 * Optional: AIDA_BASE_URL, AIDA_BROWSER_CHANNEL (msedge/chrome), AIDA_E2E_ARTIFACTS.
 * Install frontend dev dependencies first. This test never substitutes API results.
 */
'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const frontendRequire = createRequire(path.join(__dirname, '..', 'frontend', 'package.json'));
const { chromium } = frontendRequire('playwright');

const baseURL = (process.env.AIDA_BASE_URL || 'http://127.0.0.1:3000').replace(/\/$/, '');
const artifacts = path.resolve(process.env.AIDA_E2E_ARTIFACTS || path.join(__dirname, '..', 'artifacts', 'e2e'));
const report = { started_at: new Date().toISOString(), base_url: baseURL, steps: [], browser_errors: [], external_requests: [] };
fs.mkdirSync(artifacts, { recursive: true });

async function step(name, action) {
  const start = Date.now();
  try {
    const details = await action();
    report.steps.push({ name, status: 'passed', duration_ms: Date.now() - start, ...details });
    process.stdout.write(`PASS ${name}\n`);
  } catch (error) {
    report.steps.push({ name, status: 'failed', duration_ms: Date.now() - start, error: error.message });
    throw error;
  }
}

function watchContext(context) {
  context.on('page', page => page.on('pageerror', error => report.browser_errors.push(error.message)));
  return context.route('**/*', async route => {
    const requestURL = new URL(route.request().url());
    const allowedHosts = new Set([new URL(baseURL).hostname, 'localhost', '127.0.0.1', '[::1]']);
    if (['http:', 'https:'].includes(requestURL.protocol) && !allowedHosts.has(requestURL.hostname)) {
      report.external_requests.push(`${requestURL.origin}${requestURL.pathname}`);
      await route.abort('blockedbyclient');
      return;
    }
    await route.continue();
  });
}

async function queryResponse(page, action) {
  const pending = page.waitForResponse(response => /\/api\/v1\/query(?:\?|$)/.test(response.url()) && response.request().method() === 'POST');
  await action();
  const response = await pending;
  assert(response.ok(), `Query HTTP ${response.status()}`);
  return response.json();
}

async function ask(page, question) {
  await page.getByRole('tab', { name: 'Ask a question', exact: true }).click();
  await page.getByRole('textbox', { name: 'Ask a question', exact: true }).fill(question);
  const result = await queryResponse(page, () => page.getByRole('button', { name: 'Run query', exact: true }).click());
  if (result.success) await page.getByTestId('result-panel').waitFor();
  else await page.getByTestId('query-error').waitFor();
  return result;
}

async function assertNoHorizontalOverflow(page) {
  const widths = await page.evaluate(() => ({ viewport: window.innerWidth, body: document.documentElement.scrollWidth }));
  assert(widths.body <= widths.viewport + 1, `Horizontal overflow: ${widths.body}px on ${widths.viewport}px viewport`);
}

async function assertKPIValuesFit(page) {
  const clipped = await page.locator('.kpi-card').evaluateAll(cards => cards.flatMap(card => {
    const value = card.querySelector('[data-testid="metric-value"]');
    if (!value) return [];
    const textRange = document.createRange();
    textRange.selectNodeContents(value);
    const textBox = textRange.getBoundingClientRect();
    const cardBox = card.getBoundingClientRect();
    return textBox.right > cardBox.right - 6 || textBox.left < cardBox.left + 6
      ? [{ value: value.textContent, text_right: textBox.right, card_right: cardBox.right }] : [];
  }));
  assert.deepEqual(clipped, [], 'KPI values must fit inside their cards without clipping or overlap');
}

function assertSuccess(result) {
  assert.equal(result.success, true, result.error || 'Query did not succeed');
  assert(Array.isArray(result.results), 'Query must return structured result rows');
  assert(result.results.length > 0, 'Demo query unexpectedly returned no rows');
  assert.match(result.sql, /^SELECT\b/i, 'Query must compile to SELECT');
  assert([0, 1].includes(result.meta?.model_calls), 'Each query has at most one bounded model call');
  assert.equal(result.meta?.external_model_calls, 0, 'Inference must stay on this server');
}

async function main() {
  const options = { headless: true };
  if (process.env.AIDA_BROWSER_CHANNEL) options.channel = process.env.AIDA_BROWSER_CHANNEL;
  else if (process.platform === 'win32') options.channel = 'msedge';
  const browser = await chromium.launch(options);
  let page;
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 1050 }, reducedMotion: 'reduce' });
    await watchContext(context);
    page = await context.newPage();
    page.setDefaultTimeout(120000);
    let groupedResult;

    await step('Desktop opens with a real demo result', async () => {
      await page.goto(baseURL, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await page.getByTestId('result-panel').waitFor();
      await assertNoHorizontalOverflow(page);
      await assertKPIValuesFit(page);
      await page.screenshot({ path: path.join(artifacts, '01-desktop.png'), fullPage: true });
    });

    await step('Simple count executes locally and repeat query is deterministic', async () => {
      const first = await ask(page, 'Count all orders');
      assertSuccess(first);
      assert.equal(first.meta.model_calls, 1, 'A fresh language question must invoke the real model');
      assert(first.meta.total_tokens > 0, 'Real inference must report actual token usage');
      const second = await ask(page, 'Count all orders');
      assertSuccess(second);
      assert.equal(second.meta.model_calls, 0);
      assert.equal(second.meta.interpretation_cache_hit, true);
      assert.deepEqual(second.results, first.results, 'Repeated query result changed');
      assert.equal(second.sql, first.sql, 'Repeated query SQL changed');
      assert.deepEqual(second.parameters, first.parameters, 'Repeated query parameters changed');
      assert.equal(first.results[0].value, 7810, 'Count all orders must include all four order statuses');
      assert.match(await page.getByTestId('result-panel').getByTestId('metric-value').innerText(), /7,810/, 'Correct count must appear in the result UI');
      return { expected_count: 7810, first_execution_ms: first.meta.execution_time_ms, repeat_execution_ms: second.meta.execution_time_ms };
    });

    await step('Grouped results have an interactive chart and table', async () => {
      groupedResult = await ask(page, 'Revenue by region');
      assertSuccess(groupedResult);
      assert.deepEqual(groupedResult.results, [
        { region: 'West', value: 453155.01 }, { region: 'North', value: 429770.96 },
        { region: 'South', value: 367199.14 }, { region: 'East', value: 297851.83 },
      ], 'Regional revenue must reconcile to the deterministic synthetic data');
      await page.getByTestId('query-chart').waitFor();
      const bars = page.getByTestId('chart-bar');
      assert((await bars.count()) > 1, 'Grouped chart must expose clickable bars');
      await page.getByRole('tab', { name: 'Table', exact: true }).click();
      await page.getByTestId('result-panel').getByRole('table').waitFor();
      const downloadPending = page.waitForEvent('download');
      await page.getByRole('button', { name: 'Export CSV', exact: true }).click();
      const download = await downloadPending;
      const csvPath = path.join(artifacts, 'regional-revenue.csv');
      await download.saveAs(csvPath);
      const csv = fs.readFileSync(csvPath, 'utf8');
      assert.match(csv, /"West","453155\.01"/, 'CSV must contain the exact displayed regional value');
      assert.equal(csv.trim().split(/\r?\n/).length, 5, 'CSV must have one header and four regions');
      await page.getByRole('tab', { name: 'SQL & trust', exact: true }).click();
      assert.match(await page.getByTestId('result-panel').innerText(), /read-only SQL/i);
      await page.getByRole('tab', { name: 'Chart', exact: true }).click();
      await page.screenshot({ path: path.join(artifacts, '02-chart.png'), fullPage: true });
      const result = await queryResponse(page, () => bars.first().click());
      assertSuccess(result);
      assert.equal(result.meta.model_calls, 0, 'Chart interactions execute structured plans');
      await page.getByTestId('result-panel').waitFor();
      assert(!result.sql.includes('SELECT *'), 'Chart drilldown should retain an explicit validated projection');
      assert.notEqual(JSON.stringify(result.parameters), JSON.stringify(groupedResult.parameters), 'Chart click must apply a filter');
      assert.equal(result.plan.filters.region, 'West', 'Clicking the first bar must filter the West region');
    });

    await step('Visual builder produces a real grouped result', async () => {
      await page.getByRole('tab', { name: 'Visual builder', exact: true }).click();
      await page.getByRole('button', { name: 'Reset filters', exact: true }).click();
      await page.getByLabel('Group by', { exact: true }).selectOption('region');
      const result = await queryResponse(page, () => page.getByRole('button', { name: 'Run analysis', exact: true }).click());
      assertSuccess(result);
      assert.equal(result.meta.model_calls, 0, 'The visual builder executes structured plans');
      assert(result.results.length > 1, 'Builder should return a regional breakdown');
      await page.getByTestId('query-chart').waitFor();
      await page.screenshot({ path: path.join(artifacts, '03-builder.png'), fullPage: true });
    });

    await step('Dashboard saves definitions, persists across reload, refreshes and removes a card', async () => {
      await page.getByRole('button', { name: 'Save to dashboard', exact: true }).click();
      await page.getByRole('button', { name: /^Dashboards/ }).click();
      await page.getByTestId('dashboard-card').first().waitFor();
      const count = await page.getByTestId('dashboard-card').count();
      const stored = await page.evaluate(() => Object.fromEntries(Object.entries(localStorage)));
      assert(Object.keys(stored).length > 0, 'Dashboard definitions must persist');
      for (const [key, value] of Object.entries(stored)) {
        if (/dashboard/i.test(key)) {
          assert(!/"(?:results|rows|parameters)"\s*:/.test(value), `Saved dashboard ${key} should not persist result rows or resolved parameters`);
        }
      }
      await page.reload({ waitUntil: 'domcontentloaded', timeout: 30000 });
      await page.getByRole('button', { name: /^Dashboards/ }).click();
      await page.getByTestId('dashboard-card').first().waitFor();
      assert.equal(await page.getByTestId('dashboard-card').count(), count, 'Saved dashboard vanished after reload');
      const card = page.getByTestId('dashboard-card').first();
      const refreshed = await queryResponse(page, () => card.getByRole('button', { name: 'Refresh card', exact: true }).click());
      assertSuccess(refreshed);
      assert.equal(refreshed.meta.model_calls, 0, 'Dashboard refresh must reuse its approved plan');
      await page.screenshot({ path: path.join(artifacts, '04-dashboard.png'), fullPage: true });
      await card.getByRole('button', { name: 'Open in explorer', exact: true }).click();
      await page.getByTestId('result-panel').waitFor();
      await page.getByRole('button', { name: /^Dashboards/ }).click();
      await page.getByTestId('dashboard-card').first().getByRole('button', { name: 'Remove card', exact: true }).click();
      assert.equal(await page.getByTestId('dashboard-card').count(), count - 1, 'Card removal did not persist in the UI');
    });

    await step('Unsupported question shows a recoverable error', async () => {
      await page.getByRole('button', { name: /^Explorer/ }).click();
      const rejected = await ask(page, 'Predict customer churn using machine learning');
      assert.equal(rejected.success, false, 'Unsupported predictive request must not silently become a different query');
      assert(await page.getByTestId('query-error').isVisible(), 'A recoverable error must be shown');
      const recovered = await ask(page, 'Count all orders');
      assertSuccess(recovered);
    });

    await step('Empty averages and missing periods are not presented as fabricated zero values', async () => {
      const average = await ask(page, 'Average order value for pending');
      assert.equal(average.success, true);
      assert.equal(average.results[0].value, null, 'No completed orders should produce no average');
      assert(!/\$0\.00/.test(await page.getByTestId('result-panel').innerText()), 'A null average must not be shown as $0.00');
      const monthlyAverage = await ask(page, 'Average order value by month for pending');
      assertSuccess(monthlyAverage);
      assert(monthlyAverage.results.every(row => row.value === null), 'Pending orders must not acquire a completed-order average');
      assert.equal(await page.getByRole('button', { name: /^2025-\d\d: \$0/ }).count(), 0, 'Missing monthly averages must not become zero-valued chart points');
      const empty = await ask(page, 'Revenue by region in 2024');
      assert.equal(empty.success, true);
      assert.deepEqual(empty.results, [], 'There is no data for 2024');
      assert.match(await page.getByTestId('result-panel').innerText(), /No matching|No rows/i, 'Missing data needs a clear empty state');
    });

    await step('A second schema uses its own catalog and real language interpretation', async () => {
      await page.getByLabel('Data source', {exact: true}).selectOption('support');
      await page.getByTestId('result-panel').waitFor();
      const response = await ask(page, 'How many tickets does each team have?');
      assertSuccess(response);
      assert.equal(response.source_id, 'support');
      assert.equal(response.plan.metric, 'tickets');
      assert.equal(response.plan.dimension, 'team');
      assert.equal(response.meta.model_calls, 1);
      assert(response.data.every(row => 'team' in row));
      return {plan: response.plan, telemetry: response.meta};
    });

    await step('Upload an unseen SQLite schema, approve mappings, and ask a real question', async () => {
      await page.getByRole('button', {name: /^Data/}).click();
      await page.getByLabel('Upload SQLite database').setInputFiles(path.join(__dirname, '..', 'artifacts', 'inventory-e2e.sqlite'));
      await page.getByLabel('Source name', {exact: true}).fill('Inventory sample');
      await page.getByLabel('Measure 1 label', {exact: true}).fill('Units in stock');
      await page.getByLabel('Measure 1 aggregation', {exact: true}).selectOption('SUM');
      await page.getByLabel('Measure 1 column', {exact: true}).selectOption('units');
      await page.getByLabel('Measure 1 definition', {exact: true}).fill('Sum of units currently held in stock across the selected depots.');
      await page.getByLabel('Approve dimension depot', {exact: true}).check();
      // Leave approved values blank: literals must work without sampling rows.
      await page.getByRole('button', {name: 'Save catalog and explore', exact: true}).click();
      await page.getByTestId('result-panel').waitFor();
      const drilled = await queryResponse(page, () => page.getByRole('button', {name: /^Harbor: /}).click());
      assertSuccess(drilled);
      assert.equal(drilled.meta.model_calls, 0);
      assert.deepEqual(drilled.data, [{value: 55}]);
      const response = await ask(page, 'How many units do we have at Harbor?');
      assertSuccess(response);
      assert.equal(response.meta.model_calls, 1);
      assert.equal(response.plan.metric, 'units_in_stock');
      assert.deepEqual(response.plan.filters, {depot: 'Harbor'});
      assert.deepEqual(response.data, [{value: 55}]);
      assert(!['commerce', 'support'].includes(response.source_id));
      await page.screenshot({path: path.join(artifacts, '06-uploaded-source.png'), fullPage: true});
      await page.getByRole('button', {name: 'Save to dashboard', exact: true}).click();
      await page.getByRole('button', {name: /^Dashboards/}).click();
      await page.getByTestId('dashboard-card').first().waitFor();
      await page.getByLabel('Data source', {exact: true}).selectOption('commerce');
      assert.equal(await page.getByTestId('dashboard-card').count(), 0, 'Uploaded-source cards must not appear in another source dashboard');
      return {plan: response.plan, telemetry: response.meta};
    });

    await step('Mobile query and chart work without page overflow', async () => {
      const mobileContext = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 1, reducedMotion: 'reduce' });
      await watchContext(mobileContext);
      const mobile = await mobileContext.newPage();
      page = mobile;
      mobile.setDefaultTimeout(120000);
      await mobile.goto(baseURL, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await mobile.getByTestId('result-panel').waitFor();
      const result = await ask(mobile, 'Revenue by region');
      assertSuccess(result);
      await mobile.getByTestId('query-chart').waitFor();
      await assertNoHorizontalOverflow(mobile);
      await assertKPIValuesFit(mobile);
      await mobile.screenshot({ path: path.join(artifacts, '05-mobile.png'), fullPage: true });
      await mobileContext.close();
    });

    await step('No external browser requests and no uncaught JavaScript errors', async () => {
      assert.deepEqual(report.external_requests, [], 'App attempted to contact an external browser service');
      assert.deepEqual(report.browser_errors, [], 'Browser raised uncaught JavaScript errors');
    });
    report.status = 'passed';
  } catch (error) {
    report.status = 'failed';
    if (page) await page.screenshot({ path: path.join(artifacts, 'failure.png'), fullPage: true }).catch(() => {});
    throw error;
  } finally {
    report.finished_at = new Date().toISOString();
    fs.writeFileSync(path.join(artifacts, 'report.json'), JSON.stringify(report, null, 2));
    await browser.close();
  }
}

main().catch(error => {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exitCode = 1;
});
