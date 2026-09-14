#!/usr/bin/env node
/* Browser journey for AIDA 4 accounts: landing -> sign-up -> onboarding -> workspace -> sign-out -> sign-in.
 * Needs the backend with AIDA_REQUIRE_AUTH=1 and the frontend running. Creates a throwaway account.
 * No model calls unless AIDA_E2E_ASK=1, which also asks one real question.
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const {chromium} = createRequire(path.join(__dirname, '../frontend/package.json'))('playwright');
const root = path.resolve(__dirname, '..');
const base = process.env.AIDA_BASE_URL || 'http://127.0.0.1:3000';
const artifacts = path.join(root, 'artifacts/e2e-auth');
const account = {name: 'Journey Tester', email: `journey.${Date.now()}@example.com`, password: 'Harbor-7-lantern-2026'};
const report = {started_at: new Date().toISOString(), base_url: base, steps: [], browser_errors: [], failed_requests: []};
fs.mkdirSync(artifacts, {recursive: true});
const save = () => fs.writeFileSync(path.join(artifacts, 'report.json'), JSON.stringify(report, null, 2));
async function step(name, action) {
  const started = Date.now();
  try {const detail = await action(); report.steps.push({name, status: 'passed', duration_ms: Date.now() - started, ...detail}); process.stdout.write(`PASS ${name}\n`);}
  catch (error) {report.steps.push({name, status: 'failed', duration_ms: Date.now() - started, error: error.message}); throw error;}
  finally {save();}
}
async function launch() {
  try {return await chromium.launch();}
  catch {return chromium.launch({channel: 'msedge'});}
}

(async () => {
  const browser = await launch();
  const context = await browser.newContext({viewport: {width: 1366, height: 900}});
  const page = await context.newPage();
  page.on('console', message => {if (message.type() === 'error') report.browser_errors.push(message.text());});
  page.on('pageerror', error => report.browser_errors.push(error.message));
  page.on('response', response => {if (response.url().includes('/api/v1/') && response.status() >= 500) report.failed_requests.push(`${response.status()} ${response.url()}`);});
  const queryRequests = [];
  page.on('request', request => {if (request.url().endsWith('/api/v1/query')) queryRequests.push(request.postDataJSON());});
  const shot = name => page.screenshot({path: path.join(artifacts, `${name}.png`), fullPage: false});
  const fullShot = async name => {
    await page.evaluate(() => window.scrollTo(0, 0));
    return page.screenshot({path: path.join(artifacts, `${name}.png`), fullPage: true, animations: 'disabled'});
  };
  const mainNavigation = () => page.getByRole('navigation', {name: 'Main navigation'});
  const waitForPlan = () => page.waitForResponse(response => response.url().endsWith('/api/v1/query') && response.request().postDataJSON()?.plan, {timeout: 30000});
  let inspectedPlan;
  try {
    await step('landing explains AIDA and credits the author', async () => {
      await page.goto(base, {waitUntil: 'networkidle'});
      await page.getByRole('heading', {level: 1, name: /A clearer view/}).waitFor();
      assert.equal(await page.getByRole('button', {name: 'AIDA stands for Artificial Intelligence Data Analyst'}).count(), 1);
      assert.match(await page.locator('.watermark').innerText(), /ghanashyam/i);
      await page.getByRole('tab', {name: 'Validate'}).click();
      await page.getByText('Every cited phrase appears in your question').waitFor();
      await page.getByRole('button', {name: 'Attack attempt'}).click();
      const layers = page.locator('.layer-stack button');
      assert.equal(await layers.count(), 6);
      await page.locator('#trust').scrollIntoViewIfNeeded();
      for (let index = 5; index >= 0; index -= 1) {
        const name = (await layers.nth(index).innerText()).replace(/^\d+\s*/, '').trim();
        await layers.nth(index).click();
        await page.locator('.layer-detail h3', {hasText: name}).waitFor();
        await page.waitForFunction(() => {
          const panel = document.querySelector('.layer-detail'), content = panel?.querySelector('.layer-content');
          return !!panel && !!content && getComputedStyle(panel).opacity === '1' && getComputedStyle(content).opacity === '1';
        }, null, {timeout: 5000});
      }
      await shot('01-landing');
    });
    await step('workspace requires a session', async () => {
      await page.goto(`${base}/workspace`);
      await page.waitForURL(/\/login\?next=%2Fworkspace|\/login\?next=\/workspace/);
    });
    await step('sign-up validates passwords, then creates the account', async () => {
      await page.goto(`${base}/signup`);
      await page.getByLabel('Full name').fill(account.name);
      await page.getByLabel('Work email').fill(account.email);
      const passwords = page.locator('input[autocomplete="new-password"]');
      await passwords.nth(0).fill('short1');
      await passwords.nth(1).fill('short1');
      await page.getByRole('button', {name: 'Create account'}).click();
      await page.getByRole('alert').filter({hasText: 'at least 10 characters'}).waitFor();
      await passwords.nth(0).fill(account.password);
      await passwords.nth(1).fill(account.password);
      await shot('02-signup');
      await page.getByRole('button', {name: 'Create account'}).click();
      await page.waitForURL(/\/onboarding$/);
      const cookies = await context.cookies();
      const session = cookies.find(cookie => cookie.name === 'aida_session');
      assert(session && session.httpOnly && session.sameSite === 'Strict', 'Session cookie must be HttpOnly and SameSite=Strict');
      return {cookie: {httpOnly: session.httpOnly, sameSite: session.sameSite}};
    });
    await step('onboarding collects organization, goals, data and consent', async () => {
      await page.getByLabel('Company or team name').fill('Journey Logistics');
      await page.getByLabel('Your role').fill('Operations analyst');
      await page.getByRole('button', {name: '2-10 people'}).click();
      await shot('03-onboarding');
      await page.getByRole('button', {name: 'Continue'}).click();
      await page.getByRole('button', {name: 'Logistics'}).click();
      await page.getByRole('button', {name: 'Continue'}).click();
      await page.getByRole('button', {name: /Add the logistics sample/}).click();
      await page.getByRole('button', {name: 'Continue'}).click();
      const finish = page.getByRole('button', {name: 'Open my workspace'});
      const consent = page.getByRole('checkbox');
      if (await consent.count()) {assert(await finish.isDisabled(), 'Hosted inference needs explicit consent'); await consent.check();}
      await shot('04-consent');
      await finish.click();
      await page.waitForURL(/\/workspace\?source=/);
    });
    await step('workspace opens the private logistics sample', async () => {
      await page.locator('[data-testid="result-panel"]').waitFor({timeout: 60000});
      assert.match(await page.locator('.user-menu').innerText(), /Journey Tester/);
      assert.match(await page.locator('select[aria-label="Data source"] option:checked').innerText(), /Logistics sample/);
      assert.equal(await page.getByRole('link', {name: 'AIDA home'}).first().innerText(), 'AIDA');
      await shot('05-workspace');
    });
    await step('result table supports search, filter and sort', async () => {
      await page.getByRole('tab', {name: 'Table'}).click();
      const rows = page.locator('.data-table tbody tr');
      const before = await rows.count();
      assert(before > 0, 'The preview returns rows');
      const header = page.locator('.data-table thead tr').first().locator('button').first();
      await header.click();
      const firstFilter = page.locator('.filter-row input').first();
      const firstCell = (await rows.first().locator('td').first().innerText()).trim();
      await firstFilter.fill(firstCell.slice(0, 3));
      assert((await rows.count()) >= 1);
      await page.getByLabel('Search table').fill('zzzz-no-match');
      assert.equal(await page.locator('.data-table tbody tr td').filter({hasText: /\S/}).count() <= 1, true);
      await page.getByLabel('Search table').fill('');
      await firstFilter.fill('');
      await shot('06-table');
      return {rows: before};
    });
    await step('every offered chart type renders without a new query', async () => {
      await page.getByRole('tab', {name: 'Chart', exact: true}).click();
      const before = queryRequests.length;
      const options = await page.locator('.chart-type button').evaluateAll(buttons => buttons.map(button => button.getAttribute('aria-label')));
      assert(options.length >= 2, 'The logistics sample offers multiple chart views');
      for (const option of options) {
        const type = option.replace(/ chart$/, '').toLowerCase();
        const button = page.getByRole('button', {name: option, exact: true});
        await button.click();
        assert.equal(await button.getAttribute('aria-pressed'), 'true');
        const chart = page.getByTestId('result-panel').getByTestId('query-chart');
        await chart.waitFor();
        assert.equal(await chart.getAttribute('data-chart-type'), type, `${type} renderer selected`);
        if (type === 'bar') assert((await chart.getByTestId('chart-bar').count()) > 0);
        if (type === 'line' || type === 'area') {
          assert((await chart.locator('svg polyline').count()) > 0);
          assert.equal((await chart.locator('svg path').count()) > 0, type === 'area');
        }
        if (type === 'donut') assert((await chart.getByTestId('donut-segment').count()) > 0);
        await chart.scrollIntoViewIfNeeded();
        await shot(`07-chart-${type}`);
      }
      assert.equal(queryRequests.length, before, 'Changing chart presentation must not execute a query');
      await fullShot('07-workspace-full');
      return {chart_types: options};
    });
    await step('SQL and trust exposes compiled SQL, lineage and the validated plan', async () => {
      await page.getByRole('tab', {name: 'SQL & trust'}).click();
      const sql = page.locator('.sql-content');
      assert.match(await sql.locator(':scope > pre').first().innerText(), /\bSELECT\b/i);
      assert((await sql.getByTestId('query-lineage').locator('.lineage-tables > span').count()) > 0);
      assert((await sql.locator('.lineage-columns tbody tr').count()) > 0);
      const plan = sql.locator('details').filter({has: page.locator('summary', {hasText: 'View the validated query plan'})});
      await plan.locator('summary').click();
      inspectedPlan = JSON.parse(await plan.locator('pre').innerText());
      assert.equal(inspectedPlan.version, 2);
      assert(inspectedPlan.metrics.length > 0);
      assert.match(await page.locator('.result-footer').innerText(), /0 model calls/);
      await sql.scrollIntoViewIfNeeded();
      await shot('08-sql-trust');
      return {metrics: inspectedPlan.metrics, dimensions: inspectedPlan.dimensions};
    });
    await step('saved dashboard refreshes and reopens its exact plan without a model', async () => {
      await page.getByRole('button', {name: 'Save to dashboard'}).click();
      await page.getByRole('status').filter({hasText: 'Saved to this source'}).waitFor();
      const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('aida:dashboard:v2') || '[]'));
      assert.equal(saved.length, 1);
      assert.deepEqual(saved[0].plan, inspectedPlan);
      const refreshed = waitForPlan();
      await mainNavigation().getByRole('button', {name: /^Dashboards/}).click();
      const result = await (await refreshed).json();
      assert.equal(result.success, true);
      assert.equal(result.meta.model_calls, 0);
      const card = page.getByTestId('dashboard-card');
      assert.equal(await card.count(), 1);
      await card.getByTestId('query-chart').waitFor();
      await shot('09-dashboard');
      await fullShot('09-dashboard-full');
      const reopened = waitForPlan();
      await card.getByRole('button', {name: 'Open in explorer'}).click();
      const reopenedResult = await (await reopened).json();
      assert.equal(reopenedResult.success, true);
      assert.equal(reopenedResult.meta.model_calls, 0);
      assert.deepEqual(reopenedResult.plan, inspectedPlan);
      await page.getByTestId('result-panel').waitFor();
      await page.getByRole('tab', {name: 'Chart', exact: true}).click();
      return {saved_cards: saved.length, model_calls: reopenedResult.meta.model_calls};
    });
    await step('data catalog shows the connected source and approved definitions', async () => {
      await mainNavigation().getByRole('button', {name: 'Data catalog', exact: true}).click();
      await page.getByRole('heading', {name: 'Measures with explicit definitions'}).waitFor();
      assert.match(await page.locator('.dataset-banner h2').innerText(), /logistics/i);
      assert((await page.locator('.catalog-metric').count()) > 0);
      assert((await page.locator('.catalog-dimension').count()) > 0);
      await shot('10-data-catalog');
      await fullShot('10-data-catalog-full');
      await mainNavigation().getByRole('button', {name: 'Explorer', exact: true}).click();
      await page.getByTestId('result-panel').waitFor();
    });
    await step('workspace metrics fit tablet and mobile; navigation works at 390px and 320px', async () => {
      const measurements = [];
      const assertMetricsFit = async width => {
        const metrics = await page.locator('.kpi-card > strong').evaluateAll(elements => elements.map(element => ({value: element.textContent.trim(), width: element.clientWidth, contentWidth: element.scrollWidth})));
        assert(metrics.length > 0, 'Overview metrics are present');
        assert(metrics.every(metric => metric.contentWidth <= metric.width + 1), `KPI values are clipped at ${width}px: ${JSON.stringify(metrics)}`);
        return metrics;
      };
      const waitForClosedNavigation = () => page.waitForFunction(() => {
        const sidebar = document.querySelector('.sidebar');
        return sidebar && !sidebar.classList.contains('is-open') && sidebar.getBoundingClientRect().right <= 1;
      });
      if (await page.getByRole('button', {name: 'Dismiss notification'}).count()) await page.getByRole('button', {name: 'Dismiss notification'}).click();
      await page.setViewportSize({width: 768, height: 1024});
      const tabletMetrics = await assertMetricsFit(768);
      await fullShot('11-workspace-768-full');
      for (const width of [390, 320]) {
        await page.setViewportSize({width, height: 844});
        await waitForClosedNavigation();
        await page.evaluate(() => window.scrollTo(0, 0));
        const overflow = await page.evaluate(() => ({viewport: window.innerWidth, page: document.documentElement.scrollWidth,
          elements: [...document.querySelectorAll('.main-shell *')].filter(element => element.getBoundingClientRect().right > window.innerWidth + 1 && getComputedStyle(element).position !== 'fixed').slice(0, 12).map(element => ({tag: element.tagName, class: element.className, right: element.getBoundingClientRect().right}))}));
        measurements.push({...overflow, metrics: await assertMetricsFit(width)});
        await shot(`11-workspace-${width}`);
        await fullShot(`11-workspace-${width}-full`);
        assert(overflow.page <= width, `Workspace overflows at ${width}px: ${JSON.stringify(overflow)}`);
        await page.getByRole('button', {name: 'Open navigation', exact: true}).click();
        await page.locator('.sidebar.is-open').waitFor();
        await page.waitForFunction(() => document.querySelector('.sidebar').getBoundingClientRect().x >= -1);
        const sidebar = await page.locator('.sidebar').boundingBox();
        assert(sidebar.x >= -1 && sidebar.x + sidebar.width <= width, 'Open navigation fits viewport');
        await shot(`12-navigation-${width}`);
        // The sidebar covers the left part of the backdrop; click the exposed right edge.
        await page.getByRole('button', {name: 'Close navigation', exact: true}).click({position: {x: width - 8, y: 40}});
        await page.locator('.sidebar.is-open').waitFor({state: 'detached'});
        await waitForClosedNavigation();
        await page.getByRole('button', {name: 'Open navigation', exact: true}).click();
        await mainNavigation().getByRole('button', {name: 'Data catalog', exact: true}).click();
        await page.getByRole('heading', {name: 'Measures with explicit definitions'}).waitFor();
        assert.equal(await page.locator('.sidebar.is-open').count(), 0, 'Selecting a page closes navigation');
        await waitForClosedNavigation();
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), `Catalog fits ${width}px viewport`);
        await shot(`13-catalog-${width}`);
        await page.getByRole('button', {name: 'Open navigation', exact: true}).click();
        const refreshed = waitForPlan();
        await mainNavigation().getByRole('button', {name: /^Dashboards/}).click();
        assert.equal((await (await refreshed).json()).meta.model_calls, 0);
        await page.getByTestId('dashboard-card').getByTestId('query-chart').waitFor();
        await waitForClosedNavigation();
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), `Dashboard fits ${width}px viewport`);
        await fullShot(`14-dashboard-${width}-full`);
        await page.getByRole('button', {name: 'Open navigation', exact: true}).click();
        await mainNavigation().getByRole('button', {name: 'Explorer', exact: true}).click();
        await page.getByTestId('result-panel').waitFor();
      }
      await page.setViewportSize({width: 1366, height: 900});
      assert(queryRequests.every(request => request.plan && !request.question), 'All presentation journeys execute explicit plans only');
      return {tablet_metrics: tabletMetrics, viewports: measurements, question_requests: 0};
    });
    if (process.env.AIDA_E2E_ASK === '1') await step('a real question returns an interpreted answer', async () => {
      await page.getByRole('tab', {name: 'Ask a question'}).click();
      const pending = page.waitForResponse(response => response.url().endsWith('/api/v1/query') && response.request().postDataJSON()?.question, {timeout: 180000});
      await page.getByLabel('Ask a question').fill('How many shipments were delivered late by carrier?');
      await page.getByRole('button', {name: 'Run query'}).click();
      const body = await (await pending).json();
      await page.locator('[data-testid="result-panel"], [data-testid="query-error"]').first().waitFor({timeout: 30000});
      await shot('07-question');
      return {success: body.success, error_type: body.error_type || null, model_calls: body.meta?.model_calls};
    });
    await step('sign-out ends the session', async () => {
      await page.getByRole('button', {name: 'Sign out'}).click();
      await page.waitForURL(/\/login/);
      await page.goto(`${base}/workspace`);
      await page.waitForURL(/\/login/);
    });
    await step('wrong password is rejected without detail; correct password signs in', async () => {
      await page.getByLabel('Work email').fill(account.email);
      await page.getByLabel('Password').fill('Wrong-password-99');
      await page.getByRole('button', {name: 'Sign in'}).click();
      const message = await page.getByRole('alert').innerText();
      assert.doesNotMatch(message, /not found|no account|unknown user/i);
      await page.getByLabel('Password').fill(account.password);
      await page.getByRole('button', {name: 'Sign in'}).click();
      await page.waitForURL(/\/workspace/);
      await page.locator('.user-menu').waitFor();
      return {error_message: message};
    });
    await step('no browser errors or server failures', async () => {
      const unexpected = report.browser_errors.filter(text => !/401|Unauthorized|status of 4\d\d/.test(text));
      assert.deepEqual(unexpected, []);
      assert.deepEqual(report.failed_requests, []);
    });
    report.status = 'passed';
  } catch (error) {
    report.status = 'failed';
    await shot('failure').catch(() => undefined);
    process.stdout.write(`FAIL ${error.message}\n`);
    process.exitCode = 1;
  } finally {
    report.finished_at = new Date().toISOString();
    save();
    await browser.close();
  }
})();
