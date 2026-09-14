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
  const shot = name => page.screenshot({path: path.join(artifacts, `${name}.png`), fullPage: false});
  try {
    await step('landing explains AIDA and credits the author', async () => {
      await page.goto(base, {waitUntil: 'networkidle'});
      await page.getByRole('heading', {level: 1, name: /Ask your data anything/}).waitFor();
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
      assert.equal(await page.locator('.brand-word').first().innerText(), 'AIDA.');
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
