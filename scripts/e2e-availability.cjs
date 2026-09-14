#!/usr/bin/env node
/* Account-page availability journeys against a running frontend (default http://localhost:3000).
 * Backend failures and interest storage are mocked in the browser where noted, so no account is created and nothing is
 * stored. Direct API checks only send requests the interest route rejects before storage.
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {randomUUID} = require('node:crypto');
const {createRequire} = require('node:module');
const {chromium} = createRequire(path.join(__dirname, '../frontend/package.json'))('playwright');

const base = process.env.AIDA_BASE_URL || 'http://localhost:3000';
const artifacts = path.join(__dirname, '../artifacts/e2e-availability');
fs.mkdirSync(artifacts, {recursive: true});
const unavailable = {status: 503, contentType: 'application/json', body: JSON.stringify({error: 'The data service is unavailable. Start the backend, then try again.'})};

async function step(name, action) {
  try {
    const note = await action();
    console.log(`PASS ${name}${note ? ` (${note})` : ''}`);
  } catch (error) {
    console.log(`FAIL ${name}: ${error.message}`);
    process.exitCode = 1;
  }
}

(async () => {
  const browser = await chromium.launch().catch(() => chromium.launch({channel: 'msedge'}));
  const open = async (mocks = {}) => {
    const context = await browser.newContext({viewport: {width: 1280, height: 860}});
    for (const [pattern, handler] of Object.entries(mocks)) await context.route(pattern, handler);
    return {context, page: await context.newPage()};
  };
  const healthy = await fetch(`${base}/api/v1/health`).then(response => response.ok ? response.json() : null).then(body => body?.status === 'healthy').catch(() => false);
  console.log(`backend healthy: ${healthy}`);

  await step('backend up: sign-up shows the account form', async () => {
    if (!healthy) return 'skipped, backend not running';
    const {context, page} = await open();
    await page.goto(`${base}/signup`);
    await page.getByRole('heading', {name: 'Create your account'}).waitFor({timeout: 30000});
    await context.close();
  });

  for (const [route, heading] of [['/signup', 'Sign up'], ['/login', 'Sign in'], ['/onboarding', 'Sign up'], ['/workspace', 'Sign up']]) {
    await step(`backend down: ${route} keeps registration available`, async () => {
      const {context, page} = await open({'**/api/v1/health': request => request.fulfill(unavailable)});
      await page.goto(base + route);
      await page.getByRole('heading', {name: heading}).waitFor({timeout: 30000});
      assert.equal(await page.locator('input[type=password]').count(), 0, 'no password field on the interest page');
      assert.equal(await page.getByText(/Currently unavailable|Sign-ups are coming soon/).count(), 0);
      await context.close();
    });
  }

  await step('backend fails mid-submit: details carry over, the password is never sent', async () => {
    if (!healthy) return 'skipped, backend not running';
    let captured = null;
    const {context, page} = await open({
      '**/api/v1/auth/signup': request => request.fulfill(unavailable),
      '**/api/interest': route => {captured = route.request().postDataJSON(); return route.fulfill({status: 200, contentType: 'application/json', body: '{"stored":true}'});},
    });
    const password = `Harbor-7-${randomUUID().slice(0, 8)}`;
    await page.goto(`${base}/signup`);
    await page.getByRole('heading', {name: 'Create your account'}).waitFor({timeout: 30000});
    await page.getByLabel('Full name').fill('Asha Mehta');
    await page.getByLabel('Work email').fill('asha.mehta@example.com');
    const passwords = page.locator('input[autocomplete="new-password"]');
    await passwords.nth(0).fill(password);
    await passwords.nth(1).fill(password);
    await page.getByRole('button', {name: 'Create account'}).click();
    await page.getByRole('heading', {name: 'Sign up'}).waitFor();
    assert.equal(await page.getByLabel('Full name').inputValue(), 'Asha Mehta');
    assert.equal(await page.getByLabel('Work email').inputValue(), 'asha.mehta@example.com');
    await page.getByRole('checkbox').check();
    await page.getByRole('button', {name: 'Sign up', exact: true}).click();
    await page.waitForURL(/\/waitlist$/);
    await page.getByRole('heading', {name: 'You’re on the list.'}).waitFor();
    assert(captured, 'interest request was sent');
    assert.equal(captured.source, 'signup');
    assert.equal(captured.consent, true);
    const serialized = JSON.stringify(captured);
    assert(!/password/i.test(serialized) && !serialized.includes(password), 'no password in the interest payload');
    await context.close();
    return `payload fields: ${Object.keys(captured).join(', ')}`;
  });

  await step('registration saved: redirect without personal details in URL or browser storage', async () => {
    let captured = null;
    const {context, page} = await open({
      '**/api/v1/health': request => request.fulfill(unavailable),
      '**/api/interest': route => {captured = route.request().postDataJSON(); return route.fulfill({status: 200, contentType: 'application/json', body: '{"stored":true}'});},
    });
    await page.goto(`${base}/signup`);
    await page.getByLabel('Full name').fill('Asha Mehta');
    await page.getByLabel('Work email').fill('asha.mehta@example.com');
    await page.getByRole('checkbox').check();
    await page.screenshot({path: path.join(artifacts, 'signup-desktop.png'), fullPage: true});
    await page.setViewportSize({width: 390, height: 844});
    await page.screenshot({path: path.join(artifacts, 'signup-mobile.png'), fullPage: true});
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), 'signup fits mobile viewport');
    await page.setViewportSize({width: 1280, height: 860});
    await page.getByRole('button', {name: 'Sign up', exact: true}).click();
    await page.waitForURL(/\/waitlist$/);
    await page.getByRole('heading', {name: 'You’re on the list.'}).waitFor();
    await page.screenshot({path: path.join(artifacts, 'waitlist-desktop.png'), fullPage: true});
    await page.setViewportSize({width: 390, height: 844});
    await page.screenshot({path: path.join(artifacts, 'waitlist-mobile.png'), fullPage: true});
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), 'waitlist fits mobile viewport');
    assert.equal(new URL(page.url()).search, '');
    assert.equal(captured.email, 'asha.mehta@example.com');
    assert.equal(captured.consent, true);
    assert(!/password/i.test(JSON.stringify(captured)));
    const stored = await page.evaluate(() => JSON.stringify({local: {...localStorage}, session: {...sessionStorage}}));
    assert(!stored.includes('asha.mehta') && !stored.includes('Asha Mehta'), 'no PII persisted in browser storage');
    await page.reload();
    await page.getByRole('heading', {name: 'You’re on the list.'}).waitFor();
    await context.close();
  });

  for (const status of [200, 503]) await step(`storage response ${status} with stored:false: preserve details and do not confirm`, async () => {
    const {context, page} = await open({
      '**/api/v1/health': request => request.fulfill(unavailable),
      '**/api/interest': request => request.fulfill({status, contentType: 'application/json', body: JSON.stringify({stored: false, error: 'We could not save your details right now. Please try again a little later.'})}),
    });
    await page.goto(`${base}/signup`);
    await page.getByLabel('Full name').fill('Asha Mehta');
    await page.getByLabel('Work email').fill('asha.mehta@example.com');
    await page.getByRole('checkbox').check();
    await page.getByRole('button', {name: 'Sign up', exact: true}).click();
    await page.getByRole('alert').filter({hasText: 'could not save your details'}).waitFor();
    assert.equal(await page.getByRole('heading', {name: 'You’re on the list.'}).count(), 0);
    assert.equal(new URL(page.url()).pathname, '/signup');
    assert.equal(await page.getByLabel('Full name').inputValue(), 'Asha Mehta');
    assert.equal(await page.getByLabel('Work email').inputValue(), 'asha.mehta@example.com');
    assert.equal(await page.getByRole('checkbox').isChecked(), true);
    await context.close();
  });

  await step('direct waitlist visit does not claim a registration was saved', async () => {
    const {context, page} = await open();
    await page.goto(`${base}/waitlist`);
    await page.getByRole('heading', {name: 'Make a start.'}).waitFor();
    assert.equal(await page.getByRole('heading', {name: 'You’re on the list.'}).count(), 0);
    await context.close();
  });

  await step('interest API rejects bad input before storage', async () => {
    const client = `198.51.100.${Math.floor(Math.random() * 200) + 1}`;
    const post = (body, headers = {}) => fetch(`${base}/api/interest`, {method: 'POST', headers: {'Content-Type': 'application/json', 'x-real-ip': client, ...headers}, body: JSON.stringify(body)});
    const valid = {name: 'Asha Mehta', email: 'asha.mehta@example.com', source: 'signup', consent: true};
    assert.equal((await post({...valid, email: 'not-an-email'})).status, 400);
    assert.equal((await post({...valid, consent: false})).status, 400);
    assert.equal((await post(valid, {Origin: 'https://attacker.example'})).status, 403);
    const trap = await post({...valid, website: 'https://spam.example'});
    assert.equal(trap.status, 200);
    assert.equal((await trap.json()).stored, true);
    assert.equal((await fetch(`${base}/api/interest`, {method: 'POST', headers: {'Content-Type': 'application/json', 'x-real-ip': client}, body: 'x'.repeat(5000)})).status, 413);
  });

  await browser.close();
})();
