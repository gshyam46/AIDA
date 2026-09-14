#!/usr/bin/env node
/* Account-page availability journeys against a running frontend (default http://localhost:3000).
 * Backend failures and interest storage are mocked in the browser where noted, so no account is created and nothing is
 * stored. Direct API checks only send requests the interest route rejects before storage.
 */
'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');
const {randomUUID} = require('node:crypto');
const {createRequire} = require('node:module');
const {chromium} = createRequire(path.join(__dirname, '../frontend/package.json'))('playwright');

const base = process.env.AIDA_BASE_URL || 'http://localhost:3000';
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

  for (const [route, heading] of [['/signup', 'Sign-ups are coming soon'], ['/login', 'Sign-in is paused for now'], ['/onboarding', 'Sign-ups are coming soon'], ['/workspace', 'Sign-ups are coming soon']]) {
    await step(`backend down: ${route} shows the interest page`, async () => {
      const {context, page} = await open({'**/api/v1/health': request => request.fulfill(unavailable)});
      await page.goto(base + route);
      await page.getByRole('heading', {name: heading}).waitFor({timeout: 30000});
      assert.equal(await page.locator('input[type=password]').count(), 0, 'no password field on the interest page');
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
    await page.getByRole('heading', {name: 'Sign-ups are coming soon'}).waitFor();
    assert.equal(await page.getByLabel('Full name').inputValue(), 'Asha Mehta');
    assert.equal(await page.getByLabel('Work email').inputValue(), 'asha.mehta@example.com');
    await page.getByRole('checkbox').check();
    await page.getByRole('button', {name: 'Keep me posted'}).click();
    await page.getByRole('heading', {name: 'Thank you, Asha.'}).waitFor();
    assert(captured, 'interest request was sent');
    assert.equal(captured.source, 'signup');
    assert.equal(captured.consent, true);
    const serialized = JSON.stringify(captured);
    assert(!/password/i.test(serialized) && !serialized.includes(password), 'no password in the interest payload');
    await context.close();
    return `payload fields: ${Object.keys(captured).join(', ')}`;
  });

  await step('storage failure: an honest error, no thank-you', async () => {
    const {context, page} = await open({
      '**/api/v1/health': request => request.fulfill(unavailable),
      '**/api/interest': request => request.fulfill({status: 503, contentType: 'application/json', body: JSON.stringify({stored: false, error: 'We could not save your details right now. Please try again a little later.'})}),
    });
    await page.goto(`${base}/signup`);
    await page.getByLabel('Full name').fill('Asha Mehta');
    await page.getByLabel('Work email').fill('asha.mehta@example.com');
    await page.getByRole('checkbox').check();
    await page.getByRole('button', {name: 'Keep me posted'}).click();
    await page.getByRole('alert').filter({hasText: 'could not save your details'}).waitFor();
    assert.equal(await page.getByRole('heading', {name: /Thank you/}).count(), 0);
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
