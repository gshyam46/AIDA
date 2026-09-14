#!/usr/bin/env node
/* Visual and interaction regression against the running production frontend.
 * Run: node scripts/e2e-theme.cjs
 * Uses public pages only. Does not create accounts, submit questions or call a model.
 * Screenshots and the JSON report are written under ignored artifacts/e2e-theme/.
 */
'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const {chromium} = createRequire(path.join(__dirname, '../frontend/package.json'))('playwright');

const base = process.env.AIDA_BASE_URL || 'http://127.0.0.1:3000';
const artifacts = path.resolve(__dirname, '../artifacts/e2e-theme');
const widths = [1440, 1024, 768, 390, 320];
const routes = ['/', '/signup', '/login', '/benchmarks'];
const report = {
  started_at: new Date().toISOString(), base_url: base, steps: [], routes: [],
  page_errors: [], console_errors: [], failed_requests: [], font_requests: [], mutation_requests: [],
};
fs.mkdirSync(artifacts, {recursive: true});
const save = () => fs.writeFileSync(path.join(artifacts, 'report.json'), JSON.stringify(report, null, 2));

async function step(name, action) {
  const started = Date.now();
  try {
    const detail = await action();
    report.steps.push({name, status: 'passed', duration_ms: Date.now() - started, ...detail});
    process.stdout.write(`PASS ${name}\n`);
  } catch (error) {
    report.steps.push({name, status: 'failed', duration_ms: Date.now() - started, error: error.message});
    process.stdout.write(`FAIL ${name}: ${error.message}\n`);
  } finally {save();}
}

async function launch() {
  try {return await chromium.launch();}
  catch {return chromium.launch({channel: 'msedge'});}
}

(async () => {
  let browser;
  try {
    browser = await launch();
    const context = await browser.newContext({viewport: {width: 1440, height: 1000}, reducedMotion: 'reduce'});
    const page = await context.newPage();
    page.setDefaultTimeout(12000);
    page.setDefaultNavigationTimeout(45000);
    page.on('pageerror', error => report.page_errors.push({url: page.url(), message: error.message}));
    page.on('console', message => {if (message.type() === 'error') report.console_errors.push({url: page.url(), message: message.text()});});
    page.on('requestfailed', request => report.failed_requests.push({url: request.url(), failure: request.failure()?.errorText}));
    page.on('response', response => {
      if (response.request().resourceType() === 'font') report.font_requests.push({url: response.url(), status: response.status()});
    });
    await context.route('**/api/v1/**', route => {
      const request = route.request();
      if (!['GET', 'HEAD', 'OPTIONS'].includes(request.method())) {
        report.mutation_requests.push({url: request.url(), method: request.method()});
        return route.abort('blockedbyclient');
      }
      return route.continue();
    });

    async function visit(route) {
      const response = await page.goto(`${base}${route}`, {waitUntil: 'networkidle'});
      assert(response && response.status() === 200, `Expected 200 for ${route}; received ${response?.status()}`);
      await page.locator('h1, h2').first().waitFor();
      await page.evaluate(() => document.fonts.ready);
      assert.equal(new URL(page.url()).pathname, route, `Unexpected redirect away from ${route}`);
    }

    const screenshot = name => page.screenshot({path: path.join(artifacts, `${name}.png`), fullPage: false, animations: 'disabled'});

    await step('interactive question examples and all five pipeline tabs', async () => {
      await visit('/');
      await page.getByRole('button', {name: 'Top categories', exact: true}).click();
      const headings = ['Guard and budget', 'LLM resolves your words', 'Structured plan', 'Code verifies', 'Answer with lineage'];
      for (const [index, name] of ['Screen', 'Resolve', 'Plan', 'Validate', 'Answer'].entries()) {
        const tab = page.getByRole('tab', {name, exact: true});
        await tab.click();
        assert.equal(await tab.getAttribute('aria-selected'), 'true');
        await page.getByRole('tabpanel').getByRole('heading', {name: headings[index], exact: true}).waitFor();
      }
      await page.getByRole('tabpanel').getByText('$187,283', {exact: true}).waitFor();
      await page.getByRole('button', {name: 'Calculated ratio', exact: true}).click();
      await page.getByRole('tab', {name: 'Answer', exact: true}).click();
      await page.getByRole('tabpanel').getByText('49.64', {exact: true}).waitFor();
      await page.getByRole('button', {name: 'Attack attempt', exact: true}).click();
      await page.getByRole('tab', {name: 'Answer', exact: true}).click();
      await page.getByRole('tabpanel').getByText(/No SQL ran/).waitFor();
      return {examples: 3, tabs: 5};
    });

    await step('all six security layers work with the keyboard', async () => {
      const layers = page.locator('.layer-stack button');
      await layers.first().waitFor();
      assert.equal(await layers.count(), 6);
      const titles = [];
      for (let index = 0; index < 6; index += 1) {
        const button = layers.nth(index);
        const title = (await button.innerText()).replace(/^\d+\s*/, '').trim();
        await button.focus();
        await page.keyboard.press('Enter');
        assert.equal(await button.getAttribute('aria-pressed'), 'true');
        await page.locator('.layer-detail').getByRole('heading', {name: title, exact: true}).waitFor();
        assert((await page.locator('.layer-detail .attack-result').innerText()).trim().length > 20);
        titles.push(title);
      }
      return {layers: titles};
    });

    await step('privacy toggle displays shared and private information', async () => {
      const never = page.getByRole('button', {name: 'Never leaves AIDA', exact: true});
      await never.click();
      assert.equal(await never.getAttribute('aria-pressed'), 'true');
      await page.locator('.sees-grid').getByText('Database rows or query results', {exact: true}).waitFor();
      assert.equal(await page.locator('.sees-grid > div').count(), 6);
      const shared = page.getByRole('button', {name: 'Shared with the model', exact: true});
      await shared.click();
      assert.equal(await shared.getAttribute('aria-pressed'), 'true');
      await page.locator('.sees-grid').getByText('Your question text', {exact: true}).waitFor();
      assert.equal(await page.locator('.sees-grid > div').count(), 6);
    });

    await step('favicon loads from this deployment', async () => {
      const icon = await page.locator('link[rel~="icon"]').first().getAttribute('href');
      assert(icon, 'The page must declare a favicon');
      const url = new URL(icon, base).href;
      assert.equal(new URL(url).origin, new URL(base).origin);
      const response = await context.request.get(url);
      assert.equal(response.status(), 200);
      assert.match(response.headers()['content-type'], /image\//);
      return {url, content_type: response.headers()['content-type']};
    });

    for (const width of widths) {
      await page.setViewportSize({width, height: width <= 390 ? 844 : 1000});
      for (const route of routes) await step(`${route} at ${width}px: fonts, content and horizontal overflow`, async () => {
        await visit(route);
        if (route === '/') {
          await page.getByRole('button', {name: 'Top categories', exact: true}).click();
          await page.getByRole('tab', {name: 'Answer', exact: true}).click();
          await page.evaluate(() => window.scrollTo(0, 0));
        }
        if (route === '/signup') await page.getByRole('heading', {name: 'Create your account', exact: true}).waitFor();
        if (route === '/login') await page.getByRole('heading', {name: 'Sign in', exact: true}).waitFor();
        const detail = await page.evaluate(() => ({
          viewport: window.innerWidth,
          document_width: document.documentElement.scrollWidth,
          body_width: document.body.scrollWidth,
          body_font: getComputedStyle(document.body).fontFamily,
          heading_font: getComputedStyle(document.querySelector('h1, h2')).fontFamily,
          font_faces: Array.from(document.fonts).map(font => ({family: font.family, style: font.style, status: font.status})),
          overflow_candidates: Array.from(document.querySelectorAll('main *, .auth-shell *')).map(element => {
            const rect = element.getBoundingClientRect();
            return {tag: element.tagName, class: typeof element.className === 'string' ? element.className : '', left: rect.left, right: rect.right, width: rect.width};
          }).filter(rect => rect.width > 2 && (rect.left < -1 || rect.right > window.innerWidth + 1)).slice(0, 12),
        }));
        report.routes.push({route, width, ...detail});
        if (width === 1440 || width === 390) {
          const label = route === '/' ? 'landing-answer' : route.slice(1);
          await screenshot(`${label}-${width}`);
        }
        assert(detail.document_width <= width + 1 && detail.body_width <= width + 1,
          `Page overflows ${width}px: document ${detail.document_width}px, body ${detail.body_width}px; ${JSON.stringify(detail.overflow_candidates)}`);
        assert.match(detail.body_font, /Manrope/);
        assert.match(detail.heading_font, /Newsreader/);
        for (const family of ['Manrope', 'Newsreader']) assert(detail.font_faces.some(font => font.family.replace(/["']/g, '') === family && font.status === 'loaded'), `${family} must actually load`);
        return {route, width, document_width: detail.document_width, fonts: ['Manrope', 'Newsreader']};
      });
    }

    await step('no page errors, external font requests or mutating API calls', async () => {
      assert.deepEqual(report.page_errors, []);
      assert.deepEqual(report.mutation_requests, []);
      assert(report.font_requests.length > 0, 'The browser must fetch self-hosted font files');
      for (const font of report.font_requests) {
        assert.equal(new URL(font.url).origin, new URL(base).origin);
        assert.equal(font.status, 200);
      }
      assert.deepEqual(report.failed_requests.filter(request => !request.failure?.includes('ERR_ABORTED')), []);
    });
  } catch (error) {
    report.fatal_error = error.message;
    process.stdout.write(`FAIL ${error.stack || error.message}\n`);
  } finally {
    report.status = !report.fatal_error && report.steps.every(item => item.status === 'passed') ? 'passed' : 'failed';
    report.finished_at = new Date().toISOString();
    save();
    if (browser) await browser.close();
    if (report.status !== 'passed') process.exitCode = 1;
  }
})();
