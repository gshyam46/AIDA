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
  page_errors: [], console_errors: [], failed_requests: [], expected_script_blocks: [], font_requests: [], mutation_requests: [],
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
    const observePage = (target, {scriptsDisabled = false} = {}) => {
      target.on('pageerror', error => report.page_errors.push({url: target.url(), message: error.message}));
      target.on('console', message => {if (message.type() === 'error') report.console_errors.push({url: target.url(), message: message.text()});});
      target.on('requestfailed', request => {
        const failure = {url: request.url(), failure: request.failure()?.errorText, resource_type: request.resourceType()};
        const url = new URL(failure.url);
        // Chromium can report the intentionally disabled hydration script as a CSP block.
        // Record this separately only for same-origin Next.js scripts in the explicit no-JS context.
        if (scriptsDisabled && failure.failure === 'csp' && failure.resource_type === 'script' && url.origin === new URL(base).origin && /^\/_next\/static\/chunks\/[^?#]+\.js$/.test(url.pathname)) {
          report.expected_script_blocks.push({...failure, reason: 'JavaScript disabled for static-content verification'});
        } else report.failed_requests.push(failure);
      });
      target.on('response', response => {
        if (response.request().resourceType() === 'font') report.font_requests.push({url: response.url(), status: response.status()});
      });
    };
    const preventMutations = target => target.route('**/api/v1/**', route => {
      const request = route.request();
      if (!['GET', 'HEAD', 'OPTIONS'].includes(request.method())) {
        report.mutation_requests.push({url: request.url(), method: request.method()});
        return route.abort('blockedbyclient');
      }
      return route.continue();
    });
    observePage(page);
    await preventMutations(context);

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

    const demoState = target => target.evaluate(() => ({
      question: document.querySelector('.console-question > span[aria-hidden="true"]').textContent,
      stage: document.querySelector('.stage-rail [aria-selected="true"]').textContent,
    }));
    const waitForPlayback = (target, playing) => target.waitForFunction(expected => document.querySelector('.demo-frame')?.dataset.demoPlaying === String(expected), playing);
    // Long enough to span a complete walkthrough stage, rather than merely checking a CSS class.
    const playbackWindow = 5000;
    await step('walkthrough autoplay pauses outside the viewport and in a hidden document', async () => {
      const motionContext = await browser.newContext({viewport: {width: 1440, height: 1000}, reducedMotion: 'no-preference'});
      await preventMutations(motionContext);
      const motionPage = await motionContext.newPage();
      observePage(motionPage);
      try {
        await motionPage.goto(base, {waitUntil: 'networkidle'});
        await motionPage.locator('.demo-frame').scrollIntoViewIfNeeded();
        await waitForPlayback(motionPage, true);
        const initial = await demoState(motionPage);
        await motionPage.waitForFunction(previous => document.querySelector('.console-question > span[aria-hidden="true"]').textContent !== previous.question || document.querySelector('.stage-rail [aria-selected="true"]').textContent !== previous.stage, initial, {timeout: 8000});

        await motionPage.locator('.landing-footer').scrollIntoViewIfNeeded();
        await waitForPlayback(motionPage, false);
        const offscreen = await demoState(motionPage);
        await motionPage.waitForTimeout(playbackWindow);
        assert.deepEqual(await demoState(motionPage), offscreen, 'Offscreen content must stop typing and changing stages');

        await motionPage.locator('.demo-frame').scrollIntoViewIfNeeded();
        await waitForPlayback(motionPage, true);
        // Simulate the browser visibility signal; actual tab focus differs across headless engines.
        await motionPage.evaluate(() => {
          Object.defineProperty(document, 'visibilityState', {configurable: true, value: 'hidden'});
          Object.defineProperty(document, 'hidden', {configurable: true, value: true});
          document.dispatchEvent(new Event('visibilitychange'));
        });
        await waitForPlayback(motionPage, false);
        const hidden = await demoState(motionPage);
        await motionPage.waitForTimeout(playbackWindow);
        assert.deepEqual(await demoState(motionPage), hidden, 'A hidden document must not advance the walkthrough');
        await motionPage.evaluate(() => {
          delete document.visibilityState;
          delete document.hidden;
          document.dispatchEvent(new Event('visibilitychange'));
        });
        await waitForPlayback(motionPage, true);
        await motionPage.waitForFunction(previous => document.querySelector('.console-question > span[aria-hidden="true"]').textContent !== previous.question || document.querySelector('.stage-rail [aria-selected="true"]').textContent !== previous.stage, hidden, {timeout: 8000});
        await motionPage.screenshot({path: path.join(artifacts, 'walkthrough-normal-motion.png'), fullPage: false});
        return {offscreen_pauses: true, visibility_signal_pauses: true, resumes_when_visible: true};
      } finally {await motionContext.close();}
    });

    await step('walkthrough pause persists across manual example and stage selection', async () => {
      const motionContext = await browser.newContext({viewport: {width: 1440, height: 1000}, reducedMotion: 'no-preference'});
      await preventMutations(motionContext);
      const motionPage = await motionContext.newPage();
      observePage(motionPage);
      try {
        await motionPage.goto(base, {waitUntil: 'networkidle'});
        await motionPage.locator('.demo-frame').scrollIntoViewIfNeeded();
        await waitForPlayback(motionPage, true);
        await motionPage.getByRole('button', {name: 'Pause walkthrough', exact: true}).click();
        await waitForPlayback(motionPage, false);
        await motionPage.getByRole('button', {name: 'Calculated ratio', exact: true}).click();
        await motionPage.getByRole('tab', {name: 'Answer', exact: true}).click();
        await motionPage.getByRole('tabpanel').getByText('49.64', {exact: true}).waitFor();
        const selected = await demoState(motionPage);
        await motionPage.waitForTimeout(playbackWindow);
        assert.deepEqual(await demoState(motionPage), selected, 'User-selected content must remain stable while paused');
        await waitForPlayback(motionPage, false);
        await motionPage.getByRole('button', {name: 'Play walkthrough', exact: true}).click();
        await waitForPlayback(motionPage, true);
      } finally {await motionContext.close();}
    });

    await step('reduced motion shows the complete question and keeps manual controls usable', async () => {
      await visit('/');
      await page.locator('.demo-frame').scrollIntoViewIfNeeded();
      await waitForPlayback(page, false);
      assert.equal((await demoState(page)).question.trim(), 'Top 3 categories by revenue in Q3 2025');
      const initial = await demoState(page);
      await page.waitForTimeout(playbackWindow);
      assert.deepEqual(await demoState(page), initial, 'Reduced motion must disable automatic typing and stage changes');
      assert.equal(await page.evaluate(() => document.getAnimations().filter(animation => animation.playState === 'running' && animation.effect?.getTiming().iterations === Infinity).length), 0, 'Reduced motion must not leave decorative loops running');
      await page.getByRole('button', {name: 'Attack attempt', exact: true}).click();
      await page.getByRole('tab', {name: 'Answer', exact: true}).click();
      await page.getByRole('tabpanel').getByText(/No SQL ran/).waitFor();
      await waitForPlayback(page, false);
      return {autoplay: false, manual_controls: 'usable'};
    });

    await step('landing content remains visible and readable without JavaScript', async () => {
      const staticContext = await browser.newContext({viewport: {width: 390, height: 844}, javaScriptEnabled: false, reducedMotion: 'no-preference'});
      await preventMutations(staticContext);
      const staticPage = await staticContext.newPage();
      observePage(staticPage, {scriptsDisabled: true});
      try {
        const response = await staticPage.goto(base, {waitUntil: 'networkidle'});
        assert.equal(response.status(), 200);
        const detail = await staticPage.evaluate(() => ({
          question: document.querySelector('.console-question > span[aria-hidden="true"]').textContent.trim(),
          revealed: [...document.querySelectorAll('.reveal')].map(element => ({text: element.textContent.trim().slice(0, 70), opacity: Number(getComputedStyle(element).opacity), visibility: getComputedStyle(element).visibility, height: element.getBoundingClientRect().height})),
          viewport: window.innerWidth, width: document.documentElement.scrollWidth,
        }));
        assert(detail.revealed.length > 0);
        assert(detail.revealed.every(element => element.opacity > 0 && element.visibility === 'visible' && element.height > 0), `Content was hidden without JavaScript: ${JSON.stringify(detail.revealed.filter(element => element.opacity === 0 || element.visibility !== 'visible' || element.height === 0))}`);
        assert.equal(detail.question, 'Top 3 categories by revenue in Q3 2025');
        assert(detail.width <= detail.viewport + 1, 'Static landing fits the mobile viewport');
        assert.equal(await staticPage.locator('.landing-actions').getByRole('link', {name: /Sign up/}).getAttribute('href'), '/signup');
        await staticPage.screenshot({path: path.join(artifacts, 'landing-no-javascript-390.png'), fullPage: false});
        return {visible_reveal_elements: detail.revealed.length, complete_question: true};
      } finally {await staticContext.close();}
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
          heading_weight: getComputedStyle(document.querySelector('h1, h2')).fontWeight,
          hero_accent_font: document.querySelector('.hero h1 em') ? getComputedStyle(document.querySelector('.hero h1 em')).fontFamily : null,
          hero_accent_style: document.querySelector('.hero h1 em') ? getComputedStyle(document.querySelector('.hero h1 em')).fontStyle : null,
          accent_fonts: [...document.querySelectorAll('.hero h1 em, .auth-story h1 em, .final-cta h2 em, .bench-facts strong')].map(element => getComputedStyle(element).fontFamily),
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
        assert.match(detail.heading_font, /Manrope/);
        assert.equal(Number(detail.heading_weight), 650, 'Primary headings use the shared Manrope 650 style');
        if (route === '/') {
          assert.match(detail.hero_accent_font, /Newsreader/);
          assert.equal(detail.hero_accent_style, 'italic');
        }
        const requiredFonts = ['Manrope', ...(detail.accent_fonts.some(font => /Newsreader/.test(font)) ? ['Newsreader'] : [])];
        for (const family of requiredFonts) assert(detail.font_faces.some(font => font.family.replace(/["']/g, '') === family && font.status === 'loaded'), `${family} must actually load when used`);
        return {route, width, document_width: detail.document_width, fonts: requiredFonts, heading_weight: detail.heading_weight};
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
