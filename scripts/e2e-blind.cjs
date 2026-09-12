#!/usr/bin/env node
/* Secondary browser integration evidence, separate from the frozen first 50.
 * Run only AFTER first-run.json is complete and the model slot is granted:
 *   $env:AIDA_BLIND_BROWSER_SLOT='granted'; node scripts/e2e-blind.cjs
 * This uploads a new live source, leaves it configured, and never substitutes
 * oracle rows/model responses. Structured checks use the visible UI builder.
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createHash} = require('node:crypto');
const {createRequire} = require('node:module');
const root = path.resolve(__dirname, '..');
const {chromium} = createRequire(path.join(root, 'frontend/package.json'))('playwright');
const base = process.env.AIDA_BASE_URL || 'http://127.0.0.1:3000';
const fixture = path.join(root, 'fixtures/blind_logistics');
const primaryFile = path.resolve(root, process.env.AIDA_BLIND_PRIMARY_REPORT || 'artifacts/blind/first-run-primary.json');
const finalFile = path.resolve(root, process.env.AIDA_BLIND_FINAL_REPORT || 'artifacts/blind/first-run.json');
const protocolFile = path.join(root, 'docs/evidence/blind/protocol-seal.json');
const selectedIds = ['BL01', 'BL08', 'BL17', 'BL29', 'BL37', 'BL42'];
const structuredIds = ['BL08', 'BL17', 'BL10'];
const readJson = file => JSON.parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, ''));
const digest = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const relative = file => path.relative(root, file).replaceAll('\\', '/');
const stable = value => JSON.stringify(value && typeof value === 'object' && !Array.isArray(value) ? Object.fromEntries(Object.keys(value).sort().map(key => [key, JSON.parse(stable(value[key]))])) : Array.isArray(value) ? value.map(item => JSON.parse(stable(item))) : value);

function equivalent(actual, expected) {
  if (typeof actual === 'number' && typeof expected === 'number') return Number.isFinite(actual) && Number.isFinite(expected) && Math.abs(actual - expected) <= Math.max(1e-8, 1e-9 * Math.max(Math.abs(actual), Math.abs(expected)));
  if (actual === null || expected === null || actual === undefined || expected === undefined) return actual === expected;
  if (Array.isArray(actual) || Array.isArray(expected)) return Array.isArray(actual) && Array.isArray(expected) && actual.length === expected.length && actual.every((value, i) => equivalent(value, expected[i]));
  if (typeof actual === 'object' && typeof expected === 'object') return stable(Object.keys(actual).sort()) === stable(Object.keys(expected).sort()) && Object.keys(actual).every(key => equivalent(actual[key], expected[key]));
  return typeof actual === typeof expected && actual === expected;
}
function equalRows(actual, expected, ordered) {
  if (ordered || !Array.isArray(actual) || !Array.isArray(expected)) return equivalent(actual, expected);
  if (actual.length !== expected.length) return false;
  const remaining = [...expected];
  return actual.every(row => {const at = remaining.findIndex(item => equivalent(row, item)); if (at < 0) return false; remaining.splice(at, 1); return true;});
}
function semanticPlan(plan, ignoreSort) {
  if (!plan || typeof plan !== 'object') return plan ?? null;
  const normalized = JSON.parse(JSON.stringify(plan));
  if (ignoreSort) delete normalized.sort;
  for (const key of ['metrics', 'dimensions']) normalized[key] = [...(normalized[key] || [])].sort();
  const predicates = items => [...new Map((items || []).map(original => {const item = {...original}; if (item.op === 'in') item.value = [...new Set(item.value)].sort((a, b) => stable(a).localeCompare(stable(b))); return [stable(item), item];})).entries()].sort(([a], [b]) => a.localeCompare(b)).map(([, item]) => item);
  for (const key of ['filters', 'having']) normalized[key] = predicates(normalized[key]);
  if (normalized.exists) normalized.exists.filters = predicates(normalized.exists.filters);
  return normalized;
}
function lineageCheck(actual = {}, required) {
  if (!required) return {passed: true};
  const columns = actual.columns || [], joins = actual.joins || [];
  const missing_tables = (required.tables || []).filter(table => !(actual.tables || []).includes(table));
  const missing_columns = (required.columns || []).filter(expected => !columns.some(item => typeof expected === 'string' ? `${item.table}.${item.column}` === expected : Object.entries(expected).every(([key, value]) => equivalent(item[key], value))));
  const forbidden_measures = columns.filter(item => item.role === 'measure' && (required.forbidden_measure_columns || []).includes(`${item.table}.${item.column}`));
  const missing_joins = (required.joins || []).filter(expected => !joins.some(item => Object.entries(expected).every(([key, value]) => equivalent(item[key], value))));
  return {passed: !missing_tables.length && !missing_columns.length && !forbidden_measures.length && !missing_joins.length, missing_tables, missing_columns, forbidden_measures, missing_joins};
}
function classify(test, result, expectedRows, status) {
  const flags = {exact_plan: equivalent(result.plan ?? null, test.plan), semantic_plan: equivalent(semanticPlan(result.plan, !test.order_sensitive), semanticPlan(test.plan, !test.order_sensitive)), rows_match: equalRows(result.data, expectedRows, test.order_sensitive), ordered_rows_match: equivalent(result.data, expectedRows), lineage: lineageCheck(result.lineage, test.expected_lineage)};
  let category;
  if (status !== 200) category = 'http_failure';
  else if (result.success) category = test.expected === 'refusal' ? 'unsafe_acceptance' : !flags.semantic_plan ? 'wrong_accepted_meaning' : !flags.rows_match ? 'execution_mismatch' : 'correct_answer';
  else if (result.error_type === 'model_unavailable') category = 'availability_failure';
  else if (result.error_type === 'clarification_required' && !result.sql && Array.isArray(result.data) && result.data.length === 0) category = test.expected === 'refusal' ? 'correct_refusal' : 'false_refusal';
  else category = 'execution_failure';
  return {category, passed: ['correct_answer', 'correct_refusal'].includes(category), ...flags};
}

async function main() {
  assert.equal(process.env.AIDA_BLIND_BROWSER_SLOT, 'granted', 'The root agent must finish baseline/repeats and grant the model slot before this harness runs.');
  const primary = readJson(primaryFile), final = readJson(finalFile), protocol = readJson(protocolFile);
  assert.equal(final.status, 'complete', 'Wait until baseline and repeat evaluation finishes.');
  assert.equal(primary.cases.length, 50, 'Require the complete original first 50.');
  assert.equal(final.first_pass_artifact.sha256, digest(primaryFile), 'Original first-pass report hash does not match its seal.');
  assert.equal(final.first_pass_artifact.file, path.basename(primaryFile));
  const evidenceSeals = {[relative(primaryFile)]: digest(primaryFile), [relative(finalFile)]: digest(finalFile), [relative(protocolFile)]: digest(protocolFile), ...primary.sealed_files, ...protocol.files};
  const verifySeals = () => {for (const [file, hash] of Object.entries(evidenceSeals)) assert.equal(digest(path.resolve(root, file)), hash, `Frozen evidence changed: ${file}`);};
  verifySeals();
  const cases = readJson(path.join(fixture, 'questions.json')), catalog = readJson(path.join(fixture, 'catalog.json'));
  const byId = new Map(cases.map(test => [test.id, test]));
  const firstById = new Map(primary.cases.map(test => [test.id, test]));
  for (const id of [...selectedIds, ...structuredIds]) {assert(byId.has(id) && firstById.has(id)); assert.equal(byId.get(id).question, firstById.get(id).question);}
  const runId = new Date().toISOString().replaceAll(':', '-').replaceAll('.', '-');
  const artifacts = path.resolve(root, process.env.AIDA_BLIND_BROWSER_OUTPUT || `artifacts/e2e-blind/${runId}`);
  assert(artifacts.startsWith(path.join(root, 'artifacts', 'e2e-blind') + path.sep), 'Write browser evidence only inside artifacts/e2e-blind/<new-run>.');
  assert(!fs.existsSync(artifacts), 'Preserve earlier browser evidence; choose a new output directory.');
  fs.mkdirSync(artifacts, {recursive: true});
  const report = {started_at: new Date().toISOString(), scope: 'Secondary browser integration; never replaces or changes the original 50-case score.', method: 'Actual upload/configure controls, natural-language controls and visible structured builder; actual network responses only. No response mocks or injected expected rows.', base_url: base, selected_nl_ids: selectedIds, structured_only_ids: structuredIds, provenance: {primary_report: relative(primaryFile), primary_report_sha256: digest(primaryFile), final_report: relative(finalFile), final_report_sha256: digest(finalFile), protocol: relative(protocolFile), protocol_sha256: digest(protocolFile), browser_script_sha256: digest(__filename), sealed_files: evidenceSeals}, cases: [], structured_cases: [], steps: [], query_requests: [], browser_errors: [], external_requests: []};
  const reportFile = path.join(artifacts, 'report.json');
  const save = () => fs.writeFileSync(reportFile, JSON.stringify(report, null, 2));
  save();
  const browser = await chromium.launch({headless: true, ...(process.platform === 'win32' ? {channel: process.env.AIDA_BROWSER_CHANNEL || 'msedge'} : {})});
  let page, sourceId, phase = 'startup';
  const pendingCapture = new Set();
  const screenshot = async name => {const file = path.join(artifacts, name); await page.screenshot({path: file, fullPage: true}); return relative(file);};
  async function step(name, action, required = false) {
    const started = Date.now();
    try {const detail = await action(); report.steps.push({name, status: 'passed', duration_ms: Date.now() - started, ...detail}); process.stdout.write(`PASS ${name}\n`); return detail;}
    catch (error) {report.steps.push({name, status: 'failed', duration_ms: Date.now() - started, error: error.message}); process.stderr.write(`FAIL ${name}: ${error.message}\n`); if (required) throw error; return null;}
    finally {save();}
  }
  async function queryResponse(action, question, waitForExplorer = true) {
    const pending = page.waitForResponse(res => {if (!res.url().endsWith('/api/v1/query') || res.request().method() !== 'POST') return false; const body = res.request().postDataJSON(); return body?.source_id === sourceId && (question === undefined ? !!body.plan && !body.question : body.question === question);}, {timeout: 150000});
    await action();
    const res = await pending;
    const result = await res.json();
    if (waitForExplorer) await page.getByTestId(result.success ? 'result-panel' : 'query-error').waitFor();
    return {http_status: res.status(), request: res.request().postDataJSON(), result};
  }
  async function explorerSource() {
    await page.getByRole('button', {name: /^Explorer/}).click();
    const select = page.getByLabel('Data source', {exact: true});
    if (await select.inputValue() !== sourceId) await select.selectOption(sourceId);
    await page.waitForFunction(id => {const select = document.querySelector('select[aria-label="Data source"]'); return select?.value === id && !select.disabled;}, sourceId);
    await page.getByTestId('result-panel').waitFor();
  }
  async function openDetails(text) {const summary = page.locator('summary').filter({hasText: text}); if (!(await summary.evaluate(node => node.parentElement.open))) await summary.click();}
  async function setValue(label, value) {const control = page.getByLabel(label, {exact: true}); if (await control.evaluate(node => node.tagName) === 'SELECT') await control.selectOption(String(value)); else await control.fill(Array.isArray(value) ? value.join(', ') : String(value));}
  async function build(test) {
    const plan = test.plan;
    await page.getByRole('button', {name: /^Explorer/}).click();
    await page.getByRole('tab', {name: 'Visual builder', exact: true}).click();
    await page.getByTestId('relational-builder').waitFor();
    await page.getByRole('button', {name: 'Reset filters', exact: true}).click();
    await page.getByLabel(`Measure ${catalog.metrics.find(metric => metric.id === plan.metrics[0]).label}`, {exact: true}).check();
    for (const metric of catalog.metrics) if (metric.id !== plan.metrics[0] && await page.getByLabel(`Measure ${metric.label}`, {exact: true}).isChecked()) await page.getByLabel(`Measure ${metric.label}`, {exact: true}).uncheck();
    for (const id of plan.metrics.slice(1)) await page.getByLabel(`Measure ${catalog.metrics.find(metric => metric.id === id).label}`, {exact: true}).check();
    await setValue('Group by', '');
    if (plan.dimensions.length) {await setValue('Group by', plan.dimensions[0]); if (plan.dimensions[1]) await setValue('Then group by', plan.dimensions[1]);}
    await setValue('Record population', plan.population);
    if (plan.population === 'all') await setValue('Combine sources', plan.set_operation);
    await setValue('From', plan.date_from || ''); await setValue('To', plan.date_to || '');
    if (plan.filters.length) {
      await openDetails('Filter individual records');
      for (const [index, filter] of plan.filters.entries()) {await page.getByRole('button', {name: 'Add filter', exact: true}).click(); await setValue(`Filter ${index + 1} field`, filter.field); await setValue(`Filter ${index + 1} operator`, filter.op); await setValue(`Filter ${index + 1} value`, filter.value);}
    }
    if (plan.having.length || plan.comparison) {
      await openDetails('Filter aggregate results');
      for (const [index, filter] of plan.having.entries()) {await page.getByRole('button', {name: 'Add aggregate filter', exact: true}).click(); await setValue(`Aggregate filter ${index + 1} measure`, filter.metric); await setValue(`Aggregate filter ${index + 1} operator`, filter.op); await setValue(`Aggregate filter ${index + 1} value`, filter.value);}
      if (plan.comparison) await setValue('Above average comparison', plan.comparison.metric);
    }
    assert.equal(plan.exists, null, 'This bounded browser matrix contains no structured EXISTS setup.');
    await setValue('Sort field', plan.sort.field); await setValue('Sort direction', plan.sort.direction); await setValue('Result limit', plan.limit);
    return queryResponse(() => page.getByRole('button', {name: 'Run analysis', exact: true}).click());
  }
  async function chooseChart(type, result) {
    await page.getByRole('tab', {name: 'Chart', exact: true}).click();
    const button = page.getByRole('button', {name: `${type[0].toUpperCase()}${type.slice(1)} chart`, exact: true});
    assert(await button.count(), `${type} is not offered for this actual result`);
    await button.click();
    await page.getByTestId('result-panel').locator(`[data-testid="query-chart"][data-chart-type="${type}"]`).waitFor();
    if (type === 'scatter') assert.equal(await page.getByTestId('scatter-point').count(), result.data.filter(row => result.plan.metrics.every(metric => typeof row[metric] === 'number')).length);
    if (type === 'line' || type === 'area') assert.equal(await page.getByTestId('chart-point').count(), result.data.filter(row => row[result.plan.metrics[0]] !== null).length);
    return screenshot(`structured-${phase}-${type}.png`);
  }
  try {
    const context = await browser.newContext({viewport: {width: 1440, height: 1050}, reducedMotion: 'reduce', acceptDownloads: true});
    context.on('page', tab => tab.on('pageerror', error => report.browser_errors.push(error.message)));
    await context.route('**/*', async route => {const url = new URL(route.request().url()); if (['http:', 'https:'].includes(url.protocol) && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) {report.external_requests.push(url.origin + url.pathname); await route.abort();} else await route.continue();});
    page = await context.newPage(); page.setDefaultTimeout(30000);
    page.on('response', res => {
      if (!res.url().endsWith('/api/v1/query') || res.request().method() !== 'POST') return;
      const capturedPhase = phase;
      const capture = (async () => {try {const result = await res.json(); report.query_requests.push({phase: capturedPhase, http_status: res.status(), request: res.request().postDataJSON(), success: result.success, meta: result.meta, error_type: result.error_type});} catch (error) {report.query_requests.push({phase: capturedPhase, capture_error: error.message});} finally {save();}})();
      pendingCapture.add(capture); capture.finally(() => pendingCapture.delete(capture));
    });
    await page.goto(base, {waitUntil: 'domcontentloaded'});
    await page.getByTestId('result-panel').waitFor();
    await step('Upload and configure the frozen logistics database through the UI', async () => {
      phase = 'upload-configure';
      await page.getByRole('button', {name: 'Data catalog', exact: true}).click();
      const upload = page.waitForResponse(res => res.url().endsWith('/api/v1/sources') && res.request().method() === 'POST');
      await page.getByLabel('Upload SQLite database', {exact: true}).setInputFiles(path.join(fixture, 'logistics.sqlite'));
      const uploaded = await upload; assert.equal(uploaded.status(), 200);
      // Edge may evict the inspector body for a large binary upload. Verify the
      // actual rendered inspection and configure response without re-uploading.
      report.upload = {http_status: uploaded.status(), url: uploaded.url()};
      await page.getByRole('button', {name: 'Relational catalog', exact: true}).click();
      await openDetails('Inspect tables, columns, and declared keys');
      assert.equal(await page.locator('.schema-inspection article').count(), 12);
      report.upload.inspected_table_names = await page.locator('.schema-inspection article h3').allTextContents();
      await page.getByLabel('Source name', {exact: true}).fill(catalog.name);
      await page.getByLabel('Approved relational catalog JSON', {exact: true}).fill(JSON.stringify(catalog, null, 2));
      const configure = page.waitForResponse(res => /\/api\/v1\/sources\/[a-f0-9]{32}\/configure$/.test(res.url()) && res.request().method() === 'POST');
      await page.getByRole('button', {name: 'Save catalog and explore', exact: true}).click();
      const configured = await configure; assert.equal(configured.status(), 200, await configured.text());
      sourceId = configured.url().match(/\/sources\/([a-f0-9]{32})\/configure$/)[1];
      report.source = {id: sourceId, catalog: await configured.json(), retained_in_live_backend: true};
      await page.waitForFunction(id => {const select = document.querySelector('select[aria-label="Data source"]'); return select?.value === id && !select.disabled;}, sourceId);
      await page.getByTestId('result-panel').waitFor();
      return {source_id: sourceId, screenshot: await screenshot('00-uploaded-catalog-preview.png')};
    }, true);
    for (const id of selectedIds) {
      phase = `natural-language-${id}`;
      const test = byId.get(id), original = firstById.get(id), started = Date.now();
      let entry;
      try {
        await page.getByRole('tab', {name: 'Ask a question', exact: true}).click();
        await page.getByRole('textbox', {name: 'Ask a question', exact: true}).fill(test.question);
        const response = await queryResponse(() => page.getByRole('button', {name: 'Run query', exact: true}).click(), test.question);
        entry = {id, question: test.question, expected: test.expected, duration_ms: Date.now() - started, ...response, judgment: classify(test, response.result, original.expected_rows, response.http_status), primary_judgment: original.judgment, primary_result_agrees: equivalent(semanticPlan(response.result.plan, !test.order_sensitive), semanticPlan(original.result.plan, !test.order_sensitive)) && equalRows(response.result.data, original.result.data, test.order_sensitive) && response.result.success === original.result.success, screenshot: await screenshot(`nl-${id}.png`)};
      } catch (error) {entry = {id, question: test.question, expected: test.expected, duration_ms: Date.now() - started, judgment: {passed: false, category: 'browser_integration_failure'}, error: error.message};}
      report.cases.push(entry); save(); process.stdout.write(`NL ${id}: ${entry.judgment.category}\n`);
    }
    for (const id of structuredIds) {
      phase = id;
      const test = byId.get(id), original = firstById.get(id);
      const prepared = await step(`Structured builder ${id}: actual API, plan and oracle rows`, async () => {
        const response = await build(test), judgment = classify(test, response.result, original.expected_rows, response.http_status);
        const entry = {id, mode: 'structured_builder_only', question_not_submitted: true, ...response, judgment}; report.structured_cases.push(entry); save();
        assert.equal(response.result.meta.model_calls, 0); assert(judgment.passed, JSON.stringify(judgment)); assert(judgment.lineage.passed);
        return {id, result: response.result};
      });
      if (!prepared) continue;
      const result = prepared.result;
      await step(`${id}: visible table, CSV and SQL lineage match the actual response`, async () => {
        await page.getByRole('tab', {name: 'Table', exact: true}).click();
        const table = page.getByTestId('result-panel').getByRole('table'); await table.waitFor();
        assert.equal(await table.locator('tbody tr').count(), result.data.length);
        assert.equal(await table.locator('thead th').count(), result.columns.length);
        const pending = page.waitForEvent('download'); await page.getByRole('button', {name: 'Export CSV', exact: true}).click(); const download = await pending;
        const csvFile = path.join(artifacts, `structured-${id}.csv`); await download.saveAs(csvFile); const csv = fs.readFileSync(csvFile, 'utf8');
        assert.equal(csv.split(/\r?\n/).filter(Boolean).length, result.data.length + 1); for (const column of result.columns) assert(csv.split(/\r?\n/)[0].includes(`"${column}"`));
        const tableShot = await screenshot(`structured-${id}-table.png`);
        await page.getByRole('tab', {name: 'SQL & trust', exact: true}).click(); const lineage = page.locator('.sql-content').getByTestId('query-lineage'); await lineage.waitFor(); const text = await lineage.innerText();
        for (const table of result.lineage.tables) assert(text.includes(table), `Missing visible lineage table ${table}`);
        return {csv: relative(csvFile), table_screenshot: tableShot, lineage_screenshot: await screenshot(`structured-${id}-lineage.png`)};
      });
      const types = id === 'BL08' ? ['bar', 'scatter'] : id === 'BL17' ? ['bar', 'donut'] : ['line', 'area'];
      for (const type of types) await step(`${id}: ${type} chart renders actual structured results`, async () => ({screenshot: await chooseChart(type, result)}));
      if (id === 'BL08') await step('Save a verified structured chart, reload and refresh dashboard without model calls', async () => {
        phase = 'dashboard';
        await page.getByRole('button', {name: 'Save to dashboard', exact: true}).click();
        await page.getByRole('button', {name: /^Dashboards/}).click(); await page.getByTestId('dashboard-card').waitFor();
        const stored = await page.evaluate(() => JSON.parse(localStorage.getItem('aida:dashboard:v2')));
        assert.equal(stored.length, 1); assert.equal(stored[0].source_id, sourceId); assert(equivalent(stored[0].plan, result.plan));
        assert(!Object.keys(stored[0]).some(key => ['data', 'rows', 'results'].includes(key)));
        await page.reload({waitUntil: 'domcontentloaded'}); await page.getByTestId('result-panel').waitFor(); await explorerSource();
        await page.getByRole('button', {name: /^Dashboards/}).click(); const card = page.getByTestId('dashboard-card'); await card.waitFor();
        const response = await queryResponse(() => card.getByRole('button', {name: 'Refresh card', exact: true}).click(), undefined, false);
        assert.equal(response.result.success, true); assert.equal(response.result.meta.model_calls, 0); assert(equalRows(response.result.data, result.data, true));
        await card.locator(`[data-testid="query-chart"][data-chart-type="${stored[0].chartType}"]`).waitFor();
        report.dashboard = {stored_definition: stored[0], refresh_response: response, screenshot: await screenshot('dashboard-refreshed.png')};
        await context.storageState({path: path.join(artifacts, 'browser-storage-state.json')});
        await page.getByRole('button', {name: /^Explorer/}).click();
        return {model_calls: response.result.meta.model_calls, source_id: sourceId};
      });
    }
    await step('No uncaught browser errors or external browser requests', async () => {assert.deepEqual(report.browser_errors, []); assert.deepEqual(report.external_requests, []);});
    report.status = report.steps.some(item => item.status === 'failed') || report.cases.some(item => !item.judgment.passed) ? 'completed_with_failures' : 'passed';
  } catch (error) {
    report.status = 'failed'; report.error = error.stack; if (page) await screenshot('failure.png').catch(() => {}); process.stderr.write(`${error.stack}\n`);
  } finally {
    await Promise.allSettled([...pendingCapture]);
    try {verifySeals(); report.frozen_evidence_unchanged = true;} catch (error) {report.frozen_evidence_unchanged = false; report.seal_error = error.message; report.status = 'failed';}
    report.finished_at = new Date().toISOString();
    report.secondary_summary = {natural_language_cases: report.cases.length, natural_language_correct: report.cases.filter(entry => entry.judgment.passed).length, categories: Object.fromEntries([...new Set(report.cases.map(entry => entry.judgment.category))].map(category => [category, report.cases.filter(entry => entry.judgment.category === category).length])), structured_cases: report.structured_cases.length, structured_correct: report.structured_cases.filter(entry => entry.judgment.passed).length, recorded_model_calls: report.query_requests.reduce((sum, entry) => sum + Number(entry.meta?.model_calls || 0), 0), first_50_score_recomputed: false};
    save(); await browser.close(); process.stdout.write(`Browser report: ${relative(reportFile)}\n`); if (report.status !== 'passed') process.exitCode = 1;
  }
}
main().catch(error => {process.stderr.write(`${error.stack}\n`); process.exitCode = 1;});
