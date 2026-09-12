'use strict';
// Execute the actual TypeScript helpers without a browser, model, or database.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const ts = require('typescript');
for (const extension of ['.ts', '.tsx']) require.extensions[extension] = (module, filename) => {
  const source = ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
    compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true},
    fileName: filename,
  });
  module._compile(source.outputText, filename);
};
const {relationalChartTypes} = require('../components/RelationalChart.tsx');
const {withRelationalDimensions} = require('../lib/api.ts');
const metrics = [
  {id: 'revenue', label: 'Revenue', additive: true},
  {id: 'units', label: 'Units', additive: true},
  {id: 'average', label: 'Average line value', additive: false},
  {id: 'orders', label: 'Distinct orders', additive: false},
];
const catalog = {metrics};
const base = {version: 2, metrics: ['revenue'], dimensions: ['region'], filters: [], having: [], population: 'primary', set_operation: 'union_all', exists: null, comparison: null, date_from: null, date_to: null, sort: {field: 'revenue', direction: 'desc'}, limit: 100};
const result = (metric, values, dimensions = ['region']) => ({success: true, plan: {...base, metrics: [metric], dimensions}, data: values.map((value, index) => ({[dimensions[0]]: dimensions[0] === 'month' ? `2026-0${index + 1}` : `Group ${index + 1}`, [metric]: value})), meta: {}});
let checks = 0;
function check(name, fn) {fn(); checks++; process.stdout.write(`PASS ${name}\n`);}
check('Additive positive groups offer a part-to-whole chart', () => assert(relationalChartTypes(result('revenue', [200, 300]), catalog).includes('donut')));
check('Group averages cannot become a fabricated total', () => assert(!relationalChartTypes(result('average', [20, 30]), catalog).includes('donut')));
check('Overlapping distinct counts cannot become a fabricated total', () => assert(!relationalChartTypes(result('orders', [7, 8]), catalog).includes('donut')));
check('Missing additivity metadata does not imply summability', () => assert(!relationalChartTypes(result('revenue', [200, 300])).includes('donut')));
check('Negative and missing parts do not offer a donut', () => {for (const values of [[100, -5], [100, null]]) assert(!relationalChartTypes(result('revenue', values), catalog).includes('donut'));});
check('Temporal results with missing measures retain line/area options', () => {const options = relationalChartTypes(result('average', [20, null, 30], ['month']), catalog); assert(options.includes('line')); assert(options.includes('area'));});
check('Scatter requires at least two complete numeric pairs', () => {
  const data = [{region: 'West', revenue: 100, units: 2}, {region: 'East', revenue: 200, units: null}];
  const query = {...result('revenue', []), plan: {...base, metrics: ['revenue', 'units']}, data};
  assert(!relationalChartTypes(query, catalog).includes('scatter'));
  query.data[1].units = 3;
  assert(relationalChartTypes(query, catalog).includes('scatter'));
});
check('Removing all groups clears an inapplicable average comparison', () => {
  const next = withRelationalDimensions({...base, comparison: {kind: 'above_average', metric: 'revenue'}}, []);
  assert.equal(next.comparison, null); assert.deepEqual(next.dimensions, []);
});
check('Removing the sorted grouping resets to a selected measure', () => {
  const next = withRelationalDimensions({...base, dimensions: ['region', 'category'], sort: {field: 'category', direction: 'asc'}}, ['region']);
  assert.deepEqual(next.sort, {field: 'revenue', direction: 'desc'});
});
check('A remaining valid sort and comparison are preserved', () => {
  const comparison = {kind: 'above_average', metric: 'revenue'};
  const next = withRelationalDimensions({...base, dimensions: ['region', 'category'], comparison, sort: {field: 'region', direction: 'asc'}}, ['region']);
  assert.deepEqual(next.sort, {field: 'region', direction: 'asc'}); assert.deepEqual(next.comparison, comparison);
});
check('Choosing a temporal breakdown starts in chronological order', () => assert.deepEqual(withRelationalDimensions(base, ['month']).sort, {field: 'month', direction: 'asc'}));
process.stdout.write(`${checks} relational presentation checks passed.\n`);
