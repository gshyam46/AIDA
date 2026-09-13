# Relational query verification

> **Historical record.** The interpretation evidence below came from the pre-AIDA 4 pipeline (`backend/core/relational_semantic.py`, `scripts/evaluate_relational.py`), which has been removed. The relational compiler, fixtures and oracles described here are still in use. Current interpretation results are in [BENCHMARK.md](BENCHMARK.md).

September 12, 2026. This extends the earlier single-table demo while retaining local model interpretation followed by deterministic validation, SQL compilation and execution. The query matrix and complete browser workflows passed; their separate inference and cache evidence is recorded below.

## Databases and independent checks

| Source | Physical schema | Deliberate correctness cases |
| --- | --- | --- |
| Retail warehouse | 9 tables; 1,600 current order lines, 412 archived lines and 800 orders | Two different join paths; duplicate returns; missing dimension references; nullable measures; 12 exact archive overlaps; distinct order count across line grain |
| SaaS billing | 8 tables; 1,440 current invoice lines, 370 archived lines and 600 invoices | Invoice/account and subscription/plan joins; duplicate credits; 10 archive overlaps; invoice totals distinct from line amounts |
| Public Chinook v1.4.5 | 11 tables, retained unchanged | Five-table selection, customer country versus billing country, invoice-line sale price versus track catalog price, invoice distinct count, date filtering and playlist existence |

Chinook comes from the [official release](https://github.com/lerocha/chinook-database/releases/tag/v1.4.5). Its database hash and MIT license are retained in [fixtures/chinook](../fixtures/chinook/README.md). No production customer database was used. The two generated databases include unrelated sensitive tables that are excluded from approved queries.

The 31 supported query cases have authored semantic plans and SQL written independently of the compiler. Browser expectations are materialized from those SQL statements against separate fixture instances. They are never included in model prompts or substituted for API responses. Six additional language cases test personal-data refusal, instruction overrides, unsupported calculations and an ambiguous country grouping.

## Supported operations and controls

| Operation | Implementation and correctness boundary |
| --- | --- |
| Table and column selection | Model selects approved business identifiers. Code resolves fully qualified physical columns and the unique approved path from the fact table; results expose table, column, join and operation lineage. |
| Joins | LEFT joins along approved many-to-one relationships preserve missing references. Target keys must be declared unique; compatible key affinities and exact binary comparison prevent subtle row multiplication. |
| Aggregates | Up to three COUNT, COUNT DISTINCT, SUM, AVG, MIN or MAX measures at one fact grain, with up to two grouping dimensions. Parent-table measures cannot silently multiply across child rows. |
| WHERE | AND predicates, equality, inequality, numeric comparisons and bounded IN lists. All values are parameters. Numeric storage classes are checked before relevant predicates can conceal malformed data. |
| HAVING | Up to three numeric comparisons on selected aggregates. It operates after grouping and is separate from row predicates. |
| Subqueries | One approved correlated EXISTS/NOT EXISTS child relationship, with its own filters. Above-average comparison uses a subquery over filtered group aggregates before result limiting. |
| Sets | Two schema-aligned current/archive fact populations. UNION ALL retains overlaps; UNION removes exact duplicate aligned fact rows before aggregation. |
| Dates and ordering | Grounded date text is resolved in code; dates and sort fields are validated. Stable tie-breaking and deterministic join order produce reproducible SQL. |
| Visualization | Bar charts, monthly line/area charts, nonnegative additive donut distributions, two-measure scatter plots, typed tables, CSV export and saved dashboard definitions. AVG, MIN, MAX and COUNT DISTINCT cannot be summed into donut totals. Chart interaction uses validated structured plans. |

Read-only SQLite connections, an authorizer restricted to approved tables/columns and code-owned CTEs, a two-second database deadline and a 100-row aggregate result bound remain in force. Private uploads and mappings stay inaccessible in public-demo mode. Prompt projection omits physical schemas, keys, file paths, connection details and result rows; inference remains on loopback with no proxy, redirects, hosted fallback or agent loop.

## Results and reproducibility

| Check | Observed result | Evidence |
| --- | --- | --- |
| Complete backend suite | 443 passed in 21.08 seconds; two dependency deprecation warnings | [JUnit report](evidence/relational/backend-tests.xml) |
| Compiler correctness | 88 tests, including independently authored SQL and unsafe-join/nonadditive-metric counterexamples | Included in the backend report |
| Final targeted model regression | 8/8 exact plans and independent SQL results, relational semantic contract 8 | [Scope regression](evidence/relational/model-scope-v8.json) |
| Complex language queries | 31/31 uncached real-model questions matched exact plans and independently authored SQL results | [Uncached query evidence](evidence/relational/browser-uncached-v8.json), [timing totals](evidence/relational/metrics.json) |
| Complete relational browser journey | 15/15 stages passed; 31 exact cached replays, six refusals, all five chart forms, table/CSV, builder, drilldown, dashboard, mobile and uploaded relational catalog | [Complete browser report](evidence/relational/browser.json) |
| Original single-table browser regression | 11/11 stages passed after the relational extension | [Original browser regression](evidence/relational/original-browser-regression.json) |
| Frontend correctness helpers | 11 checks passed for chart eligibility and builder state transitions | `node frontend/scripts/check-relational-presentation.cjs` |
| Frontend production build and typecheck | Passed | `npm.cmd run build`, `npm.cmd run typecheck` |
| Packaged Windows stop/start and health | Passed with the pinned local model and rebuilt frontend | `scripts/stop-demo.ps1`, `scripts/start-demo.ps1 -SkipInstall -SkipBuild` |

The first combined model matrix passed [32/37](evidence/relational/model-initial.json); an intermediate revision passed [33/37](evidence/relational/model-intermediate-v4.json). These failures exposed real semantic and compiler defects and remain preserved. A later [browser run](evidence/relational/browser-initial-v6.json) exposed an invented grouping on a deduplicated total; the [next targeted check](evidence/relational/grouping-initial-v7.json) exposed an invented credit-existence condition. Contract 8 constrains unrequested grouping and relationship operations in both the output grammar and validation. It retains one actual model call for interpreting supported language, with no retry or expected-plan injection.

The first contract-8 browser run passed all 31 supported queries, all six refusals and the initial 11 workflow stages, then the test runner incorrectly waited for a successful result panel after a refusal. That [harness failure](evidence/relational/browser-harness-wait.json) is retained. After correcting it, a fresh uncached run passed the same query matrix and refusal recovery, then caught a viewport-paint race in its mobile assertion. [Resize diagnostics](evidence/relational/resize-diagnostic.json) confirmed that desktop positioning could briefly persist after resizing, while all 20 measurements after two animation frames fit 390px. The assertion now waits for layout settling and still fails persistent overflow.

The final complete journey reused those already verified interpretations. Its optional reuse mode requires 31 successful uncached cases and unchanged parser/compiler hashes; it still asks every question through the real browser and compares each live API response with both independent SQL expectations and the earlier SQL, parameters and data. The report retains original inference telemetry separately from actual cache-hit telemetry. This is not 31 new model calls. The uploaded 11-table Chinook copy receives a new source ID and requires a fresh real model call: 1,423 tokens, 5.06 seconds inference, exact expected plan and rows. No API responses are mocked or intercepted with fixtures.

These are fixed regression questions, not held-out evidence of universal language accuracy. Expected plans, SQL and data remain outside model prompts.

The runtime uses the same Qwen3-4B-Instruct-2507 Q4_K_M weights, llama.cpp b10809, Vulkan acceleration, a 4,096-token context, one inference slot, temperature zero and seed 42. Relational output is bounded to 768 tokens. The auxiliary multi-prompt RAM cache is disabled with `--cache-ram 0`; AIDA's bounded exact interpretation cache remains enabled. This avoids accumulating historical KV snapshots while changing catalogs. CPU mode remains available but was substantially slower on this machine.

The 31 uncached supported questions consumed 44,623 prompt tokens and 3,570 completion tokens. Median inference was **6.66 seconds**, p95 **9.24 seconds**, and median database time **4.54 ms** on Windows with an Intel Core Ultra 7 266V, Intel Arc 140V and 16 GB RAM. These are local observations, not capacity guarantees. Repeats, builder actions, chart drilldowns and dashboard refreshes reported zero model calls. No external browser requests or uncaught JavaScript errors were recorded.

The final [application and model health check](evidence/relational/final-health.json) passed. [Runtime flags](evidence/relational/runtime.json) are retained alongside the pinned model manifest in the browser report. The demo was left running at `http://127.0.0.1:3000`.

Screenshots: [qualified-column lineage](evidence/relational/01-lineage.png), [donut](evidence/relational/02-donut.png), [scatter](evidence/relational/03-scatter.png), [area](evidence/relational/04-area.png), [dashboard](evidence/relational/05-dashboard.png), [mobile](evidence/relational/06-mobile.png), [uploaded database with real inference](evidence/relational/07-uploaded-relational.png).

```powershell
.\.venv\Scripts\python.exe -m pytest backend -q
node frontend/scripts/check-relational-presentation.cjs
.\.venv\Scripts\python.exe scripts/evaluate_relational.py --suite all --label regression --output artifacts/relational-regression.json
.\.venv\Scripts\python.exe scripts/prepare-relational-e2e.py
# Restart the application backend before the first-call browser assertions.
node scripts/e2e-relational.cjs
```

Run inference evaluations and browser tests sequentially. Browser tests must start with an empty backend interpretation cache; initial language calls are required to report real model tokens and one model call. Repeats, builder execution, chart drilldowns and dashboard refreshes are required to use zero model calls.

For a UI-only rerun after the entire uncached query stage passed, preserve its report, keep the same backend running, and set `AIDA_E2E_REUSE_QUERIES` to that report's path. The runner requires exact cache hits and records their prior uncached provenance. Unset the variable for normal full inference acceptance. Do not use this mode after changing the parser, compiler, catalogs or source data.

## Limits

This is a bounded relational analytics product demo, not unrestricted text-to-SQL. Each catalog approves one fact grain and a unique directed relationship graph. Arbitrary joins, self-joins, arbitrary nested SQL, general Boolean OR across unrelated fields, window functions, arbitrary arithmetic and unconstrained set operations are unavailable. Missing or ambiguous meaning must fail visibly; SQL validity alone does not prove semantic correctness.

The static datasets and fixed questions are small regression evidence, not a universal accuracy or production-capacity claim. Dashboards remain browser-local. Authentication, tenant authorization and live database connectors are not implemented. Docker packaging includes the public fixture but has not been executed here; no public cloud deployment is claimed.
