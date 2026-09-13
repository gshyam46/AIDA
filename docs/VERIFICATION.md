# AIDA hybrid verification

> **Historical record.** This evidence was produced by the pre-AIDA 4 single-call pipeline (`backend/core/semantic.py`, `scripts/evaluate_semantics.py`), which has been removed. Current behaviour is measured by `scripts/benchmark_nl.py`; see [BENCHMARK.md](BENCHMARK.md) and [SECURITY.md](SECURITY.md).

The subsequent multi-table expansion is documented separately in [RELATIONAL_VERIFICATION.md](RELATIONAL_VERIFICATION.md). The evidence below records the earlier single-table baseline and is retained for comparison.

Verification date: September 12, 2026. Windows, Python 3.13.14, Node 24.20.0, Intel Core Ultra 7 266V, 16 GB RAM and Intel Arc 140V Vulkan acceleration.

The working demo uses a real local Qwen3-4B-Instruct-2507 Q4_K_M model served by llama.cpp b10809, followed by deterministic source mapping, validation, SQL compilation and read-only SQLite execution. The earlier zero-model demo measurements are superseded; its grammar tests do not establish natural-language model accuracy.

## Final results

| Check | Observed result | Evidence |
| --- | --- | --- |
| Backend regression suite | 269 passed in 16.68 seconds; two upstream deprecation warnings | [JUnit report](evidence/backend-tests.xml) |
| Real-model original regression | 32/32: 20 supported questions and 12 safe refusals | [Model regression](evidence/model-regression.json) |
| Real-model challenge regression | 18/18: 14 supported questions and 4 safe refusals | [Challenge regression](evidence/model-challenge-regression.json) |
| Real-model acceptance | 22/22, including all 16 catalog starters and the commerce/support browser questions | [Acceptance report](evidence/model-starters.json) |
| Real-model forward case | 1/1 | [Forward report](evidence/model-forward.json) |
| Production browser journey | 11/11 stages passed; no uncaught JavaScript errors or external browser requests | [Browser report](evidence/browser.json) |
| Sequential HTTP benchmark | 60/60 requests; 6 local model calls and 54 interpretation cache hits | [Benchmark report](evidence/benchmark.json) |
| Frontend production build | Passed, including TypeScript checks | `npm.cmd run build` in `frontend` |
| Windows model/application stop and start | Passed with the pinned local model; application and model health ready | `scripts/stop-demo.ps1`, `scripts/start-demo.ps1 -SkipInstall -SkipBuild` |

The four final model suites use semantic contract version 6. They contain 73 case executions with overlap between suites, not 73 independent unseen questions. Supported cases match both explicit expected plans and independently authored SQL results. Refusal cases execute no SQL. The separate API test stub is never used by the real-model evaluation or browser journey.

## Actual model evaluation

The pinned 2,497,280,448-byte model and Windows runtime were downloaded and SHA-256 verified against `scripts/model-runtime.json`. Inference runs at `127.0.0.1:8081`; no hosted inference API is used.

`scripts/evaluate_semantics.py` performs real inference over commerce and support schemas, compares interpreted plans with authored expectations and results with independently written SQL. It also checks direct builder execution, interpretation caching and an uncached repeat. No model response is substituted.

The initial 32-case run passed 21 cases: 9/20 supported questions and 12/12 clarification cases. It exposed grouping, filtering and date failures; the [initial report](evidence/model-initial.json) is retained. The first untouched challenge run passed 17/18 and exposed a reversed ranking direction; its [initial report](evidence/model-challenge-initial.json) is also retained. Subsequent runs of these questions are regression evidence, not fresh held-out evidence.

A later starter check and browser run exposed invented date requirements for valid all-data and empty-average requests. Those failures remain in the [starter record](evidence/starter-initial.json) and [browser regression record](evidence/browser-scope-regression.json). Version 6 makes readiness a code decision: the model extracts typed fields and grounded unresolved phrases, and deterministic validation checks the complete interpretation. It cannot choose a freeform clarification status or emit SQL. Tests retain the failed questions, including `Monthly revenue trend` and `Average order value by month for pending`.

An uncached question makes at most one inference request with a 384-token output cap, temperature 0 and seed 42. Model-supplied metric and dimension IDs are mapped through the approved catalog; categorical values and date expressions must be grounded and valid. There is no retry/model-hopping agent loop. Exact interpretation caching is scoped to the question, source/catalog version, date reference, model and semantic contract. Builder actions and saved plans still pass deterministic validation.

## Code and API boundaries

Source/compiler tests use a separate inventory schema and independent expected arithmetic. API tests use an explicitly named stub to exercise application wiring, not to claim model accuracy. Semantic tests exercise prompt projection, loopback-only transport, redirects/proxies disabled, typed outputs, constraint rejection, dates, bounded cache and actual call telemetry.

Checks cover schema-only upload inspection, explicit metric definitions, numeric data validation, sensitive field exclusions, parameterized values, quoted approved identifiers, SQLite read authorizers, query deadlines, source isolation, request limits, Origin/Host protection, generic server failures, catalog-version changes and persisted private sources remaining inaccessible in public-demo mode.

The old grammar reference remains only for the commerce seed and historical regression checks. Runtime natural-language requests do not invoke it or fall back to it.

## Browser and packaging

The final browser run used the production frontend, real backend and real model. It verified count 7,810, regional revenue, chart/table views, CSV export, chart drilldown, a zero-model visual builder, dashboard save/reload/refresh/removal, recoverable unsupported intent, null averages, empty periods and mobile layout. An initial count made one model call; its repeat returned the identical plan, SQL parameters and result with zero model calls.

Source switching was verified against support operations. A third SQLite schema was uploaded through the UI, explicitly mapped to a stock metric and depot dimension, and queried using `How many units do we have at Harbor?`. Real inference produced the expected filter and returned 55, matching the independently defined fixture. The same result was reached through a zero-model chart drilldown. Dashboard definitions remained scoped to their source and catalog version. Screenshots: [desktop](demo-desktop.png), [mobile](demo-mobile.png), [uploaded source](demo-upload.png).

PowerShell scripts pin local endpoints and track verified process identities. Packaged stop/start was exercised successfully with Vulkan on this machine; the final backend was restarted to load contract version 6 before the final browser run. Docker source has been reviewed, but Docker is unavailable here, so no Docker execution or public cloud deployment is claimed.

## Measured latency and model use

The final benchmark started with an empty backend cache and sent six distinct questions ten times each through the production frontend on port 3000. All 60 requests succeeded. There were six actual local inference calls, 54 interpretation cache hits and 6,896 total model tokens.

Median HTTP latency was **3,072.74 ms for requests requiring inference** and **16.05 ms for reused interpretations**. Overall median was 16.27 ms, p95 was 2,931.76 ms and maximum was 6,378.13 ms. This is a sequential local benchmark on the hardware above, not a concurrent capacity test. The application serializes model inference and can report busy while another request is running. Hosted API charges are zero; local hardware and electricity costs are excluded.

## Practical limits

The model's interpretation is probabilistic. Valid SQL does not prove that an interpretation matches every possible question. The evaluated corpus is small and synthetic. Hardware and electricity still have a cost despite no hosted API token fee.

Onboarding supports static SQLite snapshots up to 20 MB and one explicitly approved reporting table, one metric, one optional grouping, equality filters and supported dates per query. No inferred joins, live database connectors, authentication, tenant authorization or shared server-side dashboards are implemented. Private-data use is confined to local mode; public-demo mode hides private sources and disables upload/configuration. Aggregate-only access does not itself provide differential privacy or minimum-group-size guarantees.
