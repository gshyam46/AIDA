# Connector verification — September 14, 2026

This change implements PostgreSQL, MySQL and SQL Server adapters, explicit reporting-column selection, encrypted saved connection settings, background full-snapshot extraction, manual/hourly/daily refresh, status/history, disconnect and source-scoped cache replacement. The integration preserves the freshly pulled AIDA 4 interpreter, calculations, accounts, CSRF/rate-limit protections and redesigned workspace. Connection operations and published snapshots now enforce per-account ownership.

## Executed checks before the AIDA 4 merge

### Fresh-pull integration with `172115c`

After pulling the three new `mvp2.0` commits, the connectors were adapted to AIDA 4's interpreter interface, runtime settings, account ownership, request guards, audit events and `/workspace` route. The remote landing/account/benchmark work remains intact.

- Full merged backend suite: **347 passed, 3 live-vendor tests skipped**. This suite differs from the older baseline below because the upstream AIDA 4 commits replaced the legacy parser and its tests.
- Connector plus security subset: **56 passed**, including new denial of cross-account connection listing/actions and snapshot queries, ownership preservation through refresh/reopen, CSRF checks, and loading connector settings from the explicit runtime environment.
- Repository structure check: passed (46 Python files).
- TypeScript and production build: passed, including landing, login, signup, onboarding, benchmarks and workspace routes.
- Connector browser workflow on the merged `/workspace`: all four stages passed, no page errors, zero model calls; finished `2026-09-14T10:54:38Z`. It still uses the explicitly documented SQLite remote-session surrogate; live-vendor qualification remains pending.

### Original pre-merge baseline

| Check | Result | Evidence / command |
| --- | --- | --- |
| Full backend suite | 477 passed; 3 live-vendor cases skipped | `.venv/Scripts/python.exe -m pytest backend -q` |
| Connector workflow/privacy tests | 22 passed (included above) | `backend/test_connectors.py` |
| TypeScript | Passed | `npm.cmd run typecheck` in `frontend` |
| Production frontend build | Passed | `npm.cmd run build` in `frontend` |
| Production browser workflow | Four stages passed, no page errors, zero model calls | `scripts/e2e-connectors.cjs`; local `artifacts/connector-browser/report.json` and `connections.png` |
| Real PostgreSQL/MySQL/SQL Server | **Not executed** | `backend/test_connectors_live.py`: all three environments unconfigured |

The browser uses the production build, real HTTP proxy/API, encryption, scheduler, local snapshot registry, deterministic query engine and dashboard persistence. The remote database session is **explicitly replaced by a synthetic SQLite database** using `scripts/connector-test-server.py`. It is workflow evidence, not proof of vendor authentication, TLS, grants, transaction isolation or driver installation. Unit extraction tests use the same explicit distinction. No new natural-language accuracy claim is made.

The browser test inspected a table without showing row samples; copied `item_id`, `category` and `quantity` while excluding `customer_email`; created an hourly connection; approved an inventory measure; saved a chart; changed the synthetic source; refreshed the snapshot; and verified both API and saved-dashboard totals changed from **19 to 100**. It then paused the schedule and removed the connection credentials while keeping the source queryable.

The first browser attempts exposed test-environment problems (ports already occupied by another application, then insufficient waiting for refresh completion). The final run used verified free ports 38147/38148 and explicitly polled for a changed successful-refresh timestamp before checking data. The final run passed all four stages. The existing query compiler and language interpreter were not replaced to satisfy this test.

Backend tests cover encrypted persistence/redaction, metadata-only inspection, selected-column exclusion, data updates/deletions, result-cache invalidation, catalog identity across refresh, old-reader consistency, source reopen, scheduled due-time persistence, pause/disconnect, interrupted-job recovery, wrong encryption keys, revoked host access, schema drift, extraction failure, numeric/row limits, structured driver credentials/TLS options, same-named-column joins, public-demo/origin/body-size boundaries, exclusive worker ownership and orphan generation cleanup.

## Remaining qualification

The machine had no running Docker daemon or local vendor database servers. Only the legacy Windows SQL Server ODBC driver was present, not Microsoft ODBC Driver 18. The three adapters therefore need the live test matrix described in [CONNECTIONS.md](CONNECTIONS.md#run-vendor-integration-tests) against disposable PostgreSQL, MySQL and SQL Server instances with verified TLS and read-only accounts before a customer rollout. Add real-server write-during-extraction and certificate/permission-failure scenarios during that qualification.

The merged AIDA 4 branch provides accounts, sessions and per-account source ownership. Connector integration adds ownership checks, rate limits and audit events to connection operations and preserves owner IDs across scheduled refresh. This remains a single-process service; shared dashboards, encrypted snapshot files, secret-key rotation, incremental extraction/CDC, distributed scheduling and live-vendor qualification remain outstanding. Refer to the current benchmark docs for AIDA 4 language results; connector work does not establish new language-accuracy evidence.

## Reproduce the browser walkthrough

Use a fresh synthetic harness workspace and free ports. Build the frontend first. In one terminal:

```powershell
.\.venv\Scripts\python.exe scripts/connector-test-server.py
```

In a second terminal, from `frontend`:

```powershell
$env:BACKEND_URL = 'http://127.0.0.1:38148'
$env:PORT = '38147'
$env:HOSTNAME = '127.0.0.1'
$env:AIDA_PUBLIC_DEMO = '0'
node scripts/start.cjs
```

From the repository root in a third terminal:

```powershell
$env:AIDA_BROWSER_CHANNEL = 'msedge' # Or omit when Playwright Chromium is installed.
node scripts/e2e-connectors.cjs
```

Stop both test services afterwards. The harness must never be used for customer data or deployed as the production backend. Production starts through `scripts/start-demo.ps1` or `backend.main:app`.
