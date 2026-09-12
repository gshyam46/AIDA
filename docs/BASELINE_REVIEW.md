# AIDA baseline review

Historical review recorded on 2026-09-12 against commit `6cfad332bc36fc58a7a52bad00539c43d16ea4ae`, before the demo repairs. The source references below describe that original baseline; they do not identify current file locations or claim these defects remain. [VERIFICATION.md](VERIFICATION.md) records current execution evidence.

The original architecture already separated model interpretation, intermediate representation, normalization, validation, SQL compilation and execution. Its implementation defects did not justify removing language understanding. The first repair replaced that layer with a limited regex grammar and incorrectly presented a narrow commerce demo as completion. The current correction restores a small local model and adds explicit source onboarding while retaining deterministic controls after interpretation.

## Why the original product did not work reliably

| Severity | Trigger and observed code behavior | Baseline evidence |
| --- | --- | --- |
| Blocking | Starting without a Groq key fails in application lifespan before the service can run. Every ordinary query depends on a hosted 70B model; there is no local parser route. Three main attempts plus three alternate-model attempts can greatly exceed the browser's 60-second timeout. | `backend/core/semantic_parser.py:31-48,158-233`; `backend/core/pipeline.py:28-31`; `frontend/lib/api.ts:7` |
| Blocking | A fresh checkout has neither the default `uploads/olist.sqlite` nor a generated demo database. Queries and schema loading therefore have no usable default source. | `backend/api/endpoints.py:63,115`; baseline repository inventory |
| Blocking | Uploading a database updates the displayed schema, but the next query omits its path and queries the default database. The UI can show one schema while querying another file. | `frontend/app/page.tsx:48-51,69-78` |
| Wrong answer | "Count all orders" silently adds `status = completed`; explicitly requesting pending orders adds a contradictory completed filter. This business choice is not explained before the answer. | `backend/core/ir_normalizer.py:64-66,125-127,359-382` |
| Wrong answer | "This month" and "last month" always use January 3, 2025. Unrecognized time expressions or a missing date column are silently discarded. "This month" also has no upper boundary. | `backend/core/ir_normalizer.py:295-329` |
| Wrong answer | An unmatched money metric may become the first numeric column, including an ID. An unmatched entity is accepted if only one table exists. The YAML metric mappings are strings while the normalizer expects lists and iterates each value. | `backend/core/ir_normalizer.py:189-192,212-238`; `backend/config/business_rules.yaml:4-14` |
| Query failure | SQL safety checks look for forbidden substrings anywhere in SQL. Valid names such as `created_at` and `updated_at` contain `CREATE` and `UPDATE`, so ordinary date-filtered queries can be rejected. | `backend/core/query_executor.py:111-130` |
| Product gap | The interface shows a result table and internal pipeline JSON only. There is no chart, interactive breakdown, structured query builder, dashboard, or dashboard persistence. | `frontend/app/page.tsx`; `frontend/components/ResultsDisplay.tsx`; full frontend inventory |

## Privacy and operational gaps

| Risk | Baseline behavior and consequence | Evidence |
| --- | --- | --- |
| External disclosure | The complete question, schema names, types, and table row counts are sent to the hosted provider. Questions can contain customer identifiers. No row sample is inserted by this prompt builder, but this is insufficient for a claim of no customer-data leakage. | `backend/core/semantic_parser.py:107-153,158-185` |
| Logging disclosure | Raw question text is logged at info level and in errors. Query parameters are logged at debug level. | `backend/api/endpoints.py:60`; `backend/core/semantic_parser.py:100-104`; `backend/core/query_executor.py:57-58` |
| Access boundary | Unauthenticated requests can supply arbitrary server filesystem paths to query or inspect. Upload names are joined directly to the upload directory, allowing traversal/overwrite paths. | `backend/api/endpoints.py:63,109-118,170-173` |
| Cross-request isolation | A global pipeline owns one connector with mutable current database path. Request A can connect, await the model, and resume after request B has switched the connector to another database. | `backend/api/endpoints.py:27`; `backend/core/pipeline.py:26,57-97`; `backend/core/database.py:115,135-136,211-223` |
| Sensitive columns | Retrieval compiles to `SELECT *` with no column privacy policy. Read-only SQL prevents modifications, not unauthorized disclosure. | `backend/core/sql_compiler.py:23-25,142-144` |
| Resource bounds | Upload reads the entire file without a backend size cap despite the UI saying 100 MB. Queries fetch all rows before truncation. Windows timeout fallback simply yields and does not interrupt a query. | `backend/api/endpoints.py:172`; `backend/core/database.py:223`; `backend/core/query_executor.py:62-70,161-180`; `frontend/components/FileUpload.tsx` |
| Deployment | Browser API defaults to the visitor's localhost. Compose starts without a bundled data source. Setup instructions reference missing environment examples and a missing LLM config. | `frontend/lib/api.ts:3`; `docker-compose.yml`; `README.md`; baseline inventory |
| Evidence gap | The nominal end-to-end test creates semantic IR directly, bypassing actual natural-language parsing, HTTP lifecycle, upload selection, and the browser. It therefore cannot catch the main reported failures. | `backend/test_basic.py:test_end_to_end` |

## Product direction for the repair

Preserve the intended question-to-IR-to-validated-SQL boundary. Use Qwen3-4B-Instruct-2507 Q4_K_M through local llama.cpp for language interpretation, with an opaque catalog vocabulary and one bounded inference call per uncached question. Code owns approved business definitions, exact source selection, date normalization, plan validation, SQL compilation and read-only execution. The visual builder and existing plans can bypass inference without removing it from natural-language queries.

The model receives the question and approved business metadata, never row samples or query results. The application restricts its inference endpoint to loopback and reports model failures directly. Structural validation limits executable plans; it cannot establish that every valid interpretation is semantically correct. Actual-model evaluations must therefore check both the intended plan and independent query results.

Provide reproducible commerce and support-operations datasets with different schemas and reference dates. Make metrics and filters visible, preserve chart interactions and dashboard views, and require explicit table/column mappings for uploaded SQLite reporting snapshots. Do not infer revenue from an arbitrary numeric column or invent a join.

A public demo uses synthetic sources and disables private source access, uploads and mapping changes. Local uploads use opaque source identifiers, explicit source selection, restricted columns and bounded processing. Dashboard persistence stores source IDs, query definitions and presentation choices without copying result rows into browser storage. Query labels and filter values can still be sensitive, and aggregate-only access does not provide a complete privacy guarantee.

## Acceptance evidence required

- Known arithmetic for counts, totals, grouped sums, date boundaries, filters and rankings against independently written SQL across two schemas.
- Actual local model calls producing expected structured plans and results, with token/call/latency evidence; repeatability checked on the evaluated runtime without claiming universal determinism.
- No off-machine inference or credentials required after model/runtime setup; first installation necessarily downloads software and weights.
- Unsupported, malformed or ungrounded intent rejected before execution. Curated negative tests must remain separate from any claim about all possible user questions.
- Full browser journey: selected source, natural-language result, chart interaction, builder execution, dashboard pinning/reload/removal, recoverable errors and mobile layout.
- Local upload, metadata-only inspection, explicit approval and query/source isolation; public mode must hide private sources and reject upload/configuration requests.
- Read-only SQL, restricted projections, bound parameters, execution deadlines and result limits; saved dashboards must contain definitions rather than result rows.
- Production frontend build and packaged local model/application startup. Docker execution and public deployment require their own evidence and are not established by source review.
