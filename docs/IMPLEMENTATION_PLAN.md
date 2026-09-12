# AIDA architecture restoration and demonstration plan

The intended product uses a small model for language understanding and deterministic code for business definitions, validation, SQL execution and visualization. Replacing the semantic layer with a narrow question grammar was a deviation. The current work restores the model boundary and makes source onboarding explicit.

## Implemented work

- [x] Restore local semantic interpretation using Qwen3-4B-Instruct-2507 Q4_K_M through llama.cpp.
- [x] Restrict inference to a loopback endpoint and project only approved business metadata into the prompt.
- [x] Use opaque catalog identifiers, bounded structured output, one model request per uncached question and explicit failure reporting.
- [x] Validate interpreted plans and builder plans before compiling quoted, parameterized, read-only SQLite queries.
- [x] Preserve commerce metric definitions and add a support-operations source with different tables, columns, measures and date reference.
- [x] Add local SQLite snapshot upload, metadata-only inspection, opaque source IDs and durable owner-approved reporting mappings.
- [x] Enforce selected-source query execution and source-specific dashboard definitions.
- [x] Preserve charts, tables, CSV export, chart interaction and browser-persisted dashboard views.
- [x] Separate local onboarding from synthetic-only public-demo mode; disable private source access and configuration in the latter.
- [x] Pin model/runtime provenance and hashes; provide setup, start, CPU fallback and stop scripts.
- [x] Add independent source/compiler and API-boundary regressions, including an explicitly named parser stub.

## Relational extension

- [x] Add a generic version 2 catalog and deterministic relational compiler while preserving the original single-table contract.
- [x] Resolve approved business fields through explicit table/column mappings and unique join paths; reject parent-grain measures, ambiguous paths, and joins that can multiply fact rows.
- [x] Support up to three metrics and two groupings, numeric and categorical WHERE filters, IN, aggregate HAVING, correlated EXISTS/NOT EXISTS, above-average grouped subqueries, and compatible UNION/UNION ALL populations.
- [x] Validate target key uniqueness, matching key affinities, and exact join collation; check numeric storage types before predicates and aggregation.
- [x] Add independent retail and billing schemas with nullable fields, duplicate child events, archive overlaps, unrelated sensitive tables, and misleading same-named columns.
- [x] Add the unmodified public Chinook sample to exercise independently produced tables and data; retain its license and checksum.
- [x] Provide complete reviewable catalog JSON files and [upload/configuration instructions](RELATIONAL_CATALOG.md) for all three databases.
- [x] Verify the relational compiler with 88 tests, including 24 independently authored SQL comparisons across the generated schemas and nonadditive chart counterexamples.
- [x] Record 31/31 uncached model queries against independent SQL, six safe refusals, 15/15 complete relational browser stages, 11/11 original browser regression stages, and 443 passing backend tests in [RELATIONAL_VERIFICATION.md](RELATIONAL_VERIFICATION.md). The complete UI rerun separately records exact cached interpretations and a fresh model call for an uploaded relational source.

## Completion evidence

[VERIFICATION.md](VERIFICATION.md) records the completed original single-table demonstration and its raw evidence. Its results below predate the relational extension; [RELATIONAL_VERIFICATION.md](RELATIONAL_VERIFICATION.md) records the additional integration evidence:

- [x] Real model questions across both source schemas: exact expected plans, independently computed results and actual model-call/token telemetry. Final suites pass 32/32, 18/18, 22/22 and 1/1.
- [x] Rejection or clarification for unsupported/ambiguous intent, fabricated filters and malicious instructions; failed validation executes no query.
- [x] Cached interpretation, direct builder execution and saved dashboard refresh verified with their actual model-call behavior. The 60-request benchmark makes six model calls.
- [x] All 11 browser stages, including an uploaded third schema, pass against the real local model. All 269 backend tests pass.
- [x] Production frontend build and packaged local model/application startup verified on Windows.
- [ ] Docker execution and public hosting: not tested or deployed; no hosting target was supplied.

## New-database transfer assessment

- [x] Freeze the product and independently author a new 12-table logistics database plus 50 questions and SQL oracles.
- [x] Run all 40 supported explicit plans: 40/40 independent SQL matches.
- [x] Preserve the first uncached 50-case real-model run: 19/40 supported answers correct, 13 false refusals, eight changed meanings, and six unsafe semantic acceptances among ten refusal cases.
- [x] Record ten session replays and ten fresh-parser repeats, actual request/token/timing evidence and privacy payload audits; all frozen-file hashes remain unchanged.
- [x] Run the full backend suite: 455 tests pass, including the new scoring checks.
- [x] Verify the new source in the real browser: all 15 integration checks pass; six language questions reproduce the first-pass outcomes, including two failures. Preserve both browser attempts and leave the logistics source available locally.
- [x] Document the pinned local model, prompt/database boundary, test-database selection, future model comparison and repository hygiene in [MODEL_TEST_DATA_AND_REPOSITORY.md](MODEL_TEST_DATA_AND_REPOSITORY.md).
- [ ] Repair semantic coverage, unsupported-operation handling, candidate restrictions and false-refusal guards according to [BLIND_EVALUATION.md](BLIND_EVALUATION.md).
- [ ] After fixes, run a separately authored and sealed transfer set; retain these 50 cases as regression evidence.
- [ ] Remove or document the three unreferenced `Research/` PDFs, reduce raw evidence on the product branch, sanitize local machine metadata, and consolidate the current verification status before a public release.

The transfer assessment is complete; its release criteria failed. Earlier regression pass counts do not establish broad language reliability. This phase measured the unchanged product and did not tune it to the new questions.

## Product boundaries

The original contract queries one approved reporting table with one metric, one optional grouping, and equality filters. The new relational contract queries one approved fact grain through proven many-to-one dimension paths and an optional compatible archive population, with the bounded operations described above. Both contracts use static SQLite snapshots. Private data access is intended for a single-user local process; authentication and tenant authorization are not implemented.

Model interpretation remains probabilistic. Structural validation constrains what can execute; it cannot prove that every valid plan matches the user's meaning. The evaluated questions and unresolved cases must remain visible in the verification evidence.

Automatic business-metric discovery, inferred joins, cross-fact metrics, arbitrary SQL expressions or nested subqueries, window functions, PostgreSQL/MySQL connectivity, live refresh, multiuser/shared dashboards, forecasting, minimum-group-size privacy rules and unrestricted natural-language querying are outside this implementation. See [RELATIONAL_CATALOG.md](RELATIONAL_CATALOG.md) for the precise supported filters, subqueries, union behavior, and owner-approval workflow.
