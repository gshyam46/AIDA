# Model, test data and repository status

This document records which model AIDA uses, which databases are appropriate for each kind of test, what the model can see, and which repository cleanup remains. It reflects the `mvp2.0` branch as reviewed on September 13, 2026.

## Current model decision

AIDA currently uses **Qwen3-4B-Instruct-2507 Q4_K_M** through **llama.cpp b10809**. The GGUF is 2,497,280,448 bytes, approximately 2.5 GB. The exact upstream revision, URLs, licenses and SHA-256 hashes are pinned in [`scripts/model-runtime.json`](../scripts/model-runtime.json).

The model weights and llama.cpp binaries are not committed to Git. `scripts/setup-model.ps1` downloads them into the ignored `.runtime/` directory and verifies their hashes. `scripts/start-demo.ps1` performs setup and starts the model, backend and frontend. The model server listens only on `127.0.0.1:8081`; the default runtime uses a 4,096-token context, one inference slot, four CPU threads, Vulkan layer offload when available and `--cache-ram 0`. `-GpuLayers 0` provides the CPU fallback.

The 4B model remains the development baseline because it fits the tested local machine and has no hosted inference charge. It is not approved as sufficiently accurate for customer use: the first new-database assessment answered 19/40 supported questions correctly and incorrectly executed 6/10 unsupported or ambiguous requests.

Changing to a larger model is not the first fix. Post-outcome review found seven false refusals after a correct-looking model interpretation and five cases where deterministic candidate construction made the intended operation unavailable. A larger model cannot repair those code restrictions. Fix semantic candidate coverage, clause preservation and unsupported-intent validation first. Then compare the pinned 4B baseline with an 8B-class local instruct model on the same regression suite and a separate unseen suite. A 14B-class model should be considered only after measuring memory, latency and concurrency on the actual deployment hardware.

## What the model sees

The model does not connect to SQLite and does not generate SQL. For one uncached question, it receives:

- the user's question;
- opaque temporary IDs for approved metrics, dimensions, filter fields and related-record checks;
- owner-approved business labels and definitions;
- bounded approved categorical values and aliases; and
- candidate time phrases copied from the question.

It does not receive customer rows, query results, physical table or column mappings, connection strings, database paths, SQL, credentials or filesystem paths. After the model returns structured intent, deterministic code validates every selected business ID, maps it through the reviewed catalog, selects approved join paths, compiles parameterized read-only SQL and executes it under the SQLite authorizer and timeout.

Users should therefore ask in business language without table names. Conflicting physical names are resolved by explicit business mappings. For example, **Average sale price** and **Catalog price** can map to different `UnitPrice` columns. An unclear request such as “price” should be clarified rather than guessed.

Business calculations must exist in the approved catalog. “What is profit?” is correctly refused when profit is absent. The current metric contract supports approved `COUNT`, `COUNT_DISTINCT`, `SUM`, `AVG`, `MIN` and `MAX` measures at one fact grain. It cannot define a general formula such as revenue minus cost or a ratio of aggregates. Supporting profit requires an owner-approved definition and a deterministic calculated-metric extension; the model must never invent the formula.

## Databases to use for testing

No production customer database is required for product development or a public demonstration.

| Source | Data status | Best use | Availability |
| --- | --- | --- | --- |
| Commerce demo | Generated synthetic data | Fast single-table questions, dates, filters and basic charts | Created automatically at startup |
| Support operations | Generated synthetic data with a different schema | Source switching and independent business definitions | Created automatically at startup |
| Retail warehouse (`warehouse`) | Generated synthetic multi-table data | Join paths, same-named fields, fact grain, EXISTS and archive operations | Created automatically at startup |
| SaaS billing (`billing`) | Generated synthetic multi-table data | A second unrelated relational schema, distinct counts, HAVING and child checks | Created automatically at startup |
| Chinook (`chinook`) | Public third-party sample with retained MIT license and checksum | Independently produced 11-table schema and conflicting price/country meanings | Tracked under `fixtures/chinook/` |
| Blind synthetic logistics | Generated synthetic 12-table data | Difficult joins, filters, HAVING, subqueries, UNION, nulls, empty results and refusal cases | Tracked under `fixtures/blind_logistics/`; uploaded through local onboarding |

The 50 logistics questions are no longer an unseen benchmark because their outcomes have been inspected. Keep them as regression tests. After fixes, create another independently authored database/question set, freeze the product and oracles before inference, and report that result separately.

For a private customer pilot, use a de-identified SQLite snapshot and an owner-reviewed catalog. Confirm the fact grain, metrics, field meanings, join cardinalities, date basis, allowed values and archive behavior before running questions. Do not use customer data in public-demo mode or publish it as test evidence.

## Verification order

Use the deterministic checks before model inference:

```powershell
.\.venv\Scripts\python.exe -m pytest backend -q
.\.venv\Scripts\python.exe validate_structure.py
Set-Location frontend
npm.cmd run build
npm.cmd run typecheck
node scripts/check-relational-presentation.cjs
```

Start the pinned local model and run inference suites sequentially because the current runtime has one inference slot:

```powershell
Set-Location ..
powershell -ExecutionPolicy Bypass -File scripts/start-demo.ps1 -SkipInstall -SkipBuild
.\.venv\Scripts\python.exe scripts/evaluate_semantics.py --suite acceptance --output artifacts/semantic-acceptance.json
.\.venv\Scripts\python.exe scripts/evaluate_relational.py --suite all --output artifacts/relational-evaluation.json --label regression
```

Use `scripts/evaluate_blind.py` only to reproduce or regress the existing logistics assessment. A repeat must not be described as a new blind result. Model answers must be scored against independently written SQL and intended structured plans; a plausible chart is not correctness evidence.

The next model/customer gate should require:

- no accepted answer with changed meaning;
- no execution of unsupported or ambiguous intent;
- at least 90% correct supported answers on a newly sealed transfer set;
- at most one model call per initial question;
- acceptable p95 latency on target hardware; and
- zero observed prohibited model-payload fields or sensitive-value canaries.

## Repository cleanup status

The active Python modules and frontend components are referenced by the application, tests or verification tools. The obsolete V1 backend pipeline modules and old frontend query/upload/result components were removed in `mvp2.0`. Runtime output is excluded by `.gitignore`, including `.runtime/`, `.venv/`, `node_modules/`, `backend/data/`, `artifacts/`, caches and frontend build output.

Cleanup is not complete. The following items remain intentionally visible until a separate cleanup commit decides their final home:

| Item | Current status | Recommended publication action |
| --- | --- | --- |
| Three PDFs under `Research/` | Tracked, about 2.4 MB, and not referenced by the product or current documentation | Remove from the product branch, or document their provenance and purpose if they are required research sources |
| `docs/evidence/` | 68 files, about 12 MB | Keep compact final summaries and representative screenshots in Git; move bulky raw responses to a GitHub release asset or evidence branch |
| Intermediate and failed-run evidence | Useful development history but not required to run the product | Retain only where it proves an important correction; otherwise archive outside the main product tree |
| Local machine metadata in raw evidence | Eight evidence files contain local Windows paths or test-host information | Sanitize before publishing a release artifact; preserve hashes only for the original internal evidence set |
| Historical verification totals | Documents correctly record earlier 269-test and 443-test stages plus the current 455-test stage | Add a single current-status table and clearly label older counts as historical to prevent confusion |

Do not delete the SQLite fixtures, reviewed catalogs, licenses, tests, startup scripts or final blind-assessment summary merely because they are not used in the production request path. They provide reproducibility, onboarding examples and licensing evidence. Do not commit downloaded model weights, generated customer snapshots, customer dashboard state, credentials or local process logs. Synthetic browser evidence may be retained according to the evidence policy above.

Before a public release, complete the cleanup above, run a secret and local-metadata scan, verify every documentation link, build from a fresh clone, and confirm that the repository can recreate its generated fixtures and download only hash-pinned runtime assets.
