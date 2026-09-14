# Model, test data and repository status

This document records which model AIDA uses, what the model can see, which databases to use for each kind of test, and which repository cleanup remains. It reflects the `mvp2.0` branch with the AIDA 4 pipeline, as reviewed on September 13, 2026.

## Model decision

The [database connector addition](CONNECTIONS.md) is integrated with AIDA 4 accounts, ownership and interpretation. Database inspection/extraction and scheduled refresh make no model calls. [Connector verification](CONNECTOR_VERIFICATION.md) records integration checks and pending live-vendor qualification.

<!-- model-decision -->
AIDA 4 interprets questions with a hosted Groq model by default (`AIDA_MODEL_PROVIDER=groq`). Three Groq models were benchmarked on the same 42-question selection with real inference and independent SQL oracles: `openai/gpt-oss-120b`, `openai/gpt-oss-20b` and `qwen/qwen3.8-27b`. The provisional default is **`qwen/qwen3.8-27b`, two-stage, one repair round**. In the complete AIDA 4 runs it scored 22/32 supported and 10/10 refusals with zero wrong answers, compared with 18/32, 9/10 and two wrong answers for gpt-oss-20b. gpt-oss-120b could not be measured on AIDA 4 because its daily quota was exhausted. The final prompt-3 runs are still resuming; [BENCHMARK.md](BENCHMARK.md) has the method, the numbers and the regeneration command.
<!-- /model-decision -->

Provider constraints observed on the free tier during benchmarking, which shape the choice as much as accuracy does:

| Model | Tokens per minute | Other limits observed | List price (input / output per million tokens) |
| --- | --- | --- | --- |
| `openai/gpt-oss-120b` | 8,000 | 200,000 tokens per day (rolling), which a full benchmark run exhausts | $0.15 / $0.60 |
| `openai/gpt-oss-20b` | 8,000 | 1,000 requests per day | $0.075 / $0.30 |
| `qwen/qwen3.8-27b` | 8,000 | 1,000 output tokens per minute | see `GROQ_PRICES` in `backend/core/llm.py` |
| `meta-llama/llama-prompt-guard-2-86m` | 15,000 | 14,400 requests per day | $0.04 / $0.04 |

An AIDA 4 question costs one guard call plus two model calls (resolve, then plan), and one more when a repair round runs. That is about 3,000–8,000 tokens per question, depending on the catalog size and whether a repair is needed. On free-tier limits, a single user can therefore ask only one or two questions a minute per model; a paid tier is required for team use.

The local provider (`AIDA_MODEL_PROVIDER=local`, llama.cpp on `127.0.0.1:8081` with a JSON-schema grammar) remains available for deployments where question text must not leave the server. The earlier Qwen3-4B baseline answered 19/40 supported logistics questions correctly with the previous pipeline ([BLIND_EVALUATION.md](BLIND_EVALUATION.md)); it has not been re-measured with AIDA 4.

## What the model sees

The model never connects to a database and never generates SQL. For one uncached question it receives:

- the question text, marked as untrusted data;
- a compact catalog projection: temporary ids (`m#` measures, `d#` groupings, `f#` record fields, `r#` related record sets and `r#f#` their fields), owner-approved labels, plain-language definitions, allowed values, the data date range, the reference date, the currency and the source's capability limits;
- in the planning step, the mentions it resolved in the first step; and
- in a repair round, its own rejected reply and the exact rejection reason.

It does not receive rows, query results, SQL, physical table or column names, join keys, file paths, connection details, credentials, account details or onboarding answers. Code maps the returned ids through the approved catalog, selects join paths, compiles parameterized read-only SQL and runs calculations after aggregation.

With the Groq provider, this payload leaves the server over HTTPS. Onboarding shows exactly what is sent and requires consent. Prompt Guard 2 screens the question before the planning model sees it.

Ask in business language. Conflicting physical names are resolved by explicit business mappings: for example, **Average sale price** and **Catalog price** map to different `UnitPrice` columns. When a phrase could mean more than one approved item, AIDA asks you to choose.

Calculations on top of approved measures are now supported: ratio, difference, share of total, running total and percent change. They use only measures that exist in the catalog, so "profit" works only when the catalog has both a revenue measure and a cost measure and the question asks for their difference. The model never invents a business formula.

## Databases to use for testing

No production customer database is required for development or a demonstration.

| Source | Data status | Best use | Availability |
| --- | --- | --- | --- |
| Commerce demo | Generated synthetic data | Single-table questions, dates, filters, share and running-total calculations | Created at startup |
| Support operations | Generated synthetic data with a different schema | Source switching and independent definitions | Created at startup |
| Retail warehouse (`warehouse`) | Generated synthetic multi-table data | Join paths, same-named fields, fact grain, EXISTS, archives, ratio calculations | Created at startup |
| SaaS billing (`billing`) | Generated synthetic multi-table data | A second relational schema, distinct counts, HAVING and per-seat ratios | Created at startup |
| Chinook (`chinook`) | Public third-party sample with retained MIT license and checksum | Independently produced 11-table schema with conflicting price and country meanings | Tracked under `fixtures/chinook/` |
| Logistics sample | Generated synthetic 12-table data | Difficult joins, filters, HAVING, subqueries, UNION, nulls, empty results and refusals | `fixtures/blind_logistics/`; installed privately per account from onboarding or `POST /api/v1/samples/logistics` |

The 50 logistics questions are no longer an unseen benchmark, because their outcomes have been inspected and the prompt was improved using the failures. They are regression tests. A genuine transfer claim needs a new, independently authored database and question set, frozen before inference.

For a private customer pilot, use a de-identified SQLite snapshot and an owner-reviewed catalog. Confirm the fact grain, metrics, field meanings, join cardinalities, date basis, allowed values and archive behaviour before running questions. Do not publish customer data as test evidence.

## Verification order

Deterministic checks first:

```powershell
.\.venv\Scripts\python.exe -m pytest backend -q
.\.venv\Scripts\python.exe validate_structure.py
Set-Location frontend; npm run typecheck; npm run build; Set-Location ..
```

Then real inference, which uses provider quota and can be resumed across rate-limit windows:

```powershell
.\.venv\Scripts\python.exe scripts/benchmark_nl.py --label candidate --subset selection --model openai/gpt-oss-20b
.\.venv\Scripts\python.exe scripts/benchmark_nl.py --label candidate --resume --patience 1800
.\.venv\Scripts\python.exe scripts/report_benchmark.py --runs candidate --suites candidate
```

Then the browser journey, with the backend and frontend running:

```powershell
$env:AIDA_BASE_URL = "http://localhost:3000"; node scripts/e2e-auth.cjs
```

Score model answers only against independently written SQL and intended plans; a plausible chart is not evidence of correctness. `scripts/evaluate_blind.py` reproduces the sealed pre-AIDA 4 logistics assessment and must not be described as a new blind result.

The next customer gate should require:

- no accepted answer with changed meaning, and no execution of unsupported or ambiguous intent;
- at least 90% correct supported answers on a newly sealed transfer set;
- at most three model calls and one guard call per question;
- acceptable p95 latency on the paid provider tier or target hardware; and
- zero prohibited fields (rows, SQL, physical names, secrets) in captured model payloads.

## Repository cleanup status

AIDA 4 removed the previous interpretation modules (`backend/core/semantic.py`, `backend/core/relational_semantic.py`), their tests and `scripts/evaluate_semantics.py` / `scripts/evaluate_relational.py`. The earlier V1 leftovers (chat UI, V1 compiler and LLM modules, old stores and components) were moved into the gitignored `archive/v1-untracked/` folder. `docs/VERIFICATION.md` and `docs/RELATIONAL_VERIFICATION.md` are kept as clearly labelled historical records.

| Item | Current status | Recommended action |
| --- | --- | --- |
| `archive/` | 200 untracked, gitignored files (1.5 MB) of V1 code | Delete once nobody needs to consult the V1 implementation |
| `backend/venv/` | Old, unused, gitignored virtual environment (149 MB); the project uses `.venv/` | Delete locally |
| `Research/` | Three tracked PDFs (2.4 MB) not referenced by the product | Remove from the product branch, or document their provenance and purpose |
| `docs/evidence/` | 68 tracked files (12 MB) from the pre-AIDA 4 pipeline | Keep compact summaries and screenshots; move bulky raw responses to a release asset or evidence branch; sanitize local paths before publishing |
| `scripts/e2e.cjs`, `e2e-blind.cjs`, `e2e-relational.cjs` | Browser suites for the single-call local-model version; not updated for accounts or the two-stage pipeline | Port to authenticated sessions or retire in favour of `scripts/e2e-auth.cjs` plus new journeys |
| `scripts/start-demo.ps1` | Starts the local-model stack; not re-verified with AIDA 4 or accounts | Re-verify with `AIDA_MODEL_PROVIDER=local`, or document Groq-only startup |
| `frontend/.env` | Gitignored; its only key, `NEXT_PUBLIC_API_URL`, is not read by any code | Remove the unused key |
| Untracked `pictures/`, `query.md` | Personal working files | Keep out of commits |

Do not delete the SQLite fixtures, reviewed catalogs, licenses, tests or startup scripts merely because they are not in the request path; they provide reproducibility and onboarding examples. Never commit `backend/.env`, downloaded model weights, customer snapshots, dashboard state or local process logs. Before a public release, run a secret and local-metadata scan, verify every documentation link, build from a fresh clone and rotate any API key that has appeared in logs or transcripts.
