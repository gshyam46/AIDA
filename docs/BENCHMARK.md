# AIDA benchmark report

Status on September 13, 2026: **complete for the pre-AIDA 4 baseline and the first AIDA 4 prompt; partial for the final prompt.** The final prompt-3 runs are paused. Groq's free tier caps each model at 200,000 tokens per rolling 24 hours, and all three models reached about 197,000 tokens during this day's runs, so no further questions can be scored until that usage ages out (from early September 14). Full per-question outcomes are in [BENCHMARK_DATA.md](BENCHMARK_DATA.md).

Resume the final runs once quota is available. They skip questions already scored and retry ones the provider could not serve:

```powershell
.\.venv\Scripts\python.exe scripts/benchmark_nl.py --label aida4v3-two_stage-qwen3.8-27b-selection --subset selection --model qwen/qwen3.8-27b --pipeline two_stage --repair 1 --resume --patience 7200
.\.venv\Scripts\python.exe scripts/benchmark_nl.py --label aida4v3-two_stage-gpt-oss-20b-selection --subset selection --model openai/gpt-oss-20b --pipeline two_stage --repair 1 --resume --patience 7200
.\.venv\Scripts\python.exe scripts/benchmark_nl.py --label aida4v3-two_stage-gpt-oss-120b-selection --subset selection --model openai/gpt-oss-120b --pipeline two_stage --repair 1 --resume --patience 7200
```

Each model has its own quota, so the three commands can run in separate terminals at the same time. A run that stops on rate limits exits with code 3; run the same command again later.

## Method

- `scripts/benchmark_nl.py` sends each question through the real application path (`HybridAnalytics.query`): Prompt Guard, the LLM interpreter, code validation, SQL compilation and calculations. Nothing is stubbed.
- Expected rows come from independently written SQL. Calculation cases use independently written oracles. Expected plans and rows are never sent to the model.
- A supported question counts as correct only when both the plan and the rows match. A refusal case counts as correct only when AIDA does not answer.
- **Wrong or unsafe** counts accepted answers with the wrong meaning, wrong rows, or an answer to a request that should have been refused. **False refusal** counts supported questions that AIDA asked to clarify.
- Latency excludes time spent waiting on provider rate limits. Cost is estimated from Groq list prices and the reported token usage.
- The 42-question `selection` subset mixes all five suites (logistics regression, relational demos, semantic regression, calculations and name resolution) and deliberately over-represents cases that failed before. It is harder than the full suite.
- These questions are regression tests, not a blind transfer test: the prompt was improved after looking at their failures.

## Before and after

**Before** is the previous single-call pipeline, with hand-written language rules and candidate lists, running on `openai/gpt-oss-120b`. It covered the logistics (50) and relational (34) suites before its daily quota ran out. **After** is the AIDA 4 LLM-first pipeline.

On the 27 questions scored by every complete run:

| Run | Supported correct | Refusals correct | Overall | Wrong or unsafe | False refusals |
| --- | --- | --- | --- | --- | --- |
| Before · gpt-oss-120b | 10/22 | 3/5 | 13/27 (48.1%) | **3** | 11 |
| AIDA 4 · gpt-oss-20b · prompt 1 | 11/23 | 3/4 | 14/27 (51.8%) | 1 | 12 |
| AIDA 4 · qwen3.8-27b · prompt 1 | 15/23 | 4/4 | **19/27 (70.4%)** | **0** | 8 |

One case, `logistics:BL42` (a ratio), was a required refusal before calculations existed and is scored as an answer for AIDA 4. That is why the supported and refusal totals differ by one.

On its own 84-question run, the old pipeline scored 58/71 supported and 11/13 refusals, with 3 wrong or unsafe answers. Those suites contain many simpler questions, so the number is not comparable with the harder 42-question selection.

What changed besides accuracy:

- AIDA 4 answers calculation questions the old pipeline had to refuse: ratios, shares, running totals and period change, 3/3 in the calculation suite.
- It asks the user to choose when a name is ambiguous, 2/2 in the resolution suite.
- It produced no wrong answers with Qwen3.8 27B.

## Model comparison (AIDA 4, prompt 1, no repair, full 42-question selection)

| Model | Supported correct | Refusals correct | Wrong or unsafe | False refusals | Tokens / question | Median answer | Cost for 42 questions |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `openai/gpt-oss-20b` | 18/32 (56.2%) | 9/10 | 2 | 13 | 3,469 | 1.54 s | $0.014 |
| `qwen/qwen3.8-27b` | **22/32 (68.8%)** | **10/10** | **0** | 10 | 3,420 | 1.75 s | $0.160 |
| `openai/gpt-oss-120b` | not measured on AIDA 4: its daily token quota was exhausted by the baseline run | | | | | | |

Qwen3.8 27B by suite: calculations 3/3, name resolution 3/3, logistics 12/18 supported and 4/4 refusals, relational demos 3/5 and 1/1, semantic regression 3/5 and 3/3.

## Prompt fixes and one repair round (prompt 3, partial)

The first AIDA 4 runs exposed generic interpretation gaps. They were fixed in the prompt and the validation loop, not with dataset-specific rules:

- The model clarified behaviour that AIDA handles automatically: tie ordering, groups with missing values, which date a period applies to, and current versus archived records.
- It wrote one mention for a phrase that names two items ("carrier and service-class pairs") instead of one mention per id.
- It cited related-record conditions with the wrong role, and treated "except Freight" as ambiguous instead of a `ne` filter.
- It treated "in dollars" as a unit conversion, although currency measures are already reported in dollars.
- It missed the measure in "how many legs" or "count X". When code rejects a reply for a reason like this, one repair round (`AIDA_REPAIR_ATTEMPTS=1`) now sends the exact rejection back to the model.

Early results on the questions both versions have scored so far:

| Model | Prompt 1, no repair | Prompt 3, one repair | Wrong or unsafe |
| --- | --- | --- | --- |
| gpt-oss-20b | 4/6 | 5/6 | 0 → 0 |
| qwen3.8-27b | 5/6 | **6/6** | 0 → 0 |

Six questions are too few to claim an improvement; the resumed runs will fill in the rest.

The cost of repair: gpt-oss-20b triggered a repair on about half of its logistics questions, raising those questions to about 8,000 tokens. Qwen3.8 27B needed no repairs on the same questions and stayed at about 5,200 tokens.

## Engineering: latency, cost and the deterministic engine

Both sources below are recorded measurements, not estimates:

- **Model runs** (`scripts/benchmark_nl.py` telemetry). Groq's free tier made almost every question wait on rate limits, so waits are removed. Model time is the recorded model latency minus the recorded wait. AIDA's own time is total elapsed minus Prompt Guard minus model latency, because the waits sit inside the model latency.
- **No-model engineering benchmark** (`scripts/benchmark_engine.py`). Every expected plan is run 5 times with result caches cleared and compared with independent SQL. Recorded model outputs are replayed through the current code. The backend tests, the latest browser journey and the configured limits are recorded too.

### Where the time goes

Average per question, with provider waits removed:

| Run | Median answer | p95 answer | Prompt Guard | Model calls | AIDA code and SQL |
| --- | --- | --- | --- | --- | --- |
| gpt-oss-20b · prompt 1 (42 questions) | 1.54 s | 2.00 s | 154 ms | 1.34 s | 4.0 ms |
| Qwen3.8 27B · prompt 1 (42 questions) | 1.75 s | 2.38 s | 155 ms | 1.51 s | 4.4 ms |
| gpt-oss-20b · prompt 3, partial (9 questions) | 2.64 s | 21.96 s | 158 ms | 1.98 s | 7.7 ms |
| Qwen3.8 27B · prompt 3, partial (6 questions) | 2.13 s | 2.40 s | 151 ms | 1.89 s | 8.7 ms |

- **Model calls** take 89–92% of the measured answer time.
- **AIDA's own code and SQL** take 0.3–0.4%.
- **Resolve and plan** are recorded per stage, but for the older runs the split is only clean on the few questions that never waited. New runs record waits, inference time and database time per stage.

### Cost and tokens

| Run | Cost per 1,000 questions | Input tokens | Output tokens | Model calls per question | Questions with a repair round |
| --- | --- | --- | --- | --- | --- |
| gpt-oss-120b · previous pipeline | $0.48 | 2,281 | 235 | 0.95 | — |
| gpt-oss-20b · prompt 1 | $0.33 | 3,151 | 318 | 1.60 | 0% |
| Qwen3.8 27B · prompt 1 | $3.81 | 3,084 | 336 | 1.60 | 0% |
| gpt-oss-20b · prompt 3, partial | $0.60 | 5,996 | 507 | 2.44 | 44% |
| Qwen3.8 27B · prompt 3, partial | $5.44 | 4,790 | 402 | 2.00 | 0% |

- **Input dominates.** Input tokens (instructions plus the approved catalog) are 90–92% of all tokens.
- **Prompt 3 costs more.** The longer prompt and the repair round raised tokens per question from 3,469 to 6,502 for gpt-oss-20b and from 3,420 to 5,192 for Qwen3.8 27B, on the questions each partial run scored.
- **Provider waits on the free tier:**

  | Run | Questions that waited | Time lost |
  | --- | --- | --- |
  | gpt-oss-20b · prompt 1 | 39 of 42 | 13 min |
  | Qwen3.8 27B · prompt 1 | 41 of 42 | 16 min |
  | gpt-oss-20b · prompt 3, partial | 8 of 9 | 46 min |
  | Qwen3.8 27B · prompt 3, partial | 5 of 6 | 3 min |

### Deterministic engine and replay (no model)

- **Engine correctness:** 129 of 129 expected plans returned the independent oracle rows (logistics 40, relational 31, semantic 55, name resolution 3).
- **Engine speed:**
  - uncached plan, request to result: median 2.3 ms, p95 8.6 ms;
  - logistics plans: median 5.5 ms, p95 12.6 ms;
  - time inside the database: median 2.2 ms;
  - repeat from the result cache: median 0.13 ms.
- **Replay:**
  - all 56 recorded AIDA 4 answers (20 + 22 + 8 + 6) reproduced the identical plan and identical rows through today's code;
  - code and SQL per question: median 4.3–7.7 ms, p95 about 13 ms;
  - a repeated question: median 0.4–0.6 ms, with no model call in all 56 cases.
- **Quality gates:**
  - 323 backend tests passed, including 32 attack and misuse tests and 34 interpreter contract tests;
  - the browser journey passed 9 of 9 steps.

Reproduce the engineering figures and include them in the report and website data:

```powershell
.\.venv\Scripts\python.exe scripts/benchmark_engine.py --runs aida4-two_stage-gpt-oss-20b-selection aida4-two_stage-qwen3.8-27b-selection aida4v3-two_stage-gpt-oss-20b-selection aida4v3-two_stage-qwen3.8-27b-selection
```

Then add `--engine artifacts/benchmark/engine.json` to the `report_benchmark.py` command below.

## Decision

The **provisional default is `qwen/qwen3.8-27b` with the two-stage pipeline and one repair round**. It has the highest accuracy, the only perfect refusal record and zero wrong answers in the complete AIDA 4 runs, and it stays ahead in the partial prompt-3 run. For a data analyst, an answer with the wrong meaning is the costliest failure, so accuracy and refusal quality outweigh price here. At about $0.004 per question it is still inexpensive, although gpt-oss-20b is roughly ten times cheaper.

Constraints to plan for:

- Qwen3.8 27B allows only 1,000 output tokens per minute on the free tier, so answers queue under concurrent use. Team use needs a paid Groq tier.
- gpt-oss-120b could not be evaluated on AIDA 4 today. It should be compared once its quota allows, with the same command and label pattern.
- The decision becomes final when the three prompt-3 runs complete. Regenerate this report's data tables, `docs/TEST_QUERIES.md` and the website's `/benchmarks` page data with:

```powershell
.\.venv\Scripts\python.exe scripts/report_benchmark.py --runs baseline-current-gpt-oss-120b aida4-two_stage-gpt-oss-20b-selection aida4-two_stage-qwen3.8-27b-selection aida4v3-two_stage-gpt-oss-20b-selection aida4v3-two_stage-qwen3.8-27b-selection --shared baseline-current-gpt-oss-120b aida4-two_stage-gpt-oss-20b-selection aida4-two_stage-qwen3.8-27b-selection --suites aida4v3-two_stage-qwen3.8-27b-selection --tables docs/BENCHMARK_DATA.md --queries-run aida4v3-two_stage-qwen3.8-27b-selection --web frontend/app/benchmarks/benchmark-data.json --engine artifacts/benchmark/engine.json
```

Add `aida4v3-two_stage-gpt-oss-120b-selection` to `--runs` once it has scored questions, and switch `--shared` to the prompt-3 runs when they complete.

## Remaining gaps

- Even the best configuration still asks for clarification on about a third of the hard supported questions (10 of 32 with prompt 1). Those are safe failures, but they cost the user a rephrase.
- The selection is a regression set. A newly authored, sealed database and question set is needed before claiming accuracy on unseen data.
- Rate limits and daily quotas on the free tier, not model quality, currently limit how many questions a user can ask.
