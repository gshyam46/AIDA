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

## Decision

The **provisional default is `qwen/qwen3.8-27b` with the two-stage pipeline and one repair round**. It has the highest accuracy, the only perfect refusal record and zero wrong answers in the complete AIDA 4 runs, and it stays ahead in the partial prompt-3 run. For a data analyst, an answer with the wrong meaning is the costliest failure, so accuracy and refusal quality outweigh price here. At about $0.004 per question it is still inexpensive, although gpt-oss-20b is roughly ten times cheaper.

Constraints to plan for:

- Qwen3.8 27B allows only 1,000 output tokens per minute on the free tier, so answers queue under concurrent use. Team use needs a paid Groq tier.
- gpt-oss-120b could not be evaluated on AIDA 4 today. It should be compared once its quota allows, with the same command and label pattern.
- The decision becomes final when the three prompt-3 runs complete. Regenerate this report's data tables, `docs/TEST_QUERIES.md` and the website's `/benchmarks` page data with:

```powershell
.\.venv\Scripts\python.exe scripts/report_benchmark.py --runs baseline-current-gpt-oss-120b aida4-two_stage-gpt-oss-20b-selection aida4-two_stage-qwen3.8-27b-selection aida4v3-two_stage-gpt-oss-20b-selection aida4v3-two_stage-qwen3.8-27b-selection --shared baseline-current-gpt-oss-120b aida4-two_stage-gpt-oss-20b-selection aida4-two_stage-qwen3.8-27b-selection --suites aida4v3-two_stage-qwen3.8-27b-selection --tables docs/BENCHMARK_DATA.md --queries-run aida4v3-two_stage-qwen3.8-27b-selection --web frontend/app/benchmarks/benchmark-data.json
```

Add `aida4v3-two_stage-gpt-oss-120b-selection` to `--runs` once it has scored questions, and switch `--shared` to the prompt-3 runs when they complete.

## Remaining gaps

- Even the best configuration still asks for clarification on about a third of the hard supported questions (10 of 32 with prompt 1). Those are safe failures, but they cost the user a rephrase.
- The selection is a regression set. A newly authored, sealed database and question set is needed before claiming accuracy on unseen data.
- Rate limits and daily quotas on the free tier, not model quality, currently limit how many questions a user can ask.
