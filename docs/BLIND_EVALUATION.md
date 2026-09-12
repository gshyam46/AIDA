# AIDA first transfer assessment — 12 September 2026

**The natural-language product fails the release gate on this new database.** The deterministic execution path passed all 40 explicit plans against independent SQL, but the frozen end-to-end interpreter answered only **19/40 supported questions correctly (47.5%)**. It also executed six requests that should have been refused or clarified. No product tuning or question changes were made during this assessment.

This supersedes any inference that the earlier 31-question relational regression establishes broad language reliability. Those regression results remain valid for their fixed questions. This assessment is evidence for what needs fixing before presenting AIDA as reliable across new databases.

## What was tested

The newly generated [logistics fixture](../fixtures/blind_logistics/README.md) has 12 tables, 12,000 current shipment legs, 3,000 archive legs, 120 exact overlaps, and 9,189 exception events. It includes repeated child events, nullable measures, missing relationships, multiple join paths, misleading same-named charge/status fields, and unrelated fake credential/contact tables. No customer database was used.

A separate question author, without reading the product parser, prompts, compiler or previous cases, wrote [50 questions and independent SQL oracles](../fixtures/blind_logistics/questions.json): 40 within the published feature boundary and 10 requiring refusal or clarification. [Coverage](../fixtures/blind_logistics/coverage.md) includes joins, WHERE/IN, HAVING, EXISTS/NOT EXISTS, grouped above-average subqueries, UNION/UNION ALL, dates, ordering, nulls and empty results.

This is an **agent-authored synthetic transfer assessment**, not an external human benchmark or a representative sample of customer questions. Business definitions and joins were explicitly mapped in an owner-style catalog; automatic discovery of arbitrary business semantics was not tested.

The [preregistered protocol](BLIND_EVALUATION_PROTOCOL.md) and [product seal](evidence/blind/protocol-seal.json) fix the application, model manifest, scoring and repeat selection. The evaluator seals the final database, catalog, questions and evaluator before inference. It uses the unchanged FastAPI upload/configure/query endpoints in an isolated in-process application and the original transport to the actual local Qwen model. Expected plans and SQL results never enter model requests. All first-pass questions were submitted once, with no adaptive retry or answer substitution.

## Frozen first-pass results

| Measure | Result |
| --- | --- |
| Supported explicit plans vs independent SQL | **40/40** |
| Supported language questions answered correctly | **19/40 — 47.5%** |
| Supported questions falsely refused | **13/40** |
| Supported questions accepted with changed meaning | **8/40** |
| Unsupported/ambiguous questions correctly refused | **4/10** |
| Unsupported/ambiguous questions incorrectly executed | **6/10** |
| Correct answers among all accepted answers | **19/33 — 57.6%** |
| Correct outcomes including refusals | **23/50 — 46%** |
| HTTP, model availability or execution failures | **0** |
| SQL row mismatches with a semantically equivalent accepted plan | **0** |
| Actual model requests | **49**; raw-contact request rejected before inference |
| Maximum model requests per initial question | **1** |
| Model latency | median **6.06 s**, p95 **10.24 s**, maximum **18.69 s** |
| Model token usage | **90,730 input + 6,015 output** |
| Hosted inference API charges | **$0**; local hardware/electricity not measured |
| Observed first-pass model-payload privacy failures | **0/49** |

Scoring requires both equivalent intent and independent SQL rows. It preserves requested filters, units, populations, dates, grouping, metrics, limits and explicit ordering. Unrequested presentation sort order is ignored according to flags authored before inference. The eight changed meanings are not all equally severe: BL18 returned the requested charge totals correctly but added an unsolicited weight metric; the other examples below alter or omit requested answers.

The preregistered accuracy and no-wrong-answer gates failed. The one-call bound, measured p95 target and payload audit passed. Fast and inexpensive incorrect answers do not make the product ready.

## Repeatability, runtime and privacy

The ten positions selected before inference produced the same plans/results or refusals in **10/10 session replays and 10/10 fresh-parser repeats**. Each selection contained six correct answers, three wrong meanings and one false refusal. Nine session replays used zero model calls; the rejected interpretation was not cached and made one new call. All ten fresh-parser repeats made one real model call each. The model server remained running, so these are fresh interpretation-cache tests, not independent model-process restarts. Database result caches were also available during repeats.

There were **60 actual model requests** across the first pass and repeat checks: 111,065 input tokens and 7,697 output tokens. Replays had median API latency 2.20 ms, but the uncached refusal took 18.36 seconds. Fresh-parser repeats had median model latency 7.45 seconds and p95/max 25.35 seconds. Thus the first-pass p95 target passed, while the repeat sample shows meaningful tail-latency variation. Explicit-plan preflight database time had median 4.96 ms and p95 11.29 ms; first-pass successful query database time had median 5.14 ms.

The runtime was the pinned Qwen3-4B-Instruct-2507 Q4_K_M model with llama.cpp b10809, Vulkan offload, one inference slot, a 4,096-token context and auxiliary RAM caching disabled. The machine ran Windows 11 Pro on an Intel Core Ultra 7 266V with approximately 16 GB RAM. The [pre-run observation](evidence/blind/runtime.json) recorded low free system RAM; no unrelated processes were stopped. This is one-machine latency evidence, not a controlled hardware benchmark.

The observed destination for every recorded request was `http://127.0.0.1:8081/v1/chat/completions`. The original transport received unmodified requests containing the question, projected approved business catalog, contract examples and time candidates. All **60/60 payload checks passed**, including checks against 72 fake sensitive-value canaries per request; source inspection also contained none of those canaries. No oracle plans, SQL answers, database rows or result rows were sent to interpretation.

The audit checks prohibited structures and distinctive physical identifiers; it cannot distinguish every physical name from an identical approved business noun. It is an observed payload boundary check, not a proof of aggregate anonymity, authorization or system-wide network isolation. The fixture is synthetic. The six “unsafe acceptances” mean unsupported or ambiguous analytics executed; they are not six observed personal-data leaks.

All 40 explicit plans passed required lineage checks. The two accepted-language lineage failures are BL13 and BL33, where omitted question filters remove expected columns from the plan. The evidence does not demonstrate incorrect lineage for the SQL actually executed. Conversely, six wrong accepted supported answers still pass the expected-lineage subsets: lineage is useful execution evidence, not proof of semantic correctness.

## Concrete failures and next fixes

| Priority | Evidence | Required change and verification |
| --- | --- | --- |
| P0: preserve every requested operation | BL12 drops the requested leg count; BL13 drops the four-package filter and chooses weight; BL17 uses a fact-level charge predicate instead of a weight HAVING condition. | Add a bounded, source-grounded representation of requested clauses and verify coverage and aggregation level before execution. Missing or unsupported clauses must cause clarification. Keep one model call and deterministic SQL compilation; do not solve this by an agent retry loop. Verify meaning and independent answers on paraphrases and counterexamples. |
| P0: refuse unsupported or contradictory meaning | BL43 turns cumulative charges into ordinary monthly totals; BL45 invents a performance metric; BL46 guesses an ambiguous area grouping; BL48 drops the contains-Air condition; BL49 accepts contradictory archive scope; BL50 totals UNION ALL records without the required latest-per-consignment selection. | Represent unresolved calculations, ambiguity and conflicting scopes explicitly. Reject a partial supported plan when part of the request remains unrepresented. Test these six cases plus independently authored alternatives. |
| P0: repair candidate restrictions | BL13's constrained schema omits the number-word value `four`; BL30/BL32 permit only UNION ALL despite explicit duplicate-removal wording; BL25 omits the requested numeric child-event predicate. | Separate hard catalog/type/security restrictions from uncertain language cues. A keyword matcher must not make an approved requested operation impossible to express. Compare allowed candidates with requested spans before interpreting, and test numbers, child predicates, units and duplicate-handling paraphrases. |
| P0: fix date interpretation | BL33 drops last-calendar-month and returns all current data; BL35 filters the correct day but reports weight instead of count. BL24 treats the ordinary verb `may` as a month candidate. | Ground date spans in context, preserve explicit date intent and validate the requested metric independently. Test date/ordinary-word collisions and equivalent date wording against fixed reference dates. |
| P1: remove false conditions introduced by guards | Correct-looking model interpretations for BL07/BL08/BL16/BL28/BL34/BL38 are rejected as missing literal filters. BL20 is treated as requesting ranking because it says `at least one`. | Make guard evidence correspond to actual predicates and ranking clauses. Do not extend stop-word lists one benchmark phrase at a time. Test grouping modifiers, ties, entity counts and existence wording without weakening structural validation. |
| P1: improve source onboarding and refusal messages | Harmless category columns ending `_name` had to be renamed before onboarding; BL42/BL44 refuse for incidental reasons instead of explaining the unsupported calculation. | Provide an explicit reviewed reporting-field classification or document prepared reporting schemas; keep sensitive fields denied. Explain the actual unresolved capability to the user. |

The compiler does not need to be replaced based on these results. Its 40-plan success isolates the immediate work to interpretation, candidate construction and validation. Independent post-outcome review found seven false refusals after a raw model interpretation matching the intended meaning, and five cases where the generated request schema excluded a required operation. Attributing every failure to insufficient model size would be inaccurate. These results do not establish that the present model is sufficient for all future language coverage either.

The current question set becomes a regression set once used for fixes. A second independently authored, sealed transfer set is required after those fixes; rerunning these 50 questions must not be advertised as a fresh blind result.

## Secondary browser integration

After the first pass and repeats were complete, a separate browser session uploaded and configured the same database through the existing website. Six fixed questions were then submitted through the actual question controls. All six reproduced the first-pass outcomes: BL01/BL29/BL37 answered correctly, BL08 was falsely refused, BL17 executed the wrong meaning, and BL42 refused. That is **4/6 correct outcomes**, including the refusal. This secondary sample is not added to the 50-case accuracy denominator.

All **15 integration checks passed**. The visible structured builder executed BL08, BL17 and BL10 against the independent rows with zero model calls; bar, scatter, donut, line and area charts rendered; tables, CSV and visible SQL lineage matched the actual responses. A saved dashboard survived page reload and refreshed its original plan with zero model calls. No uncaught browser errors or external browser requests were observed. Root review also inspected the [scatter plot](evidence/blind/browser/structured-BL08-scatter.png) and [refreshed dashboard](evidence/blind/browser/dashboard-refreshed.png).

The [browser report](evidence/blind/browser/report.json) deliberately has `completed_with_failures` status and exit code 1 because the two language failures remain. Six real model calls were made in that secondary run. It uses no response mocks and does not inject expected rows into the UI. Its browser-local dashboard storage is test evidence; it does not populate dashboards in a separate user browser.

An [earlier browser attempt](evidence/blind/browser-initial-failure/report.json) stopped before any language question when Edge evicted an upload response body from its inspection cache. A harness-only change verified the actual rendered 12-table inspection and configuration response instead. Both attempts are preserved; no product fix or changed question was needed for the integration rerun.

The configured **Blind synthetic logistics** source remains available in the local demo at **http://127.0.0.1:3000**. It is a locally uploaded source, not added to public-demo mode. The final browser check observed frontend HTTP 200 and the local model ready. No public deployment was performed.

## Evidence and reproduction

- [Immutable first 50 outcomes](evidence/blind/first-run-primary.json): each question, intended plan, independent SQL/rows, actual API result, actual model request/response and payload audit.
- [Complete run with repeat checks](evidence/blind/first-run.json).
- [Compact derived statistics and per-case differences](evidence/blind/summary.json).
- [Independent post-outcome case review](evidence/blind/case-review.md).
- [Runtime observation](evidence/blind/runtime.json).
- [Evaluation code](../scripts/evaluate_blind.py) and [scorer tests](../backend/test_blind_evaluator.py).

The full backend suite passed **455 tests** after the frozen model run, including 12 evaluation-scorer tests. There were two upstream deprecation warnings. These tests validate implementation contracts; their passing result does not override the failed language assessment.

[Evidence checksums and recorded verification](evidence/blind/index.json) include the immutable first-pass SHA256 `fd31e03b8073a8a49f20631b6e17bc86d4387c6c0afd19f62f3a73b6b7d9c3d6` and complete-run SHA256 `02beb622af159e942b177af31ceb1515778f472918837c6ed2e817ec8c48479e`.

With the pinned local model running, use new output paths so earlier evidence is never overwritten:

```powershell
.\.venv\Scripts\python.exe scripts/evaluate_blind.py --preflight-only --output artifacts/blind/new-preflight.json
.\.venv\Scripts\python.exe scripts/evaluate_blind.py --output artifacts/blind/new-run.json
.\.venv\Scripts\python.exe scripts/report_blind.py --input artifacts/blind/new-run.json --output artifacts/blind/new-summary.json
```

The secondary [browser script](../scripts/e2e-blind.cjs) defaults to the original local artifact paths and verifies their seals. After the evaluator finishes, run it by itself with `$env:AIDA_BLIND_BROWSER_SLOT='granted'` followed by `node scripts/e2e-blind.cjs`; unset that task-specific variable afterwards. It creates a new timestamped artifact directory and a new uploaded source. A failing language outcome causes a nonzero exit even when all UI checks pass.

Running this again measures repeatability or regression, not a new unseen-question test. Product hash changes intentionally stop the original sealed evaluator. Use a separately labeled protocol and evidence directory for a future modified product; preserve this baseline seal.
