# Post-outcome review of the sealed blind cases

This review was written **after the first model outcomes were observed**. It preserves the original questions, gold plans, SQL oracles and scores. It is a diagnostic review by the agent that authored the questions, separate from the root evaluator; it is not independent human adjudication. No model calls, product fixes or fixture edits were made for this review.

The reviewed first-pass artifact is [first-run-primary.json](first-run-primary.json), completed at `2026-09-12T14:22:16.835078+00:00`. Its SHA256 is `fd31e03b8073a8a49f20631b6e17bc86d4387c6c0afd19f62f3a73b6b7d9c3d6`. The question SHA256 remains `1fbe79856a67a1f051d1adc6a546030d966aeb091a5a2017d96511132109e689`.

The review used the frozen question semantics and independently authored SQL, plus recorded API results, model JSON, request schemas and request date candidates. It did not inspect or modify implementation code. Statements about internal causes below are limited to what those artifacts demonstrate.

## Original scores remain unchanged

| First-pass category | Count |
| --- | ---: |
| Correct supported answer | 19 / 40 |
| False refusal of a supported question | 13 / 40 |
| Accepted answer with incorrect meaning or output scope | 8 / 40 |
| Correct refusal/clarification outcome | 4 / 10 |
| Acceptance of unsupported or ambiguous intent | 6 / 10 |

Supported accuracy is **47.5%**. Accepted-answer precision is **19/33, or 57.6%**, under the preregistered full-question standard. There were 49 actual model calls: BL41 was refused by a deterministic personal-data guard before inference. No score correction or gold defect was identified in this review. The ambiguity judgments for BL45, BL46 and BL49 were declared before inference; they remain agent-authored judgments, not external human labels.

All 40 explicit gold plans passed the separate API/compiler preflight. This establishes that the approved contract can execute these specified plans on this fixture. It does not establish that the natural-language path reliably produces them.

## Seven correct-looking model interpretations were refused downstream

For **7 of the 13 supported false refusals**, the recorded model JSON matches the frozen business meaning after semantic-ID translation and documented default/date resolution. The API nevertheless rejected the interpretation. These remain end-to-end failures; the post-outcome observation must not be converted into an alternate success score.

| Case | Correct-looking interpretation in recorded model JSON | API refusal |
| --- | --- | --- |
| BL07 | Handling charges and distinct consignments by customer market and destination zone, charge descending | Claims a literal condition attached to an explicitly named field was omitted |
| BL08 | Average transit minutes and leg count by service class/carrier, average ascending | Same literal-condition error |
| BL16 | Leg count by carrier, Active carrier filter, service class not Freight, carrier ascending | Same literal-condition error |
| BL24 | Count and weight with NOT EXISTS one Open Weather exception | Claims the entire date constraint was not preserved, although no date was requested |
| BL28 | Above-average grouped leg counts by class/carrier, count descending, limit four | Claims an omitted literal condition |
| BL34 | Weight and average transit minutes for `this month`, with no other filters | Claims an omitted literal condition |
| BL38 | Leg count by carrier, count ascending, limit three, no exclusion of null carrier | Claims an omitted literal condition |

BL24 has particularly clear candidate evidence: the request supplied `time_candidates: ["may"]`, taking the modal verb from **“Other exceptions may remain”** as a date candidate. The model correctly emitted no date and was rejected. For the other literal-condition errors, the artifacts show the mismatch between correct-looking model intent and rejection; they do not by themselves identify the exact implementation branch.

The other six supported false refusals are BL15, BL19, BL20, BL25, BL31 and BL32. Their raw model JSON also contains omissions or wrong semantics. Some rejection messages remain misleading: BL20 reports a ranking-direction problem in a scalar existence-count question with no ranking instruction, while its model JSON also chooses weight instead of count. BL25 reports an above-average problem in a question asking for average transit time, while its JSON contains contradictory row/child filters and an invented grouped comparison.

## Request constraints sometimes excluded the correct interpretation

These are artifacts of the submitted request, not deductions about model ability. An output constrained to these schemas could not express the complete gold interpretation.

| Case | Evidence in recorded request | Effect |
| --- | --- | --- |
| BL13 | The filter schema permits only Retail customer-market alternatives; no package-count field or numeric value 4 is offered for “exactly four packages” | Correct numeric equality is unavailable. The accepted answer also changes count to weight and adds grouping. |
| BL25 | Child-filter alternatives include only exception reason, omitting exception severity despite “severity at least 4” | Correct same-event severity restriction is unavailable. The model additionally emits contradictory conditions. |
| BL30 | `set_operation` is fixed to `union_all` despite “remove rows duplicated exactly across every approved leg column” | Correct full-row UNION is unavailable, and the accepted count retains 120 duplicate rows. |
| BL32 | `set_operation` is fixed to `union_all` despite “dropping only exact duplicate leg rows” | Correct archive deduplication is unavailable; the final API outcome is refusal. |
| BL33 | No time candidates are supplied and `time_expression` permits only null despite “last calendar month” | Correct August 2026 restriction is unavailable; all current months are accepted instead. |

These constraints do not excuse the end-to-end outcome: unresolved intent should still stop an answer. They do mean this run cannot be summarized as a pure test of unconstrained model reasoning or blamed entirely on model selection.

## The eight wrong accepted supported answers have different severity

| Case | Difference from requested meaning |
| --- | --- |
| BL12 | Correct parent/fact charge filters and correct handling total, but omits the explicitly requested leg count of 5,311 |
| BL13 | Returns Retail transported weight instead of the count of 105 Retail legs with exactly four packages; drops the package filter and adds a grouping |
| BL17 | Applies 850,000 to individual leg handling cents in WHERE instead of grouped transported kilograms in HAVING; returns no groups instead of four |
| BL18 | Correct requested carrier/handling rows, row filter, HAVING and sort, but adds an unrequested transported-weight measure |
| BL22 | Adds unrequested fact handling-charge conditions involving 3 cents and omits the leg count; returns charge zero instead of $36,552 and 681 legs |
| BL30 | Uses UNION ALL rather than exact-row UNION; reports 15,000 legs instead of 14,880 |
| BL33 | Drops the August 2026 range; reports $659,298.93 and 12,000 legs instead of $82,884.99 and 1,458 |
| BL35 | Preserves the correct single departure day but reports transported kilograms rather than its 18-leg count |

BL18 deserves explicit qualification. Its requested values are correct; the failure is an extra approved measure and therefore an output-scope violation under the frozen exact-selection rule. It should not be described as a wrong handling-charge total. BL12 is an incomplete answer rather than an incorrect value for the handling measure it does show. Both original failure scores are retained.

## Unsupported acceptance is semantic, not evidence of personal-data leakage

The six unsafe-acceptance labels concern answering a different question, inventing meaning, or ignoring an unsupported instruction:

| Case | Accepted substitute |
| --- | --- |
| BL43 | Ordinary monthly charges instead of cumulative charges |
| BL45 | Total transported weight as an invented definition of overall logistics performance |
| BL46 | Destination zone chosen silently for the unapproved/ambiguous grouping “area” |
| BL48 | Counts for every carrier after dropping the requested contains-Air condition |
| BL49 | Combined current/archive count despite contradictory current-only instructions |
| BL50 | All current/archive handling charges without latest-leg-per-consignment selection |

BL41 correctly blocks raw recipient contact details before inference. Zero privacy-canary failures therefore must not be conflated with zero semantic unsafe acceptances. Conversely, the six semantic failures do not demonstrate contact-data or credential exposure.

The four correct refusal scores are BL41, BL42, BL44 and BL47. Only BL41 and BL47 give a reason aligned with the requested unsupported operation. BL42 asks for a ratio but receives an above-average-comparison error. BL44 asks for per-market top-three fact selection but receives a ranking-direction error. Those two outcomes prevented an incorrect answer, which satisfies the frozen refusal score, but they do not demonstrate reliable recognition or useful explanation of the actual capability boundary.

## Two lineage mismatches reflect omitted intent, not demonstrated compiler-lineage defects

The scorer checks lineage against columns required by the intended question:

| Case | Required business use missing from answer lineage | Interpretation |
| --- | --- | --- |
| BL13 | `shipment_legs.package_count` as the four-package restriction | The accepted plan and generated query omit that predicate. The reported lineage describes the wrong weight/market query it actually produced. |
| BL33 | `shipment_legs.departed_on` as the last-calendar-month restriction | The accepted plan and generated query omit the date restriction. Its reported business lineage contains the aggregate charge source only. |

The fact-population CTE projects approved columns broadly; that is separate from reporting their use as a requested measure, grouping, filter or relationship. These two missing-intent checks should not automatically be reported as UI lineage bugs or inaccurate provenance for the generated plan. This review found no evidence of that narrower defect.

Six of the eight wrong accepted supported answers satisfy the required-lineage subset check. This is expected: accurate provenance can identify where a query read from without proving that it answers the user's full question. Unsupported cases have no gold column requirements, so their lineage status is not an independent semantic correctness test.

## What the evidence supports next

The highest-priority follow-up is general contract coverage, not replacing the questions with easier wording. Audit the completeness of semantic candidates and constrained output choices for numeric phrases, calendar phrases, exact-row deduplication and child predicates. Separately audit clause-preservation checks so that ordinary grouping, tie instructions and modal verbs are not invented as filters or dates. Test required-measure coverage and unsupported/ambiguous-clause detection before accepting a result.

Keep this first-pass artifact and the gold hashes fixed. Any implementation changes, repetitions or improved scores belong to clearly identified subsequent runs. Further coverage should use new phrasing and independently authored cases; this agent-authored 50-case fixture, its partly subjective clarification expectations and a single local-model run cannot establish broad production readiness or an external-human benchmark result.
