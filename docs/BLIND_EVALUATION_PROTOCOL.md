# AIDA blind transfer evaluation protocol

This assessment freezes the existing product before testing a newly authored logistics database and 50 new questions. Forty questions have independently written expected semantics and SQL; ten require refusal or clarification. Question authors may read the public feature contract and new fixture, but not the parser, compiler, prompts, previous question suites or model outputs. The fixture author may validate its structural catalog against the existing compiler.

This is an agent-authored transfer assessment on synthetic data, not an externally collected human benchmark. It tests the approved relational feature boundary. It does not establish accuracy across arbitrary operational schemas or business language.

## Before inference

- Hash the existing application modules and pinned model manifest in `docs/evidence/blind/protocol-seal.json` before question authoring completes.
- Finish structural source onboarding, independent oracle syntax checks and any author corrections before inference. Record fixture accommodations such as reporting-column renames.
- Seal the final database, catalog, schema contract, question file and evaluator hashes. Do not change them or application code during the first run.
- Exercise each supported explicit plan through the real API without language interpretation. Compare it with independent SQL; record failures separately from model behavior.

## First run

Each of the 50 questions is submitted once through the unchanged FastAPI application, in an isolated in-process API session with its own uploaded source and empty interpretation cache. It uses the real local Qwen model through the original inference transport. A recording wrapper observes requests and returns the original response without mutation. No expected plan, oracle SQL or result rows enter model requests; there are no retries or substituted responses.

Record request status, semantic intent, plan, SQL, parameters, rows, lineage, actual model requests/tokens and API/model/database timings. Preserve the first 50 outcomes separately before repeat checks. Unsupported questions may be rejected before inference; model availability errors never count as correct refusals.

## Scoring

| Outcome | Rule |
| --- | --- |
| Correct answer | Expected answer, accepted semantically equivalent plan, independent SQL rows match. |
| Wrong accepted meaning | Expected answer, successful response, but selected metrics, groups, predicates, dates, population, comparison, ordering or limit change the requested meaning. Coincidentally equal rows do not make it correct. |
| Execution mismatch | Expected answer and equivalent intent, but database rows differ from the independent oracle. |
| False refusal | Supported question rejected as needing clarification. |
| Unsafe acceptance | A question that requires refusal receives a successful executable answer. |
| Correct refusal | Unsupported/ambiguous question rejected as clarification with no SQL and no rows. |
| Availability/execution failure | Model unavailable, HTTP/server error or database failure. These are failures, not refusals. |
| Lineage defect | Required physical tables/columns are omitted or incorrect; reported independently of answer accuracy. |

Exact plan equality is reported separately. Semantic comparison ignores ordering of commutative AND predicates, IN membership values, and metric/group sets; it preserves predicate operators, values, dates, population, comparison, explicit sort and limit. The independent author marks `order_sensitive` before inference. Explicit ordering/ranking questions require ordered results, including deterministic ties. Other questions compare typed row multisets and ignore automatic presentation sorting while still recording exact-plan and ordered-row differences. Numeric comparison uses absolute tolerance `1e-8` and relative tolerance `1e-9`; nulls never equal zero.

Report supported-answer accuracy, precision of accepted answers, false refusals, unsafe acceptances, direct-plan correctness, and each failure category separately. An overall score must not hide poor answer coverage behind correct refusals.

## Repeatability, privacy and cost

Before outcomes are known, select supported positions 1, 5, 9, 13, 17, 21, 25, 29, 33 and 37. Replay these through the original session, then repeat them with fresh parser instances. Compare behavior and plans/results, including wrong answers and refusals. A stable wrong answer is repeatable but incorrect. Rejected interpretations are not assumed to be cached.

Audit observed model payloads for prohibited physical mappings, database/result structures and synthetic sensitive-value canaries. Record destination, request count, token usage and observed API charges separately from hardware/electricity costs, which are not measured. Inference remains on loopback. These checks do not prove aggregate anonymity or multiuser authorization.

Suggested readiness gates are preregistered in the seal: no wrong accepted answers or unsafe acceptances, at least 90% correct supported answers, at most one model call per initial question, and p95 model latency at most 15 seconds on this machine. These are assessment targets, not observed results or universal guarantees. Failure of a gate produces a prioritized backlog; no benchmark-driven product tuning is part of the first measurement.
