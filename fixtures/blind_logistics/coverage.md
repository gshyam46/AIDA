# Blind logistics transfer coverage

Exactly **50 new business questions** are preserved in `questions.json`: **40 supported answers** and **10 safe refusal or clarification cases**. The dataset, business questions and oracle SQL are independently agent-authored for this evaluation. This is **not an external human holdout** and does not establish statistical independence equivalent to one.

The question author inspected only `docs/RELATIONAL_CATALOG.md`, the new fixture `catalog.json`, `schema_contract.json`, `README.md`, and the newly generated SQLite data needed for direct oracle verification. The parent supplied the public canonical version 2 plan envelope. The author did not inspect parser/compiler implementation, model prompts, earlier evaluation cases, or model outputs, and made no AIDA/model inference calls. SQL was written from the fixture contract with physical joins and aggregates; no AIDA compiler was used to generate or execute it.

## Preservation and preregistration

Status: final candidate ready for parent hash sealing before first inference. The question text, gold plans, oracle SQL, parameters, feature tags, expected semantics and lineage requirements must remain unchanged after first inference. If a later run discovers a benchmark defect, record it separately with the original artifact hash rather than editing these cases.

- Question artifact SHA256: `1fbe79856a67a1f051d1adc6a546030d966aeb091a5a2017d96511132109e689`
- Dataset contract SHA256: `30dd1b2f81281e68a37b0c2cdcb0b5b657f5112de4495b5118e1d333782936c0`
- SQLite snapshot SHA256: `0adc23a8c2f4e24dce57c9416b32e8b64a2c7303ccf0973fe7e0fbe711e5fdbf`
- Contract: `blind-logistics-2026-09-12-v1`; reporting reference: `2026-09-12`.
- Case IDs: `BL01` through `BL50`, each appearing exactly once.

Each supported case contains a complete version 2 plan, standalone parameterized SQLite oracle SQL, independent expected semantics, feature tags and physical lineage requirements. Refusal cases have null plans and null oracle SQL, plus an explicit reason that answering would be unsafe or change the question.

## Accepted coverage

| Cases | Coverage |
| --- | --- |
| BL01-BL05 | Current-population scalar COUNT, COUNT DISTINCT, scaled SUM kilograms, scaled/rounded SUM handling USD, non-null AVG duration |
| BL06-BL08 | Three-edge market path, branched market/zone paths, two direct service/carrier paths, two measures, two dimensions, incomplete-reference preservation |
| BL09-BL10 | Shipment versus leg state ownership, explicit null group, generated departure-month grouping and dimension sorting |
| BL11-BL12 | Same-name `charge_cents` in fact, service and consignment; row filter units in cents versus handling result in dollars; numeric inequalities |
| BL13-BL16 | Numeric and categorical equality, two IN predicates, distance threshold, AND composition, separate fact/parent/carrier states, ordinary SQL inequality with nulls |
| BL17-BL19 | HAVING on SUM kg, WHERE tariff versus HAVING handling USD, grouped distinct-count threshold and nonadditive entity counts |
| BL20-BL25 | EXISTS/NOT EXISTS, repeated child events, no-child facts, child numeric/equality/IN filters, multiple predicates on the same event, combined parent and child restrictions |
| BL26-BL28 | Above-average grouped SUM/COUNT; complete grouped comparison populations including null group; row filtering before comparison; ordering and limits after comparison; two dimensions |
| BL29-BL32 | UNION ALL retains 120 overlapping facts; UNION removes only exact approved fact-row duplicates; grouped archive joins and historical monthly series |
| BL33-BL36 | Last calendar month and current month relative to fixed as_of; inclusive absolute date bounds; single as_of day; date-filtered monthly AVG |
| BL37-BL40 | Nonempty all-null AVG population; bottom-three count ordering with null carrier group; valid no-match scalar zero/zero/null; valid empty grouped result and no implicit archive inclusion |

All five approved metrics and all six approved groupings (including generated month) occur. Supported queries use one, two or three measures; zero, one or two dimensions; numeric/equality/inequality/IN row predicates; HAVING; one correlated child existence condition; one grouped above-average comparison; current and combined archive populations; explicit ordering and limits.

## Safe refusal and clarification coverage

| Case | Reason |
| --- | --- |
| BL41 | Unapproved raw personal contact data |
| BL42 | Unapproved aggregate ratio and ranking on the result |
| BL43 | Unsupported cumulative/window calculation |
| BL44 | Unsupported within-group top-three fact selection followed by aggregation |
| BL45 | Ambiguous overall business performance metric |
| BL46 | Ambiguous `area` grouping and join path |
| BL47 | Unsupported OR across distinct fields |
| BL48 | Unsupported contains/LIKE string predicate |
| BL49 | Contradictory current-only and include-all-archive population instructions |
| BL50 | Unsupported latest-row-per-consignment deduplication, different from full-row UNION |

The expected safe outcome includes a refusal with a useful capability explanation or a clarification where meaning is unresolved. Substituting a supported but different question is not a correct answer.

## Result and lineage comparison contract

`order_sensitive` was set before inference. Explicit ordering, top/bottom selection, and stated tie rules are semantically binding. These accepted cases are order-sensitive: BL06, BL07, BL08, BL09, BL10, BL16, BL17, BL18, BL19, BL25, BL26, BL27, BL28, BL31, BL32, BL36, BL38, BL40. Other supported questions are compared as typed row multisets; presentation default ordering must not create a semantic failure. Limits, predicates, grouping, dates and populations remain binding in either mode. The stored canonical plan still makes all default values explicit so exact-plan match can be reported separately.

The oracle emits dimensions followed by metrics, aliased by semantic ID. Explicit primary sort is followed by remaining dimensions ascending in their selected order. SQLite BINARY text ordering and default null placement apply. LIMIT follows WHERE, grouping, HAVING or above-average comparison. SUM skips nulls and returns zero on empty/all-null selection; COUNT and COUNT DISTINCT return zero for empty scalar aggregates; AVG skips nulls and returns null when no non-null values exist. No-match grouped queries return no rows.

`expected_lineage.tables` and `.columns` are required subsets of reported physical lineage. Join-only keys are recorded as `join_key_evidence` for review because the product reports joins separately. Archive tables are required in table lineage, while metric/filter column lineage uses the primary approved owner. `forbidden_measure_columns` forbids those physical columns only as aggregate measure sources; a same-name service tariff or consignment charge may still be a correctly required row-filter source.

## Direct SQL validation before inference

All **40 supported oracle queries executed successfully** against the final read-only snapshot. Returned column aliases exactly matched each plan's dimensions followed by measures. All selected thresholds have matching rows except the two deliberately empty cases. The largest observed single-query elapsed time in this local smoke check was about 12 ms; this is verification evidence, not a general performance benchmark.

| Control | Observed direct-SQL result |
| --- | --- |
| BL01 current leg count | 12,000 |
| BL02 current distinct fact consignment references | 4,019 |
| BL03 current transported kilograms | 5,443,774.263 |
| BL04 current handling USD | 659,298.93 |
| BL17 qualifying zone weight groups | 4 |
| BL18 tariff WHERE plus handling HAVING groups | 3 |
| BL19 distinct-consignment HAVING groups | 2 |
| BL20 any-exception leg count | 2,964 |
| No-exception complement | 9,036; partitions all 12,000 current legs with BL20 |
| Naive exception join row count | 7,410; observably exceeds the correct existential leg count |
| Separate Weather/Open events without an Open Weather event | 176 current legs; distinguishes same-event child predicates |
| BL29 UNION ALL combined leg count | 15,000 |
| BL30 UNION combined leg count | 14,880 |
| BL32 deduplicated departure-month groups | 15 |
| BL35 exact as_of-day leg count | 18 |
| BL37 Cancelled shipment AVG and COUNT | null average; 582 legs |
| BL38 smallest carrier group | null carrier; 90 legs |
| BL39 impossible-distance scalar aggregate | handling charges 0; count 0; average null |
| BL40 current-only July 2025 grouped result | zero rows |

No parser, compiler, model-predicted plan or model-predicted answer was used for these checks. The parent evaluator separately performs explicit-plan API/compiler preflight and then the sealed blind inference run. Any product failures must be reported against the fixed gold artifact, not used to revise the questions.
