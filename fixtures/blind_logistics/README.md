# Blind synthetic logistics snapshot

This is an original, fully synthetic parcel/freight logistics database for an agent-authored blind transfer evaluation of AIDA. It is **not an external human holdout**. No real customer, employee, consignment or credential records are present. All fixture data and its generation code are offered under CC0-1.0.

The dataset author used generic shipping concepts and the public version 2 catalog contract. The author was prohibited from reading the semantic parser modules, model prompts, prior question suites and model outputs. The question/oracle author receives the frozen business contract, not interpreter internals. This separation tests transfer to an independently authored domain but cannot establish independence equivalent to an external benchmark or human author. No internet downloads or model calls are required to build the fixture.

## Reproduce and verify

From the project root:

```powershell
.\.venv\Scripts\python.exe .\fixtures\blind_logistics\build_database.py
```

The script deterministically creates `logistics.sqlite`, `catalog.json`, `schema_contract.json` and `provenance.json` with seed `260912`. It uses a temporary database in this fixture directory, checks integrity and expected population counts, then replaces only this fixture's database. The JSON provenance records SHA256 hashes of the database, generator, catalog and contract. Byte reproducibility is expected with the same Python/SQLite runtime; the SQLite version is recorded because database byte layout may differ between runtime versions.

Import `logistics.sqlite` through the existing SQLite upload flow and approve `catalog.json` as a version 2 relational catalog. This fixture is deliberately absent from the builtin source registry. Its 12 physical tables and 15,000 stored fact rows fit below the 20 MB upload ceiling.

Initial structural validation rejected four categorical columns because the current schema inspector marks names ending in `_name` sensitive. Before question sealing or any model calls, the fixture renamed `markets.market_name` to `markets.market`, `zones.zone_name` to `zones.zone`, `service_classes.class_name` to `service_classes.service_class`, and `carriers.carrier_name` to `carriers.carrier`. This was an accommodation to the existing structural contract. Semantic IDs, business labels, category values and measure definitions stayed fixed; application code was not changed. The same accommodation is recorded in `schema_contract.json` and `provenance.json`.

## Approved meaning

`schema_contract.json` is the frozen, machine-readable authority for physical DDL, approved paths, business labels, units, nulls, archive behavior, date rules, ordering and supported bounds. `catalog.json` is the application manifest derived from this contract. Neither file contains model predictions or expected answers for test questions.

The grain is one shipment leg. Five measures are approved:

| Measure | Definition |
| --- | --- |
| Transported weight | Sum leg grams / 1,000, in kilograms; repeat transport on different legs contributes each time. |
| Handling charges | Sum leg charge cents / 100, in USD, rounded to two decimals. |
| Average transit minutes | Mean of non-null leg transit minutes; no end-to-end shipment inference. |
| Distinct consignments | Count distinct non-null consignment references in selected facts, including missing references. |
| Leg count | Count selected fact rows, including rows with null measures or missing dimensions. |

Paths lead from leg to consignment to shipper to customer market; from consignment to destination depot to zone; and from leg directly to service class and carrier. Shipment state comes from the consignment. Required dimension joins preserve facts with missing references in a SQL NULL group. Grouped distinct counts are not additive.

The default population has 12,000 current legs. The archive has 2,880 separate historical legs plus 120 exact copies of current legs. `UNION ALL` therefore selects 15,000 rows; `UNION` across all approved fact columns selects 14,880. An old date range alone never opts into archives.

Reporting dates are inclusive on leg departure date. The fixed reference date is **2026-09-12**, with advertised bounds **2025-07-01 through 2026-09-12**. Current facts span 2026-01-01 through the reference date. Separate historic facts span 2025-07-01 through 2025-12-31. Archive overlap rows retain their exact current dates.

## Deliberate difficulties

Repeated child exception events make naive event joins multiply metrics. The approved `exceptions` relation uses existential selection and can filter reason, state or severity. Several matching child predicates must describe the same event. Repeated events also occur for archived-only legs.

`charge_cents` separately means leg handling, whole-consignment charge, service tariff, carrier fee and exception fee. Only the leg amount is a measure. Numeric row-filter fields explicitly name their storage units. `status` separately means shipment, leg, carrier, shipper and exception state; the approved mappings distinguish them. Measures contain nulls, reference paths contain null and absent keys, and recorded parent identifiers are retained even when the parent row is absent. Foreign-key declarations describe intended paths; enforcement is disabled solely to represent an incomplete reporting snapshot.

Cancelled consignments have no recorded leg transit minutes, so the approved shipment-state filter can select an all-null average. Their fact rows, weights and handling charges remain in the snapshot.

Two unrelated tables, `operator_credentials` and `contact_directory`, contain conspicuously fabricated sensitive-looking values. They are outside the approved catalog and cannot supply report fields. All emails use `example.invalid`; no credentials are usable.

String values use canonical catalog spelling and SQLite BINARY comparison. Numeric aggregates preserve precision except for USD rounding. Explicit ordering applies before the result limit, with remaining group dimensions as ascending tie breakers; SQL NULL sorts first ascending. Questions without requested ordering should be scored as typed row multisets. The contract records exact comparison tolerances and query bounds.
