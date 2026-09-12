# AIDA demonstration guide

Start with `powershell -ExecutionPolicy Bypass -File scripts/start-demo.ps1` and open **http://127.0.0.1:3000**. Initial setup downloads the pinned local model and runtime. Use `-GpuLayers 0` if GPU initialization fails; CPU interpretation may take longer. Check the model-status indicator before demonstrating natural-language questions.

The [new-database assessment](BLIND_EVALUATION.md) found substantial natural-language failures: 19/40 supported questions correct and six unsupported requests incorrectly executed. Present this as a local demonstration with documented limits. Its separate 15-check browser journey passed upload, structured querying, charts, CSV, lineage and dashboard refresh; those UI results do not establish interpretation accuracy.

Use [Model, test data and repository status](MODEL_TEST_DATA_AND_REPOSITORY.md) to choose the correct fixture, explain why the model never receives database rows or physical mappings, and distinguish the current 4B baseline from a future larger-model comparison.

The walkthrough below is a repeatable acceptance journey. [VERIFICATION.md](VERIFICATION.md) records actual model and browser evidence. The model extracts fields and grounded unresolved phrases; code decides readiness. Semantic field IDs are opaque tokens, while approved filter values are grounded in the question. Relational SQL and join keys are chosen by the compiler from owner-approved mappings.

For the automated semantic acceptance check, run `.\.venv\Scripts\python.exe scripts/evaluate_semantics.py --suite acceptance --output artifacts/semantic-acceptance.json` from the repository root. This covers the 16 starter questions and six browser-workflow questions. Run model suites sequentially. For browser testing, use a fresh backend before asking any questions, generate the fixture with `.\.venv\Scripts\python.exe scripts/create-e2e-source.py`, then run `node scripts/e2e.cjs`. The full browser test needs local mode and must not overlap a model evaluation.

## Commerce: interpretation followed by inspectable execution

1. Select **Commerce demo**. Explain that this is synthetic order data and that its relative-date reference is December 31, 2025.
2. Ask `Count all orders`. Inspect the interpreted metric and generated plan. The count definition includes every status; the first uncached accepted question should show a local model call. Check the numerical result against the current verification report.
3. Ask `Revenue by region`. Inspect the revenue definition: completed-order amounts only. Switch between chart and table, then select a region to explore its breakdown. That chart action updates a structured plan directly.
4. Ask `Revenue in West last month`. Inspect the actual filter and date bounds: West and November 1–30, 2025. A reasonable-looking chart alone is insufficient evidence of a correct interpretation.
5. Repeat an accepted question. Inspect whether the interpretation cache was used. Contrast the recorded model latency with database execution time; do not describe local inference as free compute.
6. Ask `Average order value by month for pending`. No date window is required: the plan uses all available months with the Pending filter. The average remains null because the approved metric includes completed orders only. This is a valid empty contribution, not a missing-information error.

## Support operations: prove the source is different

1. Switch to **Support operations demo**. The metric list, dimensions and date reference must change; its reference is June 30, 2026.
2. Ask `Tickets by team`. The compiler must query support tickets using the approved count metric, rather than commerce orders.
3. Ask `Average resolution time by priority`. Show that the metric is defined in hours and includes resolved tickets only.
4. Ask `Tickets last month`. Inspect May 1–31, 2026 as the date range. This demonstrates that relative dates belong to the selected catalog.

## Builder, charts and dashboards

1. Select a metric, grouping and filter in the visual builder. Execute and inspect the generated plan; language interpretation is unnecessary for this action.
2. Save the result to a dashboard, reload the page, and refresh the card. Browser storage keeps its source ID, plan and presentation settings; the aggregate result is queried again.
3. Switch data source and confirm that the visible dashboard belongs to the selected source.
4. Export the current aggregate table as CSV. Explain that this writes the displayed result to the user's device.

## Retail warehouse: joins, subqueries and combined records

Select **Retail warehouse**. This is a separate schema with order lines, orders, customers, regions, products, categories, return events and archive lines. Revenue here sums all selected line totals before refunds; it does not inherit Commerce demo's completed-only rule.

1. Ask `Revenue and units by region`. The chart shows both measures with independent labeled scales. Choose **Scatter chart** to compare them. Open **SQL & trust** and inspect the route from order lines through orders and customers to regions; every source column is listed with its purpose.
2. Ask `Revenue by region and category for completed orders`. Inspect both grouping dimensions and the `order_status` record filter. The compiler uses the region path and the product/category path. The unrelated status columns on customers and products must not supply this filter.
3. Ask `Categories with revenue above 30000`. Inspect the threshold after aggregation in HAVING. Compare this with `Units by category in North or West where line quantity is at least 3`, which filters individual rows before aggregation using IN and a numeric comparison.
4. Ask `Revenue by category for lines with damaged returns`, then `Revenue by region for lines without returns`. Inspect EXISTS and NOT EXISTS. Repeated return events must not multiply the revenue of a qualifying order line.
5. Ask `Regions with above-average revenue`. Inspect the grouped subquery and the comparison with the average of group totals. This is not a comparison against average line value.
6. Ask `Revenue by month including current and archived records`. Switch between **Line chart**, **Area chart** and **Bar chart**. Dates remain in chronological order on temporal charts; null measures remain gaps.
7. Ask `Order lines including current and archived records with exact duplicates removed`. Inspect UNION before the aggregate. In the visual builder, select all records and change **Combine sources** to compare UNION with UNION ALL. The fixture includes overlapping records so the distinction is observable.
8. For an additive, nonnegative single-measure result with at most 12 groups, choose **Donut chart**. Its caption describes the share of displayed groups. Save the chart, refresh its dashboard and inspect the zero-model-call execution. Select a non-null chart group to apply a direct filter; the first grouping is removed, preserving a second grouping if one was selected.

## Billing and Chinook: prove table and column selection transfers

Select **SaaS billing** and ask `Billed amount by plan tier and billing country for paid invoices`. Inspect the distinct paths through subscriptions/plans and invoices/accounts. Ask `Billed amount by account segment for lines with service issue credits` and check that a credit existence filter does not multiply invoice-line amounts. `Invoices and seats by plan tier` combines a distinct invoice count with a quantity sum at the same approved line grain.

Select **Chinook music store**, the bundled unmodified public 11-table music-store sample. Its [provenance and MIT license](../fixtures/chinook/README.md) are included in the repository.

1. Ask `Units sold by album for customer country USA`. Inspect the separate album and customer paths, covering invoice lines, tracks, albums, invoices and customers.
2. Ask `Average sale price by customer country where catalog price is greater than 1`. The average must use `InvoiceLine.UnitPrice`, while the row threshold uses `Track.UnitPrice`. Inspect both columns in lineage rather than assuming that matching column names have the same meaning.
3. Ask `Units sold by billing country with units sold greater than 100`. Check the billing-country mapping and HAVING threshold.
4. Ask `Units sold by customer country with playlist membership`. Inspect the membership EXISTS subquery; multiple playlist memberships must not duplicate purchased quantities.
5. Ask `Units sold by month in 2013` and inspect the source-specific date window.

The relational visual builder exposes up to three measures, two groups, record filters, aggregate filters, related-record checks and an above-average comparison. Aggregate thresholds and above-average comparisons are alternative modes in this bounded contract. Chart choices depend on valid data: scatter needs two numeric measures, donut needs one explicitly additive nonnegative measure and a small set of groups (averages and distinct counts are excluded), and line/area require one temporal grouping and one measure. Use Table to inspect missing values and all returned groups.

## Local SQLite onboarding

This step is available in local mode. For a bundled-data demonstration, use `-PublicDemo`; uploads and private source access must be disabled.

1. Open **Data catalog** and upload a small standalone SQLite reporting snapshot. Use synthetic data for a presentation.
2. Review table/column metadata. Inspection must not display sampled row values. The pending source must require a mapping before it can be queried.
3. Choose one reporting table. Approve metric labels and business definitions, numeric columns, categorical dimensions and an optional ISO date column with an explicit reference date.
4. Save the mapping. Query the newly selected source with the builder, then ask an equivalent natural-language question and compare the plans/results.
5. Show that sensitive/identifier columns are excluded by default. Explain that naming heuristics and owner review are safeguards, not proof that every permitted aggregate is anonymous.

For an uploaded relational snapshot, choose **Relational catalog** after upload. Expand **Inspect tables, columns, and declared keys**, then paste an approved version 2 manifest into **Approved relational catalog JSON**. Ready-to-paste [catalog JSON files](../fixtures/catalogs/) and the [catalog guide](RELATIONAL_CATALOG.md) cover the bundled databases. Save only after reviewing the fact-row grain, physical fields, directed relationships, measures and related-record populations. The server verifies targets are unique before accepting many-to-one joins; child populations are checked through EXISTS. An unconfigured source remains unavailable after a rejected mapping; an already configured source retains its previous approved mapping.

## Automated relational journey

From the repository root, with the local model running:

```powershell
.\.venv\Scripts\python.exe scripts/evaluate_relational.py --suite all --output artifacts/relational-evaluation.json --label regression
.\.venv\Scripts\python.exe scripts/prepare-relational-e2e.py
powershell -ExecutionPolicy Bypass -File scripts/stop-demo.ps1
powershell -ExecutionPolicy Bypass -File scripts/start-demo.ps1 -SkipInstall -SkipBuild
node scripts/e2e-relational.cjs
```

The preparation command generates expected rows from independently authored SQL over fixture instances. The browser uses the production frontend, backend and real local model; it does not inject expected interpretations. Keep model suites sequential and do not ask questions between the fresh restart and the browser test, because cache assertions depend on that boundary. Run the original `scripts/e2e.cjs` separately against another fresh backend to check single-table behavior. Current pass/failure counts and screenshots belong in [VERIFICATION.md](VERIFICATION.md).

## Failure behavior and honest claims

Ask for an unsupported window calculation or customer emails. Inspect the clarification and confirm that no SQL result is returned. A model-reported unresolved phrase must be grounded in the actual question and use an allowed reason; code handles the clarification. Omitted optional dates or filters never create a missing requirement. If the local model is unavailable, the UI must state that directly; the builder remains usable.

The demonstrated architecture is **local language interpretation → validated plan → deterministic SQL → interactive result**. It avoids an agent loop and hosted inference, while retaining the semantic layer the product needs. Relational support covers one approved fact grain, verified directed relationships, AND/IN predicates and explicit subquery/set-operation forms. Arbitrary SQL, arbitrary joins, general OR expressions, window functions and unrestricted nested queries remain unsupported. It does not claim infallible interpretation, authentication, tenant isolation, operational database connectors or a completed public deployment.
