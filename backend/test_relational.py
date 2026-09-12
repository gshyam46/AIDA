"""Relational correctness, fanout prevention, schema mapping and execution safety."""
from __future__ import annotations

import copy
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

from backend.core.analytics import ClarificationRequired
from backend.core.relational import RelationalEngine, validate_relational_manifest
from backend.core.relational_demo import ensure_relational_demos, relational_demo_cases, relational_plan
from backend.core.sources import SourceError, inspect_database


@pytest.fixture(scope="module")
def fixture_sources(tmp_path_factory):
    return {item["id"]: item for item in ensure_relational_demos(tmp_path_factory.mktemp("relational"))}


@pytest.fixture(scope="module")
def engines(fixture_sources):
    return {key: RelationalEngine(item["path"], item["manifest"], key, True) for key, item in fixture_sources.items()}


@pytest.mark.parametrize("case", relational_demo_cases(), ids=lambda case: case["id"])
def test_plan_matches_independently_authored_sql(engines, case):
    engine = engines[case["source_id"]]
    response = engine.query(plan=case["plan"])
    assert response["success"], response
    with closing(sqlite3.connect(engine.database_path)) as connection:
        connection.row_factory = sqlite3.Row
        expected = [dict(row) for row in connection.execute(case["oracle_sql"], case["oracle_parameters"])]
    assert response["data"] == expected
    assert response["columns"] == case["plan"]["dimensions"] + case["plan"]["metrics"]
    assert response["meta"]["model_calls"] == 0
    assert response["lineage"]["operations"]


def test_exists_preserves_grain_despite_multiple_child_events(engines):
    engine = engines["warehouse"]
    plan = relational_plan(["revenue"], exists={"relation": "returns", "negate": False, "filters": []})
    result = engine.query(plan=plan)
    with closing(sqlite3.connect(engine.database_path)) as connection:
        correct = connection.execute("SELECT ROUND(SUM(li.line_total_cents)/100.0,2) FROM order_items li WHERE EXISTS(SELECT 1 FROM returns r WHERE r.line_id=li.line_id)").fetchone()[0]
        fanout = connection.execute("SELECT ROUND(SUM(li.line_total_cents)/100.0,2) FROM order_items li JOIN returns r ON r.line_id=li.line_id").fetchone()[0]
    assert fanout > correct
    assert result["data"] == [{"revenue": correct}]
    assert "EXISTS" in result["sql"] and "JOIN \"returns\"" not in result["sql"]


@pytest.mark.parametrize("source,metric,overlap", [("warehouse", "line_count", 12), ("billing", "billing_lines", 10)])
def test_union_all_and_union_have_distinct_duplicate_semantics(engines, source, metric, overlap):
    engine = engines[source]
    kept = engine.query(plan=relational_plan([metric], population="all"))["data"][0][metric]
    deduplicated = engine.query(plan=relational_plan([metric], population="all", set_operation="union"))["data"][0][metric]
    assert kept - deduplicated == overlap


def test_only_required_paths_are_joined_and_null_keys_are_preserved(engines):
    engine = engines["warehouse"]
    ungrouped = engine.query(plan=relational_plan(["revenue"]))
    grouped = engine.query(plan=relational_plan(["revenue"], ["region"]))
    assert ungrouped["lineage"]["tables"] == ["order_items"]
    assert set(grouped["lineage"]["tables"]) == {"order_items", "orders", "customers", "regions"}
    assert "products" not in grouped["sql"]
    assert any(row["region"] is None for row in grouped["data"])
    assert round(sum(row["revenue"] for row in grouped["data"]), 2) == ungrouped["data"][0]["revenue"]


def test_same_named_status_and_amount_columns_are_resolved_by_catalog(engines):
    result = engines["billing"].query(plan=relational_plan(["billed_amount"], ["plan_name"], filters=[{"field": "invoice_status", "op": "eq", "value": "Paid"}]))
    columns = result["lineage"]["columns"]
    assert {"table": "invoice_lines", "column": "amount_cents", "role": "measure", "semantic_id": "billed_amount"} in columns
    assert {"table": "invoices", "column": "status", "role": "filter", "semantic_id": "invoice_status"} in columns
    assert not any(c["table"] in {"plans", "subscriptions"} and c["column"] == "status" for c in columns)


def test_cache_is_source_scoped_immutable_to_callers_and_zero_model(engines):
    engine = engines["warehouse"]
    plan = relational_plan(["revenue"], ["channel"])
    first = engine.query(plan=plan)
    original = copy.deepcopy(first["data"])
    first["data"][0]["revenue"] = -100
    second = engine.query(plan=plan)
    assert second["data"] == original
    assert second["meta"]["cache_hit"] is True
    other = RelationalEngine(engine.database_path, engine.manifest, "other_source", True).query(plan=plan)
    assert other["meta"]["cache_hit"] is False
    assert other["meta"]["execution_id"] != second["meta"]["execution_id"]


def test_empty_avg_is_null_and_empty_sum_is_zero(engines):
    result = engines["warehouse"].query(plan=relational_plan(["average_line_value", "revenue"], date_from="2030-01-01", date_to="2030-12-31"))
    assert result["data"] == [{"average_line_value": None, "revenue": 0.0}]


def test_sql_values_are_parameters_and_physical_names_never_come_from_plan(fixture_sources):
    fixture = fixture_sources["warehouse"]
    manifest = copy.deepcopy(fixture["manifest"])
    manifest["dimensions"][0].pop("values")
    engine = RelationalEngine(fixture["path"], manifest, "literal_filters", True)
    injected = "North' OR 1=1; DROP TABLE orders; --"
    result = engine.query(plan=relational_plan(["revenue"], ["region"], filters=[{"field": "region", "op": "eq", "value": injected}]))
    assert result["success"] and result["data"] == []
    assert injected not in result["sql"]
    assert injected in result["parameters"].values()
    with closing(sqlite3.connect(fixture["path"])) as connection:
        assert connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 800


@pytest.mark.parametrize("changes", [
    {"metrics": ["orders.total_cents"]}, {"metrics": ["revenue", "revenue"]}, {"metrics": []}, {"metrics": ["revenue", "units", "orders", "line_count"]},
    {"dimensions": ["region", "category", "channel"]}, {"dimensions": ["customer_name"]}, {"limit": 101}, {"limit": True}, {"limit": 0},
    {"sql": "SELECT * FROM customers"}, {"sort": {"field": "email", "direction": "asc"}}, {"sort": {"field": "revenue", "direction": "desc; DROP TABLE orders"}},
    {"date_from": "2026-02-30"}, {"date_from": "2026-09-01", "date_to": "2026-01-01"}, {"version": 1}, {"population": "secret"}, {"set_operation": "union"},
    {"filters": [{"field": "line_quantity", "op": "gt", "value": float("nan")}]}, {"filters": [{"field": "region", "op": "in", "value": []}]},
    {"filters": [{"field": "region", "op": "gt", "value": "North"}]}, {"filters": [{"field": "region", "op": "eq", "value": "Neverland"}]},
    {"filters": [{"field": "line_quantity", "op": "eq", "value": True}]}, {"filters": [{"field": "region", "op": "contains", "value": "North"}]},
    {"having": [{"metric": "units", "op": "gt", "value": 100}]}, {"having": [{"metric": "revenue", "op": "gt", "value": float("inf")}]},
    {"exists": {"relation": "customers", "negate": False, "filters": []}}, {"exists": {"relation": "returns", "negate": "false", "filters": []}},
    {"comparison": {"kind": "above_average", "metric": "revenue"}},
    {"filters": [{"field": [], "op": "eq", "value": "North"}]}, {"population": []}, {"exists": {"relation": [], "negate": False, "filters": []}},
])
def test_unsafe_or_unsupported_plans_fail_before_execution(engines, changes):
    plan = relational_plan(["revenue"])
    plan.update(changes)
    result = engines["warehouse"].query(plan=plan)
    assert result["success"] is False
    assert result["error_type"] == "clarification_required"
    assert "sql" not in result


def test_manifest_rejects_unproven_fanout_key_and_parent_measure(fixture_sources):
    fixture = fixture_sources["warehouse"]
    schema = inspect_database(fixture["path"])
    bad_edge = copy.deepcopy(fixture["manifest"])
    bad_edge["relations"][1]["to_column"] = "region_id"
    with pytest.raises(SourceError, match="proven"):
        validate_relational_manifest(bad_edge, schema)
    bad_measure = copy.deepcopy(fixture["manifest"])
    bad_measure["metrics"][0].update(table="orders", column="total_cents")
    with pytest.raises(SourceError, match="fact grain"):
        validate_relational_manifest(bad_measure, schema)


@pytest.mark.parametrize("mismatched_affinity", [False, True])
def test_sqlite_collation_and_affinity_cannot_defeat_unique_join_proof(tmp_path, mismatched_affinity):
    path = tmp_path / "key_semantics.sqlite"
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript("CREATE TABLE labels(label_key TEXT PRIMARY KEY COLLATE BINARY,label TEXT); " + ("CREATE TABLE events(event_key INTEGER PRIMARY KEY,label_key INTEGER,amount INTEGER);" if mismatched_affinity else "CREATE TABLE events(event_key INTEGER PRIMARY KEY,label_key TEXT COLLATE NOCASE,amount INTEGER);"))
        connection.executemany("INSERT INTO labels VALUES(?,?)", [("1", "First"), ("01", "Second")] if mismatched_affinity else [("A", "First"), ("a", "Second")])
        connection.execute("INSERT INTO events VALUES(1,?,4)", (1 if mismatched_affinity else "A",))
        connection.commit()
        assert connection.execute("SELECT SUM(e.amount) FROM events e LEFT JOIN labels l ON e.label_key=l.label_key").fetchone()[0] == 8
    manifest = {"version": 2, "name": "Key semantics", "fact": {"table": "events", "key": "event_key", "columns": ["event_key", "label_key", "amount"]}, "tables": {"labels": {"table": "labels"}}, "relations": [{"id": "event_label", "from": "fact", "from_column": "label_key", "to": "labels", "to_column": "label_key", "kind": "many_to_one"}], "metrics": [{"id": "total", "label": "Total", "description": "Sum of event amounts.", "table": "fact", "column": "amount", "aggregate": "SUM"}], "dimensions": [{"id": "label", "label": "Label", "table": "labels", "column": "label", "type": "string"}]}
    if mismatched_affinity:
        with pytest.raises(SourceError, match="affinities"):
            RelationalEngine(path, manifest, "key_semantics", True)
    else:
        result = RelationalEngine(path, manifest, "key_semantics", True).query(plan=relational_plan(["total"], ["label"]))
        assert result["data"] == [{"label": "First", "total": 4}]


@pytest.mark.parametrize("mutation", ["ambiguous", "cycle", "unreachable", "sensitive", "custom_sql", "nonfinite_scale", "unused_fact", "wrong_union_type", "bad_shape"])
def test_manifest_rejects_unsafe_mappings(fixture_sources, mutation):
    fixture = fixture_sources["warehouse"]
    manifest = copy.deepcopy(fixture["manifest"])
    schema = inspect_database(fixture["path"])
    if mutation == "ambiguous":
        edge = copy.deepcopy(manifest["relations"][1]); edge["id"] = "duplicate_path"; manifest["relations"].append(edge)
    elif mutation == "cycle":
        manifest["relations"][0]["from"] = "customers"; manifest["relations"][0]["from_column"] = "customer_id"
    elif mutation == "unreachable":
        manifest["relations"].pop()
    elif mutation == "sensitive":
        manifest["dimensions"][0].update(table="customers", column="email")
    elif mutation == "custom_sql":
        manifest["metrics"][0]["aggregate"] = "SUM); DROP TABLE orders; --"
    elif mutation == "nonfinite_scale":
        manifest["metrics"][0]["scale"] = float("nan")
    elif mutation == "unused_fact":
        manifest["fact"]["columns"].append("shipping_address")
    elif mutation == "wrong_union_type":
        next(c for t in schema if t["name"] == "archived_order_items" for c in t["columns"] if c["name"] == "quantity")["type"] = "TEXT"
    elif mutation == "bad_shape":
        manifest["fact"]["columns"] = [[]]
    with pytest.raises(SourceError):
        validate_relational_manifest(manifest, schema)


def test_authorizer_rejects_unapproved_tables_columns_writes_and_attach(engines):
    engine = engines["warehouse"]
    with closing(sqlite3.connect(engine.database_path)) as connection:
        connection.set_authorizer(engine._authorizer)
        for sql in ["SELECT email FROM customers", "SELECT * FROM customer_secrets", "DELETE FROM order_items", "UPDATE orders SET status='Completed'", "ATTACH DATABASE ':memory:' AS exfil", "SELECT load_extension('x')"]:
            with pytest.raises(sqlite3.DatabaseError):
                connection.execute(sql)


def test_numeric_comparison_validates_values_before_where_can_hide_them(tmp_path):
    fixtures = ensure_relational_demos(tmp_path)
    fixture = fixtures[0]
    with closing(sqlite3.connect(fixture["path"])) as connection:
        connection.execute("UPDATE order_items SET quantity='invalid numeric text' WHERE line_id=1")
        connection.commit()
    engine = RelationalEngine(fixture["path"], fixture["manifest"], "bad_numeric", True)
    # COUNT does not read quantity as a metric; the guard must come from the
    # predicate itself. Both directions are unsafe with SQLite dynamic typing.
    for op in ["gt", "lt"]:
        result = engine.query(plan=relational_plan(["line_count"], filters=[{"field": "line_quantity", "op": op, "value": 10}]))
        assert result["error_type"] == "clarification_required"
        assert "numeric filter" in result["error"]


def test_numeric_related_filter_checks_storage_class(tmp_path):
    fixture = ensure_relational_demos(tmp_path)[0]
    manifest = copy.deepcopy(fixture["manifest"])
    manifest["exists_relations"][0]["fields"].append({"id": "refund_cents", "label": "Refund cents", "column": "refund_cents", "type": "number"})
    with closing(sqlite3.connect(fixture["path"])) as connection:
        connection.execute("UPDATE returns SET refund_cents='not a number' WHERE return_id=1")
        connection.commit()
    engine = RelationalEngine(fixture["path"], manifest, "bad_child_numeric", True)
    result = engine.query(plan=relational_plan(["line_count"], exists={"relation": "returns", "negate": False, "filters": [{"field": "refund_cents", "op": "gt", "value": 10}]}))
    assert result["error_type"] == "clarification_required"


def test_invalid_numeric_aggregate_and_dates_are_not_silently_coerced(tmp_path):
    fixture = ensure_relational_demos(tmp_path)[0]
    with closing(sqlite3.connect(fixture["path"])) as connection:
        connection.execute("UPDATE order_items SET line_total_cents='invalid amount' WHERE line_id=1")
        connection.execute("UPDATE orders SET ordered_on='03/04/2026' WHERE order_id=1")
        connection.commit()
    engine = RelationalEngine(fixture["path"], fixture["manifest"], "bad_storage", True)
    assert "nonnumeric" in engine.query(plan=relational_plan(["revenue"]))["error"]
    assert "malformed dates" in engine.query(plan=relational_plan(["units"], ["month"]))["error"]


def test_public_catalog_does_not_disclose_physical_schema_or_samples(engines):
    catalog = engines["warehouse"].catalog()
    serialized = json.dumps(catalog)
    assert catalog["capabilities"]["relational"]
    assert "order_items" not in serialized and "line_total_cents" not in serialized
    assert "synthetic-secret" not in serialized and "@example.invalid" not in serialized
    assert all("column" not in item and "table" not in item for item in catalog["metrics"] + catalog["dimensions"])


@pytest.mark.parametrize("aggregate,can_sum_groups", [("SUM", True), ("COUNT", True), ("AVG", False), ("MIN", False), ("MAX", False), ("COUNT_DISTINCT", False)])
def test_part_to_whole_metadata_matches_actual_group_aggregation(fixture_sources, aggregate, can_sum_groups):
    fixture = fixture_sources["warehouse"]
    manifest = copy.deepcopy(fixture["manifest"])
    metric = manifest["metrics"][0]
    metric["aggregate"] = aggregate
    if aggregate == "COUNT":
        metric.pop("column")
        metric.update(scale=1, format="number")
    elif aggregate == "COUNT_DISTINCT":
        metric.update(column="order_id", scale=1, format="number")
    engine = RelationalEngine(fixture["path"], manifest, "chart_additivity", True)
    groups = engine.query(plan=relational_plan(["revenue"], ["category"]))
    overall = engine.query(plan=relational_plan(["revenue"]))
    assert groups["success"] and overall["success"]
    # These fixture groups demonstrate the error a donut would introduce:
    # group averages/extrema do not compose; distinct orders overlap categories.
    summed_groups = round(sum(row["revenue"] for row in groups["data"]), 2)
    assert (summed_groups == overall["data"][0]["revenue"]) is can_sum_groups
    assert next(item for item in engine.catalog()["metrics"] if item["id"] == "revenue")["additive"] is can_sum_groups
    assert groups["chart"]["additive"]["revenue"] is can_sum_groups


def test_visual_additivity_metadata_does_not_change_semantic_projection(engines):
    from backend.core.relational_semantic import _project_catalog
    catalog = engines["warehouse"].catalog()
    without_visual_metadata = copy.deepcopy(catalog)
    for metric in without_visual_metadata["metrics"]:
        metric.pop("additive")
    assert _project_catalog(catalog) == _project_catalog(without_visual_metadata)


def test_compiler_is_identical_across_python_hash_seeds(fixture_sources):
    fixture = fixture_sources["warehouse"]
    program = "from pathlib import Path; import json,sys; from backend.core.relational import RelationalEngine; p=json.load(sys.stdin); e=RelationalEngine(Path(p['path']),p['manifest'],'warehouse',True); print(json.dumps(e._compile(e.validate_plan(p['plan'])),sort_keys=True))"
    payload = json.dumps({"path": str(fixture["path"]), "manifest": fixture["manifest"], "plan": relational_plan(["revenue", "units"], ["region", "category"], filters=[{"field": "order_status", "op": "eq", "value": "Completed"}])})
    outputs = []
    for seed in ["1", "42"]:
        process = subprocess.run([sys.executable, "-c", program], input=payload, text=True, capture_output=True, env={**os.environ, "PYTHONHASHSEED": seed}, check=True, timeout=40)
        outputs.append(process.stdout)
    assert outputs[0] == outputs[1]
