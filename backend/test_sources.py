"""Independent-schema onboarding, correctness and isolation regressions."""
import copy
import json
import sqlite3
from contextlib import closing

import pytest

from backend.core.analytics import AnalyticsEngine
from backend.core.sources import SourceError, SourceRegistry, inspect_database


@pytest.fixture
def snapshot(tmp_path):
    path = tmp_path / "inventory.sqlite"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute('CREATE TABLE stock (item_id INTEGER PRIMARY KEY, observed_on TEXT, warehouse TEXT, units INTEGER, unit_cost REAL, customer_email TEXT, notes TEXT)')
        connection.executemany("INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?)", [
            (1, "2026-02-01", "Pune", 10, 2.50, "private@example.test", "private narrative"),
            (2, "2026-02-02", "Pune", 20, 3.50, "secret@example.test", "other narrative"),
            (3, "2026-03-01", "Chennai", 7, 9.00, "hidden@example.test", "sensitive narrative"),
            (4, "2026-03-02", "O'Reilly", 3, 5.00, "last@example.test", "last narrative"),
        ])
    return path


@pytest.fixture
def config():
    return {"name": "Warehouse snapshot", "table": "stock", "metrics": [
        {"id": "units", "label": "Stock units", "description": "Sum of available stock units across snapshot positions.", "aggregate": "SUM", "column": "units"},
        {"id": "positions", "label": "Stock positions", "description": "Number of stock positions, one per stored row.", "aggregate": "COUNT"},
        {"id": "cost", "label": "Average unit cost", "description": "Unweighted average listed unit cost across stock positions.", "aggregate": "AVG", "column": "unit_cost", "format": "currency"},
    ], "dimensions": [{"id": "warehouse", "label": "Warehouse", "column": "warehouse"}], "date_column": "observed_on", "as_of": "2026-03-31", "currency": "INR"}


def onboard(tmp_path, snapshot, config):
    registry = SourceRegistry(tmp_path / "data")
    inspection = registry.inspect_upload(snapshot.read_bytes(), "Inventory")
    registry.configure(inspection["id"], config)
    return registry, inspection["id"], registry.engine(inspection["id"])


def test_upload_inspects_schema_not_samples_and_requires_approval(tmp_path, snapshot, config):
    registry = SourceRegistry(tmp_path / "data")
    result = registry.inspect_upload(snapshot.read_bytes(), "../../outside.sqlite")
    assert len(result["id"]) == 32 and not result["configured"]
    assert "private@example.test" not in json.dumps(result)
    assert "private narrative" not in json.dumps(result)
    assert "Pune" not in json.dumps(result)
    assert (registry.directory / f"{result['id']}.sqlite").is_file()
    with pytest.raises(SourceError, match="Approve"):
        registry.engine(result["id"])
    registry.configure(result["id"], config)
    assert registry.engine(result["id"]).query(plan={"metric": "positions"})["data"] == [{"value": 4}]


def test_explicit_different_schema_arithmetic_dates_and_grouping(tmp_path, snapshot, config):
    _, _, engine = onboard(tmp_path, snapshot, config)
    assert engine.query(plan={"metric": "units"})["data"] == [{"value": 40}]
    result = engine.query(plan={"metric": "units", "dimension": "warehouse"})
    assert result["data"] == [{"warehouse": "Pune", "value": 30}, {"warehouse": "Chennai", "value": 7}, {"warehouse": "O'Reilly", "value": 3}]
    assert engine.query(plan={"metric": "cost", "filters": {"warehouse": "Pune"}})["data"] == [{"value": 3.0}]
    assert engine.query(plan={"metric": "units", "date_from": "2026-03-01", "date_to": "2026-03-31"})["data"] == [{"value": 10}]
    assert engine.query(plan={"metric": "positions", "dimension": "month"})["data"] == [{"month": "2026-02", "value": 2}, {"month": "2026-03", "value": 2}]
    catalog = engine.catalog()
    assert catalog["dataset"]["as_of"] == "2026-03-31"
    assert "table" not in catalog and all("column" not in entry for entry in catalog["metrics"] + catalog["dimensions"])
    assert "customer_email" not in json.dumps(catalog)


def test_upload_snapshot_is_immutable_and_catalog_survives_restart(tmp_path, snapshot, config):
    registry, source_id, engine = onboard(tmp_path, snapshot, config)
    with closing(sqlite3.connect(snapshot)) as connection, connection:
        connection.execute("DELETE FROM stock")
    assert engine.query(plan={"metric": "units"})["data"] == [{"value": 40}]
    reopened = SourceRegistry(tmp_path / "data")
    assert reopened.engine(source_id).query(plan={"metric": "units"})["data"] == [{"value": 40}]
    assert "path" not in json.dumps(registry.inspect_source(source_id))
    assert "path" not in (registry.directory / f"{source_id}.json").read_text()


@pytest.mark.parametrize("column", ["item_id", "customer_email", "notes"])
def test_sensitive_columns_and_identifiers_cannot_be_approved(tmp_path, snapshot, config, column):
    registry, source_id, _ = onboard(tmp_path, snapshot, config)
    config["dimensions"][0]["column"] = column
    with pytest.raises(SourceError, match="excluded"):
        registry.configure(source_id, config)


def test_custom_sql_unmapped_fields_and_id_metrics_rejected(tmp_path, snapshot, config):
    registry, source_id, engine = onboard(tmp_path, snapshot, config)
    for plan in [{"metric": "units", "sql": "SELECT * FROM stock"}, {"metric": "revenue"}, {"metric": "units", "dimension": "customer_email"}, {"metric": "units", "filters": {"notes": "secret"}}, {"metric": "units", "limit": 101}]:
        assert not engine.query(plan=plan)["success"]
    invalid = copy.deepcopy(config)
    invalid["metrics"][0]["column"] = "item_id"
    with pytest.raises(SourceError, match="excluded"):
        registry.configure(source_id, invalid)
    invalid["metrics"][0]["column"] = "units"
    invalid["metrics"][0]["aggregate"] = "SUM(units); DROP TABLE stock;"
    with pytest.raises(SourceError):
        registry.configure(source_id, invalid)


def test_literal_filters_are_bound_and_do_not_become_sql(tmp_path, snapshot, config):
    _, _, engine = onboard(tmp_path, snapshot, config)
    result = engine.query(plan={"metric": "units", "filters": {"warehouse": "O'Reilly"}})
    assert result["data"] == [{"value": 3}]
    assert "O'Reilly" not in result["sql"]
    injection = "' OR 1=1; DROP TABLE stock; --"
    assert engine.query(plan={"metric": "units", "filters": {"warehouse": injection}})["data"] == [{"value": 0}]
    assert engine.query(plan={"metric": "positions"})["data"] == [{"value": 4}]


def test_authorizer_denies_sensitive_read_write_attach_and_extensions(tmp_path, snapshot, config):
    _, _, engine = onboard(tmp_path, snapshot, config)
    with closing(sqlite3.connect(engine.database_path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.set_authorizer(engine._authorizer)
        for statement in ["SELECT customer_email FROM stock", "SELECT item_id FROM stock", "SELECT name FROM sqlite_schema", "DELETE FROM stock", "ATTACH ':memory:' AS external", "SELECT load_extension('missing')", "PRAGMA user_version"]:
            with pytest.raises(sqlite3.DatabaseError):
                connection.execute(statement)
        assert connection.execute("SELECT SUM(units) FROM stock").fetchone()[0] == 40


def test_public_mode_hides_private_sources_and_disables_onboarding(tmp_path, snapshot, config):
    registry, source_id, _ = onboard(tmp_path, snapshot, config)
    public = SourceRegistry(tmp_path / "data", public_demo=True)
    assert public.list_sources() == []
    with pytest.raises(SourceError, match="public demo"):
        public.inspect_upload(snapshot.read_bytes(), "Private")
    with pytest.raises(SourceError, match="public demo"):
        public.configure(source_id, config)
    with pytest.raises(SourceError, match="public demo"):
        public.engine(source_id)
    public.register_support_demo()
    assert public.list_sources() == [{"id": "support", "name": "Support operations demo", "configured": True, "synthetic": True}]


@pytest.mark.parametrize("content", [b"SELECT * FROM customer", b"SQLite format 3\x00" + b"x" * 100, b"SQLite format 3\x00" + b"x" * (20 * 1024 * 1024)], ids=["sql-text", "corrupt", "oversized"])
def test_invalid_uploads_leave_no_registered_source(tmp_path, content):
    registry = SourceRegistry(tmp_path)
    with pytest.raises(SourceError):
        registry.inspect_upload(content, "Invalid")
    assert registry.list_sources() == []
    assert not list(registry.directory.glob("*.sqlite"))


@pytest.mark.parametrize("statement", ["CREATE VIEW v AS SELECT * FROM stock", "CREATE TRIGGER t AFTER INSERT ON stock BEGIN DELETE FROM stock; END", "CREATE VIRTUAL TABLE virtual USING fts5(content)"])
def test_active_sqlite_schema_objects_are_rejected(tmp_path, snapshot, statement):
    with closing(sqlite3.connect(snapshot)) as connection, connection:
        connection.execute(statement)
    with pytest.raises(SourceError, match="ordinary tables"):
        SourceRegistry(tmp_path / "data").inspect_upload(snapshot.read_bytes(), "Unsafe schema")


def test_wrong_dynamic_numeric_type_is_not_silently_zero(tmp_path, snapshot, config):
    with closing(sqlite3.connect(snapshot)) as connection, connection:
        connection.execute("UPDATE stock SET units='not a number' WHERE item_id=1")
    _, _, engine = onboard(tmp_path, snapshot, config)
    result = engine.query(plan={"metric": "units"})
    assert not result["success"] and "nonnumeric" in result["error"]


def test_count_and_averages_have_defined_empty_semantics(tmp_path, snapshot, config):
    _, _, engine = onboard(tmp_path, snapshot, config)
    for metric, value in [("units", 0), ("positions", 0), ("cost", None)]:
        assert engine.query(plan={"metric": metric, "filters": {"warehouse": "Absent"}})["data"] == [{"value": value}]


def test_builtin_commerce_compiler_matches_independent_business_definitions(tmp_path):
    seed = AnalyticsEngine(tmp_path / "commerce.sqlite")
    seed.ensure_demo_data()
    engine = SourceRegistry(tmp_path / "data").register_commerce_demo(seed.database_path)
    expressions = {"revenue": "ROUND(COALESCE(SUM(CASE WHEN status='Completed' THEN amount_cents END),0)/100.0,2)", "orders": "COUNT(*)",
                   "average_order_value": "ROUND(AVG(CASE WHEN status='Completed' THEN amount_cents END)/100.0,2)"}
    groups = {None: None, "status": "status", "region": "region", "month": "substr(order_date,1,7)"}
    for metric, expression in expressions.items():
        for dimension, group in groups.items():
            sql = (f"SELECT {group} AS {dimension}, " if group else "SELECT ") + f"{expression} AS value FROM analytics_orders WHERE order_date >= '2025-02-01' AND order_date <= '2025-06-30'"
            if group:
                sql += f" GROUP BY {group} ORDER BY " + (f"{dimension} ASC" if dimension == "month" else f"value DESC, {dimension} ASC")
            with closing(sqlite3.connect(seed.database_path)) as connection:
                connection.row_factory = sqlite3.Row
                expected = [dict(row) for row in connection.execute(sql)]
            plan = {"metric": metric, "dimension": dimension, "date_from": "2025-02-01", "date_to": "2025-06-30"}
            assert engine.query(plan=plan)["data"] == expected


def test_second_builtin_schema_uses_own_metrics_dates_and_cache(tmp_path):
    registry = SourceRegistry(tmp_path)
    engine = registry.register_support_demo()
    first = engine.query(plan={"metric": "tickets"})
    assert first["data"] == [{"value": 168}]
    assert not first["meta"]["cache_hit"]
    assert engine.query(plan={"metric": "tickets"})["meta"]["cache_hit"]
    assert engine.catalog()["dataset"]["as_of"] == "2026-06-30"
    assert not engine.query(plan={"metric": "revenue"})["success"]
    result = engine.query(plan={"metric": "resolution_time", "dimension": "priority"})
    with closing(sqlite3.connect(engine.database_path)) as connection:
        expected = [{"priority": key, "value": value} for key, value in connection.execute("SELECT priority, AVG(resolution_hours) FROM support_tickets WHERE state='Resolved' GROUP BY priority ORDER BY AVG(resolution_hours) DESC,priority")]
    assert result["data"] == expected


def test_source_ids_cannot_name_server_paths(tmp_path):
    registry = SourceRegistry(tmp_path)
    for source_id in ["../../private", "C:\\secrets.sqlite", "file:///etc/passwd", "support?mode=rw", "", None]:
        with pytest.raises(SourceError):
            registry.engine(source_id)


@pytest.mark.parametrize("stored_date,success", [("2026-03-31T23:59:59", True), ("2026-03-31 16:45:00", True), ("31/03/2026", False), ("2026-02-30", False), ("2026-03-31Tgarbage", False)])
def test_iso_timestamp_inclusive_dates_and_invalid_format_rejection(tmp_path, snapshot, config, stored_date, success):
    with closing(sqlite3.connect(snapshot)) as connection, connection:
        connection.execute("UPDATE stock SET observed_on=? WHERE item_id=1", (stored_date,))
    _, _, engine = onboard(tmp_path, snapshot, config)
    result = engine.query(plan={"metric": "units", "date_from": "2026-03-31", "date_to": "2026-03-31"})
    assert result["success"] is success, result
    if success:
        assert result["data"] == [{"value": 10}]
    else:
        assert "ISO" in result["error"]


def test_no_date_mapping_rejects_date_queries(tmp_path, snapshot, config):
    config.pop("date_column")
    config.pop("as_of")
    _, _, engine = onboard(tmp_path, snapshot, config)
    assert not engine.query(plan={"metric": "units", "date_from": "2026-01-01"})["success"]
    assert not engine.query(plan={"metric": "units", "dimension": "month"})["success"]
    assert engine.query(plan={"metric": "units"})["success"]


def test_only_explicit_approved_value_aliases_are_available(tmp_path, snapshot, config):
    config["dimensions"][0].update({"values": ["Pune", "Chennai", "O'Reilly"], "aliases": {"western depot": "Pune"}})
    registry, source_id, engine = onboard(tmp_path, snapshot, config)
    assert engine.catalog()["dimensions"][0]["aliases"] == {"western depot": "Pune"}
    assert engine.query(plan={"metric": "units", "filters": {"warehouse": "western depot"}})["data"] == [{"value": 30}]
    config["dimensions"][0]["aliases"] = {"Chennai": "Pune"}
    with pytest.raises(SourceError, match="conflicting"):
        registry.configure(source_id, config)
    config["dimensions"][0]["aliases"] = {"northern depot": "Missing"}
    with pytest.raises(SourceError, match="canonical"):
        registry.configure(source_id, config)
