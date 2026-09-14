"""Commerce seed, numeric correctness through approved plans, and HTTP privacy regressions."""
import concurrent.futures
import hashlib
import socket
import sqlite3

import pytest
from fastapi.testclient import TestClient

from backend.core import analytics, catalog_engine
from backend.core.analytics import AnalyticsEngine
from backend.core.llm import ModelUnavailable
from backend.main import create_app

REVENUE = "ROUND(SUM(amount_cents)/100.0,2)"


class OfflineInterpreter:
    """HTTP boundary tests do not perform inference; live quality has its own benchmark."""
    def status(self):
        return {"available": False, "model": "unit-test", "provider": "test"}

    def interpret(self, *args, **kwargs):
        raise ModelUnavailable("Local model is unavailable.")


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path, interpreter=OfflineInterpreter())) as instance:
        yield instance


@pytest.fixture
def engine(client):
    return client.app.state.registry.engine("commerce")


def reference(engine, sql, values=()):
    with sqlite3.connect(engine.database_path) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(sql, values)]


def rows(client, plan):
    response = client.post("/api/v1/query", json={"plan": plan}).json()
    assert response["success"], response
    return response["data"]


@pytest.mark.parametrize("plan,sql", [
    ({"metric": "orders"}, "SELECT COUNT(*) AS value FROM analytics_orders"),
    ({"metric": "revenue"}, f"SELECT {REVENUE} AS value FROM analytics_orders WHERE status='Completed'"),
    ({"metric": "average_order_value"}, "SELECT ROUND(AVG(amount_cents)/100.0,2) AS value FROM analytics_orders WHERE status='Completed'"),
    ({"metric": "orders", "dimension": "status"}, "SELECT status,COUNT(*) AS value FROM analytics_orders GROUP BY status ORDER BY value DESC,status ASC"),
    ({"metric": "revenue", "filters": {"region": "West"}, "date_from": "2025-11-01", "date_to": "2025-11-30"},
     f"SELECT {REVENUE} AS value FROM analytics_orders WHERE status='Completed' AND region='West' AND order_date BETWEEN '2025-11-01' AND '2025-11-30'"),
    ({"metric": "revenue", "dimension": "category", "limit": 3},
     f"SELECT category,{REVENUE} AS value FROM analytics_orders WHERE status='Completed' GROUP BY category ORDER BY value DESC,category LIMIT 3"),
])
def test_plan_matches_independent_sql(client, engine, plan, sql):
    response = client.post("/api/v1/query", json={"plan": plan}).json()
    assert response["success"], response
    assert response["data"] == reference(engine, sql)
    assert response["meta"]["model_calls"] == 0
    assert response["meta"]["estimated_model_cost_usd"] == 0


def test_every_advertised_example_is_listed(client):
    assert client.get("/api/v1/examples").json()["examples"] == analytics.EXAMPLES


def test_group_totals_reconcile(client):
    total = rows(client, {"metric": "revenue"})[0]["value"]
    regions = rows(client, {"metric": "revenue", "dimension": "region"})
    assert sum(row["value"] for row in regions) == pytest.approx(total)


def test_bound_parameters_never_become_sql(client):
    response = client.post("/api/v1/query", json={"plan": {"metric": "revenue", "filters": {"region": "West"}}}).json()
    assert "West" not in response["sql"]
    assert "West" in response["parameters"].values()


@pytest.mark.parametrize("plan", [
    {"metric": "revenue", "database_path": "../../private.db"},
    {"metric": "revenue", "dimension": "region; DROP TABLE analytics_orders"},
    {"metric": "revenue", "dimension": False},
    {"metric": "revenue", "dimension": []},
    {"metric": "revenue", "sort": False},
    {"metric": "revenue", "filters": {"region": "West' OR 1=1 --"}},
    {"metric": "revenue", "filters": {"email": "secret@example.com"}},
    {"metric": "revenue", "limit": True},
    {"metric": "revenue", "limit": 100000},
    {"metric": "revenue", "date_from": "2025-12-31", "date_to": "2025-01-01"},
])
def test_invalid_plans_rejected(client, plan):
    response = client.post("/api/v1/query", json={"plan": plan}).json()
    assert not response["success"] and "sql" not in response


def test_empty_result_semantics(client):
    dates = {"date_from": "2030-01-01", "date_to": "2030-12-31"}
    assert rows(client, {"metric": "orders", **dates}) == [{"value": 0}]
    assert rows(client, {"metric": "revenue", **dates}) == [{"value": 0.0}]
    assert rows(client, {"metric": "average_order_value", **dates}) == [{"value": None}]
    assert rows(client, {"metric": "revenue", "dimension": "region", **dates}) == []


def test_cached_results_are_immutable(engine):
    first = engine.query(plan={"metric": "revenue", "dimension": "region"})
    original = first["data"][0]["value"]
    first["data"][0]["value"] = -999
    again = engine.query(plan={"metric": "revenue", "dimension": "region"})
    assert again["data"][0]["value"] == original and again["meta"]["cache_hit"]


def test_parallel_queries_do_not_mix_results(engine):
    plans = [{"metric": "revenue", "filters": {"region": "West"}}, {"metric": "orders", "dimension": "status"},
             {"metric": "revenue", "filters": {"region": "East"}}, {"metric": "average_order_value"}] * 8
    expected = [engine.query(plan=plan)["data"] for plan in plans]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda plan: engine.query(plan=plan), plans))
    assert [response["data"] for response in responses] == expected


def test_execution_runs_with_network_disabled(engine, monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("Network must not be used for analytics")
    monkeypatch.setattr(socket, "socket", denied)
    monkeypatch.setattr(socket, "create_connection", denied)
    assert engine.query(plan={"metric": "revenue", "dimension": "category"})["success"]


def test_database_unchanged_and_authorizer_denies_writes(engine):
    before = hashlib.sha256(engine.database_path.read_bytes()).hexdigest()
    for dimension in (None, "region", "month"):
        engine.query(plan={"metric": "revenue", "dimension": dimension})
    assert hashlib.sha256(engine.database_path.read_bytes()).hexdigest() == before
    with sqlite3.connect(engine.database_path.as_uri() + "?mode=ro", uri=True) as connection:
        connection.set_authorizer(engine._authorizer)
        for sql in ["DELETE FROM analytics_orders", "SELECT order_id FROM analytics_orders", "SELECT name FROM sqlite_master", "ATTACH DATABASE ':memory:' AS other"]:
            with pytest.raises(sqlite3.DatabaseError):
                connection.execute(sql).fetchall()


def test_actual_sqlite_timeout(engine, monkeypatch):
    monkeypatch.setattr(catalog_engine, "MAX_SECONDS", 0)
    result = engine.query(plan={"metric": "orders", "dimension": "channel", "date_from": "2025-03-01"})
    assert not result["success"] and result["error_type"] == "execution_error"


def test_seed_reproducible_and_no_customer_fields(tmp_path):
    first, second = AnalyticsEngine(tmp_path / "first.sqlite"), AnalyticsEngine(tmp_path / "second.sqlite")
    first.ensure_demo_data()
    second.ensure_demo_data()
    assert reference(first, "SELECT * FROM analytics_orders") == reference(second, "SELECT * FROM analytics_orders")
    assert {column["name"] for column in reference(first, "PRAGMA table_info(analytics_orders)")} == {"order_id", "order_date", "region", "category", "channel", "status", "amount_cents"}


def test_seed_connections_close(tmp_path, monkeypatch):
    real_connect, connections = sqlite3.connect, []

    class TrackedConnection(sqlite3.Connection):
        closed = False

        def close(self):
            self.closed = True
            super().close()

    def tracked_connect(*args, **kwargs):
        connection = real_connect(*args, factory=TrackedConnection, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(analytics.sqlite3, "connect", tracked_connect)
    seed = AnalyticsEngine(tmp_path / "demo.sqlite")
    seed.ensure_demo_data()
    seed.ensure_demo_data()
    assert len(connections) == 2 and all(connection.closed for connection in connections)


def test_existing_unrelated_database_is_not_labeled_synthetic(tmp_path):
    path = tmp_path / "private.sqlite"
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA user_version = 1")
    finally:
        connection.close()
    with pytest.raises(RuntimeError, match="not a compatible AIDA synthetic"):
        AnalyticsEngine(path).ensure_demo_data()


def test_http_happy_path_and_real_health(client):
    health = client.get("/api/v1/health")
    assert health.status_code == 200
    assert health.json()["dataset"]["id"] == "commerce"
    assert client.get("/api/v1/catalog").json()["privacy"]["aggregate_only"] is True
    result = client.post("/api/v1/query", json={"plan": {"metric": "orders"}})
    assert result.status_code == 200 and result.json()["success"]
    assert result.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("body", [
    {}, {"question": " "}, {"question": "a" * 1501},
    {"question": "revenue", "plan": {"metric": "orders"}},
    {"question": "revenue", "database_path": "C:/private/customer.db"},
    {"sql": "SELECT * FROM secret"},
])
def test_api_does_not_accept_arbitrary_inputs(client, body):
    response = client.post("/api/v1/query", json=body)
    assert response.status_code == 422
    assert "customer.db" not in response.text and "secret" not in response.text


def test_api_caps_request_body(client):
    assert client.post("/api/v1/upload").status_code == 404
    assert client.post("/api/v1/query", content=b"x" * 9000, headers={"content-type": "application/json"}).status_code == 413


def test_questions_not_logged(client, caplog):
    marker = "private-customer-marker-47829"
    client.post("/api/v1/query", json={"question": marker})
    assert marker not in caplog.text


def test_unexpected_exception_is_consumed_without_logging_values(client, caplog, monkeypatch):
    marker = "private-result-marker-8019"

    def unexpected(**kwargs):
        raise RuntimeError(marker)
    monkeypatch.setattr(client.app.state.engine, "query", unexpected)
    response = client.post("/api/v1/query", json={"question": "Total revenue"})
    assert response.status_code == 500
    assert marker not in response.text and marker not in caplog.text
    assert response.headers["cache-control"] == "no-store"


def test_streamed_request_without_content_length_is_capped(client):
    response = client.post("/api/v1/query", content=iter([b"x" * 4096, b"y" * 4097]), headers={"content-type": "application/json"})
    assert response.status_code == 413 and response.headers["cache-control"] == "no-store"
