"""Service/API/extraction tests use an explicit SQLite remote-session surrogate.

Live PostgreSQL/MySQL/SQL Server tests are in test_connectors_live.py; these tests
do not claim to verify a network handshake, TLS or vendor transaction semantics.
"""
import json
import sqlite3
import time
from contextlib import contextmanager, closing
from decimal import Decimal

import pytest
import sqlalchemy as sa
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from backend.core import connectors as c
from backend.core.sources import SourceError, SourceRegistry
from backend.main import create_app
from backend.test_hybrid_api import StubInterpreter
start_worker = c.ConnectionService.start


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv("AIDA_CONNECTOR_KEY", Fernet.generate_key().decode())
    monkeypatch.setenv("AIDA_CONNECTOR_HOSTS", "127.0.0.1:5432")
    monkeypatch.setenv("AIDA_CONNECTOR_LOCAL_TEST", "1")
    path = tmp_path / "remote.sqlite"
    remote = sa.create_engine(sa.URL.create("sqlite", database=str(path)))
    with remote.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE stock (item_id INTEGER PRIMARY KEY, category TEXT, quantity INTEGER, customer_email TEXT)")
        conn.exec_driver_sql("INSERT INTO stock VALUES (1, 'Garden', 11, 'private@example.test'), (2, 'Tools', 8, 'hidden@example.test')")

    @contextmanager
    def surrogate(spec, settings=None):
        with remote.connect() as conn:
            yield conn
    monkeypatch.setattr(c, "remote_session", surrogate)
    monkeypatch.setattr(c.ConnectionService, "start", lambda self: None)
    config = {"name": "Reporting inventory", "schedule": "manual",
              "connection": {"engine": "postgresql", "host": "127.0.0.1", "port": 5432, "database": "test",
                             "schema": "main", "username": "reader", "password": "never-print-this-secret", "tls": False},
              "selection": [{"table": "stock", "columns": ["item_id", "category", "quantity"]}]}
    yield tmp_path, remote, config
    remote.dispose()


def mapping():
    return {"name": "Stock", "table": "stock", "metrics": [{"id": "quantity", "label": "Stock quantity", "description": "Sum of units in inventory", "aggregate": "SUM", "column": "quantity"}],
            "dimensions": [{"id": "category", "label": "Category", "column": "category"}]}


def create_ready(service, config):
    connection = service.create(config)
    service.tick()
    record = service.records[connection["id"]]
    assert record["status"] == "succeeded", service._public(record)
    service.registry.configure(record["source_id"], mapping())
    return record


def test_api_onboard_refresh_cache_and_disconnect(setup):
    root, remote, config = setup
    parser = StubInterpreter()
    with TestClient(create_app(root / "app", interpreter=parser, public_demo=False)) as client:
        service = client.app.state.connections
        inspected = client.post("/api/v1/connections/inspect", json=config["connection"])
        assert inspected.status_code == 200
        assert "customer_email" in inspected.text
        assert "private@example.test" not in inspected.text
        created = client.post("/api/v1/connections", json=config)
        assert created.status_code == 202
        cid = created.json()["id"]
        assert client.post(f"/api/v1/connections/{cid}/refresh", json={}).status_code == 400
        service.tick()
        record = client.get("/api/v1/connections").json()["connections"][0]
        sid = record["source_id"]
        assert record["status"] == "succeeded" and record["rows"] == 2
        inspected = client.get(f"/api/v1/sources/{sid}").json()
        assert [col["name"] for col in inspected["tables"][0]["columns"]] == ["item_id", "category", "quantity"]
        catalog = client.post(f"/api/v1/sources/{sid}/configure", json=mapping()).json()
        query = {"source_id": sid, "catalog_version": catalog["dataset"]["catalog_version"], "plan": {"metric": "quantity"}}
        old_engine = service.registry.engine(sid)
        assert client.post("/api/v1/query", json=query).json()["data"] == [{"value": 19}]
        assert client.post("/api/v1/query", json=query).json()["meta"]["cache_hit"]
        with remote.begin() as db:
            db.exec_driver_sql("UPDATE stock SET quantity = 100 WHERE item_id = 1")
            db.exec_driver_sql("DELETE FROM stock WHERE item_id = 2")
        assert client.post(f"/api/v1/connections/{cid}/refresh", json={}).status_code == 200
        service.tick()
        result = client.post("/api/v1/query", json=query).json()
        assert result["success"] and result["data"] == [{"value": 100}]
        assert not result["meta"]["cache_hit"] and result["meta"]["snapshot_updated_at"]
        assert old_engine.query(plan={"metric": "quantity"})["data"] == [{"value": 19}]
        assert parser.calls == []
        persisted = (service.directory / f"{cid}.json").read_text()
        assert "never-print-this-secret" not in persisted and "reader" not in persisted and "127.0.0.1" not in persisted
        public = client.get("/api/v1/connections").text
        assert "encrypted" not in public and "never-print" not in public
        assert client.post(f"/api/v1/connections/{cid}/disconnect", json={}).status_code == 200
        assert not (service.directory / f"{cid}.json").exists()
        assert client.post("/api/v1/query", json=query).json()["data"] == [{"value": 100}]
        reopened = SourceRegistry(root / "app")
        assert reopened.engine(sid).query(plan={"metric": "quantity"})["data"] == [{"value": 100}]


def test_schema_drift_keeps_previous_snapshot(setup):
    root, remote, config = setup
    service = c.ConnectionService(SourceRegistry(root / "app"))
    record = create_ready(service, config)
    old_path = service.registry.engine(record["source_id"]).database_path
    with remote.begin() as db:
        db.exec_driver_sql("ALTER TABLE stock RENAME COLUMN quantity TO units")
    service.action(record["id"], "refresh", {})
    service.tick()
    assert record["status"] == "failed" and "missing" in record["error"]
    assert service.registry.engine(record["source_id"]).database_path == old_path
    assert service.registry.engine(record["source_id"]).query(plan={"metric": "quantity"})["data"] == [{"value": 19}]


def test_atomic_failure_redacts_driver_errors(setup, monkeypatch):
    root, _, config = setup
    service = c.ConnectionService(SourceRegistry(root / "app"))
    record = create_ready(service, config)
    def failure(*args):
        raise RuntimeError("password=never-print-this-secret; row=private@example.test")
    monkeypatch.setattr(service.adapter, "extract", failure)
    service.action(record["id"], "refresh", {})
    service.tick()
    assert record["status"] == "failed" and record["error"] == c.FAILURE
    assert record["last_success"] and record["rows"] == 2
    assert not list(service.registry.directory.glob(".refresh-*"))
    assert "never-print" not in json.dumps(service.list())


def test_schedule_persistence_due_refresh_and_pause(setup):
    root, remote, config = setup
    service = c.ConnectionService(SourceRegistry(root / "app"))
    record = create_ready(service, config)
    service.action(record["id"], "schedule", {"schedule": "hourly"})
    assert 3590 < record["next_refresh"] - time.time() <= 3600
    record["next_refresh"] = time.time() - 1
    service._persist(record)
    reopened = c.ConnectionService(SourceRegistry(root / "app"))
    with remote.begin() as db:
        db.exec_driver_sql("UPDATE stock SET quantity=20")
    reopened.tick()
    current = reopened.records[record["id"]]
    assert current["status"] == "succeeded" and len(current["history"]) == 2
    assert reopened.registry.engine(current["source_id"]).query(plan={"metric": "quantity"})["data"] == [{"value": 40}]
    reopened.action(record["id"], "schedule", {"schedule": "manual"})
    assert current["next_refresh"] is None


def test_restart_recovers_interrupted_job(setup):
    root, _, config = setup
    service = c.ConnectionService(SourceRegistry(root / "app"))
    created = service.create(config)
    reopened = c.ConnectionService(SourceRegistry(root / "app"))
    reopened._recover()
    record = reopened.records[created["id"]]
    assert record["status"] == "failed" and "restart" in record["error"]
    reopened.action(created["id"], "refresh", {})
    reopened.tick()
    assert record["status"] == "succeeded"


def test_wrong_key_and_revoked_destination_preserve_data(setup, monkeypatch):
    root, _, config = setup
    service = c.ConnectionService(SourceRegistry(root / "app"))
    record = create_ready(service, config)
    monkeypatch.setenv("AIDA_CONNECTOR_KEY", Fernet.generate_key().decode())
    reopened = c.ConnectionService(SourceRegistry(root / "app"))
    reopened.action(record["id"], "refresh", {})
    reopened.tick()
    assert "unlocked" in reopened.records[record["id"]]["error"]
    monkeypatch.setenv("AIDA_CONNECTOR_HOSTS", "")
    service.action(record["id"], "refresh", {})
    service.tick()
    assert "destination" in record["error"]
    assert service.registry.engine(record["source_id"]).query(plan={"metric": "quantity"})["data"] == [{"value": 19}]


@pytest.mark.parametrize("patch", [{"host": "169.254.169.254"}, {"host": "http://127.0.0.1"}, {"host": "localhost;password=oops"}, {"port": True}, {"port": 0}, {"engine": "oracle"}, {"tls": "false"}, {"url": "sqlite:///secret"}])
def test_connection_policy_rejects_unapproved_inputs(setup, patch):
    _, _, config = setup
    with pytest.raises(SourceError):
        c.validate_connection({**config["connection"], **patch})


def test_remote_tls_cannot_be_disabled(setup, monkeypatch):
    _, _, config = setup
    monkeypatch.setenv("AIDA_CONNECTOR_HOSTS", "db.example.test:5432")
    with pytest.raises(SourceError, match="TLS"):
        c.validate_connection({**config["connection"], "host": "db.example.test"})


def test_limits_and_numeric_precision_fail_closed(setup, monkeypatch):
    root, _, config = setup
    for value in [Decimal("12345678901234567890.1234"), float("nan"), 2**63]:
        with pytest.raises(SourceError):
            c.scalar(value)
    monkeypatch.setattr(c, "MAX_ROWS", 1)
    service = c.ConnectionService(SourceRegistry(root / "app"))
    created = service.create(config)
    service.tick()
    assert service.records[created["id"]]["status"] == "failed"
    assert service.records[created["id"]]["source_id"] is None
    assert not service.registry.list_sources()


def test_public_demo_and_cross_origin_block_connections(setup):
    root, _, config = setup
    with TestClient(create_app(root / "public", interpreter=StubInterpreter(), public_demo=True)) as client:
        assert client.get("/api/v1/connections").json()["connections"] == []
        for endpoint in ["connections", "connections/inspect", "connections/abc/refresh"]:
            assert client.post(f"/api/v1/{endpoint}", json=config).status_code == 403
    with TestClient(create_app(root / "local", interpreter=StubInterpreter(), public_demo=False)) as client:
        assert client.post("/api/v1/connections", json=config, headers={"Origin": "https://evil.example"}).status_code == 403
        assert client.post("/api/v1/connections", content=b"x" * 65537).status_code == 413


@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlserver"])
def test_driver_configuration_requires_tls_and_structured_credentials(setup, monkeypatch, engine):
    _, _, config = setup
    spec = {**config["connection"], "engine": engine, "schema": "test", "tls": True, "password": "a;{}:@/secret"}
    captured = {}
    def build(url, **options):
        captured.update(url=url, **options)
        return captured
    monkeypatch.setattr(sa, "create_engine", build)
    if engine == "sqlserver":
        import pyodbc
        monkeypatch.setattr(pyodbc, "drivers", lambda: ["ODBC Driver 18 for SQL Server"])
    c.create_remote_engine(spec)
    assert captured["url"].password == spec["password"]
    assert captured["hide_parameters"] and not captured["echo"]
    if engine == "postgresql":
        assert captured["connect_args"]["sslmode"] == "verify-full"
    elif engine == "mysql":
        assert captured["connect_args"]["ssl"].check_hostname
        assert not captured["connect_args"]["local_infile"]
    else:
        assert captured["url"].query["TrustServerCertificate"] == "no"


def test_same_named_columns_and_join_survive_extraction(setup):
    root, remote, config = setup
    with remote.begin() as db:
        db.exec_driver_sql("CREATE TABLE regions (region_id INTEGER PRIMARY KEY, category TEXT)")
        db.exec_driver_sql("INSERT INTO regions VALUES (1, 'West'), (2, 'East')")
    config["selection"].append({"table": "regions", "columns": ["region_id", "category"]})
    service = c.ConnectionService(SourceRegistry(root / "app"))
    record = create_ready(service, config)
    path = service.registry.engine(record["source_id"]).database_path
    with closing(sqlite3.connect(path)) as db:
        assert db.execute('SELECT r.category, SUM(s.quantity) FROM stock s JOIN regions r ON s.item_id=r.region_id GROUP BY r.category ORDER BY r.category').fetchall() == [('East', 8), ('West', 11)]


def test_worker_lock_and_crash_generation_cleanup(setup):
    root, _, _ = setup
    registry = SourceRegistry(root / "app")
    orphan = registry.directory / ('a' * 32 + '.' + 'b' * 32 + '.sqlite')
    orphan.write_bytes(b"orphan generation")
    first = c.ConnectionService(registry)
    second = c.ConnectionService(SourceRegistry(root / "app"))
    start_worker(first)
    try:
        assert not orphan.exists()
        with pytest.raises(SourceError, match="Another refresh worker"):
            start_worker(second)
    finally:
        first.close()
    start_worker(second)
    second.close()


def test_authenticated_connections_and_snapshots_are_account_scoped(setup):
    from backend.test_security import H, signup, second_client
    root, remote, config = setup
    with TestClient(create_app(root / "accounts", interpreter=StubInterpreter(), public_demo=False, require_auth=True)) as owner:
        assert owner.get('/api/v1/connections').status_code == 401
        assert owner.post('/api/v1/connections', json=config, headers=H).status_code == 401
        signup(owner)
        member = second_client(owner)
        created = member.post('/api/v1/connections', json=config, headers=H)
        assert created.status_code == 202, created.text
        cid = created.json()['id']
        service = owner.app.state.connections
        service.tick()
        record = member.get('/api/v1/connections').json()['connections'][0]
        sid = record['source_id']
        assert owner.get('/api/v1/connections').json()['connections'] == []
        for action, body in [('refresh', {}), ('schedule', {'schedule':'manual'}), ('disconnect', {})]:
            assert owner.post(f'/api/v1/connections/{cid}/{action}', json=body, headers=H).status_code == 400
        assert owner.get(f'/api/v1/sources/{sid}').status_code == 400
        assert member.post(f'/api/v1/sources/{sid}/configure', json=mapping(), headers=H).status_code == 200
        assert owner.post('/api/v1/query', json={'source_id':sid,'plan':{'metric':'quantity'}}, headers=H).status_code == 400
        assert member.post(f'/api/v1/connections/{cid}/refresh', json={}).status_code == 403
        assert member.post(f'/api/v1/connections/{cid}/refresh', json={}, headers=H).status_code == 200
        service.tick()
        assert owner.get(f'/api/v1/sources/{sid}').status_code == 400
        assert member.post('/api/v1/query', json={'source_id':sid,'plan':{'metric':'quantity'}}, headers=H).json()['data'] == [{'value':19}]
        member_id = member.get('/api/v1/auth/session').json()['user']['id']
        reopened = SourceRegistry(root / 'accounts')
        assert reopened.inspect_source(sid, {'id':member_id,'role':'member'})['id'] == sid
        assert member.post(f'/api/v1/connections/{cid}/disconnect', json={}, headers=H).status_code == 200
        assert member.get(f'/api/v1/sources/{sid}').status_code == 200
        member.close()


def test_explicit_runtime_settings_enable_connector_key_and_destination(setup, monkeypatch):
    root, _, config = setup
    settings = {'AIDA_CONNECTOR_KEY': Fernet.generate_key().decode(), 'AIDA_CONNECTOR_HOSTS':'127.0.0.1:5432', 'AIDA_CONNECTOR_LOCAL_TEST':'1'}
    monkeypatch.delenv('AIDA_CONNECTOR_KEY')
    monkeypatch.delenv('AIDA_CONNECTOR_HOSTS')
    service = c.ConnectionService(SourceRegistry(root / 'configured'), settings=settings)
    record = create_ready(service, config)
    assert record['status'] == 'succeeded'
