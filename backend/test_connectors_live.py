"""Opt-in network tests. Supply read-only credentials for a disposable test schema.

No live database is contacted unless its corresponding environment variable exists.
See docs/CONNECTIONS.md for fixture SQL and invocation. These tests never seed or
modify the remote database; the DBA prepares aida_connector_probe beforehand.
"""
import json
import os
import sqlite3
from contextlib import closing

import pytest
from cryptography.fernet import Fernet

from backend.core.connectors import ConnectionService
from backend.core.sources import SourceRegistry


@pytest.mark.parametrize("engine,variable", [
    ("postgresql", "AIDA_TEST_POSTGRESQL"), ("mysql", "AIDA_TEST_MYSQL"), ("sqlserver", "AIDA_TEST_SQLSERVER")])
def test_live_metadata_extract_query_and_second_refresh(tmp_path, monkeypatch, engine, variable):
    raw = os.environ.get(variable)
    if not raw:
        pytest.skip(f"{variable} is not configured; no live {engine} evidence")
    spec = json.loads(raw)
    assert spec["engine"] == engine
    monkeypatch.setenv("AIDA_CONNECTOR_KEY", Fernet.generate_key().decode())
    monkeypatch.setenv("AIDA_CONNECTOR_HOSTS", f"{spec['host']}:{spec['port']}")
    service = ConnectionService(SourceRegistry(tmp_path / engine))
    inspected = service.inspect(spec)
    assert any(t["name"] == "aida_connector_probe" for t in inspected["tables"])
    created = service.create({"name": "Disposable connector probe", "connection": spec, "schedule": "manual",
                              "selection": [{"table": "aida_connector_probe", "columns": ["probe_id", "category", "quantity"]}]})
    service.tick()
    record = service.records[created["id"]]
    assert record["status"] == "succeeded", record["error"]
    sid = record["source_id"]
    service.registry.configure(sid, {"name": "Connector probe", "table": "aida_connector_probe",
        "metrics": [{"id": "quantity", "label": "Quantity", "description": "Total test units", "aggregate": "SUM", "column": "quantity"}],
        "dimensions": [{"id": "category", "label": "Category", "column": "category"}]})
    first = service.registry.engine(sid)
    assert first.query(plan={"metric": "quantity"})["data"] == [{"value": 19}]
    with closing(sqlite3.connect(first.database_path)) as db:
        assert "private_note" not in [row[1] for row in db.execute('PRAGMA table_info("aida_connector_probe")')]
    service.action(created["id"], "refresh", {})
    service.tick()
    assert record["status"] == "succeeded", record["error"]
    second = service.registry.engine(sid)
    assert second.database_path != first.database_path and second.catalog_version == first.catalog_version
    assert second.query(plan={"metric": "quantity"})["data"] == [{"value": 19}]
