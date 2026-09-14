"""Independent public-data SQL checks and API wiring; no model accuracy claims."""
import copy
import json
import sqlite3
from contextlib import closing

import pytest
from fastapi.testclient import TestClient

from backend.core.chinook import chinook_cases, chinook_source
from backend.core.interpreter import Interpretation
from backend.core.relational import RelationalEngine, validate_relational_manifest
from backend.core.sources import inspect_database
from backend.main import create_app


@pytest.fixture(scope="module")
def chinook_engine():
    source = chinook_source()
    manifest = validate_relational_manifest(source["manifest"], inspect_database(source["path"]))
    return RelationalEngine(source["path"], manifest, "chinook", synthetic=True)


@pytest.mark.parametrize("case", chinook_cases(), ids=lambda case: case["id"])
def test_public_chinook_matches_independent_sql(chinook_engine, case):
    result = chinook_engine.query(plan=case["plan"])
    assert result["success"], result.get("error")
    with closing(sqlite3.connect(chinook_source()["path"])) as connection:
        connection.row_factory = sqlite3.Row
        expected = [dict(row) for row in connection.execute(case["oracle_sql"])]
    assert result["data"] == expected
    assert result["meta"]["model_calls"] == 0
    assert "Email" not in result["sql"] and "FirstName" not in result["sql"]


def test_public_sample_keeps_conflicting_unit_price_columns_distinct(chinook_engine):
    case = next(case for case in chinook_cases() if case["id"] == "chinook_physical_price")
    result = chinook_engine.query(plan=case["plan"])
    assert result["success"]
    assert "InvoiceLine" in result["sql"] and "Track" in result["sql"]
    assert "UnitPrice" in result["sql"] and result["lineage"]
    assert "Invoice" in result["sql"] and "Customer" in result["sql"]
    assert "Employee" not in result["sql"]


class RelationalStub:
    """Explicitly marked API test stub. Real-model proof lives in the benchmark."""
    def __init__(self):
        self.calls = []

    def status(self):
        return {"available": True, "status": "ready", "model": "api-test-stub"}

    def interpret(self, question, catalog):
        self.calls.append((question, catalog["dataset"]["id"]))
        return Interpretation(copy.deepcopy(chinook_cases()[0]["plan"]), [], {}, [], [], {"api_test_stub": True}, {"model_calls": 2})


def test_relational_api_routes_selected_source_and_rejects_cross_source_plan(tmp_path):
    interpreter = RelationalStub()
    with TestClient(create_app(tmp_path, interpreter=interpreter)) as client:
        catalog = client.get("/api/v1/catalog?source_id=chinook").json()
        result = client.post("/api/v1/query", json={"source_id": "chinook", "question": "Units sold by customer country", "catalog_version": catalog["dataset"]["catalog_version"]}).json()
        assert result["success"] and result["source_id"] == "chinook"
        assert result["semantic_ir"]["api_test_stub"] and interpreter.calls == [("Units sold by customer country", "chinook")]
        crossed = client.post("/api/v1/query", json={"source_id": "warehouse", "plan": result["plan"]}).json()
        assert not crossed["success"] and "sql" not in crossed
        assert crossed["meta"]["model_calls"] == 0
        stale = client.post("/api/v1/query", json={"source_id": "chinook", "question": "Units sold", "catalog_version": "0000000000000000"}).json()
        assert not stale["success"] and stale["error_type"] == "catalog_changed"
        assert len(interpreter.calls) == 1


def test_uploaded_relational_mapping_persists_and_stays_private(tmp_path):
    source = chinook_source()
    with TestClient(create_app(tmp_path, interpreter=RelationalStub())) as client:
        uploaded = client.post("/api/v1/sources", content=source["path"].read_bytes(), headers={"Content-Type": "application/octet-stream", "X-Source-Name": "Independent music snapshot"}).json()
        assert "Country" in json.dumps(uploaded) and "USA" not in json.dumps(uploaded)
        assert any(table["foreign_keys"] for table in uploaded["tables"])
        source_id = uploaded["id"]
        config = copy.deepcopy(source["manifest"])
        config["name"] = "Independent music snapshot"
        approved = client.post(f"/api/v1/sources/{source_id}/configure", json=config)
        assert approved.status_code == 200, approved.text
        result = client.post("/api/v1/query", json={"source_id": source_id, "plan": chinook_cases()[0]["plan"]}).json()
        assert result["success"] and result["source_id"] == source_id
    with TestClient(create_app(tmp_path, interpreter=RelationalStub(), public_demo=True)) as client:
        assert source_id not in {item["id"] for item in client.get("/api/v1/sources").json()["sources"]}
        assert client.get(f"/api/v1/sources/{source_id}").status_code == 400
        assert client.post("/api/v1/query", json={"source_id": source_id, "plan": chinook_cases()[0]["plan"]}).status_code == 400
    with TestClient(create_app(tmp_path, interpreter=RelationalStub(), public_demo=False)) as client:
        result = client.post("/api/v1/query", json={"source_id": source_id, "plan": chinook_cases()[0]["plan"]}).json()
        assert result["success"]
