"""API boundary tests; the parser below is explicitly a stub, not model evidence."""
import copy
import json
import sqlite3
from contextlib import closing

import pytest
from fastapi.testclient import TestClient

from backend.core.semantic import ModelUnavailable, SemanticResult
from backend.main import create_app


class StubParser:
    """Records API-to-interpreter wiring without network or model inference."""
    def __init__(self, plan=None, failure=None):
        self.plan = plan or {"metric": "tickets", "dimension": "team"}
        self.failure = failure
        self.calls = []

    def status(self):
        return {"configured": True, "ready": True, "model": "explicit-api-test-stub", "inference_location": "local"}

    def parse(self, question, catalog, as_of=None):
        self.calls.append({"question": question, "catalog": copy.deepcopy(catalog), "as_of": as_of})
        if self.failure:
            raise self.failure
        return SemanticResult(plan=copy.deepcopy(self.plan), ir={"kind": "api-test-stub"}, telemetry={"model_calls": 1, "interpretation_source": "api_test_stub", "input_tokens": 10, "output_tokens": 8})


@pytest.fixture
def parser():
    return StubParser()


@pytest.fixture
def client(tmp_path, parser):
    with TestClient(create_app(tmp_path / "application", semantic_parser=parser, public_demo=False)) as instance:
        yield instance


@pytest.fixture
def uploaded_bytes(tmp_path):
    path = tmp_path / "supply.sqlite"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE supply (sku_id INTEGER PRIMARY KEY, aisle TEXT, on_hand INTEGER, checked_at TEXT, buyer_email TEXT)")
        connection.executemany("INSERT INTO supply VALUES (?,?,?,?,?)", [(1, "Garden", 11, "2026-08-01", "private@example.test"), (2, "Garden", 14, "2026-08-02", "secret@example.test"), (3, "Tools", 8, "2026-08-03", "hidden@example.test")])
    return path.read_bytes()


def upload(client, uploaded_bytes):
    response = client.post("/api/v1/sources", content=uploaded_bytes, headers={"Content-Type": "application/octet-stream", "X-Source-Name": "Supply snapshot"})
    assert response.status_code == 200, response.text
    return response.json()


def mapping():
    return {"name": "Supply snapshot", "table": "supply", "metrics": [{"id": "stock", "label": "Available stock", "description": "Sum of on-hand units across stored stock positions.", "aggregate": "SUM", "column": "on_hand"}], "dimensions": [{"id": "aisle", "label": "Aisle", "column": "aisle"}], "date_column": "checked_at", "as_of": "2026-08-31"}


def test_saved_plan_version_cannot_silently_change_business_meaning(client, uploaded_bytes, parser):
    source_id = upload(client, uploaded_bytes)["id"]
    first = client.post(f"/api/v1/sources/{source_id}/configure", json=mapping()).json()
    request = {"source_id": source_id, "catalog_version": first["dataset"]["catalog_version"], "plan": {"metric": "stock"}}
    assert client.post("/api/v1/query", json=request).json()["data"] == [{"value": 33}]
    changed = mapping()
    changed["metrics"][0]["aggregate"] = "AVG"
    second = client.post(f"/api/v1/sources/{source_id}/configure", json=changed).json()
    assert second["dataset"]["catalog_version"] != request["catalog_version"]
    stale = client.post("/api/v1/query", json=request).json()
    assert stale["success"] is False and stale["error_type"] == "catalog_changed"
    assert "sql" not in stale and parser.calls == []
    request["catalog_version"] = second["dataset"]["catalog_version"]
    assert client.post("/api/v1/query", json=request).json()["data"] == [{"value": 11.0}]


def test_source_onboarding_roundtrip_to_bounded_aggregate(client, uploaded_bytes, parser):
    sources = client.get("/api/v1/sources").json()
    assert {source["id"] for source in sources["sources"]} == {"commerce", "support", "warehouse", "billing", "chinook"}
    assert sources["uploads_enabled"]
    inspected = upload(client, uploaded_bytes)
    source_id = inspected["id"]
    assert not inspected["configured"] and not inspected["synthetic"]
    assert "private@example.test" not in json.dumps(inspected)
    assert "Garden" not in json.dumps(inspected)
    assert client.get(f"/api/v1/sources/{source_id}").json() == inspected
    pending = client.post("/api/v1/query", json={"source_id": source_id, "plan": {"metric": "stock"}})
    assert pending.status_code == 400 and "Approve" in pending.json()["error"]
    approved = client.post(f"/api/v1/sources/{source_id}/configure", json=mapping())
    assert approved.status_code == 200, approved.text
    assert approved.json()["dataset"]["as_of"] == "2026-08-31"
    assert "buyer_email" not in json.dumps(approved.json())
    response = client.post("/api/v1/query", json={"source_id": source_id, "plan": {"metric": "stock", "dimension": "aisle"}})
    assert response.status_code == 200 and response.json()["success"], response.text
    assert response.json()["data"] == [{"aisle": "Garden", "value": 25}, {"aisle": "Tools", "value": 8}]
    assert response.json()["source_id"] == source_id
    assert response.json()["meta"]["model_calls"] == 0
    assert parser.calls == []
    assert client.get("/api/v1/catalog", params={"source_id": source_id}).json()["dataset"]["id"] == source_id


def test_question_uses_selected_catalog_and_its_actual_reference_date(client, parser):
    response = client.post("/api/v1/query", json={"source_id": "support", "question": "Count support requests for each team"})
    assert response.status_code == 200 and response.json()["success"], response.text
    assert response.json()["data"] == [{"team": "Accounts", "value": 56}, {"team": "Billing", "value": 56}, {"team": "Technical", "value": 56}]
    assert response.json()["meta"]["model_calls"] == 1
    assert response.json()["meta"]["interpretation_source"] == "api_test_stub"
    assert parser.calls[0]["as_of"] == "2026-06-30"
    assert parser.calls[0]["catalog"]["dataset"]["id"] == "support"
    assert {metric["id"] for metric in parser.calls[0]["catalog"]["metrics"]} == {"tickets", "resolution_time"}


def test_model_plan_cannot_bypass_source_mapping(client, parser):
    parser.plan = {"metric": "revenue", "dimension": "region"}
    response = client.post("/api/v1/query", json={"source_id": "support", "question": "Show revenue by region"})
    assert response.status_code == 200
    assert not response.json()["success"]
    assert response.json()["error_type"] == "clarification_required"
    parser.plan = {"metric": "tickets", "sql": "SELECT * FROM support_tickets"}
    response = client.post("/api/v1/query", json={"source_id": "support", "question": "Return everything"})
    assert not response.json()["success"]
    assert "sql" not in response.json()


def test_model_unavailable_is_explicit_and_builder_remains_available(tmp_path):
    parser = StubParser(failure=ModelUnavailable("The local semantic model is unavailable."))
    with TestClient(create_app(tmp_path, semantic_parser=parser, public_demo=False)) as client:
        response = client.post("/api/v1/query", json={"source_id": "support", "question": "Count support tickets"})
        assert response.json()["error_type"] == "model_unavailable"
        assert response.json()["data"] == []
        builder = client.post("/api/v1/query", json={"source_id": "support", "plan": {"metric": "tickets"}})
        assert builder.json()["data"] == [{"value": 168}]


@pytest.mark.parametrize("headers", [{"Host": "attacker.example"}, {"Origin": "https://attacker.example"}, {"Origin": "null"}, {"Origin": "http://localhost:9999"}])
def test_local_mode_rejects_untrusted_hosts_and_cross_origin_changes(client, headers):
    response = client.post("/api/v1/query", json={"plan": {"metric": "orders"}}, headers=headers)
    assert response.status_code == 403
    assert response.headers["Cache-Control"] == "no-store"


def test_same_origin_browser_request_is_accepted(client):
    response = client.post("/api/v1/query", json={"plan": {"metric": "orders"}}, headers={"Origin": "http://testserver"})
    assert response.status_code == 200 and response.json()["success"]


def test_query_mapping_and_upload_body_limits(client):
    assert client.post("/api/v1/query", content=b"x" * 8193).status_code == 413
    assert client.post("/api/v1/sources/commerce/configure", content=b"x" * 65537).status_code == 413
    assert client.post("/api/v1/sources", content=b"x" * (20 * 1024 * 1024 + 1), headers={"Content-Type": "application/octet-stream"}).status_code == 413
    assert client.post("/api/v1/query", content=iter([b"x" * 4000, b"x" * 4193])).status_code == 413


def test_public_mode_rejects_uploads_and_mapping_before_reading_body(tmp_path, uploaded_bytes):
    with TestClient(create_app(tmp_path, semantic_parser=StubParser(), public_demo=True)) as client:
        assert client.get("/api/v1/sources").json()["uploads_enabled"] is False
        assert client.post("/api/v1/sources", content=uploaded_bytes, headers={"Content-Type": "application/octet-stream"}).status_code == 403
        assert client.post("/api/v1/sources/support/configure", json=mapping()).status_code == 403
        assert client.post("/api/v1/query", json={"source_id": "support", "plan": {"metric": "tickets"}}, headers={"Host": "demo.example"}).json()["success"]


@pytest.mark.parametrize("body", [{}, {"question": "   "}, {"question": "Tickets", "plan": {"metric": "tickets"}}, {"source_id": "../../secret", "question": "Tickets"}, {"question": "Tickets", "database_path": "C:/private.sqlite"}, {"question": "x" * 1501}])
def test_request_validation_does_not_echo_raw_question_or_path(client, body):
    response = client.post("/api/v1/query", json=body)
    assert response.status_code == 422
    assert "input" not in response.json()
    assert "C:/private.sqlite" not in response.text


def test_unknown_source_and_invalid_mapping_return_controlled_errors(client, uploaded_bytes):
    assert client.get("/api/v1/catalog", params={"source_id": "missing"}).status_code == 400
    assert client.post("/api/v1/sources", json={"path": "C:/private.sqlite"}).status_code == 415
    source_id = upload(client, uploaded_bytes)["id"]
    invalid = mapping()
    invalid["metrics"][0]["aggregate"] = []
    response = client.post(f"/api/v1/sources/{source_id}/configure", json=invalid)
    assert response.status_code == 400
    invalid = mapping()
    invalid["dimensions"][0]["column"] = "buyer_email"
    response = client.post(f"/api/v1/sources/{source_id}/configure", json=invalid)
    assert response.status_code == 400 and "excluded" in response.json()["error"]


def test_public_restart_hides_persisted_private_sources_from_every_route(tmp_path, uploaded_bytes):
    directory = tmp_path / "persisted"
    parser = StubParser()
    with TestClient(create_app(directory, semantic_parser=parser, public_demo=False)) as local:
        approved_id = upload(local, uploaded_bytes)["id"]
        assert local.post(f"/api/v1/sources/{approved_id}/configure", json=mapping()).status_code == 200
        pending_id = upload(local, uploaded_bytes)["id"]
    with TestClient(create_app(directory, semantic_parser=parser, public_demo=True)) as public:
        listed = public.get("/api/v1/sources").json()
        assert {source["id"] for source in listed["sources"]} == {"commerce", "support", "warehouse", "billing", "chinook"}
        assert "Supply snapshot" not in json.dumps(listed)
        for source_id in (approved_id, pending_id):
            inspection = public.get(f"/api/v1/sources/{source_id}")
            assert inspection.status_code == 400 and "tables" not in inspection.json()
            for endpoint in ("catalog", "schema", "examples"):
                response = public.get(f"/api/v1/{endpoint}", params={"source_id": source_id})
                assert response.status_code == 400
                assert "on_hand" not in response.text and "private@example.test" not in response.text
            assert public.post("/api/v1/query", json={"source_id": source_id, "plan": {"metric": "stock"}}).status_code == 400
            assert public.post("/api/v1/query", json={"source_id": source_id, "question": "Show available stock"}).status_code == 400
            assert public.post(f"/api/v1/sources/{source_id}/configure", json=mapping()).status_code == 403
        assert parser.calls == []
