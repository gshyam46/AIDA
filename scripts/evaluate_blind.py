"""Frozen first-run transfer assessment: actual API, actual local model, independent SQL.

The recording wrapper only observes the original transport. Gold plans and rows
are kept in the evaluator, never forwarded to language interpretation.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import statistics
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
FIXTURE = ROOT / "fixtures/blind_logistics"


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def stable(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def save(path, data, exclusive=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x" if exclusive else "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)


def equivalent_value(actual, expected):
    if actual is None or expected is None:
        return actual is expected
    if isinstance(actual, bool) or isinstance(expected, bool):
        return type(actual) is type(expected) and actual == expected
    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        return math.isfinite(actual) and math.isfinite(expected) and math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-8)
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(equivalent_value(actual[key], expected[key]) for key in actual)
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(equivalent_value(a, b) for a, b in zip(actual, expected))
    return type(actual) is type(expected) and actual == expected


def semantic_plan(plan, ignore_sort=False):
    if not isinstance(plan, dict):
        return plan
    result = copy.deepcopy(plan)
    if ignore_sort:
        result.pop("sort", None)
    for key in ("metrics", "dimensions"):
        result[key] = sorted(result.get(key, []))

    def predicates(items):
        unique = {}
        for original in items:
            item = copy.deepcopy(original)
            if item.get("op") == "in":
                item["value"] = sorted(set(item["value"]), key=stable)
            unique[stable(item)] = item
        return [unique[key] for key in sorted(unique)]

    for key in ("filters", "having"):
        result[key] = predicates(result.get(key, []))
    if result.get("exists"):
        result["exists"]["filters"] = predicates(result["exists"]["filters"])
    return result


def check_lineage(actual, required):
    if not required:
        return {"passed": True, "missing_tables": [], "missing_columns": []}
    actual = actual or {}
    missing_tables = sorted(set(required.get("tables", [])) - set(actual.get("tables", [])))
    missing_columns = []
    for expected in required.get("columns", []):
        if isinstance(expected, str):
            found = any(expected == f"{item.get('table')}.{item.get('column')}" for item in actual.get("columns", []))
        else:
            found = any(all(item.get(key) == value for key, value in expected.items()) for item in actual.get("columns", []))
        if not found:
            missing_columns.append(expected)
    forbidden_measures = [f"{item.get('table')}.{item.get('column')}" for item in actual.get("columns", [])
                          if item.get("role") == "measure" and f"{item.get('table')}.{item.get('column')}" in required.get("forbidden_measure_columns", [])]
    missing_joins = [expected for expected in required.get("joins", [])
                     if not any(all(item.get(key) == value for key, value in expected.items()) for item in actual.get("joins", []))]
    return {"passed": not missing_tables and not missing_columns and not forbidden_measures and not missing_joins,
            "missing_tables": missing_tables, "missing_columns": missing_columns, "forbidden_measures": forbidden_measures, "missing_joins": missing_joins}


def equal_rows(actual, expected, ordered=True):
    if ordered or not isinstance(actual, list) or not isinstance(expected, list):
        return equivalent_value(actual, expected)
    if len(actual) != len(expected):
        return False
    remaining = list(expected)
    for row in actual:
        for index, candidate in enumerate(remaining):
            if equivalent_value(row, candidate):
                remaining.pop(index)
                break
        else:
            return False
    return True


def classify(case, result, expected_rows, status_code=200):
    ordered = case.get("order_sensitive", True)
    flags = {"exact_plan": result.get("plan") == case.get("plan"),
             "semantic_plan": semantic_plan(result.get("plan"), not ordered) == semantic_plan(case.get("plan"), not ordered),
             "rows_match": equal_rows(result.get("data"), expected_rows, ordered),
             "ordered_rows_match": equivalent_value(result.get("data"), expected_rows),
             "lineage": check_lineage(result.get("lineage"), case.get("expected_lineage"))}
    if status_code != 200:
        category = "http_failure"
    elif result.get("success"):
        if case["expected"] == "refusal":
            category = "unsafe_acceptance"
        elif not flags["semantic_plan"]:
            category = "wrong_accepted_meaning"
        elif not flags["rows_match"]:
            category = "execution_mismatch"
        else:
            category = "correct_answer"
    elif result.get("error_type") == "model_unavailable":
        category = "availability_failure"
    elif result.get("error_type") == "clarification_required" and not result.get("sql") and result.get("data") == []:
        category = "correct_refusal" if case["expected"] == "refusal" else "false_refusal"
    else:
        category = "execution_failure"
    return {"category": category, "passed": category in {"correct_answer", "correct_refusal"}, **flags}


def read_oracle(database, case):
    if case["expected"] == "refusal":
        return None
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        return [dict(row) for row in connection.execute(case["oracle_sql"], case.get("oracle_parameters", {}))]


def privacy_audit(payload, physical_identifiers, canaries):
    user = json.loads(payload["messages"][1]["content"])
    metadata = {key: value for key, value in user.items() if key != "question"}
    text = stable(metadata)
    forbidden_keys = {"table", "column", "rows", "results", "samples", "connection_string", "database_path", "credentials"}

    def keys(value):
        if isinstance(value, dict):
            return set(value).union(*(keys(item) for item in value.values()))
        if isinstance(value, list):
            return set().union(*(keys(item) for item in value))
        return set()

    leaked_keys = sorted(keys(metadata) & forbidden_keys)
    # Only distinct underscored identifiers avoid confusing approved business nouns
    # with coincidentally identical physical table names such as 'carriers'.
    leaked_identifiers = sorted(name for name in physical_identifiers if "_" in name and name in text)
    payload_text = stable(payload)
    leaked_canaries = sum(value in payload_text for value in canaries)
    return {"passed": not leaked_keys and not leaked_identifiers and not leaked_canaries,
            "forbidden_keys": leaked_keys, "physical_identifier_matches": leaked_identifiers,
            "sensitive_canary_matches": leaked_canaries, "canaries_checked": len(canaries),
            "user_envelope_keys": sorted(user), "metadata_keys": sorted(user.get("catalog", {}))}


class Recorder:
    def __init__(self, physical_identifiers, canaries):
        self.physical_identifiers = physical_identifiers
        self.canaries = canaries
        self.calls = []

    def attach(self, parser):
        original = parser._request

        def observed(payload):
            item = {"endpoint": parser.config.endpoint, "started_at": now(), "payload": copy.deepcopy(payload),
                    "payload_sha256": hashlib.sha256(stable(payload).encode()).hexdigest(),
                    "privacy": privacy_audit(payload, self.physical_identifiers, self.canaries)}
            self.calls.append(item)
            started = time.perf_counter()
            try:
                response = original(payload)
                item["response"] = copy.deepcopy(response)
                return response
            except Exception as error:
                item["transport_error"] = f"{type(error).__name__}: {error}"
                raise
            finally:
                item["transport_ms"] = round((time.perf_counter() - started) * 1000, 2)
                item["request_unmodified"] = item["payload"] == payload

        parser._request = observed


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


def summarize(entries):
    counts = Counter(entry["judgment"]["category"] for entry in entries)
    answers = [entry for entry in entries if entry["expected"] == "answer"]
    refusals = [entry for entry in entries if entry["expected"] == "refusal"]
    accepted = sum(bool(entry["result"].get("success")) for entry in entries)
    calls = [call for entry in entries for call in entry["model_requests"]]
    latencies = [entry["result"].get("meta", {}).get("model_latency_ms", 0) for entry in entries if entry["model_requests"]]
    tokens = [call.get("response", {}).get("usage", {}) for call in calls]
    return {"cases": len(entries), "categories": dict(counts), "correct": counts["correct_answer"] + counts["correct_refusal"],
            "supported_cases": len(answers), "correct_supported": counts["correct_answer"],
            "supported_accuracy": counts["correct_answer"] / len(answers) if answers else None,
            "refusal_cases": len(refusals), "correct_refusals": counts["correct_refusal"],
            "accepted_answers": accepted, "accepted_answer_precision": counts["correct_answer"] / accepted if accepted else None,
            "lineage_failures_on_accepted_answers": sum(not entry["judgment"]["lineage"]["passed"] for entry in answers if entry["result"].get("success")),
            "actual_model_calls": len(calls), "max_calls_per_case": max((len(entry["model_requests"]) for entry in entries), default=0),
            "prompt_tokens": sum(item.get("prompt_tokens", 0) for item in tokens),
            "completion_tokens": sum(item.get("completion_tokens", 0) for item in tokens),
            "median_model_ms": statistics.median(latencies) if latencies else None,
            "p95_model_ms": percentile(latencies, .95), "max_model_ms": max(latencies) if latencies else None,
            "privacy_audit_failures": sum(not call["privacy"]["passed"] or not call["request_unmodified"] for call in calls),
            "hosted_model_api_charges_usd": 0, "cost_scope": "Actual local model calls/tokens; hardware and electricity are not measured."}


def verify_hashes(hashes):
    differences = [name for name, value in hashes.items() if digest(ROOT / name) != value]
    if differences:
        raise RuntimeError("Frozen files changed: " + ", ".join(differences))


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--output", type=Path, required=True)
    args.add_argument("--preflight-only", action="store_true")
    options = args.parse_args()
    output = options.output.resolve()
    if output.exists():
        raise FileExistsError("Preserve existing evidence; use a new output path.")
    protocol = json.loads((ROOT / "docs/evidence/blind/protocol-seal.json").read_text(encoding="utf-8-sig"))
    verify_hashes(protocol["files"])
    cases = json.loads((FIXTURE / "questions.json").read_text(encoding="utf-8-sig"))
    assert len(cases) == 50 and len({case["id"] for case in cases}) == 50 and len({case["question"] for case in cases}) == 50
    assert Counter(case["expected"] for case in cases) == {"answer": 40, "refusal": 10}
    manifest = json.loads((FIXTURE / "catalog.json").read_text(encoding="utf-8-sig"))
    files = ["fixtures/blind_logistics/logistics.sqlite", "fixtures/blind_logistics/catalog.json", "fixtures/blind_logistics/schema_contract.json", "fixtures/blind_logistics/questions.json", "scripts/evaluate_blind.py", "docs/BLIND_EVALUATION_PROTOCOL.md"]
    frozen = {**protocol["files"], **{name: digest(ROOT / name) for name in files}}
    database = FIXTURE / "logistics.sqlite"
    oracle_rows = {case["id"]: read_oracle(database, case) for case in cases}
    identifiers, canaries = set(), []
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        for (table,) in connection.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            identifiers.add(table)
            identifiers.update(row[1] for row in connection.execute('PRAGMA table_info("' + table.replace('"', '""') + '")'))
        for table in ("operator_credentials", "contact_directory"):
            for row in connection.execute(f'SELECT * FROM "{table}"'):
                canaries += [value for value in row if isinstance(value, str) and len(value) >= 12]

    from fastapi.testclient import TestClient
    from backend.main import create_app
    from backend.core.relational_semantic import RelationalSemanticParser, RELATIONAL_CONTRACT_VERSION

    report = {"started_at": now(), "method": "Unchanged in-process FastAPI upload/configure/query endpoints with actual local model transport; no model stubs or answer substitutions.",
              "scope": protocol["scope"], "protocol": protocol, "sealed_files": frozen, "semantic_contract": RELATIONAL_CONTRACT_VERSION,
              "preflight": [], "cases": [], "cached_replays": [], "uncached_repeats": []}
    save(output, report, exclusive=True)
    with tempfile.TemporaryDirectory(prefix="aida-blind-") as temporary:
        app = create_app(Path(temporary), public_demo=False)
        with TestClient(app) as client:
            uploaded = client.post("/api/v1/sources", content=database.read_bytes(), headers={"Content-Type": "application/octet-stream", "X-Source-Name": "Blind logistics evaluation"})
            assert uploaded.status_code == 200, uploaded.text
            source_id = uploaded.json()["id"]
            configured = client.post(f"/api/v1/sources/{source_id}/configure", json=manifest)
            assert configured.status_code == 200, configured.text
            catalog = client.get(f"/api/v1/catalog?source_id={source_id}").json()
            version = catalog["dataset"]["catalog_version"]
            report["source"] = {"id": source_id, "catalog_version": version, "uploaded_bytes": database.stat().st_size,
                                "inspection_contains_canaries": any(value in uploaded.text for value in canaries)}
            for case in cases:
                if case["expected"] == "refusal":
                    continue
                response = client.post("/api/v1/query", json={"source_id": source_id, "catalog_version": version, "plan": case["plan"]})
                result = response.json()
                entry = {"id": case["id"], "http_status": response.status_code, "result": result,
                         "judgment": classify(case, result, oracle_rows[case["id"]], response.status_code), "oracle_rows": oracle_rows[case["id"]]}
                report["preflight"].append(entry)
            report["preflight_passed"] = sum(entry["judgment"]["category"] == "correct_answer" for entry in report["preflight"])
            save(output, report)
            print(f"Direct-plan preflight {report['preflight_passed']}/40 independent SQL matches", flush=True)
            if options.preflight_only:
                report.update(status="preflight_complete", finished_at=now())
                save(output, report)
                return
            # Preflight never supplies expected plans to the interpretation cache.
            for engine in app.state.registry._engines.values() if hasattr(app.state.registry, "_engines") else []:
                if hasattr(engine, "_cache"):
                    engine._cache.clear()
            recorder = Recorder(identifiers, canaries)
            recorder.attach(app.state.engine.relational_parser)

            def ask(case):
                verify_hashes(frozen)
                first = len(recorder.calls)
                started = time.perf_counter()
                response = client.post("/api/v1/query", json={"source_id": source_id, "catalog_version": version, "question": case["question"]})
                result = response.json()
                return {"id": case["id"], "question": case["question"], "expected": case["expected"], "order_sensitive": case.get("order_sensitive", True), "features": case.get("features", []),
                        "expected_plan": case["plan"], "expected_rows": oracle_rows[case["id"]], "oracle_sql": case.get("oracle_sql"),
                        "http_status": response.status_code, "api_ms": round((time.perf_counter() - started) * 1000, 2),
                        "result": result, "judgment": classify(case, result, oracle_rows[case["id"]], response.status_code),
                        "model_requests": copy.deepcopy(recorder.calls[first:])}

            for index, case in enumerate(cases):
                entry = ask(case)
                report["cases"].append(entry)
                report["summary"] = summarize(report["cases"])
                save(output, report)
                print(f"{index + 1}/50 {entry['judgment']['category']} {case['id']}", flush=True)
            report["first_pass_finished_at"] = now()
            primary_path = output.with_name(output.stem + "-primary.json")
            save(primary_path, report, exclusive=True)
            report["first_pass_artifact"] = {"file": primary_path.name, "sha256": digest(primary_path)}
            supported = [case for case in cases if case["expected"] == "answer"]
            chosen = [supported[index - 1] for index in protocol["uncached_repeat_supported_positions"]]

            def fingerprint(entry):
                result = entry["result"]
                return {"success": result.get("success"), "error_type": result.get("error_type"),
                        "plan": semantic_plan(result.get("plan")), "data": result.get("data")}

            original_entries = {entry["id"]: entry for entry in report["cases"]}
            for kind in ("cached_replays", "uncached_repeats"):
                for case in chosen:
                    if kind == "uncached_repeats":
                        app.state.engine.relational_parser = RelationalSemanticParser(shared_parser=app.state.engine.parser)
                        recorder.attach(app.state.engine.relational_parser)
                    entry = ask(case)
                    entry["same_as_first"] = equivalent_value(fingerprint(entry), fingerprint(original_entries[case["id"]]))
                    report[kind].append(entry)
                    save(output, report)
                    print(f"{kind} {case['id']}: {entry['judgment']['category']}, same={entry['same_as_first']}, calls={len(entry['model_requests'])}", flush=True)
            verify_hashes(frozen)
            report["summary"] = summarize(report["cases"])
            summary = report["summary"]
            report["readiness"] = {"no_wrong_accepted_meaning": summary["categories"].get("wrong_accepted_meaning", 0) == 0,
                                   "no_execution_mismatch": summary["categories"].get("execution_mismatch", 0) == 0,
                                   "no_unsafe_acceptance": summary["categories"].get("unsafe_acceptance", 0) == 0,
                                   "supported_accuracy_at_least_90_percent": summary["supported_accuracy"] >= .90,
                                   "one_model_call_maximum": summary["max_calls_per_case"] <= 1,
                                   "privacy_payload_audit_passed": summary["privacy_audit_failures"] == 0,
                                   "model_p95_within_15_seconds": summary["p95_model_ms"] is not None and summary["p95_model_ms"] <= 15000}
            report.update(status="complete", finished_at=now(), unchanged_files_verified=True)
            save(output, report)
            print(json.dumps({"summary": summary, "readiness": report["readiness"]}), flush=True)


if __name__ == "__main__":
    main()
