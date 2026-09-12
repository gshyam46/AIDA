"""Derive compact review evidence without changing the frozen blind assessment."""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

from evaluate_blind import digest, equivalent_value, percentile, semantic_plan, summarize, verify_hashes


def timings(values):
    return {"n": len(values), "median_ms": statistics.median(values) if values else None,
            "p95_ms": percentile(values, .95), "max_ms": max(values) if values else None}


def phase(entries):
    calls = [call for entry in entries for call in entry["model_requests"]]
    result = summarize(entries)
    result.update(api=timings([entry["api_ms"] for entry in entries]),
                  transport=timings([call["transport_ms"] for call in calls]),
                  successful_database=timings([entry["result"]["meta"]["database_time_ms"] for entry in entries
                                               if entry["result"].get("success")]),
                  zero_model_call_cases=[entry["id"] for entry in entries if not entry["model_requests"]])
    if entries and "same_as_first" in entries[0]:
        result["same_as_first"] = sum(entry["same_as_first"] for entry in entries)
        result["changed_cases"] = [entry["id"] for entry in entries if not entry["same_as_first"]]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Keep prior review evidence; use a new output path.")
    report = json.loads(args.input.read_text(encoding="utf-8"))
    assert report["status"] == "complete" and report["unchanged_files_verified"]
    verify_hashes(report["sealed_files"])
    primary = args.input.with_name(report["first_pass_artifact"]["file"])
    assert digest(primary) == report["first_pass_artifact"]["sha256"]
    assert json.loads(primary.read_text(encoding="utf-8"))["cases"] == report["cases"]
    phases = {name: phase(report[name]) for name in ("cases", "cached_replays", "uncached_repeats")}
    details = []
    for entry in report["cases"]:
        actual, expected = entry["result"].get("plan"), entry["expected_plan"]
        normalized_actual = semantic_plan(actual, not entry["order_sensitive"])
        normalized_expected = semantic_plan(expected, not entry["order_sensitive"])
        changes = ({key: {"expected": normalized_expected.get(key), "actual": normalized_actual.get(key)}
                    for key in sorted(normalized_expected.keys() | normalized_actual.keys())
                    if not equivalent_value(normalized_expected.get(key), normalized_actual.get(key))}
                   if actual and expected else {})
        details.append({"id": entry["id"], "question": entry["question"], "expected": entry["expected"],
                        "category": entry["judgment"]["category"], "error": entry["result"].get("error"),
                        "semantic_differences": changes, "lineage": entry["judgment"]["lineage"],
                        "actual_rows": len(entry["result"].get("data", [])),
                        "expected_rows": len(entry["expected_rows"]) if entry["expected_rows"] is not None else None})
    all_calls = [call for name in phases for entry in report[name] for call in entry["model_requests"]]
    derived = {"source": str(args.input), "source_sha256": digest(args.input),
               "first_pass_sha256": digest(primary), "frozen_files_verified": True,
               "preflight": {"correct": report["preflight_passed"], "total": len(report["preflight"]),
                             "lineage_passes": sum(entry["judgment"]["lineage"]["passed"] for entry in report["preflight"]),
                             "database": timings([entry["result"]["meta"]["database_time_ms"] for entry in report["preflight"]])},
               "phases": phases, "readiness": report["readiness"],
               "all_phases": {"actual_model_calls": len(all_calls),
                              "destinations": sorted({call["endpoint"] for call in all_calls}),
                              "privacy_audit_failures": sum(not call["privacy"]["passed"] for call in all_calls),
                              "modified_requests": sum(not call["request_unmodified"] for call in all_calls),
                              "canaries_per_request": sorted({call["privacy"]["canaries_checked"] for call in all_calls}),
                              "prompt_tokens": sum(call.get("response", {}).get("usage", {}).get("prompt_tokens", 0) for call in all_calls),
                              "completion_tokens": sum(call.get("response", {}).get("usage", {}).get("completion_tokens", 0) for call in all_calls)},
               "cases": details}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(derived, handle, indent=2)
    print(json.dumps({key: value for key, value in derived.items() if key != "cases"}, indent=2))


if __name__ == "__main__":
    main()
