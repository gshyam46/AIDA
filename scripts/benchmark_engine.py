"""Engineering benchmark without model calls.

1. Deterministic engine: every benchmark case that has an expected plan is executed directly, with result
   caches cleared, and compared with its independent oracle rows.
2. Replay: recorded model outputs from AIDA 4 runs are fed back through the current validation, SQL and
   calculation code; rows and plans are compared with what the run recorded.
3. Quality gates and configured limits: backend test results, the latest browser journey and rate limits.

Writes artifacts/benchmark/engine.json for scripts/report_benchmark.py --engine.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import benchmark_nl as nl  # noqa: E402  (shares case building and oracles; no model is called here)
from backend.core.hybrid import HybridAnalytics  # noqa: E402
from backend.core.interpreter import Interpreter  # noqa: E402
from backend.core.llm import ModelReply, SemanticConfig  # noqa: E402
from backend.core.security import LIMITS, MISUSE_BLOCK_SECONDS, MISUSE_STRIKES, MISUSE_WINDOW_SECONDS  # noqa: E402

BENCHMARKS = ROOT / "artifacts" / "benchmark"


def stats(values: list[float | None]) -> dict:
    present = [value for value in values if value is not None]
    if not present:
        return {"n": 0, "p50": None, "p95": None}
    return {"n": len(present), "p50": round(statistics.median(present), 2), "p95": round(nl.percentile(present, 0.95), 2)}


def clear_result_cache(registry, source_id: str) -> None:
    cache = getattr(registry.engine(source_id), "_cache", None)
    if cache is not None:
        cache.clear()


def engine_benchmark(registry, logistics_id: str, repeats: int) -> dict:
    hybrid = HybridAnalytics(registry, object())
    cases = [case for case in nl.build_cases(set(nl.SUITES), logistics_id) if case["expected"] == "answer" and isinstance(case.get("plan"), dict)]
    results = []
    for case in cases:
        rows = nl.oracle_rows(case, registry)
        timings, database, result = [], [], {}
        for _ in range(repeats):
            clear_result_cache(registry, case["source_id"])
            started = time.perf_counter()
            result = hybrid.query(plan=copy.deepcopy(case["plan"]), source_id=case["source_id"])
            timings.append((time.perf_counter() - started) * 1000)
            database.append(result.get("meta", {}).get("database_time_ms"))
        started = time.perf_counter()
        cached = hybrid.query(plan=copy.deepcopy(case["plan"]), source_id=case["source_id"])
        cached_ms = (time.perf_counter() - started) * 1000
        judgment = nl.judge(case, result, rows)
        results.append({"key": case["key"], "suite": case["suite"], "success": bool(result.get("success")), "rows_match": bool(judgment["rows_match"]),
                        "row_count": len(result.get("data") or []), "uncached_ms": round(statistics.median(timings), 2),
                        "database_ms": stats(database)["p50"], "cached_ms": round(cached_ms, 2), "cache_hit": bool(cached.get("meta", {}).get("cache_hit")),
                        "error": None if result.get("success") else (result.get("error") or "")[:200]})

    def summarize(items: list[dict]) -> dict:
        return {"plans": len(items), "succeeded": sum(item["success"] for item in items), "rows_match": sum(item["rows_match"] for item in items),
                "uncached_ms": stats([item["uncached_ms"] for item in items]), "database_ms": stats([item["database_ms"] for item in items]),
                "cached_ms": stats([item["cached_ms"] for item in items]), "cache_hits": sum(item["cache_hit"] for item in items)}

    return {"repeats": repeats, "summary": summarize(results),
            "by_suite": {suite: summarize([item for item in results if item["suite"] == suite]) for suite in sorted({item["suite"] for item in results})},
            "cases": results}


class ReplayClient:
    """Returns a run's recorded model outputs in order, so only AIDA's own code runs."""

    def __init__(self, stages: list[dict]):
        self.outputs = [copy.deepcopy(stage["output"]) for stage in stages]
        self.gate = threading.BoundedSemaphore(1)

    def status(self) -> dict:
        return {"available": True}

    def guard(self, text: str):
        return None, 0.0

    def complete(self, messages, *, max_tokens, schema=None) -> ModelReply:
        if not self.outputs:
            raise RuntimeError("the recording has no further model output")
        content = self.outputs.pop(0)
        return ModelReply(content, json.dumps(content), 0, 0, 0.0, "replay", 0.0)


def replay_benchmark(registry, logistics_id: str, labels: list[str]) -> dict:
    runs = {}
    for label in labels:
        data = json.loads((BENCHMARKS / f"{label}.json").read_text(encoding="utf-8"))
        if not data.get("prompt_version"):
            continue
        config = SemanticConfig(pipeline=data.get("pipeline") or "two_stage", repair_attempts=int(data.get("repair_attempts") or 0))
        items = []
        for entry in data["entries"]:
            if not entry.get("success") or not entry.get("model_stages"):
                continue
            source_id = logistics_id if entry["source_id"] == "logistics" else entry["source_id"]
            hybrid = HybridAnalytics(registry, Interpreter(config, client=ReplayClient(entry["model_stages"])))
            clear_result_cache(registry, source_id)
            try:
                started = time.perf_counter()
                result = hybrid.query(question=entry["question"], source_id=source_id)
                code_ms = (time.perf_counter() - started) * 1000
                started = time.perf_counter()
                repeat = hybrid.query(question=entry["question"], source_id=source_id)
                repeat_ms = (time.perf_counter() - started) * 1000
            except Exception as error:  # a recording that no longer fits the code is a finding, not a crash
                items.append({"key": entry["key"], "success": False, "rows_identical": False, "plan_identical": False, "code_ms": None, "database_ms": None,
                              "repeat_ms": None, "repeat_model_calls": None, "error": f"{type(error).__name__}: {error}"[:200]})
                continue
            digest = hashlib.sha256(json.dumps(result.get("data"), sort_keys=True, default=str).encode()).hexdigest()
            plan = json.loads(json.dumps(result.get("plan"), default=str))
            items.append({"key": entry["key"], "success": bool(result.get("success")), "rows_identical": digest == entry.get("rows_sha256"),
                          "plan_identical": plan == entry.get("actual_plan"), "code_ms": round(code_ms, 2), "database_ms": result.get("meta", {}).get("database_time_ms"),
                          "repeat_ms": round(repeat_ms, 2), "repeat_model_calls": repeat.get("meta", {}).get("model_calls"),
                          "error": None if result.get("success") else (result.get("error") or "")[:200]})
        runs[label] = {"model": data.get("model"), "replayed": len(items), "succeeded": sum(item["success"] for item in items),
                       "rows_identical": sum(item["rows_identical"] for item in items), "plans_identical": sum(item["plan_identical"] for item in items),
                       "code_ms": stats([item["code_ms"] for item in items]), "database_ms": stats([item["database_ms"] for item in items]),
                       "repeat_ms": stats([item["repeat_ms"] for item in items]), "repeat_without_model_calls": sum(item["repeat_model_calls"] == 0 for item in items),
                       "items": items}
    return runs


def quality_gates() -> dict:
    python = sys.executable
    tests = subprocess.run([python, "-m", "pytest", "backend", "-q", "-p", "no:cacheprovider"], cwd=ROOT, capture_output=True, text=True, timeout=1800)
    tail = next((line for line in reversed(tests.stdout.strip().splitlines()) if re.search(r"\d+ (passed|failed)", line)), "")
    counts = {("errors" if kind.startswith("error") else kind): int(number) for number, kind in re.findall(r"(\d+) (passed|failed|errors?|skipped)", tail)}
    collected = subprocess.run([python, "-m", "pytest", "backend/test_security.py", "backend/test_interpreter.py", "--collect-only", "-q", "-p", "no:cacheprovider"],
                               cwd=ROOT, capture_output=True, text=True, timeout=600)
    per_file: dict[str, int] = {}
    for line in collected.stdout.splitlines():
        if "::" in line:
            name = re.split(r"[\\/]", line.split("::")[0])[-1]
            per_file[name] = per_file.get(name, 0) + 1
    journey = None
    report_path = ROOT / "artifacts" / "e2e-auth" / "report.json"
    if report_path.exists():
        report = json.loads(report_path.read_text(encoding="utf-8"))
        steps = report.get("steps", [])
        journey = {"status": report.get("status"), "steps": len(steps), "passed": sum(step.get("status") == "passed" for step in steps), "finished_at": report.get("finished_at")}
    return {"backend_tests": {"exit_code": tests.returncode, "summary": tail.strip("= "), **counts},
            "security_tests": per_file.get("test_security.py"), "interpreter_tests": per_file.get("test_interpreter.py"), "browser_journey": journey}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", nargs="*", default=[], help="Recorded AIDA 4 run labels to replay (artifacts/benchmark/<label>.json).")
    parser.add_argument("--repeats", type=int, default=5, help="Uncached executions per plan.")
    parser.add_argument("--skip-tests", action="store_true", help="Do not run the backend test suite.")
    parser.add_argument("--output", type=Path, default=BENCHMARKS / "engine.json")
    args = parser.parse_args()
    report = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "method": "No model calls. Plans run through HybridAnalytics.query with result caches cleared; replays feed recorded model outputs to the current interpreter.",
              "environment": {"python": platform.python_version(), "system": platform.system(), "cpu_count": os.cpu_count()}}
    with tempfile.TemporaryDirectory(prefix="aida-engine-") as directory:
        registry, logistics_id = nl.build_registry(Path(directory), True)
        report["engine"] = engine_benchmark(registry, logistics_id, args.repeats)
        report["replay"] = replay_benchmark(registry, logistics_id, args.runs)
    report["quality"] = None if args.skip_tests else quality_gates()
    report["limits"] = {bucket: {"requests": count, "window_seconds": window} for bucket, (count, window) in LIMITS.items()}
    report["misuse"] = {"strikes": MISUSE_STRIKES, "window_seconds": MISUSE_WINDOW_SECONDS, "pause_seconds": MISUSE_BLOCK_SECONDS}
    nl.write(args.output, report)
    engine = report["engine"]["summary"]
    print(json.dumps({"engine": {key: engine[key] for key in ("plans", "rows_match", "uncached_ms", "database_ms", "cached_ms", "cache_hits")},
                      "replay": {label: {key: value[key] for key in ("replayed", "rows_identical", "plans_identical", "code_ms", "repeat_ms", "repeat_without_model_calls")}
                                 for label, value in report["replay"].items()},
                      "quality": report["quality"]}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
