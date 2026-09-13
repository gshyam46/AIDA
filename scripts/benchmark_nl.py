"""Natural-language benchmark with real inference and independent SQL oracles.

Suites: the logistics transfer questions (now a regression set, not blind),
relational demo and Chinook cases, single-table semantic cases, calculated
measures and name resolution. Questions run through HybridAnalytics, the same
code path as POST /api/v1/query. Expected plans and oracle rows stay in this
process and never enter model requests.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import statistics
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from backend.core.settings import apply_env_file  # noqa: E402

apply_env_file()

from backend.core.analytics import AnalyticsEngine  # noqa: E402
from backend.core.chinook import chinook_cases, chinook_source  # noqa: E402
from backend.core.hybrid import HybridAnalytics  # noqa: E402
from backend.core.interpreter import PROMPT_VERSION, Interpreter  # noqa: E402
from backend.core.llm import GROQ_PRICES, SemanticConfig  # noqa: E402
from backend.core.relational_demo import ensure_relational_demos, relational_demo_cases  # noqa: E402
from backend.core.sources import SourceRegistry  # noqa: E402
import benchmark_cases as bench  # noqa: E402
import evaluate_blind as blind  # noqa: E402

SUITES = ("blind", "relational", "semantic", "capabilities", "resolution")
# Stratified model-selection subset: every earlier failure class plus controls from each suite.
SELECTION = [
    *(f"logistics:BL{number:02d}" for number in (1, 6, 7, 8, 12, 13, 16, 17, 20, 24, 25, 28, 30, 32, 33, 35, 38, 42, 45, 46, 48, 49)),
    "relational:chinook_five_tables", "relational:chinook_physical_price", "relational:warehouse_having", "relational:warehouse_union_distinct",
    "relational:billing_exists", "relational:refusal_6",
    "semantic-acceptance:4", "semantic-acceptance:8", "semantic-regression:5", "semantic-regression:10", "semantic-challenge:3",
    "semantic-regression:22", "semantic-regression:29", "semantic-challenge:18",
    "capabilities:commerce_share_by_region", "capabilities:warehouse_revenue_per_unit", "capabilities:commerce_cumulative_revenue",
    "resolution:1", "resolution:4", "resolution:6",
]
_RETRY = re.compile(r"try again in about (\d+) seconds", re.I)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_registry(workspace: Path, with_logistics: bool) -> tuple[SourceRegistry, str | None]:
    seed = AnalyticsEngine(workspace / "aida-demo.sqlite")
    seed.ensure_demo_data()
    registry = SourceRegistry(workspace)
    registry.register_commerce_demo(seed.database_path)
    registry.register_support_demo()
    for source in [*ensure_relational_demos(registry.directory), chinook_source()]:
        registry.register_source(source["path"], source["manifest"], source["id"])
    logistics_id = None
    if with_logistics:
        fixture = ROOT / "fixtures" / "blind_logistics"
        logistics_id = registry.inspect_upload((fixture / "logistics.sqlite").read_bytes(), "Logistics benchmark")["id"]
        registry.configure(logistics_id, json.loads((fixture / "catalog.json").read_text(encoding="utf-8-sig")))
    return registry, logistics_id


def build_cases(suites: set[str], logistics_id: str | None) -> list[dict]:
    cases = []
    if "blind" in suites:
        overrides = bench.logistics_capability_overrides()
        for item in json.loads((ROOT / "fixtures/blind_logistics/questions.json").read_text(encoding="utf-8-sig")):
            case = {"key": f"logistics:{item['id']}", "suite": "logistics", "source_id": logistics_id, "question": item["question"],
                    "expected": item["expected"], "plan": item.get("plan"), "order_sensitive": item.get("order_sensitive", True), "oracle": ("blind", item)}
            if item["id"] in overrides:
                override = overrides[item["id"]]
                case.update(expected="answer", plan=None, compare="calculation", groups=override["groups"], order_sensitive=override["ordered"],
                            oracle=("calculation", override["oracle"]), gold_note="The original refusal predates calculation support; scored against independently calculated values.")
            cases.append(case)
    if "relational" in suites:
        for item in [*chinook_cases(), *relational_demo_cases()]:
            cases.append({"key": f"relational:{item['id']}", "suite": "relational", "source_id": item["source_id"], "question": item["question"],
                          "expected": "answer", "plan": item["plan"], "order_sensitive": True, "oracle": ("sql", item["oracle_sql"])})
        for index, question in enumerate(bench.RELATIONAL_REFUSALS, 1):
            cases.append({"key": f"relational:refusal_{index}", "suite": "relational", "source_id": "chinook", "question": question,
                          "expected": "refusal", "plan": None, "order_sensitive": True, "oracle": None})
    if "semantic" in suites:
        seen = set()
        for group, items in (("acceptance", bench.SEMANTIC_ACCEPTANCE), ("regression", bench.SEMANTIC_REGRESSION), ("challenge", bench.SEMANTIC_CHALLENGE)):
            for index, (source_id, question, expected) in enumerate(items, 1):
                if (source_id, question) in seen:
                    continue
                seen.add((source_id, question))
                cases.append({"key": f"semantic-{group}:{index}", "suite": "semantic", "source_id": source_id, "question": question,
                              "expected": "refusal" if expected is None else "answer", "plan": expected, "order_sensitive": True, "oracle": ("semantic", expected)})
    if "capabilities" in suites:
        for item in bench.capability_cases():
            cases.append({"key": f"capabilities:{item['id']}", "suite": "capabilities", "source_id": item["source_id"], "question": item["question"],
                          "expected": "answer", "plan": None, "compare": "calculation", "groups": item["groups"], "order_sensitive": item["ordered"],
                          "oracle": ("calculation", item["oracle"])})
    if "resolution" in suites:
        for index, (source_id, question, expected) in enumerate(bench.NAME_RESOLUTION, 1):
            cases.append({"key": f"resolution:{index}", "suite": "resolution", "source_id": source_id, "question": question,
                          "expected": "refusal" if expected is None else "answer", "plan": expected, "order_sensitive": True, "oracle": ("semantic", expected)})
    return cases


def oracle_rows(case: dict, registry: SourceRegistry) -> list[dict] | None:
    if case["expected"] == "refusal":
        return None
    kind, value = case["oracle"]
    engine = registry.engine(case["source_id"])
    if kind == "blind":
        return blind.read_oracle(engine.database_path, value)
    if kind == "sql":
        return blind.read_oracle(engine.database_path, {"expected": "answer", "oracle_sql": value})
    if kind == "calculation":
        return value(engine.database_path)
    return bench.independent_rows(engine, value, case["source_id"])[0]


def plan_matches(actual: object, expected: object, ordered: bool) -> bool:
    if not isinstance(actual, dict) or not isinstance(expected, dict):
        return False
    actual = {key: value for key, value in actual.items() if not (key in ("calculations", "post") and not value)}
    if expected.get("version") == 2:
        return blind.semantic_plan(actual, not ordered) == blind.semantic_plan(expected, not ordered)
    return actual == expected


def judge(case: dict, result: dict, rows: list[dict] | None) -> dict:
    ordered = case["order_sensitive"]
    if case.get("compare") == "calculation":
        rows_match = bool(result.get("success")) and bench.calculation_rows_match(result.get("data"), rows or [], case["groups"], ordered)
        plan_match = rows_match
    else:
        rows_match = bool(result.get("success")) and rows is not None and blind.equal_rows(result.get("data"), rows, ordered)
        plan_match = bool(result.get("success")) and plan_matches(result.get("plan"), case["plan"], ordered)
    if result.get("error_type") == "model_unavailable":
        category = "availability_failure"
    elif case["expected"] == "refusal":
        category = "unsafe_acceptance" if result.get("success") else "correct_refusal"
    elif result.get("success"):
        category = "correct_answer" if plan_match and rows_match else "wrong_accepted_meaning" if not plan_match else "execution_mismatch"
    elif result.get("error_type") == "clarification_required":
        category = "false_refusal"
    else:
        category = "execution_failure"
    return {"category": category, "rows_match": rows_match, "plan_match": plan_match}


def ask(hybrid: HybridAnalytics, case: dict, patience: int) -> tuple[dict, float, str | None]:
    waited = 0.0
    result: dict = {}
    for _ in range(8):
        result = hybrid.query(question=case["question"], source_id=case["source_id"])
        message = result.get("error") or ""
        if result.get("error_type") == "model_unavailable" and "rate limit" in message.lower():
            match = _RETRY.search(message)
            delay = int(match[1]) if match else 60
            if delay > patience:
                return result, waited, "quota_exhausted"
            time.sleep(delay + 1)
            waited += delay + 1
            continue
        return result, waited, None
    return result, waited, "rate_limited"


def percentile(values: list[float], fraction: float) -> float | None:
    return blind.percentile(sorted(values), fraction) if values else None


def summarize(entries: list[dict], model: str) -> dict:
    counts = Counter(entry["category"] for entry in entries)
    answers = [entry for entry in entries if entry["expected"] == "answer"]
    refusals = [entry for entry in entries if entry["expected"] == "refusal"]
    accepted = [entry for entry in entries if entry["success"]]
    telemetry = [entry["telemetry"] for entry in entries]
    latencies = [item["model_latency_ms"] for item in telemetry if item.get("model_calls") and item.get("model_latency_ms")]
    api_ms = [entry["wall_ms"] - entry["rate_limit_wait_seconds"] * 1000 for entry in entries]
    prompt = sum(item.get("prompt_tokens") or 0 for item in telemetry)
    completion = sum(item.get("completion_tokens") or 0 for item in telemetry)
    reported = [item.get("estimated_model_cost_usd") for item in telemetry if item.get("estimated_model_cost_usd") is not None]
    price = GROQ_PRICES.get(model)
    ratio = lambda part, whole: round(part / whole, 4) if whole else None  # noqa: E731
    return {
        "cases": len(entries), "categories": dict(counts),
        "supported_cases": len(answers), "correct_supported": counts["correct_answer"], "supported_accuracy": ratio(counts["correct_answer"], len(answers)),
        "supported_rows_correct": sum(entry["rows_match"] for entry in answers),
        "refusal_cases": len(refusals), "correct_refusals": counts["correct_refusal"], "refusal_accuracy": ratio(counts["correct_refusal"], len(refusals)),
        "overall_correct": counts["correct_answer"] + counts["correct_refusal"], "overall_accuracy": ratio(counts["correct_answer"] + counts["correct_refusal"], len(entries)),
        "accepted_answers": len(accepted), "accepted_precision": ratio(counts["correct_answer"], len(accepted)),
        "wrong_or_unsafe_answers": counts["wrong_accepted_meaning"] + counts["unsafe_acceptance"] + counts["execution_mismatch"],
        "false_refusals": counts["false_refusal"], "availability_failures": counts["availability_failure"],
        "model_calls": sum(item.get("model_calls") or 0 for item in telemetry),
        "guard_calls": sum(item.get("guard_calls") or 0 for item in telemetry),
        "repair_calls": sum(item.get("repair_calls") or 0 for item in telemetry),
        "max_model_calls_per_question": max((item.get("model_calls") or 0 for item in telemetry), default=0),
        "prompt_tokens": prompt, "completion_tokens": completion,
        "tokens_per_question": round((prompt + completion) / len(entries), 1) if entries else None,
        "median_model_latency_ms": round(statistics.median(latencies), 1) if latencies else None,
        "p95_model_latency_ms": percentile(latencies, .95),
        "median_answer_ms_excluding_rate_limit_waits": round(statistics.median(api_ms), 1) if api_ms else None,
        "p95_answer_ms_excluding_rate_limit_waits": percentile(api_ms, .95),
        "estimated_cost_usd": round(sum(reported), 6) if reported else (round(prompt / 1e6 * price[0] + completion / 1e6 * price[1], 6) if price else None),
    }


def write(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True, help="Name for this run, e.g. aida4-two_stage-gpt-oss-120b.")
    parser.add_argument("--suites", default=",".join(SUITES))
    parser.add_argument("--subset", choices=["all", "selection"], default="all")
    parser.add_argument("--model", help="Override AIDA_GROQ_MODEL for this run.")
    parser.add_argument("--pipeline", choices=["two_stage", "single"])
    parser.add_argument("--repair", type=int, choices=[0, 1])
    parser.add_argument("--no-guard", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--cases", nargs="+", help="Run only these case keys.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--patience", type=int, default=240, help="Longest single rate-limit wait before stopping the run.")
    args = parser.parse_args()
    for flag, variable, value in ((args.model, "AIDA_GROQ_MODEL", args.model), (args.pipeline, "AIDA_PIPELINE", args.pipeline),
                                  (args.repair is not None, "AIDA_REPAIR_ATTEMPTS", str(args.repair)), (args.no_guard, "AIDA_GUARD_MODEL", "")):
        if flag:
            os.environ[variable] = value
    os.environ.setdefault("AIDA_GROQ_MAX_WAIT_SECONDS", "60")
    output = args.output or ROOT / "artifacts" / "benchmark" / f"{args.label}.json"
    suites = {item.strip() for item in args.suites.split(",") if item.strip()}
    config = SemanticConfig.from_env()
    with tempfile.TemporaryDirectory(prefix="aida-benchmark-") as directory:
        registry, logistics_id = build_registry(Path(directory), "blind" in suites)
        cases = build_cases(suites, logistics_id)
        wanted = set(args.cases or []) | (set(SELECTION) if args.subset == "selection" else set())
        if wanted:
            cases = [case for case in cases if case["key"] in wanted]
        cases = cases[:args.limit] if args.limit else cases
        hybrid = HybridAnalytics(registry, Interpreter(config))
        status = hybrid.interpreter.status()
        if not status.get("available"):
            print(json.dumps({"error": "Model unavailable", "status": status}))
            return 2
        previous = json.loads(output.read_text(encoding="utf-8")) if args.resume and output.exists() else None
        report = previous or {"label": args.label, "started_at": now(), "provider": config.provider, "model": config.model, "pipeline": config.pipeline,
                              "repair_attempts": config.repair_attempts, "guard_model": config.guard_model, "prompt_version": PROMPT_VERSION,
                              "subset": args.subset, "python": platform.python_version(),
                              "method": "HybridAnalytics.query per question with real inference; independent SQL oracles; provider rate-limit rejections are waited out and excluded from answer latency.",
                              "entries": []}
        # Cases the provider could not serve are retried on resume rather than kept as results.
        report["entries"] = [entry for entry in report["entries"] if entry["category"] != "availability_failure"]
        done = {entry["key"] for entry in report["entries"]}
        stopped = None
        for index, case in enumerate(cases, 1):
            if case["key"] in done:
                continue
            rows = oracle_rows(case, registry)
            started = time.perf_counter()
            result, waited, stopped = ask(hybrid, case, args.patience)
            if stopped:
                print(f"Stopped ({stopped}) at {case['key']}: {result.get('error')}", flush=True)
                break
            judgment = judge(case, result, rows)
            meta = result.get("meta", {})
            report["entries"].append({
                "key": case["key"], "suite": case["suite"], "source_id": "logistics" if case["suite"] == "logistics" else case["source_id"],
                "question": case["question"], "expected": case["expected"], **judgment, "success": bool(result.get("success")),
                "error_type": result.get("error_type"), "error": (result.get("error") or "")[:300] or None,
                "clarification_reason": result.get("clarification_reason"), "suggestions": result.get("suggestions"),
                "actual_plan": result.get("plan"), "expected_plan": case["plan"], "gold_note": case.get("gold_note"),
                "notes": (result.get("interpretation") or {}).get("notes"),
                "row_count": len(result.get("data") or []), "rows_sha256": hashlib.sha256(json.dumps(result.get("data"), sort_keys=True, default=str).encode()).hexdigest(),
                "telemetry": {key: meta.get(key) for key in ("model_calls", "guard_calls", "repair_calls", "prompt_tokens", "completion_tokens", "model_latency_ms",
                                                              "guard_latency_ms", "rate_limit_wait_seconds", "elapsed_ms", "interpretation_source", "model", "pipeline_stages", "estimated_model_cost_usd", "rejections")},
                "model_stages": (result.get("semantic_ir") or {}).get("stages"),
                "rate_limit_wait_seconds": waited + (meta.get("rate_limit_wait_seconds") or 0), "wall_ms": round((time.perf_counter() - started) * 1000, 1)})
            report["summary"] = summarize(report["entries"], config.model)
            write(output, report)
            print(f"{index}/{len(cases)} {judgment['category']:<22} {case['key']}", flush=True)
        report["summary"] = summarize(report["entries"], config.model)
        report["by_suite"] = {suite: summarize([entry for entry in report["entries"] if entry["suite"] == suite], config.model)
                              for suite in sorted({entry["suite"] for entry in report["entries"]})}
        report.update(status="stopped_quota" if stopped else "complete", finished_at=now())
        write(output, report)
        print(json.dumps({"summary": report["summary"], "by_suite": {name: {key: value[key] for key in ("supported_accuracy", "refusal_accuracy", "overall_accuracy", "wrong_or_unsafe_answers")}
                                                                     for name, value in report["by_suite"].items()}}, indent=1), flush=True)
        return 3 if stopped else 0


if __name__ == "__main__":
    raise SystemExit(main())
