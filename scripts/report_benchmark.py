"""Turn benchmark_nl runs into markdown: run comparison, shared-case before/after, per-question outcomes and test queries.

Only recorded results are reported. Cases the provider could not serve (quota) are excluded from scoring and counted separately.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from benchmark_nl import percentile, summarize  # noqa: E402

BENCHMARKS = ROOT / "artifacts" / "benchmark"
SHORT = {"correct_answer": "pass", "correct_refusal": "pass (refused)", "false_refusal": "false refusal", "wrong_accepted_meaning": "WRONG MEANING",
         "execution_mismatch": "WRONG ROWS", "unsafe_acceptance": "UNSAFE ANSWER", "execution_failure": "error", "availability_failure": "not run (quota)"}
SOURCES = {"logistics": "Logistics sample (12 tables, `fixtures/blind_logistics`)", "commerce": "Commerce demo", "support": "Support operations demo",
           "warehouse": "Retail warehouse", "billing": "SaaS billing", "chinook": "Chinook music store"}
SUITES = {"logistics": "Logistics regression (joins, subqueries, archives, refusals)", "relational": "Relational demos", "semantic": "Semantic regression and challenge",
          "capabilities": "Calculations (ratio, share, running total, change)", "resolution": "Name resolution and clarification"}


def load(label: str) -> tuple[dict, list[dict], int]:
    data = json.loads((BENCHMARKS / f"{label}.json").read_text(encoding="utf-8"))
    scored = [entry for entry in data["entries"] if entry["category"] != "availability_failure"]
    return data, scored, len(data["entries"]) - len(scored)


def pct(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def row(cells: list[object]) -> str:
    return "| " + " | ".join(str(cell).replace("|", "\\|").replace("\n", " ") for cell in cells) + " |"


def fraction(part: int, whole: int) -> str:
    return f"{part}/{whole}" if whole else "—"


def run_table(labels: list[str]) -> list[str]:
    lines = [row(["Run", "Model", "Pipeline · repair · prompt", "Scored", "Supported correct", "Refusals correct", "Wrong or unsafe", "False refusals",
                  "Tokens / question", "Median answer (s)", "Est. cost (USD)", "Status"]),
             row(["---"] * 12)]
    for label in labels:
        data, scored, skipped = load(label)
        summary = summarize(scored, data.get("model", ""))
        median = summary["median_answer_ms_excluding_rate_limit_waits"]
        lines.append(row([f"`{label}`", data.get("model"), f"{data.get('pipeline', 'single call')} · {data.get('repair_attempts', '—')} · {data.get('prompt_version', 'pre-AIDA 4')}",
                          f"{len(scored)}" + (f" (+{skipped} not run)" if skipped else ""),
                          f"{fraction(summary['correct_supported'], summary['supported_cases'])} ({pct(summary['supported_accuracy'])})",
                          f"{fraction(summary['correct_refusals'], summary['refusal_cases'])} ({pct(summary['refusal_accuracy'])})",
                          summary["wrong_or_unsafe_answers"], summary["false_refusals"], summary["tokens_per_question"] or "—",
                          "—" if median is None else f"{median / 1000:.2f}", "—" if summary["estimated_cost_usd"] is None else f"{summary['estimated_cost_usd']:.4f}",
                          data.get("status", "running")]))
    return lines


def shared_table(labels: list[str]) -> list[str]:
    runs = {label: load(label) for label in labels}
    shared = set.intersection(*({entry["key"] for entry in scored} for _, scored, _ in runs.values()))
    expectations = {key: {entry["expected"] for _, scored, _ in runs.values() for entry in scored if entry["key"] == key} for key in shared}
    changed = sorted(key for key, values in expectations.items() if len(values) > 1)
    note = f" The expected outcome differs between runs for {', '.join(changed)}: these were refusals before calculations existed and are scored as answers for AIDA 4." if changed else ""
    lines = [f"{len(shared)} questions were scored in every run below.{note}", "",
             row(["Run", "Supported correct", "Refusals correct", "Overall", "Wrong or unsafe", "False refusals"]), row(["---"] * 6)]
    for label, (data, scored, _) in runs.items():
        summary = summarize([entry for entry in scored if entry["key"] in shared], data.get("model", ""))
        lines.append(row([f"`{label}`", fraction(summary["correct_supported"], summary["supported_cases"]), fraction(summary["correct_refusals"], summary["refusal_cases"]),
                          f"{fraction(summary['overall_correct'], summary['cases'])} ({pct(summary['overall_accuracy'])})", summary["wrong_or_unsafe_answers"], summary["false_refusals"]]))
    return lines


def suite_table(label: str) -> list[str]:
    data, scored, _ = load(label)
    lines = [row(["Suite", "Supported correct", "Refusals correct", "Wrong or unsafe", "False refusals"]), row(["---"] * 5)]
    for suite in sorted({entry["suite"] for entry in scored}):
        summary = summarize([entry for entry in scored if entry["suite"] == suite], data.get("model", ""))
        lines.append(row([SUITES.get(suite, suite), fraction(summary["correct_supported"], summary["supported_cases"]),
                          fraction(summary["correct_refusals"], summary["refusal_cases"]), summary["wrong_or_unsafe_answers"], summary["false_refusals"]]))
    return lines


def matrix(labels: list[str]) -> list[str]:
    runs = {label: {entry["key"]: entry for entry in json.loads((BENCHMARKS / f"{label}.json").read_text(encoding="utf-8"))["entries"]} for label in labels}
    keys: list[str] = []
    for entries in runs.values():
        keys.extend(key for key in entries if key not in keys)
    lines = [row(["Case", "Question", "Expected", *[f"`{label}`" for label in labels]]), row(["---"] * (3 + len(labels)))]
    for key in keys:
        first = next(entries[key] for entries in runs.values() if key in entries)
        lines.append(row([key, first["question"], first["expected"], *[SHORT.get(entries[key]["category"], entries[key]["category"]) if key in entries else "" for entries in runs.values()]]))
    return lines


def queries(label: str) -> list[str]:
    data, _, _ = load(label)
    lines = ["# Test data and queries", "",
             f"Every question below was asked through the real AIDA pipeline (`{data.get('model')}`, {data.get('pipeline')} pipeline, repair {data.get('repair_attempts')}, prompt `{data.get('prompt_version')}`) "
             "and scored against independently written SQL. Generated by `scripts/report_benchmark.py` from "
             f"`artifacts/benchmark/{label}.json`; do not edit by hand.", "",
             "**Expected** is `answer` when the catalog supports the question exactly and `refusal` when AIDA must ask for clarification or refuse "
             "(unsupported operations, unknown or ambiguous names, restricted data). **Outcome** `pass` means the rows and plan matched the oracle, or the request was correctly refused. "
             "`false refusal` means AIDA asked for clarification although the question was answerable. `WRONG` or `UNSAFE` outcomes mean an incorrect answer was shown.", "",
             "## Sample data", "",
             row(["Source", "What it contains", "How to open it"]), row(["---"] * 3),
             row(["Logistics sample", "Synthetic 12-table logistics database: shipment legs, consignments, carriers, customers, exceptions, archives, nulls and duplicates", "Choose *Add the logistics sample* during onboarding, or `POST /api/v1/samples/logistics`"]),
             row(["Commerce demo", "Synthetic single-table orders with revenue, status, region, category and channel", "Built in"]),
             row(["Support operations demo", "Synthetic single-table support tickets", "Built in"]),
             row(["Retail warehouse", "Synthetic multi-table retail orders, lines, returns and archives", "Built in"]),
             row(["SaaS billing", "Synthetic multi-table invoices, lines, accounts and plans", "Built in"]),
             row(["Chinook music store", "Public 11-table sample (MIT)", "Built in"]), ""]
    by_source: dict[str, list[dict]] = {}
    for entry in data["entries"]:
        by_source.setdefault(entry["source_id"], []).append(entry)
    for source, entries in by_source.items():
        passed = sum(entry["category"] in ("correct_answer", "correct_refusal") for entry in entries)
        lines += [f"## {SOURCES.get(source, source)}", "", f"{passed}/{len(entries)} passed.", "",
                  row(["Case", "Question", "Expected", "Outcome", "What AIDA said when it did not answer"]), row(["---"] * 5)]
        for entry in entries:
            said = "" if entry["success"] else (f"[{entry['clarification_reason']}] " if entry.get("clarification_reason") else "") + (entry.get("error") or "")
            lines.append(row([entry["key"], entry["question"], entry["expected"], SHORT.get(entry["category"], entry["category"]), said[:220]]))
        lines.append("")
    return lines


SUMMARY_KEYS = ("cases", "supported_cases", "correct_supported", "supported_accuracy", "refusal_cases", "correct_refusals", "refusal_accuracy",
                "overall_correct", "overall_accuracy", "wrong_or_unsafe_answers", "false_refusals", "model_calls", "repair_calls",
                "tokens_per_question", "median_answer_ms_excluding_rate_limit_waits", "p95_answer_ms_excluding_rate_limit_waits", "estimated_cost_usd")


def describe(data: dict) -> str:
    if not data.get("prompt_version"):
        return "Previous pipeline"
    return f"AIDA 4 · prompt {data['prompt_version'].rsplit('-', 1)[-1]} · repair {data.get('repair_attempts', 0)}"


def compact(summary: dict) -> dict:
    return {key: summary[key] for key in SUMMARY_KEYS}


def engineering(data: dict, scored: list[dict]) -> dict:
    """Latency, cost and token facts for one run, from recorded telemetry only."""
    count = len(scored)
    telemetry = [entry["telemetry"] for entry in scored]
    summary = summarize(scored, data.get("model", ""))
    output = {"questions": count,
              "prompt_tokens_per_question": round(sum(item.get("prompt_tokens") or 0 for item in telemetry) / count, 1) if count else 0,
              "completion_tokens_per_question": round(sum(item.get("completion_tokens") or 0 for item in telemetry) / count, 1) if count else 0,
              "model_calls_per_question": round(sum(item.get("model_calls") or 0 for item in telemetry) / count, 2) if count else 0,
              "repair_rate": round(sum(1 for item in telemetry if (item.get("repair_calls") or 0) > 0) / count, 4) if count else 0,
              "cost_per_1000_questions_usd": round(summary["estimated_cost_usd"] / count * 1000, 3) if count and summary["estimated_cost_usd"] is not None else None,
              "latency_recorded": bool(data.get("prompt_version"))}
    if not data.get("prompt_version") or not count:
        return output
    answers = [entry["wall_ms"] - entry["rate_limit_wait_seconds"] * 1000 for entry in scored]
    clean = [entry for entry in scored if not entry["rate_limit_wait_seconds"] and not entry["telemetry"].get("rate_limit_wait_seconds") and entry["telemetry"].get("model_calls")]
    timed = [entry for entry in scored if entry["telemetry"].get("model_calls") and entry["telemetry"].get("elapsed_ms") is not None]

    def mean(values: list[float]) -> float | None:
        return round(sum(values) / len(values), 1) if values else None

    def stages(entry: dict, test) -> float:
        return sum(stage.get("latency_ms") or 0 for stage in entry["telemetry"].get("pipeline_stages") or [] if test(stage.get("stage", "")))

    output.update({
        "answer_ms_p50": round(statistics.median(answers), 1), "answer_ms_p95": round(percentile(answers, 0.95), 1),
        # Recorded model latency includes in-request rate-limit sleeps; subtracting the recorded wait gives model time, and
        # elapsed minus guard minus model latency (waits included) leaves AIDA's own code and SQL time for every question.
        "timed_questions": len(timed),
        "breakdown_mean_ms": {"guard_ms": mean([entry["telemetry"].get("guard_latency_ms") or 0 for entry in timed]),
                              "model_ms": mean([max((entry["telemetry"].get("model_latency_ms") or 0) - (entry["telemetry"].get("rate_limit_wait_seconds") or 0) * 1000, 0)
                                                for entry in timed]),
                              "code_and_sql_ms": mean([max(entry["telemetry"]["elapsed_ms"] - (entry["telemetry"].get("guard_latency_ms") or 0)
                                                           - (entry["telemetry"].get("model_latency_ms") or 0), 0) for entry in timed])},
        "clean_questions": len(clean),
        "stage_split_mean_ms": {"resolve_ms": mean([stages(entry, lambda name: name == "resolve") for entry in clean]),
                                "plan_ms": mean([stages(entry, lambda name: name in ("plan", "single")) for entry in clean]),
                                "repair_ms": mean([stages(entry, lambda name: name.endswith("_repair")) for entry in clean])},
        "questions_waiting_on_provider": sum(1 for entry in scored if entry["rate_limit_wait_seconds"]),
        "provider_wait_seconds_total": round(sum(entry["rate_limit_wait_seconds"] for entry in scored), 1)})
    return output


def engine_facts(path: Path | None) -> dict | None:
    if not path or not path.exists():
        return None
    report = json.loads(path.read_text(encoding="utf-8"))
    report.get("engine", {}).pop("cases", None)
    for run in report.get("replay", {}).values():
        run.pop("items", None)
    return report


def web(args: argparse.Namespace) -> dict:
    """Facts for the website, derived only from recorded benchmark entries."""
    loaded = {label: load(label) for label in args.runs}
    runs = []
    for label, (data, scored, skipped) in loaded.items():
        planned = [entry for entry in scored if entry["success"] and entry["plan_match"]]
        recorded_reasons = bool(data.get("prompt_version"))
        reasons = Counter((entry.get("clarification_reason") or "code_rejected") if recorded_reasons else "not_recorded"
                          for entry in scored if entry["category"] == "false_refusal")
        runs.append({"label": label, "model": data.get("model"), "pipeline": describe(data), "prompt_version": data.get("prompt_version"),
                     "repair_attempts": data.get("repair_attempts"), "status": data.get("status") or "in progress", "started_at": data.get("started_at"),
                     "scored": len(scored), "not_run": skipped, "summary": compact(summarize(scored, data.get("model", ""))),
                     "categories": dict(Counter(entry["category"] for entry in scored)),
                     "by_suite": {suite: compact(summarize([entry for entry in scored if entry["suite"] == suite], data.get("model", "")))
                                  for suite in sorted({entry["suite"] for entry in scored})},
                     "plan_correct_answers": len(planned), "rows_correct_when_plan_correct": sum(bool(entry["rows_match"]) for entry in planned),
                     "false_refusal_reasons": dict(reasons), "engineering": engineering(data, scored)})

    def comparison(labels: list[str]) -> dict:
        shared = set.intersection(*({entry["key"] for entry in loaded[label][1]} for label in labels))
        expectations = {key: {entry["expected"] for label in labels for entry in loaded[label][1] if entry["key"] == key} for key in shared}
        return {"runs": labels, "questions": len(shared), "expectation_changed": sorted(key for key, values in expectations.items() if len(values) > 1),
                "summaries": {label: compact(summarize([entry for entry in loaded[label][1] if entry["key"] in shared], loaded[label][0].get("model", ""))) for label in labels}}

    by_model: dict[str, list[str]] = {}
    for label, (data, _, _) in loaded.items():
        if data.get("prompt_version"):
            by_model.setdefault(data.get("model"), []).append(label)
    prompt_pairs = [comparison(labels) for labels in by_model.values() if len({loaded[label][0]["prompt_version"] for label in labels}) > 1]
    questions: dict[str, dict] = {}
    for label, (data, _, _) in loaded.items():
        for entry in data["entries"]:
            item = questions.setdefault(entry["key"], {"key": entry["key"], "suite": SUITES.get(entry["suite"], entry["suite"]), "source": SOURCES.get(entry["source_id"], entry["source_id"]).split(" (")[0],
                                                      "question": entry["question"], "expected": entry["expected"], "outcomes": {}})
            item["outcomes"][label] = entry["category"]
    return {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "source": "artifacts/benchmark/*.json via scripts/report_benchmark.py",
            "runs": runs, "before_after": comparison(args.shared) if args.shared else None, "prompt_pairs": prompt_pairs,
            "questions": list(questions.values()), "engineering": engine_facts(args.engine)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", nargs="+", required=True, help="Run labels to compare (artifacts/benchmark/<label>.json).")
    parser.add_argument("--shared", nargs="+", help="Runs to compare on the questions they all scored.")
    parser.add_argument("--suites", help="Run label for the per-suite table.")
    parser.add_argument("--tables", type=Path, default=BENCHMARKS / "report-tables.md")
    parser.add_argument("--queries-run", help="Run label used to generate docs/TEST_QUERIES.md.")
    parser.add_argument("--web", type=Path, help="Write website benchmark facts as JSON to this path.")
    parser.add_argument("--engine", type=Path, help="Engineering benchmark JSON from scripts/benchmark_engine.py to include in --web output.")
    args = parser.parse_args()
    lines = ["## Runs", "", *run_table(args.runs), ""]
    if args.shared:
        lines += ["## Shared questions", "", *shared_table(args.shared), ""]
    if args.suites:
        lines += [f"## By suite: `{args.suites}`", "", *suite_table(args.suites), ""]
    lines += ["## Per question", "", *matrix(args.runs), ""]
    args.tables.write_text("\n".join(lines), encoding="utf-8")
    if args.web:
        args.web.write_text(json.dumps(web(args), indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {args.web}")
    if args.queries_run:
        (ROOT / "docs" / "TEST_QUERIES.md").write_text("\n".join(queries(args.queries_run)), encoding="utf-8")
    print(f"Wrote {args.tables}" + (" and docs/TEST_QUERIES.md" if args.queries_run else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
