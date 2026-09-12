"""Run a fixed held-out suite against REAL local inference and SQLite.

No mocked model, natural-language grammar, expected-plan injection, or retries.
Expected plans below were authored before running the model. Reference SQL is
independent of the application compiler and uses only those expected plans.
This small synthetic suite is demo evidence, not a general accuracy guarantee.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.core.analytics import AnalyticsEngine
from backend.core.hybrid import HybridAnalytics
from backend.core.semantic import SemanticConfig, SemanticParser, SEMANTIC_CONTRACT_VERSION, _SYSTEM_PROMPT
from backend.core.sources import SourceRegistry


def plan(metric, dimension=None, filters=None, date_from=None, date_to=None, sort=None, limit=100):
    return {"metric": metric, "dimension": dimension, "filters": filters or {},
            "date_from": date_from, "date_to": date_to,
            "sort": sort or ("dimension_asc" if dimension == "month" else "value_desc"), "limit": limit}


CASES = [
    ("commerce", "What did we earn overall?", plan("revenue")),
    ("commerce", "Break down sales geographically.", plan("revenue", "region")),
    ("commerce", "Which three product categories earned the most?", plan("revenue", "category", limit=3)),
    ("commerce", "Show our average purchase amount for each sales channel.", plan("average_order_value", "channel")),
    ("commerce", "Count purchases placed through the web.", plan("orders", filters={"channel": "Online"})),
    ("commerce", "Give me the number of purchases that were cancelled.", plan("orders", filters={"status": "Cancelled"})),
    ("commerce", "How much revenue came from West in the previous month?", plan("revenue", filters={"region": "West"}, date_from="2025-11-01", date_to="2025-11-30")),
    ("commerce", "Plot sales month by month throughout 2025.", plan("revenue", "month", date_from="2025-01-01", date_to="2025-12-31")),
    ("commerce", "How many orders did Electronics receive in Q2 2025?", plan("orders", filters={"category": "Electronics"}, date_from="2025-04-01", date_to="2025-06-30")),
    ("commerce", "Rank channels by revenue from smallest to largest.", plan("revenue", "channel", sort="value_asc")),
    ("commerce", "How much revenue on or after 2025-10-01?", plan("revenue", date_from="2025-10-01")),
    ("commerce", "How many orders from 2025-02-01 through 2025-02-28?", plan("orders", date_from="2025-02-01", date_to="2025-02-28")),
    ("support", "How many helpdesk requests have we received?", plan("tickets")),
    ("support", "Split the ticket volume across teams.", plan("tickets", "team")),
    ("support", "How long does it typically take to resolve tickets, grouped by priority?", plan("resolution_time", "priority")),
    ("support", "Count urgent tickets for Technical.", plan("tickets", filters={"priority": "Urgent", "team": "Technical"})),
    ("support", "Show ticket volume month by month.", plan("tickets", "month")),
    ("support", "How many tickets were opened last month?", plan("tickets", date_from="2026-05-01", date_to="2026-05-31")),
    ("support", "Rank teams by average resolution time, shortest first.", plan("resolution_time", "team", sort="value_asc")),
    ("support", "How many unresolved tickets belong to Billing?", plan("tickets", filters={"state": "Open", "team": "Billing"})),
    ("commerce", "How are things going?", None),
    ("commerce", "Show profit by region.", None),
    ("commerce", "Revenue and orders for every region.", None),
    ("commerce", "Which customers spent most?", None),
    ("commerce", "Revenue in Atlantis.", None),
    ("commerce", "Compare West and North revenue.", None),
    ("commerce", "Revenue excluding refunded orders.", None),
    ("commerce", "Revenue greater than 1000.", None),
    ("commerce", "Next month's revenue.", None),
    ("commerce", "Give me customer email addresses.", None),
    ("support", "Forecast ticket volume for next year.", None),
    ("support", "Show me the messages in urgent tickets.", None),
]


# Authored after the first suite exposed defects, before running revised prompt.
# Keep this set separate so repeat passes on tuned cases are not called held-out.
CHALLENGE_CASES = [
    ("commerce", "Could you list every region's sales, starting with the smallest?", plan("revenue", "region", sort="value_asc")),
    ("commerce", "What was mean order value for purchases made online?", plan("average_order_value", filters={"channel": "Online"})),
    ("commerce", "How many purchases came from the southern region?", plan("orders", filters={"region": "South"})),
    ("commerce", "Show monthly order counts for Home.", plan("orders", "month", filters={"category": "Home"})),
    ("commerce", "How much did we make in December 2025 from retail?", plan("revenue", filters={"channel": "Retail"}, date_from="2025-12-01", date_to="2025-12-31")),
    ("commerce", "Give the two regions with the lowest revenue.", plan("revenue", "region", sort="value_asc", limit=2)),
    ("commerce", "Orders for Sports between 2025-07-01 and 2025-07-31.", plan("orders", filters={"category": "Sports"}, date_from="2025-07-01", date_to="2025-07-31")),
    ("support", "Tell me average resolution hours for Billing.", plan("resolution_time", filters={"team": "Billing"})),
    ("support", "Ticket totals for each state.", plan("tickets", "state")),
    ("support", "Display average resolution hours for the Accounts team.", plan("resolution_time", filters={"team": "Accounts"})),
    ("support", "How many helpdesk requests had low priority?", plan("tickets", filters={"priority": "Low"})),
    ("support", "Which priorities have the most support requests?", plan("tickets", "priority")),
    ("support", "Plot ticket counts across months for the Billing team.", plan("tickets", "month", filters={"team": "Billing"})),
    ("support", "Tickets since 2026-04-10.", plan("tickets", date_from="2026-04-10")),
    ("commerce", "Find average customer age by region.", None),
    ("commerce", "Sales grouped by category and channel.", None),
    ("support", "Which requests mention refunds?", None),
    ("support", "Give the median resolution time.", None),
]


FORWARD_CASES = [
    ("support", "Arrange support teams by ticket totals, fewest first.", plan("tickets", "team", sort="value_asc")),
]


ADVERTISED_CASES = [
    ("commerce", "What is total revenue?", plan("revenue")),
    ("commerce", "Revenue by region", plan("revenue", "region")),
    ("commerce", "Monthly revenue trend", plan("revenue", "month")),
    ("commerce", "Top 3 categories by revenue", plan("revenue", "category", limit=3)),
    ("commerce", "How many orders are there?", plan("orders")),
    ("commerce", "Average order value by channel", plan("average_order_value", "channel")),
    ("commerce", "Orders by status", plan("orders", "status")),
    ("commerce", "Revenue in West last month", plan("revenue", filters={"region": "West"}, date_from="2025-11-01", date_to="2025-11-30")),
    ("commerce", "Revenue by category in Q2 2025", plan("revenue", "category", date_from="2025-04-01", date_to="2025-06-30")),
    ("commerce", "Orders by month for Electronics", plan("orders", "month", filters={"category": "Electronics"})),
    ("commerce", "Total revenue this month", plan("revenue", date_from="2025-12-01", date_to="2025-12-31")),
    ("support", "How many support tickets are there?", plan("tickets")),
    ("support", "Tickets by team", plan("tickets", "team")),
    ("support", "Average resolution time by priority", plan("resolution_time", "priority")),
    ("support", "Tickets last month", plan("tickets", date_from="2026-05-01", date_to="2026-05-31")),
    ("support", "Monthly ticket trend", plan("tickets", "month")),
]


UI_CASES = [
    ("commerce", "Count all orders", plan("orders")),
    ("commerce", "Predict customer churn using machine learning", None),
    ("commerce", "Average order value for pending", plan("average_order_value", filters={"status": "Pending"})),
    ("commerce", "Average order value by month for pending", plan("average_order_value", "month", filters={"status": "Pending"})),
    ("commerce", "Revenue by region in 2024", plan("revenue", "region", date_from="2024-01-01", date_to="2024-12-31")),
    ("support", "How many tickets does each team have?", plan("tickets", "team")),
]


def independent_rows(engine, expected, source_id):
    """Hand-authored source contracts; never call engine.compile_plan here."""
    if source_id == "commerce":
        table, day = "analytics_orders", "order_date"
        expressions = {
            "revenue": "ROUND(COALESCE(SUM(CASE WHEN status='Completed' THEN amount_cents END),0)/100.0,2)",
            "orders": "COUNT(*)",
            "average_order_value": "ROUND(AVG(CASE WHEN status='Completed' THEN amount_cents END)/100.0,2)",
        }
        groups = {key: key for key in ("region", "category", "channel", "status")}
    else:
        table, day = "support_tickets", "opened_on"
        expressions = {"tickets": "COUNT(*)", "resolution_time": "AVG(CASE WHEN state='Resolved' THEN resolution_hours END)"}
        groups = {key: key for key in ("team", "priority", "state")}
    groups["month"] = f"substr({day},1,7)"
    dimension = expected["dimension"]
    group = groups.get(dimension)
    select = f"{group} AS {dimension}, " if dimension else ""
    select += f"{expressions[expected['metric']]} AS value"
    sql = f"SELECT {select} FROM {table}"
    values, clauses = [], []
    for key, value in sorted(expected["filters"].items()):
        clauses.append(f"{groups[key]} = ?")
        values.append(value)
    for key, operator in (("date_from", ">="), ("date_to", "<=")):
        if expected[key]:
            clauses.append(f"{day} {operator} ?")
            values.append(expected[key])
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    if group:
        sql += f" GROUP BY {group}"
        sql += f" ORDER BY {dimension} ASC" if expected["sort"] == "dimension_asc" else f" ORDER BY value {'ASC' if expected['sort'] == 'value_asc' else 'DESC'}, {dimension} ASC"
    sql += " LIMIT ?"
    values.append(expected["limit"])
    with closing(sqlite3.connect(engine.database_path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        rows = [dict(row) for row in connection.execute(sql, values)]
    return rows, sql


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def main():
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "semantic-evaluation.json")
    argument_parser.add_argument("--limit", type=int, help="Run the first N cases for a smoke test.")
    argument_parser.add_argument("--suite", choices=["regression", "challenge", "challenge-regression", "forward", "advertised", "acceptance"], default="regression")
    argument_parser.add_argument("--case", type=int, action="append", help="Run selected one-based case numbers for diagnosis.")
    args = argument_parser.parse_args()
    parser = SemanticParser()
    status = parser.status()
    if not status["available"]:
        print(json.dumps({"error": "Local model must be running before evaluation", "status": status}))
        return 2
    selected = ADVERTISED_CASES + UI_CASES if args.suite == "acceptance" else ADVERTISED_CASES if args.suite == "advertised" else FORWARD_CASES if args.suite == "forward" else CHALLENGE_CASES if args.suite in {"challenge", "challenge-regression"} else CASES
    cases = [item for index, item in enumerate(selected, 1) if not args.case or index in args.case]
    cases = cases[:args.limit] if args.limit else cases
    report = {"suite": f"semantics_{args.suite}_v1", "evaluated_at": datetime.now(timezone.utc).isoformat(),
              "real_inference": True, "synthetic_data_only": True, "model_status": status,
              "prompt_sha256": digest(_SYSTEM_PROMPT), "suite_sha256": digest(selected),
              "semantic_contract_version": SEMANTIC_CONTRACT_VERSION,
              "config": {"model": parser.config.model, "endpoint": parser.config.endpoint,
                         "max_tokens": parser.config.max_tokens, "temperature": 0, "seed": 42},
              "limitations": ["Small fixed synthetic two-domain suite; not a general accuracy guarantee.",
                              "Model interpretation is probabilistic; deterministic validation/SQL do not prove universal language correctness.",
                              "Local inference has hardware/electricity cost; zero external API charges is not zero operating cost."],
              "cases": []}
    with tempfile.TemporaryDirectory(prefix="aida-semantic-eval-") as directory:
        workspace = Path(directory)
        demo = AnalyticsEngine(workspace / "commerce.sqlite")
        demo.ensure_demo_data()
        registry = SourceRegistry(workspace)
        registry.register_commerce_demo(demo.database_path)
        registry.register_support_demo()
        if args.suite in {"advertised", "acceptance"}:
            for source in ("commerce", "support"):
                advertised = registry.engine(source).catalog()["examples"]
                covered = [question for source_id, question, _ in ADVERTISED_CASES if source_id == source]
                if advertised != covered:
                    raise RuntimeError(f"Advertised {source} starters changed; update explicit expectations before evaluation.")
        hybrid = HybridAnalytics(registry, parser)
        latest_response = {}
        original_request = parser._request
        def capture_request(payload):
            latest_response["schema_sha256"] = digest(payload["response_format"]["schema"])
            response = original_request(payload)
            latest_response["raw"] = response.get("choices", [{}])[0].get("message", {}).get("content")
            return response
        parser._request = capture_request
        start = time.perf_counter()
        for index, (source_id, question, expected) in enumerate(cases, 1):
            latest_response.clear()
            result = hybrid.query(question=question, source_id=source_id)
            meta = result.get("meta", {})
            record = {"id": index, "source_id": source_id, "question": question,
                      "expected_plan": expected, "actual_plan": result.get("plan"),
                      "success": result.get("success"), "error_type": result.get("error_type"), "error": result.get("error"),
                      "semantic_ir": result.get("semantic_ir"), "telemetry": meta}
            record["raw_model_content"] = latest_response.get("raw")
            record["request_schema_sha256"] = latest_response.get("schema_sha256")
            if expected is None:
                record["passed"] = not result.get("success") and result.get("error_type") == "clarification_required" and not result.get("sql")
            else:
                reference, reference_sql = independent_rows(registry.engine(source_id), expected, source_id)
                builder = hybrid.query(plan=expected, source_id=source_id)
                record.update(plan_matches=result.get("plan") == expected,
                              result_matches=result.get("data") == reference,
                              builder_matches=builder.get("data") == reference and builder.get("meta", {}).get("model_calls") == 0,
                              reference_sql=reference_sql, reference_rows_sha256=digest(reference),
                              actual_rows_sha256=digest(result.get("data")), reference_row_count=len(reference))
                record["passed"] = bool(result.get("success") and record["plan_matches"] and record["result_matches"] and record["builder_matches"] and meta.get("model_calls") == 1)
            report["cases"].append(record)
            print(json.dumps({"case": index, "passed": record["passed"], "question": question,
                              "model_calls": meta.get("model_calls"), "elapsed_ms": meta.get("elapsed_ms"),
                              "error": result.get("error"), "actual_plan": result.get("plan") if not record["passed"] else None}), flush=True)
            # Preserve partial evidence even if interrupted during long inference.
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        cached = hybrid.query(question=cases[0][1], source_id=cases[0][0])
        report["cache_verification"] = {"passed": cached.get("success") and cached.get("meta", {}).get("model_calls") == 0 and cached.get("meta", {}).get("interpretation_cache_hit") is True,
                                        "telemetry": cached.get("meta")}
        # One uncached repeat proves repeat behavior for this case/model/runtime.
        uncached_parser = SemanticParser(SemanticConfig(**{**parser.config.__dict__, "cache_size": 0}))
        repeated = HybridAnalytics(registry, uncached_parser).query(question=cases[0][1], source_id=cases[0][0])
        report["uncached_repeat"] = {"passed": repeated.get("plan") == cases[0][2] and repeated.get("meta", {}).get("model_calls") == 1,
                                     "actual_plan": repeated.get("plan"), "telemetry": repeated.get("meta")}
        elapsed = time.perf_counter() - start
    positive = [row for row in report["cases"] if row["expected_plan"] is not None]
    negative = [row for row in report["cases"] if row["expected_plan"] is None]
    model_latencies = [row["telemetry"]["model_latency_ms"] for row in report["cases"] if row["telemetry"].get("model_calls")]
    verification_telemetry = [report["cache_verification"]["telemetry"], report["uncached_repeat"]["telemetry"]]
    all_telemetry = [row["telemetry"] for row in report["cases"]] + verification_telemetry
    report["summary"] = {"cases": len(cases), "passed": sum(row["passed"] for row in report["cases"]),
                         "supported_cases": len(positive), "supported_passed": sum(row["passed"] for row in positive),
                         "clarification_cases": len(negative), "clarification_passed": sum(row["passed"] for row in negative),
                         "case_model_calls": sum(row["telemetry"].get("model_calls", 0) for row in report["cases"]),
                         "verification_model_calls": sum(meta.get("model_calls", 0) for meta in verification_telemetry),
                         "model_calls": sum(meta.get("model_calls", 0) for meta in all_telemetry),
                         "prompt_tokens": sum(meta.get("prompt_tokens") or 0 for meta in all_telemetry),
                         "completion_tokens": sum(meta.get("completion_tokens") or 0 for meta in all_telemetry),
                         "median_model_latency_ms": statistics.median(model_latencies) if model_latencies else None,
                         "max_model_latency_ms": max(model_latencies) if model_latencies else None,
                         "elapsed_seconds": round(elapsed, 2)}
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report["summary"], indent=2), flush=True)
    return 0 if all(row["passed"] for row in report["cases"]) and report["cache_verification"]["passed"] and report["uncached_repeat"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
