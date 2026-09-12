"""Legacy grammar reference and reproducible commerce seed.

The application imports this module for synthetic data only. Natural-language
requests run through HybridAnalytics and SemanticParser; this legacy parser is
not a runtime fallback. Its tests are not evidence of model accuracy.
"""
from __future__ import annotations

import calendar
import copy
import hashlib
import json
import random
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path
from typing import Any


DATE_FROM = "2025-01-01"
DATE_TO = "2025-12-31"
AS_OF = date.fromisoformat(DATE_TO)
DATA_VERSION = 1
APPLICATION_ID = 0x41494441  # AIDA: prevents accidentally opening unrelated files.
MAX_ROWS = 100
MAX_SECONDS = 2.0
METRICS = {
    "revenue": {
        "label": "Revenue", "format": "currency",
        "description": "Sum of completed order amounts, in USD. Pending, cancelled and refunded orders contribute zero.",
        "sql": "ROUND(COALESCE(SUM(CASE WHEN status = 'Completed' THEN amount_cents ELSE 0 END), 0) / 100.0, 2)",
    },
    "orders": {
        "label": "Orders", "format": "number",
        "description": "Number of orders across every status, unless a status filter is applied.",
        "sql": "COUNT(*)",
    },
    "average_order_value": {
        "label": "Average order value", "format": "currency",
        "description": "Average completed order amount, in USD. Returns null when no completed orders match.",
        "sql": "ROUND(AVG(CASE WHEN status = 'Completed' THEN amount_cents END) / 100.0, 2)",
    },
}
DIMENSIONS = {
    "region": {"label": "Region", "values": ["North", "South", "East", "West"]},
    "category": {"label": "Category", "values": ["Electronics", "Home", "Clothing", "Sports", "Beauty"]},
    "channel": {"label": "Channel", "values": ["Online", "Retail", "Partner"]},
    "status": {"label": "Status", "values": ["Completed", "Pending", "Cancelled", "Refunded"]},
    "month": {"label": "Month"},
}
EXAMPLES = [
    "What is total revenue?", "Revenue by region", "Monthly revenue trend",
    "Top 3 categories by revenue", "How many orders are there?",
    "Average order value by channel", "Orders by status",
    "Revenue in West last month", "Revenue by category in Q2 2025",
    "Orders by month for Electronics", "Total revenue this month",
]
FILTER_ALIASES = {
    "region": {"north": "North", "south": "South", "east": "East", "west": "West"},
    "category": {"electronics": "Electronics", "home": "Home", "clothing": "Clothing", "sports": "Sports", "beauty": "Beauty"},
    "channel": {"online": "Online", "web": "Online", "retail": "Retail", "in store": "Retail", "partner": "Partner"},
    "status": {"completed": "Completed", "complete": "Completed", "pending": "Pending", "cancelled": "Cancelled", "canceled": "Cancelled", "refunded": "Refunded"},
}
DIMENSION_ALIASES = {
    "region": r"regions?", "category": r"categor(?:y|ies)",
    "channel": r"channels?", "status": r"status(?:es)?", "month": r"months?",
}


class ClarificationRequired(ValueError):
    """An input cannot be represented without inventing business meaning."""


def _month_range(year: int, month: int) -> tuple[str, str]:
    return date(year, month, 1).isoformat(), date(year, month, calendar.monthrange(year, month)[1]).isoformat()


def validate_plan(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize and reject unknown keys, identifiers, filter values and types."""
    if not isinstance(raw, dict):
        raise ClarificationRequired("A query plan must be an object. Use the query builder to select a metric.")
    allowed = {"metric", "dimension", "filters", "date_from", "date_to", "sort", "limit"}
    if set(raw) - allowed:
        raise ClarificationRequired("Unsupported query plan fields. Use metric, dimension, filters, date_from, date_to, sort and limit.")
    metric = raw.get("metric")
    if not isinstance(metric, str) or metric not in METRICS:
        raise ClarificationRequired("Choose one metric: revenue, orders, or average order value.")
    dimension = raw.get("dimension")
    if dimension is not None and (not isinstance(dimension, str) or dimension not in DIMENSIONS):
        raise ClarificationRequired("Group by region, category, channel, status, month, or choose no grouping.")
    raw_filters = raw.get("filters", {})
    if not isinstance(raw_filters, dict):
        raise ClarificationRequired("Filters must be an object containing supported dimension values.")
    filters = {}
    for key, value in raw_filters.items():
        if key not in FILTER_ALIASES or not isinstance(value, str):
            raise ClarificationRequired("Filters support one region, category, channel or status value each.")
        normalized = FILTER_ALIASES[key].get(value.strip().lower())
        if normalized is None:
            raise ClarificationRequired(f"Unknown {key}. Choose one of: {', '.join(DIMENSIONS[key]['values'])}.")
        filters[key] = normalized
    dates = {}
    for key in ("date_from", "date_to"):
        value = raw.get(key)
        if value is not None:
            if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                raise ClarificationRequired("Dates must use YYYY-MM-DD.")
            try:
                date.fromisoformat(value)
            except ValueError as exc:
                raise ClarificationRequired("Use valid calendar dates in YYYY-MM-DD format.") from exc
        dates[key] = value
    if dates["date_from"] and dates["date_to"] and dates["date_from"] > dates["date_to"]:
        raise ClarificationRequired("The start date must be on or before the end date.")
    limit = raw.get("limit", 100)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_ROWS:
        raise ClarificationRequired(f"The result limit must be a whole number from 1 to {MAX_ROWS}.")
    sort = raw.get("sort", "dimension_asc" if dimension == "month" else "value_desc")
    if not isinstance(sort, str) or sort not in {"value_desc", "value_asc", "dimension_asc"}:
        raise ClarificationRequired("Sort must be value_desc, value_asc, or dimension_asc.")
    return {"metric": metric, "dimension": dimension, "filters": dict(sorted(filters.items())), **dates, "sort": sort, "limit": limit}


def parse_question(question: str) -> dict[str, Any]:
    """Recognize a deliberately bounded grammar; never drop unknown intent."""
    if not isinstance(question, str) or not question.strip():
        raise ClarificationRequired("Ask about revenue, orders or average order value, or use the query builder.")
    if len(question) > 500:
        raise ClarificationRequired("Keep the question under 500 characters and ask for one metric at a time.")
    work = question.lower().replace("\u2019", "'").replace("in-store", "in store")
    work = re.sub(r"\bwhat's\b", "what is", work)
    work = re.sub(r"^\s*may i\b", "can i", work)
    work = re.sub(r"\bon or after\b", "since", work)
    work = re.sub(r"\bon or before\b", "through", work)
    if re.search(r"[;`]|--|/\*|\*/|\b(select|drop|insert|delete|update|pragma|attach|union|password|email|phone|address|customers?|names?|individual|raw|records?|rows?)\b", work):
        raise ClarificationRequired("This demo supports aggregate commerce metrics only. Customer details, raw rows and SQL are unavailable.")
    if re.search(r"\b(compare|comparison|versus|vs|growth|change|difference|forecast|predict|median|percentile|profit|margin|not|exclude|excluding|except|without|or)\b", work):
        raise ClarificationRequired("That comparison, exclusion or metric is not supported yet. Ask for a single metric and one optional grouping, or use the builder.")
    original = work
    if re.search(r"\b(?:by|per|for|from|in|during|with|where|equals?|to|and|across|between|since|until|through|before|after)\s*[?.!]*$", work):
        raise ClarificationRequired("The question ends with an incomplete grouping or filter. Complete it or use the query builder.")
    plan: dict[str, Any] = {"filters": {}}

    # A sort is a separate clause, so consume it before metric/group recognition.
    # Otherwise 'by region sorted by region' would lose the requested ordering.
    sort_pattern = r"\b(?:sorted|sort|ordered|order)\s+by\s+(average order value|revenue|sales|orders|region|category|channel|status|month)(?:\s+(ascending|descending))?\b"
    sort_clauses = re.findall(sort_pattern, work)
    if len(sort_clauses) > 1:
        raise ClarificationRequired("Use one sort instruction per question.")
    explicit_sort = sort_clauses[0] if sort_clauses else None
    work = re.sub(sort_pattern, " ", work)

    # Bind time before consuming other numbers or connective words.
    periods: list[tuple[str | None, str | None]] = []
    def take_time(pattern: str, converter: Any) -> None:
        nonlocal work
        def replace(match: re.Match[str]) -> str:
            try:
                periods.append(converter(match))
            except (ValueError, OverflowError) as exc:
                raise ClarificationRequired("That date period is not valid. Use YYYY-MM-DD dates or a month in 2025.") from exc
            return " "
        work = re.sub(pattern, replace, work)

    take_time(r"\b(?:between|from)\s+(\d{4}-\d{2}-\d{2})\s+(?:and|to|through)\s+(\d{4}-\d{2}-\d{2})\b", lambda m: (m[1], m[2]))
    take_time(r"\b(?:since|from|on or after)\s+(\d{4}-\d{2}-\d{2})\b", lambda m: (m[1], None))
    take_time(r"\b(?:through|until|on or before)\s+(\d{4}-\d{2}-\d{2})\b", lambda m: (None, m[1]))
    take_time(r"\bon\s+(\d{4}-\d{2}-\d{2})\b", lambda m: (m[1], m[1]))
    take_time(r"\bafter\s+(\d{4}-\d{2}-\d{2})\b", lambda m: ((date.fromisoformat(m[1]) + timedelta(days=1)).isoformat(), None))
    take_time(r"\bbefore\s+(\d{4}-\d{2}-\d{2})\b", lambda m: (None, (date.fromisoformat(m[1]) - timedelta(days=1)).isoformat()))
    take_time(r"\b(?:this|current)\s+month\b", lambda m: _month_range(AS_OF.year, AS_OF.month))
    previous_month = AS_OF.replace(day=1) - timedelta(days=1)
    take_time(r"\b(?:last|previous)\s+month\b", lambda m: _month_range(previous_month.year, previous_month.month))
    take_time(r"\b(?:this|current)\s+year\b|\byear to date\b|\bytd\b", lambda m: (f"{AS_OF.year}-01-01", AS_OF.isoformat()))
    take_time(r"\b(?:last|previous)\s+year\b", lambda m: (f"{AS_OF.year-1}-01-01", f"{AS_OF.year-1}-12-31"))
    quarter = (AS_OF.month - 1) // 3 + 1
    take_time(r"\bthis\s+quarter\b", lambda m: (date(AS_OF.year, 3 * quarter - 2, 1).isoformat(), AS_OF.isoformat()))
    prior_quarter = AS_OF.replace(month=3 * quarter - 2, day=1) - timedelta(days=1)
    take_time(r"\blast\s+quarter\b", lambda m: (date(prior_quarter.year, 3*((prior_quarter.month-1)//3)+1, 1).isoformat(), prior_quarter.isoformat()))
    take_time(r"\b(?:last|past)\s+(\d+)\s+days?\b", lambda m: ((AS_OF - timedelta(days=int(m[1])-1)).isoformat(), AS_OF.isoformat()) if 1 <= int(m[1]) <= 3660 else (_ for _ in ()).throw(ValueError()))
    take_time(r"\b(?:in\s+)?q([1-4])(?:\s+(\d{4}))?\b", lambda m: (date(int(m[2] or AS_OF.year), int(m[1])*3-2, 1).isoformat(), _month_range(int(m[2] or AS_OF.year), int(m[1])*3)[1]))
    month_names = {name.lower(): index for index, name in enumerate(calendar.month_name) if name}
    month_names.update({name.lower(): index for index, name in enumerate(calendar.month_abbr) if name})
    month_pattern = "|".join(sorted(month_names, key=len, reverse=True))
    take_time(rf"\b(?:from|between)\s+({month_pattern})\s+(?:to|through|and)\s+({month_pattern})(?:\s+(\d{{4}}))?\b", lambda m: (_month_range(int(m[3] or AS_OF.year), month_names[m[1]])[0], _month_range(int(m[3] or AS_OF.year), month_names[m[2]])[1]))
    take_time(rf"\b(?:in\s+)?({month_pattern})(?:\s+(\d{{4}}))?\b", lambda m: _month_range(int(m[2] or AS_OF.year), month_names[m[1]]))
    take_time(r"\b(?:in|for|during)\s+(\d{4})\b", lambda m: (date(int(m[1]), 1, 1).isoformat(), date(int(m[1]), 12, 31).isoformat()))
    if len(periods) > 1:
        # A single lower bound and upper bound form one interval, all other
        # combinations could encode a comparison or conflicting time range.
        if len(periods) == 2 and ((periods[0][0] and not periods[0][1] and not periods[1][0] and periods[1][1]) or (not periods[0][0] and periods[0][1] and periods[1][0] and not periods[1][1])):
            periods = [(periods[0][0] or periods[1][0], periods[0][1] or periods[1][1])]
        else:
            raise ClarificationRequired("Use one date period per question; date comparisons are not supported yet.")
    if periods:
        plan["date_from"], plan["date_to"] = periods[0]

    # Extract the most specific monetary metric first to avoid double counting.
    metric_patterns = [
        ("average_order_value", r"\b(?:average|avg|mean)(?:\s+of)?\s+(?:order|transaction|purchase)\s+(?:values?|amounts?|revenue|sales)\b|\b(?:average|avg|mean)\s+(?:value|amount)\s+(?:per|of an?)\s+(?:order|transaction|purchase)\b|\baov\b"),
        ("revenue", r"\b(?:revenue|sales)(?:\s+(?:amount|value))?\b"),
        ("orders", r"\b(?:orders?|purchases?|transactions?)\b"),
    ]
    metrics_found = []
    for metric, pattern in metric_patterns:
        matches = re.findall(pattern, work)
        if matches:
            metrics_found.extend([metric] * len(matches))
            work = re.sub(pattern, " ", work)
    if len(metrics_found) != 1:
        raise ClarificationRequired("Ask for exactly one metric: revenue, orders, or average order value.")
    plan["metric"] = metrics_found[0]
    if (plan["metric"] != "orders" and re.search(r"\bcount\b|\bnumber of\b|\bhow many\b", original)) or (plan["metric"] != "revenue" and re.search(r"\bsum\b", original)) or (plan["metric"] == "average_order_value" and re.search(r"\btotal\b", original)) or (plan["metric"] == "orders" and re.search(r"\bhow much\b", original)):
        raise ClarificationRequired("That aggregation conflicts with the metric definition. Use revenue for sum, orders for count, or average order value for mean.")

    # Top/bottom applies to one grouping and must include an explicit N.
    ranking = re.search(r"\b(top|bottom)\s+(\d+)\b", work)
    if ranking:
        plan["limit"] = int(ranking[2])
        plan["sort"] = "value_desc" if ranking[1] == "top" else "value_asc"
        work = work[:ranking.start()] + " " + work[ranking.end():]
    groups = []
    if re.search(r"\bmonthly\b|\bover time\b|\bmonth by month\b", work):
        groups.append("month")
        work = re.sub(r"\bmonthly\b|\bover time\b|\bmonth by month\b", " ", work)
    for dimension, pattern in DIMENSION_ALIASES.items():
        group_pattern = rf"\b(?:grouped by|broken down by|breakdown by|for each|by|per|across)\s+(?:(?:the|each)\s+)?(?:{pattern})\b"
        if re.search(group_pattern, work):
            groups.append(dimension)
            work = re.sub(group_pattern, " ", work)
        if ranking and re.search(rf"\b(?:{pattern})\b", work):
            groups.append(dimension)
            work = re.sub(rf"\b(?:{pattern})\b", " ", work)
    if len(set(groups)) > 1:
        raise ClarificationRequired("Choose one grouping per chart: region, category, channel, status or month.")
    plan["dimension"] = groups[0] if groups else None
    if ranking and not plan["dimension"]:
        raise ClarificationRequired("Choose a grouping for the ranking, such as 'Top 3 categories by revenue'.")

    for key, aliases in FILTER_ALIASES.items():
        values = []
        for alias in sorted(aliases, key=len, reverse=True):
            # Consume only a label attached to this value. A global label
            # removal could erase another constraint such as 'all regions'.
            label = DIMENSION_ALIASES[key]
            pattern = rf"\b(?:(?:{label})\s*(?:(?:is|equals?)\s+|[=:]\s*)?)?{re.escape(alias)}\b(?:\s+(?:{label})\b)?"
            if re.search(pattern, work):
                values.append(aliases[alias])
                work = re.sub(pattern, " ", work)
        if len(set(values)) > 1:
            raise ClarificationRequired(f"Select a single {key} filter or group by {key} to see every value.")
        if values:
            plan["filters"][key] = values[0]

    requested_sort = None
    if explicit_sort:
        target, direction = explicit_sort
        target = {"sales": "revenue", "average order value": "average_order_value"}.get(target, target)
        if target == plan["metric"]:
            requested_sort = "value_asc" if direction == "ascending" else "value_desc"
        elif target == plan["dimension"]:
            if direction == "descending":
                raise ClarificationRequired("Dimension sorting supports ascending order. Use the query builder to choose a supported sort.")
            requested_sort = "dimension_asc"
        else:
            raise ClarificationRequired("Sort by the chosen metric or grouping. Sorting by another field is not supported.")

    ascending = bool(re.search(r"\b(?:ascending|lowest first|smallest first)\b", work))
    descending = bool(re.search(r"\b(?:descending|highest first|largest first)\b", work))
    if (ascending and descending) or (ranking and ((ranking[1] == "top" and ascending) or (ranking[1] == "bottom" and descending))) or (requested_sort and ((ascending and requested_sort != "value_asc") or (descending and requested_sort != "value_desc"))) or (ranking and requested_sort and requested_sort != plan["sort"]):
        raise ClarificationRequired("The requested ranking and sort conflict. Choose top with descending order or bottom with ascending order.")
    if ascending:
        plan["sort"] = "value_asc"
        work = re.sub(r"\b(?:ascending|lowest first|smallest first)\b", " ", work)
    if descending:
        plan["sort"] = "value_desc"
        work = re.sub(r"\b(?:descending|highest first|largest first)\b", " ", work)
    if requested_sort:
        plan["sort"] = requested_sort

    if re.search(r"\b(?:by|per|across)\b", work) and not ranking:
        raise ClarificationRequired("Specify a supported grouping after by, per or across: region, category, channel, status or month.")
    if re.search(r"\b(?:breakdown|distribution)\b", work) and not plan["dimension"]:
        raise ClarificationRequired("Choose a grouping for the breakdown: region, category, channel, status or month.")
    if re.search(r"\bwhere\b", work) and not plan["filters"] and not periods:
        raise ClarificationRequired("Specify a supported filter after where, or use the query builder.")

    # Only conversational scaffolding may remain. Business words, unexplained
    # numbers and unsupported constraints trigger clarification, never a guess.
    filler = r"\b(?:please|can|could|would|you|i|we|our|me|us|show|give|tell|find|get|see|want|know|what|whats|is|are|was|were|the|a|an|of|for|in|during|at|from|with|where|to|and|how|many|much|all|total|sum|count|number|there|do|have|has|by|breakdown|distribution|chart|graph|plot|visualize|display)\b"
    work = re.sub(filler, " ", work)
    if plan["dimension"] == "month":
        work = re.sub(r"\btrend\b", " ", work)
    remaining = re.sub(r"[\s,.?!:'\"]+", " ", work).strip()
    if remaining:
        raise ClarificationRequired("I could not map the whole question to the supported metrics and filters. Use the query builder or try one of the examples.")
    return validate_plan(plan)


def compile_plan(raw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """No input string can become an SQL identifier or expression."""
    plan = validate_plan(raw)
    dimension = plan["dimension"]
    expression = "substr(order_date, 1, 7)" if dimension == "month" else dimension
    columns = f"{expression} AS {dimension}, " if dimension else ""
    sql = f"SELECT {columns}{METRICS[plan['metric']]['sql']} AS value FROM analytics_orders"
    params: dict[str, Any] = {}
    conditions = []
    for key, value in plan["filters"].items():
        conditions.append(f"{key} = :filter_{key}")
        params[f"filter_{key}"] = value
    for bound, operation in (("date_from", ">="), ("date_to", "<=")):
        if plan[bound]:
            conditions.append(f"order_date {operation} :{bound}")
            params[bound] = plan[bound]
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    if dimension:
        sql += f" GROUP BY {expression}"
        if plan["sort"] == "dimension_asc":
            sql += f" ORDER BY {dimension} ASC"
        else:
            sql += f" ORDER BY value {'ASC' if plan['sort'] == 'value_asc' else 'DESC'}, {dimension} ASC"
    sql += " LIMIT :result_limit"
    params["result_limit"] = plan["limit"]
    return sql, params


def _read_authorizer(action: int, arg1: str, arg2: str, _db: str, _trigger: str) -> int:
    if action == sqlite3.SQLITE_SELECT:
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_READ and arg1 == "analytics_orders" and arg2 in {"", "order_date", "region", "category", "channel", "status", "amount_cents"}:
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_FUNCTION and arg2 in {"sum", "count", "avg", "round", "coalesce", "substr"}:
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


class AnalyticsEngine:
    """One immutable synthetic dataset per process; replace it only on restart.

    The bounded result cache uses canonical plans as keys because this owned
    dataset never changes while the service is running. It is not a cache for
    arbitrary or mutable customer databases.
    """
    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path).resolve()
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._lock = threading.Lock()
        self._row_count = 0
        self._ready = False

    def ensure_demo_data(self) -> None:
        """Create an owned, reproducible synthetic dataset once; never overwrite."""
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if self.database_path.exists():
                with closing(sqlite3.connect(self.database_path.as_uri() + "?mode=ro", uri=True)) as connection:
                    version = connection.execute("PRAGMA user_version").fetchone()[0]
                    application = connection.execute("PRAGMA application_id").fetchone()[0]
                    if version != DATA_VERSION or application != APPLICATION_ID:
                        raise RuntimeError("This file is not a compatible AIDA synthetic demo database. Configure a new demo database path.")
                    self._row_count = connection.execute("SELECT COUNT(*) FROM analytics_orders").fetchone()[0]
                self._ready = True
                return
            randomizer = random.Random(1729)
            rows = []
            categories = DIMENSIONS["category"]["values"]
            regions = DIMENSIONS["region"]["values"]
            channels = DIMENSIONS["channel"]["values"]
            for day_index in range(365):
                current = date(2025, 1, 1) + timedelta(days=day_index)
                daily_count = 14 + randomizer.randrange(12) + (7 if current.month >= 10 else 0)
                for _ in range(daily_count):
                    category = randomizer.choices(categories, weights=[30, 24, 22, 14, 10])[0]
                    region = randomizer.choices(regions, weights=[28, 23, 19, 30])[0]
                    channel = randomizer.choices(channels, weights=[58, 30, 12])[0]
                    status = randomizer.choices(DIMENSIONS["status"]["values"], weights=[80, 8, 7, 5])[0]
                    bases = {"Electronics": 21000, "Home": 12500, "Clothing": 6500, "Sports": 8500, "Beauty": 4200}
                    cents = bases[category] + randomizer.randrange(100, bases[category] * 2)
                    rows.append((len(rows) + 1, current.isoformat(), region, category, channel, status, cents))
            with closing(sqlite3.connect(self.database_path)) as connection, connection:
                connection.execute("CREATE TABLE analytics_orders (order_id INTEGER PRIMARY KEY, order_date TEXT NOT NULL, region TEXT NOT NULL, category TEXT NOT NULL, channel TEXT NOT NULL, status TEXT NOT NULL, amount_cents INTEGER NOT NULL CHECK(amount_cents >= 0))")
                connection.executemany("INSERT INTO analytics_orders VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
                connection.execute("CREATE INDEX ix_analytics_date ON analytics_orders(order_date)")
                connection.execute("CREATE INDEX ix_analytics_dimensions ON analytics_orders(region, category, channel, status)")
                connection.execute(f"PRAGMA user_version = {DATA_VERSION}")
                connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
            self._row_count = len(rows)
            self._ready = True

    def catalog(self) -> dict[str, Any]:
        return {
            "metrics": [{"id": key, **{k: v for k, v in value.items() if k != "sql"}} for key, value in METRICS.items()],
            "dimensions": [{"id": key, **copy.deepcopy(value)} for key, value in DIMENSIONS.items()],
            "filter_values": {key: value["values"][:] for key, value in DIMENSIONS.items() if "values" in value},
            "examples": EXAMPLES[:],
            "dataset": {"id": "synthetic-commerce-2025", "name": "Commerce demo", "synthetic": True, "date_from": DATE_FROM, "date_to": DATE_TO, "as_of": DATE_TO, "currency": "USD", "row_count": self._row_count},
            "privacy": {"external_requests": False, "model_calls": 0, "customer_data": False, "aggregate_only": True, "description": "Fixed synthetic data. Questions and results stay in this application; no LLM or external data service is called."},
            "capabilities": {"max_rows": MAX_ROWS, "query_timeout_seconds": MAX_SECONDS, "groupings_per_query": 1, "metrics_per_query": 1, "filters": "One equality value per region, category, channel and status", "relative_dates": f"Relative dates are anchored to the demo's as-of date, {DATE_TO}."},
        }

    def query(self, question: str | None = None, plan: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        meta = {"engine": "deterministic", "model_calls": 0, "estimated_cost_usd": 0, "estimated_model_cost_usd": 0, "synthetic": True, "as_of": DATE_TO, "cache_hit": False}
        try:
            if not self._ready:
                raise RuntimeError("Initialize the owned synthetic dataset before querying.")
            if question is not None and plan is not None:
                raise ClarificationRequired("Provide a question or a query plan, not both.")
            canonical = validate_plan(plan) if plan is not None else parse_question(question)
            key = hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()
            meta["execution_id"] = key[:16]
            with self._lock:
                cached = copy.deepcopy(self._cache.get(key))
            if cached is not None:
                meta["cache_hit"] = True
                payload = cached
            else:
                sql, parameters = compile_plan(canonical)
                deadline = time.monotonic() + MAX_SECONDS
                connection = sqlite3.connect(self.database_path.as_uri() + "?mode=ro", uri=True, timeout=MAX_SECONDS)
                try:
                    connection.row_factory = sqlite3.Row
                    connection.execute("PRAGMA query_only = ON")
                    connection.set_authorizer(_read_authorizer)
                    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                    data = [dict(row) for row in connection.execute(sql, parameters).fetchmany(MAX_ROWS + 1)]
                    if len(data) > MAX_ROWS:
                        raise RuntimeError("The aggregate result exceeded the row limit.")
                finally:
                    connection.close()
                dimension = canonical["dimension"]
                label = METRICS[canonical["metric"]]["label"]
                title = label + (f" by {dimension}" if dimension else "")
                explanation = METRICS[canonical["metric"]]["description"]
                if canonical["filters"]:
                    explanation += " Filters: " + ", ".join(f"{key} = {value}" for key, value in canonical["filters"].items()) + "."
                if canonical["date_from"] or canonical["date_to"]:
                    explanation += f" Dates: {canonical['date_from'] or 'start of dataset'} through {canonical['date_to'] or 'end of dataset'} (inclusive)."
                else:
                    explanation += f" Full dataset: {DATE_FROM} through {DATE_TO}."
                payload = {
                    "success": True, "data": data, "results": data,
                    "columns": ([dimension] if dimension else []) + ["value"],
                    "plan": canonical, "sql": sql, "parameters": parameters,
                    "chart": {"type": "line" if dimension == "month" else "bar" if dimension else "stat", "x": dimension, "y": "value", "title": title, "format": METRICS[canonical["metric"]]["format"]},
                    "explanation": explanation, "title": title,
                }
                with self._lock:
                    self._cache[key] = copy.deepcopy(payload)
                    self._cache.move_to_end(key)
                    while len(self._cache) > 128:
                        self._cache.popitem(last=False)
            meta["row_count"] = len(payload["data"])
            meta["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 2)
            meta["execution_time_ms"] = meta["elapsed_ms"]
            payload["meta"] = meta
            payload["metadata"] = meta
            return payload
        except ClarificationRequired as exc:
            meta["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 2)
            return {"success": False, "data": [], "results": [], "error": str(exc), "error_type": "clarification_required", "suggestions": EXAMPLES[:4], "meta": meta, "metadata": meta}
        except (sqlite3.Error, OSError, RuntimeError):
            meta["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 2)
            return {"success": False, "data": [], "results": [], "error": "The local demo database could not complete the query. Check the backend startup and database configuration.", "error_type": "execution_error", "meta": meta, "metadata": meta}
