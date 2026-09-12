"""Deterministic aggregate compiler for explicitly approved SQLite catalogs."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from datetime import date
from pathlib import Path
from typing import Any

from .analytics import ClarificationRequired, MAX_ROWS, MAX_SECONDS
from .sources import quote_identifier


class CatalogEngine:
    def __init__(self, database_path: Path, manifest: dict[str, Any], source_id: str, synthetic: bool = False):
        self.database_path = Path(database_path).resolve()
        self.manifest = copy.deepcopy(manifest)
        self.source_id = source_id
        self.synthetic = synthetic
        self.metrics = {metric["id"]: metric for metric in self.manifest["metrics"]}
        self.dimensions = {dimension["id"]: dimension for dimension in self.manifest["dimensions"]}
        if self.manifest.get("date_column"):
            self.dimensions["month"] = {"id": "month", "label": "Month", "column": self.manifest["date_column"]}
        self._lock = threading.Lock()
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.catalog_version = hashlib.sha256(json.dumps(self.manifest, sort_keys=True).encode()).hexdigest()[:16]

    def catalog(self) -> dict[str, Any]:
        manifest = self.manifest
        return {
            "metrics": [{key: value for key, value in metric.items() if key in {"id", "label", "description", "format"}} for metric in self.metrics.values()],
            "dimensions": [{key: copy.deepcopy(value) for key, value in dimension.items() if key in {"id", "label", "values", "aliases"}} for dimension in self.dimensions.values()],
            "filter_values": {key: copy.deepcopy(value["values"]) for key, value in self.dimensions.items() if "values" in value},
            "examples": manifest["examples"] or [f"{metric['label']}" for metric in self.metrics.values()],
            "dataset": {"id": self.source_id, "name": manifest["name"], "synthetic": self.synthetic, "as_of": manifest["as_of"], "date_from": manifest["date_from"], "date_to": manifest["date_to"], "currency": manifest["currency"], "catalog_version": self.catalog_version},
            "privacy": {"aggregate_only": True, "customer_data": not self.synthetic, "description": "The database and query results stay on this server. Only owner-approved catalog labels are available to semantic interpretation; no row samples are included."},
            "capabilities": {"max_rows": MAX_ROWS, "query_timeout_seconds": MAX_SECONDS, "groupings_per_query": 1, "metrics_per_query": 1, "filters": "One equality value per approved dimension", "relative_dates": f"Relative dates use the approved reference date {manifest['as_of']}." if manifest["as_of"] else "No date mapping has been approved."},
        }

    def validate_plan(self, raw: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) - {"metric", "dimension", "filters", "date_from", "date_to", "sort", "limit"}:
            raise ClarificationRequired("Use a structured query plan with metric, dimension, filters, dates, sort and limit.")
        metric = raw.get("metric")
        if not isinstance(metric, str) or metric not in self.metrics:
            raise ClarificationRequired("Choose a metric approved for the selected source.")
        dimension = raw.get("dimension")
        if dimension is not None and (not isinstance(dimension, str) or dimension not in self.dimensions):
            raise ClarificationRequired("Choose a dimension approved for the selected source.")
        filters = raw.get("filters", {})
        if not isinstance(filters, dict) or len(filters) > 20:
            raise ClarificationRequired("Filters must contain one equality value per approved dimension.")
        canonical_filters = {}
        for key, value in filters.items():
            if key not in self.dimensions or key == "month" or not isinstance(value, str) or not value.strip() or len(value) > 150 or any(ord(c) < 32 for c in value):
                raise ClarificationRequired("Use a bounded string filter on an approved categorical dimension.")
            values = self.dimensions[key].get("values")
            normalized = {**{item.casefold(): item for item in values}, **self.dimensions[key].get("aliases", {})}.get(value.strip().casefold()) if values else value.strip()
            if normalized is None:
                raise ClarificationRequired(f"Choose an approved value for {self.dimensions[key]['label']}.")
            canonical_filters[key] = normalized
        dates = {}
        for key in ("date_from", "date_to"):
            value = raw.get(key)
            if value is not None:
                if not self.manifest.get("date_column"):
                    raise ClarificationRequired("No date column is approved for this source.")
                if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    raise ClarificationRequired("Dates must use YYYY-MM-DD.")
                try:
                    date.fromisoformat(value)
                except ValueError as exc:
                    raise ClarificationRequired("Use a valid calendar date.") from exc
            dates[key] = value
        if dates["date_from"] and dates["date_to"] and dates["date_from"] > dates["date_to"]:
            raise ClarificationRequired("Start date must precede the end date.")
        limit = raw.get("limit", MAX_ROWS)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_ROWS:
            raise ClarificationRequired("Result limit must be a whole number from 1 to 100.")
        sort = raw.get("sort", "dimension_asc" if dimension == "month" else "value_desc")
        if not isinstance(sort, str) or sort not in {"value_desc", "value_asc", "dimension_asc"}:
            raise ClarificationRequired("Choose value_desc, value_asc or dimension_asc ordering.")
        return {"metric": metric, "dimension": dimension, "filters": dict(sorted(canonical_filters.items())), **dates, "sort": sort, "limit": limit}

    def compile_plan(self, raw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        plan = self.validate_plan(raw)
        metric = self.metrics[plan["metric"]]
        parameters: dict[str, Any] = {"result_limit": plan["limit"]}
        operand = quote_identifier(metric["column"]) if "column" in metric else "1"
        conditions = []
        for index, (column, value) in enumerate(metric["where"].items()):
            conditions.append(f"{quote_identifier(column)} = :metric_{index}")
            parameters[f"metric_{index}"] = value
        if conditions:
            operand = f"CASE WHEN {' AND '.join(conditions)} THEN {operand} END"
        expression = f"{metric['aggregate']}({operand})"
        if metric["aggregate"] in {"COUNT", "SUM"}:
            expression = f"COALESCE({expression}, 0)"
        if metric["scale"] != 1:
            expression = f"({expression} / :metric_scale)"
            parameters["metric_scale"] = float(metric["scale"])
        if metric["format"] == "currency":
            expression = f"ROUND({expression}, 2)"
        dimension = plan["dimension"]
        group = None
        if dimension:
            group = quote_identifier(self.dimensions[dimension]["column"])
            if dimension == "month":
                group = f"substr({group}, 1, 7)"
        select = f"{group} AS {quote_identifier(dimension)}, " if dimension else ""
        select += f"{expression} AS value"
        # SQLite has dynamic types: refuse selected groups with text masquerading
        # as a numeric measure instead of silently summing it as zero.
        if "column" in metric:
            select += f", SUM(CASE WHEN ({operand}) IS NOT NULL AND typeof(({operand})) NOT IN ('integer','real') THEN 1 ELSE 0 END) AS internal_invalid"
        sql = f"SELECT {select} FROM {quote_identifier(self.manifest['table'])}"
        clauses = []
        for index, (key, value) in enumerate(plan["filters"].items()):
            clauses.append(f"{quote_identifier(self.dimensions[key]['column'])} = :filter_{index}")
            parameters[f"filter_{index}"] = value
        for key, operator in (("date_from", ">="), ("date_to", "<=")):
            if plan[key]:
                clauses.append(f"substr({quote_identifier(self.manifest['date_column'])}, 1, 10) {operator} :{key}")
                parameters[key] = plan[key]
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        if group:
            sql += f" GROUP BY {group}"
            sql += f" ORDER BY {quote_identifier(dimension)} ASC" if plan["sort"] == "dimension_asc" else f" ORDER BY value {'ASC' if plan['sort'] == 'value_asc' else 'DESC'}, {quote_identifier(dimension)} ASC"
        sql += " LIMIT :result_limit"
        return sql, parameters

    def _authorizer(self, action: int, arg1: str, arg2: str, database: str, trigger: str) -> int:
        columns = {value["column"] for value in self.dimensions.values()}
        columns.update(value["column"] for value in self.metrics.values() if "column" in value)
        columns.update(column for value in self.metrics.values() for column in value["where"])
        if self.manifest.get("date_column"):
            columns.add(self.manifest["date_column"])
        if action == sqlite3.SQLITE_SELECT:
            return sqlite3.SQLITE_OK
        # SQLite emits database=None for its special table-only COUNT read.
        if action == sqlite3.SQLITE_READ and arg1 == self.manifest["table"] and arg2 in columns | {""} and (database == "main" or (database is None and arg2 == "")) and not trigger:
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_FUNCTION and arg2 in {"count", "sum", "avg", "min", "max", "round", "coalesce", "substr", "typeof", "length", "date", "datetime"}:
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY

    def query(self, question: str | None = None, plan: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        meta = {"engine": "deterministic", "source_id": self.source_id, "model_calls": 0, "estimated_cost_usd": 0, "estimated_model_cost_usd": 0, "synthetic": self.synthetic, "as_of": self.manifest["as_of"], "cache_hit": False}
        try:
            if question is not None or plan is None:
                raise ClarificationRequired("Language interpretation must produce an approved query plan before execution.")
            canonical = self.validate_plan(plan)
            key = hashlib.sha256(json.dumps({"source": self.source_id, "catalog": self.catalog_version, "plan": canonical}, sort_keys=True).encode()).hexdigest()
            meta["execution_id"] = key[:16]
            with self._lock:
                payload = copy.deepcopy(self._cache.get(key))
            if payload is not None:
                meta["cache_hit"] = True
            else:
                sql, parameters = self.compile_plan(canonical)
                deadline = time.monotonic() + MAX_SECONDS
                connection = sqlite3.connect(self.database_path.as_uri() + "?mode=ro&immutable=1", uri=True, timeout=MAX_SECONDS)
                try:
                    connection.row_factory = sqlite3.Row
                    connection.execute("PRAGMA query_only=ON")
                    connection.execute("PRAGMA trusted_schema=OFF")
                    connection.set_authorizer(self._authorizer)
                    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                    if canonical["dimension"] == "month" or canonical["date_from"] or canonical["date_to"]:
                        # ISO calendar dates and ISO timestamps share their first
                        # ten characters. Reject malformed formats instead of
                        # silently filtering out locale-formatted customer dates.
                        date_column = quote_identifier(self.manifest["date_column"])
                        invalid_date_sql = (
                            f"SELECT COUNT(1) FROM {quote_identifier(self.manifest['table'])} "
                            f"WHERE {date_column} IS NOT NULL AND ("
                            f"typeof({date_column}) != 'text' OR length({date_column}) < 10 OR "
                            f"date(substr({date_column}, 1, 10)) IS NULL OR "
                            f"date(substr({date_column}, 1, 10)) != substr({date_column}, 1, 10) OR "
                            f"(length({date_column}) > 10 AND (substr({date_column}, 11, 1) NOT IN ('T',' ') OR datetime({date_column}) IS NULL)))"
                        )
                        if connection.execute(invalid_date_sql).fetchone()[0]:
                            raise ClarificationRequired("The approved date column must contain ISO YYYY-MM-DD dates or ISO timestamps. Normalize the source dates before filtering or grouping by month.")
                    data = [dict(row) for row in connection.execute(sql, parameters).fetchmany(MAX_ROWS + 1)]
                finally:
                    connection.close()
                if len(data) > MAX_ROWS:
                    raise RuntimeError("The result exceeded its row bound.")
                for row in data:
                    if row.pop("internal_invalid", 0):
                        raise ClarificationRequired("The approved numeric column contains nonnumeric values. Clean the reporting source before querying this metric.")
                    if isinstance(row.get("value"), float) and not math.isfinite(row["value"]):
                        raise ClarificationRequired("The metric exceeded its numeric range. Correct the source values or metric scale.")
                metric = self.metrics[canonical["metric"]]
                dimension = canonical["dimension"]
                title = metric["label"] + (f" by {self.dimensions[dimension]['label']}" if dimension else "")
                explanation = metric["description"]
                if canonical["filters"]:
                    explanation += " Filters: " + ", ".join(f"{self.dimensions[k]['label']} = {v}" for k, v in canonical["filters"].items()) + "."
                if canonical["date_from"] or canonical["date_to"]:
                    explanation += f" Dates: {canonical['date_from'] or 'start of source'} through {canonical['date_to'] or 'end of source'}, inclusive."
                payload = {"success": True, "data": data, "results": data, "columns": ([dimension] if dimension else []) + ["value"], "plan": canonical, "sql": sql, "parameters": parameters, "chart": {"type": "line" if dimension == "month" else "bar" if dimension else "stat", "x": dimension, "y": "value", "title": title, "format": metric["format"]}, "explanation": explanation, "title": title}
                with self._lock:
                    self._cache[key] = copy.deepcopy(payload)
                    while len(self._cache) > 128:
                        self._cache.popitem(last=False)
            meta["row_count"] = len(payload["data"])
        except ClarificationRequired as exc:
            payload = {"success": False, "data": [], "results": [], "error": str(exc), "error_type": "clarification_required", "suggestions": self.catalog()["examples"][:4]}
        except (sqlite3.Error, OSError, RuntimeError):
            payload = {"success": False, "data": [], "results": [], "error": "The selected local database could not complete the bounded read-only query. Check its approved schema and source data.", "error_type": "execution_error"}
        meta["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 2)
        meta["execution_time_ms"] = meta["elapsed_ms"]
        payload["meta"] = meta
        payload["metadata"] = meta
        return payload
