"""Bounded relational plans compiled from owner-approved metadata, never model SQL.

Measures live at one fact grain. Only proven many-to-one dimension joins are
allowed; child populations are expressed through EXISTS, so they cannot multiply
measures. The same compiler works for any approved SQLite snapshot.
"""
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
from .sources import SourceError, inspect_database, quote_identifier

_ID = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_OPS = {"eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "in": "IN"}
_AGGREGATES = {"COUNT", "COUNT_DISTINCT", "SUM", "AVG", "MIN", "MAX"}


def _label(value: Any, name: str, maximum: int = 500) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise SourceError(f"Provide a bounded {name}.")
    return value.strip()


def _id(value: Any) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value) or value.startswith("internal_"):
        raise SourceError("Catalog identifiers must be lowercase words with optional digits and underscores.")
    return value


def _calendar(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise SourceError("Dates must use YYYY-MM-DD.")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise SourceError("Provide a valid calendar date.") from exc
    return value


def validate_relational_manifest(config: dict[str, Any], tables_metadata: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        return _validate_relational_manifest(config, tables_metadata)
    except SourceError:
        raise
    except (TypeError, ValueError, KeyError, AttributeError) as exc:
        raise SourceError("Use well-typed values in the bounded relational catalog contract.") from exc


def _validate_relational_manifest(config: dict[str, Any], tables_metadata: list[dict[str, Any]]) -> dict[str, Any]:
    """Approve physical mappings and prove every dimension edge cannot fan out."""
    allowed = {"version", "name", "fact", "tables", "relations", "metrics", "dimensions", "fields", "date", "exists_relations", "as_of", "currency", "date_from", "date_to", "examples"}
    if not isinstance(config, dict) or config.get("version") != 2 or set(config) - allowed:
        raise SourceError("Use the version 2 relational catalog contract.")
    schema = {table["name"]: table for table in tables_metadata}

    def table_name(raw: Any) -> str:
        if not isinstance(raw, str) or raw not in schema:
            raise SourceError("Every catalog table must exist in the inspected snapshot.")
        return raw

    def column(table: str, raw: Any, *, sensitive: bool = False, numeric: bool = False) -> str:
        metadata = next((item for item in schema[table]["columns"] if item["name"] == raw), None)
        if metadata is None:
            raise SourceError("Every catalog column must exist in its explicitly mapped table.")
        if metadata.get("sensitive") and not sensitive:
            raise SourceError("Identifiers and personal or free-text columns cannot be exposed as dimensions or filters.")
        if numeric and not metadata.get("numeric"):
            raise SourceError("Numeric measures and fields require numeric declared column types.")
        return raw

    def affinity(table: str, name: str) -> str:
        declared = next(item["type"] for item in schema[table]["columns"] if item["name"] == name).upper()
        if "INT" in declared:
            return "INTEGER"
        if any(token in declared for token in ("CHAR", "CLOB", "TEXT")):
            return "TEXT"
        if not declared or "BLOB" in declared:
            return "BLOB"
        if any(token in declared for token in ("REAL", "FLOA", "DOUB")):
            return "REAL"
        return "NUMERIC"

    fact = config.get("fact")
    if not isinstance(fact, dict) or set(fact) - {"table", "key", "columns", "archive_table"}:
        raise SourceError("Approve a fact table, key and aligned fact columns.")
    fact_table = table_name(fact.get("table"))
    fact_key = column(fact_table, fact.get("key"), sensitive=True)
    raw_columns = fact.get("columns")
    if not isinstance(raw_columns, list) or not 1 <= len(raw_columns) <= 40 or len(set(raw_columns)) != len(raw_columns):
        raise SourceError("Approve between 1 and 40 distinct fact columns.")
    fact_columns = [column(fact_table, item, sensitive=True) for item in raw_columns]
    if fact_key not in fact_columns:
        raise SourceError("The fact grain key must be part of the approved fact population.")
    normalized_fact = {"table": fact_table, "key": fact_key, "columns": fact_columns}
    if fact.get("archive_table"):
        archive = table_name(fact["archive_table"])
        if archive == fact_table:
            raise SourceError("A union population must use a separate approved table.")
        for item in fact_columns:
            column(archive, item, sensitive=True)
            primary_type = next(c["type"] for c in schema[fact_table]["columns"] if c["name"] == item)
            archive_type = next(c["type"] for c in schema[archive]["columns"] if c["name"] == item)
            if primary_type.upper() != archive_type.upper():
                raise SourceError("Union branches must have matching declared column types and aligned names.")
        normalized_fact["archive_table"] = archive
    raw_tables = config.get("tables", {})
    if not isinstance(raw_tables, dict) or len(raw_tables) > 12 or "fact" in raw_tables:
        raise SourceError("Approve at most twelve named dimension tables; fact is reserved.")
    tables = {}
    physical = {"fact": fact_table}
    for key, raw in raw_tables.items():
        _id(key)
        if not isinstance(raw, dict) or set(raw) != {"table"}:
            raise SourceError("Dimension tables contain a physical table mapping only.")
        tables[key] = {"table": table_name(raw["table"])}
        physical[key] = tables[key]["table"]
    if len(set(physical.values())) != len(physical):
        raise SourceError("Each physical table has one logical role in this bounded catalog.")
    used_columns: dict[str, set[str]] = {table: set() for table in schema}
    used_columns[fact_table].add(fact_key)

    def mapped_column(table: Any, name: Any, **kwargs: Any) -> str:
        if not isinstance(table, str) or table not in physical:
            raise SourceError("Choose an approved logical table mapping.")
        result = column(physical[table], name, **kwargs)
        used_columns[physical[table]].add(result)
        if table == "fact" and result not in fact_columns:
            raise SourceError("Fact mappings must reference approved population columns.")
        return result

    relations = config.get("relations", [])
    if not isinstance(relations, list) or len(relations) > 12:
        raise SourceError("Approve at most twelve many-to-one relationships.")
    normalized_relations, incoming, relation_ids = [], {}, set()
    for raw in relations:
        if not isinstance(raw, dict) or set(raw) != {"id", "from", "from_column", "to", "to_column", "kind"} or raw["kind"] != "many_to_one":
            raise SourceError("Only explicit many-to-one relation mappings are supported.")
        key = _id(raw["id"])
        if key in relation_ids or raw["to"] == "fact" or raw["to"] in incoming or raw["from"] == raw["to"]:
            raise SourceError("Ambiguous, cyclic and reverse fanout joins are not allowed.")
        relation_ids.add(key)
        left = mapped_column(raw["from"], raw["from_column"], sensitive=True)
        right = mapped_column(raw["to"], raw["to_column"], sensitive=True)
        if affinity(physical[raw["from"]], left) != affinity(physical[raw["to"]], right):
            raise SourceError("Join keys must have matching SQLite type affinities so coercion cannot multiply fact rows.")
        target = schema[physical[raw["to"]]]
        primary_keys = [item["name"] for item in target["columns"] if item.get("primary_key")]
        unique_keys = target.get("unique_keys", [])
        if primary_keys != [right] and [right] not in unique_keys:
            raise SourceError("A many-to-one target must have a proven single-column primary or unique key.")
        normalized_relations.append({**raw, "from_column": left, "to_column": right})
        incoming[raw["to"]] = raw["from"]
    for key in tables:
        visited, node = set(), key
        while node != "fact":
            if node in visited or node not in incoming:
                raise SourceError("Every dimension table needs exactly one acyclic join path from the fact grain.")
            visited.add(node)
            node = incoming[node]
    identifiers: set[str] = set()

    def unique_id(raw: Any) -> str:
        key = _id(raw)
        if key in identifiers:
            raise SourceError("Metric, dimension and field identifiers must be distinct.")
        identifiers.add(key)
        return key

    metrics = config.get("metrics")
    if not isinstance(metrics, list) or not 1 <= len(metrics) <= 20:
        raise SourceError("Approve between one and twenty metrics.")
    normalized_metrics = []
    for raw in metrics:
        if not isinstance(raw, dict) or set(raw) - {"id", "label", "description", "aggregate", "table", "column", "scale", "format"}:
            raise SourceError("Unsupported relational metric definition.")
        aggregate = raw.get("aggregate")
        if aggregate not in _AGGREGATES or raw.get("table", "fact") != "fact":
            raise SourceError("Measures must use approved aggregates at the fact grain; parent measures would fan out.")
        item = {"id": unique_id(raw.get("id")), "label": _label(raw.get("label"), "metric label", 80), "description": _label(raw.get("description"), "metric definition"), "aggregate": aggregate, "table": "fact"}
        if aggregate != "COUNT":
            item["column"] = mapped_column("fact", raw.get("column"), sensitive=aggregate == "COUNT_DISTINCT", numeric=aggregate != "COUNT_DISTINCT")
        elif raw.get("column") is not None:
            raise SourceError("COUNT counts fact rows; use COUNT_DISTINCT for distinct entities.")
        scale = raw.get("scale", 1)
        if isinstance(scale, bool) or not isinstance(scale, (float, int)) or not math.isfinite(scale) or not 0 < scale <= 1e9:
            raise SourceError("Metric scales must be finite positive divisors up to one billion.")
        format_ = raw.get("format", "number")
        if format_ not in {"number", "currency", "percent"}:
            raise SourceError("Choose a numeric, currency or percent format.")
        item.update(scale=scale, format=format_)
        normalized_metrics.append(item)

    def field(raw: Any, child_table: str | None = None) -> dict[str, Any]:
        allowed_fields = {"id", "label", "column", "type", "values", "aliases"} | ({"table"} if child_table is None else set())
        if not isinstance(raw, dict) or set(raw) - allowed_fields:
            raise SourceError("Unsupported approved field definition.")
        type_ = raw.get("type", "string")
        if type_ not in {"string", "number"}:
            raise SourceError("Approved fields are categorical strings or numeric measures.")
        item = {"id": unique_id(raw.get("id")), "label": _label(raw.get("label"), "field label", 80), "type": type_}
        if child_table is None:
            item["table"] = raw.get("table")
            item["column"] = mapped_column(item["table"], raw.get("column"), numeric=type_ == "number")
        else:
            item["column"] = column(child_table, raw.get("column"), numeric=type_ == "number")
            used_columns[child_table].add(item["column"])
        if "values" in raw:
            values = raw["values"]
            if type_ != "string" or not isinstance(values, list) or not 1 <= len(values) <= 50:
                raise SourceError("Approve one to fifty categorical values, or omit the value allowlist.")
            item["values"] = [_label(v, "categorical value", 150) for v in values]
            if len({v.casefold() for v in item["values"]}) != len(values):
                raise SourceError("Approved categorical values must be distinct.")
        if "aliases" in raw:
            aliases = raw["aliases"]
            if not isinstance(aliases, dict) or len(aliases) > 80 or "values" not in item:
                raise SourceError("Aliases must map bounded phrases to approved categorical values.")
            item["aliases"] = {_label(k, "alias", 150).casefold(): v for k, v in aliases.items()}
            if any(v not in item["values"] for v in item["aliases"].values()):
                raise SourceError("Aliases must resolve to approved categorical values.")
        return item

    dimensions, fields = [], []
    for name, output in (("dimensions", dimensions), ("fields", fields)):
        values = config.get(name, [])
        if not isinstance(values, list) or len(values) > 20:
            raise SourceError("Approve at most twenty dimensions or row-filter fields.")
        output.extend(field(raw) for raw in values)
    approved_date = config.get("date")
    if approved_date is not None:
        if not isinstance(approved_date, dict) or set(approved_date) != {"table", "column"}:
            raise SourceError("Approve one explicit reporting date mapping.")
        approved_date = {"table": approved_date["table"], "column": mapped_column(approved_date["table"], approved_date["column"])}
        if "month" in identifiers:
            raise SourceError("Month is reserved for the approved reporting date.")
    child_relations = config.get("exists_relations", [])
    if not isinstance(child_relations, list) or len(child_relations) > 5:
        raise SourceError("Approve at most five related child populations.")
    normalized_children = []
    for raw in child_relations:
        if not isinstance(raw, dict) or set(raw) != {"id", "label", "table", "parent", "parent_column", "child_column", "fields"}:
            raise SourceError("Approve explicit parent and child key mappings for EXISTS.")
        key = _id(raw["id"])
        if key in relation_ids:
            raise SourceError("Relationship identifiers must be distinct.")
        relation_ids.add(key)
        child_table = table_name(raw["table"])
        parent_key = mapped_column(raw["parent"], raw["parent_column"], sensitive=True)
        child_key = column(child_table, raw["child_column"], sensitive=True)
        if affinity(physical[raw["parent"]], parent_key) != affinity(child_table, child_key):
            raise SourceError("Related parent and child keys must have matching SQLite type affinities.")
        used_columns[child_table].add(child_key)
        if not isinstance(raw["fields"], list) or len(raw["fields"]) > 10:
            raise SourceError("Approve at most ten fields per child population.")
        normalized_children.append({"id": key, "label": _label(raw["label"], "related population label", 80), "table": child_table, "parent": raw["parent"], "parent_column": parent_key, "child_column": child_key, "fields": [field(item, child_table) for item in raw["fields"]]})
    if set(fact_columns) != used_columns[fact_table]:
        raise SourceError("Every fact population column must have an explicit measure, field, date or relationship purpose.")
    examples = config.get("examples", [])
    if not isinstance(examples, list) or len(examples) > 30:
        raise SourceError("Provide at most thirty approved starter questions.")
    manifest = {"version": 2, "name": _label(config.get("name"), "source name", 80), "fact": normalized_fact, "tables": tables, "relations": normalized_relations, "metrics": normalized_metrics, "dimensions": dimensions, "fields": fields, "date": approved_date, "exists_relations": normalized_children, "examples": [_label(e, "starter question", 300) for e in examples], "currency": _label(config.get("currency", "USD"), "currency", 8)}
    manifest.update({key: _calendar(config.get(key)) for key in ("as_of", "date_from", "date_to")})
    if manifest["date_from"] and manifest["date_to"] and manifest["date_from"] > manifest["date_to"]:
        raise SourceError("Catalog date bounds must be chronological.")
    return manifest


class RelationalEngine:
    def __init__(self, database_path: Path, manifest: dict[str, Any], source_id: str, synthetic: bool = False):
        self.database_path = Path(database_path).resolve()
        self.manifest = validate_relational_manifest(manifest, inspect_database(self.database_path))
        self.source_id, self.synthetic = source_id, synthetic
        self.metrics = {item["id"]: item for item in self.manifest["metrics"]}
        self.dimensions = {item["id"]: item for item in self.manifest["dimensions"]}
        if self.manifest["date"]:
            self.dimensions["month"] = {"id": "month", "label": "Month", "type": "string", **self.manifest["date"]}
        self.fields = {**self.dimensions, **{item["id"]: item for item in self.manifest["fields"]}}
        self.children = {item["id"]: item for item in self.manifest["exists_relations"]}
        self.physical = {"fact": self.manifest["fact"]["table"], **{key: value["table"] for key, value in self.manifest["tables"].items()}}
        self.incoming = {edge["to"]: edge for edge in self.manifest["relations"]}
        self.aliases = {key: f"t{index}" for index, key in enumerate(self.physical)}
        self.catalog_version = hashlib.sha256(json.dumps(self.manifest, sort_keys=True).encode()).hexdigest()[:16]
        self._lock = threading.Lock()
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._allowed: dict[str, set[str]] = {table: set() for table in self.physical.values()}
        self._allowed[self.physical["fact"]].update(self.manifest["fact"]["columns"])
        archive = self.manifest["fact"].get("archive_table")
        if archive:
            self._allowed[archive] = set(self.manifest["fact"]["columns"])
        for item in self.fields.values():
            self._allowed[self.physical[item["table"]]].add(item["column"])
        for edge in self.manifest["relations"]:
            self._allowed[self.physical[edge["from"]]].add(edge["from_column"])
            self._allowed[self.physical[edge["to"]]].add(edge["to_column"])
        for item in self.children.values():
            self._allowed[self.physical[item["parent"]]].add(item["parent_column"])
            self._allowed.setdefault(item["table"], set()).update({item["child_column"]} | {field["column"] for field in item["fields"]})

    def catalog(self) -> dict[str, Any]:
        def public(item: dict[str, Any]) -> dict[str, Any]:
            return {key: copy.deepcopy(value) for key, value in item.items() if key in {"id", "label", "description", "format", "type", "values", "aliases"}}
        manifest = self.manifest
        public_metrics = [{**public(metric), "additive": metric["aggregate"] in {"SUM", "COUNT"}} for metric in self.metrics.values()]
        populations = [{"id": "primary", "label": "Current records", "description": "The primary fact table only."}]
        if manifest["fact"].get("archive_table"):
            populations.append({"id": "all", "label": "Current and archived records", "description": "Combine aligned current and archive fact rows. UNION ALL keeps every row; UNION removes exact duplicate fact rows."})
        return {"metrics": public_metrics, "dimensions": [public(d) for d in self.dimensions.values()], "fields": [public(f) for f in manifest["fields"]], "filter_values": {key: value["values"] for key, value in self.fields.items() if "values" in value}, "exists_relations": [{"id": child["id"], "label": child["label"], "fields": [public(f) for f in child["fields"]]} for child in self.children.values()], "populations": populations, "examples": copy.deepcopy(manifest["examples"]), "dataset": {"id": self.source_id, "name": manifest["name"], "synthetic": self.synthetic, "as_of": manifest["as_of"], "date_from": manifest["date_from"], "date_to": manifest["date_to"], "currency": manifest["currency"], "catalog_version": self.catalog_version}, "privacy": {"aggregate_only": True, "customer_data": not self.synthetic, "description": "The local model receives approved semantic labels, never physical tables, customer rows, or query results."}, "capabilities": {"relational": True, "max_rows": MAX_ROWS, "query_timeout_seconds": MAX_SECONDS, "metrics_per_query": 3, "groupings_per_query": 2, "filters": "AND comparisons on approved fields, including IN; numeric aggregate HAVING; correlated EXISTS and NOT EXISTS; above-average grouped aggregates.", "set_operations": ["union_all", "union"] if len(populations) > 1 else [], "relative_dates": f"Dates use {manifest['as_of']} as the reference." if manifest["as_of"] else "No relative-date reference approved."}}

    def _filters(self, raw: Any, fields: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(raw, list) or len(raw) > 12:
            raise ClarificationRequired("Use at most twelve approved row filters.")
        result = []
        for item in raw:
            if not isinstance(item, dict) or set(item) != {"field", "op", "value"} or item.get("field") not in fields or item.get("op") not in _OPS:
                raise ClarificationRequired("Filters must select an approved field, operator and value.")
            field = fields[item["field"]]
            if item["field"] == "month":
                raise ClarificationRequired("Use the date range to filter reporting dates.")
            operator, value = item["op"], item["value"]
            if operator not in {"eq", "ne", "in"} and field["type"] != "number":
                raise ClarificationRequired("Ordering comparisons require an approved numeric field.")
            values = value if operator == "in" else [value]
            if not isinstance(values, list) or not 1 <= len(values) <= 20:
                raise ClarificationRequired("IN filters require between one and twenty scalar values.")
            canonical = []
            for candidate in values:
                if field["type"] == "number":
                    if isinstance(candidate, bool) or not isinstance(candidate, (int, float)) or not math.isfinite(candidate) or abs(candidate) > 1e15:
                        raise ClarificationRequired("Numeric filters require bounded finite numbers.")
                else:
                    if not isinstance(candidate, str) or not candidate.strip() or len(candidate) > 150 or any(ord(c) < 32 for c in candidate):
                        raise ClarificationRequired("Categorical filters require bounded nonempty strings.")
                    candidate = candidate.strip()
                    if "values" in field:
                        candidate = {**{v.casefold(): v for v in field["values"]}, **field.get("aliases", {})}.get(candidate.casefold())
                        if candidate is None:
                            raise ClarificationRequired(f"Choose an approved value for {field['label']}.")
                if candidate not in canonical:
                    canonical.append(candidate)
            result.append({"field": item["field"], "op": operator, "value": canonical if operator == "in" else canonical[0]})
        return result

    def validate_plan(self, raw: dict[str, Any]) -> dict[str, Any]:
        try:
            return self._validate_plan(raw)
        except ClarificationRequired:
            raise
        except (TypeError, ValueError, KeyError, AttributeError) as exc:
            raise ClarificationRequired("Use well-typed values in the bounded relational plan contract.") from exc

    def _validate_plan(self, raw: dict[str, Any]) -> dict[str, Any]:
        allowed = {"version", "metrics", "dimensions", "filters", "having", "population", "set_operation", "exists", "comparison", "date_from", "date_to", "sort", "limit"}
        if not isinstance(raw, dict) or raw.get("version") != 2 or set(raw) - allowed:
            raise ClarificationRequired("Use a version 2 structured relational plan.")
        selected = {}
        for name, mapping, minimum, maximum in (("metrics", self.metrics, 1, 3), ("dimensions", self.dimensions, 0, 2)):
            values = raw.get(name, [])
            if not isinstance(values, list) or not minimum <= len(values) <= maximum or any(not isinstance(v, str) or v not in mapping for v in values) or len(set(values)) != len(values):
                raise ClarificationRequired(f"Choose {minimum} to {maximum} distinct approved {name}.")
            selected[name] = list(values)
        filters = self._filters(raw.get("filters", []), self.fields)
        having = raw.get("having", [])
        if not isinstance(having, list) or len(having) > 3:
            raise ClarificationRequired("Use at most three aggregate thresholds.")
        for item in having:
            if not isinstance(item, dict) or set(item) != {"metric", "op", "value"} or item.get("metric") not in selected["metrics"] or item.get("op") not in _OPS or item.get("op") == "in":
                raise ClarificationRequired("HAVING must compare a selected metric with a numeric threshold.")
            value = item["value"]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or abs(value) > 1e15:
                raise ClarificationRequired("Aggregate thresholds must be bounded finite numbers.")
        population, operation = raw.get("population", "primary"), raw.get("set_operation", "union_all")
        if population not in {"primary", "all"} or operation not in {"union_all", "union"} or (population == "all" and not self.manifest["fact"].get("archive_table")) or (population == "primary" and operation != "union_all"):
            raise ClarificationRequired("Choose an approved primary or current-and-archive fact population.")
        exists = raw.get("exists")
        if exists is not None:
            if not isinstance(exists, dict) or set(exists) != {"relation", "negate", "filters"} or exists.get("relation") not in self.children or not isinstance(exists.get("negate"), bool):
                raise ClarificationRequired("Choose an approved related population for EXISTS or NOT EXISTS.")
            child = self.children[exists["relation"]]
            exists = {"relation": exists["relation"], "negate": exists["negate"], "filters": self._filters(exists["filters"], {f["id"]: f for f in child["fields"]})}
        comparison = raw.get("comparison")
        if comparison is not None:
            if not isinstance(comparison, dict) or set(comparison) != {"kind", "metric"} or comparison.get("kind") != "above_average" or comparison.get("metric") not in selected["metrics"] or not selected["dimensions"] or having:
                raise ClarificationRequired("Above-average compares grouped metric totals across all filtered groups; select a grouping and omit HAVING.")
        dates = {}
        try:
            dates = {key: _calendar(raw.get(key)) for key in ("date_from", "date_to")}
        except SourceError as exc:
            raise ClarificationRequired(str(exc)) from exc
        if any(dates.values()) and not self.manifest["date"]:
            raise ClarificationRequired("No date mapping is approved for this source.")
        if dates["date_from"] and dates["date_to"] and dates["date_from"] > dates["date_to"]:
            raise ClarificationRequired("Start date must precede end date.")
        sort = raw.get("sort", {"field": "month" if "month" in selected["dimensions"] else selected["metrics"][0], "direction": "asc" if "month" in selected["dimensions"] else "desc"})
        if not isinstance(sort, dict) or set(sort) != {"field", "direction"} or sort.get("field") not in selected["metrics"] + selected["dimensions"] or sort.get("direction") not in {"asc", "desc"}:
            raise ClarificationRequired("Sort by a selected metric or dimension in ascending or descending order.")
        limit = raw.get("limit", MAX_ROWS)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_ROWS:
            raise ClarificationRequired("Result limit must be an integer from 1 to 100.")
        return {"version": 2, **selected, "filters": filters, "having": copy.deepcopy(having), "population": population, "set_operation": operation, "exists": exists, "comparison": copy.deepcopy(comparison), **dates, "sort": copy.deepcopy(sort), "limit": limit}

    def _column(self, table: str, column: str) -> str:
        return f"{self.aliases[table]}.{quote_identifier(column)}"

    def _compile(self, plan: dict[str, Any]) -> dict[str, Any]:
        parameters: dict[str, Any] = {"result_limit": plan["limit"]}
        required = {"fact"}
        lineage_columns, operations = [], ["aggregate"]

        def use(table: str, column: str, role: str, semantic_id: str) -> str:
            required.add(table)
            record = {"table": self.physical[table], "column": column, "role": role, "semantic_id": semantic_id}
            if record not in lineage_columns:
                lineage_columns.append(record)
            return self._column(table, column)

        metric_sql = {}
        numeric_operands = []
        for key in plan["metrics"]:
            metric = self.metrics[key]
            operand = use("fact", metric["column"], "measure", key) if "column" in metric else "1"
            aggregate = metric["aggregate"]
            expression = f"COUNT(DISTINCT {operand})" if aggregate == "COUNT_DISTINCT" else f"{aggregate}({operand})"
            if aggregate in {"COUNT", "COUNT_DISTINCT", "SUM"}:
                expression = f"COALESCE({expression}, 0)"
            if metric["scale"] != 1:
                parameters[f"scale_{key}"] = float(metric["scale"])
                expression = f"({expression} / :scale_{key})"
            if metric["format"] == "currency":
                expression = f"ROUND({expression}, 2)"
            metric_sql[key] = expression
            if aggregate not in {"COUNT", "COUNT_DISTINCT"}:
                numeric_operands.append(operand)
        dimension_sql = {}
        for key in plan["dimensions"]:
            item = self.dimensions[key]
            expression = use(item["table"], item["column"], "group", key)
            dimension_sql[key] = f"substr({expression}, 1, 7)" if key == "month" else expression

        def conditions(filters: list[dict[str, Any]], fields: dict[str, dict[str, Any]], prefix: str, child: dict[str, Any] | None = None) -> list[str]:
            result = []
            for index, item in enumerate(filters):
                field = fields[item["field"]]
                if child is None:
                    expression = use(field["table"], field["column"], "filter", item["field"])
                else:
                    expression = f"e0.{quote_identifier(field['column'])}"
                    lineage_columns.append({"table": child["table"], "column": field["column"], "role": "related_filter", "semantic_id": item["field"]})
                name = f"{prefix}_{index}"
                if item["op"] == "in":
                    names = []
                    for position, value in enumerate(item["value"]):
                        names.append(f":{name}_{position}")
                        parameters[f"{name}_{position}"] = value
                    result.append(f"{expression} IN ({', '.join(names)})")
                else:
                    parameters[name] = item["value"]
                    result.append(f"{expression} {_OPS[item['op']]} :{name}")
            return result

        where = conditions(plan["filters"], self.fields, "filter")
        if where:
            operations.append("where")
        date_expression = None
        if "month" in plan["dimensions"] or plan["date_from"] or plan["date_to"]:
            mapping = self.manifest["date"]
            date_expression = use(mapping["table"], mapping["column"], "date", "month")
            for key, operator in (("date_from", ">="), ("date_to", "<=")):
                if plan[key]:
                    parameters[key] = plan[key]
                    where.append(f"substr({date_expression}, 1, 10) {operator} :{key}")
            if plan["date_from"] or plan["date_to"]:
                operations.append("date_range")
        child = None
        if plan["exists"]:
            selected = plan["exists"]
            child = self.children[selected["relation"]]
            parent_expression = use(child["parent"], child["parent_column"], "related_key", selected["relation"])
            child_conditions = [f"e0.{quote_identifier(child['child_column'])} COLLATE BINARY = {parent_expression} COLLATE BINARY"]
            child_conditions += conditions(selected["filters"], {f["id"]: f for f in child["fields"]}, "related", child)
            where.append(f"{'NOT ' if selected['negate'] else ''}EXISTS (SELECT 1 FROM {quote_identifier(child['table'])} e0 WHERE {' AND '.join(child_conditions)})")
            operations.append("not_exists" if selected["negate"] else "exists")
            lineage_columns.append({"table": child["table"], "column": child["child_column"], "role": "related_key", "semantic_id": selected["relation"]})
        needed_edges = {}
        for table in sorted(required):
            while table != "fact":
                edge = self.incoming[table]
                needed_edges[edge["id"]] = edge
                required.add(edge["from"])
                table = edge["from"]
        pending, ordered, reached = dict(needed_edges), [], {"fact"}
        while pending:
            for key, edge in sorted(pending.items()):
                if edge["from"] in reached:
                    ordered.append(edge)
                    reached.add(edge["to"])
                    del pending[key]
        fact = self.manifest["fact"]
        population = "SELECT " + ", ".join(quote_identifier(c) for c in fact["columns"]) + " FROM " + quote_identifier(fact["table"])
        physical_tables = [fact["table"]]
        if plan["population"] == "all":
            population += f" {'UNION ALL' if plan['set_operation'] == 'union_all' else 'UNION'} SELECT " + ", ".join(quote_identifier(c) for c in fact["columns"]) + " FROM " + quote_identifier(fact["archive_table"])
            physical_tables.append(fact["archive_table"])
            operations.append(plan["set_operation"])
        prefix = f"WITH __facts AS ({population})"
        from_sql = " FROM __facts t0"
        lineage_joins = []
        for edge in ordered:
            from_sql += f" LEFT JOIN {quote_identifier(self.physical[edge['to']])} {self.aliases[edge['to']]} ON {self._column(edge['from'], edge['from_column'])} COLLATE BINARY = {self._column(edge['to'], edge['to_column'])} COLLATE BINARY"
            physical_tables.append(self.physical[edge["to"]])
            lineage_joins.append({"from": self.physical[edge["from"]], "from_column": edge["from_column"], "to": self.physical[edge["to"]], "to_column": edge["to_column"], "kind": "many_to_one"})
        if child:
            physical_tables.append(child["table"])
        if ordered:
            operations.append("join")
        filtered_from = from_sql + (" WHERE " + " AND ".join(where) if where else "")
        select = ", ".join(f"{expression} AS {quote_identifier(key)}" for key, expression in {**dimension_sql, **metric_sql}.items())
        grouped = "SELECT " + select + filtered_from
        if dimension_sql:
            grouped += " GROUP BY " + ", ".join(dimension_sql.values())
        if plan["having"]:
            thresholds = []
            for index, item in enumerate(plan["having"]):
                parameters[f"having_{index}"] = item["value"]
                thresholds.append(f"{metric_sql[item['metric']]} {_OPS[item['op']]} :having_{index}")
            grouped += " HAVING " + " AND ".join(thresholds)
            operations.append("having")
        sql = prefix + ", __grouped AS (" + grouped + ") SELECT " + ", ".join(quote_identifier(key) for key in plan["dimensions"] + plan["metrics"]) + " FROM __grouped"
        if plan["comparison"]:
            metric = quote_identifier(plan["comparison"]["metric"])
            sql += f" WHERE {metric} > (SELECT AVG({metric}) FROM __grouped)"
            operations.append("above_average")
        sql += f" ORDER BY {quote_identifier(plan['sort']['field'])} {plan['sort']['direction'].upper()}"
        for key in plan["dimensions"]:
            if key != plan["sort"]["field"]:
                sql += f", {quote_identifier(key)} ASC"
        sql += " LIMIT :result_limit"
        quality_sql = None
        if numeric_operands:
            invalid = " OR ".join(f"({operand} IS NOT NULL AND typeof({operand}) NOT IN ('integer','real'))" for operand in sorted(set(numeric_operands)))
            quality_sql = prefix + " SELECT COALESCE(SUM(CASE WHEN " + invalid + " THEN 1 ELSE 0 END), 0)" + filtered_from
        # Validate numeric predicate operands before applying those predicates:
        # SQLite's storage-class ordering can otherwise make text pass `> 10`.
        filter_quality = []
        for item in plan["filters"]:
            field = self.fields[item["field"]]
            if field["type"] == "number":
                operand = self._column(field["table"], field["column"])
                filter_quality.append(prefix + f" SELECT COUNT(1){from_sql} WHERE {operand} IS NOT NULL AND typeof({operand}) NOT IN ('integer','real')")
        if child:
            child_fields = {field["id"]: field for field in child["fields"]}
            for item in plan["exists"]["filters"]:
                field = child_fields[item["field"]]
                if field["type"] == "number":
                    operand = quote_identifier(field["column"])
                    filter_quality.append(f"SELECT COUNT(1) FROM {quote_identifier(child['table'])} WHERE {operand} IS NOT NULL AND typeof({operand}) NOT IN ('integer','real')")
        date_sql = None
        if date_expression:
            d = date_expression
            invalid = f"{d} IS NOT NULL AND (typeof({d}) != 'text' OR length({d}) < 10 OR date(substr({d},1,10)) IS NULL OR date(substr({d},1,10)) != substr({d},1,10) OR (length({d}) > 10 AND (substr({d},11,1) NOT IN ('T',' ') OR datetime({d}) IS NULL)))"
            date_sql = prefix + f" SELECT COUNT(1){from_sql} WHERE {invalid}"
        # Display logical data flow, independent of the compiler's construction order.
        operation_order = ["union_all", "union", "join", "where", "date_range", "exists", "not_exists", "aggregate", "having", "above_average"]
        operations.sort(key=operation_order.index)
        return {"sql": sql, "parameters": parameters, "quality_sql": quality_sql, "filter_quality": filter_quality, "date_sql": date_sql, "lineage": {"tables": list(dict.fromkeys(physical_tables)), "columns": lineage_columns, "joins": lineage_joins, "operations": operations}}

    def compile_plan(self, raw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        compiled = self._compile(self.validate_plan(raw))
        return compiled["sql"], compiled["parameters"]

    def _authorizer(self, action: int, arg1: str, arg2: str, database: str, trigger: str) -> int:
        if action == sqlite3.SQLITE_SELECT:
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_READ and arg1 == "__facts" and arg2 == "" and database is None and trigger == "__grouped":
            # SQLite's table-only COUNT read targets the generated population
            # CTE itself. Its physical reads were separately allowlisted above.
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_READ and arg1 in self._allowed and arg2 in self._allowed[arg1] | {""} and (database == "main" or (database is None and arg2 == "")) and trigger in {None, "__facts", "__grouped"}:
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_FUNCTION and arg2 in {"count", "sum", "avg", "min", "max", "round", "coalesce", "substr", "typeof", "length", "date", "datetime"}:
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY

    def query(self, question: str | None = None, plan: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        meta = {"engine": "deterministic_relational", "source_id": self.source_id, "model_calls": 0, "estimated_cost_usd": 0, "estimated_model_cost_usd": 0, "synthetic": self.synthetic, "as_of": self.manifest["as_of"], "cache_hit": False}
        try:
            if question is not None or plan is None:
                raise ClarificationRequired("Language interpretation must produce an approved relational plan before execution.")
            canonical = self.validate_plan(plan)
            key = hashlib.sha256(json.dumps({"source": self.source_id, "catalog": self.catalog_version, "plan": canonical}, sort_keys=True).encode()).hexdigest()
            meta["execution_id"] = key[:16]
            with self._lock:
                payload = copy.deepcopy(self._cache.get(key))
            if payload is not None:
                meta["cache_hit"] = True
            else:
                compiled = self._compile(canonical)
                deadline = time.monotonic() + MAX_SECONDS
                connection = sqlite3.connect(self.database_path.as_uri() + "?mode=ro&immutable=1", uri=True, timeout=MAX_SECONDS)
                try:
                    connection.row_factory = sqlite3.Row
                    connection.execute("PRAGMA query_only=ON")
                    connection.execute("PRAGMA trusted_schema=OFF")
                    connection.set_authorizer(self._authorizer)
                    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                    if any(connection.execute(check, compiled["parameters"]).fetchone()[0] for check in compiled["filter_quality"]):
                        raise ClarificationRequired("An approved numeric filter field contains nonnumeric values. Clean the source before comparing numeric fields.")
                    if compiled["date_sql"] and connection.execute(compiled["date_sql"], compiled["parameters"]).fetchone()[0]:
                        raise ClarificationRequired("The approved date column contains malformed dates. Normalize dates to ISO calendar dates or timestamps.")
                    if compiled["quality_sql"] and connection.execute(compiled["quality_sql"], compiled["parameters"]).fetchone()[0]:
                        raise ClarificationRequired("An approved numeric measure contains nonnumeric values. Clean the reporting source before aggregation.")
                    data = [dict(row) for row in connection.execute(compiled["sql"], compiled["parameters"]).fetchmany(MAX_ROWS + 1)]
                finally:
                    connection.close()
                if len(data) > MAX_ROWS:
                    raise RuntimeError("Result limit exceeded.")
                if any(isinstance(value, float) and not math.isfinite(value) for row in data for value in row.values()):
                    raise ClarificationRequired("A metric exceeded its numeric range.")
                title = ", ".join(self.metrics[key]["label"] for key in canonical["metrics"])
                if canonical["dimensions"]:
                    title += " by " + " and ".join(self.dimensions[key]["label"] for key in canonical["dimensions"])
                formats = {key: self.metrics[key]["format"] for key in canonical["metrics"]}
                additive = {key: self.metrics[key]["aggregate"] in {"SUM", "COUNT"} for key in canonical["metrics"]}
                chart_type = "line" if canonical["dimensions"] == ["month"] else "bar" if canonical["dimensions"] else "stat"
                chart = {"type": chart_type, "x": canonical["dimensions"][0] if canonical["dimensions"] else None, "y": canonical["metrics"][0], "series": list(canonical["metrics"]), "format": formats[canonical["metrics"][0]], "formats": formats, "additive": additive, "title": title}
                explanation = " ".join(self.metrics[key]["description"] for key in canonical["metrics"])
                if canonical["comparison"]:
                    explanation += " Above-average compares grouped totals across every filtered group before sorting and limiting."
                if canonical["population"] == "all":
                    explanation += " Includes current and archived fact rows; " + ("exact duplicate rows are removed." if canonical["set_operation"] == "union" else "every row is retained, including overlapping archive rows.")
                payload = {"success": True, "data": data, "results": data, "columns": canonical["dimensions"] + canonical["metrics"], "plan": canonical, "sql": compiled["sql"], "parameters": compiled["parameters"], "chart": chart, "lineage": compiled["lineage"], "explanation": explanation, "title": title}
                with self._lock:
                    self._cache[key] = copy.deepcopy(payload)
                    while len(self._cache) > 128:
                        self._cache.popitem(last=False)
            meta["row_count"] = len(payload["data"])
        except ClarificationRequired as exc:
            payload = {"success": False, "data": [], "results": [], "error": str(exc), "error_type": "clarification_required", "suggestions": self.catalog()["examples"][:4]}
        except (sqlite3.Error, OSError, RuntimeError):
            payload = {"success": False, "data": [], "results": [], "error": "The selected snapshot could not complete the bounded read-only relational query. Check its approved mappings and source data.", "error_type": "execution_error"}
        meta["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 2)
        meta["execution_time_ms"] = meta["elapsed_ms"]
        payload["meta"] = meta
        payload["metadata"] = meta
        return payload
