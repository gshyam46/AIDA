"""Local SQLite snapshot onboarding and explicit, durable semantic catalogs.

Inspection reads schema metadata only. A source becomes queryable only after its
owner approves mappings and business definitions; neither SQL nor file paths are
accepted from query requests. Uploaded snapshots are owned by this registry.
"""
from __future__ import annotations

import copy
import json
import math
import os
import re
import sqlite3
import threading
import uuid
import weakref
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

MAX_UPLOAD_BYTES = 20 * 1024 * 1024
_ID = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
BUILTIN_SOURCE_IDS = {"commerce", "support", "warehouse", "billing", "chinook"}
_SOURCE_ID = re.compile(r"(?:[a-f0-9]{32}|commerce|support|warehouse|billing|chinook)\Z")
_SENSITIVE = re.compile(r"(?:^|_)(?:id|uuid|guid|email|phone|mobile|address|name|firstname|lastname|first_name|last_name|dob|birth|ssn|pan|aadhaar|aadhar|passport|password|secret|token|account|iban|card|ip|notes|comment|comments|description|message|body|text)(?:_|$)", re.I)


class SourceError(ValueError):
    pass


def quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or len(value) > 128:
        raise SourceError("The source has an unsupported SQL identifier.")
    return '"' + value.replace('"', '""') + '"'


def _text(value: Any, field: str, maximum: int = 300) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise SourceError(f"Provide a valid {field} (1 to {maximum} characters).")
    return value.strip()


def _public_id(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value) or value in {"value", "month", "internal_invalid"}:
        raise SourceError(f"{field} must start with a lowercase letter and contain only lowercase letters, digits or underscores; value and month are reserved.")
    return value


def _date(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise SourceError(f"{field} must use YYYY-MM-DD.")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise SourceError(f"{field} must be a valid calendar date.") from exc
    return value


def inspect_database(path: Path) -> list[dict[str, Any]]:
    """Inspect physical tables without executing views, triggers, or row reads."""
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True, timeout=2)) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA trusted_schema=OFF")
            definitions = connection.execute("SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
            if len(definitions) > 300:
                raise SourceError("This demo accepts sources with at most 300 schema objects.")
            if any(kind in {"view", "trigger"} or (sql and re.search(r"CREATE\s+VIRTUAL\s+TABLE", sql, re.I)) for kind, _, sql in definitions):
                raise SourceError("Upload a SQLite snapshot containing ordinary tables and indexes only; views, triggers and virtual tables are not supported.")
            tables = []
            for kind, name, _ in definitions:
                if kind != "table":
                    continue
                columns = []
                for _, column, declared_type, _, _, primary_key, hidden in connection.execute(f"PRAGMA table_xinfo({quote_identifier(name)})"):
                    if hidden:
                        raise SourceError("Generated or hidden columns are not supported in uploaded snapshots.")
                    # Metadata suggestions are conservative; the owner still approves mappings.
                    sensitive = bool(primary_key or _SENSITIVE.search(re.sub(r"([a-z])([A-Z])", r"\1_\2", column)))
                    numeric = any(token in declared_type.upper() for token in ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC"))
                    columns.append({"name": column, "type": declared_type, "numeric": numeric, "sensitive": sensitive, "primary_key": bool(primary_key)})
                if len(columns) > 200:
                    raise SourceError("This demo accepts at most 200 columns per table.")
                unique_keys = []
                primary_columns = [column["name"] for column in columns if column["primary_key"]]
                if primary_columns:
                    unique_keys.append(primary_columns)
                for _, index_name, unique, _, partial in connection.execute(f"PRAGMA index_list({quote_identifier(name)})"):
                    if not unique or partial:
                        continue
                    indexed = [row[2] for row in connection.execute(f"PRAGMA index_info({quote_identifier(index_name)})")]
                    if indexed and all(isinstance(column, str) for column in indexed) and indexed not in unique_keys:
                        unique_keys.append(indexed)
                foreign_keys = [{"id": row[0], "sequence": row[1], "table": row[2], "from": row[3], "to": row[4]}
                                for row in connection.execute(f"PRAGMA foreign_key_list({quote_identifier(name)})")]
                tables.append({"name": name, "columns": columns, "unique_keys": unique_keys, "foreign_keys": foreign_keys})
            if not tables or len(tables) > 50:
                raise SourceError("The snapshot must contain between 1 and 50 ordinary tables.")
            return tables
    except sqlite3.Error as exc:
        raise SourceError("The upload is not a readable, standalone SQLite snapshot. Export a database backup including committed WAL changes.") from exc


def validate_manifest(config: dict[str, Any], tables: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise SourceError("Source configuration must be an object.")
    if config.get("version") == 2:
        from .relational import validate_relational_manifest
        return validate_relational_manifest(config, tables)
    allowed = {"name", "table", "metrics", "dimensions", "date_column", "as_of", "currency", "date_from", "date_to", "examples"}
    if set(config) - allowed:
        raise SourceError("Unsupported source configuration fields.")
    manifest: dict[str, Any] = {"name": _text(config.get("name"), "source name", 80)}
    table = next((table for table in tables if table["name"] == config.get("table")), None)
    if table is None:
        raise SourceError("Choose a table from the inspected schema. Cross-table joins require a separately prepared reporting table.")
    manifest["table"] = table["name"]
    columns = {column["name"]: column for column in table["columns"]}

    def column(name: Any, numeric: bool = False) -> str:
        if not isinstance(name, str) or name not in columns:
            raise SourceError("Choose a column from the selected table.")
        if columns[name]["sensitive"]:
            raise SourceError(f"Column {name!r} is excluded because it appears to contain identifiers or personal/free-text data. Prepare a de-identified reporting column first.")
        if numeric and not columns[name]["numeric"]:
            raise SourceError("Numeric measures require a numeric declared column type.")
        return name

    metrics = config.get("metrics")
    if not isinstance(metrics, list) or not 1 <= len(metrics) <= 30:
        raise SourceError("Approve between 1 and 30 metric definitions.")
    manifest["metrics"] = []
    identifiers: set[str] = set()
    for raw in metrics:
        if not isinstance(raw, dict) or set(raw) - {"id", "label", "description", "aggregate", "column", "format", "scale", "where"}:
            raise SourceError("Unsupported metric definition fields.")
        key = _public_id(raw.get("id"), "Metric identifier")
        if key in identifiers:
            raise SourceError("Metric identifiers must be unique.")
        identifiers.add(key)
        aggregate = raw.get("aggregate")
        if not isinstance(aggregate, str) or aggregate not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise SourceError("Choose COUNT, SUM, AVG, MIN or MAX; custom SQL expressions are unavailable.")
        metric = {"id": key, "label": _text(raw.get("label"), "metric label", 80), "description": _text(raw.get("description"), "metric business definition", 500), "aggregate": aggregate}
        if aggregate != "COUNT":
            metric["column"] = column(raw.get("column"), numeric=True)
        elif raw.get("column") is not None:
            raise SourceError("COUNT counts table rows; omit its column.")
        format_ = raw.get("format", "number")
        if not isinstance(format_, str) or format_ not in {"number", "currency"}:
            raise SourceError("Metric format must be number or currency.")
        metric["format"] = format_
        scale = raw.get("scale", 1)
        if isinstance(scale, bool) or not isinstance(scale, (int, float)) or not math.isfinite(scale) or not 0 < scale <= 1e9:
            raise SourceError("Scale is a positive divisor no larger than one billion (use 100 for cents).")
        metric["scale"] = scale
        conditions = raw.get("where", {})
        if not isinstance(conditions, dict) or len(conditions) > 10:
            raise SourceError("Metric conditions must contain at most 10 column/equality values.")
        metric["where"] = {column(k): _text(v, "fixed metric condition", 150) for k, v in conditions.items()}
        manifest["metrics"].append(metric)
    raw_dimensions = config.get("dimensions", [])
    if not isinstance(raw_dimensions, list) or len(raw_dimensions) > 20:
        raise SourceError("Approve at most 20 dimensions.")
    dimensions = []
    identifiers = set()
    for raw in raw_dimensions:
        if not isinstance(raw, dict) or set(raw) - {"id", "label", "column", "values", "aliases"}:
            raise SourceError("Unsupported dimension definition fields.")
        key = _public_id(raw.get("id"), "Dimension identifier")
        if key in identifiers:
            raise SourceError("Dimension identifiers must be unique.")
        identifiers.add(key)
        dimension = {"id": key, "label": _text(raw.get("label"), "dimension label", 80), "column": column(raw.get("column"))}
        if "values" in raw:
            values = raw["values"]
            if not isinstance(values, list) or not 1 <= len(values) <= 100:
                raise SourceError("An approved dimension value list must contain 1 to 100 values; omit it for literal filters.")
            dimension["values"] = [_text(value, "dimension filter value", 150) for value in values]
            if len({v.casefold() for v in dimension["values"]}) != len(values):
                raise SourceError("Approved filter values must be distinct, ignoring case.")
        if "aliases" in raw:
            aliases = raw["aliases"]
            if not isinstance(aliases, dict) or len(aliases) > 100 or not dimension.get("values"):
                raise SourceError("Approved aliases require an explicit dimension value list and at most 100 aliases.")
            normalized_aliases = {}
            canonical_values = {value.casefold(): value for value in dimension["values"]}
            for alias, target in aliases.items():
                alias = _text(alias, "dimension value alias", 150)
                target = _text(target, "dimension alias target", 150)
                if target not in dimension["values"] or (alias.casefold() in canonical_values and canonical_values[alias.casefold()] != target):
                    raise SourceError("Every alias must refer to an approved canonical dimension value without conflicting with another value.")
                if alias.casefold() in normalized_aliases:
                    raise SourceError("Dimension aliases must be distinct, ignoring case.")
                normalized_aliases[alias.casefold()] = target
            dimension["aliases"] = normalized_aliases
        dimensions.append(dimension)
    manifest["dimensions"] = dimensions
    date_column = config.get("date_column")
    manifest["date_column"] = column(date_column) if date_column else None
    manifest["as_of"] = _date(config.get("as_of"), "Reference date")
    if date_column and not manifest["as_of"]:
        raise SourceError("Set an explicit reference date for relative periods when approving a date column.")
    manifest["date_from"] = _date(config.get("date_from"), "Dataset start date")
    manifest["date_to"] = _date(config.get("date_to"), "Dataset end date")
    if manifest["date_from"] and manifest["date_to"] and manifest["date_from"] > manifest["date_to"]:
        raise SourceError("Dataset start date must precede its end date.")
    currency = config.get("currency", "USD")
    if not isinstance(currency, str) or not re.fullmatch("[A-Z]{3}", currency):
        raise SourceError("Currency must be a three-letter uppercase code.")
    manifest["currency"] = currency
    examples = config.get("examples", [])
    if not isinstance(examples, list) or len(examples) > 12:
        raise SourceError("Provide at most 12 example questions.")
    manifest["examples"] = [_text(value, "example question", 300) for value in examples]
    return manifest


class SourceRegistry:
    def __init__(self, data_dir: str | Path, public_demo: bool = False):
        self.directory = Path(data_dir).resolve() / "sources"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.public_demo = public_demo
        self._lock = threading.RLock()
        self._entries: dict[str, dict[str, Any]] = {}
        self._engines: dict[str, Any] = {}
        for file in self.directory.glob("*.json"):
            if not _SOURCE_ID.fullmatch(file.stem) or file.stem in BUILTIN_SOURCE_IDS:
                continue
            try:
                record = json.loads(file.read_text(encoding="utf-8"))
                if record.get("id") != file.stem or record.get("synthetic") is not False:
                    continue
                snapshot = record.get("snapshot_file", f"{file.stem}.sqlite")
                if not re.fullmatch(re.escape(file.stem) + r"(?:\.[a-f0-9]{32})?\.sqlite", snapshot):
                    continue
                path = self.directory / snapshot
                if path.is_file() and not path.is_symlink():
                    record["path"] = path
                    self._entries[file.stem] = record
            except (ValueError, OSError):
                continue

    def _entry(self, source_id: str) -> dict[str, Any]:
        if not isinstance(source_id, str) or not _SOURCE_ID.fullmatch(source_id) or source_id not in self._entries:
            raise SourceError("Unknown data source. Select an available source.")
        entry = self._entries[source_id]
        if self.public_demo and not entry["synthetic"]:
            raise SourceError("Private sources are unavailable in public demo mode.")
        return entry

    def _persist(self, record: dict[str, Any]) -> None:
        destination = self.directory / f"{record['id']}.json"
        temporary = self.directory / f".{record['id']}.{uuid.uuid4().hex}.tmp"
        try:
            temporary.write_text(json.dumps({k: v for k, v in record.items() if k != "path"}, ensure_ascii=False), encoding="utf-8")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    def list_sources(self) -> list[dict[str, Any]]:
        with self._lock:
            return [{"id": record["id"], "name": record["name"], "configured": bool(record.get("manifest")), "synthetic": record["synthetic"]} for record in self._entries.values() if not self.public_demo or record["synthetic"]]

    def inspect_upload(self, content: bytes, name: str) -> dict[str, Any]:
        if self.public_demo:
            raise SourceError("Database upload is disabled in public demo mode. Run AIDA locally to onboard private sources.")
        name = _text(name, "source name", 80)
        if not isinstance(content, bytes) or len(content) > MAX_UPLOAD_BYTES or len(content) < 100 or not content.startswith(b"SQLite format 3\x00"):
            raise SourceError("Upload a valid SQLite snapshot no larger than 20 MB.")
        source_id = uuid.uuid4().hex
        path = self.directory / f"{source_id}.sqlite"
        try:
            with path.open("xb") as file:
                file.write(content)
            tables = inspect_database(path)
            record = {"id": source_id, "name": name, "path": path, "synthetic": False, "tables": tables, "manifest": None}
            with self._lock:
                self._persist(record)
                self._entries[source_id] = record
        except Exception:
            path.unlink(missing_ok=True)
            raise
        return self.inspect_source(source_id)

    def inspect_source(self, source_id: str) -> dict[str, Any]:
        record = self._entry(source_id)
        return {"id": record["id"], "name": record["name"], "configured": bool(record.get("manifest")), "synthetic": record["synthetic"], "tables": copy.deepcopy(record["tables"])}

    def publish_snapshot(self, source_id: str | None, staged: Path, name: str) -> str:
        """Publish a validated immutable generation; existing readers keep the old file.

        Keep the catalog version stable for data-only refreshes. A new engine discards
        result caches. Schema drift fails closed and preserves the previous snapshot.
        """
        if self.public_demo:
            raise SourceError("Connections are disabled in public demo mode.")
        tables = inspect_database(staged)
        with self._lock:
            previous = self._entry(source_id) if source_id else None
            if previous and previous["synthetic"]:
                raise SourceError("Built-in sources cannot be refreshed.")
            if previous and previous["tables"] != tables:
                raise SourceError("Selected schema changed. Create a new connection and review its catalog; the last good snapshot is still available.")
            source_id = source_id or uuid.uuid4().hex
            destination = self.directory / f"{source_id}.{uuid.uuid4().hex}.sqlite"
            manifest = previous.get("manifest") if previous else None
            if manifest:
                validate_manifest(manifest, tables)
            record = {"id": source_id, "name": previous["name"] if previous else _text(name, "source name", 80),
                      "path": destination, "snapshot_file": destination.name, "synthetic": False,
                      "snapshot_updated_at": datetime.now(timezone.utc).isoformat(),
                      "tables": tables, "manifest": manifest}
            os.replace(staged, destination)
            try:
                self._persist(record)
            except Exception:
                destination.unlink(missing_ok=True)
                raise
            self._entries[source_id] = record
            old_engine = self._engines.pop(source_id, None)
            if previous:
                if old_engine:
                    # A query keeps its engine alive until its SQLite handle closes.
                    weakref.finalize(old_engine, self._remove_old_snapshot, previous["path"])
                else:
                    self._remove_old_snapshot(previous["path"])
            return source_id

    def _remove_old_snapshot(self, path: Path) -> None:
        if path.parent == self.directory:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass  # Retry orphan-generation cleanup at the next exclusive startup.

    def cleanup_snapshot_generations(self) -> None:
        """Call only after the connector service holds its exclusive process lock."""
        active = {record["path"] for record in self._entries.values()}
        for path in self.directory.glob("*.sqlite"):
            if re.fullmatch(r"[a-f0-9]{32}\.[a-f0-9]{32}\.sqlite", path.name) and path not in active:
                self._remove_old_snapshot(path)

    def configure(self, source_id: str, config: dict[str, Any]) -> dict[str, Any]:
        if self.public_demo:
            raise SourceError("Source configuration is disabled in public demo mode.")
        with self._lock:
            record = self._entry(source_id)
            if record["synthetic"]:
                raise SourceError("Built-in demo definitions cannot be changed.")
            manifest = validate_manifest(config, record["tables"])
            record = {**record, "name": manifest["name"], "manifest": manifest}
            self._persist(record)
            self._entries[source_id] = record
            self._engines.pop(source_id, None)
            return self.engine(source_id).catalog()

    def register_source(self, path: str | Path, manifest: dict[str, Any], source_id: str) -> Any:
        """Internal bootstrap only; never expose server paths in HTTP input."""
        if source_id not in BUILTIN_SOURCE_IDS:
            raise SourceError("Only reserved built-in demo identifiers may be registered internally.")
        path = Path(path).resolve()
        tables = inspect_database(path)
        approved = validate_manifest(manifest, tables)
        with self._lock:
            self._entries[source_id] = {"id": source_id, "name": approved["name"], "path": path, "synthetic": True, "tables": tables, "manifest": approved}
            self._engines.pop(source_id, None)
        return self.engine(source_id)

    def engine(self, source_id: str = "commerce") -> Any:
        from .catalog_engine import CatalogEngine
        with self._lock:
            record = self._entry(source_id)
            if not record.get("manifest"):
                raise SourceError("Approve the source's metrics, dimensions and date mapping before querying it.")
            if source_id not in self._engines:
                # Revalidate persisted mappings against the actual schema on reopen.
                approved = validate_manifest(record["manifest"], inspect_database(record["path"]))
                if approved.get("version") == 2:
                    from .relational import RelationalEngine
                    engine_type = RelationalEngine
                else:
                    engine_type = CatalogEngine
                self._engines[source_id] = engine_type(record["path"], approved, source_id, record["synthetic"])
                self._engines[source_id].snapshot_updated_at = record.get("snapshot_updated_at")
            return self._engines[source_id]

    def register_commerce_demo(self, path: str | Path) -> Any:
        from .analytics import DIMENSIONS, EXAMPLES, METRICS
        metric_mappings = [
            {"id": "revenue", "aggregate": "SUM", "column": "amount_cents", "scale": 100, "where": {"status": "Completed"}},
            {"id": "orders", "aggregate": "COUNT"},
            {"id": "average_order_value", "aggregate": "AVG", "column": "amount_cents", "scale": 100, "where": {"status": "Completed"}},
        ]
        for metric in metric_mappings:
            metric.update({key: value for key, value in METRICS[metric["id"]].items() if key != "sql"})
        aliases = {"region": {"western": "West", "eastern": "East", "northern": "North", "southern": "South"}, "channel": {"web": "Online", "in store": "Retail"}, "status": {"canceled": "Cancelled", "complete": "Completed"}}
        return self.register_source(path, {"name": "Commerce demo", "table": "analytics_orders", "metrics": metric_mappings, "dimensions": [{"id": key, "column": key, **value, **({"aliases": aliases[key]} if key in aliases else {})} for key, value in DIMENSIONS.items() if key != "month"], "date_column": "order_date", "as_of": "2025-12-31", "date_from": "2025-01-01", "date_to": "2025-12-31", "currency": "USD", "examples": EXAMPLES}, "commerce")

    def register_support_demo(self) -> Any:
        path = self.directory / "support.sqlite"
        if not path.exists():
            with closing(sqlite3.connect(path)) as connection, connection:
                connection.execute("CREATE TABLE support_tickets (ticket_id INTEGER PRIMARY KEY, opened_on TEXT NOT NULL, team TEXT NOT NULL, priority TEXT NOT NULL, state TEXT NOT NULL, resolution_hours REAL)")
                rows = [(index + 1, f"2026-{month:02d}-{day:02d}", ["Billing", "Technical", "Accounts"][index % 3], ["Low", "Normal", "Urgent"][index % 3], "Open" if index % 5 == 0 else "Resolved", None if index % 5 == 0 else float(2 + index % 46)) for index, (month, day) in enumerate((month, day) for month in range(1, 7) for day in range(1, 29))]
                connection.executemany("INSERT INTO support_tickets VALUES (?,?,?,?,?,?)", rows)
        return self.register_source(path, {"name": "Support operations demo", "table": "support_tickets", "metrics": [
            {"id": "tickets", "label": "Tickets", "description": "Number of support tickets, including open and resolved tickets.", "aggregate": "COUNT"},
            {"id": "resolution_time", "label": "Average resolution time", "description": "Mean resolution hours among resolved support tickets; open tickets are excluded.", "aggregate": "AVG", "column": "resolution_hours", "where": {"state": "Resolved"}},
        ], "dimensions": [{"id": "team", "label": "Team", "column": "team", "values": ["Billing", "Technical", "Accounts"]}, {"id": "priority", "label": "Priority", "column": "priority", "values": ["Low", "Normal", "Urgent"]}, {"id": "state", "label": "State", "column": "state", "values": ["Open", "Resolved"], "aliases": {"unresolved": "Open", "closed": "Resolved"}}], "date_column": "opened_on", "as_of": "2026-06-30", "date_from": "2026-01-01", "date_to": "2026-06-28", "examples": ["How many support tickets are there?", "Tickets by team", "Average resolution time by priority", "Tickets last month", "Monthly ticket trend"]}, "support")
