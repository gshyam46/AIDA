"""Bounded database extraction, encrypted connection storage and durable refresh jobs.

This service is for a single local AIDA process. It never calls a language model.
Only server-allowlisted destinations are accepted; no URLs or arbitrary SQL input.
"""
from __future__ import annotations

import copy
import json
import math
import os
import re
import sqlite3
import ssl
import threading
import time
import uuid
from contextlib import contextmanager, closing
from datetime import date, datetime, time as wall_time, timezone
from decimal import Decimal
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
import sqlalchemy as sa
from sqlalchemy.pool import NullPool

from .sources import SourceError, _text, quote_identifier

ENGINES = {"postgresql": 5432, "mysql": 3306, "sqlserver": 1433}
SCHEDULES = {"manual": 0, "hourly": 3600, "daily": 86400}
MAX_ROWS = 1_000_000
MAX_BYTES = 256 * 1024 * 1024
MAX_SECONDS = 120
FAILURE = "Connection or refresh failed. Check credentials, database permissions, TLS trust, snapshot isolation and network access. The last good snapshot is unchanged."


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def validate_connection(raw):
    if not isinstance(raw, dict) or set(raw) - {"engine", "host", "port", "database", "schema", "username", "password", "tls"}:
        raise SourceError("Provide the database engine, host, port, database, schema, username, password and TLS option.")
    engine = raw.get("engine")
    if not isinstance(engine, str) or engine not in ENGINES:
        raise SourceError("Choose PostgreSQL, MySQL or SQL Server.")
    result = {k: _text(raw.get(k), k, 128) for k in ("host", "database", "username")}
    host = result["host"].lower()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", host):
        raise SourceError("Use a hostname or IPv4 address, without a URL or connection-string options.")
    port = raw.get("port", ENGINES[engine])
    if type(port) is not int or not 1 <= port <= 65535:
        raise SourceError("Port must be an integer between 1 and 65535.")
    allowed = {item.strip().lower() for item in os.environ.get("AIDA_CONNECTOR_HOSTS", "").split(",") if item.strip()}
    if f"{host}:{port}" not in allowed:
        raise SourceError("This database destination is not approved. The operator must add host:port to AIDA_CONNECTOR_HOSTS.")
    tls = raw.get("tls", True)
    if type(tls) is not bool:
        raise SourceError("TLS must be true or false.")
    if not tls and not (host in {"127.0.0.1", "localhost"} and os.environ.get("AIDA_CONNECTOR_LOCAL_TEST") == "1"):
        raise SourceError("Verified TLS is required. Plaintext is available only for operator-enabled loopback test databases.")
    password = raw.get("password")
    if not isinstance(password, str) or not 1 <= len(password) <= 1024 or "\x00" in password:
        raise SourceError("Provide a database password (1 to 1024 characters).")
    schema = raw.get("schema") or ("public" if engine == "postgresql" else "dbo" if engine == "sqlserver" else result["database"])
    result.update(engine=engine, host=host, port=port, schema=_text(schema, "schema", 128), password=password, tls=tls)
    if engine == "mysql" and result["schema"] != result["database"]:
        raise SourceError("For MySQL, schema must equal database.")
    return result


def create_remote_engine(spec):
    """Passwords are structured driver parameters and never interpolated into SQL."""
    spec = validate_connection(spec)  # Recheck policy on every scheduled attempt.
    common = dict(host=spec["host"], port=spec["port"], database=spec["database"], username=spec["username"], password=spec["password"])
    ca = os.environ.get("AIDA_CONNECTOR_CA_FILE")
    if spec["engine"] == "postgresql":
        url = sa.URL.create("postgresql+psycopg", **common)
        args = {"connect_timeout": 10, "sslmode": "verify-full" if spec["tls"] else "disable",
                "options": "-c statement_timeout=30000 -c lock_timeout=5000 -c default_transaction_read_only=on"}
        if ca:
            args["sslrootcert"] = ca
    elif spec["engine"] == "mysql":
        url = sa.URL.create("mysql+pymysql", **common)
        args = {"connect_timeout": 10, "read_timeout": 30, "write_timeout": 10, "local_infile": False,
                "ssl": ssl.create_default_context(cafile=ca) if spec["tls"] else None}
    else:
        try:
            import pyodbc
            if "ODBC Driver 18 for SQL Server" not in pyodbc.drivers():
                raise SourceError("Install Microsoft ODBC Driver 18 for SQL Server on the backend host, then retry.")
        except ImportError:
            raise SourceError("Install pyodbc, the ODBC runtime and Microsoft ODBC Driver 18 on the backend host.") from None
        # URL.create escapes credentials; pyodbc dialect quotes ODBC values.
        url = sa.URL.create("mssql+pyodbc", **common, query={"driver": "ODBC Driver 18 for SQL Server",
            "Encrypt": "yes" if spec["tls"] else "no", "TrustServerCertificate": "no", "ApplicationIntent": "ReadOnly"})
        args = {"timeout": 10}
    return sa.create_engine(url, connect_args=args, poolclass=NullPool, hide_parameters=True, echo=False)


@contextmanager
def remote_session(spec):
    engine = create_remote_engine(spec)
    try:
        with engine.connect() as connection:
            if spec["engine"] == "postgresql":
                connection = connection.execution_options(isolation_level="REPEATABLE READ", postgresql_readonly=True)
                connection.begin()
            elif spec["engine"] == "mysql":
                connection.exec_driver_sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                connection.exec_driver_sql("SET SESSION MAX_EXECUTION_TIME=30000")
                connection.commit()
                connection.exec_driver_sql("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            else:
                connection.connection.driver_connection.timeout = 30
                connection = connection.execution_options(isolation_level="SNAPSHOT")
                connection.begin()  # Requires DBA-enabled ALLOW_SNAPSHOT_ISOLATION.
            try:
                yield connection
            finally:
                connection.rollback()
    finally:
        engine.dispose()


def storage_type(type_):
    if isinstance(type_, (sa.Boolean, sa.Integer)):
        return "INTEGER"
    if isinstance(type_, (sa.Float, sa.Numeric)):
        return "REAL"
    if isinstance(type_, (sa.String, sa.Date, sa.DateTime, sa.Time, sa.Uuid)):
        return "TEXT"
    # No automatic stringify of JSON, arrays, geometry or opaque binary values.
    return None


def metadata(connection, schema):
    started = time.monotonic()
    inspector = sa.inspect(connection)
    names = sorted(inspector.get_table_names(schema=schema))
    if not names or len(names) > 200:
        raise SourceError("Choose a reporting schema with 1 to 200 ordinary tables.")
    tables = []
    for name in names:
        if time.monotonic() - started > 30:
            raise SourceError("Schema inspection exceeded 30 seconds. Choose a smaller reporting schema.")
        quote_identifier(name)
        columns = inspector.get_columns(name, schema=schema)
        if len(columns) > 200:
            raise SourceError("Reporting tables may have at most 200 columns.")
        pk = inspector.get_pk_constraint(name, schema=schema).get("constrained_columns") or []
        # Unfiltered PKs only: partial/expression unique indexes must not prove joins.
        tables.append({"name": name, "primary_key": pk,
            "columns": [{"name": c["name"], "type": str(c["type"]), "storage_type": storage_type(c["type"]),
                         "supported": storage_type(c["type"]) is not None and not c.get("computed"),
                         "primary_key": c["name"] in pk} for c in columns],
            "foreign_keys": [{"columns": f["constrained_columns"], "table": f["referred_table"],
                              "schema": f.get("referred_schema") or schema, "target_columns": f["referred_columns"]}
                             for f in inspector.get_foreign_keys(name, schema=schema)]})
    return tables


def selected_metadata(tables, selection):
    if not isinstance(selection, list) or not 1 <= len(selection) <= 50:
        raise SourceError("Select 1 to 50 reporting tables and explicitly choose their columns.")
    known = {t["name"]: t for t in tables}
    chosen, seen = [], set()
    for item in selection:
        if not isinstance(item, dict) or set(item) != {"table", "columns"}:
            raise SourceError("Each selection needs a table and a list of columns.")
        name, columns = item["table"], item["columns"]
        if not isinstance(name, str) or name not in known or name.casefold() in seen:
            raise SourceError("Selected table is missing or duplicated; review the reporting schema.")
        seen.add(name.casefold())
        if not isinstance(columns, list) or not columns or not all(isinstance(c, str) for c in columns) or len(set(columns)) != len(columns):
            raise SourceError("Choose distinct columns for each selected table.")
        table = known[name]
        available = {c["name"]: c for c in table["columns"]}
        if any(c not in available or not available[c]["supported"] for c in columns):
            raise SourceError("A selected column is missing, generated, or has an unsupported type. Prepare a reporting column first.")
        selected = [copy.deepcopy(c) for c in table["columns"] if c["name"] in columns]
        if len({c["name"].casefold() for c in selected}) != len(selected):
            raise SourceError("SQLite cannot represent column names differing only by case.")
        for c in selected:
            quote_identifier(c["name"])
        chosen.append({**table, "columns": selected, "primary_key": table["primary_key"] if set(table["primary_key"]) <= set(columns) else []})
    return sorted(chosen, key=lambda t: t["name"])


def scalar(value):
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise SourceError("Integer exceeds SQLite's signed 64-bit range. Prepare a reporting column.")
        return value
    if isinstance(value, Decimal):
        converted = float(value)
        if not value.is_finite() or not math.isfinite(converted) or Decimal(str(converted)) != value:
            raise SourceError("Decimal precision would be lost. Export bounded integer minor units, such as cents.")
        return converted
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SourceError("Non-finite numeric values cannot be imported.")
        return value
    if isinstance(value, datetime):
        if value.tzinfo:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    if isinstance(value, (date, wall_time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise SourceError("Unsupported value type. Prepare scalar reporting columns before importing.")


class DatabaseAdapter:
    def inspect(self, spec):
        with remote_session(spec) as connection:
            return metadata(connection, spec["schema"])

    def extract(self, spec, selection, expected, destination):
        started, count = time.monotonic(), 0
        with remote_session(spec) as remote:
            chosen = selected_metadata(metadata(remote, spec["schema"]), selection)
            if chosen != expected:
                raise SourceError("Selected schema changed. Create a new connection and review its catalog; the last good snapshot is still available.")
            if spec["engine"] == "mysql":
                for table in chosen:
                    kind = remote.execute(sa.text("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=:schema AND TABLE_NAME=:name"),
                                          {"schema": spec["schema"], "name": table["name"]}).scalar()
                    if kind != "InnoDB":
                        raise SourceError("Consistent MySQL extraction requires InnoDB reporting tables.")
            with closing(sqlite3.connect(destination)) as local, local:
                local.execute(f"PRAGMA max_page_count={MAX_BYTES // 4096}")
                for table in chosen:
                    columns = table["columns"]
                    definitions = [f"{quote_identifier(c['name'])} {c['storage_type']}" for c in columns]
                    if table["primary_key"]:
                        definitions.append("PRIMARY KEY (" + ",".join(quote_identifier(k) for k in table["primary_key"]) + ")")
                    local.execute(f"CREATE TABLE {quote_identifier(table['name'])} ({','.join(definitions)})")
                    source = sa.Table(table["name"], sa.MetaData(), *(sa.Column(c["name"]) for c in columns), schema=spec["schema"])
                    statement = sa.select(*source.c)
                    result = remote.execution_options(stream_results=True).execute(statement)
                    try:
                        while True:
                            if time.monotonic() - started > MAX_SECONDS:
                                raise SourceError("Refresh exceeded its time limit. Use a smaller reporting selection.")
                            batch = result.fetchmany(500)
                            if not batch:
                                break
                            count += len(batch)
                            if count > MAX_ROWS:
                                raise SourceError("Refresh exceeds one million rows. Use a smaller reporting selection.")
                            values = [tuple(scalar(value) for value in row) for row in batch]
                            local.executemany(f"INSERT INTO {quote_identifier(table['name'])} VALUES ({','.join('?' for _ in columns)})", values)
                    finally:
                        result.close()
        if destination.stat().st_size > MAX_BYTES:
            raise SourceError("Refresh exceeds the 256 MB snapshot limit.")
        return count


class ConnectionService:
    def __init__(self, registry, adapter=None):
        self.registry = registry
        self.directory = registry.directory.parent / "connections"
        self.directory.mkdir(exist_ok=True)
        self.adapter = adapter or DatabaseAdapter()
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.records = {}
        self.thread = None
        self.process_lock = None
        self.cipher = None
        key = os.environ.get("AIDA_CONNECTOR_KEY")
        if key:
            try:
                self.cipher = Fernet(key.encode("ascii"))
            except (ValueError, UnicodeError):
                pass  # Existing uploads remain usable if the operator misconfigures a key.
        for path in self.directory.glob("*.json"):
            if not re.fullmatch(r"[a-f0-9]{32}", path.stem):
                continue
            record = json.loads(path.read_text(encoding="utf-8"))
            if record.get("id") != path.stem:
                continue
            self.records[path.stem] = record
        self.recover_ids = set(self.records)

    def _recover(self):
        for id in self.recover_ids:
            record = self.records[id]
            if record["status"] in {"running", "queued"}:
                interval = SCHEDULES[record["schedule"]]
                record.update(status="failed", error="Refresh interrupted by service restart; retry manually or at the next scheduled refresh.",
                              next_refresh=time.time() + interval if interval else None)
                self._persist(record)
        self.recover_ids.clear()

    def start(self):
        if not self.registry.public_demo and self.cipher:
            # A single owner prevents duplicate jobs and racing snapshot publication.
            self.process_lock = (self.directory / ".worker.lock").open("a+b")
            self.process_lock.seek(0, 2)
            if self.process_lock.tell() == 0:
                self.process_lock.write(b"0")
                self.process_lock.flush()
            self.process_lock.seek(0)
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(self.process_lock.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.process_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                self.process_lock.close()
                self.process_lock = None
                raise SourceError("Another refresh worker owns this data directory. Run one backend worker per local workspace.") from None
            self._recover()
            self.registry.cleanup_snapshot_generations()
            self.thread = threading.Thread(target=self._worker, name="aida-refresh", daemon=True)
            self.thread.start()

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join()
        if self.process_lock:
            self.process_lock.close()

    def _enabled(self):
        if self.registry.public_demo:
            raise SourceError("Connections are disabled in public demo mode.")
        if not self.cipher:
            raise SourceError("The operator must configure a valid AIDA_CONNECTOR_KEY to enable database connections.")

    def _persist(self, record):
        path = self.directory / f"{record['id']}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(record), encoding="utf-8")
        os.replace(temporary, path)

    def _record(self, id):
        if not isinstance(id, str) or id not in self.records:
            raise SourceError("Unknown database connection.")
        return self.records[id]

    def _secret(self, record):
        try:
            return json.loads(self.cipher.decrypt(record["encrypted"].encode("ascii")))
        except (InvalidToken, ValueError):
            raise SourceError("Stored credentials cannot be unlocked. Restore the original connector key or recreate the connection.") from None

    def list(self):
        with self.lock:
            return {"enabled": not self.registry.public_demo and self.cipher is not None,
                    "engines": list(ENGINES), "connections": [] if self.registry.public_demo else [self._public(r) for r in self.records.values()]}

    def _public(self, record):
        return copy.deepcopy({k: v for k, v in record.items() if k != "encrypted"})

    def inspect(self, config):
        self._enabled()
        spec = validate_connection(config)
        try:
            return {"tables": self.adapter.inspect(spec)}
        except SourceError:
            raise
        except Exception:
            raise SourceError(FAILURE) from None

    def create(self, config):
        self._enabled()
        if not isinstance(config, dict) or set(config) != {"name", "connection", "selection", "schedule"}:
            raise SourceError("Provide name, connection, selection and schedule.")
        name = _text(config["name"], "connection name", 80)
        schedule = config["schedule"]
        if not isinstance(schedule, str) or schedule not in SCHEDULES:
            raise SourceError("Choose manual, hourly or daily refresh.")
        spec = validate_connection(config["connection"])
        expected = selected_metadata(self.inspect(spec)["tables"], config["selection"])
        secret = {"connection": spec, "selection": config["selection"], "expected": expected}
        with self.lock:
            if len(self.records) >= 50:
                raise SourceError("At most 50 saved connections are supported. Disconnect an unused connection first.")
            record = {"id": uuid.uuid4().hex, "name": name, "engine": spec["engine"], "source_id": None,
                      "schedule": schedule, "next_refresh": None, "status": "queued", "last_success": None,
                      "error": None, "rows": None, "history": [], "created_at": utcnow(),
                      "encrypted": self.cipher.encrypt(json.dumps(secret).encode()).decode()}
            self._persist(record)
            self.records[record["id"]] = record
            return self._public(record)

    def action(self, id, action, config):
        self._enabled()
        with self.lock:
            record = self._record(id)
            if record["status"] in {"queued", "running"}:
                raise SourceError("A refresh is already queued or running. Wait for it to finish.")
            if action == "refresh":
                if config:
                    raise SourceError("Refresh accepts no options.")
                record.update(status="queued", error=None)
            elif action == "schedule":
                if set(config) != {"schedule"} or not isinstance(config["schedule"], str) or config["schedule"] not in SCHEDULES:
                    raise SourceError("Choose manual, hourly or daily refresh.")
                interval = SCHEDULES[config["schedule"]]
                record.update(schedule=config["schedule"], next_refresh=time.time() + interval if interval else None)
            elif action == "disconnect":
                if config:
                    raise SourceError("Disconnect accepts no options.")
                (self.directory / f"{id}.json").unlink()
                del self.records[id]
                return {"disconnected": True, "source_id": record["source_id"]}
            else:
                raise SourceError("Unknown connection action.")
            self._persist(record)
            return self._public(record)

    def tick(self):
        """One serial job per tick; persisted due times survive backend restarts."""
        if self.registry.public_demo or not self.cipher:
            return
        with self.lock:
            due = next((r for r in self.records.values() if r["status"] == "queued" or
                        (r["status"] != "running" and r["next_refresh"] is not None and r["next_refresh"] <= time.time())), None)
            if due is None:
                return
            due.update(status="running", error=None)
            self._persist(due)
            record = copy.deepcopy(due)
        staged = self.registry.directory / f".refresh-{record['id']}.sqlite"
        started = utcnow()
        try:
            secret = self._secret(record)
            validate_connection(secret["connection"])
            rows = self.adapter.extract(secret["connection"], secret["selection"], secret["expected"], staged)
            if self.stop_event.is_set():
                raise SourceError("Refresh stopped during backend shutdown; the last good snapshot is unchanged.")
            source_id = self.registry.publish_snapshot(record["source_id"], staged, record["name"])
            outcome = {"status": "succeeded", "source_id": source_id, "rows": rows, "last_success": utcnow(), "error": None}
        except Exception as exc:
            outcome = {"status": "failed", "error": str(exc) if isinstance(exc, SourceError) else FAILURE}
        finally:
            staged.unlink(missing_ok=True)
        with self.lock:
            due = self._record(record["id"])
            due.update(outcome)
            interval = SCHEDULES[due["schedule"]]
            due["next_refresh"] = time.time() + interval if interval else None
            due["history"] = [{"started_at": started, "finished_at": utcnow(), "status": outcome["status"], "error": outcome["error"]}, *due["history"]][:20]
            self._persist(due)

    def _worker(self):
        while not self.stop_event.wait(1):
            try:
                self.tick()
            except Exception:
                # Never emit driver exceptions, SQL, credentials or row values to logs.
                self.stop_event.wait(5)
