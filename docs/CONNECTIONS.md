# Database connections, snapshots and refresh

Implemented September 14, 2026 for the single-user local workspace. Customers can connect a database and let AIDA create its reporting snapshot; they do not need to manufacture a SQLite file. This is not a hosted multi-tenant release. Database authentication is implemented; application user authentication and tenant authorization are still absent.

## End-to-end workflow and model calls

| Step | What happens | LLM calls |
| --- | --- | --- |
| Connect | Backend authenticates to an approved host using a read-only database account and verified TLS. | 0 |
| Inspect | Code reads ordinary-table, column, primary-key and foreign-key metadata. No row samples are returned to the setup screen. | 0 |
| Select | Owner explicitly selects tables and columns to copy. Nothing is selected automatically. | 0 |
| Extract | A background job streams the selected rows into a bounded local SQLite snapshot. Rows stay in the database/backend boundary. | 0 |
| Define | Owner approves metric definitions, dimensions and physical join mappings in the existing single-table or relational catalog screen. | 0 |
| Ask | Local Qwen interprets a business question using approved semantic definitions. | At most 1 per uncached question |
| Execute | Code validates the interpretation, resolves approved join paths, compiles parameterized read-only SQL and executes it locally. | 0 |
| Visualize | Code renders aggregates as charts/tables. Chart interactions and the builder modify structured plans. | 0 |
| Save/refresh dashboard | Browser stores a plan and catalog version; refresh executes the saved plan against the latest successful snapshot. | 0 |
| Refresh source | A background job repeats the approved extraction, validates the replacement and publishes it atomically. | 0 |

The LLM receives no connection credentials, physical schema mappings, customer rows or query results. It receives the question, owner-approved business labels/definitions and permitted semantic values. The local model remains necessary for natural language. These connectors do not improve the previously measured [language accuracy](BLIND_EVALUATION.md). For example, profit still needs an approved supported business measure; the model cannot invent revenue-minus-cost logic.

## Supported connections

| Engine | Python driver | Database requirements |
| --- | --- | --- |
| PostgreSQL | psycopg 3 via SQLAlchemy | TLS certificate/hostname validation; read-only, repeatable-read extraction |
| MySQL | PyMySQL via SQLAlchemy | MySQL 8 reporting schema; InnoDB tables; verified TLS; consistent read-only transaction |
| SQL Server | pyodbc via SQLAlchemy | Microsoft ODBC Driver 18 installed on backend host; trusted server certificate; database has `ALLOW_SNAPSHOT_ISOLATION ON` |
| SQLite | Existing file upload | Standalone snapshot up to 20 MB; no scheduled file reread |

Oracle, Snowflake, BigQuery, MariaDB-specific behavior, CSV and Parquet import are not implemented. Vendor adapters are implemented, but real vendor integration tests must pass on the target environment before claiming that deployment is qualified. The development environment had no running vendor database servers and lacked ODBC Driver 18; see [connector verification](CONNECTOR_VERIFICATION.md).

## Start and connect on Windows

1. The operator approves exact database destinations before starting AIDA:

   ```powershell
   $env:AIDA_CONNECTOR_HOSTS = 'reporting.example.com:5432,mysql.example.com:3306,sql.example.com:1433'
   # If the database uses a private CA, supply the PEM CA file on the backend host.
   $env:AIDA_CONNECTOR_CA_FILE = 'C:\certificates\company-database-ca.pem'
   powershell -ExecutionPolicy Bypass -File scripts/start-demo.ps1
   ```

   Use actual destinations; an empty allowlist permits no connection. Entries are hostname/IPv4 plus port, with no URLs or wildcards. The backend must have network/VPN access to those hosts. Network egress rules should also restrict access to the approved database servers.

2. Open `http://127.0.0.1:3000` → **Data catalog** → **Database connections** → **Connect database**.
3. Enter a connection name, engine, host, port, database, schema, read-only username and password. MySQL uses the database as its schema. Leave verified TLS enabled.
4. Select **Test connection and inspect**. Fix connectivity, TLS or permissions before continuing. SQL Server reports a missing ODBC driver explicitly.
5. Select the reporting columns to copy. Include keys needed by approved joins. Exclude personal/free-text columns you do not need. Selecting a column authorizes its values to be copied into the backend's local snapshot, even if you later exclude it from the query catalog.
6. Choose **Manual**, **Every hour** or **Every day**, then **Create reporting snapshot**. Credentials are saved encrypted for subsequent refreshes, including manual refresh.
7. Watch the connection move from queued/running to succeeded. Open **Open snapshot / approve catalog** and approve the existing single-table or [relational catalog](RELATIONAL_CATALOG.md). A source is not queryable before this step.
8. Ask a business question or use the visual builder. Save charts to a dashboard. **SQL & trust** includes the snapshot's publication timestamp for connected-source query results.
9. Use **Refresh now** to copy current data, change the schedule to Manual to pause automatic extraction, or **Disconnect and remove credentials**. Disconnect preserves the last reporting snapshot and its catalog; it removes the saved credentials and schedule.

No SQL expression, source file path, connection URL, driver name or arbitrary connection-string options are accepted from this form. Business catalog JSON is still an explicit owner-review step for relational databases.

## Credential storage and deployment configuration

The Windows launcher calls `scripts/connector-env.ps1`. It creates a random 32-byte key, protects it with Windows DPAPI for the current Windows account, and stores only the protected bytes in ignored `.runtime/connector-key.dpapi`. On subsequent launches it unlocks the same key into the backend environment. No key is printed or committed. Preserve this file and the Windows account's DPAPI recovery capability to keep existing credentials usable.

For another platform or an operator-managed secret store, supply `AIDA_CONNECTOR_KEY` as a Fernet key before starting the backend. Existing supplied keys take precedence over Windows key generation. Generate it with `Fernet.generate_key()` from Python's cryptography package and place it in the deployment secret manager. Do not rotate the key by simply replacing it: there is no bulk re-encryption operation yet. Restore the original key or disconnect and recreate affected connections.

The complete connection settings, password, selected columns and expected schema are Fernet-encrypted in `AIDA_DATA_DIR/connections/<opaque-id>.json`. The status/history envelope contains the connection display name, engine, opaque source ID, timestamps and row counts. Driver exception strings and SQL parameter values are never returned or logged by this workflow. Passwords are cleared from React state after creation/cancel and are not stored in browser local storage.

Snapshot rows are ordinary local SQLite files, not encrypted by this change. Use an encrypted, access-controlled disk/volume and a customer-controlled backend when data must remain within the customer's environment. Copying data is an extraction workload: prefer an appropriate reporting replica, and size/schedule extraction with the database owner. Snapshotting reduces dashboard query load on the source; it does not eliminate extraction load or make customer data anonymous.

The supplied Docker backend supports the Python PostgreSQL/MySQL dependencies. SQL Server additionally needs the OS ODBC runtime and Microsoft ODBC Driver 18 in a derived image; follow the [Microsoft installation instructions](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server). The default compose mode remains public demo, with all connections disabled. For a local private workspace set `AIDA_PUBLIC_DEMO=0`, supply the connector key and allowlist, and bind the UI to loopback. Private shared hosting still requires application authentication and tenant isolation.

## Read-only database account preparation

The DBA creates a dedicated account with SELECT access only to the intended reporting schema/tables. Do not use an administrator, schema owner or a role inheriting write privileges. The adapter executes SELECT and transaction/session configuration statements, not source-data mutations. SQL Server's `ApplicationIntent=ReadOnly` is a routing hint, not a substitute for database permissions.

Example grants, adapted by the DBA after creating the dedicated login:

```sql
-- PostgreSQL: run in the reporting database as its administrator.
GRANT CONNECT ON DATABASE reporting_database TO aida_reader;
GRANT USAGE ON SCHEMA reporting TO aida_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO aida_reader;
-- Apply appropriate default SELECT grants separately if new reporting tables are added.

-- MySQL: the account host must match the AIDA backend's network identity.
GRANT SELECT ON reporting_database.* TO 'aida_reader'@'AIDA_BACKEND_IP';

-- SQL Server: create/map a database user for the dedicated login first.
GRANT SELECT ON SCHEMA::reporting TO aida_reader;
-- DBA approval required: enable snapshot isolation on the chosen reporting database.
ALTER DATABASE reporting_database SET ALLOW_SNAPSHOT_ISOLATION ON;
```

For PostgreSQL CA handling, see the [SQLAlchemy PostgreSQL driver documentation](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html). MySQL TLS uses a Python context with certificate and hostname checking; see [SQLAlchemy MySQL TLS connections](https://docs.sqlalchemy.org/en/20/dialects/mysql.html#ssl-connections). SQL Server uses `Encrypt=yes;TrustServerCertificate=no`; see [Microsoft ODBC connection attributes](https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute).

## Refresh behavior and current limits

- One background worker per local data directory; an OS lock rejects a second connector worker. Run one Uvicorn worker. Schedules run only while the backend is running.
- Hourly/daily means one/24 hours after completion or schedule change, not a wall-clock cron time. A missed due time after downtime runs once; interrupted jobs become failed and can be retried manually. Failed scheduled jobs retry at the next interval.
- Jobs are serial, with at most one outstanding job per connection and 50 saved connections. A queued/running connection cannot be rescheduled or disconnected until completion. Refresh history retains 20 outcomes.
- Full snapshot replacement only. Updates and deletions appear on the next successful refresh. Incremental watermarks, CDC, job cancellation and continuous live queries are not implemented.
- One selected source schema, up to 200 inspected tables/200 columns per table, up to 50 selected tables, one million rows total and a 256 MB snapshot. Extraction checks a 120-second elapsed budget between batches; driver calls also have individual timeouts, so this is not a hard process-level deadline. Schema inspection checks a 30-second budget between tables.
- Only supported scalar columns are copied; computed, JSON, arrays, binary and vendor-specific complex types require prepared reporting columns. Integers must fit signed 64 bits. Non-finite numbers and decimals that cannot round-trip through the chosen numeric representation fail closed. SQLite REAL uses floating-point arithmetic: integer minor units remain preferable for exact financial measures. Aware timestamps normalize to UTC; other timestamps preserve their supplied calendar representation.
- Selected primary keys are preserved if all constituent columns are copied. This initial connector does not copy foreign-key declarations or alternate unique indexes into SQLite. The metadata screen shows source keys; relational catalog mappings still need explicit approval, and joins must use preserved primary keys.
- Selected-schema drift stops refresh and leaves the old snapshot available. Create a new connection and approve its changed catalog. Unselected columns are never silently added to an extraction.
- Replacement uses a new immutable file and an atomic source-manifest update. In-flight queries finish against their old generation; new queries receive a new engine and empty result cache. The semantic catalog version stays stable for data-only refresh, so saved dashboards remain usable. Old generations are removed after their engine's active readers finish; orphan generations are cleaned at an exclusive worker startup.
- Relative-date business reference (`as_of`) remains the approved catalog value; a data refresh does not silently rewrite date semantics. Review it when advancing a reporting period.

## API

All routes are local-mode only except the empty/disabled public-demo connection list. Body limits and same-origin write checks apply. Credentials never appear in responses.

| Method and path | Body / outcome |
| --- | --- |
| `GET /api/v1/connections` | Enabled status and connection status/history; no secrets |
| `POST /api/v1/connections/inspect` | Structured connection fields; returns metadata only |
| `POST /api/v1/connections` | `{name, connection, selection, schedule}`; returns HTTP 202 with queued connection |
| `POST /api/v1/connections/{id}/refresh` | `{}`; queues a refresh |
| `POST /api/v1/connections/{id}/schedule` | `{"schedule":"manual"}`, `hourly` or `daily` |
| `POST /api/v1/connections/{id}/disconnect` | `{}`; removes credentials/schedule, retains snapshot |

Example connection fields:

```json
{
  "engine": "postgresql",
  "host": "reporting.example.com",
  "port": 5432,
  "database": "reporting_database",
  "schema": "reporting",
  "username": "aida_reader",
  "password": "SUPPLY_SECURELY_AT_RUNTIME",
  "tls": true
}
```

Example selection: `[{"table":"order_lines","columns":["line_id","order_id","region","revenue_cents"]}]`. After the connection succeeds, take its `source_id` to the existing `/sources/{id}/configure` and query API described in the README.

## Run vendor integration tests

On each disposable test database, the DBA prepares this fixture in the chosen reporting schema:

```sql
CREATE TABLE aida_connector_probe (
  probe_id INTEGER PRIMARY KEY,
  category VARCHAR(30),
  quantity INTEGER,
  private_note VARCHAR(80)
);
INSERT INTO aida_connector_probe VALUES
  (1, 'Garden', 11, 'synthetic excluded field'),
  (2, 'Tools', 8, 'synthetic excluded field');
```

Set `AIDA_TEST_POSTGRESQL`, `AIDA_TEST_MYSQL`, and/or `AIDA_TEST_SQLSERVER` to JSON connection objects using that database's dedicated read-only test account. Load them from your secret manager or ignored local secret file; do not paste credentials into tracked files. Configure trusted TLS and the SQL Server driver/isolation setting as above. Then run:

```powershell
.\.venv\Scripts\python.exe -m pytest backend/test_connectors_live.py -q
```

Unset engines explicitly skip. Tests inspect, selectively extract, approve/query the catalog, check excluded-column absence, and repeat refresh with stable catalog identity. They do not mutate the source or call an LLM. To use a disposable loopback server without TLS only, the operator must explicitly set `AIDA_CONNECTOR_LOCAL_TEST=1` and use `localhost`/`127.0.0.1` with `tls:false`. Remote plaintext is rejected.
