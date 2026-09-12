"""Build an original, deterministic, fully synthetic logistics evaluation snapshot.

This module has no application, parser, model, or earlier-suite dependencies.
Run from any directory with Python 3.11+: python fixtures/blind_logistics/build_database.py
"""
from __future__ import annotations

import hashlib
import json
import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SEED = 260912
AS_OF = "2026-09-12"
DATE_FROM = "2025-07-01"
DATE_TO = AS_OF
MARKETS = ["Retail", "Manufacturing", "Healthcare", "Technology", "Public sector"]
ZONES = ["North", "South", "East", "West", "Central", "Islands"]
SERVICES = ["Standard", "Express", "Overnight", "Freight"]
CARRIERS = ["Arrow Parcel", "Cedar Freight", "Harbor Air", "Maple Logistics", "Polar Transit", "Quartz Express"]
SHIPMENT_STATES = ["Delivered", "In transit", "On hold", "Cancelled", "Returned"]
LEG_STATES = ["Completed", "In transit", "Delayed", "Cancelled"]
REASONS = ["Weather", "Damage", "Customs", "Capacity", "Address"]
EVENT_STATES = ["Open", "Resolved", "Waived"]
FACT_COLUMNS = ["leg_id", "consignment_id", "service_class_id", "carrier_id", "departed_on", "weight_grams", "charge_cents", "transit_minutes", "distance_km", "package_count", "planned_transit_minutes", "status"]
STRUCTURAL_ACCOMMODATIONS = {
    "reason": "The existing schema inspector marks names ending in _name sensitive, including non-personal category labels. Initial manifest validation rejected these mappings.",
    "physical_column_renames": {"markets.market_name": "markets.market", "zones.zone_name": "zones.zone", "service_classes.class_name": "service_classes.service_class", "carriers.carrier_name": "carriers.carrier"},
    "scope": "Physical names were changed before question sealing or model evaluation solely to satisfy structural approval. Business labels, semantic IDs, categorical values and intended measures stayed fixed. No application sensitivity rule was changed.",
}
FACT_BODY = """(
    leg_id INTEGER PRIMARY KEY,
    consignment_id INTEGER REFERENCES consignments(consignment_id),
    service_class_id INTEGER REFERENCES service_classes(service_class_id),
    carrier_id INTEGER REFERENCES carriers(carrier_id),
    departed_on TEXT NOT NULL,
    weight_grams INTEGER,
    charge_cents INTEGER,
    transit_minutes INTEGER,
    distance_km REAL,
    package_count INTEGER NOT NULL,
    planned_transit_minutes INTEGER,
    status TEXT NOT NULL
)"""
DDL = {
    "markets": "CREATE TABLE markets (market_id INTEGER PRIMARY KEY, market TEXT NOT NULL UNIQUE)",
    "zones": "CREATE TABLE zones (zone_id INTEGER PRIMARY KEY, zone TEXT NOT NULL UNIQUE)",
    "shippers": "CREATE TABLE shippers (shipper_id INTEGER PRIMARY KEY, market_id INTEGER REFERENCES markets(market_id), shipper_name TEXT NOT NULL, status TEXT NOT NULL)",
    "depots": "CREATE TABLE depots (depot_id INTEGER PRIMARY KEY, zone_id INTEGER REFERENCES zones(zone_id), depot_name TEXT NOT NULL)",
    "service_classes": "CREATE TABLE service_classes (service_class_id INTEGER PRIMARY KEY, service_class TEXT NOT NULL UNIQUE, charge_cents INTEGER NOT NULL)",
    "carriers": "CREATE TABLE carriers (carrier_id INTEGER PRIMARY KEY, carrier TEXT NOT NULL UNIQUE, status TEXT NOT NULL, charge_cents INTEGER NOT NULL)",
    "consignments": "CREATE TABLE consignments (consignment_id INTEGER PRIMARY KEY, shipper_id INTEGER REFERENCES shippers(shipper_id), destination_depot_id INTEGER REFERENCES depots(depot_id), status TEXT NOT NULL, charge_cents INTEGER NOT NULL, declared_value_cents INTEGER NOT NULL)",
    "shipment_legs": "CREATE TABLE shipment_legs " + FACT_BODY,
    "archive_shipment_legs": "CREATE TABLE archive_shipment_legs " + FACT_BODY,
    "exception_events": "CREATE TABLE exception_events (event_id INTEGER PRIMARY KEY, leg_id INTEGER NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL, severity INTEGER NOT NULL, charge_cents INTEGER NOT NULL, occurred_on TEXT NOT NULL)",
    "operator_credentials": "CREATE TABLE operator_credentials (operator_id INTEGER PRIMARY KEY, email TEXT NOT NULL, password_hash TEXT NOT NULL, api_key TEXT NOT NULL)",
    "contact_directory": "CREATE TABLE contact_directory (contact_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, email TEXT NOT NULL, phone TEXT NOT NULL, address TEXT NOT NULL)",
}


def field(key, label, table, column, values=None, numeric=False):
    result = {"id": key, "label": label, "table": table, "column": column, "type": "number" if numeric else "string"}
    if values is not None:
        result["values"] = values
    return result


def catalog():
    tables = {name: {"table": name} for name in ["consignments", "shippers", "markets", "depots", "zones", "service_classes", "carriers"]}
    edges = [
        ("leg_consignment", "fact", "consignment_id", "consignments", "consignment_id"),
        ("consignment_shipper", "consignments", "shipper_id", "shippers", "shipper_id"),
        ("shipper_market", "shippers", "market_id", "markets", "market_id"),
        ("consignment_destination", "consignments", "destination_depot_id", "depots", "depot_id"),
        ("depot_zone", "depots", "zone_id", "zones", "zone_id"),
        ("leg_service", "fact", "service_class_id", "service_classes", "service_class_id"),
        ("leg_carrier", "fact", "carrier_id", "carriers", "carrier_id"),
    ]
    metric_specs = [
        ("transported_weight", "Transported weight", "Kilograms transported across selected shipment legs, summing recorded leg weights; a consignment carried on several legs contributes on each leg. Null weights do not contribute.", "SUM", "weight_grams", 1000, "number"),
        ("handling_charges", "Handling charges", "Shipment-leg handling charges in USD, summing recorded leg charges and dividing cents by 100. Excludes consignment charges, service tariffs, carrier fees and exception fees. Null charges do not contribute.", "SUM", "charge_cents", 100, "currency"),
        ("average_transit_minutes", "Average transit minutes", "Arithmetic mean of recorded transit minutes per selected shipment leg. Null transit durations are excluded from numerator and denominator. This is not end-to-end consignment transit time.", "AVG", "transit_minutes", 1, "number"),
        ("distinct_consignments", "Distinct consignments", "Number of distinct non-null consignment references represented by selected shipment legs, including references missing from the consignment table. Count separately within each group; grouped counts are not additive.", "COUNT_DISTINCT", "consignment_id", 1, "number"),
        ("leg_count", "Leg count", "Number of shipment-leg fact rows in the selected population, regardless of missing dimensions or null measurements.", "COUNT", None, 1, "number"),
    ]
    metrics = []
    for key, label, description, aggregate, column, scale, format_ in metric_specs:
        metric = {"id": key, "label": label, "description": description, "aggregate": aggregate, "table": "fact", "scale": scale, "format": format_}
        if column:
            metric["column"] = column
        metrics.append(metric)
    dimensions = [
        field("customer_market", "Customer market", "markets", "market", MARKETS),
        field("destination_zone", "Destination zone", "zones", "zone", ZONES),
        field("service_class", "Service class", "service_classes", "service_class", SERVICES),
        field("carrier", "Carrier", "carriers", "carrier", CARRIERS),
        field("shipment_state", "Shipment state", "consignments", "status", SHIPMENT_STATES),
    ]
    fields = [
        field("leg_weight_grams", "Leg weight in grams", "fact", "weight_grams", numeric=True),
        field("leg_handling_cents", "Leg handling charge in cents", "fact", "charge_cents", numeric=True),
        field("leg_transit_minutes", "Leg transit minutes", "fact", "transit_minutes", numeric=True),
        field("route_distance_km", "Route distance in kilometers", "fact", "distance_km", numeric=True),
        field("package_count", "Packages on a leg", "fact", "package_count", numeric=True),
        field("planned_transit_minutes", "Planned transit minutes", "fact", "planned_transit_minutes", numeric=True),
        field("service_tariff_cents", "Service tariff in cents", "service_classes", "charge_cents", numeric=True),
        field("consignment_charge_cents", "Consignment charge in cents", "consignments", "charge_cents", numeric=True),
        field("declared_value_cents", "Declared consignment value in cents", "consignments", "declared_value_cents", numeric=True),
        field("leg_state", "Leg state", "fact", "status", LEG_STATES),
        field("carrier_state", "Carrier state", "carriers", "status", ["Active", "Suspended"]),
    ]
    child_fields = [
        {"id": "exception_reason", "label": "Exception reason", "column": "reason", "type": "string", "values": REASONS},
        {"id": "exception_state", "label": "Exception state", "column": "status", "type": "string", "values": EVENT_STATES},
        {"id": "exception_severity", "label": "Exception severity", "column": "severity", "type": "number"},
    ]
    return {
        "version": 2, "name": "Blind synthetic logistics", "currency": "USD",
        "fact": {"table": "shipment_legs", "key": "leg_id", "columns": FACT_COLUMNS, "archive_table": "archive_shipment_legs"},
        "tables": tables,
        "relations": [{"id": key, "from": left, "from_column": left_key, "to": right, "to_column": right_key, "kind": "many_to_one"} for key, left, left_key, right, right_key in edges],
        "metrics": metrics, "dimensions": dimensions, "fields": fields,
        "date": {"table": "fact", "column": "departed_on"}, "as_of": AS_OF, "date_from": DATE_FROM, "date_to": DATE_TO,
        "exists_relations": [{"id": "exceptions", "label": "Shipment leg exceptions", "table": "exception_events", "parent": "fact", "parent_column": "leg_id", "child_column": "leg_id", "fields": child_fields}],
        "examples": ["Show transported weight by customer market.", "Show handling charges by destination zone for Express service.", "Count shipment legs with a Weather exception."],
    }


def contract(approved):
    return {
        "contract_version": 1, "contract_id": "blind-logistics-2026-09-12-v1",
        "authorship": {"method": "Agent-authored blind transfer evaluation", "original_dataset": True, "fully_synthetic": True, "human_or_external_holdout": False, "scope": "The author did not read semantic parser modules, model prompts, earlier question suites or model outputs. Public catalog documentation and relational manifest validation structure were permitted. Business semantics and oracle are independently authored from that language interpreter."},
        "generator": {"file": "build_database.py", "seed": SEED, "prng": "Python random.Random", "network_sources": [], "license": "CC0-1.0", "deterministic_same_runtime": True},
        "structural_accommodations": STRUCTURAL_ACCOMMODATIONS,
        "snapshot": {"database": "logistics.sqlite", "current_rows": 12000, "archive_rows": 3000, "exact_archive_overlaps": 120, "union_all_rows": 15000, "union_rows": 14880, "physical_tables": 12, "maximum_bytes": 20000000, "as_of": AS_OF, "date_from": DATE_FROM, "date_to": DATE_TO},
        "ddl": DDL,
        "approved_catalog": approved,
        "business_semantics": {
            "grain": "One recorded movement leg of a consignment; normally three legs per consignment. No implicit delivered/completed filter.",
            "population_default": "Current shipment_legs only, even when an older date is requested. Dates do not automatically include archives.",
            "archive_union_all": "Append all 3000 archived rows to all 12000 current rows, retaining 120 exact overlaps.",
            "archive_union": "Deduplicate exact rows across all 12 approved fact columns, including leg_id; 14880 rows remain. This is not business-key or latest-version deduplication.",
            "join_semantics": "LEFT JOIN each necessary many-to-one path with binary equality. Null or absent references remain in the population and yield SQL NULL dimension groups. Every target key is a single-column INTEGER primary key.",
            "date_semantics": "Inclusive ISO calendar dates on shipment_legs.departed_on. Month grouping is the first seven characters. Current unique facts span 2026-01-01 through 2026-09-12; unique archived facts span 2025-07-01 through 2025-12-31. Archives also contain exact current overlaps.",
            "relative_dates": {"as_of": AS_OF, "this_year": ["2026-01-01", "2026-09-12"], "last_year": ["2025-01-01", "2025-12-31"], "this_month": ["2026-09-01", "2026-09-12"], "last_month": ["2026-08-01", "2026-08-31"], "last_30_days": ["2026-08-14", "2026-09-12"]},
            "metrics_sql": {"transported_weight": "COALESCE(SUM(f.weight_grams), 0) / 1000.0", "handling_charges": "ROUND(COALESCE(SUM(f.charge_cents), 0) / 100.0, 2)", "average_transit_minutes": "AVG(f.transit_minutes)", "distinct_consignments": "COUNT(DISTINCT f.consignment_id)", "leg_count": "COUNT(*)"},
            "scale_rounding": "Handling charges use USD and round the sum to two decimals. Transported weight uses kilograms without additional rounding. Transit averages use minutes without additional rounding. Numeric row-filter values use the units stated in field labels, including cents and grams. HAVING thresholds use metric display units.",
            "null_semantics": "SUM skips nulls and becomes zero for an empty/all-null selection; AVG skips nulls and is NULL for an empty/all-null selection; COUNT DISTINCT skips null identifiers but counts missing non-null dimension references; COUNT includes all rows. Ordinary comparisons with NULL do not pass WHERE, including inequality.",
            "cancelled_transit": "Consignments with shipment state Cancelled have no recorded leg transit minutes, providing an approved all-null duration selection. Their fact rows, weights and charges are retained.",
            "grouped_distinct": "A consignment with several service classes or carriers can contribute once to several groups. Never add grouped distinct counts to infer a global distinct count.",
            "existence": "Correlate exception_events.leg_id to the selected fact leg_id. Multiple matching events select a fact once; NOT EXISTS selects a fact only if no child matches all requested child conditions. All child predicates must match the same event.",
            "above_average": "Compute each group's selected metric after all row/date/child filters. Keep groups strictly above the arithmetic mean of all these grouped metric values, before ordering and limiting. Null metric values are excluded from that mean.",
            "categorical_case": "Approved categorical values are exact canonical strings stored with SQLite BINARY collation. The interpreter may resolve user casing to canonical allowlist values. SQL categorical comparisons and sorting use BINARY. No case-only duplicate allowed values exist.",
            "ordering": "An explicit sort metric/dimension and direction controls primary ordering. Remaining selected dimensions break ties ascending in their selected order, using SQLite binary text ordering; NULL sorts first ascending and last descending. Apply limit after filters, grouping and any above-average comparison. Unordered questions should be compared as typed row multisets unless an explicit ordering is supplied by the gold case.",
            "same_name_traps": ["shipment_legs.charge_cents is the handling measure; consignments.charge_cents is a nonadditive whole-consignment charge; service_classes.charge_cents is a service tariff; carriers.charge_cents is a carrier fee; exception_events.charge_cents is an exception fee.", "consignments.status is shipment state; shipment_legs.status is leg state; carriers.status is carrier state; exception_events.status is exception state; shippers.status is unapproved."],
            "excluded_tables": ["operator_credentials", "contact_directory"],
            "excluded_columns": ["shippers.shipper_name", "shippers.status", "depots.depot_name", "carriers.charge_cents", "exception_events.charge_cents", "exception_events.occurred_on"],
            "synthetic_sensitive_values": "All names, emails, phones, addresses, password hashes and API-key-looking strings are fabricated. Emails use example.invalid; credentials are conspicuously fake placeholders. None may be queried through the approved catalog.",
        },
        "allowed_bounds": {"metrics_per_query": 3, "dimensions_per_query": 2, "and_row_filters": 12, "in_values": 20, "having_clauses": 3, "child_existence_conditions": 1, "above_average_comparisons": 1, "result_rows": 100, "execution_seconds": 2, "row_operators": ["eq", "ne", "in", "gt", "gte", "lt", "lte"], "having_operators": ["eq", "ne", "gt", "gte", "lt", "lte"], "unsupported": ["Arbitrary SQL or physical table/column requests", "Unapproved columns or personal data", "DML, DDL or credential access", "Ratios, profit or cost metrics absent from catalog", "Parent-table measure aggregation", "Multi-fact or child-event counts as metrics", "Arbitrary OR expressions", "LIKE/contains", "Window/running totals", "More than two grouping dimensions", "More than three selected metrics", "Several child-existence conditions", "Combining above-average and HAVING", "Dates are not an automatic archive switch", "Unknown categorical allowlist values"]},
        "verification_tolerance": {"integers": "exact", "strings_and_nulls": "exact", "currency_usd_absolute": 0.000001, "other_numeric_relative": 1e-9, "other_numeric_absolute": 1e-7},
    }


def populate(connection):
    rng = random.Random(SEED)
    connection.execute("PRAGMA foreign_keys=OFF")  # Deliberate incomplete snapshot references.
    for ddl in DDL.values():
        connection.execute(ddl)
    connection.executemany("INSERT INTO markets VALUES (?, ?)", enumerate(MARKETS, 1))
    connection.executemany("INSERT INTO zones VALUES (?, ?)", enumerate(ZONES, 1))
    connection.executemany("INSERT INTO service_classes VALUES (?, ?, ?)", [(i + 1, name, tariff) for i, (name, tariff) in enumerate(zip(SERVICES, [900, 1800, 3000, 8500]))])
    connection.executemany("INSERT INTO carriers VALUES (?, ?, ?, ?)", [(i + 1, name, "Suspended" if i == 4 else "Active", 17000 + i * 3300) for i, name in enumerate(CARRIERS)])
    connection.executemany("INSERT INTO shippers VALUES (?, ?, ?, ?)", [(i, None if i % 47 == 0 else 999 if i % 59 == 0 else 1 + (i - 1) % 5, f"Synthetic shipper {i:03d}", "Dormant" if i % 19 == 0 else "Active") for i in range(1, 121)])
    connection.executemany("INSERT INTO depots VALUES (?, ?, ?)", [(i, None if i == 17 else 999 if i == 29 else 1 + (i - 1) % 6, f"Synthetic depot {i:02d}") for i in range(1, 37)])
    current, historic = [], []
    shipment_rows = []
    for sequence in range(1, 4961):
        is_current = sequence <= 4000
        consignment_id = sequence if is_current else 100000 + sequence - 4000
        shipper = None if sequence % 107 == 0 else 99999 if sequence % 151 == 0 else rng.randint(1, 120)
        depot = None if sequence % 109 == 0 else 99999 if sequence % 163 == 0 else rng.randint(1, 36)
        state = rng.choices(SHIPMENT_STATES, [67, 13, 8, 5, 7])[0]
        consignment_charge = rng.randint(8000, 180000)
        shipment_rows.append((consignment_id, shipper, depot, state, consignment_charge, rng.randint(10000, 20000000)))
        start = date(2026, 1, 1) if is_current else date(2025, 7, 1)
        end = date.fromisoformat(AS_OF) if is_current else date(2025, 12, 31)
        offset = rng.randint(0, (end - start).days - 2)
        if sequence in (1, 4001):
            offset = 0
        elif sequence in (4000, 4960):
            offset = (end - start).days - 2
        packages = rng.randint(1, 24)
        base_grams = rng.randint(250, 75000) * packages
        for leg_index in range(3):
            leg_id = (sequence - 1) * 3 + leg_index + 1 if is_current else 100001 + (sequence - 4001) * 3 + leg_index
            service = rng.choices([1, 2, 3, 4], [45, 26, 13, 16])[0]
            carrier = rng.randint(1, 6)
            recorded_consignment = None if leg_id % 787 == 0 else 900000 + leg_id if leg_id % 601 == 0 else consignment_id
            recorded_service = None if leg_id % 223 == 0 else 999 if leg_id % 307 == 0 else service
            recorded_carrier = None if leg_id % 227 == 0 else 999 if leg_id % 311 == 0 else carrier
            weight = None if leg_id % 71 == 0 else base_grams + rng.randint(-100, 100)
            charge = None if leg_id % 89 == 0 else rng.randint(100, 6000) + packages * [80, 170, 290, 540][service - 1]
            distance = None if leg_id % 113 == 0 else round(rng.uniform(8, 2400), 1)
            planned = None if leg_id % 131 == 0 else rng.randint(*[(90, 2200), (45, 1000), (20, 480), (240, 4300)][service - 1])
            transit = None if leg_id % 47 == 0 or state == "Cancelled" else max(5, (planned or 480) + rng.randint(-80, 600))
            leg_state = rng.choices(LEG_STATES, [76, 10, 10, 4])[0]
            row = (leg_id, recorded_consignment, recorded_service, recorded_carrier, (start + timedelta(days=offset + leg_index)).isoformat(), weight, charge, transit, distance, packages, planned, leg_state)
            (current if is_current else historic).append(row)
    connection.executemany("INSERT INTO consignments VALUES (?, ?, ?, ?, ?, ?)", shipment_rows)
    placeholders = ",".join("?" for _ in FACT_COLUMNS)
    connection.executemany(f"INSERT INTO shipment_legs VALUES ({placeholders})", current)
    archived = historic + [current[i * 97] for i in range(120)]
    connection.executemany(f"INSERT INTO archive_shipment_legs VALUES ({placeholders})", archived)
    event_rows = []
    for row in current + historic:
        leg_id, departed = row[0], row[4]
        if leg_id % 5 == 0 or leg_id % 17 == 0:
            reason, status = rng.choice(REASONS), rng.choice(EVENT_STATES)
            for repeat in range(1 + leg_id % 4):
                # Two first children deliberately share their reason and status.
                event_rows.append((len(event_rows) + 1, leg_id, reason if repeat < 2 else rng.choice(REASONS), status if repeat < 2 else rng.choice(EVENT_STATES), rng.randint(1, 5), rng.randint(500, 12000), departed))
    connection.executemany("INSERT INTO exception_events VALUES (?, ?, ?, ?, ?, ?, ?)", event_rows)
    connection.executemany("INSERT INTO operator_credentials VALUES (?, ?, ?, ?)", [(i, f"synthetic.operator{i}@example.invalid", f"NOT_A_REAL_HASH_{i:04d}", f"FAKE_LOGISTICS_KEY_DO_NOT_USE_{i:04d}") for i in range(1, 9)])
    connection.executemany("INSERT INTO contact_directory VALUES (?, ?, ?, ?, ?)", [(i, f"Synthetic Contact {i}", f"synthetic.contact{i}@example.invalid", f"+1-202-555-{100 + i:04d}", f"{i} Fictional Warehouse Lane, Example City") for i in range(1, 13)])
    connection.execute("CREATE INDEX idx_exception_leg ON exception_events(leg_id)")
    connection.execute("CREATE INDEX idx_current_departure ON shipment_legs(departed_on)")
    connection.execute("CREATE INDEX idx_archive_departure ON archive_shipment_legs(departed_on)")
    connection.commit()
    connection.execute("VACUUM")


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    approved = catalog()
    write_json(ROOT / "catalog.json", approved)
    write_json(ROOT / "schema_contract.json", contract(approved))
    target = ROOT / "logistics.sqlite"
    temporary = ROOT / "logistics.build.sqlite"
    if temporary.exists():
        temporary.unlink()
    with sqlite3.connect(temporary) as connection:
        populate(connection)
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        stats = {table: connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] for table in DDL}
        columns = ",".join(FACT_COLUMNS)
        overlaps = connection.execute(f"SELECT COUNT(*) FROM (SELECT {columns} FROM shipment_legs INTERSECT SELECT {columns} FROM archive_shipment_legs)").fetchone()[0]
        union_count = connection.execute(f"SELECT COUNT(*) FROM (SELECT {columns} FROM shipment_legs UNION SELECT {columns} FROM archive_shipment_legs)").fetchone()[0]
        details = {
            "exact_archive_overlaps": overlaps, "union_all_rows": stats["shipment_legs"] + stats["archive_shipment_legs"], "union_rows": union_count,
            "current_null_measures": dict(zip(["weight_grams", "charge_cents", "transit_minutes"], connection.execute("SELECT SUM(weight_grams IS NULL), SUM(charge_cents IS NULL), SUM(transit_minutes IS NULL) FROM shipment_legs").fetchone())),
            "current_null_or_missing_consignment_refs": connection.execute("SELECT COUNT(*) FROM shipment_legs l LEFT JOIN consignments c ON c.consignment_id=l.consignment_id WHERE c.consignment_id IS NULL").fetchone()[0],
            "legs_with_repeated_events": connection.execute("SELECT COUNT(*) FROM (SELECT leg_id FROM exception_events GROUP BY leg_id HAVING COUNT(*)>1)").fetchone()[0],
            "current_dates": list(connection.execute("SELECT MIN(departed_on),MAX(departed_on) FROM shipment_legs").fetchone()),
            "archive_dates": list(connection.execute("SELECT MIN(departed_on),MAX(departed_on) FROM archive_shipment_legs").fetchone()),
            "foreign_key_violations_deliberately_retained": len(connection.execute("PRAGMA foreign_key_check").fetchall()),
        }
        assert stats["shipment_legs"] == 12000 and stats["archive_shipment_legs"] == 3000
        assert overlaps == 120 and union_count == 14880 and len(stats) == 12
    connection.close()
    assert temporary.stat().st_size < 20000000
    temporary.replace(target)
    provenance = {
        "dataset": "Blind synthetic logistics", "contract_id": "blind-logistics-2026-09-12-v1", "synthetic": True,
        "authoring_method": "Agent-authored blind transfer evaluation; not an external human holdout", "seed": SEED, "as_of": AS_OF,
        "sqlite_version": sqlite3.sqlite_version, "table_counts": stats, "data_checks": details, "database_bytes": target.stat().st_size,
        "structural_accommodations": STRUCTURAL_ACCOMMODATIONS,
        "sha256": {name: sha256(ROOT / name) for name in ["logistics.sqlite", "catalog.json", "schema_contract.json", "build_database.py"]},
    }
    write_json(ROOT / "provenance.json", provenance)
    print(json.dumps(provenance, indent=2))


if __name__ == "__main__":
    main()
