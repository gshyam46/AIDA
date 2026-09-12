"""Two independent synthetic warehouses and independently authored SQL oracles.

Fixtures deliberately contain nullable measures/foreign keys, same-named status
columns, overlapping archive rows, and duplicate child events. No production
data is used. Query expectations are SQL written here, not compiler output.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def _metric(key: str, label: str, column: str | None, aggregate: str, description: str, scale: int = 1) -> dict[str, Any]:
    result = {"id": key, "label": label, "table": "fact", "aggregate": aggregate, "description": description, "scale": scale, "format": "currency" if scale == 100 else "number"}
    if column is not None:
        result["column"] = column
    return result


def _dimension(key: str, label: str, table: str, column: str, values: list[str] | None = None, type_: str = "string") -> dict[str, Any]:
    result = {"id": key, "label": label, "table": table, "column": column, "type": type_}
    if values:
        result["values"] = values
    return result


def _edge(key: str, source: str, foreign: str, target: str, primary: str) -> dict[str, str]:
    return {"id": key, "from": source, "from_column": foreign, "to": target, "to_column": primary, "kind": "many_to_one"}


def warehouse_manifest() -> dict[str, Any]:
    return {
        "version": 2, "name": "Retail warehouse", "currency": "USD", "as_of": "2026-09-12", "date_from": "2025-01-01", "date_to": "2026-08-28",
        "fact": {"table": "order_items", "key": "line_id", "columns": ["line_id", "order_id", "product_id", "quantity", "line_total_cents"], "archive_table": "archived_order_items"},
        "tables": {"orders": {"table": "orders"}, "customers": {"table": "customers"}, "regions": {"table": "regions"}, "products": {"table": "products"}, "categories": {"table": "categories"}},
        "relations": [_edge("line_order", "fact", "order_id", "orders", "order_id"), _edge("order_customer", "orders", "customer_id", "customers", "customer_id"), _edge("customer_region", "customers", "region_id", "regions", "region_id"), _edge("line_product", "fact", "product_id", "products", "product_id"), _edge("product_category", "products", "category_id", "categories", "category_id")],
        "metrics": [
            _metric("revenue", "Revenue", "line_total_cents", "SUM", "Sum of order-line totals in dollars, before refunds, across every order status unless filtered.", 100),
            _metric("units", "Units", "quantity", "SUM", "Sum of quantities on order lines."),
            _metric("orders", "Orders", "order_id", "COUNT_DISTINCT", "Distinct orders represented by selected order lines; one order may belong to multiple product groups."),
            _metric("average_line_value", "Average line value", "line_total_cents", "AVG", "Average non-null order-line total in dollars, not average order value.", 100),
            _metric("line_count", "Order lines", None, "COUNT", "Number of selected order-line records, including duplicate rows when UNION ALL is requested."),
        ],
        "dimensions": [
            _dimension("region", "Region", "regions", "region_label", ["North", "South", "East", "West"]),
            _dimension("category", "Category", "categories", "category_label", ["Electronics", "Home", "Outdoor", "Office"]),
            _dimension("order_status", "Order status", "orders", "status", ["Completed", "Processing", "Cancelled"]),
            _dimension("channel", "Sales channel", "orders", "channel", ["Online", "Store", "Partner"]),
            _dimension("customer_segment", "Customer segment", "customers", "segment", ["Consumer", "Business"]),
        ],
        "fields": [_dimension("line_quantity", "Line quantity", "fact", "quantity", type_="number")],
        "date": {"table": "orders", "column": "ordered_on"},
        "exists_relations": [{"id": "returns", "label": "Returns", "table": "returns", "parent": "fact", "parent_column": "line_id", "child_column": "line_id", "fields": [{"id": "return_reason", "label": "Return reason", "column": "reason", "type": "string", "values": ["Damaged", "Wrong item", "Changed mind"]}, {"id": "return_status", "label": "Return status", "column": "status", "type": "string", "values": ["Approved", "Pending"]}]}],
        "examples": ["Revenue and units by region", "Revenue by region and category for completed orders", "Categories with revenue above 30000", "Revenue by category for lines with damaged returns", "Revenue by region for lines without returns", "Regions with above-average revenue", "Revenue by month including current and archived records", "Order lines including current and archived records with exact duplicates removed"],
    }


def billing_manifest() -> dict[str, Any]:
    return {
        "version": 2, "name": "SaaS billing", "currency": "USD", "as_of": "2026-09-12", "date_from": "2025-01-01", "date_to": "2026-08-28",
        "fact": {"table": "invoice_lines", "key": "entry_id", "columns": ["entry_id", "invoice_id", "subscription_id", "seat_quantity", "amount_cents"], "archive_table": "archived_invoice_lines"},
        "tables": {"invoices": {"table": "invoices"}, "accounts": {"table": "accounts"}, "subscriptions": {"table": "subscriptions"}, "plans": {"table": "plans"}},
        "relations": [_edge("line_invoice", "fact", "invoice_id", "invoices", "invoice_id"), _edge("invoice_account", "invoices", "account_id", "accounts", "account_id"), _edge("line_subscription", "fact", "subscription_id", "subscriptions", "subscription_id"), _edge("subscription_plan", "subscriptions", "plan_id", "plans", "plan_id")],
        "metrics": [
            _metric("billed_amount", "Billed amount", "amount_cents", "SUM", "Sum of invoice-line amounts in dollars before credits, across every invoice status unless filtered.", 100),
            _metric("seats", "Seats", "seat_quantity", "SUM", "Sum of billed seat quantities on invoice lines."),
            _metric("invoices", "Invoices", "invoice_id", "COUNT_DISTINCT", "Distinct invoices represented by selected lines; one invoice may contain several plans."),
            _metric("average_line_amount", "Average line amount", "amount_cents", "AVG", "Average non-null invoice-line amount in dollars, not average invoice total.", 100),
            _metric("billing_lines", "Billing lines", None, "COUNT", "Number of selected invoice-line records."),
        ],
        "dimensions": [
            _dimension("segment", "Account segment", "accounts", "segment", ["Startup", "Growth", "Enterprise"]),
            _dimension("plan_name", "Plan tier", "plans", "tier", ["Basic", "Pro", "Enterprise"]),
            _dimension("invoice_status", "Invoice status", "invoices", "status", ["Paid", "Open", "Void"]),
            _dimension("country", "Billing country", "accounts", "country", ["US", "UK", "India", "Germany"]),
        ],
        "fields": [_dimension("line_seats", "Seats on each line", "fact", "seat_quantity", type_="number")],
        "date": {"table": "invoices", "column": "issued_on"},
        "exists_relations": [{"id": "credits", "label": "Credits", "table": "credits", "parent": "fact", "parent_column": "entry_id", "child_column": "entry_id", "fields": [{"id": "credit_reason", "label": "Credit reason", "column": "reason", "type": "string", "values": ["Service issue", "Billing correction", "Promotion"]}, {"id": "credit_status", "label": "Credit status", "column": "status", "type": "string", "values": ["Applied", "Pending"]}]}],
        "examples": ["Billed amount and seats by account segment", "Billed amount by plan tier and billing country for paid invoices", "Plan tiers with billed amount above 100000", "Billed amount by account segment for lines with service issue credits", "Billed amount by plan tier for lines without credits", "Account segments with above-average billed amount", "Billed amount by month including current and archived records", "Billing lines including current and archived records with exact duplicates removed"],
    }


def _warehouse(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript("""
        CREATE TABLE regions(region_id INTEGER PRIMARY KEY, region_label TEXT NOT NULL, status TEXT);
        CREATE TABLE customers(customer_id INTEGER PRIMARY KEY, region_id INTEGER REFERENCES regions(region_id), segment TEXT, status TEXT, customer_name TEXT, email TEXT);
        CREATE TABLE categories(category_id INTEGER PRIMARY KEY, category_label TEXT, status TEXT);
        CREATE TABLE products(product_id INTEGER PRIMARY KEY, category_id INTEGER REFERENCES categories(category_id), status TEXT, price_cents INTEGER, product_description TEXT);
        CREATE TABLE orders(order_id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(customer_id), ordered_on TEXT, status TEXT, channel TEXT, total_cents INTEGER, shipping_address TEXT);
        CREATE TABLE order_items(line_id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES orders(order_id), product_id INTEGER REFERENCES products(product_id), quantity INTEGER, line_total_cents INTEGER);
        CREATE TABLE archived_order_items(line_id INTEGER, order_id INTEGER, product_id INTEGER, quantity INTEGER, line_total_cents INTEGER);
        CREATE TABLE returns(return_id INTEGER PRIMARY KEY, line_id INTEGER REFERENCES order_items(line_id), reason TEXT, status TEXT, refund_cents INTEGER, notes TEXT);
        CREATE TABLE customer_secrets(customer_id INTEGER PRIMARY KEY, access_token TEXT, email TEXT);
        CREATE INDEX return_line ON returns(line_id);
        CREATE INDEX item_order ON order_items(order_id);
        """)
        connection.executemany("INSERT INTO regions VALUES(?,?,?)", [(i + 1, name, "Enabled") for i, name in enumerate(["North", "South", "East", "West"])])
        connection.executemany("INSERT INTO categories VALUES(?,?,?)", [(i + 1, name, "Listed") for i, name in enumerate(["Electronics", "Home", "Outdoor", "Office"])])
        connection.executemany("INSERT INTO customers VALUES(?,?,?,?,?,?)", [(i, 1 + i % 4, "Business" if i % 3 == 0 else "Consumer", "Active" if i % 5 else "Dormant", f"Synthetic customer {i}", f"synthetic-{i}@example.invalid") for i in range(1, 81)])
        connection.executemany("INSERT INTO customer_secrets VALUES(?,?,?)", [(i, f"synthetic-secret-{i}", f"synthetic-{i}@example.invalid") for i in range(1, 81)])
        connection.executemany("INSERT INTO products VALUES(?,?,?,?,?)", [(i, 1 + i % 4, "Available" if i % 7 else "Retired", 500 + i * 173, f"Synthetic product {i}") for i in range(1, 49)])
        lines, archive, orders, return_rows = [], [], [], []
        line_id = 0
        for order_id in range(1, 801):
            archived = order_id > 640
            year = 2025 if archived else 2026
            month = 1 + (order_id * 7) % 8
            total = 0
            order_lines = []
            for position in range(1 + order_id % 4):
                line_id += 1
                product = 1 + (order_id * 3 + position * 7) % 48
                quantity = 1 + (order_id + position) % 6
                amount = None if line_id % 113 == 0 else quantity * (500 + product * 173) - (order_id % 4) * 25
                record = (line_id, order_id, None if line_id % 97 == 0 else product, quantity, amount)
                order_lines.append(record)
                total += amount or 0
                if line_id % 7 == 0:
                    for event in range(1 + line_id % 3):
                        return_rows.append((len(return_rows) + 1, line_id, ["Damaged", "Wrong item", "Changed mind"][(line_id + event) % 3], "Approved" if event % 2 == 0 else "Pending", (amount or 0) // 3, "Synthetic event only"))
            (archive if archived else lines).extend(order_lines)
            orders.append((order_id, None if order_id % 157 == 0 else 1 + order_id % 80, f"{year}-{month:02d}-{1 + order_id % 28:02d}", ["Completed", "Processing", "Cancelled"][order_id % 3], ["Online", "Store", "Partner"][(order_id // 3) % 3], total, "Synthetic address"))
        archive.extend(lines[:12])  # Same fact identity and values: UNION differs from UNION ALL.
        connection.executemany("INSERT INTO orders VALUES(?,?,?,?,?,?,?)", orders)
        connection.executemany("INSERT INTO order_items VALUES(?,?,?,?,?)", lines)
        connection.executemany("INSERT INTO archived_order_items VALUES(?,?,?,?,?)", archive)
        connection.executemany("INSERT INTO returns VALUES(?,?,?,?,?,?)", return_rows)
        connection.commit()
    finally:
        connection.close()


def _billing(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript("""
        CREATE TABLE accounts(account_id INTEGER PRIMARY KEY, segment TEXT, country TEXT, status TEXT, account_name TEXT, billing_email TEXT);
        CREATE TABLE plans(plan_id INTEGER PRIMARY KEY, tier TEXT, status TEXT, price_cents INTEGER);
        CREATE TABLE subscriptions(subscription_id INTEGER PRIMARY KEY, plan_id INTEGER REFERENCES plans(plan_id), status TEXT);
        CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, account_id INTEGER REFERENCES accounts(account_id), issued_on TEXT, status TEXT, amount_cents INTEGER, payment_token TEXT);
        CREATE TABLE invoice_lines(entry_id INTEGER PRIMARY KEY, invoice_id INTEGER REFERENCES invoices(invoice_id), subscription_id INTEGER REFERENCES subscriptions(subscription_id), seat_quantity INTEGER, amount_cents INTEGER);
        CREATE TABLE archived_invoice_lines(entry_id INTEGER, invoice_id INTEGER, subscription_id INTEGER, seat_quantity INTEGER, amount_cents INTEGER);
        CREATE TABLE credits(credit_id INTEGER PRIMARY KEY, entry_id INTEGER REFERENCES invoice_lines(entry_id), reason TEXT, status TEXT, amount_cents INTEGER, comments TEXT);
        CREATE TABLE authentication_events(event_id INTEGER PRIMARY KEY, account_id INTEGER, ip_address TEXT);
        CREATE INDEX credit_entry ON credits(entry_id);
        CREATE INDEX billing_invoice ON invoice_lines(invoice_id);
        """)
        connection.executemany("INSERT INTO accounts VALUES(?,?,?,?,?,?)", [(i, ["Startup", "Growth", "Enterprise"][i % 3], ["US", "UK", "India", "Germany"][i % 4], "Active" if i % 9 else "Suspended", f"Synthetic account {i}", f"billing-{i}@example.invalid") for i in range(1, 73)])
        connection.executemany("INSERT INTO plans VALUES(?,?,?,?)", [(1, "Basic", "Published", 1500), (2, "Pro", "Published", 4500), (3, "Enterprise", "Retired", 12000)])
        connection.executemany("INSERT INTO subscriptions VALUES(?,?,?)", [(i, 1 + i % 3, "Active" if i % 7 else "Cancelled") for i in range(1, 97)])
        connection.executemany("INSERT INTO authentication_events VALUES(?,?,?)", [(i, i, "192.0.2.1") for i in range(1, 73)])
        invoices, lines, archive, credits = [], [], [], []
        entry_id = 0
        for invoice_id in range(1, 601):
            archived = invoice_id > 480
            year = 2025 if archived else 2026
            month = 1 + (invoice_id * 5) % 8
            total = 0
            invoice_lines = []
            for position in range(2 + invoice_id % 3):
                entry_id += 1
                subscription = 1 + (invoice_id * 5 + position * 11) % 96
                seats = 1 + (invoice_id * 3 + position) % 24
                amount = None if entry_id % 107 == 0 else seats * [1500, 4500, 12000][subscription % 3]
                invoice_lines.append((entry_id, invoice_id, None if entry_id % 131 == 0 else subscription, seats, amount))
                total += amount or 0
                if entry_id % 11 == 0:
                    for event in range(1 + entry_id % 3):
                        credits.append((len(credits) + 1, entry_id, ["Service issue", "Billing correction", "Promotion"][(entry_id + event) % 3], "Applied" if event % 2 == 0 else "Pending", (amount or 0) // 4, "Synthetic credit only"))
            (archive if archived else lines).extend(invoice_lines)
            invoices.append((invoice_id, None if invoice_id % 149 == 0 else 1 + invoice_id % 72, f"{year}-{month:02d}-{1 + invoice_id % 28:02d}", ["Paid", "Open", "Void"][(invoice_id // 2) % 3], total, "synthetic-payment-token"))
        archive.extend(lines[:10])
        connection.executemany("INSERT INTO invoices VALUES(?,?,?,?,?,?)", invoices)
        connection.executemany("INSERT INTO invoice_lines VALUES(?,?,?,?,?)", lines)
        connection.executemany("INSERT INTO archived_invoice_lines VALUES(?,?,?,?,?)", archive)
        connection.executemany("INSERT INTO credits VALUES(?,?,?,?,?,?)", credits)
        connection.commit()
    finally:
        connection.close()


def ensure_relational_demos(directory: Path) -> list[dict[str, Any]]:
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    result = []
    for source_id, create, manifest in (("warehouse", _warehouse, warehouse_manifest), ("billing", _billing, billing_manifest)):
        path = directory / f"{source_id}.sqlite"
        if not path.exists():
            create(path)
        result.append({"id": source_id, "path": path, "manifest": manifest(), "synthetic": True})
    return result


def relational_plan(metrics: list[str], dimensions: list[str] | None = None, **changes: Any) -> dict[str, Any]:
    dimensions = dimensions or []
    result = {"version": 2, "metrics": metrics, "dimensions": dimensions, "filters": [], "having": [], "population": "primary", "set_operation": "union_all", "exists": None, "comparison": None, "date_from": None, "date_to": None, "sort": {"field": "month" if dimensions == ["month"] else metrics[0], "direction": "asc" if dimensions == ["month"] else "desc"}, "limit": 100}
    result.update(changes)
    return result


def relational_demo_cases() -> list[dict[str, Any]]:
    """Stable model evaluation cases with hand-written SQL and exact sort order."""
    cases = []

    def add(key: str, source: str, question: str, plan: dict[str, Any], oracle: str) -> None:
        cases.append({"id": key, "source_id": source, "question": question, "plan": plan, "oracle_sql": oracle, "oracle_parameters": {}})

    retail_region = "FROM order_items li LEFT JOIN orders o ON li.order_id=o.order_id LEFT JOIN customers cu ON o.customer_id=cu.customer_id LEFT JOIN regions r ON cu.region_id=r.region_id"
    retail_category = "FROM order_items li LEFT JOIN products p ON li.product_id=p.product_id LEFT JOIN categories c ON p.category_id=c.category_id"
    retail_both = retail_region + " LEFT JOIN products p ON li.product_id=p.product_id LEFT JOIN categories c ON p.category_id=c.category_id"
    billing_segment = "FROM invoice_lines li LEFT JOIN invoices i ON li.invoice_id=i.invoice_id LEFT JOIN accounts a ON i.account_id=a.account_id"
    billing_plan = "FROM invoice_lines li LEFT JOIN subscriptions s ON li.subscription_id=s.subscription_id LEFT JOIN plans p ON s.plan_id=p.plan_id"
    billing_both = billing_segment + " LEFT JOIN subscriptions s ON li.subscription_id=s.subscription_id LEFT JOIN plans p ON s.plan_id=p.plan_id"
    add("warehouse_join_multi_metric", "warehouse", "Revenue and units by region", relational_plan(["revenue", "units"], ["region"]), f"SELECT r.region_label AS region, ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2) AS revenue, COALESCE(SUM(li.quantity),0) AS units {retail_region} GROUP BY r.region_label ORDER BY revenue DESC,region ASC LIMIT 100")
    add("warehouse_two_join_paths", "warehouse", "Revenue by region and category for completed orders", relational_plan(["revenue"], ["region", "category"], filters=[{"field": "order_status", "op": "eq", "value": "Completed"}]), f"SELECT r.region_label AS region,c.category_label AS category,ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2) AS revenue {retail_both} WHERE o.status='Completed' GROUP BY r.region_label,c.category_label ORDER BY revenue DESC,region ASC,category ASC LIMIT 100")
    add("warehouse_having", "warehouse", "Categories with revenue above 30000", relational_plan(["revenue"], ["category"], having=[{"metric": "revenue", "op": "gt", "value": 30000}]), f"SELECT c.category_label AS category,ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2) AS revenue {retail_category} GROUP BY c.category_label HAVING ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2)>30000 ORDER BY revenue DESC,category ASC LIMIT 100")
    add("warehouse_exists", "warehouse", "Revenue by category for lines with damaged returns", relational_plan(["revenue"], ["category"], exists={"relation": "returns", "negate": False, "filters": [{"field": "return_reason", "op": "eq", "value": "Damaged"}]}), f"SELECT c.category_label AS category,ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2) AS revenue {retail_category} WHERE EXISTS(SELECT 1 FROM returns re WHERE re.line_id=li.line_id AND re.reason='Damaged') GROUP BY c.category_label ORDER BY revenue DESC,category ASC LIMIT 100")
    add("warehouse_not_exists", "warehouse", "Revenue by region for lines without returns", relational_plan(["revenue"], ["region"], exists={"relation": "returns", "negate": True, "filters": []}), f"SELECT r.region_label AS region,ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2) AS revenue {retail_region} WHERE NOT EXISTS(SELECT 1 FROM returns re WHERE re.line_id=li.line_id) GROUP BY r.region_label ORDER BY revenue DESC,region ASC LIMIT 100")
    add("warehouse_above_average", "warehouse", "Regions with above-average revenue", relational_plan(["revenue"], ["region"], comparison={"kind": "above_average", "metric": "revenue"}), f"WITH totals AS (SELECT r.region_label AS region,ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2) AS revenue {retail_region} GROUP BY r.region_label) SELECT region,revenue FROM totals WHERE revenue>(SELECT AVG(revenue) FROM totals) ORDER BY revenue DESC,region ASC LIMIT 100")
    add("warehouse_union_all", "warehouse", "Revenue by month including current and archived records", relational_plan(["revenue"], ["month"], population="all"), "WITH all_lines AS (SELECT line_id,order_id,product_id,quantity,line_total_cents FROM order_items UNION ALL SELECT line_id,order_id,product_id,quantity,line_total_cents FROM archived_order_items) SELECT substr(o.ordered_on,1,7) AS month,ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2) AS revenue FROM all_lines li LEFT JOIN orders o ON li.order_id=o.order_id GROUP BY substr(o.ordered_on,1,7) ORDER BY month ASC LIMIT 100")
    add("warehouse_union_distinct", "warehouse", "Order lines including current and archived records with exact duplicates removed", relational_plan(["line_count"], population="all", set_operation="union"), "WITH all_lines AS (SELECT line_id,order_id,product_id,quantity,line_total_cents FROM order_items UNION SELECT line_id,order_id,product_id,quantity,line_total_cents FROM archived_order_items) SELECT COUNT(1) AS line_count FROM all_lines")
    add("warehouse_in_numeric", "warehouse", "Units by category in North or West where line quantity is at least 3", relational_plan(["units"], ["category"], filters=[{"field": "region", "op": "in", "value": ["North", "West"]}, {"field": "line_quantity", "op": "gte", "value": 3}]), f"SELECT c.category_label AS category,COALESCE(SUM(li.quantity),0) AS units {retail_both} WHERE r.region_label IN ('North','West') AND li.quantity>=3 GROUP BY c.category_label ORDER BY units DESC,category ASC LIMIT 100")
    add("warehouse_distinct_grain", "warehouse", "Orders and units by category", relational_plan(["orders", "units"], ["category"]), f"SELECT c.category_label AS category,COUNT(DISTINCT li.order_id) AS orders,COALESCE(SUM(li.quantity),0) AS units {retail_category} GROUP BY c.category_label ORDER BY orders DESC,category ASC LIMIT 100")
    add("warehouse_dates", "warehouse", "Average line value by month from 2026-03-01 through 2026-06-30", relational_plan(["average_line_value"], ["month"], date_from="2026-03-01", date_to="2026-06-30"), "SELECT substr(o.ordered_on,1,7) AS month,ROUND(AVG(li.line_total_cents)/100.0,2) AS average_line_value FROM order_items li LEFT JOIN orders o ON li.order_id=o.order_id WHERE substr(o.ordered_on,1,10)>='2026-03-01' AND substr(o.ordered_on,1,10)<='2026-06-30' GROUP BY substr(o.ordered_on,1,7) ORDER BY month ASC LIMIT 100")
    add("warehouse_sort", "warehouse", "Bottom 2 categories by revenue excluding cancelled orders", relational_plan(["revenue"], ["category"], filters=[{"field": "order_status", "op": "ne", "value": "Cancelled"}], sort={"field": "revenue", "direction": "asc"}, limit=2), f"SELECT c.category_label AS category,ROUND(COALESCE(SUM(li.line_total_cents),0)/100.0,2) AS revenue {retail_both} WHERE o.status!='Cancelled' GROUP BY c.category_label ORDER BY revenue ASC,category ASC LIMIT 2")
    add("billing_join_multi_metric", "billing", "Billed amount and seats by account segment", relational_plan(["billed_amount", "seats"], ["segment"]), f"SELECT a.segment,ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2) AS billed_amount,COALESCE(SUM(li.seat_quantity),0) AS seats {billing_segment} GROUP BY a.segment ORDER BY billed_amount DESC,segment ASC LIMIT 100")
    add("billing_two_join_paths", "billing", "Billed amount by plan tier and billing country for paid invoices", relational_plan(["billed_amount"], ["plan_name", "country"], filters=[{"field": "invoice_status", "op": "eq", "value": "Paid"}]), f"SELECT p.tier AS plan_name,a.country,ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2) AS billed_amount {billing_both} WHERE i.status='Paid' GROUP BY p.tier,a.country ORDER BY billed_amount DESC,plan_name ASC,country ASC LIMIT 100")
    add("billing_having", "billing", "Plan tiers with billed amount above 100000", relational_plan(["billed_amount"], ["plan_name"], having=[{"metric": "billed_amount", "op": "gt", "value": 100000}]), f"SELECT p.tier AS plan_name,ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2) AS billed_amount {billing_plan} GROUP BY p.tier HAVING ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2)>100000 ORDER BY billed_amount DESC,plan_name ASC LIMIT 100")
    add("billing_exists", "billing", "Billed amount by account segment for lines with service issue credits", relational_plan(["billed_amount"], ["segment"], exists={"relation": "credits", "negate": False, "filters": [{"field": "credit_reason", "op": "eq", "value": "Service issue"}]}), f"SELECT a.segment,ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2) AS billed_amount {billing_segment} WHERE EXISTS(SELECT 1 FROM credits c WHERE c.entry_id=li.entry_id AND c.reason='Service issue') GROUP BY a.segment ORDER BY billed_amount DESC,segment ASC LIMIT 100")
    add("billing_not_exists", "billing", "Billed amount by plan tier for lines without credits", relational_plan(["billed_amount"], ["plan_name"], exists={"relation": "credits", "negate": True, "filters": []}), f"SELECT p.tier AS plan_name,ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2) AS billed_amount {billing_plan} WHERE NOT EXISTS(SELECT 1 FROM credits c WHERE c.entry_id=li.entry_id) GROUP BY p.tier ORDER BY billed_amount DESC,plan_name ASC LIMIT 100")
    add("billing_above_average", "billing", "Account segments with above-average billed amount", relational_plan(["billed_amount"], ["segment"], comparison={"kind": "above_average", "metric": "billed_amount"}), f"WITH totals AS (SELECT a.segment,ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2) AS billed_amount {billing_segment} GROUP BY a.segment) SELECT segment,billed_amount FROM totals WHERE billed_amount>(SELECT AVG(billed_amount) FROM totals) ORDER BY billed_amount DESC,segment ASC LIMIT 100")
    add("billing_union_all", "billing", "Billed amount by month including current and archived records", relational_plan(["billed_amount"], ["month"], population="all"), "WITH all_lines AS (SELECT entry_id,invoice_id,subscription_id,seat_quantity,amount_cents FROM invoice_lines UNION ALL SELECT entry_id,invoice_id,subscription_id,seat_quantity,amount_cents FROM archived_invoice_lines) SELECT substr(i.issued_on,1,7) AS month,ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2) AS billed_amount FROM all_lines li LEFT JOIN invoices i ON li.invoice_id=i.invoice_id GROUP BY substr(i.issued_on,1,7) ORDER BY month ASC LIMIT 100")
    add("billing_union_distinct", "billing", "Billing lines including current and archived records with exact duplicates removed", relational_plan(["billing_lines"], population="all", set_operation="union"), "WITH all_lines AS (SELECT entry_id,invoice_id,subscription_id,seat_quantity,amount_cents FROM invoice_lines UNION SELECT entry_id,invoice_id,subscription_id,seat_quantity,amount_cents FROM archived_invoice_lines) SELECT COUNT(1) AS billing_lines FROM all_lines")
    add("billing_in_numeric", "billing", "Seats by plan tier in US or India where seats on each line exceed 10", relational_plan(["seats"], ["plan_name"], filters=[{"field": "country", "op": "in", "value": ["US", "India"]}, {"field": "line_seats", "op": "gt", "value": 10}]), f"SELECT p.tier AS plan_name,COALESCE(SUM(li.seat_quantity),0) AS seats {billing_both} WHERE a.country IN ('US','India') AND li.seat_quantity>10 GROUP BY p.tier ORDER BY seats DESC,plan_name ASC LIMIT 100")
    add("billing_distinct_grain", "billing", "Invoices and seats by plan tier", relational_plan(["invoices", "seats"], ["plan_name"]), f"SELECT p.tier AS plan_name,COUNT(DISTINCT li.invoice_id) AS invoices,COALESCE(SUM(li.seat_quantity),0) AS seats {billing_plan} GROUP BY p.tier ORDER BY invoices DESC,plan_name ASC LIMIT 100")
    add("billing_dates", "billing", "Average line amount by month from 2026-02-01 through 2026-05-31", relational_plan(["average_line_amount"], ["month"], date_from="2026-02-01", date_to="2026-05-31"), "SELECT substr(i.issued_on,1,7) AS month,ROUND(AVG(li.amount_cents)/100.0,2) AS average_line_amount FROM invoice_lines li LEFT JOIN invoices i ON li.invoice_id=i.invoice_id WHERE substr(i.issued_on,1,10)>='2026-02-01' AND substr(i.issued_on,1,10)<='2026-05-31' GROUP BY substr(i.issued_on,1,7) ORDER BY month ASC LIMIT 100")
    add("billing_sort", "billing", "Bottom 2 plan tiers by billed amount excluding void invoices", relational_plan(["billed_amount"], ["plan_name"], filters=[{"field": "invoice_status", "op": "ne", "value": "Void"}], sort={"field": "billed_amount", "direction": "asc"}, limit=2), f"SELECT p.tier AS plan_name,ROUND(COALESCE(SUM(li.amount_cents),0)/100.0,2) AS billed_amount {billing_both} WHERE i.status!='Void' GROUP BY p.tier ORDER BY billed_amount ASC,plan_name ASC LIMIT 2")
    return cases
