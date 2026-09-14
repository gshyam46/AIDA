"""Reproducible synthetic commerce seed and shared execution bounds.

The commerce demo is queried through its approved catalog (CatalogEngine). This
module only creates the owned synthetic database and defines its business labels.
"""
from __future__ import annotations

import random
import sqlite3
import threading
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path


DATE_FROM = "2025-01-01"
DATE_TO = "2025-12-31"
DATA_VERSION = 1
APPLICATION_ID = 0x41494441  # AIDA: prevents accidentally opening unrelated files.
MAX_ROWS = 100
MAX_SECONDS = 2.0
METRICS = {
    "revenue": {"label": "Revenue", "format": "currency",
                "description": "Sum of completed order amounts, in USD. Pending, cancelled and refunded orders contribute zero."},
    "orders": {"label": "Orders", "format": "number",
               "description": "Number of orders across every status, unless a status filter is applied."},
    "average_order_value": {"label": "Average order value", "format": "currency",
                            "description": "Average completed order amount, in USD. Returns null when no completed orders match."},
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


class ClarificationRequired(ValueError):
    """An input cannot be represented without inventing business meaning."""


class AnalyticsEngine:
    """Creates the owned synthetic commerce dataset once and never overwrites it."""

    def __init__(self, database_path: str | Path):
        self.database_path = Path(database_path).resolve()
        self._lock = threading.Lock()
        self.row_count = 0

    def ensure_demo_data(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if self.database_path.exists():
                with closing(sqlite3.connect(self.database_path.as_uri() + "?mode=ro", uri=True)) as connection:
                    version = connection.execute("PRAGMA user_version").fetchone()[0]
                    application = connection.execute("PRAGMA application_id").fetchone()[0]
                    if version != DATA_VERSION or application != APPLICATION_ID:
                        raise RuntimeError("This file is not a compatible AIDA synthetic demo database. Configure a new demo database path.")
                    self.row_count = connection.execute("SELECT COUNT(*) FROM analytics_orders").fetchone()[0]
                return
            randomizer = random.Random(1729)
            rows = []
            categories = DIMENSIONS["category"]["values"]
            regions = DIMENSIONS["region"]["values"]
            channels = DIMENSIONS["channel"]["values"]
            bases = {"Electronics": 21000, "Home": 12500, "Clothing": 6500, "Sports": 8500, "Beauty": 4200}
            for day_index in range(365):
                current = date(2025, 1, 1) + timedelta(days=day_index)
                daily_count = 14 + randomizer.randrange(12) + (7 if current.month >= 10 else 0)
                for _ in range(daily_count):
                    category = randomizer.choices(categories, weights=[30, 24, 22, 14, 10])[0]
                    region = randomizer.choices(regions, weights=[28, 23, 19, 30])[0]
                    channel = randomizer.choices(channels, weights=[58, 30, 12])[0]
                    status = randomizer.choices(DIMENSIONS["status"]["values"], weights=[80, 8, 7, 5])[0]
                    cents = bases[category] + randomizer.randrange(100, bases[category] * 2)
                    rows.append((len(rows) + 1, current.isoformat(), region, category, channel, status, cents))
            with closing(sqlite3.connect(self.database_path)) as connection, connection:
                connection.execute("CREATE TABLE analytics_orders (order_id INTEGER PRIMARY KEY, order_date TEXT NOT NULL, region TEXT NOT NULL, category TEXT NOT NULL, channel TEXT NOT NULL, status TEXT NOT NULL, amount_cents INTEGER NOT NULL CHECK(amount_cents >= 0))")
                connection.executemany("INSERT INTO analytics_orders VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
                connection.execute("CREATE INDEX ix_analytics_date ON analytics_orders(order_date)")
                connection.execute("CREATE INDEX ix_analytics_dimensions ON analytics_orders(region, category, channel, status)")
                connection.execute(f"PRAGMA user_version = {DATA_VERSION}")
                connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
            self.row_count = len(rows)
