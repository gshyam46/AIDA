"""Regression coverage for meaning that a permissive parser must not discard."""
import sqlite3

import pytest

from backend.core import analytics
from backend.core.analytics import AnalyticsEngine, ClarificationRequired, parse_question


@pytest.mark.parametrize("question", [
    "Count revenue by region",
    "Count completed revenue",
    "Sum orders",
    "Sum average order value",
    "Total average order value",
    "Average revenue by month",
    "Average sales by region",
    "How much orders",
    "Orders and orders in West",
    "Revenue and revenue in West",
    "Revenue in West across all regions",
    "Revenue in West from all regions",
    "Revenue in West region and region",
    "Revenue equals total",
    "Revenue per total",
    "Revenue by West",
    "Revenue breakdown",
    "Revenue distribution",
    "Revenue where",
    "Revenue by region and",
    "Revenue by region sorted by orders",
    "Revenue by region sorted by region descending",
    "Revenue by region sorted by region sorted by revenue",
    "Revenue by month sorted by revenue ascending descending",
    "Top 3 categories by revenue ascending",
    "Bottom 3 categories by revenue descending",
    "Top 3 categories by revenue sorted by category",
    "Top 3 categories by revenue sorted by revenue ascending",
    "Top 3 categories and bottom 2 categories by revenue",
    "Revenue by region ascending descending",
    "Revenue in West or East",
    "Revenue before March",
    "Revenue from 2025-06-01 and since 2025-07-01",
    "Revenue last 0 days",
    "Revenue after 9999-12-31",
])
def test_unsupported_meaning_requires_clarification(question):
    with pytest.raises(ClarificationRequired):
        parse_question(question)


@pytest.mark.parametrize("question,expected", [
    ("May I have total revenue?", {"metric": "revenue", "date_from": None, "date_to": None}),
    ("What's the total revenue this month?", {"metric": "revenue", "date_from": "2025-12-01", "date_to": "2025-12-31"}),
    ("Revenue by region sorted by region", {"dimension": "region", "sort": "dimension_asc"}),
    ("Revenue by month sorted by revenue", {"dimension": "month", "sort": "value_desc"}),
    ("Revenue by region sort by sales ascending", {"dimension": "region", "sort": "value_asc"}),
    ("Orders by status sorted by orders", {"metric": "orders", "dimension": "status", "sort": "value_desc"}),
    ("Top 3 categories by revenue sorted by revenue", {"dimension": "category", "sort": "value_desc", "limit": 3}),
    ("Average order amount by channel", {"metric": "average_order_value", "dimension": "channel"}),
    ("Average amount per order", {"metric": "average_order_value"}),
    ("Revenue by each region", {"dimension": "region"}),
    ("Revenue month by month", {"dimension": "month", "sort": "dimension_asc"}),
    ("Revenue by region where region equals West", {"dimension": "region", "filters": {"region": "West"}}),
    ("Orders where status = completed", {"metric": "orders", "filters": {"status": "Completed"}}),
    ("Revenue in the West region", {"filters": {"region": "West"}}),
    ("Revenue after 2025-06-01", {"date_from": "2025-06-02", "date_to": None}),
    ("Revenue before 2025-06-01", {"date_from": None, "date_to": "2025-05-31"}),
    ("Revenue on or after 2025-06-01", {"date_from": "2025-06-01", "date_to": None}),
    ("Revenue on or before 2025-06-01", {"date_from": None, "date_to": "2025-06-01"}),
    ("Revenue after 2025-06-01 and before 2025-07-01", {"date_from": "2025-06-02", "date_to": "2025-06-30"}),
])
def test_supported_constraints_keep_their_meaning(question, expected):
    plan = parse_question(question)
    assert {key: plan[key] for key in expected} == expected


def test_all_database_connections_close(tmp_path, monkeypatch):
    real_connect = sqlite3.connect
    connections = []

    class TrackedConnection(sqlite3.Connection):
        closed = False

        def close(self):
            self.closed = True
            super().close()

    def tracked_connect(*args, **kwargs):
        connection = real_connect(*args, factory=TrackedConnection, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(analytics.sqlite3, "connect", tracked_connect)
    engine = AnalyticsEngine(tmp_path / "demo.sqlite")
    engine.ensure_demo_data()
    engine.ensure_demo_data()
    assert engine.query("Count all orders")["success"]
    assert len(connections) == 3
    assert all(connection.closed for connection in connections)


def test_existing_unrelated_database_is_not_labeled_synthetic(tmp_path):
    path = tmp_path / "private.sqlite"
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA user_version = 1")
    finally:
        connection.close()
    engine = AnalyticsEngine(path)
    with pytest.raises(RuntimeError, match="not a compatible AIDA synthetic"):
        engine.ensure_demo_data()
    assert engine.query("Count orders")["success"] is False
