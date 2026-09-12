"""Create a small third-domain SQLite fixture for the real upload UI test."""
import sqlite3
from contextlib import closing
from pathlib import Path

path = Path(__file__).resolve().parents[1] / "artifacts" / "inventory-e2e.sqlite"
path.parent.mkdir(exist_ok=True)
with closing(sqlite3.connect(path)) as connection:
    connection.execute("CREATE TABLE IF NOT EXISTS stock (sku_id INTEGER PRIMARY KEY, depot TEXT, units INTEGER)")
    connection.execute("DELETE FROM stock")
    connection.executemany("INSERT INTO stock VALUES (?, ?, ?)", [(1, "Central", 10), (2, "Central", 25), (3, "Harbor", 40), (4, "Harbor", 15), (5, "Airport", 60)])
    connection.commit()
print(path)
