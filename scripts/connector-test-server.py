"""Browser test harness: real API/compiler and an explicitly substituted SQLite remote.

Run only on loopback with disposable synthetic data. It verifies product wiring,
not vendor authentication/TLS. Production never imports this file.
"""
import os
import sys
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import sqlalchemy as sa
from cryptography.fernet import Fernet
import uvicorn
from backend.core import connectors
from backend.main import create_app

DIRECTORY = ROOT / "artifacts" / "connector-browser"
DIRECTORY.mkdir(parents=True, exist_ok=True)
os.environ["AIDA_CONNECTOR_KEY"] = Fernet.generate_key().decode()
os.environ["AIDA_CONNECTOR_HOSTS"] = "127.0.0.1:5432"
os.environ["AIDA_CONNECTOR_LOCAL_TEST"] = "1"
remote = sa.create_engine(sa.URL.create("sqlite", database=str(DIRECTORY / "remote.sqlite")))
with remote.begin() as db:
    db.exec_driver_sql("CREATE TABLE IF NOT EXISTS stock (item_id INTEGER PRIMARY KEY, category TEXT, quantity INTEGER, customer_email TEXT)")
    db.exec_driver_sql("DELETE FROM stock")
    db.exec_driver_sql("INSERT INTO stock VALUES (1, 'Garden', 11, 'synthetic@example.test'), (2, 'Tools', 8, 'synthetic@example.test')")


@contextmanager
def test_session(spec):
    with remote.connect() as db:
        yield db


connectors.remote_session = test_session
# A unique workspace avoids reusing encrypted credentials with an ephemeral test key.
import uuid
app = create_app(DIRECTORY / uuid.uuid4().hex, public_demo=False)
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("AIDA_CONNECTOR_TEST_PORT", "38148")), access_log=False)
