"""Materialize independent expected rows for browser assertions; no model calls."""
import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.core.chinook import chinook_cases, chinook_source
from backend.core.relational_demo import ensure_relational_demos, relational_demo_cases


def main():
    with tempfile.TemporaryDirectory(prefix="aida-browser-oracles-") as directory:
        sources = {source["id"]: source for source in [*ensure_relational_demos(Path(directory)), chinook_source()]}
        cases = []
        for case in [*relational_demo_cases(), *chinook_cases()]:
            if case["plan"] is None:
                continue
            with closing(sqlite3.connect(sources[case["source_id"]]["path"])) as connection:
                connection.row_factory = sqlite3.Row
                data = [dict(row) for row in connection.execute(case["oracle_sql"], case.get("oracle_parameters", {}))]
            cases.append({**case, "expected_data": data})
        target = ROOT / "artifacts" / "relational-e2e-oracles.json"
        target.parent.mkdir(exist_ok=True)
        target.write_text(json.dumps({"scope": "Authored SQL over independent fixture instances; never sent to the model or API.", "cases": cases,
                                      "upload_manifest": chinook_source()["manifest"]}, indent=2), encoding="utf-8")
        print(f"Prepared {len(cases)} independent browser cases: {target}")


if __name__ == "__main__":
    main()
