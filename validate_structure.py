"""Check hybrid source layout and pinned artifact metadata, without inference.

This is a syntax/provenance-shape check. Actual language accuracy, runtime
installation and browser behavior require their separate verification commands.
"""
import ast
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent
REQUIRED = [
    "backend/main.py", "backend/core/analytics.py", "backend/core/interpreter.py", "backend/core/llm.py",
    "backend/core/timeframes.py", "backend/core/calculations.py",
    "backend/core/hybrid.py", "backend/core/sources.py", "backend/core/catalog_engine.py",
    "backend/core/relational.py", "backend/core/settings.py", "backend/core/auth.py", "backend/core/security.py",
    "backend/core/relational_demo.py", "backend/core/chinook.py", "fixtures/chinook/Chinook_Sqlite.sqlite",
    "backend/api/endpoints.py", "backend/api/models.py", "backend/test_sources.py",
    "backend/test_hybrid_api.py", "frontend/app/page.tsx", "frontend/lib/api.ts",
    "scripts/model-runtime.json", "scripts/setup-model.ps1", "scripts/start-model.ps1",
    "scripts/start-demo.ps1", "scripts/stop-demo.ps1", "scripts/benchmark_nl.py", "scripts/benchmark_cases.py",
    "README.md", "docs/BASELINE_REVIEW.md", "docs/IMPLEMENTATION_PLAN.md",
    "docs/DEMO_GUIDE.md", "docs/VERIFICATION.md",
]


def check_model_manifest():
    manifest = json.loads((ROOT / "scripts/model-runtime.json").read_text(encoding="utf-8-sig"))
    for key in ("runtime", "model"):
        artifact = manifest[key]
        if not re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"]):
            raise SystemExit(f"Invalid pinned {key} SHA-256")
        if urlsplit(artifact["url"]).scheme != "https":
            raise SystemExit(f"Pinned {key} artifact must use HTTPS")
        if not artifact.get("license"):
            raise SystemExit(f"Missing {key} license provenance")
    model = manifest["model"]
    if not all(model.get(key) for key in ("name", "publisher", "quantization", "quantization_repository", "revision", "file", "upstream")):
        raise SystemExit("Incomplete model/quantization provenance")
    if type(model["bytes"]) is not int or model["bytes"] <= 0:
        raise SystemExit("Invalid model artifact byte size")

def main():
    for name in REQUIRED:
        if not (ROOT / name).is_file():
            raise SystemExit(f"Missing: {name}")
    check_model_manifest()
    skipped = {"venv", ".venv", "node_modules", "__pycache__", "archive"}
    files = [file for file in sorted((ROOT / "backend").rglob("*.py")) if not skipped.intersection(file.relative_to(ROOT).parts)]
    files += sorted((ROOT / "scripts").glob("*.py"))
    for file in files:
        ast.parse(file.read_text(encoding="utf-8-sig"), filename=str(file))
    print(f"Hybrid source layout, pinned artifact metadata and {len(files)} Python files validated. Run pytest, the actual-model evaluation and browser E2E for behavior.")

if __name__ == "__main__":
    main()
