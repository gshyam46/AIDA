"""Runtime settings for the packaged service entry point.

Library and test code read explicit arguments or ``os.environ`` only. The
service merges ``backend/.env`` beneath real environment variables so secrets
such as GROQ_API_KEY stay out of source control.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
_KEY = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")


def read_env_file(path: Path = ENV_PATH) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError:
        return values
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip().removeprefix("export ").strip(), value.strip()
        if not _KEY.fullmatch(key):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def runtime_environment(path: Path = ENV_PATH) -> dict[str, str]:
    """Real environment variables override values from the local .env file."""
    return {**read_env_file(path), **os.environ}


def apply_env_file(path: Path = ENV_PATH) -> None:
    """Standalone scripts: fill unset variables from the local .env file."""
    for key, value in read_env_file(path).items():
        os.environ.setdefault(key, value)
