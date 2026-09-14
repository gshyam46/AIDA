"""Accounts, sessions and onboarding records for the AIDA workspace.

Passwords use salted scrypt. Session tokens are random and persisted only as
SHA-256 digests, so a copied database cannot be replayed as live sessions.
Security events store categories and salted client fingerprints, never
question text, credentials or query results.
"""
from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

SESSION_COOKIE = "aida_session"
SESSION_TTL_SECONDS = 12 * 3600
SESSION_IDLE_SECONDS = 2 * 3600
MAX_SESSIONS_PER_USER = 10
LOCK_THRESHOLD = 5
LOCK_SECONDS = 15 * 60
GENERIC_LOGIN_ERROR = "Email or password is incorrect."
TEAM_SIZES = ("1", "2-10", "11-50", "51-200", "201-1000", "1000+")
USE_CASES = ("sales", "finance", "operations", "support", "marketing", "product", "logistics", "other")
DATA_CHOICES = ("demo", "sample_logistics", "upload_later")
EVENT_KINDS = {"signup", "login_success", "login_failure", "account_locked", "logout", "rate_limited",
               "question_refused", "misuse_block", "csrf_rejected", "auth_rejected", "onboarding_completed",
               "sample_installed", "source_uploaded", "connection_created", "connection_changed"}
_SCRYPT_N, _SCRYPT_R, _SCRYPT_P = 2 ** 14, 8, 1
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9-]{1,63}\.){1,8}[A-Za-z]{2,24}\Z")
_TOKEN = re.compile(r"[A-Za-z0-9_-]{20,128}\Z")
_COMMON_PASSWORDS = {"password", "password1", "password12", "password123", "passw0rd123", "1234567890", "12345678910",
                     "qwerty1234", "qwertyuiop1", "letmein123", "welcome123", "admin12345", "iloveyou123", "changeme123",
                     "abc1234567", "football123", "monkey12345", "dragon12345", "sunshine123", "princess123"}
_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE COLLATE NOCASE, name TEXT NOT NULL,
    password_hash TEXT NOT NULL, role TEXT NOT NULL CHECK (role IN ('owner', 'member')),
    created_at INTEGER NOT NULL, failed_logins INTEGER NOT NULL DEFAULT 0, locked_until INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS sessions (
    token_hash TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS sessions_by_user ON sessions(user_id);
CREATE TABLE IF NOT EXISTS onboarding (
    user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE, profile TEXT NOT NULL, completed_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS security_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, kind TEXT NOT NULL,
    user_id TEXT, client TEXT, detail TEXT);
"""


class AuthError(ValueError):
    def __init__(self, message: str, status: int = 400, retry_after: int | None = None):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, dklen=32)
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${_b64(salt)}${_b64(digest)}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, n, r, p, salt, expected = encoded.split("$")
        n, r, p = int(n), int(r), int(p)
        if scheme != "scrypt" or not (2 ** 10 <= n <= 2 ** 20 and 1 <= r <= 32 and 1 <= p <= 16):
            return False
        expected_bytes = _unb64(expected)
        digest = hashlib.scrypt(password.encode(), salt=_unb64(salt), n=n, r=r, p=p,
                                dklen=len(expected_bytes), maxmem=64 * 1024 * 1024)
        return hmac.compare_digest(digest, expected_bytes)
    except (ValueError, TypeError, UnicodeEncodeError):
        return False


@functools.lru_cache(maxsize=1)
def _dummy_hash() -> str:
    return hash_password(secrets.token_urlsafe(18))


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def normalize_email(email: Any) -> str:
    if not isinstance(email, str) or len(email.strip()) > 254 or not _EMAIL.fullmatch(email.strip()):
        raise AuthError("Enter a valid email address.")
    return email.strip().lower()


def validate_name(name: Any) -> str:
    if not isinstance(name, str) or not 1 <= len(name.strip()) <= 80 or any(ord(char) < 32 for char in name):
        raise AuthError("Enter your name (1 to 80 characters).")
    return name.strip()


def validate_password(password: Any, email: str) -> str:
    if not isinstance(password, str) or not 10 <= len(password) <= 128:
        raise AuthError("Use a password of 10 to 128 characters.")
    if not re.search(r"[A-Za-z]", password) or not re.search(r"\d", password):
        raise AuthError("Use letters and at least one number in your password.")
    if password.casefold() in _COMMON_PASSWORDS or len(set(password)) < 5:
        raise AuthError("That password is too common or repetitive. Choose another.")
    local = email.split("@")[0]
    if len(local) >= 4 and local in password.casefold():
        raise AuthError("Your password must not contain your email name.")
    return password


def validate_onboarding(profile: Any) -> dict[str, Any]:
    allowed = {"company", "role_title", "team_size", "use_cases", "primary_goal", "data_choice", "hosted_inference_consent"}
    if not isinstance(profile, dict) or set(profile) - allowed:
        raise AuthError("Unsupported onboarding fields.")

    def text(key: str, maximum: int, required: bool = True) -> str | None:
        value = profile.get(key)
        if value is None or (isinstance(value, str) and not value.strip() and not required):
            if required:
                raise AuthError(f"Provide {key.replace('_', ' ')}.")
            return None
        if not isinstance(value, str) or not 1 <= len(value.strip()) <= maximum or any(ord(c) < 32 and c not in "\n\t" for c in value):
            raise AuthError(f"Provide a valid {key.replace('_', ' ')} (up to {maximum} characters).")
        return value.strip()

    use_cases = profile.get("use_cases")
    if (not isinstance(use_cases, list) or not 1 <= len(use_cases) <= len(USE_CASES)
            or any(item not in USE_CASES for item in use_cases) or len(set(use_cases)) != len(use_cases)):
        raise AuthError("Choose at least one supported use case.")
    if profile.get("team_size") not in TEAM_SIZES:
        raise AuthError("Choose a team size.")
    if profile.get("data_choice") not in DATA_CHOICES:
        raise AuthError("Choose how you want to start with data.")
    if not isinstance(profile.get("hosted_inference_consent"), bool):
        raise AuthError("Confirm how questions are interpreted.")
    return {"company": text("company", 120), "role_title": text("role_title", 80), "team_size": profile["team_size"],
            "use_cases": list(use_cases), "primary_goal": text("primary_goal", 300, required=False),
            "data_choice": profile["data_choice"], "hosted_inference_consent": profile["hosted_inference_consent"]}


class AuthStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        with self._db() as db:
            db.executescript(_SCHEMA)
        with self._db() as db:
            row = db.execute("SELECT value FROM meta WHERE key = 'client_salt'").fetchone()
            if row is None:
                db.execute("INSERT INTO meta(key, value) VALUES ('client_salt', ?)", (secrets.token_hex(16),))
                row = db.execute("SELECT value FROM meta WHERE key = 'client_salt'").fetchone()
        self._client_salt = row["value"]

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            with connection:
                yield connection
        finally:
            connection.close()

    @staticmethod
    def _public(row: sqlite3.Row) -> dict[str, Any]:
        return {"id": row["id"], "email": row["email"], "name": row["name"], "role": row["role"], "created_at": row["created_at"]}

    def client_fingerprint(self, address: str | None) -> str:
        return hashlib.sha256(f"{self._client_salt}|{address or 'unknown'}".encode()).hexdigest()[:16]

    def signup(self, email: Any, name: Any, password: Any, now: float | None = None) -> dict[str, Any]:
        timestamp = int(time.time() if now is None else now)
        email, name = normalize_email(email), validate_name(name)
        encoded = hash_password(validate_password(password, email))
        user_id = uuid.uuid4().hex
        try:
            with self._lock, self._db() as db:
                role = "owner" if db.execute("SELECT 1 FROM users LIMIT 1").fetchone() is None else "member"
                db.execute("INSERT INTO users(id, email, name, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                           (user_id, email, name, encoded, role, timestamp))
        except sqlite3.IntegrityError as exc:
            raise AuthError("We couldn't create an account with those details. If you already have one, sign in instead.", 409) from exc
        return {"id": user_id, "email": email, "name": name, "role": role, "created_at": timestamp}

    def authenticate(self, email: Any, password: Any, now: float | None = None) -> dict[str, Any]:
        timestamp = int(time.time() if now is None else now)
        try:
            email = normalize_email(email)
        except AuthError:
            email = None
        candidate = password if isinstance(password, str) and len(password) <= 128 else ""
        with self._db() as db:
            row = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone() if email else None
        # Unknown and locked accounts still pay the scrypt cost to keep response timing uniform.
        if row is None:
            verify_password(candidate, _dummy_hash())
            raise AuthError(GENERIC_LOGIN_ERROR, 401)
        if row["locked_until"] > timestamp:
            verify_password(candidate, _dummy_hash())
            raise AuthError("Too many sign-in attempts. Try again later.", 429, row["locked_until"] - timestamp)
        if verify_password(candidate, row["password_hash"]):
            with self._lock, self._db() as db:
                db.execute("UPDATE users SET failed_logins = 0, locked_until = 0 WHERE id = ?", (row["id"],))
            return self._public(row)
        with self._lock, self._db() as db:
            current = db.execute("SELECT failed_logins FROM users WHERE id = ?", (row["id"],)).fetchone()
            failures = (current["failed_logins"] if current else 0) + 1
            locked_until = timestamp + LOCK_SECONDS if failures >= LOCK_THRESHOLD else 0
            db.execute("UPDATE users SET failed_logins = ?, locked_until = ? WHERE id = ?",
                       (0 if locked_until else failures, locked_until, row["id"]))
            if locked_until:
                self._event(db, "account_locked", row["id"], None, "failed_login_threshold", timestamp)
        raise AuthError(GENERIC_LOGIN_ERROR, 401)

    def create_session(self, user_id: str, now: float | None = None) -> str:
        timestamp = int(time.time() if now is None else now)
        token = secrets.token_urlsafe(32)
        with self._lock, self._db() as db:
            db.execute("DELETE FROM sessions WHERE expires_at <= ? OR last_seen_at <= ?", (timestamp, timestamp - SESSION_IDLE_SECONDS))
            db.execute("INSERT INTO sessions(token_hash, user_id, created_at, expires_at, last_seen_at) VALUES (?, ?, ?, ?, ?)",
                       (_token_hash(token), user_id, timestamp, timestamp + SESSION_TTL_SECONDS, timestamp))
            db.execute("DELETE FROM sessions WHERE user_id = ? AND token_hash NOT IN (SELECT token_hash FROM sessions WHERE user_id = ? "
                       "ORDER BY created_at DESC, rowid DESC LIMIT ?)", (user_id, user_id, MAX_SESSIONS_PER_USER))
        return token

    def session_user(self, token: Any, now: float | None = None) -> dict[str, Any] | None:
        if not isinstance(token, str) or not _TOKEN.fullmatch(token):
            return None
        timestamp = int(time.time() if now is None else now)
        digest = _token_hash(token)
        with self._db() as db:
            row = db.execute("SELECT u.*, s.expires_at, s.last_seen_at FROM sessions s JOIN users u ON u.id = s.user_id "
                             "WHERE s.token_hash = ?", (digest,)).fetchone()
            if row is None:
                return None
            if row["expires_at"] <= timestamp or row["last_seen_at"] <= timestamp - SESSION_IDLE_SECONDS:
                db.execute("DELETE FROM sessions WHERE token_hash = ?", (digest,))
                return None
            if timestamp - row["last_seen_at"] >= 60:
                db.execute("UPDATE sessions SET last_seen_at = ? WHERE token_hash = ?", (timestamp, digest))
            return self._public(row)

    def revoke(self, token: Any) -> None:
        if isinstance(token, str) and _TOKEN.fullmatch(token):
            with self._lock, self._db() as db:
                db.execute("DELETE FROM sessions WHERE token_hash = ?", (_token_hash(token),))

    def save_onboarding(self, user_id: str, profile: Any, now: float | None = None) -> dict[str, Any]:
        timestamp = int(time.time() if now is None else now)
        clean = validate_onboarding(profile)
        with self._lock, self._db() as db:
            db.execute("INSERT INTO onboarding(user_id, profile, completed_at) VALUES (?, ?, ?) "
                       "ON CONFLICT(user_id) DO UPDATE SET profile = excluded.profile, completed_at = excluded.completed_at",
                       (user_id, json.dumps(clean), timestamp))
        return {**clean, "completed_at": timestamp}

    def onboarding(self, user_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute("SELECT profile, completed_at FROM onboarding WHERE user_id = ?", (user_id,)).fetchone()
        return None if row is None else {**json.loads(row["profile"]), "completed_at": row["completed_at"]}

    @staticmethod
    def _event(db: sqlite3.Connection, kind: str, user_id: str | None, client: str | None, detail: str | None, timestamp: int) -> None:
        if kind not in EVENT_KINDS:
            raise ValueError("Unknown security event kind.")
        db.execute("INSERT INTO security_events(at, kind, user_id, client, detail) VALUES (?, ?, ?, ?, ?)",
                   (timestamp, kind, user_id, client, None if detail is None else str(detail)[:200]))
        db.execute("DELETE FROM security_events WHERE id <= (SELECT MAX(id) FROM security_events) - 20000")

    def record_event(self, kind: str, user_id: str | None = None, client: str | None = None,
                     detail: str | None = None, now: float | None = None) -> None:
        with self._lock, self._db() as db:
            self._event(db, kind, user_id, client, detail, int(time.time() if now is None else now))

    def recent_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._db() as db:
            rows = db.execute("SELECT at, kind, user_id, client, detail FROM security_events ORDER BY id DESC LIMIT ?",
                              (max(1, min(int(limit), 500)),)).fetchall()
        return [dict(row) for row in rows]

    def raw_session_rows(self) -> list[dict[str, Any]]:
        with self._db() as db:
            return [dict(row) for row in db.execute("SELECT token_hash, user_id FROM sessions").fetchall()]
