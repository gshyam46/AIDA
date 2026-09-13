"""Model transport for AIDA: loopback llama.cpp or Groq over a pinned HTTPS endpoint.

The client never opens a database. It sends only the messages the interpreter
builds (question text and an approved catalog projection) and returns parsed
JSON with token and latency accounting. Proxies and redirects are disabled.
"""
from __future__ import annotations

import copy
import ipaddress
import json
import math
import os
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field as dataclass_field
from typing import Any

GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODELS_URL = "https://api.groq.com/openai/v1/models"
DEFAULT_GROQ_MODEL = "openai/gpt-oss-120b"
DEFAULT_GUARD_MODEL = "meta-llama/llama-prompt-guard-2-86m"
# USD per million input/output tokens, Groq list prices (September 2026).
GROQ_PRICES = {
    "openai/gpt-oss-120b": (0.15, 0.60), "openai/gpt-oss-20b": (0.075, 0.30),
    "qwen/qwen3.8-27b": (0.80, 4.00), "qwen/qwen3.6-27b": (0.60, 3.00),
    DEFAULT_GUARD_MODEL: (0.04, 0.04),
}
PIPELINES = ("two_stage", "single")
_LOCAL_DEFAULT_ENDPOINT = "http://127.0.0.1:8081/v1/chat/completions"
_LOCAL_DEFAULT_MODEL = "aida-semantic"
_MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}\Z")
_HOSTED_OPTIONS = {
    "openai/gpt-oss": {"reasoning_effort": "low", "include_reasoning": False, "min_completion_tokens": 1536},
    "qwen/": {"reasoning_effort": "none", "min_completion_tokens": 0},
}


class SemanticClarification(ValueError):
    """The question needs clarification, or cannot be answered, before execution."""

    def __init__(self, message: str, telemetry: dict | None = None, options: list[str] | None = None, reason: str | None = None):
        super().__init__(message)
        self.telemetry = telemetry or empty_telemetry()
        self.options = list(options or [])
        self.reason = reason


class ModelUnavailable(RuntimeError):
    """Inference failed; never silently replace it with a grammar or another provider."""

    def __init__(self, message: str, telemetry: dict | None = None):
        super().__init__(message)
        self.telemetry = telemetry or empty_telemetry()


class ModelOutputError(ValueError):
    """The model answered, but not with one usable JSON object."""

    def __init__(self, message: str, raw: str = "", truncated: bool = False):
        super().__init__(message)
        self.raw = raw
        self.truncated = truncated


def empty_telemetry() -> dict[str, Any]:
    return {"model_calls": 0, "guard_calls": 0, "repair_calls": 0,
            "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "input_tokens": 0, "output_tokens": 0,
            "model": None, "provider": None, "pipeline": None, "pipeline_stages": [], "rejections": [],
            "model_latency_ms": 0.0, "guard_latency_ms": 0.0, "rate_limit_wait_seconds": 0.0,
            "semantic_cache_hit": False, "interpretation_cache_hit": False, "interpretation_source": "validation",
            "estimated_model_cost_usd": 0.0, "model_cost_basis": None, "inference_location": None, "external_requests": False}


def estimate_cost(model: str | None, prompt_tokens: int | None, completion_tokens: int | None) -> float:
    price = GROQ_PRICES.get(model or "")
    if not price:
        return 0.0
    return round((prompt_tokens or 0) / 1e6 * price[0] + (completion_tokens or 0) / 1e6 * price[1], 8)


def _local_endpoint(endpoint: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(endpoint)
        host = parsed.hostname
        if host == "localhost":
            host = "127.0.0.1"
        if (parsed.scheme != "http" or not host or not ipaddress.ip_address(host).is_loopback
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or parsed.path.rstrip("/") != "/v1/chat/completions"):
            raise ValueError
        port = parsed.port or 80
        authority = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        return f"http://{authority}/v1/chat/completions"
    except (ValueError, TypeError) as exc:
        raise ValueError("AIDA_MODEL_ENDPOINT must be an HTTP loopback /v1/chat/completions endpoint. Off-machine local inference is disabled.") from exc


def _hosted_endpoint(endpoint: str) -> str:
    # A pinned destination prevents configuration from turning inference into SSRF.
    if endpoint != GROQ_ENDPOINT:
        raise ValueError("Hosted inference is pinned to the Groq HTTPS chat-completions endpoint.")
    return endpoint


@dataclass(frozen=True)
class SemanticConfig:
    endpoint: str = _LOCAL_DEFAULT_ENDPOINT
    model: str = _LOCAL_DEFAULT_MODEL
    timeout_seconds: float = 90.0
    max_tokens: int = 768
    max_prompt_chars: int = 24000
    cache_size: int = 128
    provider: str = "local"
    api_key: str | None = dataclass_field(default=None, repr=False, compare=False)
    concurrency: int = 1
    rate_limit_wait_seconds: float = 20.0
    pipeline: str = "two_stage"
    guard_model: str | None = None
    repair_attempts: int = 0

    def __post_init__(self):
        if self.provider == "local":
            object.__setattr__(self, "endpoint", _local_endpoint(self.endpoint))
        elif self.provider == "groq":
            if self.endpoint == _LOCAL_DEFAULT_ENDPOINT:
                object.__setattr__(self, "endpoint", GROQ_ENDPOINT)
            if self.model == _LOCAL_DEFAULT_MODEL:
                object.__setattr__(self, "model", DEFAULT_GROQ_MODEL)
            _hosted_endpoint(self.endpoint)
            if self.api_key is not None and (not isinstance(self.api_key, str) or len(self.api_key) > 400
                                             or any(ord(char) <= 32 for char in self.api_key)):
                raise ValueError("GROQ_API_KEY must be a single-line token.")
        else:
            raise ValueError("AIDA_MODEL_PROVIDER must be 'local' or 'groq'.")
        if not isinstance(self.model, str) or not _MODEL_ID.fullmatch(self.model):
            raise ValueError("Use a bounded model identifier.")
        if self.guard_model is not None and not _MODEL_ID.fullmatch(self.guard_model):
            raise ValueError("Use a bounded guard model identifier.")
        if self.pipeline not in PIPELINES:
            raise ValueError("AIDA_PIPELINE must be 'two_stage' or 'single'.")
        if not 0 <= self.repair_attempts <= 1:
            raise ValueError("AIDA_REPAIR_ATTEMPTS must be 0 or 1.")
        if not 1 <= self.concurrency <= 8:
            raise ValueError("Model concurrency must be between 1 and 8.")
        if not 0 <= self.rate_limit_wait_seconds <= 60:
            raise ValueError("Rate-limit wait must be between 0 and 60 seconds.")
        if not 1 <= self.timeout_seconds <= 300:
            raise ValueError("Model timeout must be between 1 and 300 seconds.")
        if not 256 <= self.max_tokens <= 4096:
            raise ValueError("Model output budget must be between 256 and 4096 tokens.")
        if not 2000 <= self.max_prompt_chars <= 60000:
            raise ValueError("Model prompt budget must be between 2000 and 60000 characters.")
        if not 0 <= self.cache_size <= 1024:
            raise ValueError("Semantic cache size must be between 0 and 1024.")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SemanticConfig":
        env = os.environ if environ is None else environ
        common = {"timeout_seconds": float(env.get("AIDA_MODEL_TIMEOUT_SECONDS", "90")),
                  "max_tokens": int(env.get("AIDA_MODEL_MAX_TOKENS", "768")),
                  "max_prompt_chars": int(env.get("AIDA_MODEL_MAX_PROMPT_CHARS", "24000")),
                  "cache_size": int(env.get("AIDA_SEMANTIC_CACHE_SIZE", "128")),
                  "pipeline": (env.get("AIDA_PIPELINE") or "two_stage").strip(),
                  "repair_attempts": int(env.get("AIDA_REPAIR_ATTEMPTS", "1"))}
        provider = (env.get("AIDA_MODEL_PROVIDER") or "local").strip().lower()
        if provider == "groq":
            return cls(provider="groq", endpoint=GROQ_ENDPOINT,
                       model=(env.get("AIDA_GROQ_MODEL") or DEFAULT_GROQ_MODEL).strip(),
                       api_key=(env.get("GROQ_API_KEY") or "").strip() or None,
                       concurrency=int(env.get("AIDA_MODEL_CONCURRENCY", "4")),
                       rate_limit_wait_seconds=float(env.get("AIDA_GROQ_MAX_WAIT_SECONDS", "20")),
                       guard_model=env.get("AIDA_GUARD_MODEL", DEFAULT_GUARD_MODEL).strip() or None, **common)
        return cls(provider=provider, endpoint=env.get("AIDA_MODEL_ENDPOINT", _LOCAL_DEFAULT_ENDPOINT),
                   model=env.get("AIDA_MODEL_NAME", _LOCAL_DEFAULT_MODEL), **common)


@dataclass
class ModelReply:
    content: dict[str, Any]
    raw: str
    prompt_tokens: int | None
    completion_tokens: int | None
    latency_ms: float
    model: str
    waited_seconds: float


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "Model redirects are disabled", headers, fp)


def _retry_after_seconds(headers: Any) -> float | None:
    for name in ("retry-after", "x-ratelimit-reset-tokens", "x-ratelimit-reset-requests"):
        value = headers.get(name) if headers is not None else None
        match = re.fullmatch(r"(?:(\d+)m)?(\d+(?:\.\d+)?)s?", value.strip()) if isinstance(value, str) else None
        if match:
            return int(match[1] or 0) * 60 + float(match[2])
    return None


class ModelClient:
    def __init__(self, config: SemanticConfig):
        self.config = config
        self.gate = threading.BoundedSemaphore(config.concurrency if config.provider == "groq" else 1)
        handlers: list[Any] = [urllib.request.ProxyHandler({}), _NoRedirects()]
        if config.provider == "groq":
            handlers.append(urllib.request.HTTPSHandler(context=ssl.create_default_context()))
        self._opener = urllib.request.build_opener(*handlers)
        self._status_cache: tuple[float, dict[str, Any]] | None = None

    @property
    def hosted(self) -> bool:
        return self.config.provider == "groq"

    def status(self) -> dict[str, Any]:
        """A readiness probe performs no inference and sends no question or catalog data."""
        return self._hosted_status() if self.hosted else self._local_status()

    def _local_status(self) -> dict[str, Any]:
        endpoint = self.config.endpoint.rsplit("/v1/", 1)[0] + "/health"
        result = {"available": False, "model": self.config.model, "provider": "llama.cpp", "status": "unavailable", "error": None,
                  "inference_location": "local", "external_requests": False, "pipeline": self.config.pipeline}
        try:
            with self._opener.open(urllib.request.Request(endpoint, method="GET"), timeout=2) as response:
                body = json.loads(response.read(4097))
            if isinstance(body, dict) and body.get("status") == "ok":
                result.update(available=True, status="ready")
            else:
                result["error"] = "Local model server is not ready."
        except urllib.error.HTTPError as exc:
            result["status"] = "loading" if exc.code == 503 else "unavailable"
            result["error"] = "Local model is loading." if exc.code == 503 else f"Local model health returned HTTP {exc.code}."
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            result["error"] = "Start the local model server to enable natural-language questions."
        return result

    def _hosted_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.config.api_key}", "Accept": "application/json",
                "Content-Type": "application/json", "User-Agent": "AIDA/4.0"}

    def _hosted_status(self) -> dict[str, Any]:
        now = time.monotonic()
        if self._status_cache and self._status_cache[0] > now:
            return copy.deepcopy(self._status_cache[1])
        result = {"available": False, "model": self.config.model, "provider": "groq", "status": "unavailable", "error": None,
                  "inference_location": "hosted", "external_requests": True, "pipeline": self.config.pipeline,
                  "guard_model": self.config.guard_model,
                  "data_sent": "Question text and approved catalog labels only; never rows, SQL, table names or file paths."}
        ttl = 10.0
        if not self.config.api_key:
            result["error"] = "Set GROQ_API_KEY in backend/.env to enable hosted interpretation."
        else:
            try:
                request = urllib.request.Request(GROQ_MODELS_URL, method="GET", headers=self._hosted_headers())
                with self._opener.open(request, timeout=5) as response:
                    body = json.loads(response.read(1_048_576))
                models = {item.get("id"): item for item in body.get("data", []) if isinstance(item, dict)}
                if models.get(self.config.model, {}).get("active") is True:
                    result.update(available=True, status="ready")
                    ttl = 60.0
                else:
                    result["error"] = "The configured Groq model is not available for this API key."
            except urllib.error.HTTPError as exc:
                result["error"] = ("Groq rejected the configured API key." if exc.code in (401, 403) else
                                   "Groq is rate limiting status checks; retry shortly." if exc.code == 429 else
                                   f"Groq model status returned HTTP {exc.code}.")
            except (urllib.error.URLError, TimeoutError, socket.timeout, OSError, ValueError, AttributeError):
                result["error"] = "Groq could not be reached from this server."
        self._status_cache = (now + ttl, result)
        return copy.deepcopy(result)

    def complete(self, messages: list[dict[str, str]], *, max_tokens: int, schema: dict | None = None) -> ModelReply:
        if not 64 <= max_tokens <= 4096:
            raise ValueError("Model output budget must be between 64 and 4096 tokens.")
        started = time.perf_counter()
        if self.hosted:
            options = next((value for prefix, value in _HOSTED_OPTIONS.items() if self.config.model.startswith(prefix)), {})
            body = {"model": self.config.model, "messages": messages, "temperature": 0, "seed": 42, "stream": False,
                    "max_completion_tokens": max(max_tokens, options.get("min_completion_tokens", 0)),
                    "response_format": {"type": "json_object"}}
            body.update({key: value for key, value in options.items() if key != "min_completion_tokens"})
            response, waited = self._hosted_post(body, self.config.rate_limit_wait_seconds)
        else:
            response_format: dict[str, Any] = {"type": "json_object"}
            if schema:
                response_format["schema"] = schema
            body = {"model": self.config.model, "messages": messages, "temperature": 0, "seed": 42, "max_tokens": max_tokens,
                    "stream": False, "cache_prompt": True, "response_format": response_format}
            response, waited = self._local_post(body), 0.0
        return self._reply(response, round((time.perf_counter() - started) * 1000, 2), waited)

    def guard(self, text: str) -> tuple[float | None, float]:
        """Return the prompt-attack probability for the text, or None when no guard ran."""
        if not self.hosted or not self.config.guard_model or not self.config.api_key:
            return None, 0.0
        started = time.perf_counter()
        body = {"model": self.config.guard_model, "messages": [{"role": "user", "content": text[:2000]}],
                "temperature": 0, "max_completion_tokens": 16}
        try:
            response, _ = self._hosted_post(body, 5.0)
            score = float(response["choices"][0]["message"]["content"].strip())
        except (ModelUnavailable, ModelOutputError, KeyError, IndexError, TypeError, ValueError, AttributeError):
            return None, round((time.perf_counter() - started) * 1000, 2)
        return (score if 0.0 <= score <= 1.0 else None), round((time.perf_counter() - started) * 1000, 2)

    def _reply(self, response: Any, latency_ms: float, waited: float) -> ModelReply:
        try:
            choice = response["choices"][0]
            content = choice["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ModelOutputError("The model response had no message content.") from exc
        if not isinstance(content, str) or len(content) > 24000:
            raise ModelOutputError("The model returned unusable content.")
        if choice.get("finish_reason") == "length":
            raise ModelOutputError("The model reached its response budget.", content, truncated=True)
        text = content.strip()
        fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, re.S)
        if fenced:
            text = fenced[1]
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ModelOutputError("The model reply was not valid JSON.", content) from exc
        if not isinstance(parsed, dict):
            raise ModelOutputError("The model reply was not a JSON object.", content)
        usage = response.get("usage") if isinstance(response.get("usage"), dict) else {}
        prompt, completion = (usage.get(key) if type(usage.get(key)) is int and usage.get(key) >= 0 else None
                              for key in ("prompt_tokens", "completion_tokens"))
        model = response.get("model")
        model = model if isinstance(model, str) and 0 < len(model) <= 200 else self.config.model
        return ModelReply(parsed, content, prompt, completion, latency_ms, model, waited)

    def _local_post(self, body: dict) -> dict:
        request = urllib.request.Request(self.config.endpoint, data=json.dumps(body).encode(), method="POST",
                                         headers={"Content-Type": "application/json", "Accept": "application/json"})
        try:
            with self._opener.open(request, timeout=self.config.timeout_seconds) as response:
                raw = response.read(65537)
            if len(raw) > 65536:
                raise ModelUnavailable("The local model returned an oversized response. No query was executed.")
            return json.loads(raw)
        except urllib.error.HTTPError as exc:
            raise ModelUnavailable(f"The local model returned HTTP {exc.code}. Check the local model server; the query builder remains available.") from exc
        except (urllib.error.URLError, TimeoutError, socket.timeout, ConnectionError, OSError) as exc:
            raise ModelUnavailable("The local language model is unavailable or timed out. Start the local model server; the query builder remains available.") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ModelUnavailable("The local model returned an invalid response. Check the local model server; no query was executed.") from exc

    def _hosted_post(self, body: dict, max_wait: float) -> tuple[dict, float]:
        if not self.config.api_key:
            raise ModelUnavailable("Hosted interpretation is not configured. Set GROQ_API_KEY; the query builder remains available.")
        data = json.dumps(body).encode()
        attempts, waited = 0, 0.0
        while True:
            attempts += 1
            request = urllib.request.Request(self.config.endpoint, data=data, method="POST", headers=self._hosted_headers())
            try:
                with self._opener.open(request, timeout=self.config.timeout_seconds) as response:
                    raw = response.read(262_145)
                if len(raw) > 262_144:
                    raise ModelUnavailable("The hosted model returned an oversized response. No query was executed.")
                parsed = json.loads(raw)
                if not isinstance(parsed, dict):
                    raise ModelUnavailable("The hosted model returned an invalid response. No query was executed.")
                return parsed, waited
            except urllib.error.HTTPError as exc:
                wait = _retry_after_seconds(exc.headers)
                if exc.code == 429 and attempts <= 3 and wait is not None and waited + wait <= max_wait:
                    time.sleep(wait)
                    waited += wait
                    continue
                try:
                    error = json.loads(exc.read(8192) or b"{}").get("error", {})
                except (ValueError, AttributeError, OSError):
                    error = {}
                code = error.get("code") if isinstance(error, dict) else None
                if exc.code == 400 and code in {"json_validate_failed", "output_parse_failed"}:
                    raise ModelOutputError("The hosted model could not produce valid JSON.") from exc
                if exc.code in (401, 403):
                    raise ModelUnavailable("The hosted model rejected the configured credentials. Check GROQ_API_KEY; the query builder remains available.") from exc
                if exc.code in (413, 429):
                    retry = math.ceil(wait) if wait is not None else 60
                    raise ModelUnavailable(f"The hosted model rate limit was reached. Try again in about {retry} seconds; the query builder remains available.") from exc
                raise ModelUnavailable(f"The hosted model returned HTTP {exc.code}. The query builder remains available.") from exc
            except (urllib.error.URLError, TimeoutError, socket.timeout, ConnectionError, OSError) as exc:
                raise ModelUnavailable("The hosted model is unreachable or timed out. The query builder remains available.") from exc
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ModelUnavailable("The hosted model returned an invalid response. No query was executed.") from exc
