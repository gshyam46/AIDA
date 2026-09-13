"""Shared request guards: caller identity, client fingerprints and rate-limit responses."""
import ipaddress
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


def principal(request: Request) -> dict[str, Any] | None:
    return getattr(request.state, "user", None)


def client_fingerprint(request: Request) -> str:
    peer = request.client.host if request.client else ""
    forwarded = request.headers.get("x-aida-client", "").strip()
    try:
        # Only the loopback Next.js proxy may report the browser's address.
        if forwarded and ipaddress.ip_address(peer).is_loopback:
            peer = str(ipaddress.ip_address(forwarded))
    except ValueError:
        pass
    return request.app.state.auth.client_fingerprint(peer)


def identity(request: Request) -> str:
    user = principal(request)
    return f"user:{user['id']}" if user else f"client:{client_fingerprint(request)}"


def rate_limited(request: Request, bucket: str, key: str) -> JSONResponse | None:
    wait = request.app.state.limiter.hit(bucket, key)
    if wait is None:
        return None
    user = principal(request)
    request.app.state.auth.record_event("rate_limited", user["id"] if user else None, client_fingerprint(request), bucket)
    return JSONResponse(status_code=429, headers={"Retry-After": str(wait)},
                        content={"success": False, "error": "Too many requests. Please wait a moment and try again.", "error_type": "rate_limited", "retry_after": wait})
