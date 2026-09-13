"""Source-scoped hybrid analytics and explicit local SQLite onboarding."""
from datetime import datetime, timezone
from urllib.parse import unquote

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

from .guards import client_fingerprint, identity, principal, rate_limited
from .models import QueryRequest

router = APIRouter()
MISUSE_REASONS = {"prompt_injection", "sensitive_data"}


@router.get("/health")
def health(request: Request):
    state = request.app.state
    catalog = state.engine.catalog()
    return {"status": "healthy", "version": "4.0.0", "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": "public-demo" if state.public_demo else "local", "dataset": catalog["dataset"],
            "model": state.engine.interpreter.status(), "auth_required": state.auth_enabled}


@router.get("/sources")
def sources(request: Request):
    state = request.app.state
    return {"sources": state.registry.list_sources(principal(request)), "default_source_id": "commerce",
            "uploads_enabled": not state.public_demo, "mode": "public-demo" if state.public_demo else "local",
            "model": state.engine.interpreter.status()}


@router.get("/sources/{source_id}")
def inspect_source(source_id: str, request: Request):
    return request.app.state.registry.inspect_source(source_id, principal(request))


@router.post("/sources")
async def upload_source(request: Request):
    state = request.app.state
    if state.public_demo:
        return JSONResponse(status_code=403, content={"error": "Database uploads are disabled in the public demo."})
    if request.headers.get("content-type", "").split(";")[0] not in {"application/octet-stream", "application/vnd.sqlite3", "application/x-sqlite3"}:
        return JSONResponse(status_code=415, content={"error": "Upload the SQLite file as application/octet-stream."})
    limited = rate_limited(request, "upload_user", identity(request))
    if limited is not None:
        return limited
    # Middleware has already capped the streamed file size.
    result = state.registry.inspect_upload(await request.body(), unquote(request.headers.get("x-source-name", "database.sqlite")), principal(request))
    user = principal(request)
    if user:
        state.auth.record_event("source_uploaded", user["id"], client_fingerprint(request))
    return result


@router.post("/samples/logistics")
def install_sample(request: Request):
    state = request.app.state
    if state.public_demo:
        return JSONResponse(status_code=403, content={"error": "Sample installation is disabled in the public demo."})
    limited = rate_limited(request, "upload_user", identity(request))
    if limited is not None:
        return limited
    return state.registry.install_logistics_sample(principal(request))


@router.post("/sources/{source_id}/configure")
def configure_source(source_id: str, request: Request, config: dict = Body(...)):
    state = request.app.state
    if state.public_demo:
        return JSONResponse(status_code=403, content={"error": "Database mapping is disabled in the public demo."})
    limited = rate_limited(request, "configure_user", identity(request))
    if limited is not None:
        return limited
    return state.registry.configure(source_id, config, principal(request))


@router.get("/catalog")
def catalog(request: Request, source_id: str = "commerce"):
    return request.app.state.engine.catalog(source_id, principal(request))


@router.get("/schema")
def schema(request: Request, source_id: str = "commerce"):
    return request.app.state.engine.catalog(source_id, principal(request))


@router.get("/examples")
def examples(request: Request, source_id: str = "commerce"):
    return {"examples": request.app.state.engine.catalog(source_id, principal(request))["examples"]}


@router.post("/query")
def query(body: QueryRequest, request: Request):
    state, caller = request.app.state, identity(request)
    if body.question is not None:
        paused = state.misuse.blocked_for(caller)
        if paused:
            return JSONResponse(status_code=429, headers={"Retry-After": str(paused)}, content={
                "success": False, "data": [], "error_type": "misuse_paused", "retry_after": paused,
                "error": "Questions are paused for this account after repeated attempts to reach restricted data or override instructions. The visual builder remains available."})
        for bucket, key in (("question_user", caller), ("model_global", "all")):
            limited = rate_limited(request, bucket, key)
            if limited is not None:
                return limited
    else:
        limited = rate_limited(request, "plan_user", caller)
        if limited is not None:
            return limited
    result = state.engine.query(question=body.question, plan=body.plan, source_id=body.source_id,
                                catalog_version=body.catalog_version, principal=principal(request))
    reason = result.get("clarification_reason")
    if body.question is not None and reason in MISUSE_REASONS:
        user, client = principal(request), client_fingerprint(request)
        state.auth.record_event("question_refused", user["id"] if user else None, client, reason)
        if state.misuse.strike(caller):
            state.auth.record_event("misuse_block", user["id"] if user else None, client, reason)
    return result
