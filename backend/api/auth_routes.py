"""Account sign-up, sign-in, sessions, client onboarding and owner security review."""
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .guards import client_fingerprint, principal, rate_limited

router = APIRouter()


class Credentials(BaseModel):
    model_config = ConfigDict(extra="forbid")
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=1, max_length=128)


class SignupRequest(Credentials):
    name: str = Field(min_length=1, max_length=80)


class OnboardingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    company: str = Field(min_length=1, max_length=120)
    role_title: str = Field(min_length=1, max_length=80)
    team_size: str = Field(max_length=12)
    use_cases: list[str] = Field(min_length=1, max_length=8)
    primary_goal: str | None = Field(default=None, max_length=300)
    data_choice: str = Field(max_length=32)
    hosted_inference_consent: bool = False


def _hosted(request: Request) -> bool:
    return getattr(getattr(request.app.state.engine.interpreter, "config", None), "provider", "local") == "groq"


def _session_body(request: Request, user: dict[str, Any] | None) -> dict[str, Any]:
    state = request.app.state
    return {"auth_required": state.auth_enabled, "signup_enabled": state.allow_signup, "user": user,
            "onboarding": state.auth.onboarding(user["id"]) if user else None, "hosted_inference": _hosted(request)}


def _issue_session(request: Request, user: dict[str, Any], status: int = 200) -> JSONResponse:
    state = request.app.state
    # A fresh token on every sign-in prevents session fixation; any presented token is revoked.
    state.auth.revoke(request.cookies.get(state.session_cookie))
    token = state.auth.create_session(user["id"])
    response = JSONResponse(status_code=status, content=_session_body(request, user))
    response.set_cookie(state.session_cookie, token, max_age=state.session_ttl, httponly=True, samesite="strict", secure=state.cookie_secure, path="/")
    return response


def _disabled() -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "Accounts are not enabled in this mode."})


@router.get("/auth/session")
def session(request: Request):
    state = request.app.state
    user = state.auth.session_user(request.cookies.get(state.session_cookie)) if state.auth_enabled else None
    return _session_body(request, user)


@router.post("/auth/signup")
def signup(body: SignupRequest, request: Request):
    state = request.app.state
    if not state.auth_enabled:
        return _disabled()
    if not state.allow_signup:
        return JSONResponse(status_code=403, content={"error": "New accounts are disabled. Ask the workspace owner for access."})
    client = client_fingerprint(request)
    limited = rate_limited(request, "signup_client", client)
    if limited is not None:
        return limited
    try:
        user = state.auth.signup(body.email, body.name, body.password)
    except ValueError as exc:
        return JSONResponse(status_code=getattr(exc, "status", 400), content={"error": str(exc)})
    state.auth.record_event("signup", user["id"], client)
    return _issue_session(request, user, 201)


@router.post("/auth/login")
def login(body: Credentials, request: Request):
    state = request.app.state
    if not state.auth_enabled:
        return _disabled()
    client = client_fingerprint(request)
    email = body.email.strip().lower()
    for bucket, key in (("login_client", client), ("login_email", email)):
        limited = rate_limited(request, bucket, key)
        if limited is not None:
            return limited
    try:
        user = state.auth.authenticate(body.email, body.password)
    except ValueError as exc:
        status = getattr(exc, "status", 401)
        retry = getattr(exc, "retry_after", None)
        state.auth.record_event("login_failure", None, client, "locked" if status == 429 else "invalid_credentials")
        return JSONResponse(status_code=status, content={"error": str(exc)}, headers={"Retry-After": str(retry)} if retry else None)
    state.limiter.clear("login_email", email)
    state.auth.record_event("login_success", user["id"], client)
    return _issue_session(request, user)


@router.post("/auth/logout")
def logout(request: Request):
    state = request.app.state
    token = request.cookies.get(state.session_cookie)
    user = state.auth.session_user(token) if state.auth_enabled else None
    state.auth.revoke(token)
    if user:
        state.auth.record_event("logout", user["id"], client_fingerprint(request))
    response = JSONResponse({"signed_out": True})
    response.delete_cookie(state.session_cookie, path="/", httponly=True, samesite="strict", secure=state.cookie_secure)
    return response


@router.post("/onboarding")
def onboarding(body: OnboardingRequest, request: Request):
    state, user = request.app.state, principal(request)
    if not state.auth_enabled or user is None:
        return _disabled()
    limited = rate_limited(request, "onboarding_user", user["id"])
    if limited is not None:
        return limited
    if _hosted(request) and not body.hosted_inference_consent:
        return JSONResponse(status_code=400, content={"error": "Confirm that question text and approved catalog labels may be sent to the hosted model provider."})
    try:
        profile = state.auth.save_onboarding(user["id"], body.model_dump())
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    sample = None
    if profile["data_choice"] == "sample_logistics":
        if state.public_demo:
            return JSONResponse(status_code=403, content={"error": "Sample installation is disabled in public demo mode."})
        sample = state.registry.install_logistics_sample(user)
        state.auth.record_event("sample_installed", user["id"], client_fingerprint(request))
    state.auth.record_event("onboarding_completed", user["id"], client_fingerprint(request))
    return {"onboarding": profile, "sample_source": sample}


@router.get("/security/events")
def security_events(request: Request):
    state, user = request.app.state, principal(request)
    if not state.auth_enabled or user is None:
        return _disabled()
    if user["role"] != "owner":
        return JSONResponse(status_code=403, content={"error": "Only the workspace owner can review security events."})
    return {"events": state.auth.recent_events(200)}
