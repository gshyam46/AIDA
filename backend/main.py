"""AIDA hybrid analytics: model interpretation, deterministic SQL, accounts and abuse controls."""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

if __package__:
    from .api.auth_routes import router as auth_router
    from .api.endpoints import router
    from .core.analytics import AnalyticsEngine
    from .core.auth import SESSION_COOKIE, SESSION_TTL_SECONDS, AuthStore
    from .core.hybrid import HybridAnalytics
    from .core.interpreter import Interpreter
    from .core.llm import SemanticConfig
    from .core.security import MisuseMonitor, RateLimiter
    from .core.settings import runtime_environment
    from .core.sources import SourceRegistry, SourceError, MAX_UPLOAD_BYTES
    from .core.relational_demo import ensure_relational_demos
    from .core.chinook import chinook_source
else:
    from api.auth_routes import router as auth_router
    from api.endpoints import router
    from core.analytics import AnalyticsEngine
    from core.auth import SESSION_COOKIE, SESSION_TTL_SECONDS, AuthStore
    from core.hybrid import HybridAnalytics
    from core.interpreter import Interpreter
    from core.llm import SemanticConfig
    from core.security import MisuseMonitor, RateLimiter
    from core.settings import runtime_environment
    from core.sources import SourceRegistry, SourceError, MAX_UPLOAD_BYTES
    from core.relational_demo import ensure_relational_demos
    from core.chinook import chinook_source

logger = logging.getLogger("aida")
ROOT = Path(__file__).resolve().parent
MAX_BODY_BYTES = 8192
PUBLIC_API_PATHS = {"/api/v1/health", "/api/v1/auth/session", "/api/v1/auth/login", "/api/v1/auth/signup", "/api/v1/auth/logout"}
API_SECURITY_HEADERS = {"X-Frame-Options": "DENY", "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
                        "Cross-Origin-Resource-Policy": "same-origin", "Permissions-Policy": "camera=(), microphone=(), geolocation=()"}


def create_app(data_dir: Path | None = None, interpreter=None, public_demo: bool | None = None,
               settings: dict[str, str] | None = None, require_auth: bool | None = None) -> FastAPI:
    env = os.environ if settings is None else settings
    demo_mode = public_demo if public_demo is not None else env.get("AIDA_PUBLIC_DEMO", "0") == "1"
    # The packaged service passes settings and requires accounts unless explicitly disabled.
    auth_enabled = require_auth if require_auth is not None else (settings is not None and env.get("AIDA_REQUIRE_AUTH", "1") != "0")
    docs_enabled = not auth_enabled or env.get("AIDA_ENABLE_DOCS", "0") == "1"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        directory = data_dir or Path(env.get("AIDA_DATA_DIR", ROOT / "data"))
        seed = AnalyticsEngine(directory / "aida-demo.sqlite")
        seed.ensure_demo_data()
        registry = SourceRegistry(directory, public_demo=demo_mode)
        registry.register_commerce_demo(seed.database_path)
        registry.register_support_demo()
        for source in [*ensure_relational_demos(registry.directory), chinook_source()]:
            registry.register_source(source["path"], source["manifest"], source["id"])
        app.state.registry = registry
        app.state.public_demo = demo_mode
        app.state.engine = HybridAnalytics(registry, interpreter if interpreter is not None else Interpreter(SemanticConfig.from_env(env)))
        app.state.auth = AuthStore(directory / "auth.sqlite")
        app.state.limiter = RateLimiter()
        app.state.misuse = MisuseMonitor()
        app.state.auth_enabled = auth_enabled
        app.state.allow_signup = env.get("AIDA_ALLOW_SIGNUP", "1") != "0"
        app.state.cookie_secure = env.get("AIDA_COOKIE_SECURE", "0") == "1"
        app.state.session_cookie = SESSION_COOKIE
        app.state.session_ttl = SESSION_TTL_SECONDS
        logger.info("AIDA ready: model interpretation plus validated source-scoped SQL")
        yield

    application = FastAPI(title="AIDA Analytics", version="4.0.0", lifespan=lifespan,
        description="Artificial Intelligence Data Analyst: model interpretation over approved catalogs, deterministic source-scoped SQL.",
        docs_url="/docs" if docs_enabled else None, redoc_url="/redoc" if docs_enabled else None, openapi_url="/openapi.json" if docs_enabled else None)

    @application.middleware("http")
    async def request_boundary(request: Request, call_next):
        response = None
        request.state.user = None
        path = request.url.path
        try:
            host = request.headers.get("host", "")
            hostname = urlsplit("http://" + host).hostname
            if not demo_mode and hostname not in {"127.0.0.1", "localhost", "::1", "testserver"}:
                response = JSONResponse(status_code=403, content={"error": "Local mode accepts loopback hosts only."})
            origin = request.headers.get("origin")
            if request.method in {"POST", "PUT", "DELETE", "PATCH"} and origin:
                parsed_origin = urlsplit(origin)
                if parsed_origin.scheme not in {"http", "https"} or parsed_origin.netloc != host:
                    response = JSONResponse(status_code=403, content={"error": "Cross-origin changes are not allowed."})
            if response is None and request.method == "POST":
                is_upload = path == "/api/v1/sources"
                if demo_mode and (is_upload or path.endswith("/configure")):
                    response = JSONResponse(status_code=403, content={"error": "Onboarding is disabled in public demo mode."})
                else:
                    limit = MAX_UPLOAD_BYTES if is_upload else 65536 if path.endswith("/configure") else MAX_BODY_BYTES
                    body = bytearray()
                    async for chunk in request.stream():
                        if len(body) + len(chunk) > limit:
                            response = JSONResponse(status_code=413, content={"success": False,
                                "error": "Request exceeds the size limit for this operation.", "error_type": "request_too_large"})
                            break
                        body.extend(chunk)
                    request._body = bytes(body)
            if response is None and auth_enabled and path.startswith("/api/v1/"):
                state = request.app.state
                # Browsers cannot attach this header cross-site without a CORS preflight, which is never granted.
                if request.method in {"POST", "PUT", "DELETE", "PATCH"} and request.headers.get("x-aida-request") != "1":
                    response = JSONResponse(status_code=403, content={"error": "The request is missing AIDA's request header.", "error_type": "csrf_rejected"})
                elif path not in PUBLIC_API_PATHS:
                    user = state.auth.session_user(request.cookies.get(SESSION_COOKIE))
                    if user is None:
                        response = JSONResponse(status_code=401, content={"error": "Sign in to continue.", "error_type": "authentication_required"})
                    else:
                        request.state.user = user
            if response is None:
                response = await call_next(request)
        except Exception as exc:
            # Consume errors before Uvicorn can log exception values or request data.
            logger.error("Request failed: %s", type(exc).__name__)
            response = JSONResponse(status_code=500, content={"success": False,
                "error": "The data service could not complete this request. Please retry.", "error_type": "server_error"})
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        if path.startswith("/api/"):
            for key, value in API_SECURITY_HEADERS.items():
                response.headers[key] = value
        return response

    @application.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        return JSONResponse(status_code=422, content={"success": False,
            "error": "The request has missing or unsupported fields. Check the values and try again.",
            "error_type": "invalid_request"})

    @application.exception_handler(SourceError)
    async def invalid_source(request, exc):
        return JSONResponse(status_code=400, content={"success": False, "error": str(exc), "error_type": "source_error"})

    application.include_router(router, prefix="/api/v1")
    application.include_router(auth_router, prefix="/api/v1")

    @application.get("/")
    def root():
        return {"name": "AIDA", "mode": "public-demo" if demo_mode else "local", "docs": "/docs" if docs_enabled else None}
    return application

app = create_app(settings=runtime_environment())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, access_log=False)
