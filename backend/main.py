"""AIDA hybrid analytics: private semantic interpretation, deterministic SQL."""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

if __package__:
    from .api.endpoints import router
    from .core.analytics import AnalyticsEngine
    from .core.hybrid import HybridAnalytics
    from .core.sources import SourceRegistry, SourceError, MAX_UPLOAD_BYTES
    from .core.relational_demo import ensure_relational_demos
    from .core.chinook import chinook_source
    from .core.connectors import ConnectionService
else:
    from api.endpoints import router
    from core.analytics import AnalyticsEngine
    from core.hybrid import HybridAnalytics
    from core.sources import SourceRegistry, SourceError, MAX_UPLOAD_BYTES
    from core.relational_demo import ensure_relational_demos
    from core.chinook import chinook_source
    from core.connectors import ConnectionService

logger = logging.getLogger("aida")
ROOT = Path(__file__).resolve().parent
MAX_BODY_BYTES = 8192

def create_app(data_dir: Path | None = None, semantic_parser=None, public_demo: bool | None = None) -> FastAPI:
    demo_mode = public_demo if public_demo is not None else os.environ.get("AIDA_PUBLIC_DEMO", "0") == "1"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        directory = data_dir or Path(os.environ.get("AIDA_DATA_DIR", ROOT / "data"))
        seed = AnalyticsEngine(directory / "aida-demo.sqlite")
        seed.ensure_demo_data()
        registry = SourceRegistry(directory, public_demo=demo_mode)
        registry.register_commerce_demo(seed.database_path)
        registry.register_support_demo()
        for source in [*ensure_relational_demos(registry.directory), chinook_source()]:
            registry.register_source(source["path"], source["manifest"], source["id"])
        app.state.registry = registry
        app.state.public_demo = demo_mode
        app.state.engine = HybridAnalytics(registry, semantic_parser)
        app.state.connections = ConnectionService(registry)
        app.state.connections.start()
        logger.info("AIDA ready: local semantic model plus validated source-scoped SQL")
        try:
            yield
        finally:
            app.state.connections.close()

    application = FastAPI(title="AIDA Analytics", version="3.0.0",
        description="Private model interpretation with deterministic, source-scoped SQL and explicit catalog onboarding.", lifespan=lifespan)

    @application.middleware("http")
    async def request_boundary(request: Request, call_next):
        response = None
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
                is_upload = request.url.path == "/api/v1/sources"
                if demo_mode and (is_upload or request.url.path.endswith("/configure") or request.url.path.startswith("/api/v1/connections")):
                    response = JSONResponse(status_code=403, content={"error": "Onboarding is disabled in public demo mode."})
                else:
                    limit = MAX_UPLOAD_BYTES if is_upload else 65536 if request.url.path.endswith("/configure") or request.url.path.startswith("/api/v1/connections") else MAX_BODY_BYTES
                    body = bytearray()
                    async for chunk in request.stream():
                        if len(body) + len(chunk) > limit:
                            response = JSONResponse(status_code=413, content={"success": False,
                                "error": "Request exceeds the size limit for this operation.", "error_type": "request_too_large"})
                            break
                        body.extend(chunk)
                    request._body = bytes(body)
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
        return response

    @application.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        return JSONResponse(status_code=422, content={"success": False,
            "error": "Select a source and provide either a question (1-1500 characters) or a query plan. Other fields are not supported.",
            "error_type": "invalid_request"})

    @application.exception_handler(SourceError)
    async def invalid_source(request, exc):
        return JSONResponse(status_code=400, content={"success": False, "error": str(exc), "error_type": "source_error"})

    application.include_router(router, prefix="/api/v1")

    @application.get("/")
    def root():
        return {"name": "AIDA", "mode": "public-demo" if demo_mode else "local", "docs": "/docs"}
    return application

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, access_log=False)
