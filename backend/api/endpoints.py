"""Source-scoped hybrid analytics and explicit local SQLite onboarding."""
from datetime import datetime, timezone
from urllib.parse import unquote
from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse
from .models import QueryRequest

router = APIRouter()

@router.get("/connections")
def connections(request: Request):
    return request.app.state.connections.list()

@router.post("/connections/inspect")
def inspect_connection(request: Request, config: dict = Body(...)):
    return request.app.state.connections.inspect(config)

@router.post("/connections")
def create_connection(request: Request, config: dict = Body(...)):
    return JSONResponse(status_code=202, content=request.app.state.connections.create(config))

@router.post("/connections/{connection_id}/{action}")
def connection_action(connection_id: str, action: str, request: Request, config: dict = Body(default={})):
    return request.app.state.connections.action(connection_id, action, config)

@router.get("/health")
def health(request: Request):
    state = request.app.state
    catalog = state.engine.catalog()
    return {"status": "healthy", "version": "3.0.0", "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": "public-demo" if state.public_demo else "local", "dataset": catalog["dataset"],
            "model": state.engine.parser.status()}

@router.get("/sources")
def sources(request: Request):
    state = request.app.state
    return {"sources": state.registry.list_sources(), "default_source_id": "commerce",
            "uploads_enabled": not state.public_demo, "mode": "public-demo" if state.public_demo else "local",
            "model": state.engine.parser.status()}

@router.get("/sources/{source_id}")
def inspect_source(source_id: str, request: Request):
    return request.app.state.registry.inspect_source(source_id)

@router.post("/sources")
async def upload_source(request: Request):
    if request.app.state.public_demo:
        return JSONResponse(status_code=403, content={"error": "Database uploads are disabled in the public demo."})
    if request.headers.get("content-type", "").split(";")[0] not in {"application/octet-stream", "application/vnd.sqlite3", "application/x-sqlite3"}:
        return JSONResponse(status_code=415, content={"error": "Upload the SQLite file as application/octet-stream."})
    # Middleware has already capped the streamed file size.
    return request.app.state.registry.inspect_upload(await request.body(), unquote(request.headers.get("x-source-name", "database.sqlite")))

@router.post("/sources/{source_id}/configure")
def configure_source(source_id: str, request: Request, config: dict = Body(...)):
    if request.app.state.public_demo:
        return JSONResponse(status_code=403, content={"error": "Database mapping is disabled in the public demo."})
    return request.app.state.registry.configure(source_id, config)

@router.get("/catalog")
def catalog(request: Request, source_id: str = "commerce"):
    return request.app.state.engine.catalog(source_id)

@router.get("/schema")
def schema(request: Request, source_id: str = "commerce"):
    return request.app.state.engine.catalog(source_id)

@router.get("/examples")
def examples(request: Request, source_id: str = "commerce"):
    return {"examples": request.app.state.engine.catalog(source_id)["examples"]}

@router.post("/query")
def query(body: QueryRequest, request: Request):
    return request.app.state.engine.query(question=body.question, plan=body.plan, source_id=body.source_id, catalog_version=body.catalog_version)
