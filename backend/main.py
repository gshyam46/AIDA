"""
SQL MVP - Main FastAPI Application
"""
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from api.endpoints import router
from api.models import ErrorResponse
from core.pipeline import SQLPipeline
from core.database import DatabaseConnector

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Global instances
pipeline_instance = None
db_connector_instance = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global pipeline_instance, db_connector_instance
    
    # Startup
    logger.info("Starting SQL MVP API...")
    
    # Initialize pipeline
    business_rules_path = "./config/business_rules.yaml"
    pipeline_instance = SQLPipeline(business_rules_path)
    
    # Initialize database connector
    db_connector_instance = DatabaseConnector()
    
    # Set global instances for dependency injection
    import api.endpoints
    api.endpoints.pipeline = pipeline_instance
    api.endpoints.db_connector = db_connector_instance
    
    logger.info("SQL MVP API started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down SQL MVP API...")
    if pipeline_instance:
        pipeline_instance.close()
    if db_connector_instance:
        db_connector_instance.close()
    logger.info("SQL MVP API shutdown complete")

app = FastAPI(
    title="SQL MVP API",
    description="Natural Language to SQL Pipeline",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],  # Next.js ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api/v1")

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            error_type="http_error",
            timestamp=datetime.now()
        ).dict()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            error_type="server_error",
            timestamp=datetime.now()
        ).dict()
    )

@app.get("/")
async def root():
    return {
        "message": "SQL MVP API is running",
        "version": "1.0.0",
        "docs": "/docs"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)