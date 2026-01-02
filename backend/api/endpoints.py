"""
FastAPI endpoints for SQL MVP API
"""
import os
import uuid
import logging
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from fastapi.responses import JSONResponse

from .models import (
    QueryRequest, QueryResponse, SchemaResponse, HealthResponse,
    ErrorResponse, TableInfo, ColumnInfo
)
from core.pipeline import SQLPipeline
from core.database import DatabaseConnector

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Global pipeline instance (will be initialized on startup)
pipeline: SQLPipeline = None
db_connector: DatabaseConnector = None

def get_pipeline() -> SQLPipeline:
    """Dependency to get pipeline instance"""
    if pipeline is None:
        raise HTTPException(status_code=500, detail="Pipeline not initialized")
    return pipeline

def get_db_connector() -> DatabaseConnector:
    """Dependency to get database connector instance"""
    if db_connector is None:
        raise HTTPException(status_code=500, detail="Database connector not initialized")
    return db_connector

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now()
    )

@router.post("/query", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    pipeline_instance: SQLPipeline = Depends(get_pipeline)
):
    """Execute natural language query"""
    request_id = str(uuid.uuid4())
    start_time = datetime.now()
    
    try:
        logger.info(f"[{request_id}] Processing query: {request.question[:100]}...")
        
        # Use default database if none specified
        db_path = request.database_path or os.getenv("DEFAULT_DB_PATH", "uploads/olist.sqlite")
        
        # Execute pipeline
        result = await pipeline_instance.execute(
            question=request.question,
            database_path=db_path,
            request_id=request_id
        )
        
        execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if result.success:
            return QueryResponse(
                success=True,
                question=request.question,
                pipeline_steps=result.pipeline_steps,
                semantic_ir=result.semantic_ir,
                canonical_ir=result.canonical_ir,
                sql=result.sql,
                parameters=result.parameters,
                results=result.results,
                execution_time_ms=execution_time
            )
        else:
            return QueryResponse(
                success=False,
                question=request.question,
                pipeline_steps=result.pipeline_steps,
                execution_time_ms=execution_time,
                error=result.error
            )
    
    except Exception as e:
        execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
        logger.error(f"[{request_id}] Query execution failed: {e}")
        
        return QueryResponse(
            success=False,
            question=request.question,
            pipeline_steps=[],
            execution_time_ms=execution_time,
            error=f"Internal server error: {str(e)}"
        )

@router.get("/schema", response_model=SchemaResponse)
async def get_database_schema(
    database_path: str = None,
    db: DatabaseConnector = Depends(get_db_connector)
):
    """Get database schema information"""
    try:
        # Use default database if none specified
        db_path = database_path or os.getenv("DEFAULT_DB_PATH", "uploads/olist.sqlite")
        
        # Connect to database
        if not db.connect(db_path):
            raise HTTPException(status_code=400, detail=f"Failed to connect to database: {db_path}")
        
        # Get schema
        schema = db.introspect_schema()
        
        # Convert to API models
        tables = {}
        for table_name, table_schema in schema.items():
            columns = []
            for col_name, col_info in table_schema.columns.items():
                columns.append(ColumnInfo(
                    name=col_info.name,
                    type=col_info.type,
                    nullable=col_info.nullable,
                    primary_key=col_info.primary_key,
                    default_value=col_info.default_value
                ))
            
            tables[table_name] = TableInfo(
                name=table_schema.name,
                columns=columns,
                row_count=table_schema.row_count
            )
        
        return SchemaResponse(
            success=True,
            tables=tables,
            total_tables=len(tables),
            database_path=db_path
        )
    
    except Exception as e:
        logger.error(f"Schema introspection failed: {e}")
        return SchemaResponse(
            success=False,
            error=str(e)
        )

@router.post("/upload")
async def upload_database(file: UploadFile = File(...)):
    """Upload SQLite database file"""
    try:
        # Validate file type
        if not file.filename.endswith('.sqlite') and not file.filename.endswith('.db'):
            raise HTTPException(status_code=400, detail="Only SQLite database files (.sqlite, .db) are allowed")
        
        # Create uploads directory if it doesn't exist
        upload_dir = Path("uploads")
        upload_dir.mkdir(exist_ok=True)
        
        # Save uploaded file
        file_path = upload_dir / file.filename
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Test database connection
        test_db = DatabaseConnector()
        if not test_db.connect(str(file_path)):
            # Clean up failed upload
            file_path.unlink()
            raise HTTPException(status_code=400, detail="Invalid SQLite database file")
        
        # Get basic schema info
        schema = test_db.introspect_schema()
        test_db.close()
        
        return {
            "success": True,
            "message": f"Database uploaded successfully",
            "file_path": str(file_path),
            "tables": list(schema.keys()),
            "table_count": len(schema)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Database upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@router.get("/examples")
async def get_example_queries():
    """Get example queries for the current database"""
    return {
        "examples": [
            {
                "question": "What's the total revenue this month?",
                "description": "Aggregate query with time filtering"
            },
            {
                "question": "Count all orders",
                "description": "Simple count query"
            },
            {
                "question": "Average order amount",
                "description": "Average aggregation query"
            },
            {
                "question": "Show me all customers",
                "description": "Data retrieval query"
            },
            {
                "question": "Total sales last month",
                "description": "Aggregate with time range"
            }
        ]
    }

# Note: Exception handlers will be added to the main FastAPI app in main.py