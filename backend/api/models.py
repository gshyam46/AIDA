"""
API Models for FastAPI endpoints
"""
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime

# Request Models
class QueryRequest(BaseModel):
    """Request model for natural language queries"""
    question: str = Field(..., description="Natural language question", min_length=1, max_length=1000)
    database_path: Optional[str] = Field(None, description="Optional database file path")

class DatabaseUploadRequest(BaseModel):
    """Request model for database file upload"""
    file_path: str = Field(..., description="Path to SQLite database file")

# Response Models
class PipelineStep(BaseModel):
    """Individual pipeline step information"""
    step_number: int
    step_name: str
    description: str
    input_data: Optional[Dict] = None
    output_data: Optional[Dict] = None
    execution_time_ms: Optional[int] = None
    status: str = "completed"  # "completed", "failed", "skipped"
    error: Optional[str] = None

class QueryResponse(BaseModel):
    """Response model for query execution with step-by-step breakdown"""
    success: bool
    question: str
    
    # Step-by-step pipeline execution details
    pipeline_steps: List[PipelineStep] = []
    
    # Final results (same as before for compatibility)
    semantic_ir: Optional[Dict] = None
    canonical_ir: Optional[Dict] = None
    sql: Optional[str] = None
    parameters: Optional[Dict] = None
    results: Optional[List[Dict]] = None
    execution_time_ms: int
    error: Optional[str] = None

class ColumnInfo(BaseModel):
    """Column information model"""
    name: str
    type: str
    nullable: bool
    primary_key: bool = False
    default_value: Optional[str] = None

class TableInfo(BaseModel):
    """Table information model"""
    name: str
    columns: List[ColumnInfo]
    row_count: Optional[int] = None

class SchemaResponse(BaseModel):
    """Response model for database schema"""
    success: bool
    tables: Dict[str, TableInfo] = {}
    total_tables: int = 0
    database_path: Optional[str] = None
    error: Optional[str] = None

class HealthResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: datetime
    version: str = "1.0.0"

class ErrorResponse(BaseModel):
    """Standard error response model"""
    success: bool = False
    error: str
    error_type: str
    timestamp: datetime
    request_id: Optional[str] = None

# Internal Models for Pipeline
class PipelineContext(BaseModel):
    """Context passed through the pipeline"""
    request_id: str
    question: str
    database_path: str
    start_time: datetime
    
class PipelineResult(BaseModel):
    """Result from pipeline execution with step-by-step details"""
    success: bool
    pipeline_steps: List[PipelineStep] = []
    semantic_ir: Optional[Dict] = None
    canonical_ir: Optional[Dict] = None
    sql: Optional[str] = None
    parameters: Optional[Dict] = None
    results: Optional[List[Dict]] = None
    execution_time_ms: int = 0
    error: Optional[str] = None
    error_type: Optional[str] = None