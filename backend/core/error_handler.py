"""
Structured error handling with user-friendly messages
"""
import logging
import uuid
from datetime import datetime
from typing import Dict, Optional, Any
from enum import Enum

logger = logging.getLogger(__name__)

class ErrorType(Enum):
    """Error type categories"""
    DATABASE_ERROR = "database_error"
    LLM_ERROR = "llm_error"
    VALIDATION_ERROR = "validation_error"
    BUSINESS_LOGIC_ERROR = "business_logic_error"
    COMPILATION_ERROR = "compilation_error"
    EXECUTION_ERROR = "execution_error"
    PIPELINE_ERROR = "pipeline_error"
    SAFETY_ERROR = "safety_error"
    TIMEOUT_ERROR = "timeout_error"
    CONFIGURATION_ERROR = "configuration_error"

class ErrorHandler:
    """Centralized error handling with structured logging and user-friendly messages"""
    
    def __init__(self):
        self.error_messages = {
            # Database errors
            "database_connection_failed": "Unable to connect to the database. Please check the file path and ensure the database file exists.",
            "database_file_not_found": "Database file not found. Please upload a valid SQLite database file.",
            "database_corrupted": "The database file appears to be corrupted or invalid. Please try uploading a different file.",
            "schema_introspection_failed": "Unable to read the database structure. The database may be empty or corrupted.",
            
            # LLM errors
            "llm_timeout": "The AI service is taking too long to respond. Please try again in a moment.",
            "llm_rate_limit": "The AI service is temporarily busy. Please wait a moment and try again.",
            "llm_invalid_response": "The AI service returned an unexpected response. Please try rephrasing your question.",
            "llm_network_error": "Unable to connect to the AI service. Please check your internet connection.",
            "llm_api_key_invalid": "AI service authentication failed. Please check your API key configuration.",
            
            # Validation errors
            "unknown_table": "The table '{entity}' was not found in the database. Available tables: {available_tables}",
            "unknown_column": "The column '{column}' was not found in table '{table}'. Available columns: {available_columns}",
            "unsupported_aggregation": "The aggregation function '{aggregation}' is not supported. Supported functions: sum, count, avg",
            "invalid_filter_operator": "The filter operator '{operator}' is not valid. Valid operators: =, >, <, >=, <=",
            
            # Business logic errors
            "unknown_metric": "The metric '{metric}' is not recognized. Try using terms like 'revenue', 'total', 'sales', or 'count'.",
            "unknown_entity": "The entity '{entity}' is not recognized. Try referring to specific table names in your database.",
            "invalid_time_expression": "The time expression '{expression}' is not recognized. Try 'this month', 'last month', or 'last 7 days'.",
            
            # Safety errors
            "dangerous_operation": "This operation is not allowed for security reasons. Only read-only queries are supported.",
            "system_table_access": "Access to system tables is not permitted.",
            "sql_injection_detected": "The query contains potentially dangerous content and has been blocked.",
            
            # V0 scope limitations
            "joins_not_supported": "Table joins are not supported in this version. Please use single-table queries only.",
            "subqueries_not_supported": "Subqueries are not supported in this version. Please simplify your query.",
            "multiple_metrics_not_supported": "Multiple metrics in one query are not supported. Please ask for one metric at a time.",
            
            # Execution errors
            "query_timeout": "The query took too long to execute. Please try a simpler query or add more specific filters.",
            "too_many_results": "The query returned too many results. Please add filters to narrow down your search.",
            "execution_failed": "The query could not be executed. Please check your question and try again.",
            
            # General errors
            "pipeline_error": "An internal error occurred while processing your request. Please try again.",
            "configuration_error": "System configuration error. Please contact support.",
            "unknown_error": "An unexpected error occurred. Please try again or contact support."
        }
    
    def handle_error(
        self,
        error: Exception,
        error_type: ErrorType,
        context: Dict[str, Any] = None,
        request_id: str = None
    ) -> Dict[str, Any]:
        """Handle error with structured logging and user-friendly message"""
        
        request_id = request_id or str(uuid.uuid4())
        context = context or {}
        
        # Log detailed error for debugging
        self._log_detailed_error(error, error_type, context, request_id)
        
        # Generate user-friendly message
        user_message = self._generate_user_message(error, error_type, context)
        
        return {
            "success": False,
            "error": user_message,
            "error_type": error_type.value,
            "request_id": request_id,
            "timestamp": datetime.now().isoformat()
        }
    
    def _log_detailed_error(
        self,
        error: Exception,
        error_type: ErrorType,
        context: Dict[str, Any],
        request_id: str
    ) -> None:
        """Log detailed error information for debugging"""
        
        log_data = {
            "request_id": request_id,
            "error_type": error_type.value,
            "error_message": str(error),
            "error_class": error.__class__.__name__,
            "context": context,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.error(f"[{request_id}] {error_type.value}: {str(error)}", extra=log_data)
    
    def _generate_user_message(
        self,
        error: Exception,
        error_type: ErrorType,
        context: Dict[str, Any]
    ) -> str:
        """Generate user-friendly error message"""
        
        error_str = str(error).lower()
        
        # Database errors
        if error_type == ErrorType.DATABASE_ERROR:
            if "no such file" in error_str or "not found" in error_str:
                return self.error_messages["database_file_not_found"]
            elif "database is locked" in error_str:
                return "The database is currently in use. Please try again in a moment."
            elif "not a database" in error_str or "corrupted" in error_str:
                return self.error_messages["database_corrupted"]
            else:
                return self.error_messages["database_connection_failed"]
        
        # LLM errors
        elif error_type == ErrorType.LLM_ERROR:
            if "timeout" in error_str:
                return self.error_messages["llm_timeout"]
            elif "rate limit" in error_str or "quota" in error_str:
                return self.error_messages["llm_rate_limit"]
            elif "invalid json" in error_str or "parse" in error_str:
                return self.error_messages["llm_invalid_response"]
            elif "network" in error_str or "connection" in error_str:
                return self.error_messages["llm_network_error"]
            elif "unauthorized" in error_str or "api key" in error_str:
                return self.error_messages["llm_api_key_invalid"]
            else:
                return self.error_messages["llm_invalid_response"]
        
        # Validation errors
        elif error_type == ErrorType.VALIDATION_ERROR:
            if "table" in error_str and "not exist" in error_str:
                entity = context.get("entity", "unknown")
                available_tables = context.get("available_tables", [])
                return self.error_messages["unknown_table"].format(
                    entity=entity,
                    available_tables=", ".join(available_tables)
                )
            elif "column" in error_str and "not exist" in error_str:
                column = context.get("column", "unknown")
                table = context.get("table", "unknown")
                available_columns = context.get("available_columns", [])
                return self.error_messages["unknown_column"].format(
                    column=column,
                    table=table,
                    available_columns=", ".join(available_columns)
                )
            elif "aggregation" in error_str and "not supported" in error_str:
                aggregation = context.get("aggregation", "unknown")
                return self.error_messages["unsupported_aggregation"].format(
                    aggregation=aggregation
                )
            else:
                return str(error)
        
        # Business logic errors
        elif error_type == ErrorType.BUSINESS_LOGIC_ERROR:
            if "unknown metric" in error_str:
                metric = context.get("metric", "unknown")
                return self.error_messages["unknown_metric"].format(metric=metric)
            elif "unknown entity" in error_str:
                entity = context.get("entity", "unknown")
                return self.error_messages["unknown_entity"].format(entity=entity)
            elif "time expression" in error_str:
                expression = context.get("time_expression", "unknown")
                return self.error_messages["invalid_time_expression"].format(expression=expression)
            else:
                return str(error)
        
        # Safety errors
        elif error_type == ErrorType.SAFETY_ERROR:
            if "dangerous" in error_str or "not allowed" in error_str:
                return self.error_messages["dangerous_operation"]
            elif "system table" in error_str:
                return self.error_messages["system_table_access"]
            else:
                return self.error_messages["sql_injection_detected"]
        
        # Scope limitation errors
        elif "join" in error_str:
            return self.error_messages["joins_not_supported"]
        elif "subquery" in error_str:
            return self.error_messages["subqueries_not_supported"]
        elif "multiple metric" in error_str:
            return self.error_messages["multiple_metrics_not_supported"]
        
        # Execution errors
        elif error_type == ErrorType.EXECUTION_ERROR:
            if "timeout" in error_str:
                return self.error_messages["query_timeout"]
            elif "too many" in error_str:
                return self.error_messages["too_many_results"]
            else:
                return self.error_messages["execution_failed"]
        
        # Default fallback
        else:
            return self.error_messages.get("unknown_error", str(error))
    
    def create_validation_error(self, message: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a validation error response"""
        return self.handle_error(
            ValueError(message),
            ErrorType.VALIDATION_ERROR,
            context
        )
    
    def create_business_logic_error(self, message: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a business logic error response"""
        return self.handle_error(
            ValueError(message),
            ErrorType.BUSINESS_LOGIC_ERROR,
            context
        )
    
    def create_safety_error(self, message: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a safety error response"""
        return self.handle_error(
            ValueError(message),
            ErrorType.SAFETY_ERROR,
            context
        )

# Global error handler instance
error_handler = ErrorHandler()