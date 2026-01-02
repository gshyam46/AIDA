"""
Query Executor - Safe SQL execution with timeout and result formatting
"""
import logging
import time
import signal
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from contextlib import contextmanager

from .sql_compiler import CompiledQuery
from .database import DatabaseConnector

logger = logging.getLogger(__name__)

@dataclass
class ExecutionResult:
    """Structured query execution results"""
    success: bool
    data: List[Dict]
    row_count: int
    execution_time_ms: int
    error: Optional[str]

class ExecutionConfig:
    """Configuration for query execution"""
    
    def __init__(self):
        self.timeout_seconds = 30
        self.max_rows = 10000
        self.enable_logging = True
        self.format_numbers = True

class QueryExecutor:
    """Safe execution engine with timeout and result formatting"""
    
    def __init__(self, config: ExecutionConfig = None):
        self.config = config or ExecutionConfig()
    
    def execute(self, query: CompiledQuery, db: DatabaseConnector) -> ExecutionResult:
        """Execute compiled SQL query safely with timeout"""
        start_time = time.time()
        
        try:
            # Validate query safety
            if not self._validate_query_safety(query):
                return ExecutionResult(
                    success=False,
                    data=[],
                    row_count=0,
                    execution_time_ms=0,
                    error="Query failed safety validation"
                )
            
            # Log query execution
            if self.config.enable_logging:
                logger.info(f"Executing {query.query_type} query: {query.sql}")
                logger.debug(f"Query parameters: {query.parameters}")
            
            # Execute with timeout
            with self._timeout_context(self.config.timeout_seconds):
                raw_results = db.execute_query(query.sql, query.parameters)
            
            # Format results
            formatted_data = self._format_results(raw_results, query.query_type)
            
            # Check row limit
            if len(formatted_data) > self.config.max_rows:
                logger.warning(f"Query returned {len(formatted_data)} rows, truncating to {self.config.max_rows}")
                formatted_data = formatted_data[:self.config.max_rows]
            
            execution_time = int((time.time() - start_time) * 1000)
            
            result = ExecutionResult(
                success=True,
                data=formatted_data,
                row_count=len(formatted_data),
                execution_time_ms=execution_time,
                error=None
            )
            
            logger.info(f"Query executed successfully in {execution_time}ms, returned {len(formatted_data)} rows")
            return result
            
        except TimeoutError:
            execution_time = int((time.time() - start_time) * 1000)
            error_msg = f"Query timeout after {self.config.timeout_seconds} seconds"
            logger.error(error_msg)
            
            return ExecutionResult(
                success=False,
                data=[],
                row_count=0,
                execution_time_ms=execution_time,
                error=error_msg
            )
            
        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            error_msg = f"Query execution failed: {str(e)}"
            logger.error(error_msg)
            
            return ExecutionResult(
                success=False,
                data=[],
                row_count=0,
                execution_time_ms=execution_time,
                error=error_msg
            )
    
    def _validate_query_safety(self, query: CompiledQuery) -> bool:
        """Validate query safety before execution"""
        sql_upper = query.sql.upper().strip()
        
        # Must be a SELECT statement
        if not sql_upper.startswith('SELECT'):
            logger.error(f"Non-SELECT query rejected: {query.sql}")
            return False
        
        # Check for dangerous patterns
        dangerous_patterns = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
            'TRUNCATE', 'REPLACE', 'MERGE', 'EXEC', 'EXECUTE',
            'PRAGMA', 'ATTACH', 'DETACH'
        ]
        
        for pattern in dangerous_patterns:
            if pattern in sql_upper:
                logger.error(f"Dangerous pattern '{pattern}' found in query: {query.sql}")
                return False
        
        # Validate parameters
        for key, value in query.parameters.items():
            if not self._validate_parameter(key, value):
                return False
        
        return True
    
    def _validate_parameter(self, key: str, value: Any) -> bool:
        """Validate individual parameter"""
        # Check parameter name format
        if not key.startswith('param') and not key.isalnum():
            logger.error(f"Invalid parameter name: {key}")
            return False
        
        # Check parameter value
        if isinstance(value, str):
            if len(value) > 1000:
                logger.error(f"Parameter value too long: {len(value)} characters")
                return False
            
            # Check for SQL injection patterns
            dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'sp_']
            for char in dangerous_chars:
                if char in value.lower():
                    logger.warning(f"Potentially dangerous character sequence in parameter: {char}")
        
        return True
    
    @contextmanager
    def _timeout_context(self, timeout_seconds: int):
        """Context manager for query timeout"""
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Query timeout after {timeout_seconds} seconds")
        
        # Set up timeout signal (Unix-like systems)
        try:
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            yield
        except AttributeError:
            # Windows doesn't support SIGALRM, use basic timeout
            # This is a simplified approach for cross-platform compatibility
            yield
        finally:
            try:
                signal.alarm(0)  # Cancel alarm
                signal.signal(signal.SIGALRM, old_handler)
            except AttributeError:
                pass  # Windows
    
    def _format_results(self, raw_results: List[Dict], query_type: str) -> List[Dict]:
        """Format query results with appropriate data types"""
        if not raw_results:
            return []
        
        formatted_results = []
        
        for row in raw_results:
            formatted_row = {}
            
            for key, value in row.items():
                formatted_value = self._format_value(value, key, query_type)
                formatted_row[key] = formatted_value
            
            formatted_results.append(formatted_row)
        
        return formatted_results
    
    def _format_value(self, value: Any, column_name: str, query_type: str) -> Any:
        """Format individual value based on type and context"""
        if value is None:
            return None
        
        # Handle numeric values
        if isinstance(value, (int, float)):
            if self.config.format_numbers:
                # For aggregation results, format with appropriate precision
                if query_type == 'aggregate' and column_name == 'result':
                    if isinstance(value, float):
                        # Round to 2 decimal places for monetary values
                        return round(value, 2)
                    return value
                else:
                    return value
            return value
        
        # Handle string values
        elif isinstance(value, str):
            return value.strip()
        
        # Handle other types
        else:
            return str(value)
    
    def format_results(self, raw_results: List[Dict]) -> Dict:
        """Public method to format raw database results"""
        return {
            'data': self._format_results(raw_results, 'retrieve'),
            'row_count': len(raw_results),
            'formatted_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }

# Error classes
class ExecutionError(Exception):
    """Base exception for query execution errors"""
    pass

class QueryTimeoutError(ExecutionError):
    """Query timeout error"""
    pass

class QuerySafetyError(ExecutionError):
    """Query safety validation error"""
    pass

class ParameterValidationError(ExecutionError):
    """Parameter validation error"""
    pass