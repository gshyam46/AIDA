"""
SQL Compiler - Generates safe, parameterized SQL from canonical IR
"""
import logging
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass

from .ir_normalizer import CanonicalIR, Filter

logger = logging.getLogger(__name__)

@dataclass
class CompiledQuery:
    """Compiled SQL query with parameters"""
    sql: str
    parameters: Dict[str, Any]
    query_type: str

class QueryTemplate:
    """Reusable SQL query templates"""
    
    @staticmethod
    def select_all(table: str) -> str:
        """Template for SELECT * queries"""
        return f"SELECT * FROM {table}"
    
    @staticmethod
    def select_aggregate(aggregation: str, column: str, table: str) -> str:
        """Template for aggregation queries"""
        agg_func = aggregation.upper()
        if column == '*' or column is None:
            return f"SELECT {agg_func}(*) AS result FROM {table}"
        else:
            return f"SELECT {agg_func}({column}) AS result FROM {table}"
    
    @staticmethod
    def select_count(table: str) -> str:
        """Template for COUNT queries"""
        return f"SELECT COUNT(*) AS result FROM {table}"

class ParameterBinder:
    """Safe parameter handling for SQL queries"""
    
    @staticmethod
    def bind_parameters(sql: str, filters: List[Filter]) -> Tuple[str, Dict[str, Any]]:
        """Bind filter parameters to SQL query"""
        parameters = {}
        
        for filter_obj in filters:
            # Use the parameter name from the filter
            param_key = filter_obj.parameter_name
            parameters[param_key] = filter_obj.value
        
        return sql, parameters
    
    @staticmethod
    def validate_parameter_value(value: Any) -> Any:
        """Validate and sanitize parameter values"""
        # Convert to appropriate types
        if isinstance(value, str):
            # Basic string sanitization
            if len(value) > 1000:  # Prevent extremely long strings
                raise ValueError("Parameter value too long")
            return value
        elif isinstance(value, (int, float)):
            return value
        elif value is None:
            return None
        else:
            # Convert other types to string
            return str(value)

class SQLCompiler:
    """Deterministic SQL generation engine"""
    
    def __init__(self):
        self.template = QueryTemplate()
        self.binder = ParameterBinder()
    
    def compile(self, canonical_ir: CanonicalIR) -> CompiledQuery:
        """Generate safe, parameterized SQL from canonical IR"""
        try:
            # Generate base SQL based on intent
            if canonical_ir.intent == 'aggregate':
                sql = self._compile_aggregate_query(canonical_ir)
                query_type = 'aggregate'
            elif canonical_ir.intent == 'count':
                sql = self._compile_count_query(canonical_ir)
                query_type = 'count'
            elif canonical_ir.intent == 'retrieve':
                sql = self._compile_retrieve_query(canonical_ir)
                query_type = 'retrieve'
            else:
                raise ValueError(f"Unsupported intent: {canonical_ir.intent}")
            
            # Add WHERE clause if filters exist
            if canonical_ir.filters:
                where_clause, parameters = self._generate_where_clause(canonical_ir.filters)
                sql += f" {where_clause}"
            else:
                parameters = {}
            
            # Validate all parameters
            validated_parameters = {}
            for key, value in parameters.items():
                validated_parameters[key] = self.binder.validate_parameter_value(value)
            
            compiled_query = CompiledQuery(
                sql=sql,
                parameters=validated_parameters,
                query_type=query_type
            )
            
            logger.info(f"Compiled {query_type} query for table {canonical_ir.entity}")
            return compiled_query
            
        except Exception as e:
            logger.error(f"SQL compilation failed: {e}")
            raise
    
    def _compile_aggregate_query(self, canonical_ir: CanonicalIR) -> str:
        """Compile aggregation query"""
        if not canonical_ir.aggregation:
            raise ValueError("Aggregation function is required for aggregate queries")
        
        if not canonical_ir.metric:
            raise ValueError("Metric column is required for aggregate queries")
        
        return self.template.select_aggregate(
            canonical_ir.aggregation,
            canonical_ir.metric,
            canonical_ir.entity
        )
    
    def _compile_count_query(self, canonical_ir: CanonicalIR) -> str:
        """Compile count query"""
        return self.template.select_count(canonical_ir.entity)
    
    def _compile_retrieve_query(self, canonical_ir: CanonicalIR) -> str:
        """Compile data retrieval query"""
        return self.template.select_all(canonical_ir.entity)
    
    def _generate_where_clause(self, filters: List[Filter]) -> Tuple[str, Dict[str, Any]]:
        """Generate WHERE clause with parameters"""
        if not filters:
            return "", {}
        
        conditions = []
        parameters = {}
        
        for filter_obj in filters:
            # Generate condition with parameter placeholder
            condition = f"{filter_obj.column} {filter_obj.operator} :{filter_obj.parameter_name}"
            conditions.append(condition)
            parameters[filter_obj.parameter_name] = filter_obj.value
        
        where_clause = "WHERE " + " AND ".join(conditions)
        return where_clause, parameters
    
    def generate_select_clause(self, canonical_ir: CanonicalIR) -> str:
        """Generate SELECT clause based on intent"""
        if canonical_ir.intent == 'aggregate':
            if canonical_ir.aggregation and canonical_ir.metric:
                agg_func = canonical_ir.aggregation.upper()
                return f"SELECT {agg_func}({canonical_ir.metric}) AS result"
            else:
                raise ValueError("Aggregate queries require aggregation function and metric")
        
        elif canonical_ir.intent == 'count':
            return "SELECT COUNT(*) AS result"
        
        elif canonical_ir.intent == 'retrieve':
            return "SELECT *"
        
        else:
            raise ValueError(f"Unsupported intent: {canonical_ir.intent}")
    
    def validate_sql_safety(self, sql: str) -> bool:
        """Validate that generated SQL is safe (read-only)"""
        sql_upper = sql.upper().strip()
        
        # Must start with SELECT
        if not sql_upper.startswith('SELECT'):
            logger.error(f"Generated SQL is not a SELECT statement: {sql}")
            return False
        
        # Check for dangerous keywords
        dangerous_keywords = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
            'TRUNCATE', 'REPLACE', 'MERGE', 'EXEC', 'EXECUTE', 'PRAGMA'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                logger.error(f"Dangerous keyword '{keyword}' found in SQL: {sql}")
                return False
        
        # Check for semicolons (prevent multiple statements)
        if ';' in sql.rstrip(';'):  # Allow trailing semicolon
            logger.error(f"Multiple statements detected in SQL: {sql}")
            return False
        
        return True

# Error classes
class CompilationError(Exception):
    """Base exception for SQL compilation errors"""
    pass

class UnsupportedIntentError(CompilationError):
    """Unsupported query intent error"""
    pass

class InvalidParameterError(CompilationError):
    """Invalid parameter value error"""
    pass

class SQLSafetyError(CompilationError):
    """SQL safety validation error"""
    pass