"""
IR Validator - Validates canonical IR against schema and safety rules
"""
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

from .ir_normalizer import CanonicalIR
from .database import TableSchema

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Result of IR validation"""
    valid: bool
    errors: List[str]
    warnings: List[str]

class SafetyRules:
    """Configurable safety constraints"""
    
    def __init__(self):
        # Dangerous SQL keywords that should never appear
        self.dangerous_keywords = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
            'TRUNCATE', 'REPLACE', 'MERGE', 'EXEC', 'EXECUTE'
        ]
        
        # Dangerous operators
        self.dangerous_operators = ['!=', 'NOT', '<>']
        
        # System tables that should not be accessed
        self.system_tables = [
            'sqlite_master', 'sqlite_sequence', 'sqlite_stat1',
            'sqlite_stat2', 'sqlite_stat3', 'sqlite_stat4'
        ]
        
        # Maximum number of filters to prevent complex queries
        self.max_filters = 10
        
        # Supported aggregation functions for V0
        self.supported_aggregations = ['sum', 'count', 'avg']
        
        # Supported intents for V0
        self.supported_intents = ['aggregate', 'retrieve', 'count']

class IRValidator:
    """Validates canonical IR against schema and safety rules"""
    
    def __init__(self, safety_rules: SafetyRules = None):
        self.safety_rules = safety_rules or SafetyRules()
    
    def validate(self, canonical_ir: CanonicalIR, schema: Dict[str, TableSchema]) -> ValidationResult:
        """Validate canonical IR against schema and safety rules"""
        errors = []
        warnings = []
        
        try:
            # Basic structure validation
            structure_errors = self._validate_structure(canonical_ir)
            errors.extend(structure_errors)
            
            # Schema validation
            schema_errors = self._validate_against_schema(canonical_ir, schema)
            errors.extend(schema_errors)
            
            # Safety validation
            safety_errors = self._validate_safety_rules(canonical_ir)
            errors.extend(safety_errors)
            
            # V0 scope validation
            scope_errors = self._validate_v0_scope(canonical_ir)
            errors.extend(scope_errors)
            
            # Business logic validation
            business_errors = self._validate_business_logic(canonical_ir, schema)
            errors.extend(business_errors)
            
            # Performance warnings
            perf_warnings = self._check_performance_warnings(canonical_ir)
            warnings.extend(perf_warnings)
            
            result = ValidationResult(
                valid=len(errors) == 0,
                errors=errors,
                warnings=warnings
            )
            
            if result.valid:
                logger.info("Canonical IR validation passed")
            else:
                logger.warning(f"Canonical IR validation failed with {len(errors)} errors")
            
            return result
            
        except Exception as e:
            logger.error(f"Validation process failed: {e}")
            return ValidationResult(
                valid=False,
                errors=[f"Validation process error: {str(e)}"],
                warnings=[]
            )
    
    def _validate_structure(self, canonical_ir: CanonicalIR) -> List[str]:
        """Validate basic structure of canonical IR"""
        errors = []
        
        # Required fields
        if not canonical_ir.intent:
            errors.append("Intent is required")
        
        if not canonical_ir.entity:
            errors.append("Entity (table name) is required")
        
        # Intent-specific validation
        if canonical_ir.intent == 'aggregate':
            if not canonical_ir.aggregation:
                errors.append("Aggregation function is required for aggregate intent")
            if not canonical_ir.metric:
                errors.append("Metric (column) is required for aggregate intent")
        
        elif canonical_ir.intent == 'count':
            if canonical_ir.aggregation and canonical_ir.aggregation != 'count':
                errors.append("Count intent should use 'count' aggregation or none")
        
        return errors
    
    def _validate_against_schema(self, canonical_ir: CanonicalIR, schema: Dict[str, TableSchema]) -> List[str]:
        """Validate IR against database schema"""
        errors = []
        
        # Validate entity exists
        if canonical_ir.entity not in schema:
            errors.append(f"Table '{canonical_ir.entity}' does not exist in database schema")
            return errors  # Can't continue without valid table
        
        table_schema = schema[canonical_ir.entity]
        
        # Validate metric column
        if canonical_ir.metric:
            if canonical_ir.metric not in table_schema.columns:
                errors.append(f"Column '{canonical_ir.metric}' does not exist in table '{canonical_ir.entity}'")
            else:
                # Validate aggregation compatibility with column type
                column_info = table_schema.columns[canonical_ir.metric]
                if canonical_ir.aggregation in ['sum', 'avg']:
                    if column_info.type not in ['INTEGER', 'REAL']:
                        errors.append(f"Aggregation '{canonical_ir.aggregation}' requires numeric column, but '{canonical_ir.metric}' is {column_info.type}")
        
        # Validate filter columns
        for filter_obj in canonical_ir.filters:
            if filter_obj.column not in table_schema.columns:
                errors.append(f"Filter column '{filter_obj.column}' does not exist in table '{canonical_ir.entity}'")
        
        # Validate time range column
        if canonical_ir.time_range and canonical_ir.time_range.column not in table_schema.columns:
            errors.append(f"Time range column '{canonical_ir.time_range.column}' does not exist in table '{canonical_ir.entity}'")
        
        return errors
    
    def _validate_safety_rules(self, canonical_ir: CanonicalIR) -> List[str]:
        """Validate against safety rules"""
        errors = []
        
        # Check for dangerous keywords in entity name
        entity_upper = canonical_ir.entity.upper()
        for keyword in self.safety_rules.dangerous_keywords:
            if keyword in entity_upper:
                errors.append(f"Dangerous keyword '{keyword}' detected in entity name")
        
        # Check for system tables
        if canonical_ir.entity.lower() in [t.lower() for t in self.safety_rules.system_tables]:
            errors.append(f"Access to system table '{canonical_ir.entity}' is not allowed")
        
        # Check filter operators
        for filter_obj in canonical_ir.filters:
            if filter_obj.operator in self.safety_rules.dangerous_operators:
                errors.append(f"Dangerous operator '{filter_obj.operator}' is not allowed")
        
        # Check maximum number of filters
        if len(canonical_ir.filters) > self.safety_rules.max_filters:
            errors.append(f"Too many filters ({len(canonical_ir.filters)}). Maximum allowed: {self.safety_rules.max_filters}")
        
        return errors
    
    def _validate_v0_scope(self, canonical_ir: CanonicalIR) -> List[str]:
        """Validate against V0 scope limitations"""
        errors = []
        
        # Check supported intents
        if canonical_ir.intent not in self.safety_rules.supported_intents:
            errors.append(f"Intent '{canonical_ir.intent}' is not supported in V0. Supported intents: {self.safety_rules.supported_intents}")
        
        # Check supported aggregations
        if canonical_ir.aggregation and canonical_ir.aggregation not in self.safety_rules.supported_aggregations:
            errors.append(f"Aggregation '{canonical_ir.aggregation}' is not supported in V0. Supported aggregations: {self.safety_rules.supported_aggregations}")
        
        # V0 only supports single-table queries (no joins)
        # This is implicitly enforced by having only one entity
        
        # V0 doesn't support subqueries (implicitly enforced by structure)
        
        # V0 doesn't support multiple metrics (implicitly enforced by having single metric field)
        
        return errors
    
    def _validate_business_logic(self, canonical_ir: CanonicalIR, schema: Dict[str, TableSchema]) -> List[str]:
        """Validate business logic constraints"""
        errors = []
        
        # Validate filter values are reasonable
        for filter_obj in canonical_ir.filters:
            if filter_obj.value is None or filter_obj.value == '':
                errors.append(f"Filter value for column '{filter_obj.column}' cannot be empty")
        
        # Validate time range logic
        if canonical_ir.time_range:
            if canonical_ir.time_range.start and canonical_ir.time_range.end:
                if canonical_ir.time_range.start >= canonical_ir.time_range.end:
                    errors.append("Time range start must be before end")
        
        return errors
    
    def _check_performance_warnings(self, canonical_ir: CanonicalIR) -> List[str]:
        """Check for potential performance issues"""
        warnings = []
        
        # Warn about many filters
        if len(canonical_ir.filters) > 5:
            warnings.append(f"Query has {len(canonical_ir.filters)} filters, which may impact performance")
        
        # Warn about LIKE operators
        for filter_obj in canonical_ir.filters:
            if filter_obj.operator == 'LIKE':
                warnings.append(f"LIKE operator on column '{filter_obj.column}' may be slow on large tables")
        
        return warnings
    
    def check_safety_rules(self, canonical_ir: CanonicalIR) -> List[str]:
        """Public method to check only safety rules"""
        return self._validate_safety_rules(canonical_ir)

# Error classes
class ValidationError(Exception):
    """Base exception for validation errors"""
    pass

class SchemaValidationError(ValidationError):
    """Schema validation error"""
    pass

class SafetyValidationError(ValidationError):
    """Safety validation error"""
    pass

class ScopeValidationError(ValidationError):
    """V0 scope validation error"""
    pass