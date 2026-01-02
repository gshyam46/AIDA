"""
Schema-Aware IR Normalizer - Reduces reliance on hardcoded business rules
"""
import yaml
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .semantic_parser import SemanticIR
from .database import TableSchema

logger = logging.getLogger(__name__)

@dataclass
class Filter:
    """Canonical filter representation"""
    column: str
    operator: str  # '=', '>', '<', '>=', '<=', 'LIKE'
    value: Any
    parameter_name: str

@dataclass
class TimeRange:
    """Time range filter representation"""
    column: str
    start: Optional[datetime]
    end: Optional[datetime]

@dataclass
class CanonicalIR:
    """Canonical Intermediate Representation with validated business logic"""
    intent: str
    entity: str  # Resolved table name
    metric: Optional[str]  # Resolved column name
    aggregation: Optional[str]
    filters: List[Filter]
    time_range: Optional[TimeRange]

class BusinessRules:
    """Minimal business rules - schema-aware approach"""
    
    def __init__(self, config_path: str = None):
        # Keep only essential mappings as fallbacks
        self.metric_mappings: Dict[str, List[str]] = {
            'revenue': ['amount', 'total', 'price', 'value', 'cost'],
            'sales': ['amount', 'total', 'price', 'value'],
            'total': ['amount', 'sum', 'total', 'value'],
            'price': ['amount', 'price', 'cost', 'value'],
            'count': ['id', 'count'],
        }
        
        self.entity_mappings: Dict[str, str] = {
            'order': 'orders',
            'sale': 'orders', 
            'customer': 'customers',
            'user': 'customers',
            'product': 'products',
            'item': 'products'
        }
        
        # Only essential business filters
        self.default_filters: Dict[str, List[Dict]] = {
            'orders': [{'column': 'status', 'operator': '=', 'value': 'completed'}]
        }
        
        self.aggregation_functions: List[str] = ['sum', 'count', 'avg', 'min', 'max']
        
        if config_path:
            self.load_from_file(config_path)
    
    def load_from_file(self, config_path: str) -> None:
        """Load business rules from YAML configuration file (optional)"""
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                logger.warning(f"Business rules config file not found: {config_path}")
                return
            
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            # Update with file contents but keep schema-aware approach
            if 'metric_mappings' in config:
                self.metric_mappings.update(config['metric_mappings'])
            if 'entity_mappings' in config:
                self.entity_mappings.update(config['entity_mappings'])
            if 'default_filters' in config:
                self.default_filters.update(config['default_filters'])
            if 'aggregation_functions' in config:
                self.aggregation_functions = config['aggregation_functions']
            
            logger.info(f"Loaded business rules from {config_path}")
            
        except Exception as e:
            logger.warning(f"Could not load business rules from {config_path}: {e}")

class IRNormalizer:
    """Schema-aware IR normalizer that intelligently maps based on actual database schema"""
    
    def __init__(self, business_rules: BusinessRules = None):
        self.business_rules = business_rules or BusinessRules()
        self._parameter_counter = 0
        logger.info("Schema-aware IR Normalizer initialized")
    
    def normalize(self, semantic_ir: SemanticIR, schema: Dict[str, TableSchema]) -> CanonicalIR:
        """Convert semantic IR to canonical IR using schema analysis"""
        try:
            # Reset parameter counter for each normalization
            self._parameter_counter = 0
            
            # 1. Resolve entity using schema analysis
            entity = self._resolve_entity_smart(semantic_ir.entity_hint, schema)
            
            # 2. Resolve metric using schema analysis
            metric = self._resolve_metric_smart(semantic_ir.metric_hint, entity, schema)
            
            # 3. Resolve aggregation
            aggregation = self._resolve_aggregation(semantic_ir.aggregation_hint)
            
            # 4. Resolve filters using schema analysis
            filters = self._resolve_filters_smart(semantic_ir.filter_hints, entity, schema)
            
            # 5. Add minimal business filters
            default_filters = self._apply_default_filters(entity, schema)
            filters.extend(default_filters)
            
            # 6. Resolve time range using schema analysis
            time_range = self._resolve_time_range_smart(semantic_ir.time_expression, entity, schema)
            
            # 7. Add time range as filters if present
            if time_range:
                time_filters = self._time_range_to_filters(time_range)
                filters.extend(time_filters)
            
            canonical_ir = CanonicalIR(
                intent=semantic_ir.intent,
                entity=entity,
                metric=metric,
                aggregation=aggregation,
                filters=filters,
                time_range=time_range
            )
            
            logger.info(f"Schema-aware normalization completed for entity: {entity}")
            return canonical_ir
            
        except Exception as e:
            logger.error(f"Schema-aware IR normalization failed: {e}")
            raise
    
    def _resolve_entity_smart(self, entity_hint: str, schema: Dict[str, TableSchema]) -> str:
        """Intelligently resolve entity using schema analysis"""
        if not entity_hint:
            # If no hint, return the first table (for single-table queries)
            if len(schema) == 1:
                return list(schema.keys())[0]
            raise ValueError("Entity hint is required for multi-table databases")
        
        entity_hint_lower = entity_hint.lower()
        
        # 1. Exact match (case-insensitive)
        for table_name in schema.keys():
            if table_name.lower() == entity_hint_lower:
                return table_name
        
        # 2. Check business rules mapping (validated against schema)
        if entity_hint_lower in self.business_rules.entity_mappings:
            mapped_entity = self.business_rules.entity_mappings[entity_hint_lower]
            for table_name in schema.keys():
                if table_name.lower() == mapped_entity.lower():
                    return table_name
        
        # 3. Fuzzy matching against actual schema tables
        for table_name in schema.keys():
            table_lower = table_name.lower()
            
            # Partial match (hint contains table or vice versa)
            if entity_hint_lower in table_lower or table_lower in entity_hint_lower:
                return table_name
                
            # Singular/plural matching
            if (entity_hint_lower + 's' == table_lower or 
                entity_hint_lower == table_lower + 's' or
                entity_hint_lower.rstrip('s') == table_lower.rstrip('s')):
                return table_name
        
        # 4. Fallback to single table if only one exists
        if len(schema) == 1:
            logger.warning(f"Entity hint '{entity_hint}' not found, using only available table: {list(schema.keys())[0]}")
            return list(schema.keys())[0]
        
        raise ValueError(f"Unknown entity '{entity_hint}'. Available tables: {list(schema.keys())}")
    
    def _resolve_metric_smart(self, metric_hint: Optional[str], entity: str, schema: Dict[str, TableSchema]) -> Optional[str]:
        """Intelligently resolve metric using schema analysis"""
        if not metric_hint:
            return None
        
        metric_hint_lower = metric_hint.lower()
        table_schema = schema[entity]
        
        # 1. Exact match (case-insensitive)
        for column_name in table_schema.columns.keys():
            if column_name.lower() == metric_hint_lower:
                return column_name
        
        # 2. Check business rules with schema validation
        if metric_hint_lower in self.business_rules.metric_mappings:
            candidates = self.business_rules.metric_mappings[metric_hint_lower]
            for candidate in candidates:
                for column_name in table_schema.columns.keys():
                    if column_name.lower() == candidate.lower():
                        return column_name
        
        # 3. Fuzzy matching against actual columns
        for column_name in table_schema.columns.keys():
            column_lower = column_name.lower()
            
            # Partial match
            if metric_hint_lower in column_lower or column_lower in metric_hint_lower:
                return column_name
        
        # 4. Semantic matching based on column types
        if metric_hint_lower in ['revenue', 'sales', 'total', 'amount', 'price', 'cost', 'value']:
            # Look for numeric columns that might represent money/amounts
            for column_name, column_info in table_schema.columns.items():
                if column_info.type.upper() in ['INTEGER', 'DECIMAL', 'FLOAT', 'NUMERIC', 'REAL']:
                    column_lower = column_name.lower()
                    if any(term in column_lower for term in ['amount', 'price', 'cost', 'value', 'total']):
                        return column_name
            
            # Fallback to first numeric column
            for column_name, column_info in table_schema.columns.items():
                if column_info.type.upper() in ['INTEGER', 'DECIMAL', 'FLOAT', 'NUMERIC', 'REAL']:
                    return column_name
        
        elif metric_hint_lower in ['count', 'number', 'id']:
            # Look for ID columns
            for column_name in table_schema.columns.keys():
                if column_name.lower() in ['id', 'count'] or 'id' in column_name.lower():
                    return column_name
        
        raise ValueError(f"Unknown metric '{metric_hint}' for table '{entity}'. Available columns: {list(table_schema.columns.keys())}")
    
    def _resolve_filters_smart(self, filter_hints: List, entity: str, schema: Dict[str, TableSchema]) -> List[Filter]:
        """Intelligently resolve filters using schema analysis"""
        filters = []
        table_schema = schema[entity]
        
        for hint in filter_hints:
            # Resolve column name using smart matching
            column = self._resolve_filter_column_smart(hint.column_hint, table_schema)
            
            # Validate operator
            operator = self._validate_operator(hint.operator)
            
            # Create parameter name
            param_name = f"param{self._parameter_counter}"
            self._parameter_counter += 1
            
            filters.append(Filter(
                column=column,
                operator=operator,
                value=hint.value_hint,
                parameter_name=param_name
            ))
        
        return filters
    
    def _resolve_filter_column_smart(self, column_hint: str, table_schema: TableSchema) -> str:
        """Smart column resolution for filters"""
        hint_lower = column_hint.lower()
        
        # 1. Exact match (case-insensitive)
        for column_name in table_schema.columns.keys():
            if column_name.lower() == hint_lower:
                return column_name
        
        # 2. Fuzzy matching
        for column_name in table_schema.columns.keys():
            column_lower = column_name.lower()
            if hint_lower in column_lower or column_lower in hint_lower:
                return column_name
        
        # 3. Semantic matching for common filter terms
        if hint_lower in ['status', 'state']:
            for column_name in table_schema.columns.keys():
                if 'status' in column_name.lower() or 'state' in column_name.lower():
                    return column_name
        
        raise ValueError(f"Unknown filter column '{column_hint}'. Available columns: {list(table_schema.columns.keys())}")
    
    def _resolve_time_range_smart(self, time_expression: Optional[str], entity: str, schema: Dict[str, TableSchema]) -> Optional[TimeRange]:
        """Smart time range resolution using schema analysis"""
        if not time_expression:
            return None
        
        # Find time column using schema analysis
        time_column = self._find_time_column_smart(entity, schema)
        if not time_column:
            logger.warning(f"No time column found for entity '{entity}', skipping time filter")
            return None
        
        # Use fixed current time for deterministic behavior
        now = datetime(2025, 1, 3, 12, 0, 0)  # Fixed for demo consistency
        
        if time_expression.lower() == 'this month':
            start = datetime(now.year, now.month, 1)
            return TimeRange(column=time_column, start=start, end=None)
        
        elif time_expression.lower() == 'last month':
            if now.month == 1:
                start = datetime(now.year - 1, 12, 1)
            else:
                start = datetime(now.year, now.month - 1, 1)
            
            # End is start of current month
            end = datetime(now.year, now.month, 1)
            return TimeRange(column=time_column, start=start, end=end)
        
        elif time_expression.lower() == 'last 7 days':
            start = now - timedelta(days=7)
            return TimeRange(column=time_column, start=start, end=None)
        
        else:
            logger.warning(f"Unknown time expression: {time_expression}")
            return None
    
    def _find_time_column_smart(self, entity: str, schema: Dict[str, TableSchema]) -> Optional[str]:
        """Smart time column detection using schema analysis"""
        if entity not in schema:
            return None
        
        table_schema = schema[entity]
        
        # 1. Look for common time column patterns
        time_patterns = ['created_at', 'updated_at', 'date', 'timestamp', 'time', 'created', 'updated']
        
        for pattern in time_patterns:
            for column_name in table_schema.columns.keys():
                if pattern in column_name.lower():
                    return column_name
        
        # 2. Look for columns with time-related types
        for column_name, column_info in table_schema.columns.items():
            if column_info.type.upper() in ['TIMESTAMP', 'DATETIME', 'DATE']:
                return column_name
        
        # 3. Look for columns with time-related names (broader search)
        for column_name in table_schema.columns.keys():
            column_lower = column_name.lower()
            if any(term in column_lower for term in ['date', 'time', 'created', 'updated', 'when']):
                return column_name
        
        return None
    
    def _apply_default_filters(self, entity: str, schema: Dict[str, TableSchema]) -> List[Filter]:
        """Apply minimal business filters with schema validation"""
        filters = []
        
        if entity in self.business_rules.default_filters:
            table_schema = schema[entity]
            
            for filter_config in self.business_rules.default_filters[entity]:
                # Validate that the column exists in the schema
                column = filter_config['column']
                if column in table_schema.columns:
                    param_name = f"param{self._parameter_counter}"
                    self._parameter_counter += 1
                    
                    filters.append(Filter(
                        column=column,
                        operator=filter_config['operator'],
                        value=filter_config['value'],
                        parameter_name=param_name
                    ))
                else:
                    logger.warning(f"Default filter column '{column}' not found in table '{entity}', skipping")
        
        return filters
    
    def _resolve_aggregation(self, aggregation_hint: Optional[str]) -> Optional[str]:
        """Resolve and validate aggregation function"""
        if not aggregation_hint:
            return None
        
        aggregation_lower = aggregation_hint.lower()
        
        # Check if aggregation is supported
        if aggregation_lower not in self.business_rules.aggregation_functions:
            raise ValueError(f"Unsupported aggregation '{aggregation_hint}'. Supported: {self.business_rules.aggregation_functions}")
        
        return aggregation_lower
    
    def _time_range_to_filters(self, time_range: TimeRange) -> List[Filter]:
        """Convert time range to filter objects"""
        filters = []
        
        if time_range.start:
            param_name = f"param{self._parameter_counter}"
            self._parameter_counter += 1
            
            filters.append(Filter(
                column=time_range.column,
                operator='>=',
                value=time_range.start.strftime('%Y-%m-%d %H:%M:%S'),
                parameter_name=param_name
            ))
        
        if time_range.end:
            param_name = f"param{self._parameter_counter}"
            self._parameter_counter += 1
            
            filters.append(Filter(
                column=time_range.column,
                operator='<',
                value=time_range.end.strftime('%Y-%m-%d %H:%M:%S'),
                parameter_name=param_name
            ))
        
        return filters
    
    def _validate_operator(self, operator: str) -> str:
        """Validate and normalize filter operator"""
        valid_operators = ['=', '>', '<', '>=', '<=', 'LIKE']
        
        if operator not in valid_operators:
            raise ValueError(f"Invalid operator '{operator}'. Valid operators: {valid_operators}")
        
        return operator
    
    def load_business_rules(self, config_path: str) -> None:
        """Load business rules from configuration file"""
        self.business_rules.load_from_file(config_path)

# Error classes
class NormalizationError(Exception):
    """Base exception for IR normalization errors"""
    pass

class UnknownEntityError(NormalizationError):
    """Unknown entity/table error"""
    pass

class UnknownMetricError(NormalizationError):
    """Unknown metric/column error"""
    pass

class UnsupportedAggregationError(NormalizationError):
    """Unsupported aggregation function error"""
    pass