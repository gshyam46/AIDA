"""
SQL Pipeline - Orchestrates the complete NL→SQL transformation workflow
"""
import logging
import time
from datetime import datetime
from typing import Dict, Optional

from core.database import DatabaseConnector
from core.semantic_parser import SemanticParser, LLMConfig
from core.ir_normalizer import IRNormalizer, BusinessRules
from core.ir_validator import IRValidator
from core.sql_compiler import SQLCompiler
from core.query_executor import QueryExecutor
from api.models import PipelineResult, PipelineStep

logger = logging.getLogger(__name__)

class SQLPipeline:
    """Complete NL→SQL transformation pipeline"""
    
    def __init__(self, business_rules_path: str = None):
        # Initialize components
        self.db_connector = DatabaseConnector()
        
        # Configure semantic parser to use Groq by default
        llm_config = LLMConfig()
        llm_config.configure_provider("groq", "llama-3.3-70b-versatile")
        self.semantic_parser = SemanticParser(llm_config)
        
        # Load business rules
        business_rules = BusinessRules()
        if business_rules_path:
            business_rules.load_from_file(business_rules_path)
        
        self.ir_normalizer = IRNormalizer(business_rules)
        self.ir_validator = IRValidator()
        self.sql_compiler = SQLCompiler()
        self.query_executor = QueryExecutor()
        
        logger.info("SQL Pipeline initialized")
    
    async def execute(self, question: str, database_path: str, request_id: str = None) -> PipelineResult:
        """Execute complete pipeline from question to results with step-by-step tracking"""
        start_time = time.time()
        request_id = request_id or "unknown"
        pipeline_steps = []
        
        try:
            logger.info(f"[{request_id}] Starting pipeline execution")
            
            # Step 1: Connect to database and get schema
            step_start = time.time()
            logger.info(f"[{request_id}] Step 1: Database connection and schema introspection")
            
            if not self.db_connector.connect(database_path):
                return PipelineResult(
                    success=False,
                    error=f"Failed to connect to database: {database_path}",
                    error_type="database_error",
                    pipeline_steps=[PipelineStep(
                        step_number=1,
                        step_name="Database Connection",
                        description="Connect to database and introspect schema",
                        status="failed",
                        error=f"Failed to connect to database: {database_path}"
                    )]
                )
            
            schema = self.db_connector.introspect_schema()
            step_time = int((time.time() - step_start) * 1000)
            
            # Create schema context for display
            schema_context = self._create_schema_context(schema)
            
            pipeline_steps.append(PipelineStep(
                step_number=1,
                step_name="Database Connection & Schema Introspection",
                description="Connect to database and extract table/column information",
                input_data={"database_path": database_path},
                output_data={
                    "tables_found": len(schema),
                    "schema_summary": {table: list(info['columns'].keys()) for table, info in schema_context.items()}
                },
                execution_time_ms=step_time,
                status="completed"
            ))
            
            logger.info(f"[{request_id}] Schema loaded: {len(schema)} tables")
            
            # Step 2: Parse natural language to semantic IR
            step_start = time.time()
            logger.info(f"[{request_id}] Step 2: Semantic parsing with LLM")
            
            try:
                semantic_ir = await self.semantic_parser.parse(question, schema_context)
                step_time = int((time.time() - step_start) * 1000)
                
                pipeline_steps.append(PipelineStep(
                    step_number=2,
                    step_name="Semantic Parsing (LLM)",
                    description="Extract semantic intent from natural language using LLM",
                    input_data={
                        "question": question,
                        "available_tables": list(schema_context.keys())
                    },
                    output_data=self._semantic_ir_to_dict(semantic_ir),
                    execution_time_ms=step_time,
                    status="completed"
                ))
                
                logger.info(f"[{request_id}] Semantic IR generated: intent={semantic_ir.intent}, entity={semantic_ir.entity_hint}")
            except Exception as e:
                step_time = int((time.time() - step_start) * 1000)
                pipeline_steps.append(PipelineStep(
                    step_number=2,
                    step_name="Semantic Parsing (LLM)",
                    description="Extract semantic intent from natural language using LLM",
                    input_data={"question": question},
                    execution_time_ms=step_time,
                    status="failed",
                    error=str(e)
                ))
                raise
            
            # Step 3: Normalize to canonical IR
            step_start = time.time()
            logger.info(f"[{request_id}] Step 3: IR normalization")
            
            try:
                canonical_ir = self.ir_normalizer.normalize(semantic_ir, schema)
                step_time = int((time.time() - step_start) * 1000)
                
                pipeline_steps.append(PipelineStep(
                    step_number=3,
                    step_name="Semantic Normalization",
                    description="Convert semantic hints to canonical business logic using schema analysis",
                    input_data=self._semantic_ir_to_dict(semantic_ir),
                    output_data=self._canonical_ir_to_dict(canonical_ir),
                    execution_time_ms=step_time,
                    status="completed"
                ))
                
                logger.info(f"[{request_id}] Canonical IR created: entity={canonical_ir.entity}, metric={canonical_ir.metric}")
            except Exception as e:
                step_time = int((time.time() - step_start) * 1000)
                pipeline_steps.append(PipelineStep(
                    step_number=3,
                    step_name="Semantic Normalization",
                    description="Convert semantic hints to canonical business logic",
                    execution_time_ms=step_time,
                    status="failed",
                    error=str(e)
                ))
                raise
            
            # Step 4: Validate canonical IR
            step_start = time.time()
            logger.info(f"[{request_id}] Step 4: IR validation")
            
            validation_result = self.ir_validator.validate(canonical_ir, schema)
            step_time = int((time.time() - step_start) * 1000)
            
            if not validation_result.valid:
                error_msg = f"Validation failed: {'; '.join(validation_result.errors)}"
                logger.error(f"[{request_id}] {error_msg}")
                
                pipeline_steps.append(PipelineStep(
                    step_number=4,
                    step_name="IR Validation",
                    description="Validate canonical IR against database schema",
                    input_data=self._canonical_ir_to_dict(canonical_ir),
                    execution_time_ms=step_time,
                    status="failed",
                    error=error_msg
                ))
                
                return PipelineResult(
                    success=False,
                    pipeline_steps=pipeline_steps,
                    semantic_ir=self._semantic_ir_to_dict(semantic_ir),
                    canonical_ir=self._canonical_ir_to_dict(canonical_ir),
                    error=error_msg,
                    error_type="validation_error"
                )
            
            pipeline_steps.append(PipelineStep(
                step_number=4,
                step_name="IR Validation",
                description="Validate canonical IR against database schema and business rules",
                input_data=self._canonical_ir_to_dict(canonical_ir),
                output_data={
                    "validation_passed": True,
                    "warnings": validation_result.warnings if validation_result.warnings else []
                },
                execution_time_ms=step_time,
                status="completed"
            ))
            
            # Log warnings if any
            if validation_result.warnings:
                logger.warning(f"[{request_id}] Validation warnings: {'; '.join(validation_result.warnings)}")
            
            # Step 5: Compile to SQL
            step_start = time.time()
            logger.info(f"[{request_id}] Step 5: SQL compilation")
            
            try:
                compiled_query = self.sql_compiler.compile(canonical_ir)
                step_time = int((time.time() - step_start) * 1000)
                
                pipeline_steps.append(PipelineStep(
                    step_number=5,
                    step_name="SQL Compilation",
                    description="Generate safe, parameterized SQL from canonical IR",
                    input_data=self._canonical_ir_to_dict(canonical_ir),
                    output_data={
                        "sql": compiled_query.sql,
                        "parameters": compiled_query.parameters
                    },
                    execution_time_ms=step_time,
                    status="completed"
                ))
                
                logger.info(f"[{request_id}] SQL compiled: {compiled_query.sql}")
            except Exception as e:
                step_time = int((time.time() - step_start) * 1000)
                pipeline_steps.append(PipelineStep(
                    step_number=5,
                    step_name="SQL Compilation",
                    description="Generate safe, parameterized SQL from canonical IR",
                    execution_time_ms=step_time,
                    status="failed",
                    error=str(e)
                ))
                raise
            
            # Step 6: Execute query
            step_start = time.time()
            logger.info(f"[{request_id}] Step 6: Query execution")
            
            execution_result = self.query_executor.execute(compiled_query, self.db_connector)
            step_time = int((time.time() - step_start) * 1000)
            
            if not execution_result.success:
                logger.error(f"[{request_id}] Query execution failed: {execution_result.error}")
                
                pipeline_steps.append(PipelineStep(
                    step_number=6,
                    step_name="Query Execution",
                    description="Execute SQL query against database",
                    input_data={
                        "sql": compiled_query.sql,
                        "parameters": compiled_query.parameters
                    },
                    execution_time_ms=step_time,
                    status="failed",
                    error=execution_result.error
                ))
                
                return PipelineResult(
                    success=False,
                    pipeline_steps=pipeline_steps,
                    semantic_ir=self._semantic_ir_to_dict(semantic_ir),
                    canonical_ir=self._canonical_ir_to_dict(canonical_ir),
                    sql=compiled_query.sql,
                    parameters=compiled_query.parameters,
                    error=execution_result.error,
                    error_type="execution_error"
                )
            
            pipeline_steps.append(PipelineStep(
                step_number=6,
                step_name="Query Execution",
                description="Execute SQL query against database with safety checks",
                input_data={
                    "sql": compiled_query.sql,
                    "parameters": compiled_query.parameters
                },
                output_data={
                    "rows_returned": len(execution_result.data) if execution_result.data else 0,
                    "result_preview": execution_result.data[:3] if execution_result.data else []
                },
                execution_time_ms=step_time,
                status="completed"
            ))
            
            # Success - return complete result
            total_time = int((time.time() - start_time) * 1000)
            logger.info(f"[{request_id}] Pipeline completed successfully in {total_time}ms")
            
            return PipelineResult(
                success=True,
                pipeline_steps=pipeline_steps,
                semantic_ir=self._semantic_ir_to_dict(semantic_ir),
                canonical_ir=self._canonical_ir_to_dict(canonical_ir),
                sql=compiled_query.sql,
                parameters=compiled_query.parameters,
                results=execution_result.data,
                execution_time_ms=total_time
            )
        
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            logger.error(f"[{request_id}] Pipeline execution failed: {e}")
            
            return PipelineResult(
                success=False,
                pipeline_steps=pipeline_steps,
                execution_time_ms=total_time,
                error=f"Pipeline error: {str(e)}",
                error_type="pipeline_error"
            )
    
    def _create_schema_context(self, schema: Dict) -> Dict:
        """Create schema context for LLM"""
        context = {}
        for table_name, table_schema in schema.items():
            context[table_name] = {
                'columns': table_schema.columns,
                'row_count': table_schema.row_count
            }
        return context
    
    def _semantic_ir_to_dict(self, semantic_ir) -> Dict:
        """Convert SemanticIR to dictionary for JSON serialization"""
        return {
            'intent': semantic_ir.intent,
            'entity_hint': semantic_ir.entity_hint,
            'metric_hint': semantic_ir.metric_hint,
            'aggregation_hint': semantic_ir.aggregation_hint,
            'filter_hints': [
                {
                    'column_hint': hint.column_hint,
                    'operator': hint.operator,
                    'value_hint': hint.value_hint
                }
                for hint in semantic_ir.filter_hints
            ],
            'time_expression': semantic_ir.time_expression
        }
    
    def _canonical_ir_to_dict(self, canonical_ir) -> Dict:
        """Convert CanonicalIR to dictionary for JSON serialization"""
        return {
            'intent': canonical_ir.intent,
            'entity': canonical_ir.entity,
            'metric': canonical_ir.metric,
            'aggregation': canonical_ir.aggregation,
            'filters': [
                {
                    'column': f.column,
                    'operator': f.operator,
                    'value': f.value,
                    'parameter_name': f.parameter_name
                }
                for f in canonical_ir.filters
            ],
            'time_range': {
                'column': canonical_ir.time_range.column,
                'start': canonical_ir.time_range.start.isoformat() if canonical_ir.time_range.start else None,
                'end': canonical_ir.time_range.end.isoformat() if canonical_ir.time_range.end else None
            } if canonical_ir.time_range else None
        }
    
    def configure_llm(self, provider: str, api_key: str = None, model: str = None) -> None:
        """Configure LLM provider"""
        self.semantic_parser.configure_provider(provider, api_key, model)
        logger.info(f"LLM provider configured: {provider}")
    
    def close(self) -> None:
        """Clean up resources"""
        self.db_connector.close()
        logger.info("Pipeline resources closed")