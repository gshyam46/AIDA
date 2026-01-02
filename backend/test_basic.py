"""
Basic functionality test for SQL MVP
"""
import os
import sys
import tempfile
import sqlite3
from pathlib import Path

# Add the backend directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from core.database import DatabaseConnector
from core.semantic_parser import SemanticIR, FilterHint
from core.ir_normalizer import IRNormalizer, BusinessRules
from core.ir_validator import IRValidator
from core.sql_compiler import SQLCompiler
from core.query_executor import QueryExecutor

def create_test_database():
    """Create a simple test database"""
    db_path = tempfile.mktemp(suffix='.sqlite')
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create test table
    cursor.execute('''
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    ''')
    
    # Insert test data
    cursor.execute('''
        INSERT INTO orders (amount, status, created_at) VALUES
        (100.0, 'completed', '2025-01-01 10:00:00'),
        (200.0, 'completed', '2025-01-02 11:00:00'),
        (150.0, 'pending', '2025-01-03 09:00:00')
    ''')
    
    conn.commit()
    conn.close()
    
    return db_path

def test_database_connector():
    """Test database connection and schema introspection"""
    print("Testing DatabaseConnector...")
    
    db_path = create_test_database()
    db = DatabaseConnector()
    
    # Test connection
    assert db.connect(db_path), "Failed to connect to database"
    
    # Test schema introspection
    schema = db.introspect_schema()
    assert 'orders' in schema, "Orders table not found in schema"
    assert 'id' in schema['orders'].columns, "ID column not found"
    assert 'amount' in schema['orders'].columns, "Amount column not found"
    
    db.close()
    os.unlink(db_path)
    print("✓ DatabaseConnector test passed")

def test_ir_normalizer():
    """Test IR normalization"""
    print("Testing IRNormalizer...")
    
    # Create test semantic IR
    semantic_ir = SemanticIR(
        intent='aggregate',
        entity_hint='orders',
        metric_hint='amount',
        aggregation_hint='sum',
        filter_hints=[],
        time_expression=None
    )
    
    # Create test schema
    db_path = create_test_database()
    db = DatabaseConnector()
    db.connect(db_path)
    schema = db.introspect_schema()
    
    # Test normalization
    business_rules = BusinessRules()
    normalizer = IRNormalizer(business_rules)
    canonical_ir = normalizer.normalize(semantic_ir, schema)
    
    assert canonical_ir.intent == 'aggregate', "Intent not preserved"
    assert canonical_ir.entity == 'orders', "Entity not resolved correctly"
    assert canonical_ir.metric == 'amount', "Metric not resolved correctly"
    assert canonical_ir.aggregation == 'sum', "Aggregation not preserved"
    
    db.close()
    os.unlink(db_path)
    print("✓ IRNormalizer test passed")

def test_sql_compiler():
    """Test SQL compilation"""
    print("Testing SQLCompiler...")
    
    # Create test canonical IR
    from core.ir_normalizer import CanonicalIR
    
    canonical_ir = CanonicalIR(
        intent='aggregate',
        entity='orders',
        metric='amount',
        aggregation='sum',
        filters=[],
        time_range=None
    )
    
    # Test compilation
    compiler = SQLCompiler()
    compiled_query = compiler.compile(canonical_ir)
    
    assert compiled_query.sql == 'SELECT SUM(amount) AS result FROM orders', f"Unexpected SQL: {compiled_query.sql}"
    assert compiled_query.query_type == 'aggregate', "Query type not set correctly"
    
    print("✓ SQLCompiler test passed")

def test_end_to_end():
    """Test end-to-end pipeline"""
    print("Testing end-to-end pipeline...")
    
    # Create test database
    db_path = create_test_database()
    
    try:
        # Initialize components
        db = DatabaseConnector()
        business_rules = BusinessRules()
        normalizer = IRNormalizer(business_rules)
        validator = IRValidator()
        compiler = SQLCompiler()
        executor = QueryExecutor()
        
        # Connect to database
        assert db.connect(db_path), "Failed to connect to database"
        schema = db.introspect_schema()
        
        # Create semantic IR (simulating LLM output)
        semantic_ir = SemanticIR(
            intent='aggregate',
            entity_hint='orders',
            metric_hint='amount',
            aggregation_hint='sum',
            filter_hints=[],
            time_expression=None
        )
        
        # Normalize
        canonical_ir = normalizer.normalize(semantic_ir, schema)
        
        # Validate
        validation_result = validator.validate(canonical_ir, schema)
        assert validation_result.valid, f"Validation failed: {validation_result.errors}"
        
        # Compile
        compiled_query = compiler.compile(canonical_ir)
        
        # Execute
        execution_result = executor.execute(compiled_query, db)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        assert len(execution_result.data) == 1, "Expected one result row"
        assert execution_result.data[0]['result'] == 300.0, f"Expected 300.0, got {execution_result.data[0]['result']}"
        
        db.close()
        print("✓ End-to-end test passed")
        
    finally:
        os.unlink(db_path)

def main():
    """Run all tests"""
    print("Running SQL MVP basic functionality tests...\n")
    
    try:
        test_database_connector()
        test_ir_normalizer()
        test_sql_compiler()
        test_end_to_end()
        
        print("\n🎉 All tests passed! The SQL MVP system is working correctly.")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)