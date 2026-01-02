"""
Validate the SQL MVP project structure and basic imports
"""
import os
from pathlib import Path

def check_file_exists(path, description):
    """Check if a file exists and print status"""
    if Path(path).exists():
        print(f"✓ {description}: {path}")
        return True
    else:
        print(f"❌ {description}: {path} (MISSING)")
        return False

def check_directory_structure():
    """Check that all required directories and files exist"""
    print("Checking project structure...\n")
    
    all_good = True
    
    # Backend structure
    print("Backend Structure:")
    all_good &= check_file_exists("backend/main.py", "Main FastAPI application")
    all_good &= check_file_exists("backend/requirements.txt", "Python dependencies")
    all_good &= check_file_exists("backend/core/database.py", "Database connector")
    all_good &= check_file_exists("backend/core/semantic_parser.py", "Semantic parser")
    all_good &= check_file_exists("backend/core/ir_normalizer.py", "IR normalizer")
    all_good &= check_file_exists("backend/core/ir_validator.py", "IR validator")
    all_good &= check_file_exists("backend/core/sql_compiler.py", "SQL compiler")
    all_good &= check_file_exists("backend/core/query_executor.py", "Query executor")
    all_good &= check_file_exists("backend/core/pipeline.py", "Pipeline orchestrator")
    all_good &= check_file_exists("backend/api/endpoints.py", "API endpoints")
    all_good &= check_file_exists("backend/api/models.py", "API models")
    all_good &= check_file_exists("backend/config/business_rules.yaml", "Business rules config")
    
    print()
    
    # Frontend structure
    print("Frontend Structure:")
    all_good &= check_file_exists("frontend/package.json", "Node.js dependencies")
    all_good &= check_file_exists("frontend/app/page.tsx", "Main page component")
    all_good &= check_file_exists("frontend/components/QueryInterface.tsx", "Query interface")
    all_good &= check_file_exists("frontend/components/ResultsDisplay.tsx", "Results display")
    all_good &= check_file_exists("frontend/components/SchemaDisplay.tsx", "Schema display")
    all_good &= check_file_exists("frontend/components/FileUpload.tsx", "File upload")
    all_good &= check_file_exists("frontend/lib/api.ts", "API client")
    
    print()
    
    # Configuration and deployment
    print("Configuration & Deployment:")
    all_good &= check_file_exists("docker-compose.yml", "Docker Compose configuration")
    all_good &= check_file_exists("backend/Dockerfile", "Backend Docker configuration")
    all_good &= check_file_exists("frontend/Dockerfile", "Frontend Docker configuration")
    all_good &= check_file_exists("README.md", "Documentation")
    
    print()
    
    # Database
    print("Database:")
    all_good &= check_file_exists("database/olist.sqlite", "Sample database")
    
    return all_good

def check_basic_syntax():
    """Check basic Python syntax by attempting imports"""
    print("\nChecking Python syntax...")
    
    try:
        # Test basic imports without external dependencies
        import sys
        sys.path.insert(0, 'backend')
        
        # Check if files can be parsed (syntax check)
        import ast
        
        python_files = [
            "backend/main.py",
            "backend/core/database.py",
            "backend/core/ir_normalizer.py",
            "backend/core/ir_validator.py",
            "backend/core/sql_compiler.py",
            "backend/core/query_executor.py",
            "backend/api/models.py",
            "backend/api/endpoints.py"
        ]
        
        for file_path in python_files:
            if Path(file_path).exists():
                try:
                    with open(file_path, 'r') as f:
                        ast.parse(f.read())
                    print(f"✓ Syntax OK: {file_path}")
                except SyntaxError as e:
                    print(f"❌ Syntax Error in {file_path}: {e}")
                    return False
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking syntax: {e}")
        return False

def check_configuration():
    """Check configuration files"""
    print("\nChecking configuration files...")
    
    try:
        import yaml
        
        # Check business rules YAML
        if Path("backend/config/business_rules.yaml").exists():
            with open("backend/config/business_rules.yaml", 'r') as f:
                config = yaml.safe_load(f)
                if 'metric_mappings' in config and 'entity_mappings' in config:
                    print("✓ Business rules configuration is valid")
                else:
                    print("❌ Business rules configuration is missing required sections")
                    return False
        
        return True
        
    except ImportError:
        print("⚠️  PyYAML not installed, skipping YAML validation")
        return True
    except Exception as e:
        print(f"❌ Error checking configuration: {e}")
        return False

def main():
    """Run all validation checks"""
    print("SQL MVP Project Validation")
    print("=" * 50)
    
    structure_ok = check_directory_structure()
    syntax_ok = check_basic_syntax()
    config_ok = check_configuration()
    
    print("\n" + "=" * 50)
    
    if structure_ok and syntax_ok and config_ok:
        print("🎉 Project validation PASSED!")
        print("\nNext steps:")
        print("1. Install backend dependencies: cd backend && pip install -r requirements.txt")
        print("2. Install frontend dependencies: cd frontend && npm install")
        print("3. Set up environment variables (see .env.example files)")
        print("4. Start the backend: cd backend && python main.py")
        print("5. Start the frontend: cd frontend && npm run dev")
        print("6. Open http://localhost:3000 in your browser")
        return True
    else:
        print("❌ Project validation FAILED!")
        print("Please check the missing files and fix any syntax errors.")
        return False

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)