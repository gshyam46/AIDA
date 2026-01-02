"""
Configuration Manager for business rules and LLM providers
"""
import os
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class ConfigManager:
    """Centralized configuration management with hot-reload capability"""
    
    def __init__(self, config_dir: str = "./config"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        
        # Configuration cache
        self._config_cache: Dict[str, Dict] = {}
        self._file_timestamps: Dict[str, float] = {}
        
        # Default configurations
        self._create_default_configs()
        
        logger.info(f"Configuration manager initialized with directory: {config_dir}")
    
    def _create_default_configs(self) -> None:
        """Create default configuration files if they don't exist"""
        
        # Default business rules
        business_rules_path = self.config_dir / "business_rules.yaml"
        if not business_rules_path.exists():
            default_business_rules = {
                "metric_mappings": {
                    "revenue": "amount",
                    "total": "amount",
                    "sales": "amount",
                    "orders": "id",
                    "customers": "id",
                    "users": "id",
                    "price": "amount",
                    "cost": "amount",
                    "value": "amount"
                },
                "entity_mappings": {
                    "order": "orders",
                    "sale": "orders",
                    "customer": "customers",
                    "user": "customers",
                    "product": "products",
                    "item": "products"
                },
                "default_filters": {
                    "orders": [
                        {"column": "status", "operator": "=", "value": "completed"}
                    ]
                },
                "time_columns": {
                    "orders": "created_at",
                    "customers": "created_at",
                    "products": "created_at"
                },
                "aggregation_functions": ["sum", "count", "avg", "min", "max"]
            }
            
            with open(business_rules_path, 'w') as f:
                yaml.dump(default_business_rules, f, default_flow_style=False)
            
            logger.info("Created default business rules configuration")
        
        # Default LLM configuration
        llm_config_path = self.config_dir / "llm_config.yaml"
        if not llm_config_path.exists():
            default_llm_config = {
                "providers": {
                    "openai": {
                        "models": ["gpt-3.5-turbo", "gpt-4"],
                        "default_model": "gpt-3.5-turbo",
                        "timeout": 30,
                        "max_retries": 1,
                        "temperature": 0.1
                    },
                    "anthropic": {
                        "models": ["claude-3-haiku-20240307", "claude-3-sonnet-20240229"],
                        "default_model": "claude-3-haiku-20240307",
                        "timeout": 30,
                        "max_retries": 1,
                        "temperature": 0.1
                    },
                    "groq": {
                        "models": ["llama-3.3-70b-versatile", "llama-3.3-70b-versatile", "mixtral-8x7b-32768"],
                        "default_model": "llama-3.3-70b-versatile",
                        "timeout": 30,
                        "max_retries": 1,
                        "temperature": 0.1
                    }
                },
                "default_provider": "groq",
                "fallback_provider": "openai"
            }
            
            with open(llm_config_path, 'w') as f:
                yaml.dump(default_llm_config, f, default_flow_style=False)
            
            logger.info("Created default LLM configuration")
        
        # Default system configuration
        system_config_path = self.config_dir / "system_config.yaml"
        if not system_config_path.exists():
            default_system_config = {
                "database": {
                    "max_connections": 10,
                    "schema_cache_ttl": 300,
                    "query_timeout": 30,
                    "max_rows": 10000
                },
                "security": {
                    "max_filters": 10,
                    "dangerous_keywords": [
                        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
                        "TRUNCATE", "REPLACE", "MERGE", "EXEC", "EXECUTE"
                    ],
                    "system_tables": [
                        "sqlite_master", "sqlite_sequence", "sqlite_stat1",
                        "sqlite_stat2", "sqlite_stat3", "sqlite_stat4"
                    ]
                },
                "v0_scope": {
                    "supported_intents": ["aggregate", "retrieve", "count"],
                    "supported_aggregations": ["sum", "count", "avg"],
                    "max_query_complexity": 5
                }
            }
            
            with open(system_config_path, 'w') as f:
                yaml.dump(default_system_config, f, default_flow_style=False)
            
            logger.info("Created default system configuration")
    
    def get_config(self, config_name: str, reload: bool = False) -> Dict[str, Any]:
        """Get configuration with optional hot-reload"""
        config_path = self.config_dir / f"{config_name}.yaml"
        
        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            return {}
        
        # Check if reload is needed
        current_timestamp = config_path.stat().st_mtime
        cached_timestamp = self._file_timestamps.get(config_name, 0)
        
        if reload or config_name not in self._config_cache or current_timestamp > cached_timestamp:
            try:
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                
                self._config_cache[config_name] = config
                self._file_timestamps[config_name] = current_timestamp
                
                logger.info(f"Loaded configuration: {config_name}")
                return config
                
            except Exception as e:
                logger.error(f"Failed to load configuration {config_name}: {e}")
                return self._config_cache.get(config_name, {})
        
        return self._config_cache[config_name]
    
    def update_config(self, config_name: str, config_data: Dict[str, Any]) -> bool:
        """Update configuration file"""
        try:
            config_path = self.config_dir / f"{config_name}.yaml"
            
            # Backup existing config
            if config_path.exists():
                backup_path = self.config_dir / f"{config_name}.yaml.backup"
                config_path.rename(backup_path)
            
            # Write new config
            with open(config_path, 'w') as f:
                yaml.dump(config_data, f, default_flow_style=False)
            
            # Update cache
            self._config_cache[config_name] = config_data
            self._file_timestamps[config_name] = config_path.stat().st_mtime
            
            logger.info(f"Updated configuration: {config_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update configuration {config_name}: {e}")
            return False
    
    def get_business_rules(self, reload: bool = False) -> Dict[str, Any]:
        """Get business rules configuration"""
        return self.get_config("business_rules", reload)
    
    def get_llm_config(self, reload: bool = False) -> Dict[str, Any]:
        """Get LLM configuration"""
        return self.get_config("llm_config", reload)
    
    def get_system_config(self, reload: bool = False) -> Dict[str, Any]:
        """Get system configuration"""
        return self.get_config("system_config", reload)
    
    def get_llm_provider_config(self, provider: str) -> Dict[str, Any]:
        """Get configuration for specific LLM provider"""
        llm_config = self.get_llm_config()
        providers = llm_config.get("providers", {})
        
        if provider not in providers:
            logger.warning(f"LLM provider '{provider}' not found in configuration")
            return {}
        
        return providers[provider]
    
    def validate_config(self, config_name: str) -> bool:
        """Validate configuration structure"""
        config = self.get_config(config_name)
        
        if config_name == "business_rules":
            required_keys = ["metric_mappings", "entity_mappings", "aggregation_functions"]
            return all(key in config for key in required_keys)
        
        elif config_name == "llm_config":
            required_keys = ["providers", "default_provider"]
            return all(key in config for key in required_keys)
        
        elif config_name == "system_config":
            required_keys = ["database", "security", "v0_scope"]
            return all(key in config for key in required_keys)
        
        return True
    
    def reload_all_configs(self) -> None:
        """Reload all cached configurations"""
        for config_name in list(self._config_cache.keys()):
            self.get_config(config_name, reload=True)
        
        logger.info("Reloaded all configurations")
    
    def get_config_status(self) -> Dict[str, Any]:
        """Get status of all configurations"""
        status = {
            "config_directory": str(self.config_dir),
            "configs": {},
            "last_updated": datetime.now().isoformat()
        }
        
        for config_file in self.config_dir.glob("*.yaml"):
            config_name = config_file.stem
            status["configs"][config_name] = {
                "exists": True,
                "last_modified": datetime.fromtimestamp(config_file.stat().st_mtime).isoformat(),
                "cached": config_name in self._config_cache,
                "valid": self.validate_config(config_name)
            }
        
        return status

# Global configuration manager instance
config_manager = ConfigManager()