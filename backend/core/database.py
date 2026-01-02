"""
Database Connector with SQLite connection management and schema introspection
"""
import sqlite3
import threading
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

@dataclass
class ColumnInfo:
    """Information about a database column"""
    name: str
    type: str
    nullable: bool
    primary_key: bool = False
    default_value: Optional[str] = None

@dataclass
class TableSchema:
    """Schema information for a database table"""
    name: str
    columns: Dict[str, ColumnInfo]
    row_count: Optional[int] = None

class SchemaCache:
    """In-memory cache for database schema information"""
    
    def __init__(self, ttl_seconds: int = 300):
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[str, Dict] = {}
        self._timestamps: Dict[str, float] = {}
        self._lock = threading.Lock()
    
    def get(self, db_path: str) -> Optional[Dict[str, TableSchema]]:
        """Get cached schema if still valid"""
        with self._lock:
            if db_path not in self._cache:
                return None
            
            if time.time() - self._timestamps[db_path] > self.ttl_seconds:
                del self._cache[db_path]
                del self._timestamps[db_path]
                return None
            
            return self._cache[db_path]
    
    def set(self, db_path: str, schema: Dict[str, TableSchema]) -> None:
        """Cache schema information"""
        with self._lock:
            self._cache[db_path] = schema
            self._timestamps[db_path] = time.time()
    
    def invalidate(self, db_path: str) -> None:
        """Invalidate cached schema for a database"""
        with self._lock:
            if db_path in self._cache:
                del self._cache[db_path]
                del self._timestamps[db_path]

class ConnectionPool:
    """Simple SQLite connection pool"""
    
    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self._connections: List[sqlite3.Connection] = []
        self._lock = threading.Lock()
        self._db_path: Optional[str] = None
    
    def get_connection(self, db_path: str) -> sqlite3.Connection:
        """Get a connection from the pool or create a new one"""
        with self._lock:
            # If database path changed, close all existing connections
            if self._db_path != db_path:
                self._close_all_connections()
                self._db_path = db_path
            
            # Try to reuse existing connection
            if self._connections:
                return self._connections.pop()
            
            # Create new connection
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            
            # Enable read-only mode for safety
            conn.execute("PRAGMA query_only = ON")
            
            return conn
    
    def return_connection(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool"""
        with self._lock:
            if len(self._connections) < self.max_connections:
                self._connections.append(conn)
            else:
                conn.close()
    
    def _close_all_connections(self) -> None:
        """Close all connections in the pool"""
        for conn in self._connections:
            conn.close()
        self._connections.clear()
    
    def close_all(self) -> None:
        """Close all connections and clear the pool"""
        with self._lock:
            self._close_all_connections()

class DatabaseConnector:
    """Main database connector with schema introspection and connection management"""
    
    def __init__(self, max_connections: int = 10, cache_ttl: int = 300):
        self.pool = ConnectionPool(max_connections)
        self.cache = SchemaCache(cache_ttl)
        self._current_db_path: Optional[str] = None
        self._lock = threading.Lock()
    
    def connect(self, db_path: str) -> bool:
        """Connect to a SQLite database file"""
        try:
            # Validate file exists and is readable
            if not Path(db_path).exists():
                raise FileNotFoundError(f"Database file not found: {db_path}")
            
            # Test connection
            conn = self.pool.get_connection(db_path)
            conn.execute("SELECT 1")
            self.pool.return_connection(conn)
            
            with self._lock:
                self._current_db_path = db_path
            
            logger.info(f"Connected to database: {db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database {db_path}: {e}")
            return False
    
    def introspect_schema(self) -> Dict[str, TableSchema]:
        """Extract complete schema information from the database"""
        if not self._current_db_path:
            raise RuntimeError("No database connected")
        
        # Check cache first
        cached_schema = self.cache.get(self._current_db_path)
        if cached_schema:
            logger.debug(f"Using cached schema for {self._current_db_path}")
            return cached_schema
        
        try:
            conn = self.pool.get_connection(self._current_db_path)
            schema = {}
            
            # Get all table names
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)
            table_names = [row[0] for row in cursor.fetchall()]
            
            for table_name in table_names:
                # Get column information
                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                columns = {}
                
                for row in cursor.fetchall():
                    col_info = ColumnInfo(
                        name=row[1],
                        type=self._normalize_sqlite_type(row[2]),
                        nullable=not bool(row[3]),
                        primary_key=bool(row[5]),
                        default_value=row[4]
                    )
                    columns[col_info.name] = col_info
                
                # Get row count
                try:
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]
                except:
                    row_count = None
                
                schema[table_name] = TableSchema(
                    name=table_name,
                    columns=columns,
                    row_count=row_count
                )
            
            self.pool.return_connection(conn)
            
            # Cache the schema
            self.cache.set(self._current_db_path, schema)
            
            logger.info(f"Introspected schema for {len(schema)} tables")
            return schema
            
        except Exception as e:
            logger.error(f"Schema introspection failed: {e}")
            raise
    
    def execute_query(self, sql: str, params: Dict[str, Any] = None) -> List[Dict]:
        """Execute a SQL query and return results"""
        if not self._current_db_path:
            raise RuntimeError("No database connected")
        
        if params is None:
            params = {}
        
        try:
            conn = self.pool.get_connection(self._current_db_path)
            
            # Execute query with parameters
            cursor = conn.execute(sql, params)
            
            # Convert rows to dictionaries
            results = []
            for row in cursor.fetchall():
                results.append(dict(row))
            
            self.pool.return_connection(conn)
            
            logger.debug(f"Executed query, returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def validate_connection(self) -> bool:
        """Validate that the current connection is still working"""
        if not self._current_db_path:
            return False
        
        try:
            conn = self.pool.get_connection(self._current_db_path)
            conn.execute("SELECT 1")
            self.pool.return_connection(conn)
            return True
        except:
            return False
    
    def close(self) -> None:
        """Close all connections and clear cache"""
        self.pool.close_all()
        if self._current_db_path:
            self.cache.invalidate(self._current_db_path)
        with self._lock:
            self._current_db_path = None
        logger.info("Database connections closed")
    
    def _normalize_sqlite_type(self, sqlite_type: str) -> str:
        """Normalize SQLite type names to standard types"""
        sqlite_type = sqlite_type.upper()
        
        if 'INT' in sqlite_type:
            return 'INTEGER'
        elif any(t in sqlite_type for t in ['CHAR', 'CLOB', 'TEXT']):
            return 'TEXT'
        elif any(t in sqlite_type for t in ['REAL', 'FLOA', 'DOUB', 'DECIMAL', 'NUMERIC']):
            return 'REAL'
        elif 'BLOB' in sqlite_type:
            return 'BLOB'
        else:
            return 'TEXT'  # Default fallback