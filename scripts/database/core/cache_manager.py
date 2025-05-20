import sqlite3
import time
import json
import logging
from typing import Any, Optional, Dict, List, Tuple, Union
import pickle
import os

TABLE_CACHE = "cache_entries"

class CacheManager:
    """
    Database-backed cache manager for EasyNer that stores key-value pairs with expiration times.
    Provides methods for storing, retrieving, and cleaning expired cache entries.

    Features:
    - Key-value caching with optional TTL
    - Global metrics storage
    - Function result caching via decorators
    - Automatic cache cleanup

    Usage:
        # Directly
        cache = CacheManager(db_handler)
        cache.set("my_key", complex_result, ttl_seconds=3600)
        result = cache.get("my_key")

        # Global metrics
        cache.set_global("total_entities", 12500000)
        count = cache.get_global("total_entities")

        # As a decorator
        @cache.cached(ttl_seconds=3600, prefix="entity_stats")
        def expensive_function(arg1, arg2):
            # ...expensive operation...
            return result
    """

    # Define global namespace constants
    GLOBAL_NAMESPACE = "global"
    CONFIG_NAMESPACE = "config"
    SYSTEM_NAMESPACE = "system"
    METRICS_NAMESPACE = "metrics"

    def __init__(self, db_handler=None):
        """
        Initialize the cache manager with an optional existing db_handler.

        Args:
            db_handler: Optional EasyNerDBHandler instance. If None, a new one will be created.
        """
        if db_handler:
            self.db_handler = db_handler
            self.logger = db_handler.logger
            self.conn = db_handler.conn
            self.cursor = db_handler.cursor

            # Initialize tables right away if we have a handler
            self._ensure_cache_table_exists()
            self._initialize_global_metrics()
        else:
            # Minimal initialization without importing EasyNerDBHandler
            # Properties will be set manually later
            self.db_handler = None
            self.logger = logging.getLogger("EasyNerDB")
            self.conn = None
            self.cursor = None

    def _ensure_cache_table_exists(self) -> None:
        """Create the cache table if it doesn't exist."""
        try:
            # Use atomic transaction for table creation
            self.cursor.execute("BEGIN TRANSACTION")

            query = """--sql
            CREATE TABLE IF NOT EXISTS cache_entries (
                cache_key TEXT PRIMARY KEY,
                cache_value BLOB,
                cache_type TEXT,
                created_at REAL,
                expires_at REAL
            )
            """
            self.cursor.execute(query)

            # Create an index on the expires_at column for faster cleanup queries
            index_query = """--sql
            CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON cache_entries(expires_at)
            """
            self.cursor.execute(index_query)

            # Create an index on cache key prefix for faster global lookups
            prefix_index_query = """--sql
            CREATE INDEX IF NOT EXISTS idx_cache_key_prefix ON cache_entries(cache_key)
            """
            self.cursor.execute(prefix_index_query)

            self.conn.commit()
            self.logger.debug("Cache table initialized")

        except sqlite3.Error as e:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.logger.error(f"Failed to create cache table: {e}")
            raise

    def _initialize_global_metrics(self) -> None:
        """
        Initialize global metrics if they don't exist.
        This runs only once during the initial CacheManager setup.
        """
        try:
            # Check if we've already initialized
            initialized = self.get_global("_metrics_initialized")
            if initialized:
                self.logger.debug("Global metrics already initialized")
                return

            self.logger.info("Initializing global metrics cache")

            # Set initialization flag first to prevent duplicate initialization
            self.set_global("_metrics_initialized", True)

            # Set basic system stats
            self.set_global("_system_start_time", time.time())
            self.set_global("_cache_manager_version", "1.0")

            # Record database path for reference
            db_path = getattr(self.db_handler, 'db_path', 'unknown')
            self.set_global("_database_path", db_path)

            # Initialize empty metrics containers with no expiration
            self.set_global("performance_metrics", {})
            self.set_global("system_stats", {})

            # Add environment info
            env_info = {
                "batch_size": int(os.environ.get("BATCH_SIZE", "1000")),
                "log_level": os.environ.get("LOG_LEVEL", "INFO"),
                "db_path": os.environ.get("DB_PATH", "")
            }
            self.set_global("environment", env_info)

            self.logger.info("Global metrics initialized")

        except Exception as e:
            self.logger.error(f"Error initializing global metrics: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve a value from the cache by key.

        Args:
            key: The cache key
            default: Value to return if key is not found or expired

        Returns:
            The cached value or default if not found
        """
        try:
            query = """--sql
            SELECT cache_value, cache_type, expires_at FROM cache_entries
            WHERE cache_key = ? AND (expires_at IS NULL OR expires_at > ?)
            """
            current_time = time.time()

            self.cursor.execute(query, (key, current_time))
            result = self.cursor.fetchone()

            if not result:
                self.logger.debug(f"Cache miss for key: {key}")
                return default

            value, value_type, expires_at = result

            # Deserialize based on the stored type
            if value_type == 'pickle':
                value = pickle.loads(value)
            elif value_type == 'json':
                value = json.loads(value)
            elif value_type == 'int':
                value = int(value)
            elif value_type == 'float':
                value = float(value)
            elif value_type == 'str':
                value = str(value)

            self.logger.debug(f"Cache hit for key: {key}")
            return value

        except sqlite3.Error as e:
            self.logger.error(f"Error retrieving from cache for key {key}: {e}")
            return default

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None, overwrite: bool = True) -> bool:
        """
        Store a value in the cache with optional expiration time.

        Args:
            key: The cache key
            value: The value to store
            ttl_seconds: Time to live in seconds (None means no expiration)
            overwrite: Whether to overwrite existing value

        Returns:
            True if successful, False otherwise
        """
        try:
            # Use atomic transaction
            self.cursor.execute("BEGIN TRANSACTION")

            current_time = time.time()
            expires_at = current_time + ttl_seconds if ttl_seconds is not None else None

            # Determine serialization method based on value type
            if isinstance(value, (dict, list, tuple)) or not isinstance(value, (int, float, str, bytes)):
                try:
                    serialized_value = pickle.dumps(value)
                    value_type = 'pickle'
                except (pickle.PickleError, TypeError):
                    # Fallback to JSON for values that can't be pickled
                    serialized_value = json.dumps(value)
                    value_type = 'json'
            elif isinstance(value, int):
                serialized_value = value
                value_type = 'int'
            elif isinstance(value, float):
                serialized_value = value
                value_type = 'float'
            elif isinstance(value, str):
                serialized_value = value
                value_type = 'str'
            else:
                serialized_value = str(value)
                value_type = 'str'

            if overwrite:
                query = """--sql
                INSERT OR REPLACE INTO cache_entries (cache_key, cache_value, cache_type, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """
            else:
                query = """--sql
                INSERT OR IGNORE INTO cache_entries (cache_key, cache_value, cache_type, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """

            self.cursor.execute(query, (key, serialized_value, value_type, current_time, expires_at))
            self.conn.commit()

            self.logger.debug(f"Cache set for key: {key}, expires: {expires_at}")
            return True

        except sqlite3.Error as e:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.logger.error(f"Failed to set cache for key {key}: {e}")
            return False

    def get_global(self, name: str = None, default: Any = None) -> Any:
        """
        Get a global cache value or all global values.

        Args:
            name: Specific global value name (None for all globals)
            default: Default value if not found

        Returns:
            The global value or dictionary of all global values
        """
        if name is not None:
            # Get specific global value
            key = f"{self.GLOBAL_NAMESPACE}.{name}"
            return self.get(key, default)
        else:
            # Get all globals using prefix query
            try:
                prefix = f"{self.GLOBAL_NAMESPACE}."
                query = """--sql
                    SELECT
                        SUBSTR(cache_key, LENGTH(?)+1) AS name,
                        cache_value,
                        cache_type
                    FROM cache_entries
                    WHERE cache_key LIKE ? || '%'
                    AND (expires_at IS NULL OR expires_at > ?)
                """

                self.cursor.execute(query, (prefix, prefix, time.time()))

                # Process results into dictionary
                result = {}
                for name, value, value_type in self.cursor:
                    # Skip the namespace prefix
                    clean_name = name

                    # Deserialize based on the stored type
                    if value_type == 'pickle':
                        result[clean_name] = pickle.loads(value)
                    elif value_type == 'json':
                        result[clean_name] = json.loads(value)
                    elif value_type == 'int':
                        result[clean_name] = int(value)
                    elif value_type == 'float':
                        result[clean_name] = float(value)
                    elif value_type == 'str':
                        result[clean_name] = str(value)

                return result or default

            except sqlite3.Error as e:
                self.logger.error(f"Error retrieving all global values: {e}")
                return default or {}

    def set_global(self, name: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """
        Set a global cache value with optional expiration.

        Args:
            name: Name of the global value
            value: Value to store
            ttl_seconds: Optional TTL in seconds (None for no expiration)

        Returns:
            True if successful, False otherwise
        """
        key = f"{self.GLOBAL_NAMESPACE}.{name}"
        return self.set(key, value, ttl_seconds=ttl_seconds)

    def update_global_metrics(self, metrics_dict: Dict[str, Any] = None) -> bool:
        """
        Update global metrics with current database statistics.
        Can be called during CacheManager initialization or separately.

        Args:
            metrics_dict: Optional dictionary of metrics to update
                          If None, will query database for basic statistics

        Returns:
            True if successful, False otherwise
        """
        try:
            # Start transaction for atomicity
            self.cursor.execute("BEGIN TRANSACTION")

            current_time = time.time()
            metrics = metrics_dict or {}

            # If no metrics provided, get basic database stats
            if not metrics:
                try:
                    # These are generic queries that work with most EasyNer tables
                    # In a real implementation, we might need to check if tables exist first

                    # Get total rows in main tables using schema constants
                    from ..data_model.schema import TABLE_NE, TABLE_DOCS, TABLE_NE_CLASS

                    # Only execute these queries if tables exist
                    tables_to_check = [TABLE_NE, TABLE_DOCS, TABLE_NE_CLASS]

                    # Check which tables exist
                    for table in tables_to_check:
                        self.cursor.execute("""--sql
                            SELECT COUNT(*) FROM sqlite_master
                            WHERE type='table' AND name=?
                        """, (table,))

                        if self.cursor.fetchone()[0] > 0:
                            # Table exists, count rows
                            self.cursor.execute(f"""--sql SELECT COUNT(*) FROM {table}""")
                            count = self.cursor.fetchone()[0]
                            metrics[f"{table}_count"] = count

                    # Get database file size
                    db_path = getattr(self.db_handler, 'db_path', None)
                    if db_path and os.path.exists(db_path):
                        metrics["database_size_bytes"] = os.path.getsize(db_path)

                except Exception as e:
                    self.logger.error(f"Error collecting database metrics: {e}")
                    # Continue with what we have even if some metrics failed

            # Add timestamp
            metrics["last_updated"] = current_time

            # Get existing metrics
            existing_metrics = self.get_global("metrics") or {}

            # Update with new metrics
            existing_metrics.update(metrics)

            # Store updated metrics
            self.set_global("metrics", existing_metrics)

            # Also store individual metrics for easy access
            for key, value in metrics.items():
                self.set_global(f"metric.{key}", value)

            self.conn.commit()
            self.logger.info(f"Updated {len(metrics)} global metrics")
            return True

        except Exception as e:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.logger.error(f"Failed to update global metrics: {e}")
            return False

    def get_metrics(self, category: str = None) -> Dict[str, Any]:
        """
        Get metrics by category or all metrics.

        Args:
            category: Optional category name (e.g., 'entity', 'performance')

        Returns:
            Dictionary of metrics
        """
        try:
            if category:
                # Get metrics for specific category
                prefix = f"{self.GLOBAL_NAMESPACE}.metric.{category}"
                query = """--sql
                    SELECT
                        SUBSTR(cache_key, LENGTH(?)+1) AS name,
                        cache_value,
                        cache_type
                    FROM cache_entries
                    WHERE cache_key LIKE ? || '%'
                    AND (expires_at IS NULL OR expires_at > ?)
                """

                self.cursor.execute(query, (prefix, prefix, time.time()))

                result = {}
                for name, value, value_type in self.cursor:
                    # Extract just the metric name without prefix
                    clean_name = name

                    # Deserialize based on the stored type
                    if value_type == 'pickle':
                        result[clean_name] = pickle.loads(value)
                    elif value_type == 'json':
                        result[clean_name] = json.loads(value)
                    elif value_type == 'int':
                        result[clean_name] = int(value)
                    elif value_type == 'float':
                        result[clean_name] = float(value)
                    elif value_type == 'str':
                        result[clean_name] = str(value)

                return result
            else:
                # Get all metrics
                return self.get_global("metrics") or {}

        except sqlite3.Error as e:
            self.logger.error(f"Error retrieving metrics: {e}")
            return {}

    def delete(self, key: str) -> bool:
        """
        Delete a specific cache entry.

        Args:
            key: The cache key to delete

        Returns:
            True if deleted, False otherwise
        """
        try:
            # Use atomic transaction
            self.cursor.execute("BEGIN TRANSACTION")

            query = """--sql DELETE FROM cache_entries WHERE cache_key = ?"""

            self.cursor.execute(query, (key,))

            if self.cursor.rowcount > 0:
                self.conn.commit()
                self.logger.debug(f"Deleted cache entry for key: {key}")
                return True
            else:
                self.conn.commit()
                self.logger.debug(f"No cache entry found for key: {key}")
                return False

        except sqlite3.Error as e:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.logger.error(f"Failed to delete cache for key {key}: {e}")
            return False

    def delete_by_prefix(self, prefix: str) -> int:
        """
        Delete all cache entries with keys starting with the given prefix.

        Args:
            prefix: The key prefix to match

        Returns:
            Number of entries deleted
        """
        try:
            # Use atomic transaction
            self.cursor.execute("BEGIN TRANSACTION")

            query = """--sql DELETE FROM cache_entries WHERE cache_key LIKE ?"""

            self.cursor.execute(query, (prefix + '%',))
            deleted_count = self.cursor.rowcount

            self.conn.commit()
            self.logger.info(f"Deleted {deleted_count} cache entries with prefix '{prefix}'")
            return deleted_count

        except sqlite3.Error as e:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.logger.error(f"Failed to delete cache entries with prefix '{prefix}': {e}")
            return 0

    def clean_expired(self) -> int:
        """
        Delete all expired cache entries.

        Returns:
            Number of entries deleted
        """
        try:
            # Use atomic transaction
            self.cursor.execute("BEGIN TRANSACTION")

            query = """--sql DELETE FROM cache_entries WHERE expires_at IS NOT NULL AND expires_at < ?"""
            current_time = time.time()

            self.cursor.execute(query, (current_time,))
            deleted_count = self.cursor.rowcount

            self.conn.commit()
            self.logger.info(f"Cleaned {deleted_count} expired cache entries")
            return deleted_count

        except sqlite3.Error as e:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.logger.error(f"Failed to clean expired cache entries: {e}")
            return 0

    def clean_older_than(self, seconds: int) -> int:
        """
        Delete all cache entries older than the specified time.

        Args:
            seconds: Delete entries created more than this many seconds ago

        Returns:
            Number of entries deleted
        """
        try:
            # Use atomic transaction
            self.cursor.execute("BEGIN TRANSACTION")

            query = """--sql DELETE FROM cache_entries WHERE created_at < ?"""
            threshold_time = time.time() - seconds

            self.cursor.execute(query, (threshold_time,))
            deleted_count = self.cursor.rowcount

            self.conn.commit()
            self.logger.info(f"Cleaned {deleted_count} cache entries older than {seconds} seconds")
            return deleted_count

        except sqlite3.Error as e:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.logger.error(f"Failed to clean old cache entries: {e}")
            return 0

    def clear_all(self) -> int:
        """
        Delete all cache entries.

        Returns:
            Number of entries deleted
        """
        try:
            # Use atomic transaction
            self.cursor.execute("BEGIN TRANSACTION")

            query = """--sql DELETE FROM cache_entries"""

            self.cursor.execute(query)
            deleted_count = self.cursor.rowcount

            self.conn.commit()
            self.logger.info(f"Cleared all {deleted_count} cache entries")

            # Re-initialize the global metrics after clearing
            self._initialize_global_metrics()

            return deleted_count

        except sqlite3.Error as e:
            if self.conn.in_transaction:
                self.conn.rollback()
            self.logger.error(f"Failed to clear cache: {e}")
            return 0

    def cached(self, ttl_seconds: int = 3600, prefix: str = "", overwrite: bool = True):
        """
        Decorator that caches function results.

        Args:
            ttl_seconds: Time to live in seconds
            prefix: Prefix for the cache key
            overwrite: Whether to overwrite existing value

        Returns:
            Decorated function
        """
        def decorator(func):
            def wrapper(*args, **kwargs):
                # Create a cache key from the function name and arguments
                key_parts = [prefix if prefix else func.__name__]

                # Add positional arguments
                for arg in args:
                    if isinstance(arg, (int, float, str, bool)):
                        key_parts.append(str(arg))
                    else:
                        # For complex objects, use their id or hash
                        key_parts.append(f"obj_{id(arg)}")

                # Add keyword arguments (sorted for consistency)
                for k, v in sorted(kwargs.items()):
                    if isinstance(v, (int, float, str, bool)):
                        key_parts.append(f"{k}_{v}")
                    else:
                        key_parts.append(f"{k}_obj_{id(v)}")

                cache_key = "_".join(key_parts)

                # Try to get from cache
                cached_result = self.get(cache_key)
                if cached_result is not None:
                    return cached_result

                # If not in cache, call the function
                start_time = time.time()
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                # Store the result in cache
                self.set(cache_key, result, ttl_seconds=ttl_seconds, overwrite=overwrite)

                # Optionally record execution time in metrics
                if hasattr(func, "__qualname__"):
                    metric_key = f"func_time.{func.__qualname__}"
                    self.set_global(metric_key, execution_time)

                return result
            return wrapper
        return decorator

    # Global configuration methods
    def get_config(self, name: str = None, default: Any = None) -> Any:
        """Get configuration value(s)"""
        if name is not None:
            key = f"{self.CONFIG_NAMESPACE}.{name}"
            return self.get(key, default)
        else:
            try:
                prefix = f"{self.CONFIG_NAMESPACE}."
                query = """--sql
                    SELECT
                        SUBSTR(cache_key, LENGTH(?)+1) AS name,
                        cache_value,
                        cache_type
                    FROM cache_entries
                    WHERE cache_key LIKE ? || '%'
                    AND (expires_at IS NULL OR expires_at > ?)
                """

                self.cursor.execute(query, (prefix, prefix, time.time()))

                result = {}
                for name, value, value_type in self.cursor:
                    # Process as in get_global
                    clean_name = name

                    if value_type == 'pickle':
                        result[clean_name] = pickle.loads(value)
                    elif value_type == 'json':
                        result[clean_name] = json.loads(value)
                    elif value_type == 'int':
                        result[clean_name] = int(value)
                    elif value_type == 'float':
                        result[clean_name] = float(value)
                    elif value_type == 'str':
                        result[clean_name] = str(value)

                return result or default

            except sqlite3.Error as e:
                self.logger.error(f"Error retrieving all config values: {e}")
                return default or {}

    def set_config(self, name: str, value: Any) -> bool:
        """Set configuration value (typically with no expiration)"""
        key = f"{self.CONFIG_NAMESPACE}.{name}"
        return self.set(key, value, ttl_seconds=None)  # Configs typically don't expire

    # System state methods
    def get_system_state(self, name: str = None) -> Dict[str, Any]:
        """Get system state information"""
        if name is not None:
            key = f"{self.SYSTEM_NAMESPACE}.{name}"
            return self.get(key)
        else:
            # Similar to get_global but for system namespace
            try:
                prefix = f"{self.SYSTEM_NAMESPACE}."
                query = """--sql
                    SELECT
                        SUBSTR(cache_key, LENGTH(?)+1) AS name,
                        cache_value,
                        cache_type
                    FROM cache_entries
                    WHERE cache_key LIKE ? || '%'
                    AND (expires_at IS NULL OR expires_at > ?)
                """

                self.cursor.execute(query, (prefix, prefix, time.time()))

                # Process results into dictionary (same as in get_global)
                result = {}
                for name, value, value_type in self.cursor:
                    clean_name = name

                    if value_type == 'pickle':
                        result[clean_name] = pickle.loads(value)
                    elif value_type == 'json':
                        result[clean_name] = json.loads(value)
                    elif value_type == 'int':
                        result[clean_name] = int(value)
                    elif value_type == 'float':
                        result[clean_name] = float(value)
                    elif value_type == 'str':
                        result[clean_name] = str(value)

                return result

            except sqlite3.Error as e:
                self.logger.error(f"Error retrieving system state: {e}")
                return {}

    def set_system_state(self, name: str, value: Any) -> bool:
        """Set system state value (typically without expiration)"""
        key = f"{self.SYSTEM_NAMESPACE}.{name}"
        return self.set(key, value)

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the current cache usage.

        Returns:
            Dictionary with cache statistics
        """
        try:
            stats = {
                "total_entries": 0,
                "active_entries": 0,
                "expired_entries": 0,
                "permanent_entries": 0,
                "namespaces": {},
                "oldest_entry_age": 0,
                "newest_entry_age": 0,
                "total_size_bytes": 0
            }

            # Get current time for age calculations
            current_time = time.time()

            # Get total, active, and expired counts
            self.cursor.execute("""--sql
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN expires_at IS NULL OR expires_at > ? THEN 1 END) as active,
                    COUNT(CASE WHEN expires_at IS NOT NULL AND expires_at <= ? THEN 1 END) as expired,
                    COUNT(CASE WHEN expires_at IS NULL THEN 1 END) as permanent
                FROM cache_entries
            """, (current_time, current_time))

            row = self.cursor.fetchone()
            stats["total_entries"] = row[0]
            stats["active_entries"] = row[1]
            stats["expired_entries"] = row[2]
            stats["permanent_entries"] = row[3]

            # Get oldest and newest entries
            self.cursor.execute("""--sql
                SELECT
                    MIN(created_at) as oldest,
                    MAX(created_at) as newest
                FROM cache_entries
            """)

            row = self.cursor.fetchone()
            if row[0]:
                stats["oldest_entry_age"] = current_time - row[0]
            if row[1]:
                stats["newest_entry_age"] = current_time - row[1]

            # Count entries by namespace
            self.cursor.execute("""--sql
                SELECT
                    CASE
                        WHEN cache_key LIKE 'global.%' THEN 'global'
                        WHEN cache_key LIKE 'config.%' THEN 'config'
                        WHEN cache_key LIKE 'system.%' THEN 'system'
                        WHEN cache_key LIKE 'metrics.%' THEN 'metrics'
                        ELSE 'other'
                    END as namespace,
                    COUNT(*) as count
                FROM cache_entries
                GROUP BY namespace
            """)

            for namespace, count in self.cursor:
                stats["namespaces"][namespace] = count

            # Estimate total size (approximate)
            self.cursor.execute("""--sql
                SELECT
                    SUM(LENGTH(cache_key) + LENGTH(cache_value)) as total_size
                FROM cache_entries
            """)

            row = self.cursor.fetchone()
            if row[0]:
                stats["total_size_bytes"] = row[0]

            return stats

        except sqlite3.Error as e:
            self.logger.error(f"Error getting cache stats: {e}")
            return {"error": str(e)}