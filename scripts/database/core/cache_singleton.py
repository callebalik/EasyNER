import logging
import functools
import time
import hashlib
import inspect
from typing import Optional, Any, Callable

# Global cache manager instance
_cache_manager = None
_logger = logging.getLogger("EasyNerDB")

def get_cache_manager():
    """Get the global cache manager instance, lazily initialized when needed"""
    global _cache_manager
    if _cache_manager is None:
        _logger.debug("Initializing global cache manager")
        # Lazy import to avoid circular dependencies
        from .cache_manager import CacheManager
        _cache_manager = CacheManager(db_handler=None)
    return _cache_manager

def set_cache_manager_connection(conn, cursor, logger=None):
    """
    Set the connection properties for the global cache manager.
    Must be called before using cached decorators.

    Args:
        conn: SQLite connection object
        cursor: SQLite cursor object
        logger: Optional logger instance
    """
    cache_mgr = get_cache_manager()
    cache_mgr.conn = conn
    cache_mgr.cursor = cursor
    if logger:
        cache_mgr.logger = logger
    # Initialize cache table if needed
    if conn and cursor:
        cache_mgr._ensure_cache_table_exists()
    return cache_mgr

def cached(ttl_seconds: Optional[int] = 3600, prefix: str = "", overwrite: bool = True):
    """
    Global cached decorator that works without explicit CacheManager instantiation.
    If the cache manager is not fully initialized when this decorator is used,
    it will fall back to a pass-through execution of the function.

    Args:
        ttl_seconds: Time to live in seconds (None for no expiration)
        prefix: Cache key prefix
        overwrite: Whether to overwrite existing cached values

    Returns:
        Decorator function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache_mgr = get_cache_manager()

            # If cache manager is not fully initialized, just execute the function
            if cache_mgr.conn is None or cache_mgr.cursor is None:
                _logger.debug(f"Cache manager not initialized, executing {func.__name__} without caching")
                return func(*args, **kwargs)

            try:
                # Generate a cache key from function name, args, and kwargs
                func_name = func.__name__

                # Add self.__class__.__name__ if this is an instance method
                if args and hasattr(args[0], '__class__'):
                    cls_name = args[0].__class__.__name__
                    key_parts = [prefix, cls_name, func_name]
                else:
                    key_parts = [prefix, func_name]

                # Add stringified args and kwargs
                for arg in args[1:]:  # Skip self for instance methods
                    if isinstance(arg, (int, float, str, bool)):
                        key_parts.append(str(arg))
                    else:
                        key_parts.append(f"obj_{id(arg)}")

                for k, v in sorted(kwargs.items()):
                    if isinstance(v, (int, float, str, bool)):
                        key_parts.append(f"{k}_{v}")
                    else:
                        key_parts.append(f"{k}_obj_{id(v)}")

                cache_key = "_".join(key_parts)

                # Hash the key if it's too long
                if len(cache_key) > 100:
                    hash_obj = hashlib.md5(cache_key.encode())
                    cache_key = f"{prefix}_{func_name}_{hash_obj.hexdigest()}"

                # Try to get from cache
                cached_result = cache_mgr.get(cache_key)
                if cached_result is not None:
                    return cached_result

                # Not in cache, execute function
                start_time = time.time()
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                # Store in cache
                cache_mgr.set(cache_key, result, ttl_seconds=ttl_seconds, overwrite=overwrite)

                # Record performance metrics if possible
                try:
                    metric_key = f"func_time.{func.__qualname__}"
                    cache_mgr.set_global(metric_key, execution_time)
                except:
                    pass

                return result

            except Exception as e:
                _logger.warning(f"Cache error in {func.__name__}: {e} - executing without caching")
                return func(*args, **kwargs)
        return wrapper
    return decorator