# EasyNer Caching Guidelines

You are implementing caching strategies for the EasyNer system. Your code must efficiently manage memory and improve data access performance.

ROLE: Cache Optimization Specialist
OBJECTIVE: Implement efficient caching mechanisms for improved performance

## Cache Requirements

IMPLEMENT:
1. Memory Caching:
   - Size limits
   - Eviction policies
   - Access patterns
   - Cache coherence

2. Data Caching:
   - Query results
   - Entity metadata
   - Relationship maps
   - Processing state

## Implementation Pattern

```python
from typing import Dict, Any, Optional, List
from collections import OrderedDict
from contextlib import contextmanager
import time

class LRUCache:
    """Memory-efficient LRU cache implementation."""

    def __init__(self, max_size: int = 1000):
        self.cache = OrderedDict()
        self.max_size = max_size

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache with LRU tracking."""
        try:
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        except KeyError:
            return None

    def put(self, key: str, value: Any) -> None:
        """Add item to cache with size management."""
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
        self.cache[key] = value

class CacheManager:
    """Manage caching strategies and policies."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler
        self.entity_cache = LRUCache()
        self.query_cache = LRUCache()

    @contextmanager
    def cached_operation(self, operation_name: str):
        """Execute operation with caching support.

        Args:
            operation_name: Name of operation to cache
        """
        cache_key = self._generate_cache_key(operation_name)

        try:
            # Check cache
            if result := self.query_cache.get(cache_key):
                yield result
                return

            # Execute operation
            result = yield None

            # Cache result
            self.query_cache.put(cache_key, result)

        except Exception as e:
            self.logger.error(f"Cache operation failed: {str(e)}")
            raise

    def get_cached_entity(
        self,
        entity_id: str,
        load_func: callable
    ) -> Dict[str, Any]:
        """Get entity with caching.

        Args:
            entity_id: Entity identifier
            load_func: Function to load entity if not cached

        Returns:
            Entity data
        """
        try:
            # Check cache
            if entity := self.entity_cache.get(entity_id):
                return entity

            # Load and cache
            entity = load_func(entity_id)
            self.entity_cache.put(entity_id, entity)
            return entity

        except Exception as e:
            self.logger.error(f"Entity cache access failed: {str(e)}")
            raise
```

## Cache Types

IMPLEMENT:
1. Memory Cache:
   - Entity data
   - Query results
   - Metadata
   - Relationships

2. Disk Cache:
   - Intermediate results
   - Large datasets
   - State snapshots
   - Recovery data

## Cache Policies

ENFORCE:
1. Size Management:
   - Maximum entries
   - Memory limits
   - Age thresholds
   - Priority levels

2. Eviction Rules:
   - LRU strategy
   - Priority based
   - Time based
   - Access patterns

## Performance Monitoring

TRACK:
1. Cache Metrics:
   - Hit rates
   - Miss patterns
   - Memory usage
   - Access times

2. Performance Impact:
   - Query speedup
   - Memory overhead
   - Load patterns
   - Response times

## Environment Variables

USE:
- EASYNER_CACHE_SIZE: Maximum cache entries
- EASYNER_CACHE_TTL: Entry time-to-live
- EASYNER_CACHE_POLICY: Eviction policy
- EASYNER_CACHE_MONITOR: Monitoring level