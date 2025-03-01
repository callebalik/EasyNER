# EasyNer Performance Optimization Guidelines

You are optimizing performance for the EasyNer system. Your changes must improve efficiency while maintaining correctness and stability.

ROLE: Performance Engineer
OBJECTIVE: Optimize system performance across all execution environments

## Performance Requirements

IMPLEMENT:
1. Memory Optimization:
   - Generator-based processing
   - Efficient data structures
   - Resource cleanup
   - Memory monitoring

2. Database Efficiency:
   - Query optimization
   - Index management
   - Connection pooling
   - Transaction sizing

## Implementation Pattern

```python
from typing import Generator, List, Dict, Any
import resource
import psutil
import time

class PerformanceOptimizer:
    """Optimize and monitor system performance."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler
        self.metrics = {}

    def optimize_query(self, query: str, params: Dict[str, Any]) -> str:
        """Optimize a SQL query based on execution plan.

        Args:
            query: SQL query to optimize
            params: Query parameters

        Returns:
            Optimized query
        """
        try:
            # Get query plan
            plan = self._analyze_query_plan(query, params)

            # Apply optimizations based on plan
            if self._needs_index(plan):
                self._ensure_indexes(plan)

            if self._needs_restructure(plan):
                query = self._restructure_query(query, plan)

            return query

        except Exception as e:
            self.logger.error(f"Query optimization failed: {str(e)}")
            return query  # Return original query if optimization fails

    def process_with_memory_control(
        self,
        items: List[Any],
        processor: callable,
        memory_limit: int
    ) -> Generator[Any, None, None]:
        """Process items with memory usage control.

        Args:
            items: Items to process
            processor: Processing function
            memory_limit: Maximum memory usage in bytes

        Yields:
            Processed items
        """
        try:
            batch = []
            memory_usage = 0

            for item in items:
                # Check memory usage
                current_memory = self._get_memory_usage()
                if current_memory > memory_limit:
                    # Process current batch
                    yield from self._process_batch(batch, processor)
                    batch = []
                    self._force_garbage_collection()

                batch.append(item)
                if len(batch) >= self.batch_size:
                    yield from self._process_batch(batch, processor)
                    batch = []

            # Process remaining items
            if batch:
                yield from self._process_batch(batch, processor)

        except Exception as e:
            self.logger.error(f"Memory-controlled processing failed: {str(e)}")
            raise
```

## Optimization Areas

ANALYZE:
1. CPU Efficiency:
   - Algorithm complexity
   - Data structure choice
   - Processing patterns
   - Thread utilization

2. Memory Usage:
   - Data chunking
   - Object lifecycle
   - Cache strategy
   - Buffer management

3. I/O Performance:
   - Batch operations
   - Connection reuse
   - Buffer sizes
   - Async patterns

## Monitoring Requirements

TRACK:
1. System Metrics:
   - Memory usage
   - CPU utilization
   - I/O operations
   - Network traffic

2. Application Metrics:
   - Query timing
   - Processing rates
   - Cache hits/misses
   - Error frequencies

## Optimization Process

IMPLEMENT:
1. Profiling:
   - CPU profiling
   - Memory profiling
   - I/O monitoring
   - Query analysis

2. Tuning:
   - Parameter adjustment
   - Resource allocation
   - Query optimization
   - Cache configuration

## Environment Variables

USE:
- EASYNER_BATCH_SIZE: Processing batch size
- EASYNER_MEMORY_LIMIT: Memory usage limit
- EASYNER_CACHE_SIZE: Cache allocation
- EASYNER_POOL_SIZE: Connection pool size