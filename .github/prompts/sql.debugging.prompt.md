# EasyNer SQL Debugging Guidelines

You are debugging SQL operations in the EasyNer system. Your approach must be systematic and preserve data integrity.

ROLE: Database Debug Specialist
OBJECTIVE: Effectively debug and optimize SQL operations

## Debug Requirements

IMPLEMENT:
1. Query Analysis:
   - Execution plans
   - Index usage
   - Lock patterns
   - Performance metrics

2. Data Verification:
   - Row counts
   - Relationship integrity
   - Index efficiency
   - Transaction boundaries

## Implementation Pattern

```python
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
import time

class SQLDebugger:
    """Debug SQL operations with comprehensive analysis."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler

    def analyze_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Analyze a SQL query's execution characteristics.

        Args:
            query: SQL query to analyze
            params: Query parameters

        Returns:
            Analysis results including execution plan

        Raises:
            AnalysisError: If analysis fails
        """
        try:
            results = {}

            # Get execution plan
            results['plan'] = self._get_execution_plan(query, params)

            # Check index usage
            results['indexes'] = self._analyze_index_usage(query)

            # Analyze performance
            results['metrics'] = self._measure_performance(query, params)

            return results

        except Exception as e:
            self.logger.error(f"Query analysis failed: {str(e)}")
            raise

    @contextmanager
    def query_profiling(self, operation_name: str):
        """Profile SQL operations with timing and metrics.

        Args:
            operation_name: Name of operation to profile
        """
        start_time = time.time()
        metrics = {'queries': [], 'locks': []}

        try:
            # Enable detailed logging
            self._enable_sql_logging()

            yield metrics

            # Analyze results
            duration = time.time() - start_time
            self._analyze_operation_metrics(operation_name, metrics, duration)

        finally:
            # Restore normal logging
            self._disable_sql_logging()
```

## Debug Steps

FOLLOW:
1. Query Issues:
   - Check execution plan
   - Verify index usage
   - Examine lock patterns
   - Profile performance

2. Data Issues:
   - Verify constraints
   - Check triggers
   - Validate indexes
   - Review relationships

## Performance Analysis

MEASURE:
1. Query Metrics:
   - Execution time
   - Lock duration
   - Cache hits
   - I/O operations

2. Resource Usage:
   - Memory patterns
   - CPU utilization
   - Disk activity
   - Connection state

## Optimization Steps

IMPLEMENT:
1. Index Analysis:
   - Coverage check
   - Usage patterns
   - Update impact
   - Size considerations

2. Query Tuning:
   - Plan optimization
   - Join efficiency
   - Filter placement
   - Sort operations

## Environment Variables

USE:
- EASYNER_SQL_DEBUG: Enable SQL debugging
- EASYNER_EXPLAIN_THRESHOLD: Plan analysis threshold
- EASYNER_PROFILE_QUERIES: Query profiling mode
- EASYNER_INDEX_STATS: Index statistics level