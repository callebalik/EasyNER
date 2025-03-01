# EasyNer Thread Safety Guidelines

You are implementing concurrent operations for the EasyNer system. Your code must handle parallel processing safely and efficiently.

ROLE: Concurrency Engineer
OBJECTIVE: Implement thread-safe operations for distributed processing

## Concurrency Requirements

IMPLEMENT:
1. Thread Safety:
   - Resource locking
   - State protection
   - Connection pooling
   - Race condition prevention

2. Parallel Processing:
   - Task distribution
   - Resource sharing
   - Progress tracking
   - Error isolation

## Implementation Pattern

```python
from typing import Dict, Any, List
from threading import Lock, RLock
from concurrent.futures import ThreadPoolExecutor
import queue

class ThreadSafeProcessor:
    """Handle concurrent processing with proper safety."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler
        self._lock = RLock()
        self._state = {}
        self._queue = queue.Queue()

    def process_concurrent(
        self,
        items: List[Any],
        max_workers: int = 8
    ) -> Dict[str, Any]:
        """Process items concurrently with proper safety.

        Args:
            items: Items to process
            max_workers: Maximum concurrent workers

        Returns:
            Processing results

        Raises:
            ConcurrencyError: If parallel processing fails
        """
        results = {}
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit tasks
                futures = {
                    executor.submit(self._process_item, item): item_id
                    for item_id, item in enumerate(items)
                }

                # Collect results safely
                for future in futures:
                    try:
                        item_id = futures[future]
                        with self._lock:
                            results[item_id] = future.result()
                    except Exception as e:
                        self.logger.error(f"Task failed: {str(e)}")
                        self._handle_task_failure(item_id, e)

            return results

        except Exception as e:
            self.logger.error(f"Concurrent processing failed: {str(e)}")
            raise

    def _process_item(self, item: Any) -> Any:
        """Process a single item with proper locking.

        Args:
            item: Item to process

        Returns:
            Processed item
        """
        try:
            # Acquire resources safely
            with self._lock:
                resources = self._acquire_resources()

            try:
                # Process with acquired resources
                result = self._execute_processing(item, resources)

                # Update shared state safely
                with self._lock:
                    self._update_state(item, result)

                return result

            finally:
                # Release resources
                self._release_resources(resources)

        except Exception as e:
            self.logger.error(f"Item processing failed: {str(e)}")
            raise
```

## Thread Safety Patterns

IMPLEMENT:
1. Resource Protection:
   - Lock usage
   - State isolation
   - Resource pools
   - Deadlock prevention

2. Data Sharing:
   - Thread-local storage
   - Immutable patterns
   - Message passing
   - Safe queues

## Error Handling

IMPLEMENT:
1. Task Failures:
   - Error isolation
   - Resource cleanup
   - State recovery
   - Task retry

2. Resource Cleanup:
   - Lock release
   - Connection return
   - Memory cleanup
   - State reset

## Performance Optimization

OPTIMIZE:
1. Lock Strategy:
   - Fine-grained locking
   - Lock timeout
   - Lock hierarchy
   - Lock contention

2. Resource Usage:
   - Connection pooling
   - Thread pooling
   - Memory limits
   - CPU allocation

## Environment Variables

USE:
- EASYNER_MAX_THREADS: Maximum thread count
- EASYNER_LOCK_TIMEOUT: Lock acquisition timeout
- EASYNER_RETRY_COUNT: Task retry attempts
- EASYNER_POOL_SIZE: Resource pool size