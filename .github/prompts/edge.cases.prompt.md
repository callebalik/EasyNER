# EasyNer Edge Case Guidelines

You are handling edge cases in the EasyNer system. Your code must gracefully handle unusual situations while maintaining system stability.

ROLE: Edge Case Specialist
OBJECTIVE: Implement robust handling of edge cases and error scenarios

## Edge Case Requirements

IMPLEMENT:
1. Data Edge Cases:
   - Missing values
   - Malformed input
   - Boundary conditions
   - Invalid relationships

2. System Edge Cases:
   - Resource exhaustion
   - Network failures
   - Concurrent conflicts
   - Timeout conditions

## Implementation Pattern

```python
from typing import Optional, Dict, Any, Generator
from contextlib import contextmanager
import resource

class EdgeCaseHandler:
    """Handle edge cases and error scenarios."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler

    @contextmanager
    def protected_execution(self, operation_name: str):
        """Execute operations with edge case protection.

        Args:
            operation_name: Name of operation to protect
        """
        self.logger.info(f"Starting protected execution: {operation_name}")

        try:
            # Check resource availability
            self._verify_resources()

            # Set up recovery points
            with self._recovery_context():
                yield

        except MemoryError:
            self.logger.error("Memory exhaustion detected")
            self._handle_memory_exhaustion()
            raise

        except TimeoutError:
            self.logger.error("Operation timeout detected")
            self._handle_timeout()
            raise

        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            self._handle_unknown_error(e)
            raise

        finally:
            self._cleanup_resources()

    def process_with_retry(
        self,
        data: Dict[str, Any],
        max_retries: int = 3,
        backoff_factor: float = 1.5
    ) -> Optional[Dict[str, Any]]:
        """Process data with automatic retry on failure.

        Args:
            data: Data to process
            max_retries: Maximum retry attempts
            backoff_factor: Delay multiplier between retries

        Returns:
            Processed data or None if all retries fail

        Raises:
            ProcessingError: If processing ultimately fails
        """
        try:
            for attempt in self._retry_generator(max_retries, backoff_factor):
                try:
                    with self.protected_execution("data_processing"):
                        return self._process_data(data)

                except RetryableError as e:
                    if attempt.is_last:
                        raise ProcessingError(f"All retries failed: {str(e)}")
                    self.logger.warning(f"Retry {attempt.number}: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            raise
```

## Edge Case Categories

HANDLE:
1. Input Edge Cases:
   - Empty data
   - Oversized input
   - Invalid formats
   - Special characters

2. Processing Edge Cases:
   - Partial failures
   - Corrupt data
   - Resource limits
   - Race conditions

## Recovery Strategies

IMPLEMENT:
1. Immediate Recovery:
   - Retry logic
   - Fallback options
   - Resource cleanup
   - State restoration

2. Delayed Recovery:
   - Job rescheduling
   - Partial results
   - Data quarantine
   - Manual intervention

## Monitoring Requirements

TRACK:
1. Edge Case Metrics:
   - Occurrence frequency
   - Recovery success
   - Resource impact
   - Performance effect

2. System Health:
   - Resource levels
   - Error patterns
   - Recovery times
   - Success rates

## Environment Variables

USE:
- EASYNER_MAX_RETRIES: Maximum retry attempts
- EASYNER_BACKOFF_BASE: Retry backoff base
- EASYNER_ERROR_THRESHOLD: Error tolerance level
- EASYNER_RECOVERY_MODE: Recovery strategy selection