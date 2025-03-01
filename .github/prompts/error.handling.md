# EasyNer Error Handling & Logging

You are implementing error handling and logging for the EasyNer system. Your code must provide comprehensive error tracking and recovery mechanisms.

ROLE: Error Handler & Logger
OBJECTIVE: Implement robust error handling and logging for a large-scale NER system

## Logging Requirements

INPUT CONTEXT:
- Operation type
- Data identifiers
- Resource state
- Performance metrics

SEVERITY LEVELS:
DEBUG:
- Detailed operation progress
- Resource usage stats
- Batch processing metrics
- Cache hit/miss rates

INFO:
- Batch operation completion
- Resource initialization
- Configuration loading
- Normal state transitions

WARNING:
- Performance degradation
- Resource pressure
- Recoverable errors
- Data inconsistencies

ERROR:
- Operation failures
- Resource exhaustion
- Data corruption
- Unrecoverable states

CRITICAL:
- System crashes
- Data loss risk
- Security breaches
- Infrastructure failures

## Implementation Pattern
```python
def process_operation(self, operation_id: str, data: Any) -> None:
    """Process operation with comprehensive error handling.

    Args:
        operation_id: Unique identifier for the operation
        data: Data to process

    Raises:
        OperationError: If processing fails
    """
    self.logger.info(f"Starting operation {operation_id}")
    try:
        self._validate_input(data)
        self.logger.debug(f"Input validation passed for {operation_id}")

        result = self._execute_operation(data)
        self.logger.info(f"Operation {operation_id} completed successfully")

    except ValidationError as e:
        self.logger.error(f"Validation failed for {operation_id}: {str(e)}")
        raise OperationError(f"Input validation failed: {str(e)}")

    except ResourceError as e:
        self.logger.critical(f"Resource failure in {operation_id}: {str(e)}")
        self._emergency_cleanup()
        raise

    except Exception as e:
        self.logger.error(f"Unexpected error in {operation_id}: {str(e)}")
        raise

    finally:
        self._cleanup_resources()
        self.logger.debug(f"Resource cleanup completed for {operation_id}")
```

## Error Recovery Strategy

IMPLEMENT:
1. Operation Checkpointing:
   - Save progress regularly
   - Track completed batches
   - Store intermediate states
   - Enable operation resume

2. Resource Management:
   - Track resource allocation
   - Implement cleanup handlers
   - Monitor resource limits
   - Handle cleanup failures

3. Data Integrity:
   - Validate state transitions
   - Verify data consistency
   - Track data dependencies
   - Handle partial failures

4. Recovery Actions:
   - Implement rollback logic
   - Restore consistent state
   - Clean up resources
   - Log recovery steps

ALWAYS INCLUDE:
- Context information
- Stack traces
- Resource states
- Recovery actions
- Cleanup confirmation