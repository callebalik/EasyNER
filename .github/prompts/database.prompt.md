# EasyNer Database Operations

You are implementing database operations for the EasyNer system. Your code will handle large-scale SQLite operations with critical performance requirements.

GIVEN:
- Database Type: SQLite3
- Data Volume: ~50M rows
- Environment: Multi-threaded HPC
- Performance: Critical

TASK REQUIREMENTS:
When writing database code, you MUST:

## Query Construction
1. ALWAYS:
   - Prefix SQL f-strings with "--sql "
   - Use parameterized queries
   - Validate against schema.py
   - Implement ACID compliance

2. NEVER:
   - Use string concatenation for queries
   - Leave connections open
   - Perform unbatched operations
   - Skip transaction boundaries

## Resource Management
USE:
```python
with self.db.transaction() as tx:
    # Your code here
```

AVOID:
```python
cursor.execute("INSERT INTO..." % values)  # NO string formatting
```

## Batch Processing Pattern
```python
def process_in_batches(self, data: Iterator[Any], batch_size: int = 1000):
    """Process data in efficient batches.

    Args:
        data: Iterator of records to process
        batch_size: Number of records per batch
    """
    batch = []
    try:
        for item in data:
            batch.append(item)
            if len(batch) >= batch_size:
                self._process_batch(batch)
                batch = []
    finally:
        if batch:
            self._process_batch(batch)
```

## Validation Requirements
IMPLEMENT:
1. Pre-operation checks:
   - Schema validation
   - Data type verification
   - Constraint checking

2. Post-operation validation:
   - Row count verification
   - Referential integrity
   - Data consistency

3. Error handling:
   - Detailed error messages
   - Appropriate logging
   - Clean rollback

## Performance Guidelines
OPTIMIZE:
- Use appropriate indexes
- Implement batch sizes (1000-5000)
- Monitor memory usage
- Implement proper cleanup
- Enable WAL mode for concurrent access

MEASURE:
- Query execution time
- Memory consumption
- Transaction overhead
- Cleanup efficiency