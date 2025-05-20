# EasyNer Database Migration Guidelines

You are implementing database migration and schema management for the EasyNer system. Your code must handle large-scale database changes safely.

ROLE: Database Migration Specialist
OBJECTIVE: Create safe, reversible database migrations for large datasets

## Migration Requirements

IMPLEMENT:
1. Schema Changes:
   - Backward compatible changes
   - Incremental modifications
   - Data preservation
   - Rollback capability

2. Data Migration:
   - Batched operations
   - Progress tracking
   - Data validation
   - State verification

## Migration Pattern

```python
from typing import List, Dict, Any
from contextlib import contextmanager

class DatabaseMigration:
    """Manage database migrations with safety checks."""

    def __init__(self, db_handler: EasyNerDBHandler, batch_size: int = 5000):
        self.logger = db_handler.logger
        self.db = db_handler
        self.batch_size = batch_size

    @contextmanager
    def migration_context(self, version: str):
        """Context manager for safe migrations.

        Args:
            version: Target schema version
        """
        self.logger.info(f"Starting migration to version {version}")
        try:
            # Verify current state
            self._verify_current_state()

            # Create checkpoint
            checkpoint = self._create_migration_checkpoint()

            yield checkpoint

            # Verify migration success
            self._verify_migration_success(version)

        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            self._rollback_migration(version)
            raise

    def migrate_data(self, table_name: str, transformer: callable) -> None:
        """Migrate data in batches with progress tracking.

        Args:
            table_name: Name of table to migrate
            transformer: Function to transform data
        """
        try:
            with self.migration_context(f"data_{table_name}") as checkpoint:
                total_rows = self._count_rows(table_name)

                # Process in batches
                for batch in self._get_data_batches(table_name):
                    try:
                        transformed_data = transformer(batch)
                        self._update_batch(table_name, transformed_data)
                        checkpoint.update(batch[-1]['id'])

                    except Exception as e:
                        self.logger.error(f"Batch migration failed: {str(e)}")
                        raise

        except Exception as e:
            self.logger.error(f"Data migration failed for {table_name}: {str(e)}")
            raise
```

## Safety Requirements

VERIFY:
1. Pre-migration:
   - Database consistency
   - Schema compatibility
   - Resource availability
   - Backup existence

2. During Migration:
   - Data integrity
   - Progress tracking
   - Error handling
   - Resource usage

3. Post-migration:
   - Schema validation
   - Data verification
   - Performance checks
   - Backup retention

## Rollback Strategy

IMPLEMENT:
1. Schema Rollback:
   - Version tracking
   - Structure restoration
   - Index rebuilding
   - Constraint validation

2. Data Rollback:
   - Checkpoint restoration
   - State verification
   - Relationship validation
   - Progress logging

## Performance Guidelines

OPTIMIZE:
1. Migration Speed:
   - Efficient indexing
   - Optimal batch size
   - Transaction management
   - Resource allocation

2. Resource Usage:
   - Memory management
   - Transaction size
   - I/O optimization
   - CPU utilization

## Environment Variables

USE:
- EASYNER_MIGRATION_BATCH: Migration batch size
- EASYNER_BACKUP_PATH: Backup location
- EASYNER_SCHEMA_VERSION: Target version
- EASYNER_CHECKPOINT_DIR: Checkpoint location