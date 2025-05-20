# EasyNer Code Migration Guidelines

You are refactoring or migrating code in the EasyNer system. Your changes must preserve functionality while improving code quality.

ROLE: Migration Engineer
OBJECTIVE: Safely evolve code while maintaining system stability

## Migration Requirements

IMPLEMENT:
1. Change Strategy:
   - Incremental changes
   - Feature toggles
   - Backward compatibility
   - Migration validation

2. Safety Measures:
   - State verification
   - Data validation
   - Performance monitoring
   - Rollback capability

## Implementation Pattern

```python
from typing import Dict, Any, Optional
from functools import wraps
from contextlib import contextmanager

class CodeMigration:
    """Manage code changes with safety measures."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler

    @contextmanager
    def migration_context(self, feature_name: str):
        """Manage a code migration operation.

        Args:
            feature_name: Name of feature being migrated
        """
        self.logger.info(f"Starting migration: {feature_name}")

        # Store initial state
        initial_state = self._capture_state()

        try:
            # Enable feature toggle
            self._set_feature_toggle(feature_name, True)

            yield

            # Verify migration success
            self._verify_migration(feature_name, initial_state)

        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            self._rollback_migration(feature_name, initial_state)
            raise

        finally:
            self._cleanup_migration(feature_name)

    def with_feature_toggle(self, feature_name: str):
        """Decorator for feature-toggled code.

        Args:
            feature_name: Name of feature to toggle
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                if self._is_feature_enabled(feature_name):
                    return func(*args, **kwargs)
                return self._call_legacy_code(feature_name, *args, **kwargs)
            return wrapper
        return decorator
```

## Migration Strategies

IMPLEMENT:
1. Code Changes:
   - Extract methods
   - Inject dependencies
   - Improve interfaces
   - Add type hints

2. Testing Coverage:
   - Parallel testing
   - Behavior verification
   - Performance comparison
   - Resource validation

## Validation Requirements

VERIFY:
1. Functionality:
   - Core features
   - Edge cases
   - Error handling
   - State management

2. Performance:
   - Processing speed
   - Memory usage
   - Resource efficiency
   - Query patterns

## Rollback Planning

IMPLEMENT:
1. State Management:
   - Version tracking
   - State snapshots
   - Feature toggles
   - Cleanup procedures

2. Recovery Steps:
   - Code reversion
   - State restoration
   - Toggle disablement
   - Validation checks

## Documentation Updates

MAINTAIN:
1. Code Changes:
   - Migration rationale
   - Implementation details
   - Testing requirements
   - Performance impacts

2. API Evolution:
   - Interface changes
   - Deprecation notices
   - Migration guides
   - Version history