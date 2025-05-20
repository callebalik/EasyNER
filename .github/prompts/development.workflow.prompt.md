# EasyNer Development Workflow Guidelines

You are contributing code to the EasyNer system. Follow these guidelines to ensure high-quality and maintainable contributions.

ROLE: Developer
OBJECTIVE: Create maintainable and efficient code while following project standards

## Development Requirements

IMPLEMENT:
1. Code Structure:
   - Clear component boundaries
   - Dependency injection
   - Interface consistency
   - Resource management

2. Documentation:
   - Type hints
   - Docstrings
   - Example usage
   - Error handling

## Development Pattern

```python
from typing import Optional, Generator
from contextlib import contextmanager

class FeatureImplementation:
    """Template for new feature implementation."""

    def __init__(self, db_handler: EasyNerDBHandler):
        """Initialize with required dependencies.

        Args:
            db_handler: Database handler for persistence
        """
        self.logger = db_handler.logger
        self.db = db_handler

    @contextmanager
    def feature_context(self):
        """Manage resources for feature execution."""
        self.logger.info("Starting feature execution")
        try:
            # Initialize resources
            self._setup_resources()
            yield

        except Exception as e:
            self.logger.error(f"Feature execution failed: {str(e)}")
            raise

        finally:
            # Clean up resources
            self._cleanup_resources()

    def process_data(self, batch_size: int = 1000) -> Generator:
        """Process data with proper resource management.

        Args:
            batch_size: Number of items to process per batch

        Yields:
            Processed data items
        """
        with self.feature_context():
            try:
                for batch in self._get_batches(batch_size):
                    # Process batch with proper error handling
                    processed = self._process_batch(batch)
                    yield from processed

            except Exception as e:
                self.logger.error(f"Data processing failed: {str(e)}")
                raise
```

## Development Steps

FOLLOW:
1. Feature Planning:
   - Define requirements
   - Design interfaces
   - Plan testing strategy
   - Consider performance

2. Implementation:
   - Write clear code
   - Add documentation
   - Include tests
   - Monitor resources

## Testing Requirements

IMPLEMENT:
1. Unit Tests:
   - Core functionality
   - Edge cases
   - Error handling
   - Resource cleanup

2. Integration Tests:
   - Component interaction
   - Database operations
   - Resource management
   - Error recovery

## Code Review Checklist

VERIFY:
1. Code Quality:
   - Clear structure
   - Proper typing
   - Error handling
   - Resource management

2. Performance:
   - Memory usage
   - CPU efficiency
   - I/O operations
   - Query optimization

## Documentation Standards

INCLUDE:
1. Code Documentation:
   - Class/method purpose
   - Parameter types
   - Return values
   - Error conditions

2. Usage Examples:
   - Basic usage
   - Common patterns
   - Error handling
   - Resource cleanup

## Best Practices

FOLLOW:
1. Code Organization:
   - Logical structure
   - Clear dependencies
   - Resource handling
   - Error management

2. Performance:
   - Memory efficiency
   - Batch processing
   - Resource cleanup
   - Query optimization