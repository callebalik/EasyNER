# EasyNer Code Architecture Guidelines

## Design Principles
- Use composition and injection to properly separate functionality
- Create testable and reusable classes and methods
- Implement data processing steps as separate methods or submethods
- Design for uncoupled components that are easily debuggable
- Favor immutable data structures where appropriate

## Error Handling & Logging
- Use central logging via EasyNerDBHandler (self.logger)
- Log exceptions before re-raising them
- Include context in log messages (file/record IDs, operation type)
- Use appropriate log levels based on severity:
  - DEBUG: Detailed information, typically for diagnostics
  - INFO: Confirmation that things are working as expected
  - WARNING: Indication that something unexpected happened
  - ERROR: Due to a more serious problem, some functionality is unavailable
  - CRITICAL: Serious errors causing program to abort

## Testing Strategy
- Write unit tests for individual components
- Create integration tests for database workflows
- Implement validation utilities that can be called from tests
- Test edge cases, especially for large batch operations
- Consider performance testing for critical paths

## Code Organization
- Follow consistent naming conventions
- Use meaningful variable and function names
- Group related functionality in modules
- Document public interfaces and complex algorithms
- Include type hints to improve code clarity
- Split complex operations into smaller, focused methods