# EasyNer Development Guidelines Index

This index organizes the project guidelines into focused areas for better context management and reduced query overhead.

## Getting Started
- [Contribution Guidelines](contribution.guidelines.md)
  - Development workflow
  - Code standards
  - Review process
  - Getting started guide

## Core Guidelines
1. [Database Operations](database.operations.md)
   - Database practices, validation, performance
   - SQLite3 operations for large datasets
   - Atomic transactions and batching

2. [Code Architecture](code.architecture.md)
   - Design principles and patterns
   - Error handling and logging
   - Code organization
   - Best practices

3. [Performance Optimization](performance.optimization.md)
   - Memory efficiency
   - Processing strategies
   - Database optimization
   - Profiling and benchmarking

4. [Testing Guidelines](testing.guidelines.md)
   - Test organization and structure
   - Database testing strategies
   - Performance testing methods
   - Quality assurance standards

## Environment & Deployment
5. [Environment Compatibility](environment.compatibility.md)
   - Environment support (HPC/Local)
   - Configuration management
   - Environment variables
   - Error recovery

6. [Server Guidelines](server.guidelines.md)
   - Server architecture
   - Frontend development
   - Styling guidelines
   - Security and performance

## Quick References

### Common Environment Variables
- EASYNER_DB_PATH: Database location
- EASYNER_LOG_LEVEL: Logging verbosity
- EASYNER_BATCH_SIZE: Processing batch size
- FLASK_APP/FLASK_ENV: Server configuration

### Key Practices
- Batch size: 1000-5000 records
- SQL prefix: "--sql " for f-strings
- Logging: Use EasyNerDBHandler
- Database: SQLite3 (~50M rows)
- Testing: test_{feature}_{scenario} naming
- Branches: feature/, bugfix/, hotfix/

### File Organization
- SCSS: Use partials, import via styling.scss
- Python: Modular design with clear separation
- Tests: Unit + integration coverage
- Config: Environment-specific settings
- Documentation: Per-module README files