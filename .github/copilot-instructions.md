# EasyNer Project Guidelines

## Database Practices
- Use SQLite3 for database operations (tables contain ~50M rows)
- Use schema definitions from schema.py for table/column references
- Implement atomic transactions for batch integrity
- Parameterize all queries to prevent SQL injection
- Verify SQL statements against schema definitions
- We default to idempotency, adding option to overwrite were relevant

## Code Architechture
- We use composition and injection to properly separate funtionality into testable and reusable classes and methods
- Data processing steps are implemented as separate methods, or submethods to be uncoupled and easily debuggable.

## Database Validation
- Create reusable validation methods that can be run in multiple contexts
- Include tests for referential integrity after updates
- Verify expected row counts and relationships after batch operations
- Validation methods should be callable from both interactive sessions and automated processes
- Log validation results with appropriate severity levels

## Performance & Resources
- Memory efficiency is critical - prefer generators and batched operations
- Implement proper cleanup to prevent resource leaks
- Process data in batches (recommended size: 1000-5000 records)

## Error Handling & Logging
- Use central logging via EasyNerDBHandler (self.logger)
- Log exceptions before re-raising them
- Include context in log messages (file/record IDs, operation type)
- Use appropriate log levels based on severity

## Environment Compatibility
- Support both interactive and SLURM batch environments on HPC
- Key environment variables:
  - EASYNER_DB_PATH: path to database
  - EASYNER_LOG_LEVEL: controls logging verbosity
  - EASYNER_BATCH_SIZE: configures processing batch size
- Include user interrupt handling with checkpointing

## Server Guidelines
- We use the server as a GUI for interaction and data inspection
- Reuse methods from other packages rather than creating new functionality
- When creating javascript methods we try to make them resuable by other pages
- Use DBDataExchanger and data_model module for data operations
- We write CSS styling as SCSS in partials, imported via central styling.scss
- Ensure thread safety and proper connection closing

