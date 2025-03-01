# EasyNer Database Operations Guidelines

## Database Practices
- Use SQLite3 for database operations (tables contain ~50M rows)
- Use schema definitions from schema.py for table/column references
- Implement atomic transactions for batch integrity
- Parameterize all queries to prevent SQL injection
- Verify SQL statements against schema definitions
- Default to idempotent operations, adding option to overwrite where relevant
- Prefix SQL f-string statements with "--sql " for syntax highlighting

## Database Validation
- Create reusable validation methods that can be run in multiple contexts
- Include tests for referential integrity after updates
- Verify expected row counts and relationships after batch operations
- Validation methods should be callable from both interactive sessions and automated processes
- Log validation results with appropriate severity levels

## Performance Considerations
- Process data in batches (recommended size: 1000-5000 records)
- Use generators for memory-efficient data processing
- Implement proper connection cleanup to prevent resource leaks
- Consider indexing strategy for frequent query patterns
- Use transactions for related operations to maintain data integrity

## Error Handling
- Log database exceptions before re-raising them
- Include context in log messages (table names, operation type, record counts)
- Handle constraint violations and other common SQLite errors gracefully
- Implement appropriate retry logic for transient failures