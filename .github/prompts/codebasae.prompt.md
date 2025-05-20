## Code Architechture
- We use composition and injection to properly separate funtionality into testable and reusable classes and methods
- Data processing steps are implemented as separate methods and classes, or submethods to be uncoupled and easily debuggable.
- Lib/sqlite3 is used see https://docs.python.org/3/library/sqlite3.html#sqlite3-connection-context-manager
- Performance i crucial. Database is very large over 300 million rows in total. Most commonly accessed table has 58 million rows.
- Use schema definitions from schema.py for table/column references and validating SQL statements
- Implement atomic operations
- Parameterize all queries to prevent SQL injection
- We default to idempotency, adding option to overwrite were relevant.
- Idempotency and integrity should be tested and row counts and relationsships verified after operations for referential integrity after updates, if possible reusable modular tests.

## Server Guidelines
- We use the server as a GUI for interaction and data inspection
- Reuse methods from other packages rather than creating new functionality
- When creating javascript methods we try to make them resuable by other pages
- Use DBDataExchanger and data_model module for data operations
- We write CSS styling as SCSS in partials, imported via central styling.scss
- Ensure thread safety and proper connection closing

## Syntax
-- We prefix python sql f-string statements with "--sql " for added syntax highlighting
