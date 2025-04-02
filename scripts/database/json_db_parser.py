"""
This script is used to convert between JSON and SQL, Import and Export

It implemented for high import and export speed, and low memory usage as some datasets
such as PubMed can be very large.

The backend is Sqlite3, which is a lightweight database engine that is built into Python.

Import:
- Validate the JSON file against a schema
- Maps the JSON data to the SQL schema
- Multihreaded import for performance
- Uses a connection pool for efficient database access
- Supports batch processing for large datasets
- Supports incremental import for large datasets
- Supports multiple JSON files for import
- Provides detailed logging for import processes
- Provides progress tracking for import processes
- Provides error handling for import processes
- Implements data validation and sanitization

Export:
- Exports to standard JSON format
- Specify document IDs to export
- Specify fields to export
- Supports incremental export for large datasets
"""
