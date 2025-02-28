import functools
import os
import signal
import sys
import logging
import json
import sqlite3
import time
from typing import Optional

def db_error_handler(method):
    """Decorator to handle database errors consistently"""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except sqlite3.Error as e:
            self.logger.error(f"Database error in {method.__name__}: {str(e)}")
            self.logger.debug(f"Args: {args}, Kwargs: {kwargs}")
            raise
        except Exception as e:
            self.logger.error(f"Error in {method.__name__}: {str(e)}")
            raise
    return wrapper

class EasyNerDBHandler:
    def __init__(self, db_path: Optional[str] = None, config_path: str = "../../config.json"):
        """
        Initialize the database handler.

        :param db_path: Path to the SQLite database file.

        """

        # Load config and setup paths first
        self.config = self._load_config(config_path)
        self.db_path, self.path_source = self._setup_path(db_path=db_path)
        self.name = os.path.basename(self.db_path)

        # Now set up the full logging system with proper file paths
        # Can't use @db_error_handler before logging is set up
        self._setup_logging()

        # Connect to the database
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()  # Ensure cursor is an attribute
        self.cursor.row_factory = sqlite3.Row  # Return rows as dictionaries for easy access


        # Ensure cache table exists
        self._init_cache()

        # Load environment settings and store in cache
        self._load_environment_settings()

        # Initialize components
        self._initialize_components()

        # Log and print connection info
        try:
            from scripts.utils.log_formatter import TableFormatter

            log_config = {
                'Database': self.name,
                'Path': self.db_path,
                'Path source': self.path_source,
                'Main log (INFO)': self.log_file,
                'Error log (ERRORS only)': self.error_log_file,
                'Debug log (FULL DEBUG)': self.debug_log_file,
                'Row-factory': self.cursor.row_factory,
                'Journal Mode': self._get_pragma_value("journal_mode"),
                'Busy Timeout': self._get_pragma_value("busy_timeout"),
                'Synchronous': self._get_pragma_value("synchronous"),
                'Foreign keys': self._get_pragma_value("foreign_keys"),
                'Journal size limit': self._get_pragma_value("journal_size_limit"),
                'Max parameter count': self._get_pragma_value("max_variable_number"),
                'Environment settings': self.cache_manager.get_global("environment_settings"),
                'Mapped I/O (> 1 -> True)': self._get_pragma_value("mmap_size"),
            }

            table = TableFormatter.format_table(log_config, title="Database connection initialized")
            self.logger.info(f"\n{table}")
            print(table)
        except ImportError:
            # Fallback to standard logging if TableFormatter is not available
            self.logger.info(
                f"Logging system initialized - DB: {self.name}"
                f"\n Main log - (INFO): {self.log_file}"
                f"\n Path: {self.db_path}"
                f"\n Path source: {self.path_source}"
                f"\n Error log - (ONLY ERRORS) - Resets: {self.error_log_file}"
                f"\n Debug log - (FULL DEBUG LOG): {self.debug_log_file}"
            )



    # Support for context manager protocol
    def __enter__(self):
        """
        Context manager entry point - provides direct access to database operations.

        Returns:
            self: The database handler instance
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit point - handles cleanup with proper error handling.

        Args:
            exc_type: Exception type if an exception was raised, otherwise None
            exc_value: Exception value if an exception was raised, otherwise None
            traceback: Traceback if an exception was raised, otherwise None
        """
        if exc_type:
            self.logger.error(f"Exception during database operation: {exc_type.__name__}: {exc_value}")
            # Attempt rollback for database integrity errors
            if issubclass(exc_type, sqlite3.Error):
                try:
                    self.logger.info("Rolling back transaction due to database exception")
                    self.rollback()
                except Exception as e:
                    self.logger.error(f"Error during rollback: {str(e)}")

        # Always perform clean WAL checkpoint and close connection
        try:
            # Force WAL checkpoint to prevent database corruption
            self.execute("PRAGMA wal_checkpoint(FULL);")
            self.close()
            self.logger.debug("Database connection closed cleanly")
        except Exception as e:
            self.logger.error(f"Error during connection cleanup: {str(e)}")

        # Don't suppress exceptions
        return False

    def _setup_path(self, db_path: Optional[str]) -> None:
        # Database path selection with clear precedence:
        # 1. DB_PATH environment variable
        # 2. Development mode default path (if config["develop"]=True)
        # 3. Explicitly provided db_path parameter
        # 4. Path from config file

        path_source = None
        env_db_path = os.getenv("DB_PATH")
        resolved_path = None
        if env_db_path:
            resolved_path = env_db_path
            path_source = "DB_PATH environment variable"
        elif self.config.get("develop", False):
            pwd = os.path.dirname(os.path.abspath(__file__))
            resolved_path = os.path.join(pwd, "development.db")
            path_source = "development mode default path"
        elif db_path is not None:
            resolved_path = db_path
            path_source = "explicitly provided path"
        else:
            resolved_path = self.config.get("db_path")
            path_source = "config file"

        # Ensure the path is absolute and resolved
        if not resolved_path:
            raise ValueError("Database path could not be resolved")

        if not os.path.isabs(resolved_path):
            raise ValueError("Database path must be absolute")

        return resolved_path, path_source

    @db_error_handler
    def _init_cache(self):
        """Initialize the cache table and set up the global cache manager singleton"""
        try:
            # Import here to avoid circular imports
            from .core.cache_manager import CacheManager
            from .core.cache_singleton import set_cache_manager_connection

            # Create a local instance for this db_handler
            self.cache_manager = CacheManager(db_handler=self)

            # Also initialize the global singleton cache manager
            set_cache_manager_connection(self.conn, self.cursor, self.logger)

        except ImportError as e:
            self.logger.warning(f"Failed to initialize cache system: {e}")

    @db_error_handler
    def _load_environment_settings(self):
        """Load settings from environment variables"""
        env_settings = {}

        # Batch size for processing
        try:
            env_batch_size = os.getenv("DEFAULT_BATCH_SIZE")
            if env_batch_size:
                env_settings["batch_size"] = int(env_batch_size)
        except ValueError:
            self.logger.warning(f"Invalid DEFAULT_BATCH_SIZE value: {os.getenv('DEFAULT_BATCH_SIZE')}")
            env_settings["batch_size"] = 32

        # Log level
        env_log_level = os.getenv("LOG_LEVEL")
        if env_log_level:
            valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            if env_log_level.upper() in valid_levels:
                env_settings["log_level"] = env_log_level.upper()
                # Update logger level
                level = getattr(logging, env_log_level.upper())
                self.logger.setLevel(level)
                self.logger.info(f"Set log level to {env_log_level.upper()} from environment variable")

        # Store settings in cache if we have a cache manager
        if hasattr(self, 'cache_manager'):
            self.cache_manager.set_global("environment_settings", env_settings)

        return env_settings

    @db_error_handler
    def _initialize_components(self):
        """
        Initialize database components with proper composition pattern.

        All components are attached directly to this instance for easy access:
            db.ne.aggregator.method()
            db.data_cleaner.method()

        Components receive 'self' as dependency to access conn, cursor, logger.
        """
        try:
            # Import component modules
            from .db_data_exchanger import DBDataExchanger
            from .db_data_cleaner import DBDataCleaner
            from .db_statistics import DBStatistics
            from .data_model.entity_occurrence import EntityOccurrence
            from .data_model.entity_cooccurrence import EntityCooccurrence

            # Initialize data_exchanger first (other components depend on it)
            self.data_exchanger = DBDataExchanger(self.conn, self.cursor, self.logger)

            # Initialize primary components
            self.data_cleaner = DBDataCleaner(
                self.conn, self.cursor, self.logger, self.data_exchanger, config=self.config
            )


            self.statistics = DBStatistics(
                self.conn, self.cursor, self.logger, self.data_exchanger
            )

            # Initialize entity handling components with direct reference to self
            # This maintains the desired db.ne.component.method() pattern
            self.ne = EntityOccurrence(self)
            self.co = EntityCooccurrence(self)

        except ImportError as e:
            self.logger.error(f"Component initialization failed: {str(e)}")
            raise

    @property
    def conn_params_dict(self):
        """
        Returns the connection parameters as a dictionary for Reader/Writer classes.
        """
        return {"database": self.db_path} # Return connection parameters as dict

    def _analyze_database(self, table_name: str = None):
        """
        Analyze the database to update the query planner statistics.
        """
        if table_name is None:
            self.execute("ANALYZE;")
        else:
            self.execute(f"ANALYZE {table_name};")

    def _set_default_settings(self):
        """
        Set default settings for the database connection.
        Only call on a newly created database.
        """
        self.execute("PRAGMA foreign_keys = ON;")
        self.execute("PRAGMA journal_mode = WAL;")
        self.execute("PRAGMA synchronous = NORMAL;")
        self.execute("PRAGMA journal_size_limit = 6144000;")
        self.execute("PRAGMA temp_store = MEMORY;")
        self.execute("PRAGMA busy_timeout = 10000;")


    def _load_config(self, config_path):
        """Load the JSON configuration file."""
        if not os.path.isabs(config_path):
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), config_path
            )
        try:
            with open(config_path, "r") as f:
                config = json.load(f).get("database", {})
                # Test integrity of the configuration
                if not config["develop"]:  # No db path neeeded when in development mode
                    if "db_path" not in config:
                        raise ValueError(
                            "Database configuration must contain 'db_path' key."
                        )
                else:
                    print(
                        "Setting up in development mode, ignoring database path provided"
                    )
                if "schema_path" not in config:
                    raise ValueError(
                        "Database configuration does not contain 'schema_path' key."
                    )
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise ValueError(f"Error loading configuration file: {e}")

        return config

    def _setup_logging(self):
        """Configure logging to save to db.log in the database directory."""
        self.logger = logging.getLogger("EasyNerDB")

        # Clear temporary console handlers - Fixed: Added parentheses to removeHandler method call
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        self.logger.setLevel(logging.INFO)

        # Get absolute path of the database file parent directory
        db_parent_dir = os.path.dirname(os.path.abspath(__file__))

        # Ensure the logs directory exists and is writable
        log_dir = os.path.join(db_parent_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)

        # Verify if the directory was actually created
        if not os.path.exists(log_dir):
            raise IOError(f"Failed to create log directory: {log_dir}")

        # Verify write permissions with verbose error
        if not os.access(log_dir, os.W_OK):
            self.logger.error(f"No write permission for log directory: {log_dir}")
            print(f"ERROR: No write permission for log directory: {log_dir}")
            raise PermissionError(f"No write permission for log directory: {log_dir}")

        # Store log file paths as instance variables
        self.log_file = os.path.join(log_dir, self.name + ".log")
        self.error_log_file = os.path.join(log_dir, self.name + ".err")
        self.debug_log_file = os.path.join(log_dir, self.name + ".debug.log")

        # Create file handler for logging
        try:
            log_file_handler = logging.FileHandler(self.log_file)
            log_file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter(
                "%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s"
            )
            log_file_handler.setFormatter(file_formatter)

            # Create console handler for logging
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.getLevelName(os.getenv("LOGGING_LEVEL_CONSOLE", "ERROR")))
            console_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(console_formatter)

            # Create separate error file handler for logging errors
            error_file_handler = logging.FileHandler(self.error_log_file)
            error_file_handler.setLevel(logging.ERROR)
            error_file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s"
            )
            error_file_handler.setFormatter(error_file_formatter)

            debug_file_handler = logging.FileHandler(self.debug_log_file)
            debug_file_handler.setLevel(logging.DEBUG)
            debug_file_handler.setFormatter(file_formatter)

            self.logger.addHandler(log_file_handler)
            self.logger.addHandler(console_handler)
            self.logger.addHandler(error_file_handler)
            self.logger.addHandler(debug_file_handler)

        except Exception as e:
            print(f"ERROR: Failed to set up logging handlers: {e}")
            raise

    def __del__(self):
        """
        Close the database connection when the handler is deleted.
        """
        if hasattr(self, "conn"):
            self.conn.close()

    def close(self):
        """
        Explicitly close the database connection.
        """
        if hasattr(self, "conn"):
            self.conn.close()

    def _delete_views(self, view: str = None):
        """
        Delete views from the database. If no view is provided, all views are deleted.
        Prompts the user for confirmation before deleting.

        Args:
            view (str, optional): Name of the view to delete
        """
        confirmation = input("Are you sure you want to delete the views? (y/n): ")
        if confirmation.lower() != "y":
            print("Operation cancelled.")
            return

        if view is None:
            views = self.views["views"]
            for view in views:
                self.cursor.execute(f"DROP VIEW IF EXISTS {view};")
        else:
            self.cursor.execute(f"DROP VIEW IF EXISTS {view};")
        self.conn.commit()

    def _empty_database_(self):
        """
        Empty the database.
        """
        confirmation = input("Are you sure you want to empty the database? (y/n): ")
        if confirmation.lower() != "y":
            print("Operation cancelled.")
            return
        self.cursor.execute("PRAGMA foreign_keys = OFF;")
        for table in self.tables["tables"]:
            self.cursor.execute(f"DELETE FROM {table};")
        self.cursor.execute("PRAGMA foreign_keys = ON;")
        self.conn.commit()
        self.cursor.execute("VACUUM;")
    def _backup_database_(self, backup_path: str = None):
        """
        Backup the database to a specified path.

        Args:
            backup_path (str, optional): Path where to save the backup.
                                    If None, appends '.backup' to current db path.
        """
        if backup_path is None:
            backup_path = self.db_path + ".backup"
        else:
            if not os.path.isabs(backup_path):
                backup_path = os.path.join(
                    os.path.dirname(self.db_path), backup_path
                )

        self.logger.info(f"Backing up database to {backup_path}")

        try:
            # Open the backup destination
            with sqlite3.connect(backup_path) as dest_conn:
                # Perform the backup
                self.conn.backup(dest_conn)

            self.logger.info(f"Database backed up successfully to {backup_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Backup failed: {str(e)}")
            raise

        except KeyboardInterrupt:
            self.logger.error("Backup interrupted.")
            raise

    def _log_query_plan(self, sql, params: dict = None):
        """
        Executes a query, logs its query plan, and returns the results.
        """
        try:
            if params:
                self.cursor.execute(f"EXPLAIN QUERY PLAN {sql}", params)
            else:
                self.cursor.execute(f"EXPLAIN QUERY PLAN {sql}")

            plan = self.cursor.fetchall()
            s = f"EXPLAIN QUERY PLAN {sql};\n"
            for step in plan:
                s += str(dict(step)) + "\n"
            self.logger.debug(s)

        except sqlite3.Error as e:
            print(f"SQLite error: {e} while logging query plan.")
            return None

    def execute(self, query, args=None):
        """
        Execute a SQL query.

        :param query: The SQL query to execute.
        :param args: Optional arguments for the SQL query.
        :return: The result of the query.
        """
        try:
            if args is None:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, args)

            # For SELECT queries, return the results
            if query.lstrip().upper().startswith(
                "SELECT"
            ) or query.lstrip().upper().startswith("PRAGMA"):
                return self.cursor.fetchall()
            # For other queries (INSERT, UPDATE, DELETE, etc.), commit and return empty list
            else:
                self.conn.commit()
                return []
        except Exception as e:
            self.logger.error(
                f"Error executing query: {query} with args: {args}. Exception: {e}"
            )
            raise

    def execute_with_log(self, query, args=None):
        """
        Execute a SQL query and log the query plan.

        :param query: The SQL query to execute.
        :param args: Optional arguments for the SQL query.
        :return: The result of the query.
        """
        self._log_query_plan(query, args)

        if args is None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, args)

    def commit(self):
        """
        Commit the current transaction.
        """
        self.conn.commit()

    def rollback(self):
        """
        Rollback the current transaction, so that the changes made in the current transaction are not saved.
        """
        self.conn.rollback()

    def fetchall(self):
        """
        Fetch all the rows from the last executed query.

        :return: A list of all rows from the last executed query.
        """
        return self.cursor.fetchall()

    def fetchone(self):
        """
        Fetch one row from the last executed query.

        :return: A single row from the last executed query.
        """
        return self.cursor.fetchone()

    def fetchmany(self, size):
        """
        Fetch a specified number of rows from the last executed query.

        :param size: The number of rows to fetch.
        :return: A list of rows from the last executed query.
        """
        return self.cursor.fetchmany(size)

    def fetchall_dict(self):
        """
        Fetch all rows from the last executed query as a list of dictionaries.

        :return: A list of dictionaries representing the rows.
        """
        rows = self.fetchall()
        if len(rows) == 0:
            return []
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def fetchone_dict(self):
        """
        Fetch one row from the last executed query as a dictionary.

        :return: A dictionary representing the row.
        """
        row = self.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in self.cursor.description]
        return dict(zip(columns, row))

    def fetchmany_dict(self, size):
        """
        Fetch a specified number of rows from the last executed query as a list of dictionaries.

        :param size: The number of rows to fetch.
        :return: A list of dictionaries representing the rows.
        """
        rows = self.fetchmany(size)
        if len(rows) == 0:
            return []
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def fetchall_dict_list(self):
        """
        Fetch all rows from the last executed query as a list of dictionaries.

        :return: A list of dictionaries representing the rows.
        """
        rows = self.fetchall()
        if len(rows) == 0:
            return []
        columns = [desc[0] for desc in self.cursor.description]
        return [{columns[i]: row[i] for i in range(len(columns))} for row in rows]

    @property
    def tables(self) -> dict:
        """
        Get information about the database.

        :return: A dictionary containing information about the database.
        """

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.fetchall()
        tables = [table[0] for table in tables]
        return {"tables": tables}

    @property
    def views(self) -> dict:
        """
        Get information about the views in the database.

        :return: A dictionary containing information about the views in the database.
        """
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='view';")
        views = self.fetchall()
        views = [view[0] for view in views]
        return {"views": views}

    def optimize_db_performance_parameters(self):
        # Set WAL mode, synchronous=OFF, and journal_mode=MEMORY
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_size_limit = 6144000")
        self.conn.execute("PRAGMA temp_store = MEMORY")

    def optimize_db_performance_for_write(self):
        # Set WAL mode, synchronous=OFF, and journal_mode=MEMORY
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_size_limit = 6144000")
        self.conn.execute("PRAGMA temp_store = MEMORY")
        print("Database performance parameters optimized for write operations.")

    def optimize_db_performance_for_read(self):
        # Set WAL mode, synchronous=OFF, and journal_mode=MEMORY
        self.conn.execute("PRAGMA journal_mode = MEMORY")
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_size_limit = 6144000")
        self.conn.execute("PRAGMA temp_store = MEMORY")

    def align_with_schema(self, schema_path=None):
        """
        Ensure the database schema matches the defined schema.
        Missing tables are created and missing columns are added.

        :param schema_path: Path to the schema file. If not provided, a prompt will ask for it.
        """
        if schema_path is None:
            schema_path = input("Please provide the path to the schema file: ")
        if not os.path.isabs(schema_path):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            schema_path = os.path.join(script_dir, schema_path)

        # Load the schema into an in-memory SQLite database
        schema_conn = sqlite3.connect(":memory:")
        schema_cursor = schema_conn.cursor()
        with open(schema_path, "r") as f:
            schema_sql = f.read()
        schema_cursor.executescript(schema_sql)

        # Get list of tables defined in the schema
        schema_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        schema_tables = [row[0] for row in schema_cursor.fetchall()]

        all_tables_exist = True
        for table in schema_tables:
            if table == "sqlite_sequence":
                continue  # Skip the sqlite_sequence table

            # Get columns from the schema for this table
            schema_cursor.execute(f"PRAGMA table_info('{table}');")
            schema_columns_info = schema_cursor.fetchall()
            # Using a dict for easy lookup (col_name -> info tuple: (cid, name, type, notnull, dflt_value, pk))
            schema_columns = {col[1]: col for col in schema_columns_info}

            # Check if the table exists in the current database
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (table,),
            )
            if self.cursor.fetchone() is None:

                # Table missing – reconstruct a CREATE TABLE using schema_columns info
                cols = []
                for col in schema_columns_info:
                    # Build a simple column definition using name and type; note that ALTER TABLE
                    # has limitations with constraints so this is a basic reconstruction.
                    col_def = f"{col[1]} {col[2]}"
                    if col[3]:
                        col_def += " NOT NULL"
                    if col[4] is not None:
                        col_def += f" DEFAULT {col[4]}"
                    if col[5]:
                        col_def += " PRIMARY KEY"
                    cols.append(col_def)
                create_stmt = f"CREATE TABLE {table} ({', '.join(cols)});"
                print(f"Creating missing table: {table}")
                self.cursor.execute(create_stmt)
                all_tables_exist = False
            else:
                # Table exists; check for missing columns
                self.cursor.execute(f"PRAGMA table_info('{table}');")
                db_columns_info = self.cursor.fetchall()
                db_columns = {col[1]: col for col in db_columns_info}
                for col_name, col_info in schema_columns.items():
                    if col_name not in db_columns:
                        # ALTER TABLE can only add the column name and type; additional constraints might not be restored.
                        add_stmt = (
                            f"ALTER TABLE {table} ADD COLUMN {col_name} {col_info[2]}"
                        )
                        print(f"Adding missing column '{col_name}' to table '{table}'")
                        self.cursor.execute(add_stmt)
                        all_tables_exist = False
        if all_tables_exist:
            print("All tables and columns exist and are up-to-date.")
        self.conn.commit()
        schema_conn.close()

    @property
    def is_locked(self) -> bool:
        """
        Check if the database is locked.

        :return: True if the database is locked, False otherwise.
        """
        try:
            self.conn.execute("PRAGMA wal_checkpoint;")
            return False
        except sqlite3.OperationalError:
            return True

    def clear_connections(self):
        """
        Attempt to clear any pending transactions and unlock the database
        by forcing a WAL checkpoint and closing the connection.
        Use with caution.
        """
        try:
            # Try committing any pending changes
            self.conn.commit()
        except Exception as e:
            print(f"Error committing pending transactions: {e}")

        try:
            # Force a write-ahead log checkpoint
            self.conn.execute("PRAGMA wal_checkpoint(FULL);")
            self.conn.commit()
        except Exception as e:
            print(f"Error executing WAL checkpoint: {e}")
        finally:
            # Close the connection cleanly
            self.conn.close()
            print("Database connection closed and unlocked.")

    @classmethod
    def create_db(self, db_path, schema_path):
        # Ensure the schema path is absolute; if not, resolve it relative to current script
        if not os.path.isabs(schema_path):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            schema_path = os.path.join(script_dir, schema_path)

        with open(schema_path, "r") as f:
            sql_script = f.read()

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.executescript(sql_script)
        conn.commit()
        conn.close()
        print(f"Database created at {db_path} using schema from {schema_path}")

    def _get_pragma_value(self, pragma_name):
        """
        Safely retrieve a PRAGMA value from SQLite with proper error handling.

        Args:
            pragma_name (str): The name of the pragma to retrieve (without the 'PRAGMA' keyword)

        Returns:
            The pragma value, or a default/placeholder if the pragma is not available
        """

        try:
            # Execute the PRAGMA query without modifying any settings
            result = self.execute(f"PRAGMA {pragma_name};")
            if result and len(result) > 0 and len(result[0]) > 0:
                return result[0][0]
            return "Not available"  # Return a placeholder if result is empty or malformed

        except sqlite3.Error as e:
            self.logger.warning(f"SQLite error getting pragma {pragma_name}: {str(e)}")
            return f"Not available (SQLite error: {str(e)[:30]})"

        except Exception as e:
            self.logger.debug(f"Error getting pragma {pragma_name}: {str(e)}")
            return "Not available"  # Return a placeholder on error

class BaseComponent:
    """Common base class for all components with shared logger and database connection."""
    def __init__(self, db_handler: EasyNerDBHandler):
        # Direct attribute access from the database handler
        self.logger = db_handler.logger
        self.cursor = db_handler.cursor
        self.conn = db_handler.conn
        self.log_query_plan = db_handler._log_query_plan
        self.conn_params_dict = db_handler.conn_params_dict
        self.data = db_handler.data_exchanger
        self.db = db_handler  # Keep a direct reference to the database handler

        # Optional initialization hook for subclasses
        self._initialize()

    def _initialize(self):
        """Hook for subclasses to perform additional initialization"""
        pass

    # Remove init_deps as it's redundant with proper inheritance

