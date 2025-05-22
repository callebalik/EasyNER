# pyright: reportOptionalMemberAccess=false
import json
import logging
import os
import sqlite3
import sys

from easyner.database.sqlite_backend.core.logger import BaseLogger


class DatabaseManager:
    """Manages the SQLite database connection and operations."""

    def __init__(
        self,
        db_path: str = None,
        config_path: str = "config.json",
        logger: BaseLogger = None,
    ):
        """Initialize the Database Manager.

        :param db_path: Path to the SQLite database file (optional, can be in config).
        :param config_path: Path to the configuration JSON file.
        :param logger:  Optional BaseLogger instance. If None, a default logger is created.
        """
        self.config = self._load_config(config_path)
        self.db_path = db_path or self.config.get(
            "db_path",
        )  # Use provided path or from config

        if logger:
            self.logger = logger
        else:
            self.logger = BaseLogger(
                "DatabaseManagerLogger",
            )  # Default logger if none provided
            self._setup_default_logging()  # Setup file logging for default logger

        # Development Mode Handling
        if self.config.get("develop"):
            self.logger.info(
                "Setting up in development mode, ignoring database path provided if any.",
            )
            pwd = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(pwd, "development.db")

        if not self.db_path:
            self.logger.warning(
                "No database path provided, defaulting to path from config.",
            )
            self.db_path = self.config.get("db_path")

        # Resolve db_path to be absolute
        if not os.path.isabs(self.db_path):
            project_root = os.path.dirname(
                os.path.abspath(config_path),
            )  # Config path as project root
            self.db_path = os.path.join(project_root, self.db_path)

        # Database Creation if not exists
        if not os.path.exists(self.db_path):
            self.logger.info(
                f"Database {self.db_path} does not exist. Creating database...",
            )
            schema_path = self.config.get("schema_path")
            if (
                os.getenv("DB_RUN", False) == True or not sys.stdin.isatty()
            ):  # Non-interactive env
                self.logger.info(
                    "Non-interactive environment, using default schema path from config.",
                )
            else:  # Interactive environment, prompt for schema path
                schema_path_input = input(
                    "Please provide the path to the schema file, or press <Enter> to use default from config: ",
                )
                if schema_path_input:
                    schema_path = schema_path_input
            self.create_db(self.db_path, schema_path)

        self.name = os.path.basename(self.db_path)

        # Database Connection
        self.logger.info(f"Connecting to database: {self.name} at {self.db_path}")
        self._connection = self._connect()
        self.cursor = self._connection.cursor()
        self.cursor.row_factory = sqlite3.Row  # Return rows as dictionaries

        self._set_default_settings()  # Set pragmas for connection

        self.logger.info(f"Connected to database {self.db_path} and created cursor.")

    def _connect(self):
        """Establish database connection and handle potential errors."""
        try:
            conn = sqlite3.connect(self.db_path)
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Failed to connect to database {self.db_path}: {e}")
            raise

    def _set_default_settings(self):
        """Set default PRAGMA settings for the database connection."""
        self.execute("PRAGMA foreign_keys = ON;")
        self.execute("PRAGMA journal_mode = WAL;")
        self.execute("PRAGMA synchronous = NORMAL;")
        self.execute("PRAGMA journal_size_limit = 6144000;")
        self.execute("PRAGMA temp_store = MEMORY;")
        self.execute("PRAGMA busy_timeout = 10000;")

    def _load_config(self, config_path):
        """Load configuration from JSON file."""
        config_dir = os.path.dirname(
            os.path.abspath(__file__),
        )  # Assuming config near script
        full_config_path = os.path.join(config_dir, config_path)

        try:
            with open(full_config_path) as f:
                config_data = json.load(f).get("database", {})
                # Basic config integrity check (can be expanded)
                if not config_data.get("develop") and not config_data.get("db_path"):
                    msg = "Database configuration must contain 'db_path' unless in development mode."
                    raise ValueError(msg)
                if not config_data.get("schema_path"):
                    msg = "Database configuration must contain 'schema_path'."
                    raise ValueError(msg)
                return config_data
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
            msg = f"Error loading database configuration from {config_path}: {e}"
            raise ValueError(msg)

    def _setup_default_logging(self):
        """Configure default logging to files within the 'logs' directory."""
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)  # Ensure log directory exists

        log_file = os.path.join(log_dir, self.name + ".log")
        error_log_file = os.path.join(log_dir, "db_error.log")

        file_handler = logging.FileHandler(log_file)
        error_file_handler = logging.FileHandler(error_log_file)
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s",
        )
        file_handler.setFormatter(formatter)
        error_file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        file_handler.setLevel(logging.DEBUG)
        error_file_handler.setLevel(logging.ERROR)
        console_handler.setLevel(logging.INFO)  # Console output level

        self.logger.logger.addHandler(
            file_handler,
        )  # Access underlying logger from BaseLogger
        self.logger.logger.addHandler(console_handler)
        self.logger.logger.addHandler(error_file_handler)

        self.logger.info(
            f"Default logging initialized. Log file: {log_file}, Error log file: {error_log_file}",
        )

    def __del__(self):
        """Ensure database connection is closed when instance is deleted."""
        self.close_connection()

    def close_connection(self) -> None:
        """Close the database connection."""
        if (
            hasattr(self, "_connection") and self._connection
        ):  # Check if connection exists and is not None
            try:
                self._connection.close()
                self._connection = None  # Reset connection attribute
                self.logger.info(f"Database connection to {self.name} closed.")
            except Exception as e:
                self.logger.error(f"Error closing database connection: {e}")

    def execute_query(self, query, args=None, commit=False):
        """Execute a SQL query with optional parameters."""
        try:
            if args is None:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, args)

            if query.lstrip().upper().startswith(("SELECT", "PRAGMA")):
                return self.cursor.fetchall()
            elif commit:
                self._connection.commit()  # Use _connection to commit
            return []
        except sqlite3.Error as e:
            self.logger.error(
                f"Error executing query: {query} with args: {args}. Exception: {e}",
            )
            if self._connection:
                self._connection.rollback()  # Rollback on error for data integrity
            raise

    def execute_with_log(self, query, args=None):
        """Execute a SQL query, log query plan, and return results."""
        self._log_query_plan(query, args)
        return self.execute_query(query, args)

    def _log_query_plan(self, sql, params=None):
        """Logs the query plan for a given SQL query."""
        try:
            if params:
                self.cursor.execute(f"EXPLAIN QUERY PLAN {sql}", params)
            else:
                self.cursor.execute(f"EXPLAIN QUERY PLAN {sql}")

            plan = self.cursor.fetchall()
            plan_str = f"EXPLAIN QUERY PLAN {sql};\n"
            for step in plan:
                plan_str += str(dict(step)) + "\n"
            self.logger.debug(plan_str)

        except sqlite3.Error as e:
            self.logger.error(f"Error logging query plan for: {sql}. Exception: {e}")

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            self._connection.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Commit failed: {e}")
            raise

    def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            self._connection.rollback()
        except sqlite3.Error as e:
            self.logger.error(f"Rollback failed: {e}")
            raise

    def fetchall(self):
        """Fetch all rows from the last query."""
        return self.cursor.fetchall()

    def fetchone(self):
        """Fetch one row from the last query."""
        return self.cursor.fetchone()

    def fetchmany(self, size):
        """Fetch many rows from the last query."""
        return self.cursor.fetchmany(size)

    def fetchall_dict(self):
        """Fetch all rows as list of dictionaries."""
        rows = self.fetchall()
        if not rows:
            return []
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def fetchone_dict(self):
        """Fetch one row as dictionary."""
        row = self.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in self.cursor.description]
        return dict(zip(columns, row, strict=False))

    def fetchmany_dict(self, size):
        """Fetch many rows as list of dictionaries."""
        rows = self.fetchmany(size)
        if not rows:
            return []
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def get_table_names(self):
        """Get a list of table names in the database."""
        results = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table';",
        )
        return [table[0] for table in results] if results else []

    def optimize_db_performance_parameters(self) -> None:
        """Optimize database for general performance using PRAGMA settings."""
        self.execute("PRAGMA journal_mode = WAL")
        self.execute("PRAGMA synchronous = NORMAL")  # Reduced safety, good balance
        self.execute("PRAGMA journal_size_limit = 6144000")
        self.execute("PRAGMA temp_store = MEMORY")
        self.logger.info(
            "Database performance parameters optimized for balanced performance.",
        )

    def optimize_db_performance_for_write(self) -> None:
        """Optimize database for write-heavy operations."""
        self.execute("PRAGMA journal_mode = WAL")
        self.execute("PRAGMA synchronous = OFF")  # Less safety, faster writes
        self.execute("PRAGMA journal_size_limit = 6144000")
        self.execute("PRAGMA temp_store = MEMORY")
        self.logger.info(
            "Database performance parameters optimized for write operations.",
        )

    def optimize_db_performance_for_read(self) -> None:
        """Optimize database for read-heavy operations."""
        self.execute(
            "PRAGMA journal_mode = MEMORY",
        )  # In-memory journaling, fastest reads, less durable
        self.execute("PRAGMA synchronous = OFF")  # Less safety, faster reads
        self.execute("PRAGMA journal_size_limit = 6144000")
        self.execute("PRAGMA temp_store = MEMORY")
        self.logger.info(
            "Database performance parameters optimized for read operations.",
        )

    def align_with_schema(self, schema_path=None) -> None:
        """Ensure database schema matches the provided schema file."""
        if not schema_path:
            schema_path = input("Please provide the path to the schema file: ")

        if not os.path.isabs(schema_path):
            config_dir = os.path.dirname(
                os.path.abspath(__file__),
            )  # Config path as project root
            schema_path = os.path.join(config_dir, schema_path)

        schema_conn = sqlite3.connect(":memory:")  # In-memory DB to load schema
        schema_cursor = schema_conn.cursor()
        try:
            with open(schema_path) as f:
                schema_sql = f.read()
            schema_cursor.executescript(schema_sql)
        except FileNotFoundError:
            self.logger.error(f"Schema file not found: {schema_path}")
            return
        except sqlite3.Error as e:
            self.logger.error(f"Error loading schema from {schema_path}: {e}")
            return

        schema_tables = [
            row[0]
            for row in schema_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table';",
            ).fetchall()
        ]

        tables_changed = False  # Flag to track if any changes were made

        for table in schema_tables:
            if table == "sqlite_sequence":  # Skip internal sequence table
                continue

            schema_columns = {
                col[1]: col
                for col in schema_cursor.execute(
                    f"PRAGMA table_info('{table}');",
                ).fetchall()
            }
            db_columns = {
                col[1]: col
                for col in self.execute_query(f"PRAGMA table_info('{table}');")
            }

            if table not in self.get_table_names():
                cols_def = ", ".join(
                    [
                        f"{col[1]} {col[2]}{' NOT NULL' if col[3] else ''}{' DEFAULT '+str(col[4]) if col[4] is not None else ''}{' PRIMARY KEY' if col[5] else ''}"
                        for col in schema_columns.values()
                    ],
                )
                create_stmt = f"CREATE TABLE {table} ({cols_def});"
                self.logger.info(f"Creating missing table: {table}")
                self.execute_query(create_stmt, commit=True)
                tables_changed = True
            else:
                for col_name, col_info in schema_columns.items():
                    if col_name not in db_columns:
                        add_stmt = (
                            f"ALTER TABLE {table} ADD COLUMN {col_name} {col_info[2]}"
                        )
                        self.logger.info(
                            f"Adding missing column '{col_name}' to table '{table}'",
                        )
                        self.execute_query(add_stmt, commit=True)
                        tables_changed = True

        if not tables_changed:
            self.logger.info("Database schema is already aligned with provided schema.")
        schema_conn.close()

    @property
    def is_locked(self) -> bool:
        """Check if the database is locked."""
        try:
            self.execute_query("PRAGMA wal_checkpoint;")
            return False
        except sqlite3.OperationalError:
            return True

    def clear_connections(self) -> None:
        """Attempt to clear connections and unlock database (use with caution)."""
        try:
            self._connection.commit()
        except Exception as e:
            self.logger.error(
                f"Error committing pending transactions during clear connections: {e}",
            )

        try:
            self.execute_query("PRAGMA wal_checkpoint(FULL);", commit=True)
        except Exception as e:
            self.logger.error(
                f"Error executing WAL checkpoint during clear connections: {e}",
            )
        finally:
            self.close_connection()
            self.logger.info("Database connection closed and attempted to unlock.")

    @classmethod
    def create_db(cls, db_path, schema_path):
        """Class method to create a database at the given path using the provided schema."""
        if not os.path.isabs(schema_path):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            schema_path = os.path.join(script_dir, schema_path)

        try:
            with open(schema_path) as f:
                sql_script = f.read()
        except FileNotFoundError:
            print(
                f"Schema file not found: {schema_path}",
            )  # Use print here as logger might not be setup yet
            return

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        try:
            cursor.executescript(sql_script)
            conn.commit()
            print(
                f"Database created at {db_path} using schema from {schema_path}",
            )  # Use print here
        except sqlite3.Error as e:
            print(f"Error creating database: {e}")  # Use print here
            os.remove(db_path)  # Remove the database file if creation fails
        finally:
            conn.close()
