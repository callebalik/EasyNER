import os
import sys
import logging
import json
import sqlite3



class EasyNerDBHandler:
    def __init__(self, db_path: str = None, config_path: str = "../../config.json"):
        """
        Initialize the database handler.

        :param db_path: Path to the SQLite database file.
        """
        self.config = self._load_config(config_path)

        # Check if in development mode, if so, ignore db_path provided
        if self.config["develop"]:
            print("Setting up in development mode, ignoring database path provided")
            pwd = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(pwd, "development.db")
        else:
            if db_path is not None:
                self.db_path = db_path
            else:
                print(
                    "WARNING: Using No database path provided, continuing with database path provided in config"
                )
                self.db_path = self.config.get("db_path")

        # Ensure db_path is absolute otherwise resolve it relative to the script
        if not os.path.isabs(self.db_path):
            # Resolve db_path relative to project root (config.json directory)
            project_root = os.path.dirname(os.path.abspath(config_path))
            self.db_path = os.path.join(project_root, self.db_path)

        # Create Database if not present.
        # Resolve sql schema, defaulting to schema path if user doesn't provide one after prompt
        if not os.path.exists(self.db_path):
            print(f"Database {self.db_path} does not exist. Creating database...")

            if os.getenv("DB_RUN", False) == True or not sys.stdin.isatty(): # Check if running in a non-interactive environment
                print("Running in non-interactive environment, using default schema path defined in config")
                self.schema_path = self.config.get("schema_path")
            else:
                self.schema_path = input(
                    "Please provide the path to the schema file, or press <Enter> to use the default schema path defined in config: "
                )
                if self.schema_path == "":
                    self.schema_path = self.config.get("schema_path")
            self.create_db(self.db_path, self.schema_path)

        self.name = os.path.basename(self.db_path)
        self._setup_logging()

        # Connect to the database
        self.conn = sqlite3.connect(self.db_path)
        # self._set_default_settings()
        self.cursor = self.conn.cursor()  # Ensure cursor is an attribute
        self.cursor.row_factory = sqlite3.Row # Return rows as dictionaries for easy access
        self.execute_with_log = self.execute_with_log
        self.log_query_plan = self._log_query_plan
        self.logger.debug(f"Connected to database {self.db_path} and created cursor")

        # Initialize components
        # Import here to avoid circular dependencies and ensure all components are initialized before use
        from .db_data_exchanger import DBDataExchanger
        from .db_data_cleaner import DBDataCleaner
        from .analysis.db_analysis import DBAnalysis
        from .db_statistics import DBStatistics
        from .data_model.entities import EntityOccurrence, EntityCooccurence


        self.data_exchanger = DBDataExchanger(self.conn, self.cursor, self.logger)
        self.data_cleaner = DBDataCleaner(
            self.conn, self.cursor, self.logger, self.data_exchanger, config=self.config
        )
        self.analysis = DBAnalysis(self.conn, self.cursor, self.logger, self.data_exchanger, self.log_query_plan, self.execute_with_log, self.conn_params_dict)
        self.statistics = DBStatistics(self.conn, self.cursor, self.logger, self.data_exchanger)

        self.eo = EntityOccurrence(self.conn, self.cursor, self.logger, log_query_plan=self._log_query_plan, conn_params_dict=self.conn_params_dict)
        self.co = EntityCooccurence(self.conn, self.cursor, self.logger, log_query_plan=self._log_query_plan, conn_params_dict=self.conn_params_dict)

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
        # Set the logger level to DEBUG to capture all messages
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            log_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "logs/" + self.name + ".log"
            )
            error_log_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "logs/db_error.log"
            )

            # Ensure the logs directory exists
            os.makedirs(os.path.dirname(log_file), exist_ok=True)

            # Create file handler for logging
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)

            # Create console handler for logging
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(console_formatter)

            # Create separate error file handler for logging errors
            error_file_handler = logging.FileHandler(error_log_file)
            error_file_handler.setLevel(logging.ERROR)
            error_file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s"
            )
            error_file_handler.setFormatter(error_file_formatter)

            # Add handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
            self.logger.addHandler(error_file_handler)

            self.logger.info(f"Logging initialized. Log file: {log_file}")

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
    def tables(self):
        """
        Get information about the database.

        :return: A dictionary containing information about the database.
        """

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.fetchall()
        tables = [table[0] for table in tables]
        return {"tables": tables}

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

class DBEntryPoint:
    def __init__(self, db_path: str = None, config_path: str = "../../config.json"):
        """Initialize database components with proper dependency injection."""
        self.db = EasyNerDBHandler(db_path, config_path)

        # Import core components
        from .db_data_exchanger import DBDataExchanger
        from .db_data_cleaner import DBDataCleaner
        from .analysis.db_analysis import DBAnalysis
        from .db_statistics import DBStatistics
        from .data_model.entity_occurrence import EntityOccurrence
        from .data_model.entity_cooccurrence import EntityCooccurrence

        # Initialize data_exchanger and attach it to the db instance
        self.data_exchanger = DBDataExchanger(self.db.conn, self.db.cursor, self.db.logger)
        self.db.data_exchanger = self.data_exchanger

        # Initialize other components
        self.data_cleaner = DBDataCleaner(
            self.db.conn, self.db.cursor, self.db.logger, self.data_exchanger, config=self.db.config
        )
        self.analysis = DBAnalysis(
            self.db.conn, self.db.cursor, self.db.logger, self.data_exchanger, self.db.log_query_plan, self.db.execute_with_log, self.db.conn_params_dict
        )
        self.statistics = DBStatistics(
            self.db.conn, self.db.cursor, self.db.logger, self.data_exchanger
        )

        # Initialize entity handling components
        self.ne = EntityOccurrence(self.db)
        self.co = EntityCooccurrence(self.db)

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, 'db'):
            self.db.close()
