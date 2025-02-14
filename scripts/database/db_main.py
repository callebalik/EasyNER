import sys
import os
import logging
import json
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class EasyNerDBHandler:
    def __init__(self, db_path: str = None, config_path: str ="../../config.json"):
        """
        Initialize the database handler.

        :param db_path: Path to the SQLite database file.
        """
        self.config = self._load_config(config_path)
        
        
        if db_path is not None:
            self.db_path = db_path
        else:
            print("WARNING: Using No database path provided, continuing with database path provided in config")
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
            self.schema_path = input("Please provide the path to the schema file, or press <Enter> to use the default schema path defined in config: ")
            if self.schema_path == "":
                self.schema_path = self.config.get("schema_path")
            self.create_db(self.db_path, self.schema_path)

        self.name = os.path.basename(self.db_path)
        self._setup_logging()

        # Connect to the database
        self.logger.info(f"Connecting to database {self.name}")
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()  # Ensure cursor is an attribute
        self.logger.info(f"Connected to database {self.db_path} and created cursor")

        # Initialize components
        # Import here to avoid circular dependencies and ensure all components are initialized before use
        from db_data_exchanger import DBDataExchanger 
        from db_data_cleaner import DBDataCleaner
        from db_analysis import DBAnalysis
        from db_statistics import DBStatistics

        self.data_exchanger = DBDataExchanger(self.conn, self.cursor, self.logger)
        self.data_cleaner = DBDataCleaner(self.conn, self.cursor, self.logger, self.data_exchanger)
        self.analysis = DBAnalysis(self.conn, self.cursor, self.logger)
        self.statistics = DBStatistics(self.conn, self.cursor, self.logger)
        
    def _load_config(self, config_path):
        """Load the JSON configuration file."""
        if not os.path.isabs(config_path):
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_path)
        try:
            with open(config_path, "r") as f:
                config = json.load(f).get("database", {})
                # Test integrity of the configuration
                if "db_path" not in config:
                    raise ValueError("Database configuration must contain 'db_path' key.")
                if "schema_path" not in config:
                    raise ValueError("Database configuration does not contain 'schema_path' key.")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise ValueError(f"Error loading configuration file: {e}")
            
        return config

    def _setup_logging(self):
        """Configure logging to save to db.log in the database directory."""
        
        self.logger = logging.getLogger('EasyNerDB')
        # Set the logger level to DEBUG to capture all messages
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs/" + self.name + ".log")
            error_log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs/db_error.log")

            # Ensure the logs directory exists
            os.makedirs(os.path.dirname(log_file), exist_ok=True)

            
            # Create file handler for logging
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)

            # Create console handler for logging
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)

            # Create separate error file handler for logging errors
            error_file_handler = logging.FileHandler(error_log_file)
            error_file_handler.setLevel(logging.ERROR)
            error_file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            error_file_handler.setFormatter(error_file_formatter)

            # Add handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
            self.logger.addHandler(error_file_handler)
                    
            self.logger.info(f'Logging initialized. Log file: {log_file}')

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

    def empty_database(self):
        """
        Empty the database.
        """
        confirmation = input("Are you sure you want to empty the database? (y/n): ")
        if confirmation.lower() != 'y':
            print("Operation cancelled.")
            return
        self.cursor.execute("PRAGMA foreign_keys = OFF;")
        for table in self.tables["tables"]:
            self.cursor.execute(f"DELETE FROM {table};")
        self.cursor.execute("PRAGMA foreign_keys = ON;")
        self.conn.commit()
        self.cursor.execute("VACUUM;")

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
            if query.lstrip().upper().startswith('SELECT') or query.lstrip().upper().startswith('PRAGMA'):
                return self.cursor.fetchall()
            # For other queries (INSERT, UPDATE, DELETE, etc.), commit and return empty list
            else:
                self.conn.commit()
                return []
        except Exception as e:
            self.logger.error(f"Error executing query: {query} with args: {args}. Exception: {e}")
            raise

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