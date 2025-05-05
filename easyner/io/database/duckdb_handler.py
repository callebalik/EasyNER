import duckdb
from pathlib import Path
import pandas as pd
import logging
import warnings
import os
from typing import Dict, List, Any, Union, Optional

from easyner.io.handlers.base import IOHandler


class DuckDBHandler(IOHandler):
    """
    DuckDBHandler is a class that provides methods to read and write data to and from DuckDB databases.
    It inherits from the IOHandler class and implements the read and write methods for DuckDB.
    """

    # Path to SQL files
    SQL_DIR = Path(__file__).parent

    def __init__(
        self,
        db_path: Optional[str] = ":memory:",
        threads: int = 4,
        memory_limit: str = "1GB",
        encoding="utf-8",
    ):
        """
        Initialize the DuckDB handler

        Args:
            db_path: Path to the database file, or ":memory:" for in-memory database
            threads: Number of threads to use
            memory_limit: Memory limit for DuckDB
            encoding: Encoding to use for file operations
        """
        super().__init__(encoding=encoding)
        self.logger = logging.getLogger(__name__)
        self.connection = self._initialize_db(db_path, threads, memory_limit)

    def read(self, file_path: str, **kwargs):
        """
        Read data from DuckDB database.

        Args:
            file_path: Path to the database file
            **kwargs: Additional arguments for reading

        Returns:
            The result of the query
        """
        query = kwargs.get("query")
        if not query:
            raise ValueError(
                "Query parameter is required for reading from database"
            )

        conn = self._get_connection(file_path)
        result = conn.execute(query)

        # Return as DataFrame by default
        as_df = kwargs.get("as_df", True)
        return result.fetchdf() if as_df else result.fetchall()

    def write(self, data, file_path: str, **kwargs):
        """
        Write data to DuckDB database.

        Args:
            data: Data to write (can be DataFrame or list of dictionaries)
            file_path: Path to the database file
            **kwargs: Additional arguments for writing
                table_name: Name of the table to write to
                if_exists: What to do if table exists ('fail', 'replace', 'append')
        """
        table_name = kwargs.get("table_name")
        if not table_name:
            raise ValueError(
                "table_name parameter is required for writing to database"
            )

        if_exists = kwargs.get("if_exists", "fail")

        conn = self._get_connection(file_path)

        if isinstance(data, pd.DataFrame):
            # Register DataFrame as a view
            conn.register(f"{table_name}_temp", data)

            if if_exists == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}_temp"
                )
            elif if_exists == "append":
                conn.execute(
                    f"INSERT INTO {table_name} SELECT * FROM {table_name}_temp"
                )
            else:  # 'fail'
                table_exists = conn.execute(
                    f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
                ).fetchone()[0]
                if table_exists:
                    raise ValueError(f"Table {table_name} already exists")
                conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}_temp"
                )
        else:
            # Convert to DataFrame if necessary
            df = (
                pd.DataFrame(data)
                if not isinstance(data, pd.DataFrame)
                else data
            )
            self.write(
                df, file_path, table_name=table_name, if_exists=if_exists
            )

    def _initialize_db(
        self,
        database_path: Optional[str] = ":memory:",
        threads: int = 4,
        memory_limit: str = "1GB",
    ) -> duckdb.DuckDBPyConnection:
        """
        Initialize and configure a DuckDB connection

        Args:
            database_path: Path to the database file, or ":memory:" for in-memory database
            threads: Number of threads to use
            memory_limit: Memory limit for DuckDB

        Returns:
            DuckDB connection object
        """
        # If using a file database (not in-memory), ensure the directory exists
        if database_path != ":memory:":
            db_dir = os.path.dirname(database_path)
            if db_dir:  # Only try to create if there's a directory part
                os.makedirs(db_dir, exist_ok=True)

        con = duckdb.connect(database=database_path)
        con.execute(f"PRAGMA threads={threads}")
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        return con

    def _get_connection(self, db_path: str = None):
        """
        Establish a connection to the DuckDB database.

        Args:
            db_path: Path to the DuckDB database file. If None, returns the existing connection.

        Returns:
            DuckDB connection object.
        """
        if db_path is None:
            return self.connection

        try:
            conn = duckdb.connect(db_path)
            return conn
        except Exception as e:
            self.logger.error(f"Error connecting to DuckDB database: {e}")
            raise

    def _read_sql_file(self, file_path: Union[str, Path]) -> str:
        """
        Read SQL file content as a string

        Args:
            file_path: Path to the SQL file

        Returns:
            SQL content as a string
        """
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    def create_tables(self) -> None:
        """
        Create all database tables using SQL files.

        This method reads SQL files from the SQL_DIR directory and executes them
        to create the necessary tables in the database.
        """
        # Execute SQL statements from files
        articles_table_sql = self._read_sql_file(
            self.SQL_DIR / "articles_table.sql"
        )
        sentences_table_sql = self._read_sql_file(
            self.SQL_DIR / "sentences_table.sql"
        )
        entity_sequence_sql = self._read_sql_file(
            self.SQL_DIR / "entity_sequence.sql"
        )
        entities_table_sql = self._read_sql_file(
            self.SQL_DIR / "entities_table.sql"
        )

        # Create tables using SQL from files
        self.connection.execute(articles_table_sql)
        self.connection.execute(sentences_table_sql)
        self.connection.execute(entity_sequence_sql)
        self.connection.execute(entities_table_sql)

    def create_indices(self) -> None:
        """Create database indices for performance optimization"""
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_article_id ON articles(article_id)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_sentence_article_id ON sentences(article_id)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_entity_article_sentence ON entities(article_id, sentence_id)"
        )

    def insert_data(
        self,
        articles_data: Union[List[Dict[str, Any]], pd.DataFrame],
        sentences_data: Union[List[Dict[str, Any]], pd.DataFrame],
        entities_data: Union[List[Dict[str, Any]], pd.DataFrame],
    ) -> None:
        """
        Insert data into database tables using DataFrames

        Args:
            articles_data: Articles data as DataFrame or list of dicts
            sentences_data: Sentences data as DataFrame or list of dicts
            entities_data: Entities data as DataFrame or list of dicts
        """
        # Convert to DataFrames if needed
        if not isinstance(articles_data, pd.DataFrame):
            articles_df = pd.DataFrame(articles_data)
        else:
            articles_df = articles_data

        if not isinstance(sentences_data, pd.DataFrame):
            sentences_df = pd.DataFrame(sentences_data)
        else:
            sentences_df = sentences_data

        if not isinstance(entities_data, pd.DataFrame):
            entities_df = pd.DataFrame(entities_data)
        else:
            entities_df = entities_data

        # Register DataFrames as views
        self.connection.register("articles_df", articles_df)
        self.connection.register("sentences_df", sentences_df)
        self.connection.register("entities_df", entities_df)

        # Insert data from the registered views
        self.connection.execute(
            "INSERT INTO articles SELECT * FROM articles_df"
        )
        self.connection.execute(
            "INSERT INTO sentences SELECT * FROM sentences_df"
        )
        self.connection.execute(
            """
            INSERT INTO entities (article_id, sentence_id, entity, start_pos, end_pos, inference_model, inference_model_metadata)
            SELECT article_id, sentence_id, entity, start_pos, end_pos, inference_model, inference_model_metadata FROM entities_df
        """
        )

    def get_table(
        self, table_name: str, as_df: bool = True
    ) -> Union[pd.DataFrame, List[tuple]]:
        """
        Fetch data from a table, either as DataFrame or list of tuples

        Args:
            table_name: Name of the table to fetch
            as_df: Return as DataFrame if True, otherwise as list of tuples

        Returns:
            DataFrame or list of tuples containing table data
        """
        result = self.connection.execute(f"SELECT * FROM {table_name}")
        return result.fetchdf() if as_df else result.fetchall()

    def get_table_count(self, table_name: str) -> int:
        """
        Get the count of rows in a table

        Args:
            table_name: Name of the table to count rows in

        Returns:
            Number of rows in the table
        """
        result = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}")
        return result.fetchone()[0]

    def export_to_csv(
        self, table_name: str, output_path: Union[str, Path]
    ) -> None:
        """
        Export a table to CSV file

        Args:
            table_name: Name of the table to export
            output_path: Path to save the CSV file
        """
        self.connection.execute(
            f"COPY (SELECT * FROM {table_name}) TO '{output_path}' (HEADER, DELIMITER ',');"
        )

    def get_entities_by_article(self, article_id: int) -> pd.DataFrame:
        """
        Get all entities for a specific article

        Args:
            article_id: ID of the article

        Returns:
            DataFrame containing entities for the specified article
        """
        query = """
        SELECT e.*
        FROM entities e
        WHERE e.article_id = ?
        """
        return self.connection.execute(query, [article_id]).fetchdf()

    def get_article_entity_stats(self) -> pd.DataFrame:
        """
        Get statistics about entities per article

        Returns:
            DataFrame containing statistics about entities per article
        """
        query = """
        SELECT a.article_id, a.title, COUNT(DISTINCT s.sentence_id) AS sentence_count, COUNT(e.entity) AS entity_count
        FROM articles a
        LEFT JOIN sentences s ON a.article_id = s.article_id
        LEFT JOIN entities e ON s.article_id = e.article_id AND s.sentence_id = e.sentence_id
        GROUP BY a.article_id, a.title
        ORDER BY entity_count DESC
        """
        return self.connection.execute(query).fetchdf()

    def get_articles_df(self) -> pd.DataFrame:
        """
        Get all articles from the database as a DataFrame.

        Returns:
            DataFrame containing article data
        """
        result = self.connection.execute(
            "SELECT article_id, title FROM articles"
        )
        return result.fetchdf()

    def get_articles_as_dict_list(self) -> List[Dict[str, Any]]:
        """
        Get all articles from the database as a list of dictionaries.

        Returns:
            List of dictionaries containing article data
        """
        warnings.warn(
            "The get_articles_as_dict_list method is not covered by tests and may have unexpected behavior.",
            UserWarning,
            stacklevel=2,
        )
        rows = self.connection.execute(
            "SELECT article_id, title FROM articles"
        ).fetchall()
        return [{"article_id": row[0], "title": row[1]} for row in rows]

    def get_articles(
        self, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get all articles from the database.
        This is a wrapper method for backward compatibility.

        Args:
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of article dictionaries
        """
        if as_df:
            return self.get_articles_df()
        else:
            return self.get_articles_as_dict_list()

    def get_sentences_df(self) -> pd.DataFrame:
        """
        Get all sentences from the database as a DataFrame.

        Returns:
            DataFrame containing sentence data
        """
        result = self.connection.execute(
            "SELECT article_id, sentence_id, text FROM sentences"
        )
        return result.fetchdf()

    def get_sentences_as_dict_list(self) -> List[Dict[str, Any]]:
        """
        Get all sentences from the database as a list of dictionaries.

        Returns:
            List of dictionaries containing sentence data
        """
        warnings.warn(
            "The get_sentences_as_dict_list method is not covered by tests and may have unexpected behavior.",
            UserWarning,
            stacklevel=2,
        )
        rows = self.connection.execute(
            "SELECT article_id, sentence_id, text FROM sentences"
        ).fetchall()
        return [
            {"article_id": row[0], "sentence_id": row[1], "text": row[2]}
            for row in rows
        ]

    def get_sentences(
        self, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get all sentences from the database.
        This is a wrapper method for backward compatibility.

        Args:
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of sentence dictionaries
        """
        if as_df:
            return self.get_sentences_df()
        else:
            return self.get_sentences_as_dict_list()

    def get_entities_df(self) -> pd.DataFrame:
        """
        Get all entities from the database as a DataFrame.

        Returns:
            DataFrame containing entity data
        """
        query = """
            SELECT article_id, sentence_id, entity, start_pos, end_pos,
                inference_model, inference_model_metadata
            FROM entities
        """
        result = self.connection.execute(query)
        return result.fetchdf()

    def get_entities_as_dict_list(self) -> List[Dict[str, Any]]:
        """
        Get all entities from the database as a list of dictionaries.

        Returns:
            List of dictionaries containing entity data
        """
        warnings.warn(
            "The get_entities_as_dict_list method is not covered by tests and may have unexpected behavior.",
            UserWarning,
            stacklevel=2,
        )
        query = """
            SELECT article_id, sentence_id, entity, start_pos, end_pos,
                inference_model, inference_model_metadata
            FROM entities
        """
        rows = self.connection.execute(query).fetchall()
        return [
            {
                "article_id": row[0],
                "sentence_id": row[1],
                "entity": row[2],
                "start_pos": row[3],
                "end_pos": row[4],
                "inference_model": row[5],
                "inference_model_metadata": row[6],
            }
            for row in rows
        ]

    def get_entities(
        self, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get all entities from the database.
        This is a wrapper method for backward compatibility.

        Args:
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of entity dictionaries
        """
        if as_df:
            return self.get_entities_df()
        else:
            return self.get_entities_as_dict_list()
