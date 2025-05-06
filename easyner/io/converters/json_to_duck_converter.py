import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Union, Optional
from tqdm import tqdm

from easyner.io.converters.base import BaseConverter
from easyner.io.database.duckdb_handler import DuckDBHandler
from easyner.io.database.repositories import (
    ArticleRepository,
    SentenceRepository,
    EntityRepository,
)

from easyner.io.database.schemas import CONVERSION_LOG_TABLE_SQL
from easyner.io.database.utils.sql_utils import get_file_hash
from easyner.io.handlers import PubMedJsonHandler

# Set up logger for the converter
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    # Add console handler if none exists
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(levelname)s - %(name)s: %(message)s")
    )
    logger.addHandler(handler)


class JsonToDuckConverter(BaseConverter):
    """Converter to transform JSON data into DuckDB database"""

    def __init__(
        self,
        source_dir: Union[str, Path],
        target_dir: Union[str, Path],
        connection=None,
        db_file: Optional[str] = None,
        file_pattern: str = "*.json",
        reprocess: bool = False,
    ):
        """
        Initialize the JSON to DuckDB converter

        Args:
            source_dir: Directory containing JSON files to convert
            target_dir: Directory where the DuckDB database will be stored
            connection: An existing DuckDB connection (optional)
            db_file: Custom database filename (optional, default is 'easyner.db')
            file_pattern: Pattern to match JSON files (default: "*.json")
            reprocess: Whether to reprocess already converted files (default: False)
        """
        super().__init__(source_dir, target_dir)
        self.file_pattern = file_pattern
        self._reprocess = reprocess

        # Set up db_file path - if not provided, create one in target_dir
        if db_file is None:
            self.db_file = os.path.join(str(target_dir), "easyner.db")
        else:
            self.db_file = db_file

        # Initialize the DuckDB handler to handle database operations
        if connection:
            self.db_handler = DuckDBHandler(":memory:")
            self.db_handler.connection = connection
        else:
            self.db_handler = DuckDBHandler(self.db_file)

        self.connection = self.db_handler.connection
        self._is_memory_db = self.db_file == ":memory:"

        # Create conversion log table if it doesn't exist
        self.connection.execute(CONVERSION_LOG_TABLE_SQL)
        self._converted_files = []

    def list_convertible_files(self) -> List[Path]:
        """List all JSON files in the source directory"""
        return list(self.source_dir.glob(self.file_pattern))

    def list_converted_files(self) -> List[Path]:
        """List all files that have already been successfully converted by querying the database."""
        if self._is_memory_db:
            return self._converted_files

        query = (
            "SELECT file_path FROM conversion_log WHERE status = 'converted';"
        )
        try:
            results = self.connection.execute(query).fetchall()
            return [Path(row[0]) for row in results]
        except Exception:
            return []

    def list_unconverted_files(self) -> List[Path]:
        """List all files that have not been converted yet"""
        if self._reprocess:
            return self.list_convertible_files()

        converted_files = set(self.list_converted_files())
        convertible_files = self.list_convertible_files()
        return [f for f in convertible_files if f not in converted_files]

    def _log_conversion(self, file_path: Path, status: str):
        """Logs the conversion attempt to the database."""
        if self._is_memory_db:
            if status == "converted":
                self._converted_files.append(file_path)
            return

        try:
            file_hash = get_file_hash(file_path)
            file_size = file_path.stat().st_size
            timestamp = "NOW()"

            self.connection.execute(
                """
                INSERT INTO conversion_log (file_path, file_name, file_hash, file_size_bytes, conversion_timestamp, status)
                VALUES (?, ?, ?, ?, NOW(), ?)
                ON CONFLICT(file_path) DO UPDATE SET
                    file_hash = excluded.file_hash,
                    file_size_bytes = excluded.file_size_bytes,
                    conversion_timestamp = NOW(),
                    status = excluded.status;
                """,
                (
                    str(file_path.resolve()),
                    file_path.name,
                    file_hash,
                    file_size,
                    status,
                ),
            )
        except Exception as e:
            print(f"Error logging conversion: {e}")
            # Continue processing - if we can't log, we'll still continue with conversion

    def convert(self, **kwargs) -> Dict[str, Any]:
        """
        Convert JSON files to DuckDB database

        Args:
            **kwargs: Additional arguments
                use_memory_db (bool): Use an in-memory database instead of a file
                    (default: False)
                reprocess (bool): Override instance reprocess flag
                    (default: None - use instance flag)

        Returns:
            Dictionary containing statistics about the conversion
        """
        # Process kwargs
        use_memory_db = kwargs.get("use_memory_db", False)
        reprocess_override = kwargs.get("reprocess")
        reprocess = (
            self._reprocess
            if reprocess_override is None
            else reprocess_override
        )

        processed_files = []

        # Initialize in-memory database if requested
        if use_memory_db and not self._is_memory_db:
            self.db_handler = DuckDBHandler(":memory:")
            self.connection = self.db_handler.connection
            self._is_memory_db = True
            self.connection.execute(CONVERSION_LOG_TABLE_SQL)

        # Handle reprocessing - drop and recreate all tables
        if reprocess:
            logger.info("Reprocessing: dropping and recreating all tables")
            self._drop_tables()
            self._converted_files = []  # Reset in-memory tracking

            if not self._is_memory_db:
                # Clear conversion log for persistent DB
                try:
                    self.connection.execute(
                        "DELETE FROM conversion_log WHERE status = 'converted';"
                    )
                except Exception as e:
                    logger.error(f"Error clearing conversion log: {e}")

        # Create database tables
        self.db_handler.create_base_tables()

        # Get files to process
        files_to_process = self.list_unconverted_files()

        # Track statistics
        total_articles = 0
        total_sentences = 0
        total_entities = 0

        # Process each file with progress bar
        io_handler = PubMedJsonHandler()
        for file_path in tqdm(
            files_to_process, desc="Converting JSON->Duckdb", unit="file"
        ):
            logger.info(f"Processing file: {file_path}")

            try:
                # Load and parse JSON data
                json_data = io_handler.read(str(file_path))
                data = io_handler.extract_all_dicts(json_data)

                # Process in a transaction
                try:
                    self.connection.begin_transaction()

                    # Insert data into tables
                    ArticleRepository(
                        connection=self.connection
                    ).insert_many_within_transaction(data["articles"])
                    SentenceRepository(
                        connection=self.connection
                    ).insert_many_within_transaction(data["sentences"])
                    EntityRepository(
                        connection=self.connection
                    ).insert_many_within_transaction(data["entities"])

                    self.connection.commit()
                    self._log_conversion(file_path, "converted")
                    processed_files.append(file_path)

                    # Update counts
                    total_articles += len(data["articles"])
                    total_sentences += len(data["sentences"])
                    total_entities += len(data["entities"])

                except Exception as e:
                    self.connection.rollback()
                    logger.error(f"Error processing file {file_path}: {e}")
                    self._log_conversion(file_path, "failed_conversion")
            except Exception as e:
                logger.error(
                    f"Failed to read or parse JSON file {file_path}: {e}"
                )
                self._log_conversion(file_path, "failed_parsing")

        # Create indices for better performance
        self.db_handler.create_indices()

        # Get final counts
        article_count = self.db_handler.get_table_count("articles")
        sentence_count = self.db_handler.get_table_count("sentences")
        entity_count = self.db_handler.get_table_count("entities")

        # Return statistics
        result = {
            "files_processed_this_run": len(processed_files),
            "articles_added_this_run": total_articles,
            "sentences_added_this_run": total_sentences,
            "entities_added_this_run": total_entities,
            "article_count": article_count,
            "sentence_count": sentence_count,
            "entity_count": entity_count,
            "processed_files_this_run_paths": [
                str(f) for f in processed_files
            ],
            "database_path": (
                self.db_file if not self._is_memory_db else ":memory:"
            ),
        }

        if not self._is_memory_db:
            result["total_converted_files_in_log"] = len(
                self.list_converted_files()
            )

        return result

    def _drop_tables(self):
        """Drop all conversion tables in the database"""
        try:
            # Drop tables in the correct order to avoid foreign key constraint issues
            self.connection.execute("DROP TABLE IF EXISTS entities;")
            self.connection.execute("DROP TABLE IF EXISTS sentences;")
            self.connection.execute("DROP TABLE IF EXISTS articles;")
            self.connection.execute("DROP TABLE IF EXISTS conversion_log;")

            # Recreate the conversion log table immediately
            self.connection.execute(CONVERSION_LOG_TABLE_SQL)
        except Exception as e:
            logger.error(f"Error dropping tables: {e}")
            # Even if some tables fail to drop, make sure conversion log exists
            try:
                self.connection.execute(CONVERSION_LOG_TABLE_SQL)
            except Exception as log_error:
                logger.error(
                    f"Error recreating conversion log table: {log_error}"
                )
