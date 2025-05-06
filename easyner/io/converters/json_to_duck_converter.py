import json
import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Union, Optional

import pandas as pd

from easyner.io.converters.base import BaseConverter
from easyner.io.database.duckdb_handler import DuckDBHandler
from easyner.io.database.repositories import (
    ArticleRepository,
    SentenceRepository,
    EntityRepository,
)

from easyner.io.database.schemas import CONVERSION_LOG_TABLE_SQL
from easyner.io.database.utils.column_names import (
    ARTICLE_ID,
    SENTENCE_ID,
    TEXT,
    START_CHAR,
    END_CHAR,
    INFERENCE_MODEL,
    INFERENCE_MODEL_METADATA,
    TITLE,
)
from easyner.io import get_io_handler
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
        """
        super().__init__(source_dir, target_dir)
        self.file_pattern = file_pattern

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
        self._reprocess = reprocess
        self.connection = self.db_handler.connection
        self.db_handler.connection.execute(CONVERSION_LOG_TABLE_SQL)

        self._converted_files = []
        self._is_memory_db = self.db_file == ":memory:"

    def list_convertible_files(self) -> List[Path]:
        """List all JSON files in the source directory"""
        return list(self.source_dir.glob(self.file_pattern))

    def list_converted_files(self) -> List[Path]:
        """List all files that have already been successfully converted by querying the database."""
        if self._is_memory_db:  # In-memory DB won't persist this across runs
            return (
                self._converted_files
            )  # Keep current behavior for in-memory for now

        query = (
            "SELECT file_path FROM conversion_log WHERE status = 'converted';"
        )
        try:
            results = self.connection.execute(query).fetchall()
            return [Path(row[0]) for row in results]
        except (
            Exception
        ) as e:  # Handle case where table might not exist yet or other DB errors
            # Log the error
            print(f"Error querying conversion_log: {e}")
            return []

    def list_unconverted_files(self) -> List[Path]:
        """List all files that have not been converted yet"""
        # When reprocess is True, return all files as "unconverted"
        # This ensures they're all reprocessed
        if self._reprocess:
            print(
                f"Reprocess flag is True, returning all files as unconverted"
            )
            return self.list_convertible_files()

        # Normal behavior - only return files not already in conversion log
        converted_files = set(self.list_converted_files())
        convertible_files = self.list_convertible_files()
        return [f for f in convertible_files if f not in converted_files]

    def _log_conversion(self, file_path: Path, status: str):
        """Logs the conversion attempt to the database."""
        if self._is_memory_db:
            if status == "converted":
                self._converted_files.append(
                    file_path
                )  # Maintain in-memory list for memory DB
            return

        try:
            # Ensure the conversion_log table exists
            table_exists = self.connection.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'conversion_log'"
            ).fetchone()[0]

            if not table_exists:
                # Create the table if it doesn't exist
                self.connection.execute(CONVERSION_LOG_TABLE_SQL)

            # Now proceed with the insert/update
            file_hash = get_file_hash(file_path)
            file_size = file_path.stat().st_size
            timestamp = pd.Timestamp.now()

            # Use parameterized query to prevent SQL injection
            self.connection.execute(
                """
                INSERT INTO conversion_log (file_path, file_name, file_hash, file_size_bytes, conversion_timestamp, status)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_path) DO UPDATE SET
                    file_hash = excluded.file_hash,
                    file_size_bytes = excluded.file_size_bytes,
                    conversion_timestamp = excluded.conversion_timestamp,
                    status = excluded.status;
                """,
                (
                    str(file_path.resolve()),
                    file_path.name,
                    file_hash,
                    file_size,
                    timestamp,
                    status,
                ),
            )
        except Exception as e:
            print(f"Error logging conversion: {e}")
            # Continue processing - if we can't log, we'll still continue with conversion

    def _process_json_file(
        self, file_path: Path
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process a single JSON file and extract structured data

        Args:
            file_path: Path to the JSON file

        Returns:
            Dictionary containing articles, sentences and entities data
        """
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        articles_data = []
        sentences_data = []
        entities_data = []

        # Resolved file path to use as foreign key to conversion_log
        resolved_file_path = str(file_path.resolve())

        # Process data in a single pass
        for article_id_str, article_content in data.items():
            # Convert article_id from string to integer
            article_id = int(article_id_str)

            # Articles table - include source_file reference
            articles_data.append(
                {
                    ARTICLE_ID: article_id,
                    TITLE: article_content.get("title", ""),
                    "source_file": resolved_file_path,  # Add source file reference
                }
            )

            sentences = article_content.get("sentences", [])

            # Process sentences and entities
            for sentence_idx, sentence in enumerate(sentences):
                sentences_data.append(
                    {
                        ARTICLE_ID: article_id,
                        SENTENCE_ID: sentence_idx,
                        TEXT: sentence.get("text", ""),
                    }
                )

                # Get entities once
                entities = sentence.get("entities", [])
                entity_spans = sentence.get("entity_spans", [])

                # Extend entities_data with all valid entities at once
                entities_data.extend(
                    {
                        ARTICLE_ID: article_id,
                        SENTENCE_ID: sentence_idx,
                        TEXT: entity,
                        START_CHAR: span[0] if span else None,
                        END_CHAR: span[1] if span else None,
                        INFERENCE_MODEL: None,
                        INFERENCE_MODEL_METADATA: None,
                    }
                    for entity, span in zip(entities, entity_spans)
                    if entity  # Skip empty entities
                )

        return {
            "articles": articles_data,
            "sentences": sentences_data,
            "entities": entities_data,
        }

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
        # Check if we should use an in-memory database
        use_memory_db = kwargs.get("use_memory_db", False)
        # Allow method-level override of reprocess flag
        reprocess_override = kwargs.get("reprocess")
        reprocess = (
            self._reprocess
            if reprocess_override is None
            else reprocess_override
        )

        processed_this_run_paths = []  # To track files processed in this run

        # Initialize database if in-memory database requested
        logger.debug(
            f"JsonToDuckConverter.convert() called. reprocess = {reprocess}, use_memory_db = {use_memory_db}"
        )

        if use_memory_db and not self._is_memory_db:
            self.db_handler = DuckDBHandler(":memory:")
            self.connection = self.db_handler.connection
            self._is_memory_db = True
            # When creating a new in-memory database, we need to create the conversion_log table
            self.connection.execute(CONVERSION_LOG_TABLE_SQL)

        # Create database tables
        self.db_handler.create_base_tables()

        # For reprocessing, we need to ensure that source_file column exists first
        if reprocess:
            self._ensure_source_file_column()

        # Make sure conversion_log table exists in all cases
        if reprocess:
            # If reprocessing, ensure the conversion_log table exists before trying to delete from it
            try:
                # Check if table exists before running DELETE
                table_exists = self.connection.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'conversion_log'"
                ).fetchone()[0]

                if table_exists:
                    # If reprocessing, clear the conversion log
                    self.connection.execute(
                        "DELETE FROM conversion_log WHERE status = 'converted';"
                    )
                    logger.debug("Cleared conversion_log for reprocessing")
                else:
                    # Create the conversion_log table if it doesn't exist
                    self.connection.execute(CONVERSION_LOG_TABLE_SQL)
                    logger.debug("Created conversion_log table")

                self._converted_files = []  # Reset for in-memory consistency
            except Exception as e:
                logger.error(f"Error handling reprocess flag: {e}")
                # Continue processing - if we can't clear the log, we'll still try to process all files

            # In reprocess mode, we need to process all convertible files regardless of conversion status
            files_to_process = self.list_convertible_files()
            logger.info(
                f"Reprocessing all files, found {len(files_to_process)} files."
            )
        else:
            # For persistent DB, list_converted_files queries the DB.
            # For in-memory, it uses self._converted_files which might have state
            # if convert is called multiple times on the same in-memory instance.
            files_to_process = self.list_unconverted_files()

        total_articles_in_run = 0  # RENAMED for clarity
        total_sentences_in_run = 0  # RENAMED for clarity
        total_entities_in_run = 0  # RENAMED for clarity

        # Process each file
        io_handler = PubMedJsonHandler()
        for file_path in files_to_process:
            print(f"Processing file: {file_path}")

            try:  # ADDED: Outer try-except for file reading/parsing before DB transaction
                json_data = io_handler.read(str(file_path))
                data = io_handler.extract_all_dicts(json_data)
                print(
                    f"Successfully extracted data from {file_path}: found {len(data['articles'])} articles"
                )
            except Exception as e:
                print(f"Failed to read or parse JSON file {file_path}: {e}")
                self._log_conversion(file_path, "failed_parsing")
                continue  # Move to the next file

            # Transaction block for database operations for a single file
            try:
                self.connection.begin_transaction()  # START TRANSACTION

                # Get the resolved file path to use as key
                resolved_file_path = str(file_path.resolve())

                # If we're reprocessing, first delete all data from this file
                if reprocess:
                    try:
                        logger.debug(
                            f"Reprocessing file {file_path}, cleaning up existing data"
                        )

                        # Check if source_file column exists in articles table
                        source_column_exists = self.connection.execute(
                            """
                            SELECT COUNT(*)
                            FROM information_schema.columns
                            WHERE table_name = 'articles' AND column_name = 'source_file'
                        """
                        ).fetchone()[0]

                        logger.debug(
                            f"source_file column exists: {source_column_exists}"
                        )

                        # Get all article IDs that need to be deleted
                        if source_column_exists:
                            # Use the source_file column if it exists
                            article_ids_query = self.connection.execute(
                                "SELECT article_id FROM articles WHERE source_file = ?",
                                (resolved_file_path,),
                            ).fetchall()
                            logger.debug(
                                f"Found {len(article_ids_query)} articles by source_file"
                            )
                        else:
                            # If source_file column doesn't exist, find by article IDs from the current file
                            article_ids_from_file = [
                                article[ARTICLE_ID]
                                for article in data["articles"]
                            ]
                            article_ids_str_from_file = ", ".join(
                                str(id) for id in article_ids_from_file
                            )
                            logger.debug(
                                f"Article IDs from current file: {article_ids_str_from_file}"
                            )

                            article_ids_query = self.connection.execute(
                                f"SELECT article_id FROM articles WHERE article_id IN ({article_ids_str_from_file})"
                            ).fetchall()
                            logger.debug(
                                f"Found {len(article_ids_query)} matching articles in database"
                            )

                        article_ids = [row[0] for row in article_ids_query]

                        if article_ids:
                            # Log article IDs to be deleted
                            article_ids_str = ",".join(
                                str(id) for id in article_ids
                            )
                            logger.debug(
                                f"Will delete data for article IDs: {article_ids_str}"
                            )

                            # First check how many entities and sentences will be deleted
                            entity_count = self.connection.execute(
                                f"SELECT COUNT(*) FROM entities WHERE article_id IN ({article_ids_str})"
                            ).fetchone()[0]

                            sentence_count = self.connection.execute(
                                f"SELECT COUNT(*) FROM sentences WHERE article_id IN ({article_ids_str})"
                            ).fetchone()[0]

                            logger.debug(
                                f"Will delete {entity_count} entities and {sentence_count} sentences"
                            )

                            # Delete in the correct order to respect foreign keys
                            # First delete entities that reference these articles
                            self.connection.execute(
                                f"DELETE FROM entities WHERE article_id IN ({article_ids_str})"
                            )
                            logger.debug("Deleted entities")

                            # Then delete sentences that reference these articles
                            self.connection.execute(
                                f"DELETE FROM sentences WHERE article_id IN ({article_ids_str})"
                            )
                            logger.debug("Deleted sentences")

                            # Finally, delete the articles themselves
                            self.connection.execute(
                                f"DELETE FROM articles WHERE article_id IN ({article_ids_str})"
                            )
                            logger.debug("Deleted articles")

                            logger.info(
                                f"Successfully deleted {len(article_ids)} existing articles for reprocessing"
                            )
                        else:
                            logger.debug(
                                "No existing articles found to delete for this file"
                            )
                    except Exception as e:
                        logger.error(f"Error during reprocessing cleanup: {e}")
                        # Continue anyway, the insertion may still succeed

                # Log articles being inserted for debugging
                article_ids_being_inserted = [
                    str(article[ARTICLE_ID]) for article in data["articles"]
                ]
                logger.debug(
                    f"Article IDs being inserted: {', '.join(article_ids_being_inserted)}"
                )

                # Check for duplicates in the database
                try:
                    for article_id in [
                        article[ARTICLE_ID] for article in data["articles"]
                    ]:
                        exists = self.connection.execute(
                            f"SELECT COUNT(*) FROM articles WHERE article_id = {article_id}"
                        ).fetchone()[0]
                        if exists:
                            logger.warning(
                                f"Article ID {article_id} already exists in database before insertion"
                            )
                except Exception as e:
                    logger.error(f"Error checking for duplicate articles: {e}")

                # Insert new data
                try:
                    logger.debug(f"Inserting {len(data['articles'])} articles")
                    ArticleRepository(
                        connection=self.connection
                    ).insert_many_within_transaction(data["articles"])
                    logger.debug("Articles inserted successfully")
                except Exception as e:
                    logger.error(f"Error inserting articles: {e}")
                    raise

                SentenceRepository(
                    connection=self.connection
                ).insert_many_within_transaction(data["sentences"])
                EntityRepository(
                    connection=self.connection
                ).insert_many_within_transaction(data["entities"])
                self.connection.commit()  # COMMIT TRANSACTION

                self._log_conversion(file_path, "converted")
                self._ensure_source_file_column()  # Ensure source_file column exists after tables have been created

                # Add to processed_this_run_paths
                processed_this_run_paths.append(file_path)

                # Update counts for this run
                # For reprocessing, always count articles, sentences, and entities as additions
                total_articles_in_run += len(data["articles"])
                total_sentences_in_run += len(data["sentences"])
                total_entities_in_run += len(data["entities"])

            except Exception as e:
                self.connection.rollback()  # ROLLBACK TRANSACTION
                print(f"Error processing file {file_path}: {e}")
                self._log_conversion(file_path, "failed_conversion")
                continue  # Move to the next file

        # Create indices for better performance
        self.db_handler.create_indices()

        # Get final counts using the db_handler
        article_count = self.db_handler.get_table_count("articles")
        sentence_count = self.db_handler.get_table_count("sentences")
        entity_count = self.db_handler.get_table_count("entities")

        # Return statistics
        result = {
            "files_processed_this_run": len(processed_this_run_paths),
            "articles_added_this_run": total_articles_in_run,
            "sentences_added_this_run": total_sentences_in_run,
            "entities_added_this_run": total_entities_in_run,
            "article_count": article_count,
            "sentence_count": sentence_count,
            "entity_count": entity_count,
            "processed_files_this_run_paths": [
                str(f) for f in processed_this_run_paths
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

    def _ensure_source_file_column(self):
        """
        Ensures that the source_file column exists in the articles table.
        This method is called after _log_conversion to ensure tables have been created.
        """
        try:
            # Check if source_file column exists in articles table
            column_exists = self.connection.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = 'articles' AND column_name = 'source_file'
            """
            ).fetchone()[0]

            if not column_exists:
                print("Adding source_file column to articles table")
                # Add column without constraint (DuckDB doesn't support adding columns with constraints in ALTER TABLE)
                self.connection.execute(
                    "ALTER TABLE articles ADD COLUMN source_file TEXT"
                )
        except Exception as e:
            print(f"Error adding source_file column to articles table: {e}")
            # Continue - the column may already exist or we can proceed without it
