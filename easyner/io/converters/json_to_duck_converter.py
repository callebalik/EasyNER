import concurrent.futures
import logging
import os
import queue
import threading
import time
from pathlib import Path
from typing import Any, Optional, Union

import psutil
from tqdm import tqdm

from easyner.io.converters.base import BaseConverter
from easyner.io.database.connection import DatabaseConnection
from easyner.io.database.duckdb_handler import DuckDBHandler
from easyner.io.database.repositories import (
    ArticleRepository,
    EntityRepository,
    SentenceRepository,
)
from easyner.io.database.schemas import CONVERSION_LOG_TABLE_SQL
from easyner.io.database.utils.sql_utils import get_file_hash
from easyner.io.handlers import PubMedJsonHandler
from easyner.io.utils import filter_batch_files, safe_batch_file_index_sort

# Set up logger for the converter
logger = logging.getLogger("easyner.io.converters.json_to_duck_converter")

# Only add handler if not already configured to avoid duplicates
if not logger.handlers:
    # Reset the logger's handlers to avoid duplicates
    logger.handlers = []
    # Add console handler with custom formatter
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(levelname)s - JSON->Duck: %(message)s"),
    )
    logger.addHandler(handler)
    # Prevent propagation to root logger to avoid duplicate messages
    logger.propagate = False


class JsonToDuckConverter(BaseConverter):
    """Converter to transform JSON data into DuckDB database"""

    def __init__(
        self,
        source_dir: Union[str, Path],
        target_dir: Union[str, Path],
        connection: Optional[DatabaseConnection] = None,
        db_file: Optional[str] = None,
        file_pattern: str = "*.json",
        reprocess: bool = False,
        batch_start_index: Optional[int] = None,
        batch_end_index: Optional[int] = None,
        memory_limit: Optional[str] = None,  # Add this parameter
    ):
        """Initialize the JSON to DuckDB converter

        Args:
            source_dir: Directory containing JSON files to convert
            target_dir: Directory where the DuckDB database will be stored
            connection: An existing DuckDB connection (optional)
            db_file: Custom database filename (optional, default is 'easyner.db')
            file_pattern: Pattern to match JSON files (default: "*.json")
            reprocess: Whether to reprocess already converted files (default: False)
            batch_start_index: Only process files with batch index >= this value (optional)
            batch_end_index: Only process files with batch index <= this value (optional)
            memory_limit: Memory limit for DuckDB (optional, default is 50% of system memory or 4096MB)

        """
        super().__init__(source_dir, target_dir)
        self.file_pattern = file_pattern
        self._reprocess = reprocess
        self.batch_start_index = batch_start_index
        self.batch_end_index = batch_end_index

        # Set up db_file path - if not provided, create one in target_dir
        if db_file is None:
            self.db_file = os.path.join(str(target_dir), "easyner.db")
        else:
            self.db_file = db_file

        # Determine a good default memory limit if none was provided
        if memory_limit is None:
            # Use 50% of available system memory by default, with a reasonable cap
            system_memory_mb = psutil.virtual_memory().total // (1024 * 1024)
            memory_limit = f"{min(system_memory_mb // 2, 4096)}MB"
            logger.info(
                f"Auto-configured DuckDB memory limit to {memory_limit}",
            )

        # Initialize the DuckDB handler to handle database operations
        if connection:
            self.db_handler = DuckDBHandler(":memory:")
            self.db_handler.connection = connection
        else:
            self.db_handler = DuckDBHandler(
                self.db_file,
                memory_limit=memory_limit,
            )

        self.connection = self.db_handler.connection
        self._is_memory_db = self.db_file == ":memory:"

        # Create conversion log table if it doesn't exist
        self.connection.execute(CONVERSION_LOG_TABLE_SQL)
        self._converted_files: list[Path] = []

    def list_convertible_files(self) -> list[Path]:
        """List all JSON files in the source directory and sort them by batch index"""
        files = list(self.source_dir.glob(self.file_pattern))

        # Use safe_batch_file_index_sort to sort files
        str_files = [str(f) for f in files]
        sorted_str_files = safe_batch_file_index_sort(str_files)

        # Apply batch index filtering if specified
        if (
            self.batch_start_index is not None
            or self.batch_end_index is not None
        ):
            try:
                sorted_str_files = filter_batch_files(
                    sorted_str_files,
                    start=self.batch_start_index,
                    end=self.batch_end_index,
                )
            except ValueError as e:
                logger.error(f"Error filtering batch files: {e}")
                # Continue with unfiltered files if there's an error

        # Convert back to Path objects
        sorted_files = [Path(f) for f in sorted_str_files]

        return sorted_files

    def list_converted_files(self) -> list[Path]:
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

    def list_unconverted_files(self) -> list[Path]:
        """List all files that have not been converted yet"""
        if self._reprocess:
            return self.list_convertible_files()

        converted_files = set(self.list_converted_files())
        convertible_files = self.list_convertible_files()
        return [f for f in convertible_files if f not in converted_files]

    def _log_conversion(self, file_path: Path, status: str) -> None:
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

    def convert(self, **kwargs) -> dict[str, Any]:
        """Convert JSON files to DuckDB database with memory-aware processing

        This implementation uses a queue-based approach to manage memory usage
        while respecting DuckDB's concurrency model.
        """
        # Process kwargs - keep existing initialization code
        use_memory_db: bool = kwargs.get("use_memory_db", False)
        reprocess_override: Optional[bool] = kwargs.get("reprocess")
        reprocess: bool = (
            self._reprocess
            if reprocess_override is None
            else reprocess_override
        )

        # Process batch index filter overrides - keep existing code
        batch_start: Optional[int] = kwargs.get(
            "batch_start_index",
            self.batch_start_index,
        )
        batch_end: Optional[int] = kwargs.get(
            "batch_end_index",
            self.batch_end_index,
        )

        if batch_start != self.batch_start_index:
            self.batch_start_index = batch_start
        if batch_end != self.batch_end_index:
            self.batch_end_index = batch_end

        # Set up memory monitoring parameters
        max_memory_percent: int = kwargs.get("max_memory_percent", 70)
        max_queue_size: int = kwargs.get("max_queue_size", 30)
        max_reader_threads: int = min(
            kwargs.get("max_reader_threads", 6),
            os.cpu_count() or 2,
        )

        processed_files: list[Path] = []

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
            self._converted_files = []

            if not self._is_memory_db:
                # Clear conversion log for persistent DB
                try:
                    self.connection.execute(
                        "DELETE FROM conversion_log WHERE status = 'converted';",
                    )
                except Exception as e:
                    logger.error(f"Error clearing conversion log: {e}")

        # Create database tables
        self.db_handler.create_base_tables()

        # Get files to process - keep existing code
        all_convertible_files = self.list_convertible_files()
        already_converted_files = (
            self.list_converted_files() if not reprocess else []
        )
        files_to_process = self.list_unconverted_files()

        # Display a summary table of conversion statistics - keep existing code
        table_width = 50
        logger.info("=" * table_width)
        logger.info(f"{'CONVERSION SUMMARY':^{table_width}}")
        logger.info("=" * table_width)
        # Add these lines to show detailed statistics with alignment
        logger.info(
            f"{'Total convertible files:':<30} | {len(all_convertible_files):>10}",
        )
        logger.info(
            f"{'Already converted files:':<30} | {len(already_converted_files):>10}",
        )
        logger.info(
            f"{'Files to be converted now:':<30} | {len(files_to_process):>10}",
        )
        logger.info("-" * table_width)
        logger.info("=" * table_width)
        # Track statistics
        total_articles = 0
        total_sentences = 0
        total_entities = 0

        # Create processing queue for files
        data_queue = queue.Queue(maxsize=max_queue_size)
        stop_event = threading.Event()
        db_lock = (
            threading.Lock()
        )  # Lock for database operations to ensure thread safety

        # Function to check memory usage
        def check_memory() -> float:
            """Check the current memory usage of the system"""
            memory_percentage_usage: float = psutil.virtual_memory().percent
            return memory_percentage_usage

        # Function to read a file and add it to the queue
        def read_file(file_path) -> Optional[Path]:
            try:
                logger.info(f"Reading file: {file_path}")
                # Read and parse JSON data
                io_handler = PubMedJsonHandler()
                json_data = io_handler.read(str(file_path))
                data = io_handler.extract_all_dicts(json_data)

                # Put data in queue, waiting if queue is full
                while not stop_event.is_set():
                    try:
                        # If memory is too high, wait before trying to add more to queue
                        memory_percent = check_memory()
                        if memory_percent > max_memory_percent:
                            logger.warning(
                                f"Memory usage at {memory_percent}% - waiting before adding more data",
                            )
                            time.sleep(2)
                            continue

                        data_queue.put((file_path, data), timeout=1.0)
                        break
                    except queue.Full:
                        if stop_event.is_set():
                            return None
                        time.sleep(0.5)

                return file_path

            except Exception as e:
                logger.error(
                    f"Failed to read or parse JSON file {file_path}: {e}",
                )
                self._log_conversion(file_path, "failed_parsing")
                return None

        # Database consumer function - respects DuckDB's concurrency model
        def process_database() -> None:
            nonlocal total_articles, total_sentences, total_entities, processed_files
            transaction_active = False

            while not stop_event.is_set() or not data_queue.empty():
                try:
                    # Get next item with timeout to allow checking stop_event
                    try:
                        file_path, data = data_queue.get(timeout=1.0)
                    except queue.Empty:
                        continue

                    # Process with database lock to ensure thread safety with DuckDB
                    with db_lock:
                        try:
                            # Begin transaction
                            self.connection.begin_transaction()
                            transaction_active = True

                            # Insert data into tables
                            ArticleRepository(
                                connection=self.connection,
                            ).insert_many_non_transactional(data["articles"])
                            SentenceRepository(
                                connection=self.connection,
                            ).insert_many_non_transactional(data["sentences"])
                            EntityRepository(
                                connection=self.connection,
                            ).insert_many_non_transactional(data["entities"])

                            # Commit transaction
                            self.connection.commit()
                            transaction_active = False

                            # Log success
                            self._log_conversion(file_path, "converted")
                            processed_files.append(file_path)

                            # Update counts
                            total_articles += len(data["articles"])
                            total_sentences += len(data["sentences"])
                            total_entities += len(data["entities"])

                        except Exception as e:
                            if transaction_active:
                                self.connection.rollback()
                                transaction_active = False
                            logger.error(
                                f"Error processing file {file_path}: {e}",
                            )
                            self._log_conversion(
                                file_path,
                                "failed_conversion",
                            )

                    # Mark task as done
                    data_queue.task_done()

                except Exception as e:
                    logger.error(
                        f"Unexpected error in database processor: {e}",
                    )

        # Start the database consumer thread
        db_thread = threading.Thread(target=process_database, daemon=True)
        db_thread.start()

        try:
            with tqdm(
                total=len(files_to_process),
                desc="Converting JSON->Duckdb",
                unit="file",
            ) as pbar:
                # Function to update progress bar with queue info
                def update_progress_description() -> None:
                    queue_size = data_queue.qsize()
                    memory_percent = check_memory()
                    pbar.set_description(
                        f"Converting JSON->Duckdb [Queue: {queue_size}/{max_queue_size}, Mem: {memory_percent:.0f}%]",
                    )

                # Use ThreadPoolExecutor for reading files
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_reader_threads,
                ) as executor:
                    futures = {}
                    remaining_files = list(files_to_process)

                    # Submit initial batch of files
                    initial_batch_size = min(
                        max_reader_threads,
                        len(remaining_files),
                    )
                    for _ in range(initial_batch_size):
                        if not remaining_files:
                            break
                        file_path = remaining_files.pop(0)
                        future = executor.submit(read_file, file_path)
                        futures[future] = file_path

                    # Update progress description initially
                    update_progress_description()

                    # Process results and submit new files as needed
                    while futures and not stop_event.is_set():
                        # Wait for a future to complete
                        done, _ = concurrent.futures.wait(
                            futures,
                            timeout=2.0,
                            return_when=concurrent.futures.FIRST_COMPLETED,
                        )

                        # Update the progress bar description with queue info
                        update_progress_description()

                        if not done:
                            # Check memory usage and possibly adjust queue size
                            memory_percent = check_memory()
                            if memory_percent > max_memory_percent + 5:
                                # Memory pressure is high, we might need to wait
                                logger.warning(
                                    f"Memory usage at {memory_percent}% - waiting before processing more files",
                                )
                                time.sleep(2)
                            continue

                        # Process completed futures
                        for future in done:
                            file_path = futures.pop(future)
                            result = future.result()

                            if result:  # File was processed
                                pbar.update(1)
                                # Update description after progress update
                                update_progress_description()

                            # Check if we should submit another file
                            if (
                                remaining_files
                                and check_memory() < max_memory_percent
                            ):
                                next_file = remaining_files.pop(0)
                                next_future = executor.submit(
                                    read_file,
                                    next_file,
                                )
                                futures[next_future] = next_file

                    # Wait for all data to be processed
                    data_queue.join()

        except KeyboardInterrupt:
            # Force clear the progress bar by moving to a new line
            print("\n", flush=True)

            logger.info("Keyboard interrupt detected. Gracefully stopping...")
            stop_event.set()

            # Give immediate feedback that we're working on stopping
            print("Please wait while cleaning up...", flush=True)

            if db_thread.is_alive():
                # Set a shorter timeout for better user experience
                db_thread.join(timeout=5)
                if db_thread.is_alive():
                    logger.info(
                        "Database operations still running in background.",
                    )

            # Return partial results with immediately available information
            processed_count = len(processed_files)
            logger.info(
                f"Processed {processed_count} files before interruption.",
            )

            return {
                "files_processed_this_run": processed_count,
                "articles_added_this_run": total_articles,
                "sentences_added_this_run": total_sentences,
                "entities_added_this_run": total_entities,
                "processed_files_this_run_paths": [
                    str(f) for f in processed_files
                ],
                "database_path": (
                    self.db_file if not self._is_memory_db else ":memory:"
                ),
                "status": "interrupted",
            }

        except Exception as e:
            logger.error(f"Error during conversion: {e}")
            stop_event.set()
            raise

        finally:
            # Ensure all threads are signaled to stop
            stop_event.set()

            # Ensure database thread completes
            if db_thread.is_alive():
                db_thread.join(timeout=10)

        # Create indices for better performance - keep existing code
        self.db_handler.create_indices()

        # Get final counts - keep existing code
        article_count = self.db_handler.get_table_count("articles")
        sentence_count = self.db_handler.get_table_count("sentences")
        entity_count = self.db_handler.get_table_count("entities")

        # Return statistics - keep existing code
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
            "status": "completed",
        }

        if not self._is_memory_db:
            result["total_converted_files_in_log"] = len(
                self.list_converted_files(),
            )

        return result

    def _drop_tables(self) -> None:
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
                    f"Error recreating conversion log table: {log_error}",
                )


def main(args=None) -> None:
    """Command-line entry point for the converter"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert JSON files to DuckDB database for EasyNER",
    )
    parser.add_argument(
        "source_dir",
        type=str,
        help="Directory containing JSON files to convert",
    )
    parser.add_argument(
        "target_dir",
        type=str,
        help="Directory where the DuckDB database will be stored",
    )
    parser.add_argument(
        "--db-file",
        type=str,
        default=None,
        help="Custom database filename (default: 'easyner.db' in target_dir)",
    )
    parser.add_argument(
        "--file-pattern",
        type=str,
        default="*.json",
        help="Pattern to match JSON files (default: '*.json')",
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Reprocess already converted files",
    )
    parser.add_argument(
        "--batch-start",
        type=int,
        default=None,
        help="Only process files with batch index >= this value",
    )
    parser.add_argument(
        "--batch-end",
        type=int,
        default=None,
        help="Only process files with batch index <= this value",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    # Set up logging based on command line arguments
    log_level = getattr(logging, args.log_level)
    logger.setLevel(log_level)

    # Log the arguments
    logger.info(f"Source directory: {args.source_dir}")
    logger.info(f"Target directory: {args.target_dir}")
    logger.info(f"DB File: {args.db_file or 'default (easyner.db)'}")
    logger.info(f"File pattern: {args.file_pattern}")
    logger.info(f"Reprocess: {args.reprocess}")
    if args.batch_start is not None or args.batch_end is not None:
        logger.info(
            f"Batch index range: {args.batch_start} to {args.batch_end}",
        )

    try:
        # Create the converter and run the conversion
        converter = JsonToDuckConverter(
            source_dir=args.source_dir,
            target_dir=args.target_dir,
            db_file=args.db_file,
            file_pattern=args.file_pattern,
            reprocess=args.reprocess,
            batch_start_index=args.batch_start,
            batch_end_index=args.batch_end,
        )

        try:
            result = converter.convert()

            # Check if conversion was interrupted
            if result.get("status") == "interrupted":
                logger.info("Conversion was interrupted by user.")
                logger.info(
                    f"Files processed before interruption: {result['files_processed_this_run']}",
                )
                logger.info(
                    f"Articles added before interruption: {result['articles_added_this_run']}",
                )
                logger.info(f"Database path: {result['database_path']}")
                return 130  # Standard exit code for SIGINT

            # Print statistics for completed conversion
            logger.info("Conversion completed successfully!")
            logger.info(
                f"Files processed: {result['files_processed_this_run']}",
            )
            logger.info(f"Articles added: {result['articles_added_this_run']}")
            logger.info(
                f"Sentences added: {result['sentences_added_this_run']}",
            )
            logger.info(f"Entities added: {result['entities_added_this_run']}")
            logger.info(
                f"Total articles in database: {result['article_count']}",
            )
            logger.info(
                f"Total sentences in database: {result['sentence_count']}",
            )
            logger.info(
                f"Total entities in database: {result['entity_count']}",
            )
            logger.info(f"Database path: {result['database_path']}")

            return 0

        except KeyboardInterrupt:
            logger.info("Conversion interrupted by user. Exiting gracefully.")
            return 130  # Standard exit code for SIGINT

    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        import traceback

        logger.debug(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
