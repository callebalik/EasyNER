import cProfile
import logging
import multiprocessing
import os
import pstats
import queue
import sqlite3
import threading
import time
from multiprocessing import Lock, Value

from tqdm import tqdm

from easyner.database.sqlite_backend.core.db_manager import (  # Import DatabaseManager
    DatabaseManager,
)
from easyner.database.sqlite_backend.core.logger import (  # Assuming BaseLogger is in core.core_classes
    BaseLogger,
)
from easyner.utils.logging.log_formatter import TableFormatter  # Import TableFormatter


def run_with_profiling(func, prof_filename):
    """Runs a function under cProfile profiling and saves results to a file.

    Args:
        func (callable): The function to profile.
        prof_filename (str): Filename to save profiling results.

    Returns:
        The return value of the profiled function.

    """
    profiler = cProfile.Profile()
    profiler.enable()
    result = func()  # Execute the function
    profiler.disable()
    profiler.dump_stats(prof_filename)
    # Optional: Print top stats to console for quick overview
    # stats = pstats.Stats(prof_filename)
    # stats.sort_stats('cumulative').print_stats(10) # Print top 10 functions by cumulative time
    return result


class Reader:
    """Reader class to read data from a database, process it, and put it into a queue."""

    def __init__(
        self,
        database_manager: DatabaseManager,  # Use DatabaseManager instead of conn_params
        query,
        batch_size,
        process_function,
        data_queue,
        logger=None,
        queue_size_backpressure_threshold=50,
        progress_queue=None,  # Added progress_queue
        profiling_enabled=False,  # Added profiling_enabled
        total_count=None,  # Added total_count
        shared_processed_count=None,  # Added shared_processed_count # type: ignore
        lock: Lock = None,  # Added lock # type: ignore
    ):
        """Initializes the Reader.

        Args:
            database_manager (DatabaseManager): DatabaseManager instance for connection management. # Updated type
            query (str): SQL query to execute.
            batch_size (int): Number of rows to fetch per batch.
            process_function (callable): Function to process each batch of data.
            data_queue (queue.Queue): Queue to put processed data into.
            logger (logging.Logger, optional): Logger instance. Defaults to a basic logger.
            queue_size_backpressure_threshold (int, optional): Queue size threshold for backpressure. Defaults to 50.
            progress_queue (queue.Queue, optional): Queue to send progress updates.
            profiling_enabled (bool, optional): Enable profiling for Reader's run method. Defaults to False.

        """
        self.database_manager = database_manager  # Store DatabaseManager
        self.query = query
        self.batch_size = batch_size
        self.process_function = process_function
        self.data_queue = data_queue
        self.logger = (
            logger or self._setup_logger()
        )  # Use provided logger or setup default
        self.qsize_backbpressure_threshold = queue_size_backpressure_threshold
        self.progress_queue = progress_queue  # Store progress_queue
        self.shared_processed_count: (
            shared_processed_count  # Initialize shared processed count
        )
        self.lock = lock
        self.total_count = total_count  # Store total_count
        self.profiling_enabled = profiling_enabled  # Store profiling flag

    def _setup_logger(self):
        """Sets up a basic logger for the Reader."""
        if self.logger:  # Use external logger if provided
            return self.logger

        logger = BaseLogger(
            logger_name=__name__ + "Reader",
            log_level=logging.DEBUG,
            enable_console_log=True,
        )
        return logger.logger  # Return the logger object from BaseLogger

    def _read_and_process(self):
        """Reads data from the database, processes it in batches, and puts it into the data queue."""
        conn = None
        try:
            conn = (
                self.database_manager.get_connection()
            )  # Get connection from DatabaseManager
            cursor = conn.cursor()
            cursor.execute(self.query)
            total_processed = 0
            log_queue_size_interval = (
                10  # Log queue size every N batches (adjust as needed)
            )
            batch_counter = 0
            queue_check_interval = 30  # Check queue size every X seconds when queue is full (adjust as needed)

            while True:
                batch = cursor.fetchmany(self.batch_size)
                if not batch:
                    self.logger.debug("Reader thread: no more data from cursor")
                    break  # No more data

                processed_batch = self.process_function(
                    batch,
                    self.database_manager.get_connection_params(),
                )  # Pass connection params via manager

                if processed_batch:  # Only put into queue if there is processed data
                    # Removed backpressure logic - let Queue handle blocking
                    self.data_queue.put(processed_batch)  # Put batch into queue
                    total_processed += len(
                        processed_batch,
                    )  # Count processed items, not fetched

                    if (
                        self.progress_queue
                    ):  # Send progress update if progress_queue exists
                        self.progress_queue.put(len(processed_batch))

                    batch_counter += 1
                    if batch_counter % log_queue_size_interval == 0:
                        queue_size = self.data_queue.qsize()
                        self.logger.debug(
                            f"Reader thread processed batch - Queue size: {queue_size} (total processed in thread: {total_processed})",
                        )
                    else:
                        self.logger.debug(
                            f"Reader thread processed batch of {len(processed_batch)} items (total: {total_processed})",
                        )

                if (
                    self.total_count is not None
                    and self.shared_processed_count is not None
                    and self.lock is not None
                ):
                    with self.lock:
                        if self.shared_processed_count.value >= self.total_count:
                            self.logger.info(
                                f"Reader thread: total processed ({self.shared_processed_count.value}) >= total_count ({self.total_count}). Exiting read loop.",
                            )
                            break
                        else:
                            self.shared_processed_count.value += len(processed_batch)

        except sqlite3.Error as e:
            self.logger.error(f"Database error in Reader thread: {e}")
        except Exception as e:
            self.logger.error(f"Error in Reader thread process: {e}")
        finally:
            if conn:
                self.database_manager.release_connection(
                    conn,
                )  # Release connection via DatabaseManager
            self.logger.debug("Reader thread finished.")
            self.data_queue.put(
                None,
            )  # Ensure sentinel value is ALWAYS added to queue at the end
            self.logger.debug("Reader thread: added sentinel value to queue.")

    def run(
        self,
        num_threads=1,
    ) -> None:  # `num_threads` is now OPTIONAL with default 1
        """Runs the Reader process, in single or multi-threaded mode based on num_threads.

        Args:
            num_threads (int, optional): Number of reader threads to use. Defaults to 1 (single-threaded).
            If > 1, Reader will manage its own thread pool. If 1 or not provided,
            single-threaded (intended for ReaderWriterPair).

        Keyword Args:
            profiling_enabled (bool, optional): If True, enables profiling for this run. Defaults to False.
            Profiling results are saved to 'Reader_run_profile.prof'.
            To analyze, use `import pstats; pstats.Stats('Reader_run_profile.prof').sort_stats('cumulative').print_stats(30)`

        """
        prof_filename = "Reader_run_profile.prof"  # Filename for profiling data

        if self.profiling_enabled:
            self.logger.info(
                f"Reader thread starting with profiling enabled. Results will be in '{prof_filename}'",
            )
            run_with_profiling(
                lambda: self._run_internal(num_threads),
                prof_filename,
            )  # Use lambda to call internal run with args
        else:
            self._run_internal(num_threads)

    def _run_internal(
        self,
        num_threads=1,
    ):  # `num_threads` is now OPTIONAL with default 1
        """Runs the Reader process, in single or multi-threaded mode based on num_threads.

        Args:
            num_threads (int, optional): Number of reader threads to use. Defaults to 1 (single-threaded).
            If > 1, Reader will manage its own thread pool. If 1 or not provided,
            single-threaded (intended for ReaderWriterPair).

        """
        if num_threads <= 1:  # Single-threaded execution (for ReaderWriterPair usage)
            self.logger.info(
                "Reader thread starting read and process (single-threaded mode).",
            )
            self._read_and_process()
            self.logger.info(
                "Reader thread finished read and process (single-threaded mode).",
            )
        else:  # Multi-threaded execution (for standalone Reader usage)
            threads = []
            self.logger.info(
                f"Starting {num_threads} reader threads (multi-threaded mode).",
            )
            for _ in range(num_threads):
                thread = threading.Thread(target=self._read_and_process)
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            self.data_queue.put(
                None,
            )  # Sentinel value for multi-threaded standalone Reader (if needed - depends on use case)
            self.logger.info(
                "Reader threads finished and sentinel value added to queue (multi-threaded mode).",
            )

    def get_queue(self):
        """Returns the data queue managed by this Reader.
        This allows external access to the queue to share with the Writer.
        """
        return self.data_queue


class Writer:
    """Writer class as before, now not directly managing the queue."""

    def __init__(
        self,
        database_manager: DatabaseManager,  # Use DatabaseManager instead of conn_params
        write_function,
        data_queue,
        logger=None,
        profiling_enabled=False,
        batch_chunking=2,
        num_reader_threads: int = 2,
    ):
        """Initializes the Writer.

        Args:
            database_manager (DatabaseManager): DatabaseManager instance for connection management. # Updated type
            write_function (callable): Function to write a batch of data to the database.
            data_queue (queue.Queue): Queue to get data from.
            logger (logging.Logger, optional): Logger instance. Defaults to a basic logger.
            progress_queue (queue.Queue, optional): Queue to send progress updates.
            profiling_enabled (bool, optional): Enable profiling for Writer's run method. Defaults to False.

        """
        self.database_manager = database_manager  # Store DatabaseManager
        self.write_function = write_function
        self.data_queue = data_queue
        self.logger = (
            logger or self._setup_logger()
        )  # Use provided logger or setup default
        self.written_count = 0  # Initialize processed_count for Writer
        self.profiling_enabled = profiling_enabled  # Store profiling flag
        self.batch_chunking = (
            batch_chunking  # Accumulte multiple batches before writing
        )
        self.num_reader_threads = num_reader_threads  # Store number of reader threads

    def _setup_logger(self):
        """Sets up a basic logger for the Writer."""
        if self.logger:  # Use external logger if provided
            return self.logger

        logger = BaseLogger(
            logger_name=__name__ + "Writer",
            log_level=logging.DEBUG,
            enable_console_log=True,
        )
        return logger.logger  # Return the logger object from BaseLogger

    def writer_process(self) -> None:
        """Processes data from the queue and writes it to the database."""
        conn: sqlite3.Connection = None
        try:
            conn = (
                self.database_manager.get_connection()
            )  # Get connection from DatabaseManager
            cursor = conn.cursor()

            sentinel_count = 0  # Count of sentinels received
            num_readers = (
                self.num_reader_threads
            )  # Get number of readers from the queue

            while True:
                batch = (
                    self.data_queue.get()
                )  # Get batch from queue (same queue as Reader's)
                if batch is None:  # Sentinel value received
                    self.data_queue.task_done()  # Signal task completion for sentinel

                    sentinel_count += 1
                    if sentinel_count == num_readers:  # All readers have finished
                        self.logger.debug(
                            f"Writer thread received all {num_readers} sentinels. Exiting writer process.",
                        )
                        break
                    else:
                        self.logger.debug(
                            f"Writer thread received a sentinel. Total sentinels: {sentinel_count} (of {num_readers})",
                        )
                        continue  # Skip processing sentinel

                try:
                    self.write_function(batch, cursor, conn)  # Call the write function
                    self.written_count += len(batch)
                    self.logger.debug(
                        f"Writer thread wrote a batch of {len(batch)} items (total written so far: {self.written_count})",
                    )
                except Exception as write_e:
                    conn.rollback()  # Rollback transaction in case of write error
                    self.logger.error(
                        f"Error in write_function: {write_e}. Transaction rolled back for current batch.",
                    )
                    # Consider more sophisticated error handling here - e.g., retry mechanism, error queue

                self.data_queue.task_done()  # Signal task completion for the batch

            conn.commit()  # Final commit after all batches are written
            self.logger.info(
                f"Writer thread finished processing and wrote a total of {self.written_count} items.",
            )

        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(
                f"Database error in Writer thread: {e}. Transaction rolled back.",
            )
        except Exception as e:
            self.logger.error(f"Error in Writer thread: {e}")
        finally:
            if conn:
                self.database_manager.release_connection(
                    conn,
                )  # Release connection via DatabaseManager
            self.logger.debug("Writer thread finished.")

    def run(self) -> None:
        """Runs the Writer process.

        Keyword Args:
            profiling_enabled (bool, optional): If True, enables profiling for this run. Defaults to False.
            Profiling results are saved to 'Writer_run_profile.prof'.
            To analyze, use `import pstats; pstats.Stats('Writer_run_profile.prof').sort_stats('cumulative').print_stats(30)`

        """
        prof_filename = "Writer_run_profile.prof"  # Filename for profiling data

        if self.profiling_enabled:
            self.logger.info(
                f"Writer thread starting with profiling enabled. Results will be in '{prof_filename}'",
            )
            run_with_profiling(self._run_internal, prof_filename)
        else:
            self._run_internal()

    def _run_internal(self):
        """Internal run method for the Writer thread."""
        self.logger.info("Starting writer thread.")
        writer_thread = threading.Thread(target=self.writer_process)
        writer_thread.start()
        writer_thread.join()  # Wait for writer thread to finish
        self.logger.info("Writer thread finished.")


class ReaderWriterPair:
    """Manages a Reader and Writer pair, creating and connecting their data queue internally."""

    def __init__(
        self,
        database_manager: DatabaseManager,  # Use DatabaseManager instead of conn_params
        reader_query,
        batch_size,
        process_function,
        write_function,
        num_reader_threads=4,
        logger=None,
        max_queue_size=50,
        writer_batch_chunking=2,  # ow
        process_title: str = None,
        total_count: int = None,
        profiling_reader_enabled=False,  # Added profiling_enabled
        profiling_writer_enabled=False,  # Added profiling_enabled):
    ):
        """Initializes the ReaderWriterPair, creating the queue and instances of Reader and Writer.

        Args:
            database_manager (DatabaseManager): DatabaseManager instance for connection management. # Updated type
            reader_query (str): SQL query for the Reader.
            batch_size (int): Batch size for reading.
            process_function (callable): Processing function for Reader.
            write_function (callable): Writing function for Writer.
            num_reader_threads (int, optional): Number of reader threads. Defaults to 4.
            logger (logging.Logger, optional): Logger instance. Defaults to a basic logger.
            max_queue_size (int, optional): Maximum size of the internal data queue. Defaults to 50.
            total_count (int, optional): Total count of items to process. Defaults to None.
            profiling_reader_enabled (bool, optional): Enable profiling for Reader. Defaults to False.
            profiling_writer_enabled (bool, optional): Enable profiling for Writer. Defaults to False.

        """
        self.database_manager = database_manager  # Store DatabaseManager
        self.reader_query = reader_query
        self.batch_size = batch_size
        self.process_function = process_function
        self.write_function = write_function
        self.num_reader_threads = num_reader_threads
        self.logger = (
            logger or self._setup_logger()
        )  # Use provided logger or setup default
        self.data_queue = queue.Queue(
            maxsize=max_queue_size,
        )  # Pair class creates the queue
        self.progress_queue = (
            queue.Queue()
        )  # Create progress queue for aggregated progress
        self.aggregation_thread_stop_event = (
            threading.Event()
        )  # Event to stop aggregation thread

        self.total_count = total_count  # Store total_count
        if total_count is not None:
            self.shared_processed_count = multiprocessing.Value(
                "i",
                0,
            )  # use multiprocessing.Value
            self.shared_processed_lock = (
                multiprocessing.Lock()
            )  # use multiprocessing.Lock
        else:
            self.shared_processed_count = None
            self.shared_processed_lock = None

        self.process_title = process_title  # Store process title if provided

        self.pbar_aggregated = tqdm(
            total=total_count,
            desc=f"Total Progress {'for ' + process_title if process_title else ''}",
        )  # Initialize tqdm for aggregated progress

        self.profiling_reader_enabled = os.getenv(
            "PROFILING_READER_ENABLED",
            profiling_reader_enabled,
        )  # Added profiling_reader_enabled
        self.profiling_writer_enabled = os.getenv(
            "PROFILING_WRITER_ENABLED",
            profiling_writer_enabled,
        )  # Added profiling_writer_enabled
        self.writer_batch_chunking = (
            writer_batch_chunking  # Added writer_batch_chunking
        )
        # Instantiate Reader and Writer, passing the *same* data_queue to both
        self.reader = Reader(
            self.data_queue,
            database_manager=database_manager,  # Pass DatabaseManager instance
            reader_query=reader_query,
            batch_size=batch_size,
            process_function=process_function,
            logger=self.logger,
            queue_size_backpressure_threshold=max_queue_size
            - 5,  # Adjusted backpressure threshold for when readers start to pause to avoid maxing out queue
            progress_queue=self.progress_queue,  # Pass progress_queue to Reader
            profiling_enabled=self.profiling_reader_enabled,  # Profiling reader/writer individually if needed, by default False here, controlled at ReaderWritersPair level
            total_count=self.total_count,
            shared_processed_count=self.shared_processed_count,  # Pass shared value
            lock=self.shared_processed_lock,  # Pass lock
        )
        self.writer = Writer(
            self.data_queue,
            database_manager=database_manager,  # Pass DatabaseManager instance
            write_function=write_function,
            logger=self.logger,
            profiling_enabled=self.profiling_writer_enabled,  # Profiling reader/writer individually if needed, by default False here, controlled at ReaderWriterPair level
            batch_chunking=self.writer_batch_chunking,
            num_reader_threads=self.num_reader_threads,
        )

    def _progress_aggregation_process(self):  # Aggregation thread function
        """Continuously reads progress updates from the progress_queue, aggregates them,
        and updates the tqdm progress bar.
        """
        read_total = 0  # Renamed to read_total to clarify its purpose
        update_interval = 10000  # Update progress bar every N items (adjust as needed)
        while not self.aggregation_thread_stop_event.is_set():
            try:
                progress_increment = self.progress_queue.get(
                    timeout=1,
                )  # Get read batch size
                read_total += progress_increment  # Aggregate read progress
                queue_size = self.data_queue.qsize()
                written_count = self.writer.written_count

                # Format numbers with comma separators for readability
                formatted_written = f"{written_count:,}"
                formatted_queue_size = f"{queue_size:,}"
                formatted_read_total = (
                    f"{read_total:,}"  # Optional: format read_total as well
                )

                self.pbar_aggregated.update(
                    progress_increment,
                )  # Update progress bar based on read progress
                self.pbar_aggregated.set_postfix(
                    written=formatted_written,  # Use formatted written count
                    InQueue=formatted_queue_size,  # Use formatted queue size
                    read=formatted_read_total,  # Optional: show formatted read count in postfix if desired
                )
                self.progress_queue.task_done()
            except queue.Empty:  # No progress update in queue within timeout
                continue  # Check again if stop event is set
            except (
                Exception
            ) as e:  # Catch any other potential exceptions in aggregation
                self.logger.error(f"Error in progress aggregation thread: {e}")
                break  # Exit aggregation loop if an error occurs

        self.pbar_aggregated.close()  # Ensure progress bar is closed at the end

    def _setup_logger(self):
        """Sets up a basic logger for the ReaderWriterPair if none is provided."""
        if self.logger:  # Use external logger if provided
            return self.logger
        logger = BaseLogger(
            logger_name=__name__ + "ReaderWriterPair",
            log_level=logging.DEBUG,
            enable_console_log=True,
        )
        return logger.logger  # Return the logger object from BaseLogger

    def _log_query_plan(self, sql, params=None):
        """Executes a query, logs its query plan, and returns the results."""
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
            print(f"SQLite error: {e}")
            return None

    def _log_configuration_as_table(
        self,
        num_of_batches,
        started_reader_threads,
        writer_thread_started,
    ):
        """Log configuration information as a formatted table.

        Args:
            num_of_batches: Number of batches calculated
            started_reader_threads: Number of reader threads successfully started
            writer_thread_started: Whether writer thread was started successfully

        """
        config_data = {
            "Total rows": self.total_count,
            "Batch size": self.batch_size,
            "Number of batches": num_of_batches,
            "Writer batch chunking": self.writer_batch_chunking,
            "Max queue size": self.data_queue.maxsize,
            "Reader profiling": self.profiling_reader_enabled,
            "Writer profiling": self.profiling_writer_enabled,
            "Reader threads": f"{started_reader_threads}/{self.num_reader_threads}",
            "Writer thread": writer_thread_started,
        }

        try:
            table_str = TableFormatter.format_table(
                config_data,
                title="ReaderWriterPair Processing Configuration",
            )
            self.logger.info(f"\n{table_str}")
        except ImportError:
            # Fallback to standard logging if TableFormatter is not available
            self.logger.info(
                f"ReaderWriterPair: Processing {self.total_count} rows"
                f"\nBatch size: {self.batch_size}."
                f"\nNumber of batches: {num_of_batches}."
                f"\nWriter batch chunking: {self.writer_batch_chunking}."
                f"\nMax queue size: {self.data_queue.maxsize}."
                f"\nProfiling enabled: Reader={self.profiling_reader_enabled}, Writer={self.profiling_writer_enabled}."
                f" Reader threads: Started {started_reader_threads}/{self.num_reader_threads}."
                f"\nWriting thread: {writer_thread_started}",
            )

    def run(self) -> None:
        """Runs the Reader and Writer threads CONCURRENTLY (Corrected Thread Management)."""
        self.logger.info(
            f"Starting ReaderWriterPair with {self.num_reader_threads} reader threads.",
        )

        aggregation_thread = threading.Thread(
            target=self._progress_aggregation_process,
            daemon=True,
        )  # Create aggregation thread
        aggregation_thread.start()  # Start aggregation thread

        reader_threads = []  # Keep track of reader threads
        self.logger.info(
            f"ReaderWriterPair: Starting {self.num_reader_threads} reader threads"
            f"{' for process ' + self.process_title if self.process_title else ''}."
            f"{' with total count ' + str(self.total_count) if self.total_count is not None else ''}",
        )
        for _ in range(self.num_reader_threads):
            thread = threading.Thread(
                target=self.reader.run,
                args=(
                    1,
                ),  # force reader to run in single thread mode, controlled by ReaderWriterPair
            )  # Corrected reader.run call - no args (single-threaded Reader.run will be used)
            reader_threads.append(thread)
            thread.start()

        writer_thread = threading.Thread(target=self.writer.run)  # Start writer thread
        self.logger.info("ReaderWriterPair: Starting writer thread.")
        writer_thread.start()

        self.logger.info(
            "ReaderWriterPair: Waiting for queue to be empty before joining writer.",
        )  # Corrected placement of queue.join()

        self.logger.info(
            f"ReaderWriterPair: Queue size before join: {self.data_queue.qsize()}, pending tasks: {self.data_queue.unfinished_tasks}",
        )
        self.data_queue.join()
        self.logger.info(
            f"ReaderWriterPair: Queue join completed. Queue size: {self.data_queue.qsize()}, pending tasks: {self.data_queue.unfinished_tasks}",
        )

        self.logger.info("ReaderWriterPair: Waiting for reader threads to complete.")
        for thread in reader_threads:
            thread.join()  # Wait for all reader threads to finish

        self.logger.info("ReaderWriterPair: Waiting for writer thread to complete.")
        writer_thread.join()  # Wait for writer thread to finish

        self.logger.info(
            "ReaderWriterPair: Signaling progress aggregation thread to stop.",
        )
        self.aggregation_thread_stop_event.set()  # Signal aggregation thread to stop
        aggregation_thread.join(
            timeout=2,
        )  # Wait for aggregation thread to finish, with a timeout

        self.logger.info("ReaderWriterPair process completed.")
