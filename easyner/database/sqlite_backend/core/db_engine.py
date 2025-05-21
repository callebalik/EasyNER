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
from queue import Queue

from tqdm import tqdm

from easyner.database.sqlite_backend.db_main import EasyNerDBHandler
from easyner.utils.logging.log_formatter import TableFormatter
from easyner.utils.logging.logging_mixins import TableLoggingMixin


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
    stats = pstats.Stats(prof_filename)
    stats.sort_stats("cumulative").print_stats(
        10,
    )  # Print top 10 functions by cumulative time
    return result


class Reader:
    """Reader class to read data from a database, process it, and put it into a queue.
    """

    def __init__(
        self,
        conn_params,
        query: str,
        batch_size: int,
        process_function,
        data_queue: Queue,  # Use multiprocessing.Queue for type hint
        lock: Lock,
        shared_processed_count: Value,
        stop_event: threading.Event = None,
        logger: logging.Logger = None,
        queue_size_backpressure_threshold: int = 50,
        progress_queue: Queue = None,  # Use multiprocessing.Queue for type hint
        profiling_filename: str = None,
        total_rows: int = None,
        batch_queue: Queue = None,  # Batch queue for chunk assignment (REQUIRED for chunking)
    ):
        """Initializes the Reader.

        Args:
            conn_params (dict): Database connection parameters.
            query (str): SQL query to execute.
            batch_size (int): Number of rows to fetch per batch.
            process_function (callable): Function to process each batch of data.
            data_queue (queue.Queue): Queue to put processed data into.
            logger (logging.Logger, optional): Logger instance. Defaults to a basic logger.
            queue_size_backpressure_threshold (int, optional): Queue size threshold for backpressure. Defaults to 50.
            progress_queue (queue.Queue, optional): Queue to send progress updates.
            profiling_enabled (bool, optional): Enable profiling for Reader's run method. Defaults to False.

        """
        if batch_queue is None:
            msg = "batch_queue must be provided for Reader when using chunking strategy."
            raise ValueError(
                msg,
            )
        self.batch_queue = batch_queue

        self.lock = lock
        self.stop_event = stop_event  # Store stop_event
        self.shared_processed_count = (
            shared_processed_count  # Initialize shared processed count
        )
        self.conn_params = conn_params
        self.query = query
        self.batch_size = batch_size
        self.process_function = process_function
        self.data_queue = data_queue
        self.logger = logger
        self.qsize_backbpressure_threshold = queue_size_backpressure_threshold
        self.progress_queue = progress_queue  # Store progress_queue
        self.total_count = total_rows  # Store total_count
        self.profiling_file = profiling_filename

        self.table_formatter = TableFormatter

    def _log_batch_info(
        self,
        batch,
        processed_batch,
        offset,
        limit,
        reader_duration,
        total_processed_in_thread,
        batch_empty=False,
    ):
        """Log batch processing information in a formatted table.

        Args:
            batch: The database batch that was fetched
            processed_batch: The processed batch data
            offset: The offset used in the query
            limit: The limit used in the query
            reader_duration: The time taken to read the batch from the database
            total_processed_in_thread: The total number of items processed in this thread
            batch_empty: Whether the batch was empty

        """
        # Only continue if we have a logger
        if not self.logger:
            return

        # Calculate rows per second
        rows_per_second = 0
        if reader_duration > 0 and batch:
            rows_per_second = len(batch) / reader_duration

        # Prepare data for the table
        data = {
            "Batch size from DB": len(batch) if batch else 0,
            "Reading time (s)": f"{reader_duration:.2f}",
            "Rows/second": f"{rows_per_second:.2f}",
            "Offset": offset,
            "Limit": limit,
            "Processed batch size": len(processed_batch) if processed_batch else 0,
            "Total processed in thread": total_processed_in_thread,
            "Queue size": self.data_queue.qsize() if self.data_queue else 0,
        }

        try:
            # Format as table
            log_level = logging.WARNING if batch_empty else logging.DEBUG
            table = self.table_formatter.format_table(
                data, title="Batch Processing Statistics",
            )
            self.logger.log(log_level, f"\n{table}")
        except Exception:
            # Fallback to standard logging if table formatting fails
            log_message = (
                f"Fetched batch size from DB: {len(batch) if batch else 0} in {reader_duration:.2f}s"
                f" ({rows_per_second:.2f} rows/s)"
                f"\nOffset: {offset}, Limit: {limit}"
                f"\nProcessed batch size: {len(processed_batch) if processed_batch else 0}"
                f"\nTotal processed in thread: {total_processed_in_thread}"
                f"\nQueue size: {self.data_queue.qsize() if self.data_queue else 0}"
            )
            self.logger.log(log_level, log_message)

    def _read_and_process(self):
        """Read and process batches of data from the database."""
        conn = None
        thread_start_time = time.time()
        thread_stats = {
            "total_batches": 0,
            "total_rows_fetched": 0,
            "total_rows_processed": 0,
            "empty_batches": 0,
            "max_batch_size": 0,
            "min_batch_size": float("inf"),
            "total_fetch_time": 0,
            "max_queue_size": 0,
            "start_offset": None,
            "last_offset": None,
        }

        try:
            conn = sqlite3.connect(**self.conn_params)
            cursor = conn.cursor()
            total_processed_in_thread = 0
            log_queue_size_interval = (
                2  # Log queue size every N batches (adjust as needed)
            )
            batch_counter = 0
            batch_empty = False
            queue_check_interval = 30  # Check queue size every X seconds when queue is full (adjust as needed)
            consecutive_empty_batches = 0
            max_consecutive_empty = 3
            last_max_pk = 0  # Track the last maximum primary key we've seen

            while True:
                if self.stop_event and self.stop_event.is_set():
                    self.logger.debug(
                        "Reader thread: Stop event set. Exiting read loop.",
                    )
                    break

                # Check if total_count has been reached BEFORE fetching.
                if (
                    self.total_count is not None
                    and self.shared_processed_count is not None
                    and self.lock is not None
                ):
                    with self.lock:
                        if self.shared_processed_count.value >= self.total_count:
                            self.logger.debug(
                                f"Reader thread: total processed ({self.shared_processed_count.value}) >= total_count ({self.total_count}). Exiting read loop.",
                            )
                            break

                try:
                    batch_desc = self.batch_queue.get(
                        timeout=10,
                    )  # Get batch assignment from queue
                    if batch_desc is None:
                        break
                except queue.Empty:
                    self.logger.debug(
                        "Reader thread: batch_queue is empty. Exiting read loop.",
                    )
                    break

                offset = batch_desc["offset"]
                limit = batch_desc["limit"]

                # Track offsets for statistics
                if thread_stats["start_offset"] is None:
                    thread_stats["start_offset"] = offset
                thread_stats["last_offset"] = offset

                params = {"offset": offset, "limit": limit}  # Parameters for query

                reader_start = time.time()
                cursor.execute(self.query, params)
                batch = cursor.fetchmany(
                    limit,
                )  # Redundant limit as execute already limits the query, but safety net
                reader_duration = time.time() - reader_start

                # Update thread statistics
                thread_stats["total_batches"] += 1
                thread_stats["total_fetch_time"] += reader_duration
                current_queue_size = self.data_queue.qsize() if self.data_queue else 0
                thread_stats["max_queue_size"] = max(
                    thread_stats["max_queue_size"], current_queue_size,
                )

                batch_empty = len(batch) == 0
                if batch_empty:
                    thread_stats["empty_batches"] += 1
                    self.batch_queue.task_done()  # Signal task done even if batch was unexpectedly empty
                    # Use table formatter to log empty batch
                    self._log_batch_info(
                        batch,
                        [],
                        offset,
                        limit,
                        reader_duration,
                        total_processed_in_thread,
                        batch_empty=True,
                    )
                    continue  # Skip processing and get next batch from queue (or exit if queue is empty)

                # Update row statistics
                thread_stats["total_rows_fetched"] += len(batch)
                thread_stats["max_batch_size"] = max(
                    thread_stats["max_batch_size"], len(batch),
                )
                thread_stats["min_batch_size"] = min(
                    thread_stats["min_batch_size"], len(batch),
                )

                processed_batch = self.process_function(batch, self.conn_params)

                if processed_batch:  # Only put into queue if there is processed data
                    # while (
                    #     self.data_queue.qsize() >= self.qsize_backbpressure_threshold
                    # ):  # Backpressure check
                    #     self.logger.debug(
                    #         f"Data queue at or above fill level {self.qsize_backbpressure_threshold} (size: {self.data_queue.qsize()}). Reader thread pausing..."
                    #     )
                    #     time.sleep(
                    #         queue_check_interval
                    #     )  # Pause reader thread to let writer catch up
                    #     # (Optionally) You could add a timeout to this loop to prevent indefinite blocking in extreme cases

                    self.data_queue.put(
                        processed_batch,
                    )  # Put batch into queue AFTER backpressure check
                    total_processed_in_thread += len(
                        processed_batch,
                    )  # Count processed items, not fetched
                    thread_stats["total_rows_processed"] += len(processed_batch)

                    if (
                        self.progress_queue
                    ):  # Send progress update if progress_queue exists
                        self.progress_queue.put(len(processed_batch))

                batch_counter += 1
                if batch_counter % log_queue_size_interval == 0 or batch_empty:
                    # Use table formatter to log batch information
                    self._log_batch_info(
                        batch,
                        processed_batch,
                        offset,
                        limit,
                        reader_duration,
                        total_processed_in_thread,
                        batch_empty,
                    )

                if (
                    self.total_count is not None
                    and self.shared_processed_count is not None
                    and self.lock is not None
                ):
                    with self.lock:
                        if self.shared_processed_count.value >= self.total_count:
                            self.logger.debug(
                                f"Reader thread: total processed ({self.shared_processed_count.value}) >= total_count ({self.total_count}). Exiting read loop.",
                            )
                            break
                        else:
                            self.shared_processed_count.value += len(processed_batch)

        except sqlite3.Error as e:
            # Should we rollback here?
            self.logger.error(f"Database error in Reader thread: {e}")
        except Exception as e:
            self.logger.error(f"Error in Reader thread process: {e}")
        finally:
            thread_duration = time.time() - thread_start_time

            # Handle edge case where no batches were processed
            if thread_stats["min_batch_size"] == float("inf"):
                thread_stats["min_batch_size"] = 0

            # Add final statistics
            thread_stats["total_processed"] = total_processed_in_thread
            thread_stats["thread_duration"] = thread_duration

            # Calculate rates and averages
            if thread_duration > 0:
                thread_stats["rows_per_second"] = (
                    total_processed_in_thread / thread_duration
                )
                if thread_stats["total_batches"] > 0:
                    thread_stats["avg_batch_size"] = (
                        thread_stats["total_rows_fetched"]
                        / thread_stats["total_batches"]
                    )
                    thread_stats["avg_batch_time"] = (
                        thread_stats["total_fetch_time"] / thread_stats["total_batches"]
                    )

            # Log summary table
            self._log_thread_completion_summary(thread_stats)

            if conn:
                conn.close()

            # Use debug level for routine completion messages
            self.logger.debug("Reader thread finished.")
            self.data_queue.put(
                None,
            )  # Ensure sentinel value is ALWAYS added to queue at the end
            self.logger.debug("Reader thread: added sentinel value to queue.")

    def _log_thread_completion_summary(self, stats):
        """Log a formatted table with thread completion statistics.

        Args:
            stats (dict): Dictionary containing thread statistics

        """
        try:
            # Format data for better readability
            summary_data = {
                "Total batches processed": stats["total_batches"],
                "Total rows fetched": stats["total_rows_fetched"],
                "Total rows processed": stats["total_rows_processed"],
                "Thread duration (s)": f"{stats['thread_duration']:.2f}",
                "Processing rate (rows/s)": f"{stats.get('rows_per_second', 0):.2f}",
                "Empty batches": stats["empty_batches"],
                "Avg batch size": f"{stats.get('avg_batch_size', 0):.1f}",
                "Min/Max batch size": f"{stats['min_batch_size']}/{stats['max_batch_size']}",
                "Avg batch fetch time (s)": f"{stats.get('avg_batch_time', 0):.3f}",
                "Max queue size": stats["max_queue_size"],
                "Offset range": f"{stats['start_offset']} - {stats['last_offset']}",
            }

            table = self.table_formatter.format_table(
                summary_data, title="Reader Thread Completion Summary",
            )
            self.logger.info(f"\n{table}")
        except Exception:
            # Fallback to standard logging if table formatting fails
            self.logger.info(
                f"Reader thread completed with: {stats['total_batches']} batches, "
                f"{stats['total_rows_processed']} rows processed in {stats['thread_duration']:.2f} seconds",
            )

    def run(self, num_threads=1) -> None:  # `num_threads` is now OPTIONAL with default 1
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
        if self.profiling_file:
            run_with_profiling(
                lambda: self._run_internal(num_threads), self.profiling_file,
            )  # Use lambda to call internal run with args
        else:
            self._run_internal(num_threads)

    def _run_internal(
        self, num_threads=1,
    ):  # `num_threads` is now OPTIONAL with default 1
        """Runs the Reader process, in single or multi-threaded mode based on num_threads.

        Args:
            num_threads (int, optional): Number of reader threads to use. Defaults to 1 (single-threaded).
            If > 1, Reader will manage its own thread pool. If 1 or not provided,
            single-threaded (intended for ReaderWriterPair).

        """
        if num_threads <= 1:  # Single-threaded execution (for ReaderWriterPair usage)
            self._read_and_process()
            # Use debug level to avoid cluttering logs with completion messages
            self.logger.debug("Reader thread completed")
        else:  # Multi-threaded execution (for standalone Reader usage)
            threads = []
            self.logger.info(
                f"START {num_threads} READ + PROCESS THREADS (standalone - multi-threaded mode).",
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
    """Writer class as before, now not directly managing the queue.
    """

    def __init__(
        self,
        conn_params,
        write_function,
        data_queue,
        batch_chunking: int,
        num_reader_threads: int,
        logger=None,
        profiling_filename: str = None,
        stop_event: threading.Event = None,  # Added stop_event
    ):
        """Initializes the Writer.

        Args:
            conn_params (dict): Database connection parameters.
            write_function (callable): Function to write a batch of data to the database.
            data_queue (queue.Queue): Queue to get data from.
            logger (logging.Logger, optional): Logger instance. Defaults to a basic logger.
            progress_queue (queue.Queue, optional): Queue to send progress updates.
            profiling_enabled (bool, optional): Enable profiling for Writer's run method. Defaults to False.

        """
        self.stop_event = stop_event  # Store stop_event
        self.conn_params = conn_params
        self.write_function = write_function
        self.data_queue = data_queue
        self.logger = logger
        self.written_count = 0  # Initialize processed_count for Writer
        self.profiling_filename = profiling_filename
        self.batch_chunking = (
            batch_chunking  # Accumulte multiple batches before writing
        )
        self.num_reader_threads = num_reader_threads  # Store number of reader threads

        self.table_formatter = TableFormatter

    def writer_process(self) -> None:
        """Process data from the queue and write it to the database."""
        conn: sqlite3.Connection = None
        thread_start_time = time.time()
        write_stats = {
            "total_batches": 0,
            "total_rows_written": 0,
            "accumulated_batches": 0,
            "max_batch_size": 0,
            "min_batch_size": float("inf"),
            "total_write_time": 0,
            "commit_count": 0,
            "rollback_count": 0,
            "chunk_threshold": self.batch_chunking,
            "sentinel_count": 0,
        }

        try:
            conn = sqlite3.connect(**self.conn_params)
            cursor = conn.cursor()

            sentinel_count = 0  # Count of sentinels received
            num_readers = self.num_reader_threads

            while True:
                if (
                    self.stop_event.is_set()
                ):  # Check stop event at the beginning of writer loop
                    self.logger.debug(
                        "Writer thread: Stop event detected, exiting writer loop.",
                    )
                    break

                batch = (
                    self.data_queue.get()
                )  # Get batch from queue (same queue as Reader's)

                if batch is None:  # Sentinel value received
                    self.data_queue.task_done()  # Signal task completion for sentinel
                    sentinel_count += 1
                    write_stats["sentinel_count"] += 1

                    if sentinel_count == num_readers:  # All readers have finished
                        # Use debug level since we'll log a nice table summary at INFO level
                        self.logger.debug(
                            f"Writer thread received all {num_readers} sentinels. Exiting writer process.",
                        )
                        break
                    else:
                        # Use debug level to avoid cluttering logs
                        self.logger.debug(
                            f"Writer thread received a sentinel. Total sentinels: {sentinel_count} (of {num_readers})",
                        )
                        continue  # Skip processing sentinel

                # Log batch fetch at debug level to reduce noise
                self.logger.debug(
                    f"Writer thread: Got batch from queue - size: {len(batch) if batch else 'Sentinel'}",
                )

                # ENABLE THIS BATCH CHUNKING CODE
                if self.batch_chunking > 1:
                    accumulated_size = len(batch)
                    accumulated_batches = 1
                    for _ in range(self.batch_chunking - 1):
                        if self.data_queue.empty():
                            break
                        next_batch = self.data_queue.get()
                        if next_batch is None:
                            sentinel_count += 1
                            write_stats["sentinel_count"] += 1
                            self.data_queue.task_done()
                            break
                        accumulated_size += len(next_batch)
                        accumulated_batches += 1
                        batch.extend(next_batch)
                        self.logger.debug(
                            f"Writer thread: Accumulated batch size now {accumulated_size}",
                        )
                        self.data_queue.task_done()

                    write_stats["accumulated_batches"] += accumulated_batches

                # Update batch statistics
                write_stats["total_batches"] += 1
                batch_size = len(batch)
                write_stats["max_batch_size"] = max(
                    write_stats["max_batch_size"], batch_size,
                )
                if batch_size > 0:  # Only update min_batch_size for non-empty batches
                    write_stats["min_batch_size"] = min(
                        write_stats["min_batch_size"], batch_size,
                    )

                # Perform the write operation with timing
                write_start = time.time()
                try:
                    self.write_function(batch, cursor, conn)  # Call the write function
                    self.written_count += batch_size
                    write_stats["total_rows_written"] += batch_size
                    conn.commit()  # Commit transaction after each successful write
                    write_stats["commit_count"] += 1
                    # Use debug level to avoid cluttering logs
                    self.logger.debug(
                        f"Writer thread wrote a batch of {batch_size} items (total written so far: {self.written_count})",
                    )
                except Exception as write_e:
                    conn.rollback()  # Rollback transaction in case of write error
                    write_stats["rollback_count"] += 1
                    self.logger.error(
                        f"Error in write_function: {write_e}. Transaction rolled back for current batch.",
                    )
                finally:
                    write_duration = time.time() - write_start
                    write_stats["total_write_time"] += write_duration
                    self.data_queue.task_done()  # Signal task completion for the batch

            thread_duration = time.time() - thread_start_time
            # Handle edge case where no batches were processed
            if write_stats["min_batch_size"] == float("inf"):
                write_stats["min_batch_size"] = 0

            # Add final statistics
            write_stats["thread_duration"] = thread_duration

            # Calculate rates and averages
            if thread_duration > 0:
                write_stats["rows_per_second"] = (
                    write_stats["total_rows_written"] / thread_duration
                )
                if write_stats["total_batches"] > 0:
                    write_stats["avg_batch_size"] = (
                        write_stats["total_rows_written"] / write_stats["total_batches"]
                    )
                    write_stats["avg_write_time"] = (
                        write_stats["total_write_time"] / write_stats["total_batches"]
                    )

            # Log summary table
            self._log_thread_completion_summary(write_stats)

        except sqlite3.Error as e:
            if conn:
                conn.rollback()
                write_stats["rollback_count"] += 1
            self.logger.error(
                f"Database error in Writer thread: {e}. Transaction rolled back.",
            )
        except Exception as e:
            self.logger.error(f"Error in Writer thread: {e}")
        finally:
            if conn:
                conn.close()
            self.logger.debug("Writer thread finished.")

    def _log_thread_completion_summary(self, stats):
        """Log a formatted table with writer thread completion statistics.

        Args:
            stats (dict): Dictionary containing thread statistics

        """
        try:
            # Format data for better readability
            summary_data = {
                "Total batches written": stats["total_batches"],
                "Total rows written": stats["total_rows_written"],
                "Thread duration (s)": f"{stats['thread_duration']:.2f}",
                "Writing rate (rows/s)": f"{stats.get('rows_per_second', 0):.2f}",
                "Batches accumulated": stats["accumulated_batches"],
                "Chunking threshold": stats["chunk_threshold"],
                "Avg batch size": f"{stats.get('avg_batch_size', 0):.1f}",
                "Min/Max batch size": f"{stats['min_batch_size']}/{stats['max_batch_size']}",
                "Avg write time (s)": f"{stats.get('avg_write_time', 0):.3f}",
                "Commits/Rollbacks": f"{stats['commit_count']}/{stats['rollback_count']}",
                "Sentinels received": stats["sentinel_count"],
            }

            table = self.table_formatter.format_table(
                summary_data, title="Writer Thread Completion Summary",
            )
            self.logger.info(f"\n{table}")
        except Exception:
            # Fallback to standard logging if table formatting fails
            self.logger.info(
                f"Writer thread completed with: {stats['total_batches']} batches, "
                f"{stats['total_rows_written']} rows written in {stats['thread_duration']:.2f} seconds",
            )

    def run(self) -> None:
        """Runs the Writer process.

        Keyword Args:
            profiling_enabled (bool, optional): If True, enables profiling for this run. Defaults to False.
            Profiling results are saved to 'Writer_run_profile.prof'.
            To analyze, use `import pstats; pstats.Stats('Writer_run_profile.prof').sort_stats('cumulative').print_stats(30)`

        """
        prof_filename = "Writer_run_profile.prof"  # Filename for profiling data

        if self.profiling_filename:
            self.logger.info(
                f"Writer thread starting with profiling enabled. Results will be in '{prof_filename}'",
            )
            run_with_profiling(self._run_internal, prof_filename)
        else:
            self._run_internal()

    def _run_internal(self):
        """... (Writer class run method) ..."""
        self.logger.info("Starting writer thread.")
        writer_thread = threading.Thread(target=self.writer_process)
        writer_thread.start()
        writer_thread.join()  # Wait for writer thread to finish
        self.logger.info("Writer thread finished.")


class ReaderWriterPair(TableLoggingMixin):
    """Manages a Reader and Writer pair, creating and connecting their data queue internally.
    """

    def __init__(
        self,
        conn_params,
        reader_query,
        process_function,
        write_function,
        total_rows: int,
        logger: logging.Logger = None,
        batch_size: int = 5000,
        num_reader_threads: int = 4,
        max_queue_size: int = 200,
        writer_batch_chunking: int = 10,  # X batches to accumulate before writing to baleance read/write speed
        process_title: str = None,
        profiling_reader_enabled=False,  # Added profiling_enabled
        profiling_writer_enabled=False,  # Added profiling_enabled):
    ):
        """Initializes the ReaderWriterPair, creating the queue and instances of Reader and Writer.

        Args:
            total_rows (int): Total number of rows to process. Must be provided for chunking.
            conn_params (dict): Database connection parameters.
            reader_query (str): SQL query for the Reader.
            batch_size (int): Batch size for reading.
            process_function (callable): Processing function for Reader.
            write_function (callable): Writing function for Writer.
            num_reader_threads (int, optional): Number of reader threads. Defaults to 4.
            logger (logging.Logger, optional): Logger instance. Defaults to a basic logger.
            max_queue_size (int, optional): Maximum size of the internal data queue. Defaults to 50.
            profiling_reader_enabled (bool, optional): Enable profiling for Reader. Defaults to False.
            profiling_writer_enabled (bool, optional): Enable profiling for Writer. Defaults to False.

        """
        # Validate process_function
        if not callable(process_function):
            msg = "process_function must be callable"
            raise TypeError(msg)

        # Validate write_function
        if not callable(write_function):
            msg = "write_function must be callable"
            raise TypeError(msg)

        # Validate function signatures using test calls
        try:
            # Test process_function with empty batch
            test_batch = []
            result = process_function(test_batch, conn_params)
            if not isinstance(result, list):
                msg = "process_function must return a list"
                raise TypeError(msg)
        except Exception as e:
            msg = (
                f"Invalid process_function signature. Expected: "
                f"Function(List[Any], Dict[str, Any]) -> List[Any]. Error: {str(e)}"
            )
            raise TypeError(
                msg,
            )

        # Create test objects for write_function validation
        test_conn = sqlite3.connect(":memory:")
        test_cursor = test_conn.cursor()
        test_logger = logger or self._setup_logger()

        try:
            # Test write_function with empty batch
            result = write_function([], test_cursor, test_conn)
            # if not isinstance(result, bool):
            # raise TypeError("write_function must return a boolean")
        except sqlite3.Error as e:
            test_conn.close()
            msg = f"Invalid write function. Sqlite Error: {str(e)}"
            raise TypeError(msg)
        except Exception as e:
            test_conn.close()
            msg = (
                f"Invalid write_function signature. Expected: "
                f"Function(List[Any], sqlite3.Cursor, sqlite3.Connection) -> bool. Error: {str(e)}"
            )
            raise TypeError(
                msg,
            )
        finally:
            test_conn.close()

        # Validate reader_query

        self.reader_query = reader_query
        # Check for ORDER BY clause
        if "ORDER BY" not in reader_query.upper():
            msg = "Reader query must include an ORDER BY clause to ensure consistent row processing."
            raise ValueError(
                msg,
            )
        if ":offset" not in reader_query:
            msg = "Reader query must include a :offset parameter for threaded pagination."
            raise ValueError(
                msg,
            )
        if ":limit" not in reader_query:
            msg = "Reader query must include a :limit parameter for threaded pagination."
            raise ValueError(
                msg,
            )
        # **NEW CHECKS: Ensure exactly ONE :limit and ONE :offset placeholder**
        if reader_query.lower().count(":limit") != 1:
            msg = "Reader query must contain exactly one ':limit' parameter placeholder."
            raise ValueError(
                msg,
            )
        if reader_query.lower().count(":offset") != 1:
            msg = "Reader query must contain exactly one ':offset' parameter placeholder."
            raise ValueError(
                msg,
            )

        self.conn_params = conn_params

        self.process_function = process_function
        self.write_function = write_function
        self.num_reader_threads = num_reader_threads
        self.logger = logger or self._setup_logger()

        # Initialize queues
        self.batch_size = batch_size
        self.batch_queue = queue.Queue()  # Create batch queue for chunking

        self.data_queue = queue.Queue(
            maxsize=max_queue_size,
        )  # Pair class creates the queue
        self.progress_queue = (
            queue.Queue()
        )  # Create progress queue for aggregated progress
        self.aggregation_thread_stop_event = (
            threading.Event()
        )  # Event to stop aggregation thread

        self.stop_event = threading.Event()  # Add a general stop event

        self.total_rows = total_rows  # Store total_count
        self.total_processed = multiprocessing.Value("i", 0)
        self.total_processed_lock = multiprocessing.Lock()  # Create the lock
        self.process_title = process_title  # Store process title if provided

        self.pbar_aggregated = tqdm(
            total=total_rows, desc="Total Progress",
        )  # Initialize tqdm for aggregated progress

        self.profiling_reader_enabled = os.getenv(
            "PROFILING_READER_ENABLED", profiling_reader_enabled,
        )  # Added profiling_reader_enabled
        self.profiling_reader_fileanme = None
        if self.profiling_reader_enabled:
            self.profiling_reader_fileanme = f"reader_threaded_{self.process_title if self.process_title else ''}.{time.strftime('%Y%m%d_%H%M%S')}.prof"

        self.profiling_writer_enabled = os.getenv(
            "PROFILING_WRITER_ENABLED", profiling_writer_enabled,
        )  # Added profiling_writer_enabled

        self.profiling_writer_filename = None

        if self.profiling_writer_enabled:
            self.profiling_writer_filename = f"writer_threaded_{self.process_title if self.process_title else ''}.{time.strftime('%Y%m%d_%H%M%S')}.prof"

        self.writer_batch_chunking = (
            writer_batch_chunking  # Added writer_batch_chunking
        )
        # Instantiate Reader and Writer, passing the *same* data_queue to both
        self._log_query_plan(reader_query)  # Log query plan for Reader query

        self.reader = Reader(
            conn_params,
            reader_query,
            batch_size,
            process_function,
            self.data_queue,
            logger=self.logger,
            batch_queue=self.batch_queue,  # Pass batch_queue to Reader
            queue_size_backpressure_threshold=max_queue_size
            - 5,  # Adjusted backpressure threshold for when readers start to pause to avoid maxing out queue
            progress_queue=self.progress_queue,  # Pass progress_queue to Reader
            profiling_filename=self.profiling_reader_fileanme,
            stop_event=self.stop_event,  # Add a general stop event
            lock=self.total_processed_lock,  # Pass lock to Reader
            shared_processed_count=self.total_processed,  # Pass shared_processed_count to Reader
            total_rows=self.total_rows,  # Pass total_count
        )
        self.writer = Writer(
            conn_params,
            write_function,
            self.data_queue,
            num_reader_threads=self.num_reader_threads,
            logger=self.logger,
            profiling_filename=self.profiling_writer_filename,
            batch_chunking=self.writer_batch_chunking,
            stop_event=self.stop_event,  # Add a general stop event
        )

    def _populate_batch_queue(self):
        """Populates the batch queue with batch descriptors.
        For primary key-based pagination, this initializes with the lowest primary key
        and then each reader increments based on the max key it has seen.
        """
        if self.total_rows is None:
            msg = "total_rows must be provided for chunking."
            raise ValueError(msg)

        if getattr(self, "pk_based_pagination", False):
            # When using primary key-based pagination:
            # 1. Put a single task with offset=0 to start
            # 2. Readers will increment the primary key themselves based on what they read
            self.logger.info(
                "Initializing batch queue with primary key-based pagination",
            )

            # Put a single task to start the process with the first available primary key
            self.batch_queue.put({"offset": 0, "limit": self.batch_size})

            # Then pre-populate a number of batch tasks with the same parameters
            # Each reader will dynamically update the offset based on the max primary key it sees
            batch_count = min(200, self.total_rows // self.batch_size + 1)
            for _ in range(batch_count):
                self.batch_queue.put({"offset": 0, "limit": self.batch_size})

            self.logger.info(
                f"Added {batch_count+1} initial tasks for PK-based pagination",
            )
        else:
            # Traditional offset/limit based pagination
            num_batches = (self.total_rows + self.batch_size - 1) // self.batch_size

            for i in range(num_batches):
                offset = i * self.batch_size
                limit = self.batch_size
                self.batch_queue.put({"offset": offset, "limit": limit})

            return num_batches

    def _progress_aggregation_process(self):  # Aggregation thread function
        """Continuously reads progress updates from the progress_queue, aggregates them,
        and updates the tqdm progress bar.
        """
        read_total = 0  # Renamed to read_total to clarify its purpose
        update_interval = 10000  # Update progress bar every N items (adjust as needed)
        while not self.aggregation_thread_stop_event.is_set():
            try:
                progress_increment = self.progress_queue.get(
                    timeout=10,
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
        db = EasyNerDBHandler()
        logger = db.logger
        db.close()
        return logger

    def _log_query_plan(self, sql):
        """Logs the query execution plan for the provided SQL query.
        Handles named parameters by providing sensible defaults.
        """
        import sqlite3

        try:
            # Provide default values for common named parameters
            mock_params = {
                ":limit": self.batch_size,  # Use the batch size from the class
                ":offset": 0,  # Start at 0
            }

            # Extract any other named parameters from the query
            # This regex finds all named parameters like :name in the SQL
            import re

            param_names = re.findall(r":(\w+)", sql)
            for name in param_names:
                if f":{name}" not in mock_params:
                    mock_params[f":{name}"] = 1  # Default value for other params

            with sqlite3.connect(**self.conn_params) as conn:
                cursor = conn.cursor()
                explain_sql = f"EXPLAIN QUERY PLAN {sql}"

                self.logger.debug(f"Executing query plan with params: {mock_params}")
                cursor.execute(explain_sql, mock_params)

                plan = cursor.fetchall()
                if plan:
                    plan_text = "\n".join(str(row) for row in plan)
                    self.logger.debug(f"Query plan:\n{plan_text}")
                else:
                    self.logger.debug("No query plan returned")

        except sqlite3.Error as e:
            self.logger.warning(f"Error getting query plan (non-critical): {e}")
        except Exception as e:
            self.logger.warning(f"Unexpected error in query plan (non-critical): {e}")

    def run(self) -> None:
        """Runs the Reader and Writer threads with proper queue monitoring and cleanup.
        """
        reader_threads = []
        writer_thread = None
        aggregation_thread = None

        try:
            # 1. FIRST: Set up and start all threads
            num_of_batches = self._populate_batch_queue()

            # Start aggregation thread first
            aggregation_thread = threading.Thread(
                target=self._progress_aggregation_process, daemon=True,
            )
            aggregation_thread.start()

            # Start reader threads
            started_reader_threads = 0
            for _ in range(self.num_reader_threads):
                try:
                    thread = threading.Thread(target=self.reader.run)
                    thread.daemon = True
                    reader_threads.append(thread)
                    thread.start()
                    started_reader_threads += 1
                except Exception as e:
                    self.logger.error(f"Error starting reader thread: {e}")

            # Start writer thread
            writer_thread_started = False
            try:
                writer_thread = threading.Thread(target=self.writer.run)
                writer_thread.daemon = True
                writer_thread.start()
                writer_thread_started = True
            except Exception as e:
                self.logger.error(f"Error starting writer thread: {e}")

            # Log job info
            self._log_configuration_as_table(
                num_of_batches, started_reader_threads, writer_thread_started,
            )

            # 2. SECOND: Monitor overall processing progress
            while True:
                # Exit if all data has been processed
                if self.total_processed.value >= self.total_rows:
                    self.logger.info(
                        f"All {self.total_rows} rows have been processed. Proceeding to cleanup.",
                    )
                    break

                # Exit if stop event is set
                if self.stop_event.is_set():
                    self.logger.info(
                        "Stop event detected during monitoring. Proceeding to cleanup.",
                    )
                    break

                # Brief sleep to allow for keyboard interrupts
                time.sleep(0.3)

            # 3. THIRD: Monitor batch queue until empty (needed before readers finish)
            self.logger.debug("Waiting for batch queue to be processed...")
            batch_queue_timeout = 300
            batch_queue_start = time.time()
            while self.batch_queue.unfinished_tasks > 0:
                if self.stop_event.is_set():
                    self.logger.warning(
                        "Stop event detected while waiting for batch queue. Breaking.",
                    )
                    break
                if time.time() - batch_queue_start > batch_queue_timeout:
                    self.logger.warning(
                        f"Batch queue wait timed out after {batch_queue_timeout} seconds.",
                    )
                    break
                time.sleep(1)
            self.logger.debug(
                f"Batch queue monitoring completed. Unfinished tasks: {self.batch_queue.unfinished_tasks}",
            )

            # 4. FOURTH: Wait for data queue to empty (needed before writer finishes)
            self.logger.debug("Waiting for data queue to be processed...")
            data_queue_timeout = 300
            data_queue_start = time.time()
            while self.data_queue.unfinished_tasks > 0:
                if self.stop_event.is_set():
                    self.logger.warning(
                        "Stop event detected while waiting for data queue. Breaking.",
                    )
                    break
                if time.time() - data_queue_start > data_queue_timeout:
                    self.logger.warning(
                        f"Data queue wait timed out after {data_queue_timeout} seconds.",
                    )
                    break
                time.sleep(1)
            self.logger.debug(
                f"Data queue monitoring completed. Unfinished tasks: {self.data_queue.unfinished_tasks}",
            )

            # 5. FIFTH: Join reader threads (they should be done by now)
            self.logger.info("Joining reader threads...")
            for thread in reader_threads:
                thread.join(timeout=5)

            # 6. SIXTH: Join writer thread (should be done after data queue is empty)
            self.logger.info("Joining writer thread...")
            if writer_thread:
                writer_thread.join(timeout=5)

            # 7. FINALLY: Stop and join aggregation thread
            self.logger.info("Stopping aggregation thread...")
            self.aggregation_thread_stop_event.set()
            if aggregation_thread:
                aggregation_thread.join(timeout=5)

            self.logger.info("ReaderWriterPair process completed.")

        except KeyboardInterrupt:
            timeout: float = 2  # Define timeout at the start for clarity

            try:
                self.stop_event.set()
                self.aggregation_thread_stop_event.set()
                self.logger.warning(
                    f"ReaderWriterPair: Keyboard interrupt detected. Requesting threads to stop within {timeout} s.",
                )

                # Drain both queues to unblock threads
                self._drain_queue(self.data_queue)
                self._drain_queue(self.batch_queue)
                self._drain_queue(self.progress_queue)

            except Exception as e_set_event:
                self.logger.error(
                    f"Error setting thread stop events during keyboard interrupt: {e_set_event}",
                )

            finally:
                try:
                    # Wait for threads to join (with timeout)
                    for thread in reader_threads:
                        thread.join(timeout)
                    if writer_thread:
                        writer_thread.join(timeout)
                    if aggregation_thread:
                        aggregation_thread.join(timeout)
                except Exception as e_join_threads:
                    self.logger.error(
                        f"Error joining threads during keyboard interrupt: {e_join_threads}",
                    )
                finally:
                    self.logger.warning(
                        "ReaderWriterPair: Keyboard interrupt cleanup complete.",
                    )

    # Add this helper method to the ReaderWriterPair class:
    def _drain_queue(self, q):
        """Helper method to drain a queue and unblock any threads waiting on it."""
        try:
            while True:
                q.get_nowait()
                q.task_done()
        except queue.Empty:
            pass

    def _log_configuration_as_table(
        self, num_of_batches, started_reader_threads, writer_thread_started,
    ):
        """Log configuration information as a formatted table.

        Args:
            num_of_batches: Number of batches calculated
            started_reader_threads: Number of reader threads successfully started
            writer_thread_started: Whether writer thread was started successfully

        """
        config_data = {
            "Total rows": self.total_rows,
            "Batch size": self.batch_size,
            "Number of batches": num_of_batches,
            "Writer batch chunking": self.writer_batch_chunking,
            "Max queue size": self.data_queue.maxsize,
            "Reader profiling": self.profiling_reader_enabled,
            "Writer profiling": self.profiling_writer_enabled,
            "Reader threads": f"{started_reader_threads}/{self.num_reader_threads}",
            "Writer thread": writer_thread_started,
        }

        # Use the TableLoggingMixin's log_table method
        self.log_table(config_data, title="ReaderWriterPair Processing Configuration")


class WriteFunction:
    """Class to define the write function for the Writer.
    """

    @staticmethod
    def write_batch(batch, cursor, conn, logger):
        """Write function to insert a batch of data into a database.

        Args:
            batch (List[Tuple]): List of tuples to insert.
            cursor (sqlite3.Cursor): Database cursor.
            conn (sqlite3.Connection): Database connection.
            logger (logging.Logger): Logger instance.

        Returns:
            bool: True if write was successful, False otherwise.

        """
        try:
            cursor.executemany("INSERT INTO test_table (id, name) VALUES (?, ?)", batch)
            return True
        except Exception as e:
            logger.error(f"Error writing batch: {e}")
            return False
