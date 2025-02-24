import multiprocessing
from multiprocessing import Lock, Value
import threading
import sqlite3
import queue
import logging
from tqdm import tqdm
import time
import os
import cProfile
import pstats

from scripts.database.db_main import EasyNerDBHandler


def run_with_profiling(func, prof_filename):
    """
    Runs a function under cProfile profiling and saves results to a file.

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
    stats.sort_stats('cumulative').print_stats(10) # Print top 10 functions by cumulative time
    return result


class Reader:
    """
    Reader class to read data from a database, process it, and put it into a queue.
    """

    def __init__(
        self,
        conn_params,
        query: str,
        batch_size: int,
        process_function,
        data_queue,
        lock: Lock,  # Added lock
        shared_processed_count,  # Added shared_processed_count
        stop_event: threading.Event = None,  # Added stop_event
        logger=None,
        queue_size_backpressure_threshold=50,
        progress_queue=None,  # Added progress_queue
        profiling_filename: str = None,
        total_count=None,  # Added total_count
    ):
        """
        Initializes the Reader.

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
        self.lock = lock
        self.stop_event = stop_event  # Store stop_event
        self.shared_processed_count = shared_processed_count  # Initialize shared processed count
        self.conn_params = conn_params
        self.query = query
        self.batch_size = batch_size
        self.process_function = process_function
        self.data_queue = data_queue
        self.logger = logger
        self.qsize_backbpressure_threshold = queue_size_backpressure_threshold
        self.progress_queue = progress_queue  # Store progress_queue
        self.total_count = total_count  # Store total_count
        self.profiling_file: str = None

    def _read_and_process(self):
        """... (Reader class _read_and_process method) ..."""
        conn = None

        try:
            conn = sqlite3.connect(**self.conn_params)
            cursor = conn.cursor()
            cursor.execute(self.query)
            total_processed = 0
            log_queue_size_interval = (
                2  # Log queue size every N batches (adjust as needed)
            )
            batch_counter = 0
            queue_check_interval = 30  # Check queue size every X seconds when queue is full (adjust as needed)

            while True:
                if self.stop_event and self.stop_event.is_set():
                    self.logger.info("Reader thread: Stop event set. Exiting read loop.")
                    break

                # Check if total_count has been reached BEFORE fetching.
                if self.total_count is not None and self.shared_processed_count is not None and self.lock is not None:
                    with self.lock:
                        if self.shared_processed_count.value >= self.total_count:
                            self.logger.info(
                                f"Reader thread: total processed ({self.shared_processed_count.value}) >= total_count ({self.total_count}). Exiting read loop."
                            )
                            break

                batch = cursor.fetchmany(self.batch_size)
                if not batch:
                    self.logger.debug("Reader thread: no more data from cursor")
                    break  # No more data

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
                        processed_batch
                    )  # Put bastch into queue AFTER backpressure check
                    total_processed += len(
                        processed_batch
                    )  # Count processed items, not fetched

                    if (
                        self.progress_queue
                    ):  # Send progress update if progress_queue exists
                        self.progress_queue.put(len(processed_batch))

                batch_counter += 1
                if batch_counter % log_queue_size_interval == 0:
                    queue_size = self.data_queue.qsize()
                    self.logger.debug(
                        f"READ PROCESSED BATCH (lenght {len(processed_batch)}) - Queue size: {queue_size} (total processed in thread: {total_processed})"
                    )

                if (
                    self.total_count is not None
                    and self.shared_processed_count is not None
                    and self.lock is not None
                ):
                    with self.lock:
                        if self.shared_processed_count.value >= self.total_count:
                            self.logger.info(
                                f"Reader thread: total processed ({self.shared_processed_count.value}) >= total_count ({self.total_count}). Exiting read loop."
                            )
                            break
                        else:
                            self.shared_processed_count.value += len(processed_batch)

        except sqlite3.Error as e:
            # Shoudl we rollback here?
            self.logger.error(f"Database error in Reader thread: {e}")
        except Exception as e:
            self.logger.error(f"Error in Reader thread process: {e}")
        finally:
            if conn:
                conn.close()
            self.logger.debug("Reader thread finished.")
            self.data_queue.put(
                None
            )  # Ensure sentinel value is ALWAYS added to queue at the end
            self.logger.debug("Reader thread: added sentinel value to queue.")

    def run(self, num_threads=1):  # `num_threads` is now OPTIONAL with default 1
        """
        Runs the Reader process, in single or multi-threaded mode based on num_threads.

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
            self.logger.debug(
                f"START: READ + PROCESS (profiling -> '{prof_filename}')"
            )
            run_with_profiling(
                lambda: self._run_internal(num_threads), prof_filename
            )  # Use lambda to call internal run with args
        else:
            self._run_internal(num_threads)

    def _run_internal(
        self, num_threads=1
    ):  # `num_threads` is now OPTIONAL with default 1
        """
        Runs the Reader process, in single or multi-threaded mode based on num_threads.

        Args:
            num_threads (int, optional): Number of reader threads to use. Defaults to 1 (single-threaded).
            If > 1, Reader will manage its own thread pool. If 1 or not provided,
            single-threaded (intended for ReaderWriterPair).
        """
        if num_threads <= 1:  # Single-threaded execution (for ReaderWriterPair usage)
            self.logger.debug(
                "START: READ + PROCESS"
            )
            self._read_and_process()
            self.logger.info(
                "FINISH: READ + PROCESS"
            )
        else:  # Multi-threaded execution (for standalone Reader usage)
            threads = []
            self.logger.info(
                f"START {num_threads} READ + PROCESS THREADS (standalone - multi-threaded mode)."
            )
            for _ in range(num_threads):
                thread = threading.Thread(target=self._read_and_process)
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            self.data_queue.put(
                None
            )  # Sentinel value for multi-threaded standalone Reader (if needed - depends on use case)
            self.logger.info(
                "Reader threads finished and sentinel value added to queue (multi-threaded mode)."
            )

    def get_queue(self):
        """
        Returns the data queue managed by this Reader.
        This allows external access to the queue to share with the Writer.
        """
        return self.data_queue


class Writer:
    """
    Writer class as before, now not directly managing the queue.
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
        """
        Initializes the Writer.

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



    def writer_process(self):
        """... (Writer class writer_process method) ..."""
        conn: sqlite3.Connection = None
        try:
            conn = sqlite3.connect(**self.conn_params)
            cursor = conn.cursor()

            sentinel_count = 0  # Count of sentinels received
            num_readers = (
                self.num_reader_threads
            )  # Get number of readers from the queue

            while True:
                if self.stop_event.is_set(): # Check stop event at the beginning of writer loop
                    self.logger.debug("Writer thread: Stop event detected, exiting writer loop.")
                    break

                batch = (
                    self.data_queue.get()
                )  # Get batch from queue (same queue as Reader's)
                if batch is None:  # Sentinel value received
                    self.data_queue.task_done()  # Signal task completion for sentinel
                    sentinel_count += 1

                    if sentinel_count == num_readers:  # All readers have finished
                        self.logger.debug(
                            f"Writer thread received all {num_readers} sentinels. Exiting writer process."
                        )
                        break
                    else:
                        self.logger.debug(
                            f"Writer thread received a sentinel. Total sentinels: {sentinel_count} (of {num_readers})"
                        )
                        continue  # Skip processing sentinel

                # if self.batch_chunking > 1: # If batch_chunking is enabled
                #     for _ in range(self.batch_chunking - 1):
                #         if self.data_queue.empty():
                #             break # Exit inner loop if queue is empty and run with current batch
                #         if self.data_queue.get() is None: # Must still run with current batch(es) if sentinel is received before breaking
                #             sentinel_count += 1
                #             break
                #         else: batch = batch + self.data_queue.get() # Get next batch and append to current batch

                try:
                    self.write_function(batch, cursor, conn)  # Call the write function
                    self.written_count += len(batch)
                    conn.commit()  # Commit transaction after each successful write
                    self.logger.debug(
                        f"Writer thread wrote a batch of {len(batch)} items (total written so far: {self.written_count})"
                    )
                except Exception as write_e:
                    conn.rollback()  # Rollback transaction in case of write error
                    self.logger.error(
                        f"Error in write_function: {write_e}. Transaction rolled back for current batch."
                    )
                    # Consider more sophisticated error handling here - e.g., retry mechanism, error queue
                finally:
                    self.data_queue.task_done()  # Signal task completion for the batch

            self.logger.info(
                f"Writer thread finished processing and wrote a total of {self.written_count} items."
            )

        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(
                f"Database error in Writer thread: {e}. Transaction rolled back."
            )
        except Exception as e:
            self.logger.error(f"Error in Writer thread: {e}")
        finally:
            if conn:
                conn.close()
            self.logger.debug("Writer thread finished.")

    def run(self):
        """
        Runs the Writer process.

        Keyword Args:
            profiling_enabled (bool, optional): If True, enables profiling for this run. Defaults to False.
            Profiling results are saved to 'Writer_run_profile.prof'.
            To analyze, use `import pstats; pstats.Stats('Writer_run_profile.prof').sort_stats('cumulative').print_stats(30)`
        """
        prof_filename = "Writer_run_profile.prof"  # Filename for profiling data

        if self.profiling_filename:
            self.logger.info(
                f"Writer thread starting with profiling enabled. Results will be in '{prof_filename}'"
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


class ReaderWriterPair:
    """
    Manages a Reader and Writer pair, creating and connecting their data queue internally.
    """

    def __init__(
        self,
        conn_params,
        reader_query,
        process_function,
        write_function,
        logger: logging.Logger = None,
        batch_size: int = 1000,
        num_reader_threads : int =4,
        max_queue_size: int =50,
        writer_batch_chunking: int =2,
        process_title: str = None,
        total_count: int = None,
        profiling_reader_enabled=False,  # Added profiling_enabled
        profiling_writer_enabled=False,  # Added profiling_enabled):
    ):
        """
        Initializes the ReaderWriterPair, creating the queue and instances of Reader and Writer.

        Args:
            conn_params (dict): Database connection parameters.
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
        self.reader_query = reader_query
        # Check for ORDER BY clause
        if "ORDER BY" not in reader_query.upper():
            raise ValueError(
                "Reader query must include an ORDER BY clause to ensure consistent row processing."
            )

        self.conn_params = conn_params
        self.batch_size = batch_size
        self.process_function = process_function
        self.write_function = write_function
        self.num_reader_threads = num_reader_threads
        self.logger = logger or self._setup_logger()
        self.data_queue = queue.Queue(
            maxsize=max_queue_size
        )  # Pair class creates the queue
        self.progress_queue = (
            queue.Queue()
        )  # Create progress queue for aggregated progress
        self.aggregation_thread_stop_event = (
            threading.Event()
        )  # Event to stop aggregation thread

        self.stop_event = threading.Event()  # Add a general stop event

        self.total_count = total_count  # Store total_count
        self.total_processed = multiprocessing.Value("i", 0)
        self.total_processed_lock = multiprocessing.Lock()  # Create the lock
        self.process_title = process_title  # Store process title if provided

        self.pbar_aggregated = tqdm(
            total=total_count, desc="Total Progress"
        )  # Initialize tqdm for aggregated progress

        self.profiling_reader_enabled = os.getenv(
            "PROFILING_READER_ENABLED", profiling_reader_enabled
        )  # Added profiling_reader_enabled
        self.profiling_reader_fileanme = None
        if self.profiling_reader_enabled:
            self.profiling_reader_fileanme = f"reader_threaded_{self.process_title if self.process_title else ''}.{time.strftime('%Y%m%d_%H%M%S')}.prof"


        self.profiling_writer_enabled = os.getenv(
            "PROFILING_WRITER_ENABLED", profiling_writer_enabled
        )  # Added profiling_writer_enabled

        self.profiling_writer_filename = None

        if self.profiling_writer_enabled:
            self.profiling_writer_filename = f"writer_threaded_{self.process_title if self.process_title else ''}.{time.strftime('%Y%m%d_%H%M%S')}.prof"

        self.writer_batch_chunking = (
            writer_batch_chunking  # Added writer_batch_chunking
        )
        # Instantiate Reader and Writer, passing the *same* data_queue to both
        self.reader = Reader(
            conn_params,
            reader_query,
            batch_size,
            process_function,
            self.data_queue,
            logger=self.logger,
            queue_size_backpressure_threshold=max_queue_size
            - 5,  # Adjusted backpressure threshold for when readers start to pause to avoid maxing out queue
            progress_queue=self.progress_queue,  # Pass progress_queue to Reader
            profiling_filename=self.profiling_reader_fileanme,
            stop_event=self.stop_event,  # Add a general stop event
            lock=self.total_processed_lock,  # Pass lock to Reader
            shared_processed_count=self.total_processed,  # Pass shared_processed_count to Reader
            total_count=self.total_count,  # Pass total_count


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



    def _progress_aggregation_process(self):  # Aggregation thread function
        """
        Continuously reads progress updates from the progress_queue, aggregates them,
        and updates the tqdm progress bar.
        """
        read_total = 0  # Renamed to read_total to clarify its purpose
        update_interval = 10000  # Update progress bar every N items (adjust as needed)
        while not self.aggregation_thread_stop_event.is_set():
            try:
                progress_increment = self.progress_queue.get(
                    timeout=10
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
                    progress_increment
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

    def _log_query_plan(self, sql, params=None):
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
            print(f"SQLite error: {e}")
            return None

    def run(self):
        """
        Runs the Reader and Writer threads CONCURRENTLY (Corrected Thread Management).
        """
        try:
            self.logger.info(
                f"Starting ReaderWriterPair with {self.num_reader_threads} reader threads."
            )

            aggregation_thread = threading.Thread(
                target=self._progress_aggregation_process, daemon=True
            )  # Create aggregation thread
            aggregation_thread.start()  # Start aggregation thread

            reader_threads = []  # Keep track of reader threads
            self.logger.info(
                f"ReaderWriterPair: Starting {self.num_reader_threads} reader threads"
                f"{' for process ' + self.process_title if self.process_title else ''}."
                f"{' with total count ' + str(self.total_count) if self.total_count is not None else ''}"
            )
            for _ in range(self.num_reader_threads):
                thread = threading.Thread(
                    target=self.reader.run
                )  # Corrected reader.run call - no args (single-threaded Reader.run will be used)
                reader_threads.append(thread)
                thread.start()

            writer_thread = threading.Thread(target=self.writer.run)  # Start writer thread
            self.logger.debug("START: WRITER TREAD")
            writer_thread.start()

            self.logger.debug(
                "ReaderWriterPair: Waiting for queue to be empty before joining queue to enshure are data is enventually writte"
            )  # Corrected placement of queue.join()
            """
            Ensuring Data Integrity: If the ReaderWriterPair didn't wait for the queue to be empty before joining the writer thread, there would be a risk that some data processed by the readers would still be sitting in the queue when the writer thread terminates. This data would never be written to the database, leading to data loss and inconsistency.
            """
            self.logger.debug(
                f"ReaderWriterPair: Queue size before join: {self.data_queue.qsize()}, pending tasks: {self.data_queue.unfinished_tasks}"
            )
            self.data_queue.join()

            self.logger.debug(
                f"ReaderWriterPair: Queue join completed. Queue size: {self.data_queue.qsize()}, pending tasks: {self.data_queue.unfinished_tasks}"
            )

            self.logger.info("ReaderWriterPair: WAIT FOR READER to complete.")
            for thread in reader_threads:
                thread.join()  # Wait for all reader threads to finish

            self.logger.info("ReaderWriterPair: WAIT FOR WRITER = complete.")
            writer_thread.join()  # Wait for writer thread to finish

            self.logger.info(
                "ReaderWriterPair: Signaling progress aggregation thread to stop."
            )
            self.aggregation_thread_stop_event.set()  # Signal aggregation thread to stop
            aggregation_thread.join(
                timeout=10
            )  # Wait for aggregation thread to finish, with a timeout

            self.logger.info("ReaderWriterPair process completed.")
        except KeyboardInterrupt:

            timeout: float = 2  # Define timeout at the start for clarity

            try:
                self.stop_event.set()
                self.aggregation_thread_stop_event.set()
                self.logger.warning(
                    f"ReaderWriterPair: Keyboard interrupt detected. Requesting threads to stop within {timeout} s."
                )
            except Exception as e_set_event: # More descriptive variable name
                self.logger.error(f"Error setting thread stop events during keyboard interrupt: {e_set_event}")

            finally:
                try:
                    # Wait for threads to join (with timeout)
                    for thread in reader_threads:
                        thread.join(timeout)
                    writer_thread.join(timeout)
                    aggregation_thread.join(timeout)
                except Exception as e_join_threads: # More descriptive variable name
                    self.logger.error(f"Error joining threads during keyboard interrupt: {e_join_threads}")
                finally:
                    self.logger.warning(
                        "ReaderWriterPair: Keyboard interrupt cleanup complete."
                    )
