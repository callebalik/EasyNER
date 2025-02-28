import multiprocessing
from multiprocessing import Lock, Value
import threading
import sqlite3
import queue
from queue import Queue
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
        data_queue: Queue,  # Use multiprocessing.Queue for type hint
        lock: Lock,
        shared_processed_count: Value,
        stop_event: threading.Event = None,
        logger: logging.Logger = None,
        queue_size_backpressure_threshold: int =50,
        progress_queue: Queue = None, # Use multiprocessing.Queue for type hint
        profiling_filename: str = None,
        total_rows: int = None,
        batch_queue: Queue = None,  # Batch queue for chunk assignment (REQUIRED for chunking)
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
        if batch_queue is None:
            raise ValueError("batch_queue must be provided for Reader when using chunking strategy.")
        self.batch_queue = batch_queue

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
        self.total_count = total_rows  # Store total_count
        self.profiling_file = profiling_filename

    def _read_and_process(self):
        """... (Reader class _read_and_process method) ..."""
        conn = None

        try:
            conn = sqlite3.connect(**self.conn_params)
            cursor = conn.cursor()
            total_processed_in_thread = 0
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

                try:
                    batch_desc = self.batch_queue.get(
                        timeout=10
                    )  # Get batch assignment from queue
                    if batch_desc is None:
                        break
                except queue.Empty:
                    self.logger.debug("Reader thread: batch_queue is empty. Exiting read loop.")
                    break

                offset = batch_desc["offset"]
                limit = batch_desc["limit"]

                params = {"offset": offset, "limit": limit} # Parameters for query
                self.logger.debug(f"Executing query with params: {params}")
                cursor.execute(self.query, params)
                batch = cursor.fetchmany(limit) # Redundant limit as execute already limits the query, but safety net

                self.logger.debug(f"Reader thread: Fetched batch size from DB: {len(batch)}")


                if not batch:
                    self.logger.warning(f"Reader thread: Unexpectedly got empty batch from cursor at offset {offset} from batch queue.") # Unepected as each batch should have data, unless query is incorrect and we've checked for total processed before fetching.
                    self.batch_queue.task_done() # Signal task done even if batch is unexpectedly empty
                    continue # Skip processing and get next batch from queue (or exit if queue is empty)

                processed_batch = self.process_function(batch, self.conn_params)

                self.logger.debug(f"Reader thread: Processed batch size: {len(processed_batch)}") # NEW LOG - PROCESSED BATCH SIZE


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

                    self.logger.debug(f"Reader thread: Queueing batch of size: {len(processed_batch)} before put(). Queue size: {self.data_queue.qsize()}") # NEW LOG - BEFORE PUT


                    self.data_queue.put(
                        processed_batch
                    )  # Put bastch into queue AFTER backpressure check
                    total_processed_in_thread += len(
                        processed_batch
                    )  # Count processed items, not fetched

                    self.logger.debug(f"Reader thread: Queued batch of size: {len(processed_batch)} after put(). Queue size: {self.data_queue.qsize()}") # NEW LOG - AFTER PUT


                    if (
                        self.progress_queue
                    ):  # Send progress update if progress_queue exists
                        self.progress_queue.put(len(processed_batch))

                batch_counter += 1
                if batch_counter % log_queue_size_interval == 0:
                    queue_size = self.data_queue.qsize()
                    self.logger.debug(
                        f"READ PROCESSED BATCH (lenght {len(processed_batch)}) - Queue size: {queue_size} (total processed in thread: {total_processed_in_thread})"
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
                f"START: READ + PROCESS (profiling -> '{self.profiling_file}')"
            )
            run_with_profiling(
                lambda: self._run_internal(num_threads), self.profiling_file
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
        self.batch_chunking = batch_chunking  # Accumulte multiple batches before writing
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

                self.logger.debug(f"Writer thread: Got batch from queue - size: {len(batch) if batch else 'Sentinel'}")

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

                # ENABLE THIS BATCH CHUNKING CODE
                if self.batch_chunking > 1:
                    accumulated_size = len(batch)
                    for _ in range(self.batch_chunking - 1):
                        if self.data_queue.empty():
                            break
                        next_batch = self.data_queue.get()
                        if next_batch is None:
                            sentinel_count += 1
                            self.data_queue.task_done()
                            break
                        accumulated_size += len(next_batch)
                        batch.extend(next_batch)
                        self.data_queue.task_done()
                        self.logger.debug(f"Writer thread: Accumulated batch size now {accumulated_size}")

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
        total_rows: int,
        logger: logging.Logger = None,
        batch_size: int = 5000,
        num_reader_threads : int =4,
        max_queue_size: int =200,
        writer_batch_chunking: int =10, # X batches to accumulate before writing to baleance read/write speed
        process_title: str = None,
        profiling_reader_enabled=False,  # Added profiling_enabled
        profiling_writer_enabled=False,  # Added profiling_enabled):
    ):
        """
        Initializes the ReaderWriterPair, creating the queue and instances of Reader and Writer.

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
            raise TypeError("process_function must be callable")

        # Validate write_function
        if not callable(write_function):
            raise TypeError("write_function must be callable")

        # Validate function signatures using test calls
        try:
            # Test process_function with empty batch
            test_batch = []
            result = process_function(test_batch, conn_params)
            if not isinstance(result, list):
                raise TypeError("process_function must return a list")
        except Exception as e:
            raise TypeError(
                f"Invalid process_function signature. Expected: "
                f"Function(List[Any], Dict[str, Any]) -> List[Any]. Error: {str(e)}"
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
            raise TypeError(
                f"Invalid write function. Sqlite Error: {str(e)}")
        except Exception as e:
            test_conn.close()
            raise TypeError(
                f"Invalid write_function signature. Expected: "
                f"Function(List[Any], sqlite3.Cursor, sqlite3.Connection) -> bool. Error: {str(e)}"
            )
        finally:
            test_conn.close()
        self.reader_query = reader_query
        # Check for ORDER BY clause
        if "ORDER BY" not in reader_query.upper():
            raise ValueError(
                "Reader query must include an ORDER BY clause to ensure consistent row processing."
            )
        if ":offset" not in reader_query:
            raise ValueError(
                "Reader query must include a :offset parameter for threaded pagination."
            )
        if ":limit" not in reader_query:
            raise ValueError(
                "Reader query must include a :limit parameter for threaded pagination."
            )
        # **NEW CHECKS: Ensure exactly ONE :limit and ONE :offset placeholder**
        if reader_query.lower().count(':limit') != 1:
            raise ValueError(
                "Reader query must contain exactly one ':limit' parameter placeholder."
            )
        if reader_query.lower().count(':offset') != 1:
            raise ValueError(
                "Reader query must contain exactly one ':offset' parameter placeholder."
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
            maxsize=max_queue_size
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
            total=total_rows, desc="Total Progress"
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
        if self.total_rows is None:
            raise ValueError("total_rows must be provided for chunking.")

        num_batches = (self.total_rows + self.batch_size - 1) // self.batch_size # Calculate number of batches
        self.logger.info(f"Populating batch queue with {num_batches} batches based on total_rows: {self.total_rows}.")
        for i in range(num_batches):
            offset = i * self.batch_size
            limit = self.batch_size
            self.batch_queue.put({'offset': offset, 'limit': limit})
        self.logger.info("Batch queue population complete.")


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

    def _log_query_plan(self, sql):
        """
        Logs the query execution plan for the provided SQL query.
        Handles named parameters by providing sensible defaults.
        """
        import sqlite3

        try:
            # Provide default values for common named parameters
            mock_params = {
                ':limit': self.batch_size,  # Use the batch size from the class
                ':offset': 0                # Start at 0
            }

            # Extract any other named parameters from the query
            # This regex finds all named parameters like :name in the SQL
            import re
            param_names = re.findall(r':(\w+)', sql)
            for name in param_names:
                if f':{name}' not in mock_params:
                    mock_params[f':{name}'] = 1  # Default value for other params

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


    def run(self):
        """
        Runs the Reader and Writer threads with proper queue monitoring and cleanup.
        """
        reader_threads = []
        writer_thread = None
        aggregation_thread = None

        try:
            # 1. FIRST: Set up and start all threads
            self._populate_batch_queue()

            # Start aggregation thread first
            self.logger.info("Starting progress aggregation thread")
            aggregation_thread = threading.Thread(
                target=self._progress_aggregation_process, daemon=True
            )
            aggregation_thread.start()

            # Start reader threads
            self.logger.info(f"ReaderWriterPair: Starting {self.num_reader_threads} reader threads")
            for _ in range(self.num_reader_threads):
                thread = threading.Thread(target=self.reader.run)
                thread.daemon = True
                reader_threads.append(thread)
                thread.start()

            # Start writer thread
            self.logger.info("Starting writer thread")
            writer_thread = threading.Thread(target=self.writer.run)
            writer_thread.daemon = True
            writer_thread.start()

            # 2. SECOND: Monitor overall processing progress
            while True:
                # Exit if all data has been processed
                if self.total_processed.value >= self.total_rows:
                    self.logger.info(f"All {self.total_rows} rows have been processed. Proceeding to cleanup.")
                    break

                # Exit if stop event is set
                if self.stop_event.is_set():
                    self.logger.info("Stop event detected during monitoring. Proceeding to cleanup.")
                    break

                # Brief sleep to allow for keyboard interrupts
                time.sleep(0.3)

            # 3. THIRD: Monitor batch queue until empty (needed before readers finish)
            self.logger.debug("Waiting for batch queue to be processed...")
            batch_queue_timeout = 300
            batch_queue_start = time.time()
            while self.batch_queue.unfinished_tasks > 0:
                if self.stop_event.is_set():
                    self.logger.warning("Stop event detected while waiting for batch queue. Breaking.")
                    break
                if time.time() - batch_queue_start > batch_queue_timeout:
                    self.logger.warning(f"Batch queue wait timed out after {batch_queue_timeout} seconds.")
                    break
                time.sleep(1)
            self.logger.debug(f"Batch queue monitoring completed. Unfinished tasks: {self.batch_queue.unfinished_tasks}")

            # 4. FOURTH: Wait for data queue to empty (needed before writer finishes)
            self.logger.debug("Waiting for data queue to be processed...")
            data_queue_timeout = 300
            data_queue_start = time.time()
            while self.data_queue.unfinished_tasks > 0:
                if self.stop_event.is_set():
                    self.logger.warning("Stop event detected while waiting for data queue. Breaking.")
                    break
                if time.time() - data_queue_start > data_queue_timeout:
                    self.logger.warning(f"Data queue wait timed out after {data_queue_timeout} seconds.")
                    break
                time.sleep(1)
            self.logger.debug(f"Data queue monitoring completed. Unfinished tasks: {self.data_queue.unfinished_tasks}")

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
                    f"ReaderWriterPair: Keyboard interrupt detected. Requesting threads to stop within {timeout} s."
                )

                # Drain both queues to unblock threads
                self._drain_queue(self.data_queue)
                self._drain_queue(self.batch_queue)
                self._drain_queue(self.progress_queue)

            except Exception as e_set_event:
                self.logger.error(f"Error setting thread stop events during keyboard interrupt: {e_set_event}")

            finally:
                try:
                    # Wait for threads to join (with timeout)
                    for thread in reader_threads:
                        thread.join(timeout)
                    writer_thread.join(timeout)
                    aggregation_thread.join(timeout)
                except Exception as e_join_threads:
                    self.logger.error(f"Error joining threads during keyboard interrupt: {e_join_threads}")
                finally:
                    self.logger.warning(
                        "ReaderWriterPair: Keyboard interrupt cleanup complete."
                    )

    # Add this helper method to the ReaderWriterPair class:
    def _drain_queue(self, q):
        """Helper method to drain a queue and unblock any threads waiting on it"""
        try:
            while True:
                q.get_nowait()
                q.task_done()
        except queue.Empty:
            pass