import threading
import sqlite3
import queue
import logging
from tqdm import tqdm
import time


class Reader:
    """
    Reader class to read data from a database, process it, and put it into a queue.
    """

    def __init__(
        self,
        conn_params,
        query,
        batch_size,
        process_function,
        data_queue,
        logger=None,
        queue_size_backpressure_threshold=50,
        progress_queue=None,  # Added progress_queue
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
        """
        self.conn_params = conn_params
        self.query = query
        self.batch_size = batch_size
        self.process_function = process_function
        self.data_queue = data_queue
        self.logger = logger or self._setup_logger()
        self.qsize_backbpressure_threshold = queue_size_backpressure_threshold
        self.progress_queue = progress_queue  # Store progress_queue

    def _setup_logger(self):
        """... (Reader class _setup_logger method) ..."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s - Reader"
        )  # Added 'Reader' to formatter
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _read_and_process(self):
        """... (Reader class _read_and_process method) ..."""
        conn = None
        try:
            conn = sqlite3.connect(**self.conn_params)
            cursor = conn.cursor()
            cursor.execute(self.query)
            total_processed = 0
            log_queue_size_interval = (
                10  # Log queue size every N batches (adjust as needed)
            )
            batch_counter = 0
            queue_check_interval = 20  # Check queue size every X seconds when queue is full (adjust as needed)

            while True:
                batch = cursor.fetchmany(self.batch_size)
                if not batch:
                    break  # No more data

                processed_batch = self.process_function(batch, self.conn_params)

                if processed_batch:  # Only put into queue if there is processed data
                    while (
                        self.data_queue.qsize() >= self.qsize_backbpressure_threshold
                    ):  # Backpressure check
                        self.logger.debug(
                            f"Data queue at or above fill level {self.qsize_backbpressure_threshold} (size: {self.data_queue.qsize()}). Reader thread pausing..."
                        )
                        time.sleep(
                            queue_check_interval
                        )  # Pause reader thread to let writer catch up
                        # (Optionally) You could add a timeout to this loop to prevent indefinite blocking in extreme cases

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
                        f"Reader thread processed batch - Queue size: {queue_size} (total processed in thread: {total_processed})"
                    )
                else:
                    self.logger.debug(
                        f"Reader thread processed batch of {len(processed_batch)} items (total: {total_processed})"
                    )

                self.logger.debug(
                    f"Reader thread processed a batch of {len(processed_batch)} items (total processed so far in thread: {total_processed})"
                )

        except sqlite3.Error as e:
            self.logger.error(f"Database error in Reader thread: {e}")
        except Exception as e:
            self.logger.error(f"Error in Reader thread process: {e}")
        finally:
            if conn:
                conn.close()
            self.logger.debug("Reader thread finished.")

    def run(self, num_threads=1):  # `num_threads` is now OPTIONAL with default 1
        """
        Runs the Reader process, in single or multi-threaded mode based on num_threads.

        Args:
            num_threads (int, optional): Number of reader threads to use. Defaults to 1 (single-threaded).
            If > 1, Reader will manage its own thread pool. If 1 or not provided,
            single-threaded (intended for ReaderWriterPair).
        """
        if num_threads <= 1:  # Single-threaded execution (for ReaderWriterPair usage)
            self.logger.info(
                "Reader thread starting read and process (single-threaded mode)."
            )
            self._read_and_process()
            self.logger.info(
                "Reader thread finished read and process (single-threaded mode)."
            )
        else:  # Multi-threaded execution (for standalone Reader usage)
            threads = []
            self.logger.info(
                f"Starting {num_threads} reader threads (multi-threaded mode)."
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
        self, conn_params, write_function, data_queue, logger=None, progress_queue=None
    ):
        """
        Initializes the Writer.

        Args:
            conn_params (dict): Database connection parameters.
            write_function (callable): Function to write a batch of data to the database.
            data_queue (queue.Queue): Queue to get data from.
            logger (logging.Logger, optional): Logger instance. Defaults to a basic logger.
            progress_queue (queue.Queue, optional): Queue to send progress updates.
        """
        self.conn_params = conn_params
        self.write_function = write_function
        self.data_queue = data_queue
        self.logger = logger or self._setup_logger()
        self.written_count = 0  # Initialize processed_count for Writer

    def _setup_logger(self):
        """... (Writer class _setup_logger method) ..."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s - Writer"
        )  # Added 'Writer' to formatter
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def writer_process(self):
        """... (Writer class writer_process method) ..."""
        conn: sqlite3.Connection = None
        try:
            conn = sqlite3.connect(**self.conn_params)
            cursor = conn.cursor()

            while True:
                batch = (
                    self.data_queue.get()
                )  # Get batch from queue (same queue as Reader's)
                if batch is None:  # Sentinel value received
                    self.data_queue.task_done()  # Signal task completion for sentinel
                    break  # Exit writer process

                try:
                    self.write_function(batch, cursor, conn)  # Call the write function
                    self.written_count += len(batch)
                    self.logger.debug(
                        f"Writer thread wrote a batch of {len(batch)} items (total written so far: {self.written_count})"
                    )
                except Exception as write_e:
                    conn.rollback()  # Rollback transaction in case of write error
                    self.logger.error(
                        f"Error in write_function: {write_e}. Transaction rolled back for current batch."
                    )
                    # Consider more sophisticated error handling here - e.g., retry mechanism, error queue

                self.data_queue.task_done()  # Signal task completion for the batch

            conn.commit()  # Final commit after all batches are written
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
        batch_size,
        process_function,
        write_function,
        num_reader_threads=4,
        logger=None,
        max_queue_size=20,
        total_count=None,
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
        """
        self.conn_params = conn_params
        self.reader_query = reader_query
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
        self.total_count = total_count  # Store total_count

        self.pbar_aggregated = tqdm(
            total=total_count, desc="Total Progress"
        )  # Initialize tqdm for aggregated progress

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
        )
        self.writer = Writer(
            conn_params,
            write_function,
            self.data_queue,
            logger=self.logger,
            progress_queue=self.progress_queue,  # Pass progress_queue to Writer
        )

    def _progress_aggregation_process(self):  # Aggregation thread function
        """
        Continuously reads progress updates from the progress_queue, aggregates them,
        and updates the tqdm progress bar.
        """
        read_total = 0 # Renamed to read_total to clarify its purpose
        while not self.aggregation_thread_stop_event.is_set():
            try:
                progress_increment = self.progress_queue.get(timeout=0.1) # Get read batch size
                read_total += progress_increment # Aggregate read progress
                queue_size = self.data_queue.qsize()
                written_count = self.writer.written_count

                # Format numbers with comma separators for readability
                formatted_written = f"{written_count:,}"
                formatted_queue_size = f"{queue_size:,}"
                formatted_read_total = f"{read_total:,}" # Optional: format read_total as well

                self.pbar_aggregated.update(progress_increment) # Update progress bar based on read progress
                self.pbar_aggregated.set_postfix(
                    written=formatted_written, # Use formatted written count
                    InQueue=formatted_queue_size, # Use formatted queue size
                    read=formatted_read_total # Optional: show formatted read count in postfix if desired
                )
                self.progress_queue.task_done()
            except queue.Empty:  # No progress update in queue within timeout
                continue  # Check again if stop event is set
            except Exception as e: # Catch any other potential exceptions in aggregation
                self.logger.error(f"Error in progress aggregation thread: {e}")
                break # Exit aggregation loop if an error occurs

        self.pbar_aggregated.close()  # Ensure progress bar is closed at the end

    def _setup_logger(self):
        """Sets up a basic logger for the ReaderWriterPair if none is provided."""
        logger = logging.getLogger(__name__)  # Or a more specific name
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s - ReaderWriterPair"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def run(self):
        """
        Runs the Reader and Writer threads CONCURRENTLY (Corrected Thread Management).
        """
        self.logger.info(
            f"Starting ReaderWriterPair with {self.num_reader_threads} reader threads."
        )

        aggregation_thread = threading.Thread(
            target=self._progress_aggregation_process, daemon=True
        )  # Create aggregation thread
        aggregation_thread.start()  # Start aggregation thread

        reader_threads = []  # Keep track of reader threads
        self.logger.info(
            f"ReaderWriterPair: Starting {self.num_reader_threads} reader threads."
        )
        for _ in range(self.num_reader_threads):
            thread = threading.Thread(
                target=self.reader.run
            )  # Corrected reader.run call - no args (single-threaded Reader.run will be used)
            reader_threads.append(thread)
            thread.start()

        writer_thread = threading.Thread(target=self.writer.run)  # Start writer thread
        self.logger.info("ReaderWriterPair: Starting writer thread.")
        writer_thread.start()

        self.logger.info(
            "ReaderWriterPair: Waiting for queue to be empty before joining writer."
        )  # Corrected placement of queue.join()
        self.data_queue.join()  # Wait for queue to be empty - NOW BEFORE WRITER JOIN

        self.logger.info("ReaderWriterPair: Waiting for reader threads to complete.")
        for thread in reader_threads:
            thread.join()  # Wait for all reader threads to finish

        self.logger.info("ReaderWriterPair: Waiting for writer thread to complete.")
        writer_thread.join()  # Wait for writer thread to finish

        self.logger.info(
            "ReaderWriterPair: Signaling progress aggregation thread to stop."
        )
        self.aggregation_thread_stop_event.set()  # Signal aggregation thread to stop
        aggregation_thread.join(
            timeout=2
        )  # Wait for aggregation thread to finish, with a timeout

        self.logger.info("ReaderWriterPair process completed.")
