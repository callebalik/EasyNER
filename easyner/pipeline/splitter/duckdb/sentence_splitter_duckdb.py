# ruff: noqa : E501, D100
import concurrent.futures
import gc
import logging
import os
import sys  # Added for sys.exit
import time
from logging.handlers import RotatingFileHandler
from typing import Optional

import duckdb
import pandas as pd
import psutil
import spacy
from spacy.language import Language
from tqdm import tqdm  # Added tqdm

# Configure logging with both console and file handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)
log_file = os.path.join("logs", "sentence_splitter.log")

# Console handler with INFO level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# File handler with DEBUG level and rotation
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=10 * 1024 * 1024,
    backupCount=5,  # 10MB max file size
)
file_handler.setLevel(logging.DEBUG)

# Create formatters with timezone and include PID
log_format = "%(asctime)s %(levelname)s [PID:%(process)d] %(name)s: %(message)s"
date_format = "%Y-%m-%d %H:%M:%S %z"
console_formatter = logging.Formatter(log_format, datefmt=date_format)
file_formatter = logging.Formatter(log_format, datefmt=date_format)

# Set formatters to handlers
console_handler.setFormatter(console_formatter)
file_handler.setFormatter(file_formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Import configuration from splitter_config.py
from easyner.pipeline.splitter.duckdb.splitter_config import (  # noqa: E402
    BATCH_SIZE,
    CREATE_SENTENCES_TABLE_STMT,
    DB_PATH,
    DUCKDB_MEMORY_LIMIT,
    MAX_INTERNAL_PROCESS_IF_MULTIPROCESSING,
    MAX_PIPELINES,
    MEM_THRESHOLD_MB,
    MULTIPROCESSING,
    N_PROCESS,
    SENTENCES_TABLE,
    SPACY_BATCH_SIZE,
    SPACY_EXCLUDE_COMPONENTS,
    SPACY_MODEL,
    TEMP_TABLE,
    TEXT_SEGMENTS_TABLE,
)

_nlp_cache = None  # Global variable for worker processes


def monitor_memory() -> float:
    """Check memory usage and perform garbage collection if above threshold."""
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / (1024 * 1024)

    if mem_mb > MEM_THRESHOLD_MB:
        logger.info(
            f"Memory usage high ({mem_mb:.1f} MB). Forcing garbage collection...",
        )
        gc.collect()
        mem_mb = process.memory_info().rss / (1024 * 1024)
        logger.info(f"Memory after collection: {mem_mb:.1f} MB")

    return mem_mb


def get_sentences_with_spacy_sp(
    nlp: Language,
    batch: list[tuple],
    n_process: int = 1,
) -> list:
    """Process a text segment batch using spaCy with optimized parallel processing."""
    # Distribute texts to balance workload (critical for lock contention)
    # Sort texts by approximate length and interleave to balance work
    texts_with_meta = [(item[2], item[0], item[1]) for item in batch]
    texts_with_meta.sort(key=lambda x: len(x[0]))

    # Re-organize to distribute workload evenly across processes
    stride = max(1, len(texts_with_meta) // N_PROCESS)
    reordered = []
    for i in range(stride):
        reordered.extend(texts_with_meta[i::stride])

    # Extract data from reordered list
    texts = [item[0] for item in reordered]
    metadata = [(item[1], item[2]) for item in reordered]

    # Pre-allocate with estimated capacity (5 sentences per segment is typical)
    sentences_data = []

    # Configure for optimal performance with 48 cores
    # Reduce batch size to minimize lock duration
    for doc, (pmid, segment_number) in zip(
        nlp.pipe(
            texts,
            batch_size=min(SPACY_BATCH_SIZE, max(100, len(texts) // 48)),
            n_process=n_process,
        ),
        metadata,
    ):
        # Use direct list construction for slight performance gain
        sentence_in_segment_order = 1
        for sent in doc.sents:
            sentences_data.append(
                {
                    "pmid": pmid,
                    "segment_number": segment_number,
                    "sentence_in_segment_order": sentence_in_segment_order,
                    "sentence": sent.text,
                    "start_char": sent.start_char,
                    "end_char": sent.end_char,
                },
            )
            sentence_in_segment_order += 1

        # Explicit cleanup
        del doc

    return sentences_data


def _setup_db_connection(db_path: Optional[str]) -> duckdb.DuckDBPyConnection:
    """Set up db connection to DuckDB."""
    if not db_path:
        msg = "DB_PATH environment variable is not set."
        raise ValueError(msg)
    try:
        con = duckdb.connect(database=db_path, read_only=False)
        con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        con.execute(CREATE_SENTENCES_TABLE_STMT)
        msg = f"Connected to DuckDB database at {DB_PATH} and created table {SENTENCES_TABLE}"
        logger.info(msg)
        return con
    except duckdb.Error as e:
        logger.error(f"DuckDB Error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        raise


def _load_spacy_model(model_name: str) -> Language:
    """Load a spaCy model with specific components excluded."""
    try:
        nlp = spacy.load(model_name, exclude=SPACY_EXCLUDE_COMPONENTS)
        monitor_memory()  # Check memory after loading model
        msg = (
            f"spaCy model '{model_name}' loaded with "
            f"\n Included components: {', '.join([c for c in nlp.pipe_names if c not in SPACY_EXCLUDE_COMPONENTS])}"
            f"\n Excluded components: {', '.join(SPACY_EXCLUDE_COMPONENTS)}"
            f"\n (Current Memory Usage: {monitor_memory():.1f} MB)"
        )
        logger.info(msg)
        return nlp
    except Exception as e:
        logger.error(f"Error loading spaCy model: {e}")
        raise


def get_optimal_process_count(batch_size: int) -> int:
    """Determine the optimal number of processes to use based on the batch size.

    Args:
        batch_size: The number of items in the batch.

    Returns:
        The recommended number of processes to use.

    """
    cpu_count = os.cpu_count()
    if cpu_count is None:
        logger.warning("Unable to determine CPU count. Defaulting to 1 process.")
        return 1
    if batch_size < 1000:
        return max(1, cpu_count // 4)  # Use fewer processes for small batches
    elif batch_size < 10000:
        return max(1, cpu_count // 2)
    return N_PROCESS  # Use full capacity for large batches


def get_sentences_with_spacy_mp(
    batch: list[tuple],
    persistent_executor: concurrent.futures.ProcessPoolExecutor,
    num_pipelines: int = 3,
    n_process: int = 1,
) -> list:
    """Process batch using the persistent worker pool with optimal load balancing.

    Args:
        batch: List of (pmid, segment_number, text) tuples
        persistent_executor: The pre-initialized ProcessPoolExecutor with worker processes
        num_pipelines: Number of parallel pipelines to use
        n_process: Number of processes each pipeline should use

    Returns:
        List of sentence dictionaries

    """
    # Balance workload by text length for optimal distribution
    batch_with_length = [(len(item[2]), item) for item in batch]
    batch_with_length.sort(reverse=True)  # Sort by length (longest first)

    # Use greedy algorithm for distribution
    batches = [[] for _ in range(num_pipelines)]
    batch_lengths = [0] * num_pipelines

    for text_len, item in batch_with_length:
        min_idx = batch_lengths.index(min(batch_lengths))
        batches[min_idx].append(item)
        batch_lengths[min_idx] += text_len

    # Submit to persistent workers
    futures = [
        persistent_executor.submit(
            process_batch_in_worker,
            batch_chunk,
            n_process,
        )
        for batch_chunk in batches
        if batch_chunk  # Skip any empty batches
    ]

    # Collect results with proper error handling
    sentences = []
    for future in concurrent.futures.as_completed(futures):
        try:
            sentences.extend(future.result())
        except Exception as e:  # noqa: PERF203
            msg = f"Worker process failed to process batch: {e}"
            logger.error(msg)
            # Consider if we want to re-raise or continue with partial results

    return sentences


def initialize_worker(
    model_name: Optional[str] = None,
    exclude_components: Optional[list] = None,
) -> None:
    """Initialize worker process with a spaCy model.

    Reuses existing _load_spacy_model function to maintain consistency.
    """
    global _nlp_cache
    if _nlp_cache is None:
        # Use the default model settings if none provided
        actual_model = model_name or SPACY_MODEL
        actual_exclude = exclude_components or SPACY_EXCLUDE_COMPONENTS  # noqa: F841

        # Reuse existing model loading function
        _nlp_cache = _load_spacy_model(actual_model)
        logger.info(f"Worker [PID:{os.getpid()}]: Model loaded successfully")


def process_batch_in_worker(batch_chunk: list[tuple], n_process: int) -> list:
    """Process a batch using the cached spaCy model in this worker process."""
    global _nlp_cache
    if _nlp_cache is None:
        msg = "Worker not properly initialized with spaCy model"
        raise ValueError(msg)

    # Use the existing function with the cached model
    return get_sentences_with_spacy_sp(_nlp_cache, batch_chunk, n_process)


def main() -> None:  # noqa: C901
    """Process text segments from a DuckDB database.

    Split them into sentences and store the sentences back into the database, with progress reporting.
    """
    # Initialize variables that are used in the finally block
    total_processed = 0
    total_sentences = 0
    start_time = time.time()
    time_taken = 0
    con = None

    try:
        con = _setup_db_connection(DB_PATH)

        logger.info("Creating temporary table for processing...")
        con.execute(
            f"""--sql
            CREATE TEMPORARY TABLE {TEMP_TABLE} AS
            SELECT s.pmid, s.segment_number
            FROM {TEXT_SEGMENTS_TABLE} s
            WHERE NOT EXISTS (
                SELECT 1 FROM {SENTENCES_TABLE} t
                WHERE t.pmid = s.pmid AND t.segment_number = s.segment_number
            )
            AND s.is_header = FALSE
        """,
        )

        # Calculate total number of segments to process for tqdm
        count_result = con.execute(f"SELECT COUNT(*) FROM {TEMP_TABLE}").fetchone()
        total_segments = count_result[0] if count_result else 0
        logger.info(f"Total segments to process: {total_segments}")

        if total_segments == 0:
            logger.info("No segments to process. Exiting.")
            return

        # Create persistent executor outside the batch loop

        num_pipelines = 1
        if MULTIPROCESSING:
            # Safe fallbacks for max number of pipelines
            cpu_count = os.cpu_count()
            if cpu_count is not None:
                num_pipelines = min(
                    cpu_count - 1,
                    min(get_optimal_process_count(total_segments), MAX_PIPELINES),
                )
            else:
                logger.warning(
                    "Unable to determine CPU count. Defaulting to 1 pipeline.",
                )
                num_pipelines = 1

            logger.info(f"Creating {num_pipelines} persistent worker processes...")
            persistent_executor = concurrent.futures.ProcessPoolExecutor(
                max_workers=num_pipelines,
                initializer=initialize_worker,
                initargs=(SPACY_MODEL, SPACY_EXCLUDE_COMPONENTS),
            )
            n_process = min(
                MAX_INTERNAL_PROCESS_IF_MULTIPROCESSING,
                max(1, N_PROCESS // num_pipelines),
            )
        else:
            # --- Load spaCy model once with only necessary components for segmentation ---
            nlp = _load_spacy_model(SPACY_MODEL)

        with tqdm(total=total_segments, unit="segment") as pbar:
            offset = 0
            while offset < total_segments:
                # Get a batch of segment IDs from our temp table
                # This avoids the expensive NOT EXISTS query
                batch_ids = con.execute(
                    f"""--sql
                    SELECT pmid, segment_number
                    FROM {TEMP_TABLE}
                    LIMIT {BATCH_SIZE} OFFSET {offset}
                """,
                ).fetchall()

                if not batch_ids:
                    break

                # For each segment ID, fetch the actual text content
                # Note: We use a separate query to fetch text only for segments we'll process
                # This uses much less memory than fetching all columns at once
                pmids = [id[0] for id in batch_ids]
                seg_nums = [id[1] for id in batch_ids]

                # Use parameters to avoid SQL injection with UNNEST
                segments_data = con.execute(
                    f"""--sql
                    SELECT s.pmid, s.segment_number, s.segment
                    FROM {TEXT_SEGMENTS_TABLE} s
                    WHERE (pmid, segment_number) IN (
                        SELECT UNNEST(?), UNNEST(?)
                    )
                """,
                    [pmids, seg_nums],
                ).fetchall()

                # Inside main(), replace the multiprocessing section:
                if MULTIPROCESSING:
                    # Use the optimized workflow that leverages persistent workers
                    sentences_data = get_sentences_with_spacy_mp(
                        segments_data,
                        persistent_executor,
                        num_pipelines,
                        n_process,
                    )
                else:
                    sentences_data = get_sentences_with_spacy_sp(
                        nlp,
                        segments_data,
                        n_process=N_PROCESS,
                    )
                sentences_df = pd.DataFrame(sentences_data)
                con.append(SENTENCES_TABLE, sentences_df)
                con.commit()

                # # Insert in smaller chunks to reduce memory pressure
                # if not sentences_df.empty:
                #     for i in range(0, len(sentences_df), 500):
                #         chunk = sentences_df.iloc[i : i + 500]
                #         con.append(SENTENCES_TABLE, chunk)
                #         con.commit()

                #     total_sentences += len(sentences_df)

                # Update counters
                batch_size = len(segments_data)
                total_processed += batch_size
                total_sentences += len(sentences_df) if not sentences_df.empty else 0
                offset += batch_size
                pbar.update(batch_size)

                del sentences_data
                del sentences_df
                # Update progress display
                pbar.set_postfix(
                    {
                        "Processed": f"{total_processed}/{total_segments}",
                        "Sentences": total_sentences,
                        "Memory": f"{monitor_memory():.1f}MB",
                    },
                )

    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt: Exiting the script.")
        # The 'finally' block below will be executed before the script terminates.
        sys.exit(130)  # Exit with status 130 (standard for SIGINT)
    except duckdb.Error as e:
        logger.error(f"DuckDB Error: {e}")
    except spacy.errors as e:
        logger.error(f"SpaCy Error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        # Calculate time_taken if not already done
        time_taken = time.time() - start_time

        # Create a directory for benchmarks if it doesn't exist
        os.makedirs(".benchmarks", exist_ok=True)

        # Handle the case where total_processed might be 0
        segments_per_second = 0
        if total_processed > 0 and time_taken > 0:
            segments_per_second = total_processed / time_taken

        # Final report
        logger.info(f"Finished processing {total_processed} segments")
        logger.info(f"Total sentences inserted: {total_sentences}")
        logger.info(f"Total time: {time_taken:.2f} seconds")
        if total_processed > 0 and time_taken > 0:
            logger.info(f"Speed: {total_processed / time_taken:.2f} segments/second")
        #  Create a summary of the processing, even if interrupted
        # Store in .benchmarks/splitter_summary.csv
        with open(".benchmarks/splitter_summary.csv", "w") as f:
            f.write(
                f"Total segments processed, {total_processed}\n"
                f"Total sentences inserted, {total_sentences}\n"
                f"Total time taken, {time_taken:.2f} seconds\n"
                f"Speed, {segments_per_second:.2f} segments/second\n",
            )

        if con:
            con.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")

            con.close()
            logger.info("DuckDB connection closed.")

        if "persistent_executor" in locals():
            persistent_executor.shutdown(wait=True)


if __name__ == "__main__":

    main()
