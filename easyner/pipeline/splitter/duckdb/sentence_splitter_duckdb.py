# ruff: noqa : E501, D100
import gc
import logging
import os
import sys  # Added for sys.exit
import time
from typing import Optional

import duckdb
import pandas as pd
import psutil
import spacy
from spacy.language import Language
from spacy.tokens import Doc, Span
from tqdm import tqdm  # Added tqdm

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Import configuration from splitter_config.py
from easyner.pipeline.splitter.duckdb.splitter_config import (  # noqa: E402
    BATCH_SIZE,
    CREATE_SENTENCES_TABLE_STMT,
    DB_PATH,
    DUCKDB_MEMORY_LIMIT,
    MEM_THRESHOLD_MB,
    N_PROCESS,
    SENTENCES_TABLE,
    SPACY_BATCH_SIZE,
    SPACY_EXCLUDE_COMPONENTS,
    SPACY_MODEL,
    TEMP_TABLE,
    TEXT_SEGMENTS_TABLE,
)


def monitor_memory() -> float:
    """Check memory usage and perform garbage collection if above threshold."""
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / (1024 * 1024)

    if mem_mb > MEM_THRESHOLD_MB:
        print(f"Memory usage high ({mem_mb:.1f} MB). Forcing garbage collection...")
        gc.collect()
        mem_mb = process.memory_info().rss / (1024 * 1024)
        print(f"Memory after collection: {mem_mb:.1f} MB")

    return mem_mb


def get_sentences_with_spacy_sp(nlp: Language, batch: list[tuple]) -> list:
    """Process a text segment batch using spaCy and preserves sentence order within segments.

    Assumes that each text is non-empty which should be guaranteed by the SQL query.

    Args:
        nlp: The loaded spaCy NLP object.
        batch: A list of tuples, where each tuple is (pmid, segment_number, segment_text).

    Returns:
        A list of dictionaries with keys: pmid, segment_number, sentence_in_segment_order, sentence.

    """
    # Prepare texts and corresponding metadata for spaCy pipe
    texts = [item[2] for item in batch]
    metadata = [(item[0], item[1]) for item in batch]  # (pmid, segment_number)

    sentences_data = []

    # Process texts in parallel using nlp.pipe
    # We iterate through the docs and their original metadata simultaneously
    doc: Doc
    for doc, (pmid, segment_number) in zip(
        nlp.pipe(
            texts,
            batch_size=SPACY_BATCH_SIZE,
            n_process=N_PROCESS,
            # disable=SPACY_EXCLUDE_COMPONENTS, # Already excluded in model load
        ),
        metadata,
    ):
        sentence_in_segment_order = 1  # Index same as segment_number starts from 1
        sent: Span
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

        del doc  # Explicitly clear the doc to free up memory

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


def main() -> None:
    """Process text segments from a DuckDB database.

    Split them into sentences and store the sentences back into the database, with progress reporting.
    """
    try:
        con = _setup_db_connection(DB_PATH)
        # --- Load spaCy model once with only necessary components for segmentation ---
        nlp = _load_spacy_model(SPACY_MODEL)

        print("Creating temporary table for processing...")
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
        print(f"Total segments to process: {total_segments}")

        if total_segments == 0:
            print("No segments to process. Exiting.")
            return

        total_processed = 0
        total_sentences = 0
        start_time = time.time()

        with tqdm(total=total_segments, unit="segment") as pbar:
            offset = 0
            while offset < total_segments:
                # Explicitly force garbage collection
                gc.collect()

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

                # Process this batch
                sentences_data = get_sentences_with_spacy_sp(nlp, segments_data)
                sentences_df = pd.DataFrame(sentences_data)
                con.append(SENTENCES_TABLE, sentences_df)
                con.commit()
                del sentences_data
                del sentences_df

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
                offset += batch_size
                pbar.update(batch_size)

                # Update progress display
                pbar.set_postfix(
                    {
                        "Processed": f"{total_processed}/{total_segments}",
                        "Sentences": total_sentences,
                        "Memory": f"{monitor_memory():.1f}MB",
                    },
                )

        # Final report
        print(f"Finished processing {total_processed} segments")
        print(f"Total sentences inserted: {total_sentences}")
        time_taken = time.time() - start_time
        print(f"Total time: {time_taken:.2f} seconds")
        if total_processed > 0 and time_taken > 0:
            print(f"Speed: {total_processed / time_taken:.2f} segments/second")

        # Clean up temp table
        con.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")

    except KeyboardInterrupt:
        print("KeyboardInterrupt: Exiting the script.")
        # The 'finally' block below will be executed before the script terminates.
        sys.exit(130)  # Exit with status 130 (standard for SIGINT)
    except duckdb.Error as e:
        print(f"DuckDB Error: {e}")
    except spacy.errors as e:
        print(f"SpaCy Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if con:
            con.close()
            print("DuckDB connection closed.")


if __name__ == "__main__":
    # Check if the database path is provided as a command-line argument
    # If not, prompt the user for the database path

    main()
