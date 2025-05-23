"""Configuration settings for DuckDB sentence splitter."""

import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database path
DB_PATH = os.getenv("DUCKDB__DB_PATH")
if DB_PATH is None or DB_PATH.strip() == "":
    msg = "DUCKDB__DB_PATH environment variable is not set."
    raise ValueError(msg)

# Database and table settings
TEXT_SEGMENTS_TABLE = "abstract_segments"  # View with text segments
TEMP_TABLE = "segments_to_process"
SENTENCES_TABLE = "sentences"  # Table for storing split sentences
DUCKDB_MEMORY_LIMIT = "8GB"  # Memory limit for DuckDB instance

# Table schema definition
SENTENCES_TABLE_SCHEMA = """--sql
    pmid INTEGER,
    segment_number INTEGER,
    sentence_in_segment_order INTEGER,
    sentence VARCHAR,
    start_char INTEGER,
    end_char INTEGER
"""

CREATE_SENTENCES_TABLE_STMT = f"""--sql
CREATE TABLE IF NOT EXISTS {SENTENCES_TABLE} (
    {SENTENCES_TABLE_SCHEMA}
);
"""

# SpaCy configuration
SPACY_MODEL = "en_core_web_sm"  # Same sentence performance as en_core_web_md
SPACY_EXCLUDE_COMPONENTS = ["ner", "attribute_ruler", "lemmatizer", "tagger"]
N_PROCESS = 32  # Number of parallel spaCy processes
SPACY_BATCH_SIZE = 10000

# Processing parameters
BATCH_SIZE = 800000  # Batch size for reading from DuckDB
MEM_THRESHOLD_MB = 40000  # 40GB memory threshold
# Use multiple spacy model processes
# These might run multiprocessing themselves. Carefully monitor memory
MULTIPROCESSING = True
MAX_PIPELINES = 48  # Number of pipelines to run in parallel
MAX_INTERNAL_PROCESS_IF_MULTIPROCESSING = 1
