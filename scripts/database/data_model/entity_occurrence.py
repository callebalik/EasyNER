# entity_occurrence_module.py
import csv
import sqlite3
import time

from tqdm import tqdm
from .schema import *
import logging
from ..db_data_exchanger import DBDataExchanger
from ..db_main import EasyNerDBHandler
from pathlib import Path
from ..core.db_engine import ReaderWriterPair
class BaseComponent:
    """ Common base class for all components with shared logger and database connection. """
    def init_deps(self, db_system : EasyNerDBHandler,):
        self.logger = db_system.logger
        self.cursor = db_system.cursor
        self.conn = db_system.conn
        self.log_query_plan = db_system.log_query_plan
        self.conn_params_dict = db_system.conn_params_dict
        self.parent = db_system
        self.data : DBDataExchanger = db_system.data_exchanger  # Add this line
        return self

class SchemaManager(BaseComponent):
    def __init__(self, parent):
        super().init_deps(parent)


    def setup_tables(self):
        """Create tables for entity occurrences if they don't exist"""
        self.logger.info("Setting up tables for entity occurrence analysis...")

        self.cursor.execute(schema_create_table_docs)
        self.cursor.execute(schema_create_table_sentences)
        self.cursor.execute(schema_create_table_ne_lookup)
        self.cursor.execute(schema_create_table_ne_aggregated)
        self.cursor.execute(schema_create_table_ne_class)
        self.cursor.execute(schema_create_table_ne_error)
        self.cursor.execute(schema_create_table_ne)

        self.conn.commit()

        self.logger.info("Tables created successfully.")

    def setup_views(self):
        """Create views for entity occurrences if they don't exist"""
        self.logger.info("Setting up views for entity occurrence analysis...")

        VIEW_NE_VALIDATION_NORMALIZATION.refresh(self.cursor)
        VIEW_NE_PRESENTATION.refresh(self.cursor)

        self.conn.commit()

        self.logger.info("Views created successfully.")

    def migrate_ne_table(self):
        """Migrate data from entity_occurrences to NE table"""
        self.logger.info("Starting migration to NE table...")

        migration_sql = """
        INSERT INTO NE (
            NE_ID,          -- Primary key
            TXT,            -- Entity text
            NE_CLASS_ID,    -- Entity class ID
            DOC_ID,         -- Document ID
            SENT_IDX,       -- Sentence index
            TXT_NORM,       -- Normalized text (NULL initially)
            NE_NORM_ID,     -- Normalized entity ID (NULL initially)
            ERROR_ID,       -- Error code
            OVERLAP,        -- Entity overlap flag
            SPAN_START,     -- Span start position
            SPAN_END        -- Span end position
        )
        SELECT
            id,
            entity_text,
            entity_id,
            document_id,
            sentence_index,
            NULL,           -- TXT_NORM to be populated later
            NULL,           -- NE_NORM_ID to be populated later
            error_id,
            overlap,
            span_start,
            span_end
        FROM entity_occurrences;
        """

        try:
            # Start transaction
            self.cursor.execute("BEGIN TRANSACTION")

            # Execute migration
            self.cursor.execute(migration_sql)
            migrated_count = self.cursor.rowcount
            self.logger.info(f"Migrated {migrated_count} records")

            # Verify migration
            self.verify_migration()

            # Commit if verification passed
            self.conn.commit()
            self.logger.info("Migration completed and verified successfully")

        except Exception as e:
            self.conn.rol

    def verify_migration(self):
        """Verify the migration from entity_occurrences to NE table"""

        verification_queries = [
            # Basic count comparison
            """
            SELECT
                'Total Records' as check_type,
                (SELECT COUNT(*) FROM entity_occurrences) as source_count,
                (SELECT COUNT(*) FROM NE) as target_count,
                CASE
                    WHEN (SELECT COUNT(*) FROM entity_occurrences) = (SELECT COUNT(*) FROM NE)
                    THEN 'OK'
                    ELSE 'MISMATCH'
                END as status
            """,

            # Check column mappings
            """
            SELECT
                'Column Mapping' as check_type,
                COUNT(*) as mismatches,
                'OK' as status
            FROM entity_occurrences eo
            JOIN NE ne ON eo.id = ne.NE_ID
            WHERE
                eo.entity_text != ne.TXT OR
                eo.entity_id != ne.NE_CLASS_ID OR
                eo.document_id != ne.DOC_ID OR
                eo.sentence_index != ne.SENT_IDX OR
                eo.error_id IS NOT ne.ERROR_ID OR
                eo.overlap IS NOT ne.OVERLAP OR
                eo.span_start IS NOT ne.SPAN_START OR
                eo.span_end IS NOT ne.SPAN_END
            """,

            # Check for orphaned records
            """
            SELECT
                'Orphaned Records' as check_type,
                COUNT(*) as mismatches,
                CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FAILED' END as status
            FROM NE ne
            LEFT JOIN entity_occurrences eo ON ne.NE_ID = eo.id
            WHERE eo.id IS NULL
            """,

            # Verify foreign key constraints
            """
            SELECT
                'Foreign Key Violations' as check_type,
                (
                    SELECT COUNT(*) FROM NE ne
                    LEFT JOIN documents d ON ne.DOC_ID = d.DOC_ID
                    WHERE d.DOC_ID IS NULL
                ) +
                (
                    SELECT COUNT(*) FROM NE ne
                    LEFT JOIN sentences s ON ne.DOC_ID = s.DOC_ID AND ne.SENT_IDX = s.SENT_IDX
                    WHERE s.DOC_ID IS NULL
                ) as violations,
                CASE
                    WHEN (
                        SELECT COUNT(*) FROM NE ne
                        LEFT JOIN documents d ON ne.DOC_ID = d.DOC_ID
                        WHERE d.DOC_ID IS NULL
                    ) = 0 AND
                    (
                        SELECT COUNT(*) FROM NE ne
                        LEFT JOIN sentences s ON ne.DOC_ID = s.DOC_ID AND ne.SENT_IDX = s.SENT_IDX
                        WHERE s.DOC_ID IS NULL
                    ) = 0
                    THEN 'OK'
                    ELSE 'FAILED'
                END as status
            """
        ]

        conn = self.conn
        cursor = self.cursor

        try:
            for query in verification_queries:
                cursor.execute(query)
                result = cursor.fetchone()

                # Handle different result formats
                if len(result) == 4:  # Total Records query
                    check_type, source_count, target_count, status = result
                    self.logger.info(f"{check_type}: source={source_count}, target={target_count} - Status: {status}")
                else:  # Other queries with 3 columns
                    check_type, count, status = result
                    self.logger.info(f"{check_type}: count={count} - Status: {status}")

                if status != 'OK':
                    self.logger.error(f"Verification failed for {check_type}")
                    if check_type == 'Column Mapping':
                        self._show_sample_mismatches(cursor)
                    elif check_type == 'Total Records':
                        self.logger.error(f"Count mismatch: source={source_count}, target={target_count}")

        except Exception as e:
            self.logger.error(f"Verification failed with error: {e}")
            raise

    def migrate_and_setup_ne_aggregation(self):
        """
        Complete schema migration and setup for NE aggregation.

        Uses * for column selection to preserve all columns without explicitly naming them.
        Only the foreign key constraint between NE and NE_AGGR is modified.
        """
        import sqlite3
        import time

        self.logger.info("Starting entity aggregation schema migration...")
        start_time = time.time()

        # Check if NE_AGGR exists
        ne_aggr_exists = self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE_NE_AGGR,)
        ).fetchone() is not None

        # Check for incorrect foreign key constraint
        fk_constraints = self.cursor.execute(f"PRAGMA foreign_key_list({TABLE_NE})").fetchall()

        has_incorrect_fk = False
        for fk in fk_constraints:
            if fk[2] == TABLE_NE_AGGR and fk[3] == TXT_NORM:
                has_incorrect_fk = True
                self.logger.warning(
                    f"Found incorrect FK: {TABLE_NE}.{TXT_NORM} → {TABLE_NE_AGGR}.{NE_NORM_ID}"
                )
                break

        # If NE has incorrect FK constraints, we need to recreate it
        if has_incorrect_fk:
            self.logger.info("Recreating NE table to fix FK constraints...")

            # Check if we have any data in NE
            ne_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE}").fetchone()[0]

            if ne_count > 0:
                self.logger.info(f"NE table contains {ne_count:,} rows, creating backup...")

                # Create backup of NE table with all columns using *
                self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NE}_backup AS SELECT * FROM {TABLE_NE}")
                backup_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE}_backup").fetchone()[0]
                self.logger.info(f"Backed up {backup_count:,} rows from {TABLE_NE}")

                try:
                    # Drop and recreate NE table with correct FK constraints
                    self.cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NE}")

                    # Create NE table with correct FK constraint using schema_create_table_ne from schema.py
                    self.cursor.execute(schema_create_table_ne)

                    # Copy data back from backup using * for all columns
                    self.cursor.execute(f"INSERT INTO {TABLE_NE} SELECT * FROM {TABLE_NE}_backup")

                    self.logger.info(f"Restored {self.cursor.rowcount:,} rows to {TABLE_NE} with correct schema")

                    # Make sure we commit after this important operation
                    self.conn.commit()

                except Exception as e:
                    self.conn.rollback()
                    self.logger.error(f"Error during NE table migration: {e}")
                    return False

        # Create or recreate NE_AGGR table
        try:
            if ne_aggr_exists:
                # Check if NE_AGGR has data
                aggr_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE_AGGR}").fetchone()[0]

                if aggr_count > 0:
                    self.logger.info(f"NE_AGGR table contains {aggr_count:,} rows, creating backup...")
                    self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NE_AGGR}_backup AS SELECT * FROM {TABLE_NE_AGGR}")

                # Drop the table to recreate it with correct schema
                self.cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NE_AGGR}")

            # Create NE_AGGR with proper schema from schema.py
            self.cursor.execute(schema_create_table_ne_aggregated)
            self.logger.info(f"Created {TABLE_NE_AGGR} table with correct schema")

            # Create essential indexes for NE_AGGR
            self.logger.info("Creating essential indexes for NE_AGGR...")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NE_AGGR}_class_txt ON {TABLE_NE_AGGR} ({CLASS_ID}, {TXT_NORM})")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NE_AGGR}_txt ON {TABLE_NE_AGGR} ({TXT_NORM})")

            # Restore NE_AGGR data if we had a backup
            if ne_aggr_exists and aggr_count > 0:
                self.logger.info(f"Restoring data to {TABLE_NE_AGGR}...")
                try:
                    # Use * for column selection to preserve all original columns
                    self.cursor.execute(f"INSERT INTO {TABLE_NE_AGGR} SELECT * FROM {TABLE_NE_AGGR}_backup")
                    self.logger.info(f"Restored {self.cursor.rowcount:,} rows to {TABLE_NE_AGGR}")
                except sqlite3.IntegrityError:
                    # Handle any uniqueness violations
                    self.logger.warning("Integrity error during restore. Inserting only unique combinations...")
                    self.cursor.execute(f"""
                    INSERT INTO {TABLE_NE_AGGR} ({CLASS_ID}, {TXT_NORM})
                    SELECT DISTINCT {CLASS_ID}, {TXT_NORM} FROM {TABLE_NE_AGGR}_backup
                    """)
                    self.logger.info(f"Restored {self.cursor.rowcount:,} unique rows to {TABLE_NE_AGGR}")

            # Create indexes for NE table
            self.logger.info("Creating/updating indexes for NE table...")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NE}_norm_id ON {TABLE_NE} ({NE_NORM_ID})")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NE}_txt_norm ON {TABLE_NE} ({TXT_NORM})")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NE}_class_txt ON {TABLE_NE} ({CLASS_ID}, {TXT_NORM})")

            # Final commit
            self.conn.commit()

            elapsed = time.time() - start_time
            self.logger.info(f"Schema migration completed in {elapsed:.2f} seconds")

            return True

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error during schema setup: {e}")
            return False

    def _show_sample_mismatches(self, cursor):
            """Show sample of mismatches between old and new tables"""
            cursor.execute("""
                SELECT
                    eo.id,
                    eo.entity_text,
                    ne.TXT,
                    eo.entity_id,
                    ne.NE_CLASS_ID
                FROM entity_occurrences eo
                JOIN NE ne ON eo.id = ne.NE_ID
                WHERE
                    eo.entity_text != ne.TXT OR
                    eo.entity_id != ne.NE_CLASS_ID
                LIMIT 5
            """)
            mismatches = cursor.fetchall()
            self.logger.error("Sample mismatches:")
            for mismatch in mismatches:
                self.logger.error(
                    f"ID: {mismatch[0]}, "
                    f"Old: {mismatch[1]}/{mismatch[3]}, "
                    f"New: {mismatch[2]}/{mismatch[4]}"
                )


class Preprocessor(BaseComponent):
    def __init__(self, parent):
        super().init_deps(parent)
        self._initialize_normalization_patterns()

    def _initialize_normalization_patterns(self):
        """Initialize regex patterns and translation tables for text normalization"""
        import re
        import string

        # Store these as instance variables so they're only created once
        self._leading_chars_pattern = re.compile(r'^[%"\'`-]+\s*')
        self._leading_space_chars_pattern = re.compile(r'^\s*[%"\'`-]+\s*')
        self._possessive_s_pattern = re.compile(r'\'s\b')
        self._plural_possessive_pattern = re.compile(r's\'\b')
        self._whitespace_pattern = re.compile(r'\s+')
        self._covid_pattern = re.compile(r'covid[-\s]?19')
        self._non_alnum_pattern = re.compile(r'[^\w\s-]')
        self._redundant_hyphen_pattern = re.compile(r'-+')
        self._edge_hyphen_pattern = re.compile(r'(^-|-$)')

        # Create translation table once
        self._punct_translator = str.maketrans('', '', string.punctuation.replace('-', ''))

        # Define contractions dictionary once
        self._contractions = {
            "ain't": "is not", "aren't": "are not", "can't": "cannot",
            "couldn't": "could not", "didn't": "did not", "doesn't": "does not",
            "don't": "do not", "hadn't": "had not", "hasn't": "has not",
            "haven't": "have not", "he'd": "he would", "he'll": "he will",
            "he's": "he is", "i'd": "i would", "i'll": "i will",
            "i'm": "i am", "i've": "i have", "isn't": "is not",
            "it's": "it is", "let's": "let us", "mustn't": "must not",
            "shan't": "shall not", "she'd": "she would", "she'll": "she will",
            "she's": "she is", "shouldn't": "should not", "that's": "that is",
            "there's": "there is", "they'd": "they would", "they'll": "they will",
            "they're": "they are", "they've": "they have", "we'd": "we would",
            "we'll": "we will", "we're": "we are", "we've": "we have",
            "weren't": "were not", "what'll": "what will", "what're": "what are",
            "what's": "what is", "what've": "what have", "where's": "where is",
            "who'd": "who would", "who'll": "who will", "who's": "who is",
            "who've": "who have", "won't": "will not", "wouldn't": "would not",
            "you'd": "you would", "you'll": "you will", "you're": "you are",
            "you've": "you have"
        }

        # Pre-compile contraction patterns
        self._contraction_patterns = [(re.compile(r'\b' + re.escape(c) + r'\b'), e) for c, e in self._contractions.items()]

    def _normalize_entity_text(self, text):
        """
        Normalize entity text by:
        1. Converting to lowercase
        2. Removing leading/trailing whitespace
        3. Removing special characters
        4. Expanding contractions
        5. Removing stop words (optional)
        6. Handling possessive forms
        7. Removing special leading characters (%, ", -, etc.)

        Args:
            text: The entity text to normalize

        Returns:
            Normalized entity text
        """
        from functools import lru_cache

        # Thread-local LRU cache for frequently seen identical strings
        @lru_cache(maxsize=5000)
        def normalize_text_cached(input_text):
            if not input_text:
                return ""

            # 1. Convert to lowercase & trim
            normalized = input_text.lower().strip()

            # 2. Remove special leading character combinations
            normalized = self._leading_chars_pattern.sub('', normalized)
            normalized = self._leading_space_chars_pattern.sub('', normalized)

            # 3. Handle possessive forms
            normalized = self._possessive_s_pattern.sub('', normalized)
            normalized = self._plural_possessive_pattern.sub('s', normalized)

            # 4. Remove punctuation except hyphens
            normalized = normalized.translate(self._punct_translator)

            # 5. Normalize whitespace (initial pass)
            normalized = self._whitespace_pattern.sub(' ', normalized)

            # 6. Expand contractions
            for pattern, replacement in self._contraction_patterns:
                normalized = pattern.sub(replacement, normalized)

            # 7. Domain-specific normalizations
            normalized = self._covid_pattern.sub('covid19', normalized)

            # 8. Final cleanup
            normalized = self._non_alnum_pattern.sub('', normalized)
            normalized = self._redundant_hyphen_pattern.sub('-', normalized)
            normalized = self._edge_hyphen_pattern.sub('', normalized)

            # 9. Final whitespace normalization
            normalized = self._whitespace_pattern.sub(' ', normalized).strip()

            return normalized

        # Call the cached function
        return normalize_text_cached(text)

    def populate_normalized_txt_column(self):
        """
        Populate the TXT_NORM column in the NE table with normalized entity text.
        Uses ReaderWriterPair for efficient multithreaded processing.

        - Filters out entities with error codes
        - Filters out entities that overlap with other entities (OVERLAP = TRUE)
        - Only processes entities where TXT_NORM is NULL
        """
        self.logger.info("Starting text normalization process...")

        # Create index for faster querying
        self.logger.info("Creating supporting indexes...")

        # Index for the query filtering
        ind1 = Index(TABLE_NE, [TXT_NORM, ERROR_ID, NE_OVERLAP], logger=self.logger)
        ind1.create_if_not_exists(self.cursor)

        # Index for the ordering in reader query (important for offset/limit)
        ind2 = Index(TABLE_NE, [DOC_ID, SENT_IDX], logger=self.logger)
        ind2.create_if_not_exists(self.cursor)

        # Create reader query that selects entities needing normalization
        # Not entierly correct as we will not process a batch size of NE_PRIMARY_IDS, but the filtered one. So the total_rows will > batch_size in most cases and we willl repeat processing on some rows

        ind_norm = Index(TABLE_NE, [TXT_NORM, NE_PRIMARY_ID], logger=self.logger)
        ind_norm.create_if_not_exists(self.cursor)
        ind_norm.analyze(self.cursor)

        # NULL First: SQLite naturally orders NULL values first when using ASC ordering
        # No Data Structure Changes: Keeps your existing offset/limit pagination approach
        # Efficient Processing: NULL values will be processed first, before any non-NULL values
        # Deterministic Order: Secondary sort by NE_PRIMARY_ID ensures consistent pagination
        # Index Usage: Can leverage an index on (TXT_NORM, NE_PRIMARY_ID)

        reader_query = f"""--sql
            SELECT {NE_PRIMARY_ID}, {TXT}
            FROM {TABLE_NE}
            WHERE {ERROR_ID} IS NULL
            AND {NE_OVERLAP} = 0
            AND {TXT_NORM} IS NULL -- Shouuld be almost none as we're sorting by this column - therefore at the end of WHERE clauses
            ORDER BY {TXT_NORM} ASC, {NE_PRIMARY_ID} -- SQLite considers NULL values to be smaller than any other values for sorting purposes
            LIMIT :limit OFFSET :offset
        """

        # Get total rows to process
        total_rows = self.cursor.execute(f"""
            SELECT COUNT(*) FROM {TABLE_NE}
            WHERE {TXT_NORM} IS NULL
            AND {ERROR_ID} IS NULL
            AND {NE_OVERLAP} = 0
        """).fetchone()[0]


        # Log query plan
        self.log_query_plan(reader_query, {"limit": 1000, "offset": 0})

        # Define process function that runs in reader threads with thread-local optimizations
        # Create a reference to self._normalize_entity_text for use in the process function
        normalize_func = self._normalize_entity_text

        def process_function(batch, conn_params):
            """Process a batch of entities by normalizing their text"""
            normalized_entities = []

            # Process each entity in batch using the class method
            for ne_id, text in batch:
                normalized_text = normalize_func(text)
                normalized_entities.append((normalized_text, ne_id))

            return normalized_entities

        # Define writer function to update the database
        def writer_function(results, cursor, conn):
            """Write normalized text values to database with optimized settings"""
            # Update database with normalized text values
            cursor.executemany(
                f"UPDATE {TABLE_NE} SET {TXT_NORM} = ? WHERE {NE_PRIMARY_ID} = ?",
                results
            )



        self.logger.info(f"Found {total_rows} entities that need normalization")

        if total_rows == 0:
            self.logger.info("No entities to normalize. Exiting.")
            return

        # Create reader-writer pair for parallel processing with optimized parameters
        rw_pair = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            batch_size=50000,           # 50K entities per batch
            max_queue_size=100,          # Allow up to 30 batches in queue (1 500 000 entities)
            num_reader_threads=2,       # 8 parallel reader threads for text processing
            writer_batch_chunking=15,   # Write 10 batches (500K entities) in one transaction
            reader_query=reader_query,
            process_function=process_function,
            write_function=writer_function,
            total_rows=total_rows,
            process_title="Normalizing entity text"
        )

        # Execute the normalizationcha   process
        self.logger.info("Starting multi-threaded text normalization...")
        rw_pair.run()

        self.logger.info(f"Text normalization complete. Processed {total_rows} entities.")

        # # Create final index on normalized text for faster lookups
        # self.logger.info("Creating index on normalized text column...")
        # index_norm = Index(TABLE_NE, [TXT_NORM], logger=self.logger)
        # index_norm.create_if_not_exists(self.cursor)
        # index_norm.analyze(self.cursor)

        # Analyze the table to optimize future query plans
        # self.cursor.execute(f"ANALYZE {TABLE_NE}")
        self.logger.info("Normalization process complete.")

    def preprocess(self, text):
        """Preprocess text before entity occurrence analysis"""
        try:
            # Implementation would go here
            return text
        except Exception as e:
            self.logger.error(f"Error preprocessing text: {e}")
            return None

    def set_error_entity_error_codes(
        self,
        error_info: str = "/home/carloa/Desktop/EasyNer/dictionaries/misslabeled_ner.csv",
        error_codes_path: str = "entity_error_codes.json",
        batch_size: int = 100000,
    ) -> None:
        """
        Attach error information to the entity_occurrences table. The error information is expected to be in the following format:

        entity_type,entity_text,error_id
        DIS,fires,MISSL
        DIS,forrest fire,MISSL
        DIS,earthquake,MISSL
        DIS,drought,MISSL

        Built in error codes are:
        {
            "error_codes": {
                "MISSP": "Misspelling",
                "MISSL": "Misslabeled Entity. Entity is not of the specified type",
                "AMBIG": "Ambiguous"
            }
        }

        This function will:
        0. read error_info.csv
        1. If not present create new TABLE entity_error_codes with columns id, error_id, error_description
        2. Populate entity_error_codes with error_codes and corresponding error_description from error_info
        2. If not present: create a new column error_id in TABLE entity_occurrences that holds references to the entity_error_codes table entity_error_codes.id. Default value is NULL, which means no error
        3. Set entity_occurrences.error_id according to error_info["entity_errors"] for matching entity_type and entity_text

        Args:
            error_info: A dictionary containing error information as described above
        """

        error_codes = {
            "MISSP": "Misspelling",
            "MISSL": "Misslabeled Entity. Entity is not of the specified type",
            "AMBIG": "Ambiguous",
        }

        try:

            # 1. Check if error_id column exists
            self.logger.info(f"Checking for {ERROR_ID} column in {TABLE_NE}")
            self.cursor.execute(f"PRAGMA table_info({TABLE_NE})")
            columns = [column[1] for column in self.cursor.fetchall()]
            if f"{ERROR_ID}" not in columns:
                # Add error_id column if it doesn't exist
                self.logger.info(f"Please review database schema. Column {ERROR_ID} should be presen't on {TABLE_NE} and reference {TABLE_NE_ERROR}")
                return # We can't proceed without the error_id column as the foreign key will fail

            # 2. Check if entity_error_codes table exists
            self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NE_ERROR}'")
            if not self.cursor.fetchone():
                self.logger.info(f"Creating {TABLE_NE_ERROR} table...")
                self.cursor.execute(schema_create_table_ne_error)
                self.conn.commit()

            # 3 Populate error codes table
            error_code_data = [(label, desc) for label, desc in error_codes.items()]
            try:
                self.cursor.executemany(
                    f"INSERT OR IGNORE INTO {TABLE_NE_ERROR} ({ERROR_ID}, {ERROR_DESC}) VALUES (?, ?)",
                    error_code_data,
                )
                self.logger.info(f"Inserted {len(error_code_data)} new error codes")
                self.conn.commit()
            except sqlite3.IntegrityError as e:
                self.logger.warning(f"Error inserting error codes: {e}")

            # 4. Load entity errors from CSV and prepare for bulk update
            errors = []
            with open(error_info, "r") as f:
                reader = csv.reader(f)
                next(reader)  # Skip header row

                for row in reader:
                    entity_type, entity_text, error_id = row

                    # Type checking and validation
                    if not isinstance(entity_text, str):
                        self.logger.warning(f"Invalid entity_text type: {type(entity_text)}. Skipping.")
                        continue

                    # Transform to lowercase early
                    entity_text = entity_text.lower()

                    if not isinstance(entity_type, str) or len(entity_type) > 20:
                        self.logger.warning(f"Invalid entity_type: {entity_type}. Must be string <= 20 chars. Skipping.")
                        continue

                    if not isinstance(error_id, str) or len(error_id) > 10:
                        self.logger.warning(f"Invalid error_id: {error_id}. Must be string <= 10 chars. Skipping.")
                        continue

                    # Directly convert entity_type to ID
                    entity_id = self.data.get_named_entity_id(entity_type)
                    if not entity_id:
                        self.logger.warning(f"Misslabeled NER: No entity ID found for type: {entity_type} (text for debug: {entity_text}")
                        continue

                    errors.append(
                        (error_id, entity_text, entity_id)
                    )

            # Proceed if we have any updates
            if errors:
                try:
                    # Transform data to match SQL parameter order
                    update_params = [
                        (
                            error_id,        # SET ERROR_ID = ?
                            class_id,        # WHERE CLASS_ID = ?
                            error_id,        # AND ERROR_ID != ?
                            entity_text      # AND LOWER(TXT) = ?
                        )
                        for error_id, entity_text, class_id in errors
                    ]
                    self.logger.info(f"processing {len(errors)} entity errors code updates...")
                    self.logger.info(f"First 5 entity updates: {errors[:5]}") # Print the first 5 updates

                    # Create index for lower(entity_text) and entity_id for faster updates
                    ind = Index(TABLE_NE, [CLASS_ID, ERROR_ID, TXT], logger=self.logger)
                    ind.create_if_not_exists(self.cursor)
                    ind.analyze(self.cursor)

                    # Around 3 min runtime for 50 000 000 records
                    # Might be optimized

                    stmt_set_error_ids = f"""--sql
                    UPDATE {TABLE_NE}
                    SET {ERROR_ID} = ?
                    WHERE {CLASS_ID} = ?
                    AND ({ERROR_ID} IS NULL OR {ERROR_ID} != ?)
                    AND LOWER({TXT}) = ? -- Case-insensitive match
                    """

                    self.log_query_plan(stmt_set_error_ids, update_params)  # Log query plan for first 5 updates
                    total_affected = self._batch_update_error_codes(stmt_set_error_ids, update_params, batch_size)
                    self.logger.info(f"Updated {total_affected} entity occurrences (from {len(errors)} input records)")

                    # Create index if not exists - Provides speedup for error_id queries where it's used to filter out NULL values
                    # Create index for error_id filtering
                    IDX_NE_ERROR_ID_NOT_NULL.create_if_not_exists(self.cursor)
                    IDX_NE_ERROR_ID_NOT_NULL.analyze(self.cursor)
                    self.conn.commit()

                    self.logger.info(f"Updated {len(errors)} entity occurrences")

                except KeyboardInterrupt:
                    self.logger.warning("User interrupted. Rolling back changes.")
                    self.conn.rollback()
                    return

        except FileNotFoundError as e:
            self.logger.error(f"Error: File not found: {e.filename}")
            raise
        except Exception as e:
            self.logger.error(f"Error attaching error information: {e}")
            self.conn.rollback()
            raise

    def _batch_update_error_codes(self, stmt: str, errors: list, batch_size: int) -> int:
        """
        Helper method to handle batched updates with proper progress tracking

        """
        total_affected = 0
        total_batches = len(errors)

        try:
            self.cursor.execute("BEGIN TRANSACTION")

            with tqdm(total=total_batches, desc=f"Updating {ERROR_ID} for {TABLE_NE}") as pbar:
                for i in range(0, total_batches, batch_size):
                    try:
                        batch = errors[i:i + batch_size]

                        # Execute batch
                        self.cursor.executemany(stmt, batch)
                        affected = self.cursor.rowcount
                        total_affected += affected

                        # Update progress with actual affected rows
                        pbar.update(len(batch))
                        pbar.set_postfix({'affected': total_affected}, refresh=True)

                        # Periodic commit to avoid memory issues
                        if i > 0 and i % (batch_size * 10) == 0:
                            self.conn.commit()
                            self.cursor.execute("BEGIN TRANSACTION")
                    except KeyboardInterrupt:
                        self.conn.rollback()
                        self.logger.warning("User interrupted. Rolling back changes.")
                        raise   # Re-raise the exception to be handled by the caller

            self.conn.commit()
            return total_affected

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error during batch update: {str(e)}")
            raise
        except KeyboardInterrupt:
            self.conn.rollback()
            self.logger.warning("User interrupted. Rolling back changes.")
            raise

    def identity_overlap(self):
        """
        Identify overlapping entities in the NE table and set the OVERLAP flag.
        Idempotent operation, will not overwrite existing values in OVERLAP.
        Based on the following rules:
        - If two entities have the same DOC_ID and SENT_IDX and their spans overlap, set OVERLAP = TRUE

        Groups entities by document and sentence index, then checks for overlapping spans within each group. For 30 000 000 records, with avg 10 sentences per document

        Using ReaderWriterPair for multithreaded reading and single-threaded writing
        Must therefore have a order by, limit and offset
        """


        # Create reader query

        reader_query = f"""--sql
            WITH sentence_entities AS (
                SELECT
                    n1.{DOC_ID},
                    n1.{SENT_IDX},
                    n1.{NE_PRIMARY_ID},
                    n1.{SPAN_START},
                    n1.{SPAN_END},
                    GROUP_CONCAT(                -- Concatenate other spans in the group
                        n2.{NE_PRIMARY_ID} || ',' ||      -- Entity ID
                        n2.{SPAN_START} || ',' ||  -- Start position
                        n2.{SPAN_END},            -- End position
                        ';'                        -- Separator between entities
                    ) as other_spans
                FROM {TABLE_NE} n1
                LEFT JOIN {TABLE_NE} n2 ON
                    n1.{DOC_ID} = n2.{DOC_ID} AND
                    n1.{SENT_IDX} = n2.{SENT_IDX} AND
                    n2.{NE_PRIMARY_ID} != n1.{NE_PRIMARY_ID} AND    -- Don't include the entity itself
                    n2.{SPAN_START} <= n1.{SPAN_END} AND              -- Start before the end
                    n2.{SPAN_END} >= n1.{SPAN_START}                 -- This should drastically reduce the number of comparisons

                WHERE n1.{NE_OVERLAP} IS 0    -- Only process unprocessed entities
                GROUP BY
                    n1.{DOC_ID},
                    n1.{SENT_IDX},
                    n1.{NE_PRIMARY_ID},
                    n1.{SPAN_START},
                    n1.{SPAN_END}
                ORDER BY n1.{DOC_ID}, n1.{SENT_IDX}, n1.{SPAN_START}
                LIMIT :limit OFFSET :offset
            )
            SELECT * FROM sentence_entities;

            -- Sample output:
            -- DOC_ID  SENT_IDX  NE_ID  current_start  current_end  other_spans
            -- 1       0         1      0              5           "2,3,8;3,12,15;8,15,20"
            --                                                      ^ ID|start|end ; ID|start|end ; ID|start|end

            """

        # Simplify and optimize query
        reader_query = f"""--sql
            SELECT
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM {TABLE_NE} n2
                    WHERE n1.{DOC_ID} = n2.{DOC_ID}
                    AND n1.{SENT_IDX} = n2.{SENT_IDX}
                    AND n2.{NE_PRIMARY_ID} != n1.{NE_PRIMARY_ID}
                    AND n2.{SPAN_START} <= n1.{SPAN_END}
                    AND n2.{SPAN_END} >= n1.{SPAN_START}
                ) THEN 1 ELSE 0 END as has_overlap,
                n1.{NE_PRIMARY_ID}
            FROM {TABLE_NE} n1
            WHERE n1.{NE_OVERLAP} IS 0
            ORDER BY n1.{DOC_ID}, n1.{SENT_IDX}, n1.{SPAN_START}
            LIMIT :limit OFFSET :offset
"""
        self.log_query_plan(reader_query, params={"limit": 100, "offset": 0})

        def process_function(batch, conn_params):
            """Process a batch of records to detect overlaps"""
            results = []
            print(f"Processing batch of {len(batch)} records")
            for doc_id, sent_idx, ne_id, current_start, current_end, other_spans in batch:
                if not other_spans:
                    continue

                # other_spans format: "ID|start|end"

                # Process spans in groups of 3 (ne_id , start , end)

                # other_spans = "2,3,8;3,12,15;8,15,20"

                # # First split by semicolon:
                spans = other_spans.split(';')
                # entities = ["2,3,8", "3,12,15", "8,15,20"]

                # # For each entity, split by comma:
                # entity1 = [2, 3, 8]    # id=2, start=3, end=8
                # entity2 = [3, 12, 15]  # id=3, start=12, end=15
                # entity3 = [8, 15, 20]  # id=8, start=15, end=20

                for span in spans:
                    other_id, other_start, other_end = map(int, span.split(','))

                    # Single condition for all overlap cases:
                    # If one span starts before the other ends, they overlap
                    # If A overlaps with B, then both:
                    # A starts before B ends (current_start <= other_end)
                    # B starts before A ends (other_start <= current_end)
                    # This single condition catches all cases:

                    # Complete containment in either direction
                    # Partial overlaps at either end
                    # Equal spans
                    if (current_start <= other_end and other_start <= current_end):
                        results.append((1, ne_id))
                        # self.logger.debug(
                        #     f"Overlap found in doc {doc_id}, sentence {sent_idx}: "
                        #     f"Entity {ne_id}({current_start},{current_end}) "
                        #     f"overlaps with {other_id}({other_start},{other_end})"
                        # )
                        break  # One overlap is enoughis enough to mark the entity

            return results

        # Replace current process_function with:

        def process_function(batch, conn_params):
            """Process a batch of records to detect overlaps"""
            # Each row now has just ne_id and has_overlap flag (1 or 0)
            return batch


        def writer_function(results, cursor, conn):
            """Write results back to the database"""
            cursor.executemany(f"UPDATE {TABLE_NE} SET {NE_OVERLAP} = ? WHERE {NE_PRIMARY_ID} = ?", results)


        total_rows = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE} WHERE {NE_OVERLAP} IS 0").fetchone()[0]

        self.logger.info(f"Processing {total_rows} entities for overlaps...")
        # Create reader-writer pair
        rw_pair = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            batch_size=50000,
            max_queue_size=10,
            num_reader_threads=4,
            reader_query=reader_query,
            process_function=process_function,
            write_function=writer_function,
            total_rows=total_rows
        )

        rw_pair.run()

        def populate_normalized_txt_column(self):
            """
            Populate the TXT_NORM column in the NE table with normalized entity text.
            Uses ReaderWriterPair for efficient multithreaded processing.

            - Filters out entities with error codes
            - Filters out entities that overlap with other entities (OVERLAP = TRUE)
            - Only processes entities where TXT_NORM is NULL
            """
            self.logger.info("Populating normalized text column (TXT_NORM)...")

            # Create index for faster querying
            ind = Index(TABLE_NE, [NE_OVERLAP, ERROR_ID, TXT_NORM], logger=self.logger)
            ind.create_if_not_exists(self.cursor)

            # Create reader query that selects entities needing normalization
            reader_query = f"""--sql
                SELECT {NE_PRIMARY_ID}, {TXT}
                FROM {TABLE_NE}
                WHERE {TXT_NORM} IS NULL
                AND {ERROR_ID} IS NULL
                AND {NE_OVERLAP} = 0
                ORDER BY {DOC_ID}, {SENT_IDX}
                LIMIT :limit OFFSET :offset
            """

            # Define process function to normalize text
            def process_function(batch, conn_params):
                """Process a batch of entities by normalizing their text"""
                normalized_entities = []
                for ne_id, text in batch:
                    normalized_text = self._normalize_entity_text(text)
                    normalized_entities.append((normalized_text, ne_id))
                return normalized_entities

            # Define writer function to update the database
            def writer_function(results, cursor, conn):
                """Write normalized text values to database"""
                # Set pragmas for better write performance
                cursor.execute("PRAGMA synchronous = OFF")
                cursor.execute("PRAGMA journal_mode = MEMORY")
                cursor.execute("PRAGMA temp_store = MEMORY")

                # Update database with normalized text values
                cursor.executemany(
                    f"UPDATE {TABLE_NE} SET {TXT_NORM} = ? WHERE {NE_PRIMARY_ID} = ?",
                    results
                )

            # Get total rows to process
            total_rows = self.cursor.execute(f"""
                SELECT COUNT(*) FROM {TABLE_NE}
                WHERE {TXT_NORM} IS NULL
                AND {ERROR_ID} IS NULL
                AND {NE_OVERLAP} = 0
            """).fetchone()[0]

            self.logger.info(f"Found {total_rows} entities that need normalization")

            if total_rows == 0:
                self.logger.info("No entities to normalize. Exiting.")
                return

            # Create reader-writer pair for parallel processing
            rw_pair = ReaderWriterPair(
                conn_params=self.conn_params_dict,
                batch_size=50000,
                max_queue_size=10,
                num_reader_threads=8,  # Use more threads for better performance on text processing
                writer_batch_chunking=5,  # Accumulate 5 batches before writing
                reader_query=reader_query,
                process_function=process_function,
                write_function=writer_function,
                total_rows=total_rows,
                process_title="Normalizing entity text"
            )

            # Execute the normalization
            rw_pair.run()

            self.logger.info(f"Normalization complete. Processed {total_rows} entities.")

            # Create index on TXT_NORM for faster lookups
            index_norm = Index(TABLE_NE, [TXT_NORM], logger=self.logger)
            index_norm.create_if_not_exists(self.cursor)
            index_norm.analyze(self.cursor)

            # Analyze the database to optimize query plans
            self.cursor.execute(f"ANALYZE {TABLE_NE}")

        def _normalize_entity_text(self, text):
            """
            Normalize entity text by:
            1. Converting to lowercase
            2. Removing leading/trailing whitespace
            3. Removing special characters
            4. Expanding contractions
            5. Removing stop words (optional)
            6. Handling possessive forms
            7. Removing special leading characters (%, ", -, etc.)

            Args:
                text: The entity text to normalize

            Returns:
                Normalized entity text
            """
            import re
            import string
            from functools import lru_cache

            # Use caching to avoid reprocessing identical texts
            @lru_cache(maxsize=10000)
            def _normalize_text_cached(input_text):
                if not input_text:
                    return ""

                # 1. Convert to lowercase
                normalized = input_text.lower()

                # 2. Remove leading/trailing whitespace
                normalized = normalized.strip()

                # 7. Remove special leading character combinations
                normalized = re.sub(r'^[%"\'`-]+\s*', '', normalized)  # Remove leading %, ", ', `, - with optional spaces
                normalized = re.sub(r'^\s*[%"\'`-]+\s*', '', normalized)  # Remove leading spaces followed by %, ", ', `, - with optional spaces

                # 6. Handle possessive forms (before general punctuation removal)
                # Remove 's at the end of words
                normalized = re.sub(r'\'s\b', '', normalized)  # Remove "'s" at word boundaries
                normalized = re.sub(r's\'\b', 's', normalized)  # Convert "s'" to "s" at word boundaries (plural possessive)

                # 3. Remove special characters and punctuation (keep hyphens)
                punct_translator = str.maketrans('', '', string.punctuation.replace('-', ''))
                normalized = normalized.translate(punct_translator)

                # Replace multiple spaces with single space
                normalized = re.sub(r'\s+', ' ', normalized)

                # 4. Expand common contractions
                contractions = {
                    "ain't": "is not",
                    "aren't": "are not",
                    "can't": "cannot",
                    "couldn't": "could not",
                    "didn't": "did not",
                    "doesn't": "does not",
                    "don't": "do not",
                    "hadn't": "had not",
                    "hasn't": "has not",
                    "haven't": "have not",
                    "he'd": "he would",
                    "he'll": "he will",
                    "he's": "he is",
                    "i'd": "i would",
                    "i'll": "i will",
                    "i'm": "i am",
                    "i've": "i have",
                    "isn't": "is not",
                    "it's": "it is",
                    "let's": "let us",
                    "mustn't": "must not",
                    "shan't": "shall not",
                    "she'd": "she would",
                    "she'll": "she will",
                    "she's": "she is",
                    "shouldn't": "should not",
                    "that's": "that is",
                    "there's": "there is",
                    "they'd": "they would",
                    "they'll": "they will",
                    "they're": "they are",
                    "they've": "they have",
                    "we'd": "we would",
                    "we'll": "we will",
                    "we're": "we are",
                    "we've": "we have",
                    "weren't": "were not",
                    "what'll": "what will",
                    "what're": "what are",
                    "what's": "what is",
                    "what've": "what have",
                    "where's": "where is",
                    "who'd": "who would",
                    "who'll": "who will",
                    "who's": "who is",
                    "who've": "who have",
                    "won't": "will not",
                    "wouldn't": "would not",
                    "you'd": "you would",
                    "you'll": "you will",
                    "you're": "you are",
                    "you've": "you have",
                }

                # Word boundary to ensure we match whole words only
                for contraction, expansion in contractions.items():
                    normalized = re.sub(r'\b' + contraction + r'\b', expansion, normalized)

                # Handle specific domain entities or edge cases
                # Standardize COVID-19
                normalized = re.sub(r'covid[-\s]?19', 'covid19', normalized)

                # Final cleanup: Remove any remaining non-alphanumeric except spaces and hyphens
                normalized = re.sub(r'[^\w\s-]', '', normalized)

                # Remove redundant hyphens
                normalized = re.sub(r'-+', '-', normalized)  # Multiple hyphens to single hyphen
                normalized = re.sub(r'(^-|-$)', '', normalized)  # Remove leading/trailing hyphens

                # Final whitespace normalization
                normalized = re.sub(r'\s+', ' ', normalized).strip()

                return normalized

            # Call the cached function
            return _normalize_text_cached(text)

        def populate_normalized_txt_column(self):
            """
            Populate the TXT_NORM column in the NE table with normalized entity text.
            Uses ReaderWriterPair for efficient multithreaded processing.

            - Filters out entities with error codes
            - Filters out entities that overlap with other entities (OVERLAP = TRUE)
            - Only processes entities where TXT_NORM is NULL
            """
            self.logger.info("Starting text normalization process...")

            # Create index for faster querying
            self.logger.info("Creating supporting indexes...")

            # Index for the query filtering
            ind1 = Index(TABLE_NE, [TXT_NORM, ERROR_ID, NE_OVERLAP], logger=self.logger)
            ind1.create_if_not_exists(self.cursor)

            # Index for the ordering in reader query (important for offset/limit)
            ind2 = Index(TABLE_NE, [DOC_ID, SENT_IDX], logger=self.logger)
            ind2.create_if_not_exists(self.cursor)

            # Create reader query that selects entities needing normalization
            reader_query = f"""--sql
                SELECT {NE_PRIMARY_ID}, {TXT}
                FROM {TABLE_NE}
                WHERE {TXT_NORM} IS NULL
                AND {ERROR_ID} IS NULL
                AND {NE_OVERLAP} = 0
                ORDER BY {DOC_ID}, {SENT_IDX}
                LIMIT :limit OFFSET :offset
            """

            # Log query plan
            self.log_query_plan(reader_query, {"limit": 1000, "offset": 0})

            # Define process function that runs in reader threads with thread-local optimizations
            def process_function(batch, conn_params):
                """Process a batch of entities by normalizing their text"""
                import re
                import string
                from functools import lru_cache

                # Pre-compile regex patterns for performance
                leading_chars_pattern = re.compile(r'^[%"\'`-]+\s*')
                leading_space_chars_pattern = re.compile(r'^\s*[%"\'`-]+\s*')
                possessive_s_pattern = re.compile(r'\'s\b')
                plural_possessive_pattern = re.compile(r's\'\b')
                whitespace_pattern = re.compile(r'\s+')
                covid_pattern = re.compile(r'covid[-\s]?19')
                non_alnum_pattern = re.compile(r'[^\w\s-]')
                redundant_hyphen_pattern = re.compile(r'-+')
                edge_hyphen_pattern = re.compile(r'(^-|-$)')

                # Create translation table once per thread
                punct_translator = str.maketrans('', '', string.punctuation.replace('-', ''))

                # Pre-compile contraction regex patterns
                contractions = {
                    "ain't": "is not", "aren't": "are not", "can't": "cannot",
                    "couldn't": "could not", "didn't": "did not", "doesn't": "does not",
                    "don't": "do not", "hadn't": "had not", "hasn't": "has not",
                    "haven't": "have not", "he'd": "he would", "he'll": "he will",
                    "he's": "he is", "i'd": "i would", "i'll": "i will",
                    "i'm": "i am", "i've": "i have", "isn't": "is not",
                    "it's": "it is", "let's": "let us", "mustn't": "must not",
                    "shan't": "shall not", "she'd": "she would", "she'll": "she will",
                    "she's": "she is", "shouldn't": "should not", "that's": "that is",
                    "there's": "there is", "they'd": "they would", "they'll": "they will",
                    "they're": "they are", "they've": "they have", "we'd": "we would",
                    "we'll": "we will", "we're": "we are", "we've": "we have",
                    "weren't": "were not", "what'll": "what will", "what're": "what are",
                    "what's": "what is", "what've": "what have", "where's": "where is",
                    "who'd": "who would", "who'll": "who will", "who's": "who is",
                    "who've": "who have", "won't": "will not", "wouldn't": "would not",
                    "you'd": "you would", "you'll": "you will", "you're": "you are",
                    "you've": "you have"
                }
                contraction_patterns = [(re.compile(r'\b' + re.escape(c) + r'\b'), e)
                                    for c, e in contractions.items()]

                # Create result buffer
                normalized_entities = []

                # Thread-local LRU cache for frequently seen identical strings
                @lru_cache(maxsize=5000)
                def normalize_text_cached(text):
                    if not text:
                        return ""

                    # 1. Convert to lowercase & trim
                    normalized = text.lower().strip()

                    # 2. Remove special leading character combinations
                    normalized = leading_chars_pattern.sub('', normalized)
                    normalized = leading_space_chars_pattern.sub('', normalized)

                    # 3. Handle possessive forms
                    normalized = possessive_s_pattern.sub('', normalized)
                    normalized = plural_possessive_pattern.sub('s', normalized)

                    # 4. Remove punctuation except hyphens
                    normalized = normalized.translate(punct_translator)

                    # 5. Normalize whitespace (initial pass)
                    normalized = whitespace_pattern.sub(' ', normalized)

                    # 6. Expand contractions
                    for pattern, replacement in contraction_patterns:
                        normalized = pattern.sub(replacement, normalized)

                    # 7. Domain-specific normalizations
                    normalized = covid_pattern.sub('covid19', normalized)

                    # 8. Final cleanup
                    normalized = non_alnum_pattern.sub('', normalized)
                    normalized = redundant_hyphen_pattern.sub('-', normalized)
                    normalized = edge_hyphen_pattern.sub('', normalized)

                    # 9. Final whitespace normalization
                    normalized = whitespace_pattern.sub(' ', normalized).strip()

                    return normalized

                # Process each entity in batch
                for ne_id, text in batch:
                    normalized_text = normalize_text_cached(text)
                    normalized_entities.append((normalized_text, ne_id))

                return normalized_entities

            # Define writer function to update the database
            def writer_function(results, cursor, conn):
                """Write normalized text values to database with optimized settings"""
                # Set pragmas for better write performance
                cursor.execute("PRAGMA synchronous = OFF")
                cursor.execute("PRAGMA journal_mode = MEMORY")
                cursor.execute("PRAGMA temp_store = MEMORY")
                cursor.execute("PRAGMA cache_size = 100000")  # ~100MB cache

                # Update database with normalized text values
                cursor.executemany(
                    f"UPDATE {TABLE_NE} SET {TXT_NORM} = ? WHERE {NE_PRIMARY_ID} = ?",
                    results
                )

            # Get total rows to process
            total_rows = self.cursor.execute(f"""
                SELECT COUNT(*) FROM {TABLE_NE}
                WHERE {TXT_NORM} IS NULL
                AND {ERROR_ID} IS NULL
                AND {NE_OVERLAP} = 0
            """).fetchone()[0]

            self.logger.info(f"Found {total_rows} entities that need normalization")

            if total_rows == 0:
                self.logger.info("No entities to normalize. Exiting.")
                return

            # Create reader-writer pair for parallel processing with optimized parameters
            rw_pair = ReaderWriterPair(
                conn_params=self.conn_params_dict,
                batch_size=50000,           # 50K entities per batch
                max_queue_size=16,          # Allow up to 16 batches in queue (800K entities)
                num_reader_threads=8,       # 8 parallel reader threads for text processing
                writer_batch_chunking=10,   # Write 10 batches (500K entities) in one transaction
                reader_query=reader_query,
                process_function=process_function,
                write_function=writer_function,
                total_rows=total_rows,
                process_title="Normalizing entity text"
            )

            # Execute the normalization process
            self.logger.info("Starting multi-threaded text normalization...")
            rw_pair.run()

            self.logger.info(f"Text normalization complete. Processed {total_rows} entities.")

            # Create final index on normalized text for faster lookups
            self.logger.info("Creating index on normalized text column...")
            index_norm = Index(TABLE_NE, [TXT_NORM], logger=self.logger)
            index_norm.create_if_not_exists(self.cursor)
            index_norm.analyze(self.cursor)

            # Analyze the table to optimize future query plans
            self.cursor.execute(f"ANALYZE {TABLE_NE}")
            self.logger.info("Normalization process complete.")

class Analysis(BaseComponent):
    def __init__(self, parent):
        super().init_deps(parent)
class Statistics(BaseComponent):
    def __init__(self, parent):
        super().init_deps(parent)
            return None

class Tests(BaseComponent):
    def __init__(self, parent, stats: Statistics):
        super().init_deps(parent)
        self.stats: Statistics = stats

class EntityOccurrence():
    """
    Main entrypoint class for Entity Occurrence functionality.
    Handles integration of schema management, analysis, statistics, and testing.
    """
    def __init__(self, db_system_instance):
        self.db_system = db_system_instance
        self.data_exchanger = db_system_instance.data_exchanger
        self.schema_manager = SchemaManager(self)
        self.preprocessor = Preprocessor(self)
        self.analysis = Analysis(self)
        self.statistics = Statistics(self)
        self.tests = Tests(self, self.statistics)

    @property
    def conn(self):
        return self.db_system.conn

    @property
    def logger(self):
        return self.db_system.logger

    @property
    def cursor(self):
        return self.db_system.cursor

    @property
    def conn_params_dict(self):
        return self.db_system.conn_params_dict

    def setup(self):
        """Initialize the entity occurrence system by setting up tables"""
        self.schema_manager.setup_tables()

    def record_occurrences(self, *args, **kwargs):
        """Delegate to analysis component"""
        if self.analysis.record_entity_occurrences(*args, **kwargs):
            self.tests.test_entity_occurrences()

