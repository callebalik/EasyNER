# entity_occurrence_module.py
import csv
import sqlite3

from tqdm import tqdm
from .schema import *
import logging
from ..db_data_exchanger import DBDataExchanger
from ..db_main import EasyNerDBHandler
from pathlib import Path

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

