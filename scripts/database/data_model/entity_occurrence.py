# entity_cooccurrence_module.py
import sqlite3

from .schema import *
import logging
from ..db_data_exchanger import DBDataExchanger
from ..db_main import EasyNerDBHandler
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

        base_executor.execute_operation(lambda conn: operation(conn, level),)


    def count_entity_cooccurrences_multithreaded(self, level: str = "document", batch_size=5000, num_reader_threads=32) -> None:
        """
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

