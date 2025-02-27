# entity_occurrence_module.py
import csv
import json
import sqlite3
import time

from tqdm import tqdm
from .schema import *
import logging
from ..db_data_exchanger import DBDataExchanger
from ..db_main import EasyNerDBHandler, BaseComponent
from pathlib import Path
from ..core.db_engine import ReaderWriterPair

class SchemaManager(BaseComponent):
    def setup_tables(self):
        """Create tables for entity occurrences if they don't exist"""
        self.logger.info("Setting up tables for entity occurrence analysis...")

        self.cursor.execute(schema_create_table_docs)
        self.cursor.execute(schema_create_table_sentences)
        self.cursor.execute(schema_create_table_ne_lookup)
        self.cursor.execute(schema_create_table_ne_aggregated)
        self.cursor.execute(ne_class_schema)
        self.cursor.execute(ne_error_schema)
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

    def migrate_docs_and_sentences_tables(self):
        """
        Migrate documents and sentences tables to new schemas while preserving data.
        Similar to migrate_and_setup_ne_aggregation, this method:

        1. Creates backups of existing tables
        2. Recreates tables with new schemas
        3. Restores data from backups
        4. Validates migration results
        5. Creates appropriate indexes

        Returns:
            bool: True if migration was successful, False otherwise
        """
        self.logger.info("Starting documents and sentences schema migration...")
        start_time = time.time()

        # Check if tables exist
        docs_exists = self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE_DOCS,)
        ).fetchone() is not None

        sentences_exists = self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE_SENTENCES,)
        ).fetchone() is not None

        if not docs_exists and not sentences_exists:
            self.logger.info("Tables don't exist yet. Creating with new schema.")
            self.cursor.execute(schema_create_table_docs)
            self.cursor.execute(schema_create_table_sentences)
            self.conn.commit()
            self.logger.info("Created tables with new schema successfully.")
            return True

        # First migrate documents since sentences depend on them
        docs_success = self._migrate_doc(docs_exists)
        if not docs_success:
            self.logger.error("Document migration failed. Cannot proceed with sentences migration.")
            return False

        # Then migrate sentences
        sent_success = self._migrate_sentences(sentences_exists)

        # Validate the migration whether it succeeded or not
        self._validate_docs_sentences_migration()

        elapsed = time.time() - start_time
        if docs_success and sent_success:
            self.logger.info(f"Documents and sentences migration completed successfully in {elapsed:.2f} seconds")
            return True
        else:
            self.logger.error(f"Documents and sentences migration completed with errors in {elapsed:.2f} seconds")
            return False

    def _migrate_doc(self, docs_exists):
        """
        Migrate documents table to new schema while preserving data.

        Args:
            docs_exists (bool): Whether the documents table exists

        Returns:
            bool: True if migration was successful, False otherwise
        """
        try:
            if not docs_exists:
                # Create new table if it doesn't exist
                self.cursor.execute(schema_create_table_docs)
                self.logger.info(f"Created new {TABLE_DOCS} table")
                self.conn.commit()
                return True

            # Start transaction for atomicity
            try:
                self.cursor.execute("BEGIN TRANSACTION")
                transaction_started = True
            except sqlite3.OperationalError:
                # Transaction already started
                transaction_started = False

            # Get column info for old table
            old_docs_info = self.cursor.execute(f"PRAGMA table_info({TABLE_DOCS})").fetchall()
            old_docs_columns = [col[1] for col in old_docs_info]
            self.logger.debug(f"Old documents table columns: {old_docs_columns}")

            # Count existing documents
            docs_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_DOCS}").fetchone()[0]

            if docs_count > 0:
                self.logger.info(f"{TABLE_DOCS} table contains {docs_count:,} rows, creating backup...")

                # Create backup
                self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_DOCS}_backup AS SELECT * FROM {TABLE_DOCS}")
                backup_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_DOCS}_backup").fetchone()[0]
                self.logger.info(f"Backed up {backup_count:,} rows from {TABLE_DOCS}")

                # Validate backup before proceeding
                if backup_count != docs_count:
                    self.logger.error(f"Backup validation failed: expected {docs_count} rows, got {backup_count}")
                    raise ValueError("Backup validation failed - aborting migration")

                # Create explicit mapping between old and new schema
                column_map = {
                    "id": DOC_ID,
                    "title": TITLE,
                    "word_count": WORD_COUNT,
                    "token_count": TOKEN_COUNT,  # Keep token_count as TOKEN_COUNT
                    "alpha_count": ALPHA_COUNT
                    # SENT_COUNT will be computed later
                }

                # Drop and recreate table
                self.cursor.execute(f"DROP TABLE IF EXISTS {TABLE_DOCS}")
                self.cursor.execute(schema_create_table_docs)
                self.logger.info(f"Recreated {TABLE_DOCS} table with new schema")

                # Get column info for new table to validate
                new_docs_columns = [col[1] for col in
                                   self.cursor.execute(f"PRAGMA table_info({TABLE_DOCS})").fetchall()]
                self.logger.debug(f"New documents table columns: {new_docs_columns}")

                # Build source and destination columns for INSERT
                src_cols = []
                dest_cols = []

                for old_col in old_docs_columns:
                    if old_col in column_map and column_map[old_col] in new_docs_columns:
                        src_cols.append(old_col)
                        dest_cols.append(column_map[old_col])

                # Validate that we have columns to map
                if not src_cols or not dest_cols:
                    self.logger.error("No common columns found between old and new schemas")
                    raise ValueError("Migration failed - no common columns to migrate")

                # Log the mapping for debugging
                self.logger.debug(f"Source columns: {src_cols}")
                self.logger.debug(f"Destination columns: {dest_cols}")

                # Check if sentences table exists to compute SENT_COUNT
                sentences_exists = self.cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (TABLE_SENTENCES,)
                ).fetchone() is not None

                if sentences_exists:
                    self.logger.info("Computing sentence counts for documents...")
                    # Add SENT_COUNT from sentences table if possible
                    try:
                        self.cursor.execute(f"""--sql
                            CREATE TEMPORARY TABLE doc_sentence_counts AS
                            SELECT document_id, COUNT(*) as sentence_count
                            FROM sentences
                            GROUP BY document_id
                        """)

                        # Format column lists for SQL
                        src_cols_str = ", ".join([f"b.{col}" for col in src_cols])
                        dest_cols_str = ", ".join(dest_cols)

                        # SQL to include sentence counts
                        insert_stmt = f"""--sql
                            INSERT INTO {TABLE_DOCS} ({dest_cols_str}, {SENT_COUNT})
                            SELECT {src_cols_str}, COALESCE(sc.sentence_count, 0)
                            FROM {TABLE_DOCS}_backup b
                            LEFT JOIN doc_sentence_counts sc ON b.id = sc.document_id
                        """
                        self.logger.debug(f"Executing SQL with sentence counts: {insert_stmt}")
                        self.cursor.execute(insert_stmt)
                    except sqlite3.Error as e:
                        self.logger.warning(f"Error computing sentence counts: {e}")
                        # Fall back to basic insert with default 0 for SENT_COUNT
                        insert_stmt = f"""--sql
                            INSERT INTO {TABLE_DOCS} ({dest_cols_str}, {SENT_COUNT})
                            SELECT {src_cols_str}, 0
                            FROM {TABLE_DOCS}_backup b
                        """
                        self.logger.debug(f"Executing fallback SQL with default sentence counts: {insert_stmt}")
                        self.cursor.execute(insert_stmt)
                else:
                    # Format column lists for SQL
                    src_cols_str = ", ".join([f"b.{col}" for col in src_cols])
                    dest_cols_str = ", ".join(dest_cols)

                    # SQL with default 0 for SENT_COUNT
                    insert_stmt = f"""--sql
                        INSERT INTO {TABLE_DOCS} ({dest_cols_str}, {SENT_COUNT})
                        SELECT {src_cols_str}, 0
                        FROM {TABLE_DOCS}_backup b
                    """
                    self.logger.debug(f"Executing SQL with default sentence counts: {insert_stmt}")
                    self.cursor.execute(insert_stmt)

                restored_count = self.cursor.rowcount
                self.logger.info(f"Restored {restored_count:,} rows to {TABLE_DOCS}")

                # Create indexes
                # self.logger.info("Creating supporting indexes for documents...")
                # self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_DOCS}_id ON {TABLE_DOCS} ({DOC_ID})")

                # Verify restoration
                if restored_count != backup_count:
                    self.logger.warning(f"Row count mismatch: {backup_count} in backup, {restored_count} restored")
            else:
                # Just recreate empty table with new schema
                self.cursor.execute(f"DROP TABLE IF EXISTS {TABLE_DOCS}")
                self.cursor.execute(schema_create_table_docs)
                self.logger.info(f"Recreated empty {TABLE_DOCS} table with new schema")

            # Commit transaction
            if transaction_started:
                self.conn.commit()

            return True

        except Exception as e:
            if 'transaction_started' in locals() and transaction_started:
                self.conn.rollback()
            self.logger.error(f"Error during documents table migration: {e}")
            return False

    def _migrate_sentences(self, sentences_exists: bool):
        """
        Migrate sentences table to new schema while preserving data.

        Args:
            sentences_exists (bool): Whether the sentences table exists

        Returns:
            bool: True if migration was successful, False otherwise
        """
        try:
            if not sentences_exists:
                # Create new table if it doesn't exist
                self.cursor.execute(schema_create_table_sentences)
                self.logger.info(f"Created new {TABLE_SENTENCES} table")
                self.conn.commit()
                return True

            # Start transaction for atomicity
            try:
                self.cursor.execute("BEGIN TRANSACTION")
                transaction_started = True
            except sqlite3.OperationalError:
                # Transaction already started
                transaction_started = False

            # Get column info for old table
            old_sent_info = self.cursor.execute(f"PRAGMA table_info({TABLE_SENTENCES})").fetchall()
            old_sent_columns = [col[1] for col in old_sent_info]
            self.logger.debug(f"Old sentences table columns: {old_sent_columns}")

            # Count existing sentences
            sent_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_SENTENCES}").fetchone()[0]

            if sent_count > 0:
                self.logger.info(f"{TABLE_SENTENCES} table contains {sent_count:,} rows, creating backup...")

                # Create backup
                self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_SENTENCES}_backup AS SELECT * FROM {TABLE_SENTENCES}")
                backup_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_SENTENCES}_backup").fetchone()[0]
                self.logger.info(f"Backed up {backup_count:,} rows from {TABLE_SENTENCES}")

                # Validate backup before proceeding
                if backup_count != sent_count:
                    self.logger.error(f"Backup validation failed: expected {sent_count} rows, got {backup_count}")
                    raise ValueError("Backup validation failed - aborting migration")

                # Create explicit mapping between old and new schema
                column_map = {
                    "text": TXT,
                    "sentence_index": SENT_IDX,
                    "document_id": DOC_ID,
                    "word_count": WORD_COUNT,
                    "token_count": TOKEN_COUNT,  # Keep token_count as TOKEN_COUNT
                    "alpha_count": ALPHA_COUNT
                }

                # Drop and recreate table with new schema
                self.cursor.execute(f"DROP TABLE IF EXISTS {TABLE_SENTENCES}")
                self.cursor.execute(schema_create_table_sentences)
                self.logger.info(f"Recreated {TABLE_SENTENCES} table with new schema")

                # Get column info for new table to validate
                new_sent_columns = [col[1] for col in
                                   self.cursor.execute(f"PRAGMA table_info({TABLE_SENTENCES})").fetchall()]
                self.logger.debug(f"New sentences table columns: {new_sent_columns}")

                # Build source and destination columns for INSERT
                src_cols = []
                dest_cols = []

                for old_col in old_sent_columns:
                    if old_col in column_map and column_map[old_col] in new_sent_columns:
                        src_cols.append(old_col)
                        dest_cols.append(column_map[old_col])

                # Validate that we have columns to map
                if not src_cols or not dest_cols:
                    self.logger.error("No common columns found between old and new sentence schemas")
                    raise ValueError("Migration failed - no common columns to migrate")

                # Log the mapping for debugging
                self.logger.debug(f"Source columns: {src_cols}")
                self.logger.debug(f"Destination columns: {dest_cols}")

                # Check for orphaned sentences before migration
                orphaned_count = self.cursor.execute(f"""--sql
                    SELECT COUNT(*) FROM {TABLE_SENTENCES}_backup s
                    LEFT JOIN {TABLE_DOCS} d ON s.document_id = d.{DOC_ID}
                    WHERE d.{DOC_ID} IS NULL
                """).fetchone()[0]

                # Format column lists for SQL
                src_cols_str = ", ".join([f"s.{col}" for col in src_cols])
                dest_cols_str = ", ".join(dest_cols)

                if orphaned_count > 0:
                    self.logger.warning(f"Found {orphaned_count} orphaned sentences with invalid document references")
                    self.logger.info("Will restore only sentences with valid document references")

                    insert_sql = f"""--sql
                        INSERT INTO {TABLE_SENTENCES} ({dest_cols_str})
                        SELECT {src_cols_str}
                        FROM {TABLE_SENTENCES}_backup s
                        WHERE EXISTS (SELECT 1 FROM {TABLE_DOCS} d WHERE d.{DOC_ID} = s.document_id)
                    """
                else:
                    # Standard insert if no orphaned records
                    insert_sql = f"""--sql
                        INSERT INTO {TABLE_SENTENCES} ({dest_cols_str})
                        SELECT {src_cols_str}
                        FROM {TABLE_SENTENCES}_backup s
                    """

                self.logger.debug(f"Executing SQL: {insert_sql}")
                self.cursor.execute(insert_sql)

                restored_count = self.cursor.rowcount
                self.logger.info(f"Restored {restored_count:,} rows to {TABLE_SENTENCES}")

                # # Create indexes
                # self.logger.info("Creating supporting indexes for sentences...")
                # self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_SENTENCES}_doc_id ON {TABLE_SENTENCES} ({DOC_ID})")

                # Verify restoration
                expected_count = backup_count - orphaned_count
                if restored_count != expected_count:
                    self.logger.warning(f"Row count mismatch: expected {expected_count}, got {restored_count} restored")
            else:
                # Just recreate empty table with new schema
                self.cursor.execute(f"DROP TABLE IF EXISTS {TABLE_SENTENCES}")
                self.cursor.execute(schema_create_table_sentences)
                self.logger.info(f"Recreated empty {TABLE_SENTENCES} table with new schema")

            # Update document sentence counts if needed
            # try:
            #     self.logger.info("Updating document sentence counts...")
            #     self.cursor.execute(f"""--sql
            #         UPDATE {TABLE_DOCS} SET {SENT_COUNT} = (
            #             SELECT COUNT(*)
            #             FROM {TABLE_SENTENCES} s
            #             WHERE s.{DOC_ID} = {TABLE_DOCS}.{DOC_ID}
            #         )
            #     """)
            #     self.logger.info(f"Updated sentence counts for {self.cursor.rowcount} documents")
            # except sqlite3.Error as e:
            #     self.logger.warning(f"Error updating document sentence counts: {e}")

            # Commit transaction
            if transaction_started:
                self.conn.commit()

            return True

        except Exception as e:
            if 'transaction_started' in locals() and transaction_started:
                self.conn.rollback()
            self.logger.error(f"Error during sentences table migration: {e}")
            return False

    def _validate_docs_sentences_migration(self):
        """
        Validate the documents and sentences migration results.

        Checks:
        1. Row count comparison between original and migrated tables
        2. Foreign key integrity between documents and sentences
        3. Sample data verification including TOKEN_COUNT preservation
        4. Index verification

        Returns:
            dict: Validation results with detailed metrics
        """
        self.logger.info("Validating documents and sentences migration...")
        validation = {
            "success": True,
            "documents": {
                "backup_exists": False,
                "count_match": False,
            },
            "sentences": {
                "backup_exists": False,
                "count_match": False,
                "orphaned": 0,
            },
            "integrity": {
                "valid": True,
                "invalid_references": 0
            }
        }

        try:
            # Check if backup tables exist
            docs_backup_exists = self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (f"{TABLE_DOCS}_backup",)
            ).fetchone() is not None

            sent_backup_exists = self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (f"{TABLE_SENTENCES}_backup",)
            ).fetchone() is not None

            validation["documents"]["backup_exists"] = docs_backup_exists
            validation["sentences"]["backup_exists"] = sent_backup_exists

            # Verify schema matches expected schema from schema.py
            doc_columns = {col[1] for col in
                          self.cursor.execute(f"PRAGMA table_info({TABLE_DOCS})").fetchall()}
            sent_columns = {col[1] for col in
                           self.cursor.execute(f"PRAGMA table_info({TABLE_SENTENCES})").fetchall()}

            expected_doc_columns = {DOC_ID, TITLE, WORD_COUNT, TOKEN_COUNT, ALPHA_COUNT, SENT_COUNT}
            expected_sent_columns = {DOC_ID, SENT_IDX, TXT, WORD_COUNT, TOKEN_COUNT, ALPHA_COUNT}

            missing_doc_cols = expected_doc_columns - doc_columns
            missing_sent_cols = expected_sent_columns - sent_columns

            if missing_doc_cols:
                self.logger.error(f"Documents table missing expected columns: {missing_doc_cols}")
                validation["documents"]["missing_columns"] = list(missing_doc_cols)
                validation["success"] = False

            if missing_sent_cols:
                self.logger.error(f"Sentences table missing expected columns: {missing_sent_cols}")
                validation["sentences"]["missing_columns"] = list(missing_sent_cols)
                validation["success"] = False

            # Validate documents if backup exists
            if docs_backup_exists:
                # Compare row counts
                docs_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_DOCS}").fetchone()[0]
                backup_docs_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_DOCS}_backup").fetchone()[0]

                validation["documents"]["count"] = docs_count
                validation["documents"]["backup_count"] = backup_docs_count
                validation["documents"]["count_match"] = docs_count == backup_docs_count

                if docs_count != backup_docs_count:
                    self.logger.warning(f"Document counts don't match: {backup_docs_count} in backup, {docs_count} in new table")
                    validation["success"] = False
                else:
                    self.logger.info(f"Document counts match: {docs_count} records")

                # Sample validation with emphasis on TOKEN_COUNT preservation
                if docs_count > 0:
                    sample_size = min(10, docs_count)
                    self.logger.info(f"Validating sample of {sample_size} documents...")

                    # Get random sample of IDs
                    sample_ids = self.cursor.execute(f"""--sql
                        SELECT id FROM {TABLE_DOCS}_backup
                        ORDER BY RANDOM() LIMIT {sample_size}
                    """).fetchall()

                    sample_ids = [row[0] for row in sample_ids]
                    mismatches = []

                    for id in sample_ids:
                        # Get original document with key fields
                        orig = self.cursor.execute(f"""--sql
                            SELECT id, title, token_count, word_count
                            FROM {TABLE_DOCS}_backup
                            WHERE id = ?
                        """, (id,)).fetchone()

                        if not orig:
                            continue

                        # Get migrated document
                        migrated = self.cursor.execute(f"""--sql
                            SELECT {DOC_ID}, {TITLE}, {TOKEN_COUNT}, {WORD_COUNT}
                            FROM {TABLE_DOCS}
                            WHERE {DOC_ID} = ?
                        """, (id,)).fetchone()

                        if not migrated:
                            mismatches.append(f"Document ID {id} missing in migrated table")
                        elif orig[1] != migrated[1]:  # Compare titles
                            mismatches.append(f"Document ID {id} title mismatch: '{orig[1]}' vs '{migrated[1]}'")
                        elif orig[2] != migrated[2]:  # Compare token_count
                            mismatches.append(f"Document ID {id} token_count mismatch: {orig[2]} vs {migrated[2]}")
                        elif orig[3] != migrated[3]:  # Compare word_count
                            mismatches.append(f"Document ID {id} word_count mismatch: {orig[3]} vs {migrated[3]}")

                    validation["documents"]["sample_mismatches"] = mismatches

                    if mismatches:
                        self.logger.warning(f"Found {len(mismatches)} document mismatches in sample")
                        validation["success"] = False
                    else:
                        self.logger.info("Document sample validation passed")

            # Validate sentences if backup exists
            if sent_backup_exists:
                # Compare row counts considering orphaned records
                orphaned_count = 0
                if docs_backup_exists:
                    orphaned_count = self.cursor.execute(f"""--sql
                        SELECT COUNT(*) FROM {TABLE_SENTENCES}_backup s
                        LEFT JOIN {TABLE_DOCS} d ON s.document_id = d.{DOC_ID}
                        WHERE d.{DOC_ID} IS NULL
                    """).fetchone()[0]

                sent_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_SENTENCES}").fetchone()[0]
                backup_sent_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_SENTENCES}_backup").fetchone()[0]

                expected_count = backup_sent_count - orphaned_count

                validation["sentences"]["count"] = sent_count
                validation["sentences"]["backup_count"] = backup_sent_count
                validation["sentences"]["orphaned"] = orphaned_count
                validation["sentences"]["expected_count"] = expected_count
                validation["sentences"]["count_match"] = sent_count == expected_count

                if sent_count != expected_count:
                    self.logger.warning(
                        f"Sentence counts don't match: expected {expected_count} "
                        f"(backup: {backup_sent_count} - orphaned: {orphaned_count}), got {sent_count}"
                    )
                    validation["success"] = False
                else:
                    self.logger.info(f"Sentence counts match expected value: {sent_count} records")

                # Sample validation with emphasis on TOKEN_COUNT preservation
                if sent_count > 0:
                    sample_size = min(10, sent_count)
                    self.logger.info(f"Validating sample of {sample_size} sentences...")

                    # Get random sample of sentence keys
                    sample_keys = self.cursor.execute(f"""--sql
                        SELECT {DOC_ID}, {SENT_IDX} FROM {TABLE_SENTENCES}
                        ORDER BY RANDOM() LIMIT {sample_size}
                    """).fetchall()

                    mismatches = []

                    for doc_id, sent_idx in sample_keys:
                        # Get migrated sentence with key fields
                        migrated = self.cursor.execute(f"""--sql
                            SELECT {DOC_ID}, {SENT_IDX}, {TXT}, {TOKEN_COUNT}, {WORD_COUNT}
                            FROM {TABLE_SENTENCES}
                            WHERE {DOC_ID} = ? AND {SENT_IDX} = ?
                        """, (doc_id, sent_idx)).fetchone()

                        # Get original sentence
                        orig = self.cursor.execute(f"""--sql
                            SELECT document_id, sentence_index, text, token_count, word_count
                            FROM {TABLE_SENTENCES}_backup
                            WHERE document_id = ? AND sentence_index = ?
                        """, (doc_id, sent_idx)).fetchone()

                        if not orig:
                            mismatches.append(f"Sentence ({doc_id}, {sent_idx}) not found in original table")
                        elif migrated[2] != orig[2]:  # Compare text
                            # Truncate for log readability
                            orig_text = orig[2][:30] + "..." if len(orig[2]) > 30 else orig[2]
                            mig_text = migrated[2][:30] + "..." if len(migrated[2]) > 30 else migrated[2]
                            mismatches.append(f"Sentence ({doc_id}, {sent_idx}) text mismatch: '{orig_text}' vs '{mig_text}'")
                        elif orig[3] != migrated[3]:  # Compare token_count
                            mismatches.append(f"Sentence ({doc_id}, {sent_idx}) token_count mismatch: {orig[3]} vs {migrated[3]}")
                        elif orig[4] != migrated[4]:  # Compare word_count
                            mismatches.append(f"Sentence ({doc_id}, {sent_idx}) word_count mismatch: {orig[4]} vs {migrated[4]}")

                    validation["sentences"]["sample_mismatches"] = mismatches

                    if mismatches:
                        self.logger.warning(f"Found {len(mismatches)} sentence mismatches in sample")
                        validation["success"] = False
                    else:
                        self.logger.info("Sentence sample validation passed")

            # Check referential integrity
            invalid_refs = self.cursor.execute(f"""--sql
                SELECT COUNT(*) FROM {TABLE_SENTENCES} s
                LEFT JOIN {TABLE_DOCS} d ON s.{DOC_ID} = d.{DOC_ID}
                WHERE d.{DOC_ID} IS NULL
            """).fetchone()[0]

            validation["integrity"]["invalid_references"] = invalid_refs
            if invalid_refs > 0:
                self.logger.error(f"Found {invalid_refs} sentences with invalid document references!")
                validation["integrity"]["valid"] = False
                validation["success"] = False
            else:
                self.logger.info("Referential integrity check passed: all sentences have valid document references")

            # Check indexes
            doc_indexes = self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
                (TABLE_DOCS,)
            ).fetchall()

            sent_indexes = self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
                (TABLE_SENTENCES,)
            ).fetchall()

            doc_index_names = [idx[0].lower() for idx in doc_indexes]
            sent_index_names = [idx[0].lower() for idx in sent_indexes]

            validation["indexes"] = {
                "documents": doc_index_names,
                "sentences": sent_index_names
            }

            # Check for required indexes using case-insensitive comparison
            required_doc_idx = f"idx_{TABLE_DOCS.lower()}_id"
            required_sent_idx1 = f"idx_{TABLE_SENTENCES.lower()}_doc_id"
            required_sent_idx2 = f"idx_{TABLE_SENTENCES.lower()}_doc_sent"

            missing_indexes = []

            if not any(required_doc_idx in idx_name for idx_name in doc_index_names):
                missing_indexes.append(required_doc_idx)

            if not any(required_sent_idx1 in idx_name for idx_name in sent_index_names):
                missing_indexes.append(required_sent_idx1)

            if not any(required_sent_idx2 in idx_name for idx_name in sent_index_names):
                missing_indexes.append(required_sent_idx2)

            if missing_indexes:
                self.logger.warning(f"Missing required indexes: {missing_indexes}")
                validation["indexes"]["missing"] = missing_indexes
                validation["success"] = False
            else:
                self.logger.info("All required indexes are present")

            # Final validation result
            if validation["success"]:
                self.logger.info("Documents and sentences migration validation PASSED")
            else:
                self.logger.error("Documents and sentences migration validation FAILED")

            return validation

        except Exception as e:
            self.logger.error(f"Error during migration validation: {e}")
            validation["success"] = False
            validation["error"] = str(e)
            return validation

class Preprocessor(BaseComponent):
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
                self.cursor.execute(ne_error_schema)
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

        self.logger.info("Identifying overlapping entities...")

        # Create index for faster grouping
        ind = Index(TABLE_NE, [DOC_ID, SENT_IDX], logger=self.logger)
        ind.create_if_not_exists(self.cursor)

        # Create index for faster span checks
        ind = Index(TABLE_NE, [DOC_ID, SENT_IDX, SPAN_START, SPAN_END], logger=self.logger)
        ind.create_if_not_exists(self.cursor)

        # Create index for faster overlap checks
        ind = Index(TABLE_NE, [DOC_ID, SENT_IDX, NE_OVERLAP], logger=self.logger)

        # Create index for faster overlap checks
        ind = Index(TABLE_NE, [DOC_ID, SENT_IDX, SPAN_START, SPAN_END, NE_OVERLAP], logger=self.logger)
        ind.create_if_not_exists(self.cursor)
        # ind.analyze(self.cursor)

        # # Create index for faster overlap checks
        # ind = Index(TABLE_NE, [DOC_ID, SENT_IDX, SPAN_START, SPAN_END, NE_OVERLAP, NE_PRIMARY_ID], logger=self.logger)
        # ind.create_if_not_exists(self.cursor)
        # ind.analyze(self.cursor)

        # # Create index for faster overlap checks
        # ind = Index(TABLE_NE, [DOC_ID, SENT_IDX, SPAN_START, SPAN_END, NE_OVERLAP, CLASS_ID], logger=self.logger)
        # ind.create_if_not_exists(self.cursor)
        # ind.analyze(self.cursor)

        # Allow SQLite to quickly find records where OVERLAP IS 0 (eliminating the table scan)
        # Support the ORDER BY n1.DOC_ID, n1.SENT_IDX, n1.SPAN_START clause
        # Provide efficient access for JOIN conditions

        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_NE_overlap_doc_sent_span ON NE (OVERLAP, DOC_ID, SENT_IDX, SPAN_START)") # O(n) ->  O(log(n))


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
    def record_entity_occurrences(self, batch_size=1000):
        """Record entity occurrences in batches"""
        try:
            self.logger.info("Recording entity occurrences...")
            # Implementation would go here
            return True
        except Exception as e:
            self.logger.error(f"Error recording entity occurrences: {e}")
            return False

class Aggregator(BaseComponent):
    def aggrvegate_named_entities(self, overwrite: bool = False) -> dict:
        """
        Idempotent aggregation of TABLE_NE on distinct (TXT_NORM, CLASS_ID) combinations.

        Populates TABLE_NE_AGGR table with aggregated entity data:
            - TXT_NORM: Normalized entity text
            - CLASS_ID: Entity class ID
            - FQ: Frequency of the entity across corpus
            - UNIQ_DOCS: Number of unique documents containing the entity

        Updates TABLE_NE.NE_NORM_ID with corresponding aggregated entity IDs.

        Args:
        overwrite (bool): If True, overwrites existing aggregations.
                        If False, preserves existing aggregations (default).

        Returns:
            dict: Results of the aggregation process or None if failed
        """
        self.logger.info("Starting named entity aggregation process...")
        start_time = time.time()

        try:
            # Start transaction for atomicity
            self.cursor.execute("BEGIN TRANSACTION")


            # Step 1: Prepare and validate input data
            stats_dir = self._analyze_entities_for_aggregation()
            entity_count = stats_dir['valid_entities']
            if entity_count == 0:
                self.logger.warning("No valid entities found for aggregation. Skipping process.")
                return None

            # Step 2: Perform entity aggregation
            if stats_dir['unique_combinations'] == 0:
                self.logger.info("No unique combinations found. Skipping aggregation.")
                return None
            if stats_dir['unique_combinations'] == stats_dir['existing_aggregations']:
                self.logger.info("All unique combinations are already aggregated. Skipping aggregation.")
            else:
                rows_aggregated = self._perform_entity_aggregation()

            # Step 3: Update entity references
            rows_updated = self._update_ne_norm_id_references_in_batches()

            # Step 4: Create supporting indexes
            self._create_aggregation_indexes()

            # Step 5: Validate results
            validation_result = self._validate_entity_aggregation()

            if validation_result["success"]:
                # Commit all changes
                self.conn.commit()
                elapsed_time = time.time() - start_time
                self.logger.info(f"Entity aggregation completed successfully in {elapsed_time:.2f} seconds")
                return {
                    "entities_processed": entity_count,
                    "unique_aggregations": rows_aggregated,
                    "entities_updated": rows_updated,
                    "validation": validation_result,
                    "processing_time": elapsed_time
                }
            else:
                self.logger.error(f"Entity aggregation validation failed: {validation_result['message']}")
                self.conn.rollback()
                return None

        except sqlite3.Error as e:
            self.conn.rollback()
            error_msg = f"SQLite error during entity aggregation: {e}"
            self.logger.error(error_msg)
            raise sqlite3.Error(error_msg) from e
        except KeyboardInterrupt:
            self.conn.rollback()
            self.logger.warning("User interrupted. Rolling back changes.")
            raise
        except Exception as e:
            self.conn.rollback()
            error_msg = f"Error during entity aggregation: {e}"
            self.logger.error(error_msg)
            raise

    def _analyze_entities_for_aggregation(self):
        """
        Analyzes entity data to determine aggregation scope and creates
        necessary indexes for efficient processing.

        Returns:
            dict: Statistics about the entities to be processed
        """
        import os
        import json
        import time
        import hashlib
        from pathlib import Path

        # Create necessary indexes for efficient aggregation
        # self.logger.info("Creating supporting indexes for aggregation...")
        # index_txt_class = Index(TABLE_NE, [TXT_NORM, CLASS_ID], logger=self.logger)
        # index_txt_class.create_if_not_exists(self.cursor, analyze=True)

        stats_query = f"""--sql
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN {ERROR_ID} IS NULL AND {NE_OVERLAP} = 0 AND {TXT_NORM} IS NOT NULL THEN 1 END) as valid,
                COUNT(CASE WHEN {NE_NORM_ID} IS NOT NULL THEN 1 END) as referenced
            FROM {TABLE_NE}
        """

        # Count unique combinations using GROUP BY (more efficient than string concatenation)
        unique_combos_query = f"""--sql
            SELECT COUNT(*)
            FROM (
                SELECT 1
                FROM {TABLE_NE}
                WHERE {TXT_NORM} IS NOT NULL
                GROUP BY {CLASS_ID}, {TXT_NORM}
            )
        """
        try:
            self.logger.info("Analyzing entity data...")
            stats = self.cursor.execute(stats_query).fetchone()
            unique_combinations = self.cursor.execute(unique_combos_query).fetchone()[0]
            aggr_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE_AGGR}").fetchone()[0] # Get existing aggregation count

            stats_dict = {
                "total_entities": stats[0],
                "valid_entities": stats[1],
                "unique_combinations": unique_combinations,
                "already_referenced": stats[2],
                "existing_aggregations": aggr_count
            }

            # # Save stats to cache
            # try:
            #     with open(os.path.join(cache_dir, 'entity_aggregation_stats.json'), 'w') as f:
            #         json.dump(stats_dict, f)


            self.logger.info(
                f"Found {stats_dict['valid_entities']:,} valid entities out of {stats_dict['total_entities']:,} total "
                f"with {stats_dict['unique_combinations']:,} unique combinations "
                f"({stats_dict['existing_aggregations']:,} existing aggregations)"
            )

            return stats_dict
        except Exception as e:
            self.logger.error(f"Error analyzing entity data: {e}")
            raise
        except KeyboardInterrupt:
            self.logger.warning("User interrupted. Exiting.")
            raise

    def _perform_entity_aggregation(self, overwrite: bool = False):
        """
        Performs entity aggregation by directly inserting into the aggregation table.

        Args:
            overwrite (bool): If True, overwrites existing aggregations.
                            If False, preserves existing aggregations.

        Returns:
            dict: Results of the aggregation operation
        """

        self.logger.info(f"Performing entity aggregation (overwrite={overwrite})...")
        start = time.time()

        # Choose appropriate insert method based on overwrite flag
        insert_method = "INSERT OR REPLACE" if overwrite else "INSERT OR IGNORE"

        # Perform aggregation and insert in a single step
        aggregation_query = f"""
            {insert_method} INTO {TABLE_NE_AGGR} ({CLASS_ID}, {TXT_NORM}, {FQ}, {UNIQ_DOCS})
            SELECT
                {CLASS_ID},
                {TXT_NORM},
                COUNT(*) as {FQ},
                COUNT(DISTINCT {DOC_ID}) as {UNIQ_DOCS}
            FROM {TABLE_NE}
            WHERE {ERROR_ID} IS NULL
            AND {NE_OVERLAP} = 0
            AND {TXT_NORM} IS NOT NULL
            GROUP BY {CLASS_ID}, {TXT_NORM}
        """

        self.cursor.execute(aggregation_query)
        self.conn.commit()

        # Get number of rows in the aggregation table
        rows_aggregated = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE_AGGR}").fetchone()[0]

        duration = time.time() - start
        self.logger.info(
            f"Entity aggregation (overwrite: {overwrite}) completed in {duration:.2f} seconds:"
            )

        return {
            "aggregated_rows": rows_aggregated,
            "processing_time": duration
        }

    def _update_ne_norm_id_references_in_batches(self, overwrite: bool = False) -> dict:
        """
        Updates entity references in batches to handle very large datasets efficiently.

        Args:
            NOT IMPLEMENTED overwrite (bool): If True, updates all references.
            If False, updates only records with no existing reference.

        Returns:
            dict: Results of the reference update operation
        """
        import time
        import os
        import signal
        from contextlib import contextmanager

        batch_size = int(os.environ.get('EASYNER_BATCH_SIZE', '200000'))
        self.logger.info(f"Updating entity references with batch size {batch_size} (overwrite={overwrite})...")

        start_time = time.time()

        total_count_query = f"""--sql
            SELECT COUNT(*)
            FROM {TABLE_NE}
            WHERE {TXT_NORM} IS NOT NULL
        """

        if not overwrite: # Only update entities without a reference
            total_count_query += f" AND {NE_NORM_ID} IS NULL"

        total_to_update = self.cursor.execute(total_count_query).fetchone()[0]

        if total_to_update == 0:
            self.logger.info("No entity references need updating")
            return {"total_updated": 0, "processing_time": time.time() - start_time}

        self.logger.info(f"Found {total_to_update:,} entity references to update")

        # Setup interrupt handling
        interrupted = False
        error_occurred = False
        error_message = None
        checkpoint_file = os.path.join(os.environ.get('EASYNER_CHECKPOINT_DIR', '.'),'ne_reference_checkpoint.json')

        @contextmanager
        def interrupt_handler():
            original_handler = signal.getsignal(signal.SIGINT)

            def handler(signum, frame):
                nonlocal interrupted
                interrupted = True
                self.logger.warning("Interrupt received, completing current batch before stopping...")

            try:
                signal.signal(signal.SIGINT, handler)
                yield
            except Exception as e:
                nonlocal error_occurred, error_message
                error_occurred = True
                error_message = str(e)
                self.logger.error(f"Error during entity reference update: {e}")

                # Create checkpoint for recovery
                try:
                    with open(checkpoint_file, 'w') as f:
                        json.dump({
                            'processed_count': total_updated,
                            'timestamp': time.time(),
                            'error': str(e)
                        }, f)
                except Exception as checkpoint_error:
                    self.logger.error(f"Failed to create checkpoint file: {checkpoint_error}")

                raise  # Re-raise the exception
            finally:
                signal.signal(signal.SIGINT, original_handler)

        # ---- Main update loop ----
        # Loop update until no more entities to update, this should happen at the same time total_to_update == total_updated

        total_updated = 0
        batch_count = 0

        # Get the first batch of IDs that need updating
        where_clause = f"{TXT_NORM} IS NOT NULL"
        if not overwrite:
            where_clause += f" AND {NE_NORM_ID} IS NULL"

        # Use direct update with a limited subquery for better performance
        update_query = f"""--sql
            WITH batch AS (
                SELECT {NE_PRIMARY_ID}
                FROM {TABLE_NE}
                WHERE {where_clause}
                LIMIT {batch_size}
            )
            UPDATE {TABLE_NE}
            SET {NE_NORM_ID} = (
                SELECT agg.{NE_NORM_ID}
                FROM {TABLE_NE_AGGR} agg
                WHERE agg.{CLASS_ID} = {TABLE_NE}.{CLASS_ID}
                AND agg.{TXT_NORM} = {TABLE_NE}.{TXT_NORM}
            )
            WHERE {NE_PRIMARY_ID} IN (SELECT {NE_PRIMARY_ID} FROM batch)
            AND {where_clause}
            """
        # Essential indexes for fast lookup
        index_norm_id_txt_norm = Index(TABLE_NE, [NE_NORM_ID, TXT_NORM], logger=self.logger)
        index_norm_id_txt_norm.create_if_not_exists(self.cursor, analyze=True)

        self.log_query_plan(update_query)

        try:
            with interrupt_handler():
                while total_updated < total_to_update and not interrupted:
                    batch_count += 1
                    batch_start_time = time.time()

                    # --- Execute the UPDATE query with CTE ---
                    self.logger.debug(f"Executing CTE update_query for batch {batch_count}")
                    previous_changes = self.conn.total_changes
                    self.cursor.execute(update_query) # Execute the update query
                    batch_updated = self.conn.total_changes - previous_changes

                    total_updated += batch_updated

                    if batch_updated != batch_size:
                        self.logger.warning(f"Batch {batch_count}: Only {batch_updated} out of {batch_size} entities updated")

                    if batch_updated == 0:
                        self.logger.warning("No entities updated in batch, skipping commit")
                        continue

                    # Commit every 10 batches to avoid large transactions and large rollbacks
                    if batch_count % 10 == 0:
                        self.conn.commit()
                        self.logger.info(f"Committed updates after batch {batch_count}")

                    # Log progress periodically
                    if batch_count % 10 == 0:
                        progress = (total_updated / total_to_update) * 100 if total_to_update > 0 else 100
                        elapsed = time.time() - start_time
                        rate = total_updated / elapsed if elapsed > 0 else 0
                        eta = (total_to_update - total_updated) / rate if rate > 0 else 0

                        self.logger.info(
                            f"Progress: {progress:.1f}% - Updated {total_updated:,}/{total_to_update:,} references "
                            f"(Batch {batch_count}, {int(rate)} rows/sec, ETA: {eta/60:.1f} min)"
                        )

            # Final commit if not interrupted or error
            if not interrupted and not error_occurred:
                self.conn.commit()

        except Exception:
            # Exception already logged in interrupt_handler
            # Just rollback if we haven't committed yet
            self.conn.rollback()
            # Don't re-raise, we'll return error info in result dict

        duration = time.time() - start_time

        # Clean up checkpoint file if not interrupted
        if not interrupted and os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

        self.logger.info(f"Reference update completed: {total_updated:,} references updated in {duration:.2f} seconds")

        # Verify that all entities were properly updated
        remaining_query = f"""--sql
            SELECT COUNT(*) FROM {TABLE_NE}
            WHERE {ERROR_ID} IS NULL
            AND {NE_OVERLAP} = 0
            AND {TXT_NORM} IS NOT NULL
            AND {NE_NORM_ID} IS NULL
        """
        remaining = self.cursor.execute(remaining_query).fetchone()[0]

        if remaining > 0:
            self.logger.warning(f"{remaining:,} entities still missing NE_NORM_ID after update")

        return {
            "total_updated": total_updated,
            "total_to_update": total_to_update,
            "batches_processed": batch_count,
            "interrupted": interrupted,
            "error_occurred": error_occurred,
            "error_message": error_message,
            "processing_time": duration,
            "remaining": remaining if 'remaining' in locals() else 0,
        }

    def _create_aggregation_indexes(self):
        """
        Creates indexes to optimize queries on aggregated entity data.
        """
        self.logger.info("Creating supporting indexes...")

        # Index on NE_NORM_ID for faster reference lookups not untill after populating, to avoid index overhead
        index_norm_id = Index(TABLE_NE, [NE_NORM_ID], logger=self.logger)
        index_norm_id.create_if_not_exists(self.cursor, analyze=True)

        # Index on frequency for common sorting operations
        index_fq = Index(TABLE_NE_AGGR, [FQ], logger=self.logger)
        index_fq.create_if_not_exists(self.cursor, analyze=True)

        # Index on unique documents for sorting and lookups
        index_uniq_docs = Index(TABLE_NE_AGGR, [UNIQ_DOCS], logger=self.logger)
        index_uniq_docs.create_if_not_exists(self.cursor, analyze=True)

    def _validate_entity_aggregation(self):
        """
        Validates entity aggregation results by checking:
        1. All valid entities have a NE_NORM_ID
        2. Sample validation of aggregation counts

        Returns:
            dict: Validation results with success flag and details
        """
        self.logger.info("Validating entity aggregation results...")
        validation_result = {"success": True, "checks": {}}

        try:
            # Check 1: All valid entities should have a NE_NORM_ID
            missing_norm_query = f"""
                SELECT COUNT(*) FROM {TABLE_NE}
                WHERE {ERROR_ID} IS NULL
                AND {NE_OVERLAP} = 0
                AND {TXT_NORM} IS NOT NULL
                AND {NE_NORM_ID} IS NULL
            """
            missing_norm_count = self.cursor.execute(missing_norm_query).fetchone()[0]
            validation_result["checks"]["missing_norm_ids"] = missing_norm_count

            if missing_norm_count > 0:
                validation_result["success"] = False
                validation_result["message"] = f"{missing_norm_count} entities missing NE_NORM_ID"
                self.logger.error(f"Validation failed: {validation_result['message']}")
                return validation_result

            # Check 2: Verify sample of frequency counts (for performance with large datasets)
            aggregation_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE_AGGR}").fetchone()[0]
            sample_size = min(10000, max(1000, int(aggregation_count * 0.01)))  # Sample 1% or at least 1000, max 10000

            self.logger.info(f"Validating counts using {sample_size} sample aggregations...")

            # Check frequency counts
            sample_query = f"""--sql
                SELECT COUNT(*) FROM (
                    SELECT
                        agg.{NE_NORM_ID},
                        agg.{FQ} as expected_count,
                        COUNT(ne.{NE_PRIMARY_ID}) as actual_count
                    FROM (
                        SELECT * FROM {TABLE_NE_AGGR}
                        ORDER BY RANDOM()
                        LIMIT ?
                    ) agg
                    JOIN {TABLE_NE} ne ON
                        ne.{NE_NORM_ID} = agg.{NE_NORM_ID}
                    WHERE ne.{ERROR_ID} IS NULL
                    AND ne.{NE_OVERLAP} = 0
                    GROUP BY agg.{NE_NORM_ID}
                    HAVING expected_count != actual_count
                )
            """
            mismatches = self.cursor.execute(sample_query, (sample_size,)).fetchone()[0]
            validation_result["checks"]["frequency_mismatches"] = mismatches
            validation_result["checks"]["sample_size"] = sample_size

            if mismatches > 0:
                validation_result["success"] = False
                validation_result["message"] = f"{mismatches}/{sample_size} sampled aggregations have count mismatches"
                self.logger.error(f"Validation failed: {validation_result['message']}")
                return validation_result

            # All checks passed
            validation_result["message"] = "All validation checks passed"
            self.logger.info("Entity aggregation validation successful")
            return validation_result

        except Exception as e:
            validation_result["success"] = False
            validation_result["message"] = f"Validation error: {str(e)}"
            self.logger.error(f"Error during aggregation validation: {str(e)}")
            return validation_result

class Statistics(BaseComponent):
    @property
    def entity_statistics(self):
        """Get statistics about entity occurrences"""
        try:
            stats = {
                'total_entities': self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE}").fetchone()[0],
                'unique_entities': self.cursor.execute(f"SELECT COUNT(DISTINCT {TXT_NORM}) FROM {TABLE_NE}").fetchone()[0],
                'entities_with_errors': self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE} WHERE {COL_NE_ERROR_CODE} IS NOT NULL").fetchone()[0]
            }
            return stats
        except sqlite3.Error as e:
            self.logger.error(f"Error getting entity statistics: {e}")
            return None

class Tests(BaseComponent):
    """
    Test suite for entity occurrence data.
    Provides validation methods for entity integrity and reference checks.
    """
    def __init__(self, db_handler: EasyNerDBHandler, statistics: Statistics):
        """
        Initialize Tests component with proper dependencies.

        Args:
            db_handler: Database handler providing connection and logging
            statistics: Statistics component for entity statistics
        """
        # Initialize base component resources
        super().__init__(db_handler)

        # Store reference to statistics component
        self.statistics = statistics

    def test_entity_occurrences(self):
        """Run test suite for EntityOccurrence"""
        self.test_for_duplicates()
        self.test_for_invalid_references()

    def test_for_duplicates(self):
        """Check for duplicate entity entries"""
        query = f"""
            SELECT {DOC_ID}, {SPAN_START}, COUNT(*) as cnt
            FROM {TABLE_NE}
            GROUP BY {DOC_ID}, {SPAN_END}
            HAVING cnt > 1
        """
        duplicates = self.cursor.execute(query).fetchall()
        if duplicates:
            self.logger.error(f"Found {len(duplicates)} duplicate entity entries!")
        else:
            self.logger.info("No duplicate entity entries found.")

    def test_for_invalid_references(self):
        """Check for invalid document or normalized entity references"""
        query = f"""
            SELECT COUNT(*) FROM {TABLE_NE} ne
            LEFT JOIN {TABLE_DOCS} d ON ne.{DOC_ID} = d.{DOC_ID}
            WHERE d.{DOC_ID} IS NULL
        """
        invalid_docs = self.cursor.execute(query).fetchone()[0]
        if invalid_docs:
            self.logger.error(f"Found {invalid_docs} entities with invalid document references!")
        else:
            self.logger.info("No invalid document references found.")

class EntityOccurrence:
    """
    Main entrypoint class for Entity Occurrence functionality.
    Handles integration of schema management, analysis, statistics, and testing.
    """
    def __init__(self, db_system_instance: EasyNerDBHandler):
        # Store direct reference to database handler
        self._db = db_system_instance

        # Import CacheManager only when needed to avoid circular imports
        from ..core.cache_manager import CacheManager
        self.cache = CacheManager(db_system_instance)

        # Create component instances with proper initialization
        self.schema_manager = SchemaManager(db_system_instance)
        self.preprocessor = Preprocessor(db_system_instance)
        self.analysis = Analysis(db_system_instance)
        self.statistics = Statistics(db_system_instance)
        self.aggregator = Aggregator(db_system_instance)

        # Tests component requires both db_handler and statistics component
        self.tests = Tests(db_system_instance, self.statistics)

    def run_all_tests(self):
        """
        Run a comprehensive test suite on the entity occurrence data

        Returns:
            dict: Test results from all test methods
        """
        self._db.logger.info("Running comprehensive entity occurrence tests...")

        results = {
            "entity_occurrences": self.tests.test_entity_occurrences(),
            "normalization": self.tests.validate_normalization_completeness(),
            "statistics": self.statistics.entity_statistics
        }

        # Determine overall status
        has_errors = any(
            result.get("status") == "ERROR"
            for result_group in results.values()
            if isinstance(result_group, dict)
            for result in result_group.values()
            if isinstance(result, dict) and "status" in result
        )

        results["overall_status"] = "ERROR" if has_errors else "OK"

        return results

