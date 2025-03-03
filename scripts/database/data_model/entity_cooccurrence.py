# entity_cooccurrence_module.py
import sqlite3
import time

from scripts.database.core.db_engine import ReaderWriterPair
from .schema import *
import logging
from ..db_main import BaseComponent, EasyNerDBHandler, db_error_handler

class SchemaManager(BaseComponent):

    def setup_tables(self):
        """
        Create tables for entity co-occurrence if they do not
        already exist.
        """
        self.logger.info("Setting up tables for entity co-occurrence analysis...")

        self.cursor.execute(SCHEMA_TABLE_ENTITY_COOCURRENCES)
        self.cursor.execute(SCHEMA_TABLE_DIS_PNM)
        # self.cursor.execute(SCHEMA_TABLE_COOCCURRENCES_AGGR)
        self.conn.commit()

        self.logger.info("Tables created successfully.")

    def setup_views(self):
        """
        Drop and recreate the view for entity co-occurrences.
        """
        self.logger.info("Setting up views for entity co-occurrence analysis...")

        self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_COOCCURRENCES}")
        self.cursor.execute(stmt_view_cooccurrences)

        self.conn.commit()

        self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_COOCCURRENCES_AGGREGATED}")
        self.cursor.execute(stmt_view_cooccurrences_aggregated)
        self.conn.commit()

        self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_COOCCURRENCES}_stats")
        self.cursor.execute(stmt_view_cooccurrences_stats)
        self.conn.commit()

        self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_DIS_PNM}")
        self.cursor.execute(stmt_view_dis_pnm)
        self.conn.commit()

        self.cursor.execute(f"DROP VIEW IF EXISTS view_eco_deprecated")
        self.conn.commit()
        # Add view for deprecated co-occurrence aggregated table with normalized text
        self.stmt_view_eco_deprecated = f"""--sql
        CREATE VIEW IF NOT EXISTS view_eco_deprecated AS
        SELECT
            eco.{E1_NORM_ID} as {E1_NORM_ID},
            eco.{E2_NORM_ID},
            nea1.{TXT_NORM} as e1_txt_norm,
            nea2.{TXT_NORM} as e2_txt_norm,
            eco.fq_document_level,
            eco.fq_sentence_level,
            eco.uniq_documents,
            eco.pmi
        FROM eco_aggregated_deprecated eco
        JOIN {TABLE_NE_AGGR} nea1
            ON eco.{E1_NORM_ID} = nea1.norm_id
        JOIN {TABLE_NE_AGGR} nea2
            ON eco.{E2_NORM_ID} = nea2.norm_id
        """

        self.stmt_view_deprecated_pnm_dis = f"""--sql
        CREATE VIEW IF NOT EXISTS view_deprecated_pnm_dis AS
        SELECT
            ...
        """

        # Drop existing view if it exists
        self.cursor.execute("DROP VIEW IF EXISTS view_eco_deprecated")

        # Create the new view
        self.cursor.execute(self.stmt_view_eco_deprecated)
        self.conn.commit()
        self.logger.info("Views created successfully.")


class Analysis(BaseComponent):
    """
    Main analysis engine for co-occurrence analysis.
    Assumes that the tables and views have been set up.

    Process:
    1. Record entity co-occurrences
    2. Aggregate entity co-occurrences into a summary table
    3.

    """

    def record_all_entity_cooccurrences_multithreaded(
        self, level: str = "document", batch_size=20000, num_reader_threads=32
    ) -> bool:
        """
        Counts entity co-occurrences using ReaderWriterPair.
        NOT idempotent, will add new co-occurrences to the database.

        document_batch CTE: Selects a batch of document IDs based on LIMIT and OFFSET, ordered by id.
        doc_entities CTE: Selects all entities from {TABLE_NE} that belong to the document IDs from document_batch.
        Main SELECT Statement:
            Joins doc_entities with itself to find entity pairs within the same document.
            Filters out existing co-occurrences (using NOT EXISTS).
            Filters based on overlap flags of entities.
            Applies sentence distance filter (conditionally).
            Calculates sentence distance (conditionally).
            Returns DISTINCT pairs of entity IDs and sentence distance.

        This ensures document atomicity across batches
        and allows for parallel processing of entity co-occurrences.
        """
        try:
            if level not in ["document", "sentence"]:
                raise ValueError("Level must be either 'document' or 'sentence'")

            def reader_query_fn(level):  # Define reader_query as a function
                return f"""--sql
                        WITH document_batch AS (
                            SELECT d.id AS doc_id
                            FROM {TABLE_DOCS} d
                            ORDER BY d.id
                            LIMIT :limit OFFSET :offset
                        ),
                        doc_entities AS (
                            SELECT
                                ne.id,
                                ne.{DOC_ID},
                                    ne.{SENT_IDX},
                                ne.{NE_NORM_ID}
                            FROM {TABLE_NE} ne
                            WHERE ne.{DOC_ID} IN (SELECT doc_id FROM document_batch)
                        ),
                        distinct_pairs AS (
                            SELECT DISTINCT
                                e1.id AS {E1_ID},
                                e2.id AS {E2_ID}
                            FROM doc_entities e1
                            JOIN doc_entities e2 ON
                                e1.document_id = e2.document_id AND
                                e1.id < e2.id AND -- Ensure canonical order and avoid self-joins
                                e1.{NE_NORM_ID} IS NOT NULL AND
                                e2.{NE_NORM_ID} IS NOT NULL -- This should filter out any entities with error codes or overlap as they do not have normalized IDs
                                    {"AND ABS(e1." + {SENT_IDX} + "- e2." + {SENT_IDX} + ") <= 5" if level == "sentence" else ""}
                            WHERE NOT EXISTS ( -- Do not include existing co-occurrences
                                SELECT 1
                                FROM {TABLE_COOCCURRENCES} ec
                                WHERE ec.{E1_ID} = e1.id AND ec.{E2_ID} = e2.id
                            )
                        )
                        SELECT -- Return the final distinct pair
                            p.{E1_ID},
                            p.{E2_ID}
                                {", (SELECT ABS(e1." + {SENT_IDX} + " - e2." + {SENT_IDX} + ") FROM doc_entities e1 JOIN doc_entities e2 ON e1.id = p.{E1_ID} AND e2.id = p.{E2_ID}) AS sentence_distance" if level == "sentence" else ""}
                        FROM distinct_pairs p
                """

            try:
                # For query plan logging, provide sample values
                query_with_params = (
                    reader_query_fn(level)
                    .replace(":limit", "1000")
                    .replace(":offset", "100")
                )  # Use sample values
                self.log_query_plan(query_with_params)
            except Exception as e:
                self.logger.error(f"Error creating reader query: {e}")
                raise

            def cooccurrence_process_function(batch, conn_params):
                """Processes a batch of entity co-occurrence data."""
                return batch  # For now, minimal processing, it done database side - just pass the batch through

            def cooccurrence_write_function(batch, cursor, conn, logger):
                """Writes a batch of entity co-occurrences to the database using executemany."""
                logger.info(
                    f"Writing batch of {len(batch)} co-occurrences to the database."
                )
                sql = f"""--sql
                        INSERT INTO {TABLE_COOCCURRENCES} ({E1_ID}, {E2_ID} {", sentence_distance" if level == "sentence" else ""})
                        VALUES (?, ? {", ?" if level == "sentence" else ""})
                    """
                try:
                    cursor.executemany(
                        sql, batch
                    )  # Directly use the batch from reader as it's pre-formatted
                except Exception as e:
                    conn.rollback()  # Rollback transaction on error for the current batch
                    print(
                        f"Error in write_function with : {e}. Transaction rolled back for current batch."
                    )
                    return False  # Indicate failure (optional error handling)
                return True  # Indicate success (optional success indication)

            # --- COUNT QUERY TO GET ACCURATE total_count ---
            # count_query = "SELECT COUNT(*) FROM (" + reader_query_fn(level) + ")"
            # self.cursor.execute(count_query)
            # total_count = self.cursor.fetchone()[0]
            total_count = self.cursor.execute(
                f"SELECT COUNT(*) FROM {TABLE_DOCS}"
            ).fetchone()[0]

            self.logger.info(
                f"Total documents to process for co-occurrences: {total_count:,}"
            )

            rw_pair = ReaderWriterPair(
                conn_params=self.conn_params_dict,
                reader_query=reader_query_fn(level),  # Pass reader query function
                batch_size=batch_size,
                process_function=cooccurrence_process_function,
                write_function=cooccurrence_write_function,
                num_reader_threads=num_reader_threads,
                logger=self.logger,
                max_queue_size=500,
                profiling_writer_enabled=True,
                profiling_reader_enabled=True,
                writer_batch_chunking=4,
                total_rows=total_count,  # Use the accurate count
                process_title=f"Co-occurrence counting at {level} level",
            )
            try:
                self.logger.info(
                    f"Starting ReaderWriterPair to count entity co-occurrences at {level} level."
                )
                rw_pair.run()
                self.logger.info(
                    f"ReaderWriterPair process finished for {level} level co-occurrence counting."
                )

                return True

            except Exception as e:
                self.logger.error(f"Error counting entity co-occurrences: {e}")
                raise

        except Exception as e:
            self.logger.error(f"Error counting entity co-occurrences: {e}")
            return False

    def record_dis_pnm(
        self,
        batch_size=500000,
        num_reader_threads=32,
        writer_chunking=200,
        max_queue_size=1000,
    ) -> bool:
        """
        Record all disease-phenomena (DIS-PNM) co-occurrences within documents into TABLE_DIS_PNM.
        Uses multithreaded processing with batched document approach.
        Should be highly filtered reads, so use large batch size for number of documents processed at a time.

        Args:
            - batch_size (int): Number of documents to process in each batch
            - num_reader_threads (int): Number of reader threads to use, preferably multiple of 2 and as many as the system can handle since the logic is offloaded to the readers and database
            - writer_chunking (int): Number of batches to write in a single transaction, to reduce overhead. Default is 10 = 10 * batch_size rows per transaction

        Returns:
            bool: True if successful, False otherwise
        """

        # Get the class ids for DIS and PNM
        dis_class_id = self.db.data_exchanger.get_named_entity_class_id("DIS")
        pnm_class_id = self.db.data_exchanger.get_named_entity_class_id("PNM")

        self.logger.info("Starting DIS-PNM co-occurrence extraction...")
        # Verify table exists and is accessible
        try:
            self.cursor.execute(f"SELECT 1 FROM {TABLE_DIS_PNM} LIMIT 1")
            self.logger.info(f"Table {TABLE_DIS_PNM} is accessible")
        except sqlite3.OperationalError:
            self.logger.warning(f"Table {TABLE_DIS_PNM} not accessible")

        # Composite index for disease entities filtering (NE_CLASS_ID=1)
        idx_ne_disease = Index(
            TABLE_NE,
            [CLASS_ID, DOC_ID, NE_NORM_ID],
                            where=f"{CLASS_ID}=1 AND {NE_NORM_ID} IS NOT NULL",
            logger=self.logger,
        )

        # Composite index for protein/molecule entities filtering (NE_CLASS_ID=2)
        idx_ne_pnm = Index(
            TABLE_NE,
            [CLASS_ID, DOC_ID, NE_NORM_ID],
                        where=f"{CLASS_ID}=2 AND {NE_NORM_ID} IS NOT NULL",
            logger=self.logger,
        )

        # Index for joining dis_entities and pnm_entities on DOC_ID
        idx_ne_doc_id = Index(TABLE_NE, [DOC_ID], logger=self.logger)

        any_index_created = False
        for idx in [idx_ne_disease, idx_ne_pnm, idx_ne_doc_id]:
            if (idx.create_if_not_exists(self.cursor)):
                any_index_created = True

        if any_index_created:
            self.conn.commit()
            self.cursor.execute("ANALYZE")

        # Create view for DIS-PNM co-occurrences
        VIEW_DIS_PNM_PRESENTATION.refresh(self.cursor)

        try:

            def reader_query_fn():
                return f"""--sql
                    WITH document_batch AS (
                        SELECT {DOC_ID} AS {DOC_ID}
                        FROM {TABLE_DOCS}
                        ORDER BY {DOC_ID} -- primary key ordering, ensures batch atomicity
                        LIMIT :limit OFFSET :offset
                    ),
                    dis_entities AS (
                        SELECT
                            {NE_PRIMARY_ID},
                            {DOC_ID} ,
                            {SENT_IDX}
                        FROM {TABLE_NE} ne
                        WHERE ne.{DOC_ID} IN (SELECT {DOC_ID} FROM document_batch)
                            AND ne.{CLASS_ID} = {dis_class_id}
                            AND ne.{NE_NORM_ID} IS NOT NULL -- Ensure only properly normalized entities are retrieved
                    ),
                    pnm_entities AS (
                        SELECT
                            {NE_PRIMARY_ID},
                            {DOC_ID},
                            {SENT_IDX},
                            {CLASS_ID}
                        FROM {TABLE_NE} ne
                        WHERE ne.{DOC_ID} IN (SELECT {DOC_ID} FROM document_batch)
                            AND ne.{CLASS_ID} = {pnm_class_id}
                            AND ne.{NE_NORM_ID} IS NOT NULL -- Ensure only properly normalized entities are retrieved
                    ),
                    distinct_pairs AS ( -- Get distinct pairs of DIS-PNM entities within the same document for the document batch
                        SELECT DISTINCT
                            dis.{NE_PRIMARY_ID} AS dis_id,
                            pnm.{NE_PRIMARY_ID} AS pnm_id
                        FROM dis_entities dis
                        JOIN pnm_entities pnm ON
                            dis.{DOC_ID} = pnm.{DOC_ID}
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {TABLE_DIS_PNM} dp
                            WHERE dp.{E1_ID} = dis.{NE_PRIMARY_ID} AND dp.{E2_ID} = pnm.{NE_PRIMARY_ID} -- Do not include existing co-occurrences
                        )
                    )
                    SELECT
                        dis_id,
                        pnm_id
                    FROM distinct_pairs
                """

            try:
                # For query plan logging
                self.log_query_plan(
reader_query_fn()
.replace(":limit", "1000")
.replace(":offset", "100")
)
            except Exception as e:
                self.logger.error(f"Error creating reader query: {e}")
                raise

            def process_function(batch, conn_params):
                """Pass through the batch - processing done in SQL"""
                return batch

            writer_sql = f"""--sql
                    INSERT INTO {TABLE_DIS_PNM} ({E1_ID}, {E2_ID})
                    VALUES (?, ?)
                """

            def write_function(batch, cursor, conn):
                """Write DIS-PNM co-occurrences to database"""
                cursor.executemany(
                    writer_sql, batch
                )  # Commits are handled by the ReaderWriterPair

            # Get total document count - Filtering out already processed documents is done in the reader query. We process all documents, even if some might have been processed before.
            # Not the most efficient, but ensures that all documents are processed for now.

            total_count = self.db.statistics.document_count
            self.logger.info(
                f"Total documents to process for DIS-PNM co-occurrences: {total_count:,}"
            )

            # Create and run the reader-writer pair
            rw_pair = ReaderWriterPair(
                conn_params=self.conn_params_dict,
                reader_query=reader_query_fn(),
                batch_size=batch_size,
                process_function=process_function,
                write_function=write_function,
                num_reader_threads=num_reader_threads,
                logger=self.logger,
                max_queue_size=max_queue_size,
                profiling_writer_enabled=False,
                profiling_reader_enabled=False,
                writer_batch_chunking=writer_chunking,
                total_rows=total_count,
                process_title="DIS-PNM co-occurrence extraction",
            )

            self.logger.info(
                "Starting ReaderWriterPair for DIS-PNM co-occurrence extraction"
            )
            rw_pair.run()
            self.logger.info("DIS-PNM co-occurrence extraction completed successfully")

            # Count and log results
            count = self.cursor.execute(
                f"SELECT COUNT(*) FROM {TABLE_DIS_PNM}"
            ).fetchone()[0]
            self.logger.info(f"Total DIS-PNM co-occurrences recorded: {count:,}")

            return True

        except Exception as e:
            self.logger.error(f"Error recording DIS-PNM co-occurrences: {e}")
            return False

    def _validate_dis_pnm_integrity(self):
        """
        Validates the integrity of DIS-PNM co-occurrences.

        Returns:
            bool: True if validation passes, False otherwise
        """
        self.logger.info("Validating DIS-PNM co-occurrence integrity...")

        try:
            # Check for duplicate pairs
            duplicate_count = self.cursor.execute(
                f"""--sql
                SELECT COUNT(*) FROM (
                    SELECT {E1_ID}, {E2_ID}, COUNT(*) as cnt
                    FROM {TABLE_DIS_PNM}
                    GROUP BY {E1_ID}, {E2_ID}
                    HAVING cnt > 1
                )
            """
            ).fetchone()[0]

            if duplicate_count > 0:
                self.logger.error(
                    f"Found {duplicate_count} duplicate DIS-PNM co-occurrences!"
                )

            # Check for missing entity references
            invalid_refs = self.cursor.execute(
                f"""--sql
                SELECT COUNT(*) FROM {TABLE_DIS_PNM} dp
                LEFT JOIN {TABLE_NE} ne1 ON dp.{E1_ID} = ne1.{NE_PRIMARY_ID}
                LEFT JOIN {TABLE_NE} ne2 ON dp.{E2_ID} = ne2.{NE_PRIMARY_ID}
                WHERE ne1.{NE_PRIMARY_ID} IS NULL OR ne2.{NE_PRIMARY_ID} IS NULL
            """
            ).fetchone()[0]

            if invalid_refs > 0:
                self.logger.error(
                    f"Found {invalid_refs} DIS-PNM co-occurrences with invalid entity references!"
                )

            return duplicate_count == 0 and invalid_refs == 0

        except Exception as e:
            self.logger.error(f"Error during DIS-PNM validation: {e}")
            return False

    def calc_sent_distance(self):
        """
        Calculate and record sentence distance for DIS-PNM co-occurrences.
        Updates the SENT_DIST column in the DIS_PNM table.
        Blazingly fast - 759,196 rows in 0.2 seconds.

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info("Calculating sentence distances for DIS-PNM co-occurrences...")
        start_time = time.time()

        try:
            # Count pairs with missing sentence distance
            missing_count = self.cursor.execute(
                f"""--sql
                SELECT COUNT(*) FROM {TABLE_DIS_PNM}
                WHERE {SENT_DIST} IS NULL
            """
            ).fetchone()[0]

            if missing_count == 0:
                self.logger.info(
                    "All sentence distances are already calculated. Skipping."
                )
                return True

            self.logger.info(
                f"Found {missing_count:,} co-occurrences with missing sentence distance"
            )

            # Start a transaction
            self.cursor.execute("BEGIN TRANSACTION")

            # Update sentence distance with batch processing
            update_query = f"""--sql
                UPDATE {TABLE_DIS_PNM}
                SET {SENT_DIST} = (
                    SELECT ABS(ne1.{SENT_IDX} - ne2.{SENT_IDX})
                    FROM {TABLE_NE} ne1
                    JOIN {TABLE_NE} ne2 ON ne1.{DOC_ID} = ne2.{DOC_ID}
                    WHERE ne1.{NE_PRIMARY_ID} = {TABLE_DIS_PNM}.{E1_ID}
                    AND ne2.{NE_PRIMARY_ID} = {TABLE_DIS_PNM}.{E2_ID}
                )
                WHERE {SENT_DIST} IS NULL
            """

            self.cursor.execute(update_query)
            rows_updated = self.cursor.rowcount

            # Validate the updates
            still_missing = self.cursor.execute(
                f"""--sql
                SELECT COUNT(*) FROM {TABLE_DIS_PNM}
                WHERE {SENT_DIST} IS NULL
            """
            ).fetchone()[0]

            if still_missing > 0:
                self.logger.error(
                    f"Failed to calculate sentence distance for {still_missing} co-occurrences"
                )
                self.conn.rollback()
                return False

            # Collect statistics
            stats = self.cursor.execute(
                f"""--sql
                SELECT
                    COUNT(*) as total,
                    AVG({SENT_DIST}) as avg_distance,
                    MIN({SENT_DIST}) as min_distance,
                    MAX({SENT_DIST}) as max_distance
                FROM {TABLE_DIS_PNM}
            """
            ).fetchone()

            # Commit transaction
            self.conn.commit()

            elapsed_time = time.time() - start_time
            self.logger.info(
                f"Updated sentence distances for {rows_updated:,} DIS-PNM co-occurrences in {elapsed_time:.2f} seconds. "
                f"Average distance: {stats[1]:.2f}, Min: {stats[2]}, Max: {stats[3]}"
            )

            return True

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"SQLite error calculating sentence distances: {e}")
            return False
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error calculating sentence distances: {e}")
            return False


class Aggregator(BaseComponent):
    def __init__(self, db_handler: EasyNerDBHandler):
        """
        Initialize Aggregator component with proper dependencies.

        Args:
            db_handler: Database handler providing connection and logging
        """
        # Initialize base component resources
        super().__init__(db_handler)
        VIEW_DIS_PNM_CO_AGGR_ROW_FACTORY.refresh(self.cursor)

    def aggregate_dis_pnm(self, batch_size=50000, overwrite: bool = False) -> dict:
        """
        Idempotent aggregation of DIS-PNM co-occurrences into a summary table.

        Aggregates TABLE_DIS_PNM entries by normalized entity IDs, calculating:
        - Frequency counts (document and sentence level)
        - Unique document counts
        - Sentence distance statistics

        Args:
            batch_size (int): Batch size for processing large datasets
            overwrite (bool):
                If True, overwrites existing aggregations;
                If False, preserves existing aggregations (default)

        Returns:
            dict: Results of aggregation process or None if failed
        """
        self.logger.info("Starting DIS-PNM co-occurrence aggregation...")
        start_time = time.time()

        try:
            # Start transaction for atomicity
            self.cursor.execute("BEGIN TRANSACTION")

            if not self.cursor.execute(
                f"""--sql
                SELECT name FROM sqlite_master
                WHERE type='table' AND name=?
                """,
                (TABLE_DIS_PNM,),
            ).fetchone():
                self.logger.error(
                    f"Table {TABLE_DIS_PNM} does not exist. Aborting aggregation."
                )
                return None

            if not self.cursor.execute(
                f"""--sql
                SELECT name FROM sqlite_master
                WHERE type='table' AND name=?
                """,
                (TABLE_DIS_PNM_AGGR,),
            ).fetchone():
                try:
                    self.cursor.execute(SCHEMA_TABLE_DIS_PNM_AGGR)
                    self.conn.commit()
                    self.logger.info(
                        f"Created table {TABLE_DIS_PNM_AGGR} for aggregation"
                    )
                except sqlite3.Error as e:
                    self.conn.rollback()
                    self.logger.error(f"Error creating table {TABLE_DIS_PNM_AGGR}: {e}")
                    return None

            # Step 1: Prepare and validate input data
            stats = self._analyze_dis_pnm_for_aggregation()
            if stats["valid_pairs"] == 0:
                self.logger.warning(
                    "No valid DIS-PNM co-occurrences found for aggregation. Skipping process."
                )
                self.conn.rollback()
                return None

            # Step 2: Perform co-occurrence aggregation
            if (
                stats["unique_combinations"] == stats["existing_aggregations"]
                and not overwrite
            ):
                self.logger.info(
                    "All DIS-PNM combinations are already aggregated. Skipping aggregation."
                )
                aggregation_result = {
                    "aggregated_rows": stats["existing_aggregations"],
                    "processing_time": 0,
                }
            else:
                aggregation_result = self._perform_dis_pnm_aggregation(overwrite)

            # Step 3: Create supporting indexes
            self._create_dis_pnm_aggr_indexes()

            # Step 4: Validate results
            validation_result = self._validate_dis_pnm_aggregation()

            if validation_result["success"]:
                # Commit all changes
                self.conn.commit()
                elapsed_time = time.time() - start_time
                self.logger.info(
                    f"DIS-PNM aggregation completed successfully in {elapsed_time:.2f} seconds"
                )
                return {
                    "pairs_processed": stats["valid_pairs"],
                    "unique_aggregations": aggregation_result["aggregated_rows"],
                    "validation": validation_result,
                    "processing_time": elapsed_time,
                }
            else:
                self.logger.error(
                    f"DIS-PNM aggregation validation failed: {validation_result['message']}"
                )
                self.conn.rollback()
                return None

        except sqlite3.Error as e:
            self.conn.rollback()
            error_msg = f"SQLite error during DIS-PNM aggregation: {e}"
            self.logger.error(error_msg)
            raise
        except KeyboardInterrupt:
            self.conn.rollback()
            self.logger.warning("User interrupted. Rolling back changes.")
            raise
        except Exception as e:
            self.conn.rollback()
            error_msg = f"Error during DIS-PNM aggregation: {e}"
            self.logger.error(error_msg)
            raise

    def _analyze_dis_pnm_for_aggregation(self):
        """
        Analyzes DIS-PNM co-occurrence data to determine aggregation scope
        and creates necessary indexes for efficient processing.

        Returns:
            dict: Statistics about the co-occurrences to be processed
        """
        self.logger.info("Analyzing DIS-PNM co-occurrence data...")

        # Create necessary indexes for efficient aggregation
        self.logger.info("Creating supporting indexes for aggregation...")
        e1_index = Index(TABLE_DIS_PNM, [E1_ID], logger=self.logger)
        e1_index.create_if_not_exists(self.cursor)

        e2_index = Index(TABLE_DIS_PNM, [E2_ID], logger=self.logger)
        e2_index.create_if_not_exists(self.cursor)

        # Get basic statistics
        stats_query = f"""--sql
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN ne1.{NE_NORM_ID} IS NOT NULL AND ne2.{NE_NORM_ID} IS NOT NULL THEN 1 END) as valid_pairs
            FROM {TABLE_DIS_PNM} dp
            JOIN {TABLE_NE} ne1 ON dp.{E1_ID} = ne1.{NE_PRIMARY_ID}
            JOIN {TABLE_NE} ne2 ON dp.{E2_ID} = ne2.{NE_PRIMARY_ID}
        """

        # Count unique combinations using normalized IDs
        unique_combos_query = f"""--sql
            SELECT COUNT(*)
            FROM (
                SELECT 1
                FROM {TABLE_DIS_PNM} dp
                JOIN {TABLE_NE} ne1 ON dp.{E1_ID} = ne1.{NE_PRIMARY_ID}
                JOIN {TABLE_NE} ne2 ON dp.{E2_ID} = ne2.{NE_PRIMARY_ID}
                WHERE ne1.{NE_NORM_ID} IS NOT NULL AND ne2.{NE_NORM_ID} IS NOT NULL
                GROUP BY ne1.{NE_NORM_ID}, ne2.{NE_NORM_ID}
            )
        """

        try:
            stats = self.cursor.execute(stats_query).fetchone()
            unique_combinations = self.cursor.execute(unique_combos_query).fetchone()[0]

            # Get existing aggregation count
            aggr_count = 0
            try:
                self.cursor.execute(SCHEMA_TABLE_DIS_PNM_AGGR)
                aggr_count = self.cursor.execute(
                    f"SELECT COUNT(*) FROM {TABLE_DIS_PNM_AGGR}"
                ).fetchone()[0]
            except sqlite3.OperationalError:
                self.logger.info(
                    f"{TABLE_DIS_PNM_AGGR} table does not exist yet, will be created"
                )

            stats_dict = {
                "total_pairs": stats[0],
                "valid_pairs": stats[1],
                "unique_combinations": unique_combinations,
                "existing_aggregations": aggr_count,
            }

            self.logger.info(
                f"Found {stats_dict['valid_pairs']:,} valid DIS-PNM co-occurrences out of {stats_dict['total_pairs']:,} total "
                f"with {stats_dict['unique_combinations']:,} unique combinations "
                f"({stats_dict['existing_aggregations']:,} existing aggregations)"
            )

            return stats_dict
        except Exception as e:
            self.logger.error(f"Error analyzing DIS-PNM data: {e}")
            raise

    def _perform_dis_pnm_aggregation(self, overwrite: bool = False):
        """
        Performs DIS-PNM co-occurrence aggregation by directly inserting into the aggregation table.

        Args:
            overwrite (bool): If True, overwrites existing aggregations.
                              If False, preserves existing aggregations.

        Returns:
            dict: Results of the aggregation operation
        """
        self.logger.info(
            f"Performing DIS-PNM co-occurrence aggregation (overwrite={overwrite})..."
        )
        start = time.time()

        # Choose appropriate insert method based on overwrite flag
        insert_method = "INSERT OR REPLACE" if overwrite else "INSERT OR IGNORE"

        # Perform aggregation and insert in a single step
        aggregation_query = f"""--sql
            {insert_method} INTO {TABLE_DIS_PNM_AGGR}
            ({E1_NORM_ID}, {E2_NORM_ID}, {FQ_DOCUMENT_LEVEL}, {UNIQ_DOCS}, avg_sentence_distance, min_sentence_distance, max_sentence_distance)
            SELECT
                ne1.{NE_NORM_ID} as {E1_NORM_ID},
                ne2.{NE_NORM_ID} as {E2_NORM_ID},
                COUNT(*) as {FQ_DOCUMENT_LEVEL},
                COUNT(DISTINCT ne1.{DOC_ID}) as {UNIQ_DOCS},
                AVG(dp.{SENT_DIST}) as avg_sentence_distance,
                MIN(dp.{SENT_DIST}) as min_sentence_distance,
                MAX(dp.{SENT_DIST}) as max_sentence_distance
            FROM {TABLE_DIS_PNM} dp
            JOIN {TABLE_NE} ne1 ON dp.{E1_ID} = ne1.{NE_PRIMARY_ID}
            JOIN {TABLE_NE} ne2 ON dp.{E2_ID} = ne2.{NE_PRIMARY_ID}
            WHERE ne1.{NE_NORM_ID} IS NOT NULL AND ne2.{NE_NORM_ID} IS NOT NULL
            GROUP BY ne1.{NE_NORM_ID}, ne2.{NE_NORM_ID}
        """

        self.cursor.execute(aggregation_query)

        # Get number of rows in the aggregation table
        rows_aggregated = self.cursor.execute(
            f"SELECT COUNT(*) FROM {TABLE_DIS_PNM_AGGR}"
        ).fetchone()[0]

        duration = time.time() - start
        self.logger.info(
            f"DIS-PNM co-occurrence aggregation completed in {duration:.2f} seconds: {rows_aggregated} unique pairs"
        )

        return {"aggregated_rows": rows_aggregated, "processing_time": duration}

    def _create_dis_pnm_aggr_indexes(self):
        """
        Creates indexes to optimize queries on aggregated DIS-PNM co-occurrence data.
        """
        self.logger.info("Creating supporting indexes for DIS-PNM aggregation...")

        # Index on normalized disease IDs for faster lookups
        index_dis = Index(TABLE_DIS_PNM_AGGR, [E1_NORM_ID], logger=self.logger)
        index_dis.create_if_not_exists(self.cursor, analyze=True)

        # Index on normalized protein/molecule IDs for faster lookups
        index_pnm = Index(TABLE_DIS_PNM_AGGR, [E2_NORM_ID], logger=self.logger)
        index_pnm.create_if_not_exists(self.cursor, analyze=True)

        # Index on frequency for common sorting operations
        index_freq = Index(TABLE_DIS_PNM_AGGR, [FQ_DOCUMENT_LEVEL], logger=self.logger)
        index_freq.create_if_not_exists(self.cursor, analyze=True)

    def _validate_dis_pnm_aggregation(self):
        """
        Validates DIS-PNM co-occurrence aggregation results by checking:
        1. All unique combinations are aggregated
        2. Sample validation of frequency counts

        Returns:
            dict: Validation results with success flag and details
        """
        self.logger.info("Validating DIS-PNM aggregation results...")
        validation_result = {"success": True, "checks": {}}

        try:
            # Check 1: Verify all unique combinations are aggregated
            unique_combinations_query = f"""--sql
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT ne1.{NE_NORM_ID}, ne2.{NE_NORM_ID}
                    FROM {TABLE_DIS_PNM} dp
                    JOIN {TABLE_NE} ne1 ON dp.{E1_ID} = ne1.{NE_PRIMARY_ID}
                    JOIN {TABLE_NE} ne2 ON dp.{E2_ID} = ne2.{NE_PRIMARY_ID}
                    WHERE ne1.{NE_NORM_ID} IS NOT NULL AND ne2.{NE_NORM_ID} IS NOT NULL
                )
            """
            unique_count = self.cursor.execute(unique_combinations_query).fetchone()[0]

            aggregated_count = self.cursor.execute(
                f"SELECT COUNT(*) FROM {TABLE_DIS_PNM_AGGR}"
            ).fetchone()[0]

            validation_result["checks"]["unique_combinations"] = unique_count
            validation_result["checks"]["aggregated_combinations"] = aggregated_count

            if unique_count != aggregated_count:
                validation_result["success"] = False
                validation_result["message"] = (
                    f"Validation failed: {aggregated_count} of {unique_count} unique combinations were aggregated"
                )
                return validation_result

            # Check 2: Verify sample of frequency counts
            sample_size = min(
                1000, max(100, int(aggregated_count * 0.05))
            )  # Sample 5% or at least 100, max 1000

            self.logger.info(
                f"Validating counts using {sample_size} sample aggregations..."
            )

            # Select sample of aggregated pairs
            sample_query = f"""--sql
                SELECT {E1_NORM_ID}, {E2_NORM_ID}, {FQ_DOCUMENT_LEVEL}
                FROM {TABLE_DIS_PNM_AGGR}
                ORDER BY RANDOM()
                LIMIT {sample_size}
            """
            sample_rows = self.cursor.execute(sample_query).fetchall()

            # Validate the sample against raw data
            mismatches = 0
            for row in sample_rows:
                dis_id, pnm_id, agg_freq = row

                # Get actual frequency from raw data
                actual_freq_query = f"""--sql
                    SELECT COUNT(*)
                    FROM {TABLE_DIS_PNM} dp
                    JOIN {TABLE_NE} ne1 ON dp.{E1_ID} = ne1.{NE_PRIMARY_ID}
                    JOIN {TABLE_NE} ne2 ON dp.{E2_ID} = ne2.{NE_PRIMARY_ID}
                    WHERE ne1.{NE_NORM_ID} = ? AND ne2.{NE_NORM_ID} = ?
                """
                actual_freq = self.cursor.execute(
                    actual_freq_query, (dis_id, pnm_id)
                ).fetchone()[0]

                if actual_freq != agg_freq:
                    mismatches += 1
                    if (
                        mismatches <= 5
                    ):  # Log only first 5 mismatches to avoid overwhelming logs
                        self.logger.warning(
                            f"Frequency mismatch for DIS-PNM pair ({dis_id}, {pnm_id}): "
                            f"aggregated={agg_freq}, actual={actual_freq}"
                        )

            validation_result["checks"]["sample_size"] = sample_size
            validation_result["checks"]["frequency_mismatches"] = mismatches

            if mismatches > 0:
                mismatch_percentage = (mismatches / sample_size) * 100
                validation_result["success"] = (
                    mismatch_percentage < 1
                )  # Allow up to 1% error rate
                validation_result["message"] = (
                    f"Found {mismatches} frequency mismatches ({mismatch_percentage:.2f}%) in sample of {sample_size}"
                )
                if validation_result["success"]:
                    self.logger.warning(
                        validation_result["message"] + " - within acceptable threshold"
                    )
                else:
                    self.logger.error(
                        validation_result["message"] + " - exceeds acceptable threshold"
                    )

            return validation_result

        except Exception as e:
            self.logger.error(f"Error validating DIS-PNM aggregation: {e}")
            validation_result["success"] = False
            validation_result["message"] = f"Validation failed: {e}"
            return validation_result
    def update_unique_document_counts(self):

        self.logger.info("Updating unique document counts and frequencies...")

        query_with_view = f"""--sql
        UPDATE {TABLE_CO_AGGR}
        SET fq_document_level = aggregated_stats.fq_document_level,
            fq_sentence_level = aggregated_stats.fq_sentence_level,
            uniq_docs = aggregated_stats.uniq_docs
        FROM (
            SELECT
                vcs.{E1_NORM_ID},
                vcs.{E2_NORM_ID},
                COUNT(*) AS {FQ_DOCUMENT_LEVEL},
                SUM(CASE WHEN vcs.sent_idx_1 = vcs.sent_idx_2 THEN 1 ELSE 0 END) AS fq_sentence_level,
                COUNT(DISTINCT vcs.doc_id) AS uniq_docs
            FROM {VIEW_COOCCURRENCES}_stats vcs
            GROUP BY vcs.{E1_NORM_ID}, vcs.{E2_NORM_ID}
        ) AS aggregated_stats
        WHERE co_aggregated.{E1_ID} = aggregated_stats.{E1_NORM_ID}
            AND co_aggregated.{E2_ID} = aggregated_stats.{E2_NORM_ID};
        """

        query_without_view = f"""--sql
        UPDATE {TABLE_CO_AGGR}
        SET fq_document_level = aggregated_stats.fq_document_level,
            fq_sentence_level = aggregated_stats.fq_sentence_level,
            uniq_docs = aggregated_stats.uniq_docs
        FROM (
            SELECT
                CASE
                    WHEN ne1.{NE_NORM_ID} < ne2.{NE_NORM_ID} THEN ne1.{NE_NORM_ID}
                    ELSE ne2.{NE_NORM_ID}
                END AS e1_norm_id,
                CASE
                    WHEN ne1.{NE_NORM_ID} < ne2.{NE_NORM_ID} THEN ne2.{NE_NORM_ID}
                    ELSE ne1.{NE_NORM_ID}
                END AS e2_norm_id,
                COUNT(*) AS fq_document_level,
                SUM(CASE WHEN ne1.{SENT_IDX} = ne2.{SENT_IDX} THEN 1 ELSE 0 END) AS fq_sentence_level,
                COUNT(DISTINCT ne1.{DOC_ID}) AS uniq_docs
            FROM {TABLE_COOCCURRENCES} co
            JOIN {TABLE_NE} ne1 ON co.{E1_ID} = ne1.{NE_PRIMARY_ID} -- get the raw entity data 1
            JOIN {TABLE_NE} ne2 ON co.{E2_ID} = ne2.{NE_PRIMARY_ID} -- get the raw entity data 2
            GROUP BY e1_norm_id, e2_norm_id -- Group by canonicalized normalized IDs
        ) AS aggregated_stats
        WHERE {TABLE_CO_AGGR}.{E1_ID} = aggregated_stats.e1_norm_id
            AND {TABLE_CO_AGGR}.{E2_ID} = aggregated_stats.e2_norm_id;
        """

        query = query_without_view

        try:
            self.log_query_plan(query)
            self.cursor.execute(query)
            self.conn.commit()
            self.logger.info(
                "Unique document counts and frequencies updated successfully."
            )
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(
                f"Error updating unique document counts and frequencies: {e}"
            )
            raise
        except KeyboardInterrupt:
            self.conn.rollback()
            self.logger.error(
                "Unique document counts and frequencies update cancelled."
            )
            raise


class Statistics(BaseComponent):
    """
    Statistics module class, to be integrated into DatabaseSystem.
    """

    @property
    def self_reference_count(self):
        """
        Get the number of self-references in the co-occurrence
        table.
        """
        return self.cursor.execute(
            f"SELECT COUNT(*) FROM {TABLE_COOCCURRENCES} WHERE {E1_ID} = {E2_ID}"
        ).fetchone()[0]

    def duplicate_pairs(self) -> list:
        """
        Check for duplicate entity pairs in the co-occurrence table.

        Returns:
            list: List of tuples ({E1_ID}, {E2_ID}, count) for pairs with duplicates
        """
        query = f"""
            SELECT {E1_ID}, {E2_ID}, COUNT(*) as cnt
            FROM {TABLE_COOCCURRENCES}
            GROUP BY {E1_ID}, {E2_ID}
            HAVING cnt > 1
        """
        return self.cursor.execute(query).fetchall()

        """
        Aggregates entity co-occurrences.
        Accessed via db_system.entity_cooccurrence.co_aggregate_old()
        """
class EntityCooccurrence:
    """
    Main entrypoint class for Entity Co-occurrence functionality.
    Handles integration of schema management, analysis, statistics, and testing.
    """
    def __init__(self, db_system_instance: EasyNerDBHandler):
        # Store direct reference to database handler
        self._db = db_system_instance

        # Create component instances with proper initialization
        self.schema_manager = SchemaManager(db_system_instance)
        self.analysis = Analysis(db_system_instance)
        self.statistics = Statistics(db_system_instance)

        # Tests component requires both db_handler and statistics component
        self.tests = Tests(db_system_instance, self.statistics)
        
    def record_entity_cooccurrences_multithreaded(self, *args, **kwargs):
        """
        Delegate to analysis component.
        """
        if self.analysis.record_all_entity_cooccurrences_multithreaded(*args, **kwargs):
            self.tests.has_self_references()
            self.tests.has_duplicates()

    def update_unique_document_counts(self):
        """
        Delegate to analysis component
        """
        return self.analysis.update_unique_document_counts()



