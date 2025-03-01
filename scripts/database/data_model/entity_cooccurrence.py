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
            query_with_params = reader_query_fn(level).replace(':limit', '1000').replace(':offset','100')  # Use sample values
            self.log_query_plan(query_with_params)
        except Exception as e:
            self.logger.error(f"Error creating reader query: {e}")
            raise

        def cooccurrence_process_function(batch, conn_params):
            """Processes a batch of entity co-occurrence data."""
            return batch  # For now, minimal processing, it done database side - just pass the batch through

        def cooccurrence_write_function(batch, cursor, conn, logger):
            """Writes a batch of entity co-occurrences to the database using executemany."""
            logger.info(f"Writing batch of {len(batch)} co-occurrences to the database.")
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
        total_count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_DOCS}").fetchone()[0]

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

    def record_dis_pnm(self, batch_size=500000, num_reader_threads=32, writer_chunking=200, max_queue_size=1000) -> bool:
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
        idx_ne_disease = Index(TABLE_NE, [CLASS_ID, DOC_ID, NE_NORM_ID],
                            where=f"{CLASS_ID}=1 AND {NE_NORM_ID} IS NOT NULL",
                            logger=self.logger)


        # Composite index for protein/molecule entities filtering (NE_CLASS_ID=2)
        idx_ne_pnm = Index(TABLE_NE, [CLASS_ID, DOC_ID, NE_NORM_ID],
                        where=f"{CLASS_ID}=2 AND {NE_NORM_ID} IS NOT NULL",
                        logger=self.logger)

        # Index for joining dis_entities and pnm_entities on DOC_ID
        idx_ne_doc_id = Index(TABLE_NE, [DOC_ID], logger=self.logger)

        any_index_created = False
        for idx in [idx_ne_disease, idx_ne_pnm, idx_ne_doc_id]:
            if idx.create_if_not_exists(self.cursor):
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
                self.log_query_plan(reader_query_fn().replace(':limit', '1000').replace(':offset', '100'))
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
                cursor.executemany(writer_sql, batch) # Commits are handled by the ReaderWriterPair

            # Get total document count - Filtering out already processed documents is done in the reader query. We process all documents, even if some might have been processed before.
            # Not the most efficient, but ensures that all documents are processed for now.

            total_count = self.db.statistics.document_count
            self.logger.info(f"Total documents to process for DIS-PNM co-occurrences: {total_count:,}")

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
                process_title="DIS-PNM co-occurrence extraction"
            )

            self.logger.info("Starting ReaderWriterPair for DIS-PNM co-occurrence extraction")
            rw_pair.run()
            self.logger.info("DIS-PNM co-occurrence extraction completed successfully")

            # Count and log results
            count = self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_DIS_PNM}").fetchone()[0]
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
            duplicate_count = self.cursor.execute(f"""--sql
                SELECT COUNT(*) FROM (
                    SELECT {E1_ID}, {E2_ID}, COUNT(*) as cnt
                    FROM {TABLE_DIS_PNM}
                    GROUP BY {E1_ID}, {E2_ID}
                    HAVING cnt > 1
                )
            """).fetchone()[0]

            if duplicate_count > 0:
                self.logger.error(f"Found {duplicate_count} duplicate DIS-PNM co-occurrences!")

            # Check for missing entity references
            invalid_refs = self.cursor.execute(f"""--sql
                SELECT COUNT(*) FROM {TABLE_DIS_PNM} dp
                LEFT JOIN {TABLE_NE} ne1 ON dp.{E1_ID} = ne1.{NE_PRIMARY_ID}
                LEFT JOIN {TABLE_NE} ne2 ON dp.{E2_ID} = ne2.{NE_PRIMARY_ID}
                WHERE ne1.{NE_PRIMARY_ID} IS NULL OR ne2.{NE_PRIMARY_ID} IS NULL
            """).fetchone()[0]

            if invalid_refs > 0:
                self.logger.error(f"Found {invalid_refs} DIS-PNM co-occurrences with invalid entity references!")

            return duplicate_count == 0 and invalid_refs == 0

        except Exception as e:
            self.logger.error(f"Error during DIS-PNM validation: {e}")
            return False
    def count_entity_cooccurrences_multithreaded(self, level: str = "document", batch_size=5000, num_reader_threads=32) -> None:
        """
        Counts entity co-occurrences using ReaderWriterPair.
        Accessed via db_system.entity_cooccurrence.count_entity_cooccurrences_multithreaded()
        """
        raise NotImplementedError("Multithreaded co-occurrence counting not yet implemented in this structure.")


    def co_aggregate_old(self, batch_size=50000, ignore_entities_with_error_codes: bool = True) -> None:
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



