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
            eco.e1_id_normalized,
            eco.e2_id_normalized,
            nea1.{TXT_NORM} as e1_txt_norm,
            nea2.{TXT_NORM} as e2_txt_norm,
            eco.fq_document_level,
            eco.fq_sentence_level,
            eco.uniq_documents,
            eco.pmi
        FROM eco_aggregated_deprecated eco
        JOIN {TABLE_NE_AGGR} nea1
            ON eco.e1_id_normalized = nea1.norm_id
        JOIN eo_aggr nea2
            ON eco.e2_id_normalized = nea2.norm_id
        """

        self.stmt_view_deprecated_pnm_dis = f"""--sql
        CREATE VIEW IF NOT EXISTS view_deprecated_pnm_dis AS
        SELECT
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
                            WHERE ec.e1_id = e1.id AND ec.e2_id = e2.id
                        )
                    )
                    SELECT -- Return the final distinct pair
                        p.e1_id,
                        p.e2_id
                                {", (SELECT ABS(e1." + {SENT_IDX} + " - e2." + {SENT_IDX} + ") FROM doc_entities e1 JOIN doc_entities e2 ON e1.id = p.e1_id AND e2.id = p.e2_id) AS sentence_distance" if level == "sentence" else ""}
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
                    INSERT INTO {TABLE_COOCCURRENCES} (e1_id, e2_id {", sentence_distance" if level == "sentence" else ""})
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



