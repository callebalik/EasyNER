# entity_cooccurrence_module.py
import sqlite3
from .schema import *
import logging


class BaseComponent:
    """ Common base class for all components with shared logger and database connection. """
    def init_deps(self, db_system):
        self.logger = db_system.logger
        self.cursor = db_system.cursor
        self.conn =  db_system.conn
        self.conn_params_dict = db_system.conn_params_dict
        self.parent = db_system
        return self

class SchemaManager(BaseComponent):
    def __init__(self, parent):
        super().init_deps(parent)

        self.stmt_table_entity_cooccurrences = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_COOCCURRENCES} (
                    {E1_ID} INTEGER NOT NULL,
                    {E2_ID} INTEGER NOT NULL,
                    {COL_CO_SENT_DIST} INTEGER,
                    {COL_CO_AGGR_ID} INTEGER,
                    PRIMARY KEY ({E1_ID}, {E2_ID})
                    FOREIGN KEY ({E1_ID}) REFERENCES {TABLE_NE}({NE_PRIMARY_ID}),
                    FOREIGN KEY ({E2_ID}) REFERENCES {TABLE_NE}({NE_PRIMARY_ID}),
                    FOREIGN KEY ({COL_CO_AGGR_ID}) REFERENCES {TABLE_CO_AGGR}({BACKLINK_FOR_CO_OCCURRENCES})
                )
            """
        self.stmt_table_entity_cooccurrences_aggregated = f"""--sql
            CREATE TABLE IF NOT EXISTS {TABLE_CO_AGGR} (
                {E1_NORM_ID} INTEGER NOT NULL,
                {E2_NORM_ID} INTEGER NOT NULL,
                {FQ_DOCUMENT_LEVEL} INTEGER DEFAULT NULL,
                {FQ_SENTENCE_LEVEL} INTEGER DEFAULT NULL,
                {UNIQ_DOCS} INTEGER DEFAULT NULL,
                {PMI} REAL DEFAULT NULL,
                PRIMARY KEY ({E1_NORM_ID}, {E2_NORM_ID}),
                FOREIGN KEY ({E1_NORM_ID}) REFERENCES {TABLE_NE_AGGR}({NEA_PRIMARY_ID}),
                FOREIGN KEY ({E2_NORM_ID}) REFERENCES {TABLE_NE_AGGR}({NEA_PRIMARY_ID})
            )
        """
        self.stmt_view_cooccurrences = f"""--sql
            CREATE VIEW IF NOT EXISTS {VIEW_COOCCURRENCES} AS
            SELECT
                {E1_ID},
                {E2_ID},
                {COL_CO_SENT_DIST},
                {COL_CO_AGGR_ID},
                nea1.{COL_NE_TXT_NORM} as e1_norm,
                nea2.{COL_NE_TXT_NORM} as e2_norm,
                ne1.{COL_NE_CLASS_ID} as e1_class,
                ne2.{COL_NE_CLASS_ID} as e2_class,
                ne1.{COL_NE_DOC_ID} as e1_doc_id,
                ne2.{COL_NE_DOC_ID} as e2_doc_id
            FROM {TABLE_COOCCURRENCES} co
            JOIN {TABLE_NE} ne1 ON co.e1_id = ne1.id
            JOIN {TABLE_NE} ne2 ON co.e2_id = ne2.id
            JOIN {TABLE_NE_AGGR} nea1 ON ne1.{COL_NE_AGGREGATED_ID} = nea1.norm_id
            JOIN {TABLE_NE_AGGR} nea2 ON ne2.{COL_NE_AGGREGATED_ID} = nea2.norm_id
        """
        self.stmt_view_cooccurrences_aggregated = f"""--sql
            CREATE VIEW IF NOT EXISTS {VIEW_COOCCURRENCES_AGGREGATED} AS
            SELECT
                coa.e1_id,
                coa.e2_id,
                coa.fq_document_level,
                coa.fq_sentence_level,
                coa.uniq_docs,
                coa.pmi,
                nea1.{COL_NE_TXT_NORM} as e1_norm,
                nea2.{COL_NE_TXT_NORM} as e2_norm,
                nea1.{COL_NE_CLASS_ID} as e1_class,
                nea2.{COL_NE_CLASS_ID} as e2_class
            FROM {TABLE_CO_AGGR} coa
            JOIN {TABLE_NE_AGGR} nea1 ON coa.e1_id = nea1.norm_id
            JOIN {TABLE_NE_AGGR} nea2 ON coa.e2_id = nea2.norm_id

        """
        self.stmt_view_cooccurrences_stats = f"""--sql
        CREATE VIEW IF NOT EXISTS {VIEW_COOCCURRENCES}_stats AS
        SELECT
            CASE
                WHEN ne1.{NE_NORM_ID} < ne2.{NE_NORM_ID} THEN ne1.id
                ELSE ne2.id
            END AS e1_id,  -- Still use original e1_id for joining, but canonicalize normalized IDs
            CASE
                WHEN ne1.{NE_NORM_ID} < ne2.{NE_NORM_ID} THEN ne2.id
                ELSE ne1.id
            END AS e2_id,  -- Still use original e2_id for joining, but canonicalize normalized IDs
            CASE
                WHEN ne1.{NE_NORM_ID} < ne2.{NE_NORM_ID} THEN ne1.{NE_NORM_ID}
                ELSE ne2.{NE_NORM_ID}
            END AS e1_norm_id, -- Canonicalized normalized e1_norm_id
            CASE
                WHEN ne1.{NE_NORM_ID} < ne2.{NE_NORM_ID} THEN ne2.{NE_NORM_ID}
                ELSE ne1.{NE_NORM_ID}
            END AS e2_norm_id, -- Canonicalized normalized e2_norm_id
            coa.fq_document_level as fq_document_level,
            coa.fq_sentence_level as fq_sentence_level,
            coa.uniq_docs as uniq_docs,
            coa.pmi as pmi,
            ne1.{COL_NE_DOC_ID} as doc_id,
            ne1.{NE_SENT_IDX} as sent_idx_1,
            ne2.{NE_SENT_IDX} as sent_idx_2
        FROM {TABLE_CO_AGGR} coa
        JOIN {TABLE_COOCCURRENCES} co ON coa.e1_id = co.e1_id AND coa.e2_id = co.e2_id
        JOIN {TABLE_NE} ne1 ON co.e1_id = ne1.id
        JOIN {TABLE_NE} ne2 ON co.e2_id = ne2.id
        """

        """
        Create view with compiled entity information for easy access and computations
        Entity class is not column dependant in the coocurrences tables
        Here we get any combo of 1 DIS and 1 PNM and cast the DIS id as e1_id and PNM as e2_id
        """
        self.stmt_view_dis_pnm = f"""--sql
        CREATE VIEW IF NOT EXISTS {VIEW_DIS_PNM} AS
        SELECT
            CASE
                WHEN ne1.{COL_NE_CLASS_ID} = 1 THEN coa.e1_id
                ELSE coa.e2_id
            END AS e1_id, -- DIS entity ID
            nea1.{COL_NE_TXT_NORM} as DIS,
            CASE
                WHEN ne1.{COL_NE_CLASS_ID} = 1 THEN coa.e2_id
                ELSE coa.e1_id
            END AS e2_id, -- PNM entity ID
            nea2.{COL_NE_TXT_NORM} as PNM,
            coa.fq_document_level as fq_document_level,
            coa.fq_sentence_level as fq_sentence_level,
            coa.uniq_docs as uniq_docs,
            coa.pmi as pmi
        FROM {TABLE_CO_AGGR} coa
        JOIN {TABLE_NE_AGGR} nea1 ON coa.e1_id = nea1.norm_id -- Join on original e1_id on normalized entities primary key norm_id
        JOIN {TABLE_NE_AGGR} nea2 ON coa.e2_id = nea2.norm_id
        JOIN {TABLE_NE} ne1 ON coa.e1_id = ne1.{NE_NORM_ID} -- Join on original e1_id
        JOIN {TABLE_NE} ne2 ON coa.e2_id = ne2.{NE_NORM_ID}  -- Join on original e2_id
        WHERE
            (ne1.{COL_NE_CLASS_ID} = 1 AND ne2.{COL_NE_CLASS_ID} = 2
            OR
            ne1.{COL_NE_CLASS_ID} = 2 AND ne2.{COL_NE_CLASS_ID} = 1
        )
        """
        self.stmt_materialized_table_pnm_dis = self.stmt_view_dis_pnm.replace("CREATE VIEW", "CREATE TABLE").replace(f"{VIEW_DIS_PNM}", f"{TABLE_DIS_PNM}")
        self.stmt_view_dis_pnm = f"""--sql
            CREATE VIEW IF NOT EXISTS {VIEW_DIS_PNM} AS
            SELECT DISTINCT -- Keep DISTINCT for safety
                CASE
                    WHEN nea1.{COL_NE_CLASS_ID} = 1 THEN coa.e1_id
                    ELSE coa.e2_id
                END AS e1_id, -- DIS entity ID
                nea1.{COL_NE_TXT_NORM} as DIS,
                CASE
                    WHEN nea1.{COL_NE_CLASS_ID} = 1 THEN coa.e2_id
                    ELSE coa.e1_id
                END AS e2_id, -- PNM entity ID
                nea2.{COL_NE_TXT_NORM} as PNM,
                coa.fq_document_level as fq_document_level,
                coa.fq_sentence_level as fq_sentence_level,
                coa.uniq_docs as uniq_docs,
                coa.pmi as pmi
            FROM {TABLE_CO_AGGR} coa
            JOIN {TABLE_NE_AGGR} nea1 ON coa.e1_id = nea1.norm_id
            JOIN {TABLE_NE_AGGR} nea2 ON coa.e2_id = nea2.norm_id
            WHERE
                (nea1.{COL_NE_CLASS_ID} = 1 AND nea2.{COL_NE_CLASS_ID} = 2
                OR
                nea1.{COL_NE_CLASS_ID} = 2 AND nea2.{COL_NE_CLASS_ID} = 1
                )
            """

    def setup_tables(self):
        """
        Create tables for entity co-occurrence if they do not
        already exist.
        """
        self.logger.info("Setting up tables for entity co-occurrence analysis...")

        self.cursor.execute(self.stmt_table_entity_cooccurrences)
        self.cursor.execute(self.stmt_table_entity_cooccurrences_aggregated)
        self.conn.commit()

        self.logger.info("Tables created successfully.")

    def setup_views(self):
        """
        Drop and recreate the view for entity co-occurrences.
        """
        self.logger.info("Setting up views for entity co-occurrence analysis...")

        self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_COOCCURRENCES}")
        self.cursor.execute(self.stmt_view_cooccurrences)

        self.conn.commit()

        self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_COOCCURRENCES_AGGREGATED}")
        self.cursor.execute(self.stmt_view_cooccurrences_aggregated)
        self.conn.commit()

        self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_COOCCURRENCES}_stats")
        self.cursor.execute(self.stmt_view_cooccurrences_stats)
        self.conn.commit()

        self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_DIS_PNM}")
        self.cursor.execute(self.stmt_view_dis_pnm)
        self.conn.commit()

        self.cursor.execute(f"DROP VIEW IF EXISTS view_eco_deprecated")
        self.conn.commit()
            # Add view for deprecated co-occurrence aggregated table with normalized text
        self.stmt_view_eco_deprecated = f"""--sql
        CREATE VIEW IF NOT EXISTS view_eco_deprecated AS
        SELECT
            eco.e1_id_normalized,
            eco.e2_id_normalized,
            nea1.{COL_NE_TXT_NORM} as e1_txt_norm,
            nea2.{COL_NE_TXT_NORM} as e2_txt_norm,
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

class Analysis:
    """
    Main analysis engine for co-occurrence analysis.
    Assumes that the tables and views have been set up.

    Process:
    1. Record entity co-occurrences
    2. Aggregate entity co-occurrences into a summary table
    3.

    """
    def __init__(self):
        self.logger = None
        self.cursor = None
        self.conn = None
        self.conn_params_dict = None

    def record_entity_cooccurrences_multithreaded(
        self, level: str = "document", batch_size=20000, num_reader_threads=32
    ) -> None:
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
                            ne.{NE_SENT_IDX},
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
                            {"AND ABS(e1." + {NE_SENT_IDX} + "- e2." + {NE_SENT_IDX} + ") <= 5" if level == "sentence" else ""}
                        WHERE NOT EXISTS ( -- Do not include existing co-occurrences
                            SELECT 1
                            FROM {TABLE_COOCCURRENCES} ec
                            WHERE ec.e1_id = e1.id AND ec.e2_id = e2.id
                        )
                    )
                    SELECT -- Return the final distinct pair
                        p.e1_id,
                        p.e2_id
                        {", (SELECT ABS(e1." + {NE_SENT_IDX} + " - e2." + {NE_SENT_IDX} + ") FROM doc_entities e1 JOIN doc_entities e2 ON e1.id = p.e1_id AND e2.id = p.e2_id) AS sentence_distance" if level == "sentence" else ""}
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

        self.logger.info(
            f"Starting ReaderWriterPair to count entity co-occurrences at {level} level."
        )
        rw_pair.run()
        self.logger.info(
            f"ReaderWriterPair process finished for {level} level co-occurrence counting."
        )

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
    def __init__(self, db_system_instance):
        self.db_system = db_system_instance

        self.schema_manager = SchemaManager(self)
        self.analysis = Analysis(self)
        self.statistics = Statistics(self)
        self.tests = Tests(self, self.statistics)

    @property
    def conn(self): # Good - Always refers to the current connection
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
        """
        Initialize the co-occurrence system by setting up tables and views
        """
        self.schema_manager.setup_tables()
        self.schema_manager.setup_views()
        
    def record_entity_cooccurrences_multithreaded(self, *args, **kwargs):
        """
        Delegate to analysis component.
        """
        if self.analysis.record_entity_cooccurrences_multithreaded(*args, **kwargs):
            self.tests.has_self_references()
            self.tests.has_duplicates()

    def update_unique_document_counts(self):
        """
        Delegate to analysis component
        """
        return self.analysis.update_unique_document_counts()



