# entity_cooccurrence_module.py
import sqlite3
from .schema import *

class SchemaManager:
    def __init__(self):
        self.logger = None
        self.cursor = None
        self.conn = None
        self.conn_params_dict = None

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
                        SELECT
                            ne1.named_entity as type1,
                            ne2.named_entity as type2,
                            COUNT(*) as pair_count
                        FROM temp_new_cooccurrences t
                        JOIN entity_occurrences e1 ON e1.id = t.e1_id
                        JOIN entity_occurrences e2 ON e2.id = t.e2_id
                        JOIN named_entities ne1 ON ne1.id = e1.entity_id
                        JOIN named_entities ne2 ON ne2.id = e2.entity_id
                        GROUP BY ne1.named_entity, ne2.named_entity
                        ORDER BY pair_count DESC
                        LIMIT 5
                    )
                    SELECT * FROM new_pairs
                    """
                )
                type_stats = cursor.fetchall()
                logger.info("\nTop entity type pairs:")
                for type1, type2, count in type_stats:
                    logger.info(f"  {type1} - {type2}: {count:,} pairs")
            cursor.execute(
                """
                SELECT
                    COUNT(*) as total_pairs,
                    (SELECT COUNT(DISTINCT entity_id)
                        FROM entity_occurrences
                        WHERE id IN (SELECT e1_id FROM entity_cooccurrences
                                            UNION
                                            SELECT e2_id FROM entity_cooccurrences)) as total_entities,
                    COALESCE(AVG(sentence_distance), 0) as avg_distance
                FROM entity_cooccurrences
                """
            )
            total_stats = cursor.fetchone()
            logger.info(
                f"\nCo-occurrence identification complete:"
                f"\n- Total unique pairs: {total_stats[0]:,}"
                f"\n- Unique entities involved: {total_stats[1]:,}"
                + (
                    f"\n- Average sentence distance: {total_stats[2]:.2f}"
                    if level_local == "sentence"
                    else ""
                )
            )
            return None

        base_executor.execute_operation(lambda conn: operation(conn, level),)


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
        db_manager = self.db_system.db_manager
        logger = self.db_system.core_logger
        base_executor = BaseExecutor(db_manager, logger)

        def operation(conn):
            cursor = conn.cursor()
            logger.info("Starting cooccurrence aggregation...")
            # ... (rest of the original operation logic - same as before) ...
            query = f"""
                CREATE TEMPORARY TABLE tmp_cooccurrences AS
                WITH normalized_pairs AS (
                    SELECT
                        CASE WHEN eo1.summary_id <= eo2.summary_id
                               THEN eo1.summary_id
                               ELSE eo2.summary_id END AS e1_id_normalized,
                        CASE WHEN eo1.summary_id <= eo2.summary_id
                               THEN eo2.summary_id
                               ELSE eo1.summary_id END AS e2_id_normalized,
                        eo1.document_id,
                        CASE WHEN eo1.sentence_index = eo2.sentence_index THEN 1 ELSE 0 END as same_sentenc
                        FROM entity_cooccurrences eo1
                        JOIN entity_occurrences eo2 ON eo1.cooccurrence_id = eo2.cooccurrence_id AND eo1.id < eo2.id
                        LEFT JOIN entity_occurrences_summary eos1 ON eo1.summary_id = eos1.id
                        LEFT JOIN entity_occurrences_summary eos2 ON eo2.summary_id = eos2.id
                    WHERE eos1.error_code IS NULL AND eos2.error_code IS NULL

                )
                SELECT
                    e1_id_normalized,
                    e2_id_normalized,
                    COUNT(*) as cooccurrence_count,
                    SUM(same_sentenc) as same_sentence_count,
                    COUNT(DISTINCT document_id) as document_count
                FROM normalized_pairs
                GROUP BY e1_id_normalized, e2_id_normalized
                """
            cursor.execute(query)
            insert_query = """
                INSERT INTO entity_cooccurrences_aggregated (e1_id_normalized, e2_id_normalized, cooccurrence_count, same_sentence_count, document_count)
                SELECT e1_id_normalized, e2_id_normalized, cooccurrence_count, same_sentence_count, document_count
                FROM tmp_cooccurrences
                ON CONFLICT (e1_id_normalized, e2_id_normalized) DO UPDATE SET
                    cooccurrence_count = cooccurrence_count + excluded.cooccurrence_count,
                    same_sentence_count = same_sentence_count + excluded.same_sentence_count,
                    document_count = document_count + excluded.document_count;
                """
            cursor.execute(insert_query)
            logger.info("Co-occurrence aggregation complete.")
            return None

        base_executor.execute_operation(operation)