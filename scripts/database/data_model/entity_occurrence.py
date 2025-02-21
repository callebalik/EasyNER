# entity_cooccurrence_module.py
import sqlite3

from scripts.database.core.core_classes import BaseExecutor


class EntityOccurence:
    """
    EntityCooccurence module class, to be integrated into DatabaseSystem.
    """
    def __init__(self, db_system_instance):
        self.db_system = db_system_instance

    def generate_cooccurrence_matrix(self, target_table="entity_occurrences_summary", batch_size=10000):
        """
        Generates co-occurrence matrix.
        Accessed via db_system.entity_cooccurrence.generate_cooccurrence_matrix()
        """
        pass # Placeholder - No changes requested here

    def count_entity_cooccurrences(self, level: str = "document", ignore_error_occurrencess: bool = True) -> None:
        """
        Counts entity co-occurrences at document or sentence level.
        Accessed via db_system.entity_cooccurrence.count_entity_cooccurrences()
        """
        db_manager = self.db_system.db_manager
        logger = self.db_system.core_logger
        base_executor = BaseExecutor(db_manager, logger)

        def operation(conn, level_local=level):
            cursor = conn.cursor()
            if level_local not in ["document", "sentence"]:
                raise ValueError("Level must be either 'document' or 'sentence'")

            logger.info(
                f"Starting entity co-occurrence identification at {level_local} level..."
            )
            # ... (rest of the original operation logic - same as before) ...
            cursor.execute("DROP TABLE IF EXISTS temp_new_cooccurrences")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_new_cooccurrences (
                    e1_id INTEGER NOT NULL CHECK (e1_id <= e2_id),
                    e2_id INTEGER NOT NULL,
                    sentence_distance INTEGER,
                    CHECK (e1_id <= e2_id)
                )
                """
            )
            base_query = f"""
                INSERT INTO temp_new_cooccurrences (e1_id, e2_id, sentence_distance)
                SELECT DISTINCT
                    e1.id,  -- e1.id is <= e2.id due to JOIN condition, ensuring canonical order
                    e2.id,
                    {f"ABS(e1.sentence_index - e2.sentence_index)" if level_local == "sentence" else "NULL"}
                FROM entity_occurrences e1
                JOIN entity_occurrences e2 ON
                    e1.document_id = e2.document_id AND
                    e1.id <= e2.id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM entity_cooccurrences ec
                    WHERE ec.e1_id = e1.id AND ec.e2_id = e2.id
                )
                AND (e1.overlap = FALSE OR e1.overlap IS NULL)
                AND (e2.overlap = FALSE OR e2.overlap IS NULL)
                {f"AND ABS(e1.sentence_index - e2.sentence_index) <= 5" if level_local == "sentence" else ""}
                """
            cursor.execute(base_query)
            cursor.execute("SELECT COUNT(*) FROM temp_new_cooccurrences")
            new_count = cursor.fetchone()[0]
            logger.info(f"Found {new_count:,} new co-occurrences")
            if new_count > 0:
                logger.info("Recording new co-occurrences using batch insertions...")
                BATCH_SIZE = 10_000
                offset = 0
                while True:
                    cursor.execute(
                        f"""
                        INSERT INTO entity_cooccurrences (
                            e1_id, e2_id, overlap, sentence_distance
                        )
                        SELECT
                            e1_id,
                            e2_id,
                            FALSE as overlap,
                            sentence_distance
                        FROM temp_new_cooccurrences
                        LIMIT {BATCH_SIZE} OFFSET {offset}
                        """
                    )
                    batch_rows = cursor.rowcount
                    if batch_rows == 0:
                        break
                    offset += BATCH_SIZE
                cursor.execute(
                    """
                    WITH new_pairs AS (
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