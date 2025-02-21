# named_entity_module.py
import sqlite3

from scripts.database.core.core_classes import BaseExecutor


class NamedEntity:
    """
    NamedEntity module class, to be integrated into DatabaseSystem.
    """
    def __init__(self, db_system_instance):
        self.db_system = db_system_instance

    def count_entity_class_fq(self):
        """
        Counts the frequency of each named entity.
        Accessed via db_system.named_entity.count_entity_class_fq()
        """
        db_manager = self.db_system.db_manager
        logger = self.db_system.core_logger
        base_executor = BaseExecutor(db_manager, logger)

        def operation(conn):
            cursor = conn.cursor()
            logger.info("Counting named entity frequencies...")
            # ... (rest of the original operation logic - same as before) ...
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_entity_counts AS
                SELECT entity_id, COUNT(*) AS entity_count
                FROM entity_occurrences
                WHERE error_id IS NULL
                GROUP BY entity_id;
                """
            )
            cursor.execute(
                """
                UPDATE named_entities
                SET fq = (SELECT entity_count FROM temp_entity_counts WHERE temp_entity_counts.entity_id = named_entities.id);
                """
            )
            cursor.execute("DROP TABLE temp_entity_counts;")
            logger.info(
                "Named entity frequencies updated in named_entities table."
            )
            return None

        base_executor.execute_operation(operation)