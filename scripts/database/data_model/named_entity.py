# named_entity_module.py
import sqlite3

from scripts.database.core.core_classes import BaseExecutor

class NamedEntity:
    def __init__(self, conn, cursor, logger):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger

    def count_entity_class_fq(self):
        """
        Counts the frequency of each named entity in the entity_occurrences table
        and updates the 'fq' column in the named_entities table.
        By defaults fq excludes entitiy occurrences with an error_id != Null.
        """
        try:
            self.logger.info("Counting named entity frequencies...")
            # Create temporary table for entity counts
            self.cursor.execute(
                """--sql
                CREATE TEMPORARY TABLE temp_entity_counts AS
                SELECT entity_id, COUNT(*) AS entity_count
                FROM entity_occurrences
                WHERE error_id IS NULL
                GROUP BY entity_id;
                """
            )

            # Update named_entities table with counts from temporary table
            self.cursor.execute(
                """
                UPDATE named_entities
                SET fq = (SELECT entity_count FROM temp_entity_counts WHERE temp_entity_counts.entity_id = named_entities.id);
                """
            )

            # Drop the temporary table
            self.cursor.execute("DROP TABLE temp_entity_counts;")

            self.conn.commit()
            self.logger.info(
                "Named entity frequencies updated in named_entities table."
            )
        except sqlite3.Error as e:
            self.logger.error(f"Error counting named entity frequencies: {e}")




