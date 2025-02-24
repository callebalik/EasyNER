TABLE_NE = "eo"
TABLE_NE_CLASS = "named_entities"
TABLE_NE_LOOKUP = TABLE_NE + "_lookup"
TABLE_NE_AGGR = TABLE_NE + "_" + "aggr"
DIS_stem = "DIS"
PNM_stem = "PNM"
TABLE_NE_DIS = TABLE_NE + "_" + DIS_stem
TABLE_NE_PNM = TABLE_NE + "_" + PNM_stem

TABLE_DOCS = "documents"
TABLE_SENTENCES = "sentences"
TABLE_ERROR = "eo_error_codes"

COL_NE_CLASS_ID = "entity_id"  # Named entity text column name
COL_NE_CLASS_NAME = "named_entity"  # Named entity text column name
COL_NE_TXT = "entity_text"  # Named entity text column name
COL_NE_TXT_NORM = "txt_norm"  # Normalized named entity text column name
COL_NE_DOC_ID = "document_id"  # Document ID column name
COL_NE_SENT_IDX = "sentence_index"  # Sentence index column name
COL_NE_SPAN_START = "span_start"  # Span start column name
COL_NE_SPAN_END = "span_end"  # Span end column name
COL_NE_ERROR_ID = "error_id"  # Error ID column name
COL_NE_OVERLAP = "overlap"  # Overlap column name
COL_NE_NORM_ID = "norm_id"  # Normalized ID column name
COL_NE_AGGREGATED_ID = "norm_id"  # Aggregated ID column name
COL_NE_FQ = "fq"  # Frequency column name
COL_NE_DOC_COUNT = "doc_count"  # Document count column name

VIEW_NE = "view_ne"
VIEW_NE_RAW = "view_ne_raw"

import math
import sqlite3

import numpy as np
from tqdm import tqdm

from scripts.database.db_main import EasyNerDBHandler

from ..core.db_engine import ReaderWriterPair
import pandas as pd


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
                """
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


class EntityOccurrence:
    def __init__(self, conn, cursor, logger, log_query_plan, conn_params_dict):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger
        self.log_query_plan = log_query_plan
        self.conn_params_dict = conn_params_dict

        self.stmt_table_ne = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE} (
                    id INTEGER PRIMARY KEY,
                    {COL_NE_TXT} TEXT,
                    {COL_NE_ERROR_ID} VARCHAR(20),
                    {COL_NE_CLASS_ID} INTEGER,
                    {COL_NE_DOC_ID} INTEGER,
                    {COL_NE_SENT_IDX} INTEGER,
                    {COL_NE_AGGREGATED_ID} INTEGER,
                    {COL_NE_OVERLAP} BOOLEAN,
                    {COL_NE_SPAN_START} INTEGER,
                    {COL_NE_SPAN_END} INTEGER,
                    FOREIGN KEY ({COL_NE_DOC_ID}) REFERENCES {TABLE_DOCS} (id),
                    FOREIGN KEY ({COL_NE_DOC_ID},{COL_NE_SENT_IDX}) REFERENCES {TABLE_SENTENCES} ({COL_NE_DOC_ID},{COL_NE_SENT_IDX}),
                    FOREIGN KEY ({COL_NE_CLASS_ID}) REFERENCES {TABLE_NE_CLASS} (id),
                    FOREIGN KEY ({COL_NE_AGGREGATED_ID}) REFERENCES {TABLE_NE_AGGR} ({COL_NE_AGGREGATED_ID}),
                    FOREIGN KEY ({COL_NE_ERROR_ID}) REFERENCES {TABLE_ERROR} ({COL_NE_ERROR_ID})
                );
                """

        self.stmt_table_ne_aggregated = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_AGGR} (
                    {COL_NE_NORM_ID} INTEGER PRIMARY KEY,
                    {COL_NE_CLASS_ID} INTEGER,
                    {COL_NE_TXT_NORM} TEXT,
                    UNIQUE ({COL_NE_TXT_NORM}, {COL_NE_CLASS_ID}) -- Probably not needed
                );
                """

        self.stmt_table_ne_lookup = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_LOOKUP} (
                    id INTEGER PRIMARY KEY,
                    {COL_NE_CLASS_ID} INTEGER,
                    {COL_NE_TXT_NORM} TEXT,
                    {COL_NE_NORM_ID} INTEGER,
                    FOREIGN KEY (id) REFERENCES {TABLE_NE} (id),
                    FOREIGN KEY ({COL_NE_NORM_ID}) REFERENCES {TABLE_NE_AGGR} ({COL_NE_NORM_ID})
                    )"""

    def create_entity_occurrences_table(self):
        """
        Create the entity_occurrences table.
        """
        self.cursor.execute(self.stmt_table_ne)


    def migrate_old_entity_occurences_table(self):
        """
        Migrate old entity_occurrences table to new table structure.
        """
        self.cursor.execute(
            f"""--sql
            INSERT INTO {TABLE_NE}
            SELECT *
            FROM eo_old
            """
        )
    def identify_overlap(self, overwrite: bool = False) -> None:
        """
        Find entities in the same sentence where span_start and span_end overlap between the two entities.
        Record into new column of TABLE entity_occurrences [overlap: boolean].
        """
        try:
            # Add overlap column if it doesn't exist
            self.cursor.execute(
                """
                SELECT COUNT(*) 
                FROM pragma_table_info('entity_occurrences') 
                WHERE name='overlap'
            """
            )
            if self.cursor.fetchone()[0] == 0:
                self.cursor.execute(
                    "ALTER TABLE entity_occurrences ADD COLUMN overlap BOOLEAN DEFAULT FALSE"
                )

            if overwrite:
                self.cursor.execute("UPDATE entity_occurrences SET overlap = FALSE")

            self.cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_entity_occurrences_document_id 
                ON entity_occurrences(document_id, sentence_index, id)
                """
            )

            self.logger.info("Finding overlapping entities...")

            # Find overlapping entities within the same sentence
            self.cursor.execute(
                """
                WITH overlapping_pairs AS (
                    SELECT DISTINCT
                        e1.id as id1,
                        e2.id as id2
                    FROM entity_occurrences e1
                    JOIN entity_occurrences e2 ON 
                        e1.document_id = e2.document_id AND
                        e1.sentence_index = e2.sentence_index AND
                        e1.id < e2.id
                    JOIN entity_occurrence_spans s1 ON s1.id = e1.id
                    JOIN entity_occurrence_spans s2 ON s2.id = e2.id
                    WHERE 
                        e1.overlap = FALSE AND
                        e2.overlap = FALSE AND
                        NOT (
                            s1.span_end <= s2.span_start OR
                            s2.span_end <= s1.span_start
                        )
                )
                UPDATE entity_occurrences
                SET overlap = TRUE
                WHERE id IN (
                    SELECT id1 FROM overlapping_pairs
                    UNION ALL
                    SELECT id2 FROM overlapping_pairs
                )
            """
            )

            # Get statistics about overlapping entities
            self.cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_entities,
                    SUM(CASE WHEN overlap THEN 1 ELSE 0 END) as overlapping_entities,
                    COUNT(DISTINCT document_id) as affected_documents,
                    COUNT(DISTINCT sentence_index) as affected_sentences
                FROM entity_occurrences
                WHERE overlap = TRUE
            """
            )
            stats = self.cursor.fetchone()

            self.conn.commit()
            self.logger.info(
                f"Found {stats[1]} overlapping entities across {stats[2]} documents and {stats[3]} sentences"
            )

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error while finding overlapping entities: {e}")
            raise

    """
    This manages the creation of lookup tables for entity_occurrences.
    Basically rudimentary linking of entities to their normalized forms.
    """

    def generate_lookup_table_for_normalized_entities(
        self, target_table=eo_lookup_table_name
    ):
        """
        Create a temporary table with normalized entity text and entity_id.
        """
        try:
            self.logger.info("Creating temporary normalized entities table...")

            # Drop temporary table if it exists
            # self.cursor.execute("DROP TABLE IF EXISTS temp_normalized_linked_entities")

            # Entity Linking Lookup Table
            create_table = f"""
                CREATE TABLE {target_table} (
                    id INTEGER,
                    entity_id INTEGER,
                    normalized_entity_text TEXT,
                    FOREIGN KEY (id) REFERENCES entity_occurrences(id)
                    )"""
            self.cursor.execute(create_table)

            self.logger.info("Temporary normalized entities table created.")

            query_string = f"""
                INSERT INTO {eo_lookup_table_name} (id, entity_id, normalized_entity_text)    
                SELECT 
                    id,
                    entity_id,
                    LOWER(entity_text) as normalized_entity_text
                FROM entity_occurrences
                WHERE entity_text IS NOT NULL
                AND overlap = FALSE
                AND error_id IS NULL
                """

            explain_query = f"EXPLAIN QUERY PLAN {query_string}"
            self.cursor.execute(explain_query)
            self.logger.debug(
                f"Query plan for entity normalization: {str(self.cursor.fetchall())}"
            )

            # Create temporary table with normalized entities
            self.cursor.execute(query_string)

            # Check validity of normalized entities, by getting count of entities with error codes via JOIN
            self.cursor.execute(
                """
                SELECT COUNT(*)
                FROM temp_normalized_linked_entities ne
                JOIN entity_occurrences eo ON eo.id = ne.id
                WHERE eo.error_id IS NOT NULL
                """
            )
            error_count = self.cursor.fetchone()[0]

            self.cursor.execute(
                """
                SELECT COUNT(*)
                FROM temp_normalized_linked_entities ne
                JOIN entity_occurrences eo ON eo.id = ne.id
                WHERE overlap IS TRUE
                """
            )
            overlap_count = self.cursor.fetchone()[0]

            if error_count | overlap_count > 0:
                self.logger.warning(
                    f"Found {error_count} entities with error codes in temp_normalized_linked_entities \n Found {overlap_count} entities with overlap flag set"
                )

            self.logger.info(
                f"Normalized entity texts prepared, {error_count} error counts, {overlap_count} overlap counts"
            )

            self.conn.commit()
            self.logger.info("Temporary normalized entities table created.")
        except sqlite3.Error as e:
            self.logger.error(
                f"Error creating temporary normalized entities table: {e}"
            )

    def validate_normalized_entities(
        self, eo_table=eo_table_name, target_table=eo_lookup_table_name
    ):
        # Use reader writer pair to validate normalized entities, i.e. all entity_ids in entiry_occurrences, filtered for error_id, and overlap, should be present in the lookup table
        self.logger.info(f"Validating normalized entities in {eo_table} against {target_table}")

        reader_query_fn = f"""
                SELECT id
                FROM {eo_table}
                WHERE error_id IS NULL
                AND overlap IS FALSE
                EXCEPT
                SELECT id
                FROM {target_table}
            """
        
        self.log_query_plan(reader_query_fn)

        def validate_process_function(batch):
            if batch:
                self.logger.error(
                    f"Batch Validation failed: Found {len(batch)} entities missing from lookup table."
                )
                return batch
            else:
                self.logger.info("Batch Validation successful: All entities are normalized.")
                return True  # Validation passed

        def validate_write_function(batch):
            if batch is True:
                return
            # Log to file in results folder

            self.logger.info(f"Writing mismatched entities to file to results folder.")
            df = pd.DataFrame(batch, columns=["entity_id"])
            df.to_csv("../results/mismatched_lookup.csv", index=False, mode="w")

        # Run the validation process
        reader_writer = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            batch_size=10000,
            num_reader_threads=32,
            reader_query=reader_query_fn,
            process_function=validate_process_function,
            write_function=validate_write_function,
            
        )

        reader_writer.run()


    def _process_text_column(self, column_name : str, table_name : str, process_query : str, process_function = None, write_function = None):

        def validate_table_column(table_name, column_name):
            """
            Check if a column exists in a table.
            """
            self.cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM pragma_table_info('{table_name}')
                WHERE name = '{column_name}'
                """
            )
            return self.cursor.fetchone()[0] == 1
        
        if not validate_table_column(table_name, column_name):
            self.logger.error(f"Column {column_name} does not exist in table {table_name}")
            return


        ReaderWriterPair(
            batch_size=20000,
            num_reader_threads=32,
            reader_query=process_query,
            process_function=validate_process_function,
            write_function=validate_write_function,
            conn_params=self.conn_params_dict,
            logger=self.logger,
        )

    def _normalize_entity_text_column(
        self,
        table_name: str = eo_lookup_table_name,
        column_name: str = "normalized_entity_text",
    ):
        """
        Normalizes a specified text column in a SQLite database table by:
        1. Removing leading whitespace.
        2. Removing the leading prefix '" ' (double quote followed by space).

        Args:
            database_file (str): Path to the SQLite database file.
            table_name (str): Name of the table containing the column to normalize.
            column_name (str): Name of the column to normalize (e.g., 'normalized_entity_text').
        """

        try:
            # 2. SQL statement to remove leading whitespace
            sql_remove_leading_whitespace = f"""
                UPDATE {table_name}
                SET {column_name} = LTRIM({column_name});
            """

            # 3. SQL statement to remove leading '" ' after removing whitespace
            sql_remove_leading_quote_space = f"""
                UPDATE {table_name}
                SET {column_name} =
                CASE
                    WHEN SUBSTR({column_name}, 1, 2) = '" ' THEN
                        SUBSTR({column_name}, 3)
                    WHEN SUBSTR({column_name}, 1, 3) = '% )' THEN
                        SUBSTR({column_name}, 4)
                    WHEN SUBSTR({column_name}, 1, 2) = '% ' THEN
                        SUBSTR({column_name}, 3)
                    ELSE
                    {column_name}
                END;
            """

            # 4. Execute the SQL statements

            print(
                f"Step 1: Removing leading whitespace from column '{column_name}' in table '{table_name}'..."
            )
            self.cursor.execute(sql_remove_leading_whitespace)
            print("Leading whitespace removal completed.")

            print(
                f"Step 2: Removing leading '\" ' from column '{column_name}' in table '{table_name}'..."
            )
            self.cursor.execute(sql_remove_leading_quote_space)
            print("Leading '\" ' removal completed.")

            # 5. Commit the changes to the database
            self.conn.commit()
            print("Changes committed to the database.")

        except sqlite3.Error as e:
            print(f"Database error: {e}")

        # class AggregatedData:
        """
        Contains methods for updating aggregated_entities table.
        """

    def generate_aggregated_table(
        self, batch_size=5000, target_table=eo_aggregated_table_name
    ):
        """
        Idempotent method to create aggregated entities from temp_normalized_linked_entities.
        """
        try:
            self.logger.info("Creating aggregated entities from temp table...")

            # Create index for faster processing of entity_occurrences
            # GROUP BY clause:
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_eo_document_id ON entity_occurrences (document_id)"
            )
            # COUNT(DISTINCT eo.document_id)
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_ne_text_entity_id ON ? (normalized_entity_text, entity_id)",
                (target_table,),
            )

            offset = 0

            while True:
                # Execute the batch insert query with WHERE NOT EXISTS
                self.cursor.execute(
                    """
                    INSERT INTO ? (normalized_entity_text, entity_id)
                    SELECT DISTINCT
                        ne.normalized_entity_text,
                        ne.entity_id
                        
                    FROM temp_normalized_linked_entities ne
                    JOIN entity_occurrences eo ON eo.id = ne.id
                    WHERE NOT EXISTS ( -- Ensure no duplicates, this filters selection on reruns 
                        SELECT 1
                        FROM ? teos
                        WHERE teos.normalized_entity_text = ne.normalized_entity_text
                        AND teos.entity_id = ne.entity_id
                    )
                    ON CONFLICT(normalized_entity_text, entity_id) DO NOTHING
                    LIMIT ? OFFSET ?;
                    """,
                    (target_table, target_table, batch_size, offset),
                )
                self.connection.commit()

                row_count = self.cursor.rowcount
                if row_count < batch_size:
                    break
                offset += batch_size

                print(
                    f"Batch processed, rows inserted/attempted: {row_count}, offset: {offset}"
                )  # Added feedback

            print("Batch processing complete with WHERE NOT EXISTS.")

            self.conn.commit()
            rows_before = self.cursor.execute("SELECT COUNT(*) FROM ?").fetchone()[0]
            rows_after = self.cursor.execute(
                "SELECT COUNT(*) FROM ?", (target_table,)
            ).fetchone()[0]
            self.logger.info(
                f"Aggregated entities created from temp table. Raw entity count: {rows_before}, Aggregated entity count: {rows_after}"
            )

        except sqlite3.Error as e:
            self.logger.error(f"Error creating aggregated entities: {e}")
            self.conn.rollback()
        except KeyboardInterrupt:
            self.conn.rollback()
            self.logger.error("Aggregated entity creation cancelled by user")

    def calc_intra_doc_fq(self) -> None:
        """
        For each entity, calculate the frequency of the entity within each document.
        """
        try:
            self.logger.info("Calculating intra-document frequencies for entities...")

            # Update intra-document frequencies
            self.cursor.execute(
                """
                WITH doc_entity_counts AS (
                    SELECT 
                        document_id,
                        summary_id,
                        COUNT(*) as freq
                    FROM entity_occurrences
                    WHERE summary_id IS NOT NULL
                    GROUP BY document_id, summary_id
                )
                UPDATE entity_occurrences
                SET intra_doc_fq = (
                    SELECT freq
                    FROM doc_entity_counts
                    WHERE doc_entity_counts.document_id = entity_occurrences.document_id
                    AND doc_entity_counts.summary_id = entity_occurrences.summary_id
                )
                WHERE summary_id IS NOT NULL
            """
            )

            # Get statistics about the update
            self.cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_entities,
                    COUNT(DISTINCT document_id) as unique_documents,
                    COUNT(DISTINCT summary_id) as unique_entities,
                    AVG(intra_doc_fq) as avg_frequency,
                    MAX(intra_doc_fq) as max_frequency
                FROM entity_occurrences
                WHERE intra_doc_fq IS NOT NULL
            """
            )
            stats = self.cursor.fetchone()

            self.conn.commit()

            self.logger.info(
                f"Intra-document frequency calculation complete:\n"
                f"- Total entity occurrences processed: {stats[0]:,}\n"
                f"- Unique documents: {stats[1]:,}\n"
                f"- Unique normalized entities: {stats[2]:,}\n"
                f"- Average intra-doc frequency: {stats[3]:.2f}\n"
                f"- Maximum intra-doc frequency: {stats[4]}"
            )

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error calculating intra-document frequencies: {e}")
            raise

    def set_aggregated_ref_id(
        self,
        overwrite: bool = False,
        batch_size=100000,
        link_lookup_table=eo_lookup_table_name,
        target_table=eo_aggregated_table_name,
    ):
        try:
            if overwrite:
                self.cursor.execute("UPDATE entity_occurrences SET summary_id = NULL")

            # Create composite index for WHERE clause
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_eo_error_summary ON entity_occurrences (error_id, summary_id)"
            )

            # Get total count of records to update
            query = f"""
                SELECT COUNT(*)
                FROM entity_occurrences eo
                WHERE eo.error_id IS NULL
                AND eo.summary_id IS NULL
                """
            self.cursor.execute(query)
            total_records = self.cursor.fetchone()[0]

            self.logger.info(
                f"Updating summary_id references for {total_records} records in batches of {batch_size}"
            )

            offset = 0
            with tqdm(
                total=total_records, desc="Updating summary_id references"
            ) as pbar:
                while offset < total_records:
                    # Update summary_ids for a batch of records
                    self.cursor.execute(
                        f"""
                        WITH to_update AS (
                            SELECT eo.id, s.id as summary_id
                            FROM entity_occurrences eo
                            JOIN {link_lookup_table} ne ON ne.id = eo.id
                            JOIN {target_table} s 
                                ON s.normalized_entity_text = ne.normalized_entity_text 
                                AND s.entity_id = ne.entity_id
                            WHERE eo.error_id IS NULL
                            AND eo.summary_id IS NULL
                            LIMIT ? OFFSET ?
                        )
                        UPDATE entity_occurrences
                        SET summary_id = (
                            SELECT summary_id 
                            FROM to_update 
                            WHERE to_update.id = entity_occurrences.id
                        )
                        WHERE id IN (SELECT id FROM to_update)
                        """,
                        (batch_size, offset),
                    )

                    updated_count = self.cursor.rowcount
                    if updated_count == 0:
                        break

                    self.conn.commit()
                    offset += batch_size
                    pbar.update(updated_count)

            self.logger.info("Summary ID update complete")

        except KeyboardInterrupt:
            self.conn.rollback()
            self.logger.error("KeyboardInterrupt detected. Operations rolled back.")
            raise
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error updating summary_id references: {e}")
            raise


class EntityCooccurence:
    def __init__(self, conn, cursor, logger):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger

    def generate_cooccurrence_matrix(
        self, target_table="entity_occurrences_summary", batch_size=10000
    ):
        """
        Generate a co-occurrence matrix for entities in the entity_occurrences_summary table.
        """
        pass

    def count_entity_cooccurrences(
        self, level: str = "document", ignore_error_occurrencess: bool = True
    ) -> None:
        """
        Updated version enforcing e1_id <= e2_id schema constraint.
        Identifies and records unique co-occurrences of named entities at the
        specified level ("document" or "sentence") and ensures that pairs are stored
        in their canonical order (smaller ID first).
        """
        if level not in ["document", "sentence"]:
            raise ValueError("Level must be either 'document' or 'sentence'")

        self.logger.info(
            f"Starting entity co-occurrence identification at {level} level..."
        )

        # Create temporary table with the same ordering constraints as main table
        self.cursor.execute("DROP TABLE IF EXISTS temp_new_cooccurrences")
        self.cursor.execute(
            """
                CREATE TEMPORARY TABLE temp_new_cooccurrences (
                    e1_id INTEGER NOT NULL CHECK (e1_id <= e2_id),
                    e2_id INTEGER NOT NULL,
                    sentence_distance INTEGER,
                    CHECK (e1_id <= e2_id)
                )
            """
        )

        # Use a unified query structure that directly uses the ordering constraint
        base_query = f"""
                INSERT INTO temp_new_cooccurrences (e1_id, e2_id, sentence_distance)
                SELECT DISTINCT
                    e1.id,  -- e1.id is <= e2.id due to JOIN condition, ensuring canonical order
                    e2.id,
                    {"ABS(e1.sentence_index - e2.sentence_index)" if level == "sentence" else "NULL"}
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
                {"AND ABS(e1.sentence_index - e2.sentence_index) <= 5" if level == "sentence" else ""}
            """
        self.cursor.execute(base_query)

        # Get count of new co-occurrences
        self.cursor.execute("SELECT COUNT(*) FROM temp_new_cooccurrences")
        new_count = self.cursor.fetchone()[0]
        self.logger.info(f"Found {new_count:,} new co-occurrences")

        if new_count > 0:
            self.logger.info("Recording new co-occurrences using batch insertions...")
            BATCH_SIZE = 10_000
            offset = 0
            while True:
                self.cursor.execute(
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
                batch_rows = self.cursor.rowcount
                self.conn.commit()
                if batch_rows == 0:
                    break
                offset += BATCH_SIZE

            # Get statistics about entity types involved
            self.cursor.execute(
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
            type_stats = self.cursor.fetchall()

            self.logger.info("\nTop entity type pairs:")
            for type1, type2, count in type_stats:
                self.logger.info(f"  {type1} - {type2}: {count:,} pairs")

        # Get total statistics with proper NULL handling
        self.cursor.execute(
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
        total_stats = self.cursor.fetchone()

        self.conn.commit()
        self.logger.info(
            f"\nCo-occurrence identification complete:"
            f"\n- Total unique pairs: {total_stats[0]:,}"
            f"\n- Unique entities involved: {total_stats[1]:,}"
            + (
                f"\n- Average sentence distance: {total_stats[2]:.2f}"
                if level == "sentence"
                else ""
            )
        )

    def count_entity_cooccurrences_multithreaded(
        self, level: str = "document", batch_size=5000, num_reader_threads=32
    ) -> None:
        """
        Counts entity co-occurrences using ReaderWriterPair.
        """
        if level not in ["document", "sentence"]:
            raise ValueError("Level must be either 'document' or 'sentence'")

        def reader_query_fn(level):  # Define reader_query as a function
            return f"""
                    SELECT DISTINCT
                        e1.id,
                        e2.id,
                        {"ABS(e1.sentence_index - e2.sentence_index)" if level == "sentence" else "NULL"}
                    FROM entity_occurrences e1
                    JOIN entity_occurrences e2 ON
                        e1.document_id = e2.document_id AND
                        e1.id <= e2.id
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM entity_cooccurrences ec
                        WHERE ec.e1_id = e1.id AND ec.e2_id = e2.id
                    )
                    AND (e1.overlap = FALSE)
                    AND (e2.overlap = FALSE)
                    {"AND ABS(e1.sentence_index - e2.sentence_index) <= 5" if level == "sentence" else ""}
                """

        self.log_query_plan(reader_query_fn(level))

        def cooccurrence_process_function(batch, conn_params):
            """Processes a batch of entity co-occurrence data."""
            return batch  # For now, minimal processing, it done database side - just pass the batch through

        def cooccurrence_write_function(batch, cursor, conn):
            """Writes a batch of entity co-occurrences to the database using executemany."""
            sql = f"""
                    INSERT INTO entity_cooccurrences (e1_id, e2_id {", sentence_distance" if level == "sentence" else ""})
                    VALUES (?, ? {", ?" if level == "sentence" else ""})
                """
            try:
                cursor.executemany(
                    sql, batch
                )  # Directly use the batch from reader as it's pre-formatted
            except Exception as e:
                conn.rollback()  # Rollback transaction on error for the current batch
                print(
                    f"Error in write_function: {e}. Transaction rolled back for current batch."
                )
                return False  # Indicate failure (optional error handling)
            return True  # Indicate success (optional success indication)

        # --- COUNT QUERY TO GET ACCURATE total_count ---
        count_query = "SELECT COUNT(*) FROM (" + reader_query_fn(level) + ")"
        self.cursor.execute(count_query)
        total_count = self.cursor.fetchone()[0]
        self.logger.info(
            f"Estimated total co-occurrence pairs to process: {total_count:,}"
        )

        rw_pair = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            reader_query=reader_query_fn(level),  # Pass reader query function
            batch_size=batch_size,
            process_function=cooccurrence_process_function,
            write_function=cooccurrence_write_function,
            num_reader_threads=num_reader_threads,
            logger=self.logger,
            max_queue_size=50,
            profiling_writer_enabled=True,
            profiling_reader_enabled=True,
            writer_batch_chunking=4,
            total_count=total_count,  # Use the accurate count
            process_title=f"Co-occurrence counting at {level} level",
        )

        self.logger.info(
            f"Starting ReaderWriterPair to count entity co-occurrences at {level} level."
        )
        rw_pair.run()
        self.logger.info(
            f"ReaderWriterPair process finished for {level} level co-occurrence counting."
        )

    def co_aggregate_old(
        self, batch_size=50000, ignore_entities_with_error_codes: bool = True
    ) -> None:
        """
        Aggregate entity cooccurrences based on normalized entity IDs from entity_occurrences_summary.
        Uses existing relationships through entity_occurrences.summary_id to get normalized IDs.
        """
        self.logger.info("Starting cooccurrence aggregation...")

        # Create temporary table for staging aggregated results
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
                    CASE WHEN eo1.sentence_index = eo2.sentence_index THEN 1 ELSE 0 END as same_sentence
                FROM entity_cooccurrences ec
                JOIN entity_occurrences eo1 ON ec.e1_id = eo1.id
                JOIN entity_occurrences eo2 ON ec.e2_id = eo2.id
                WHERE eo1.summary_id IS NOT NULL 
                AND eo2.summary_id IS NOT NULL
                AND eo1.document_id = eo2.document_id  -- Ensure same document
                {"AND eo1.error_id IS NULL" if ignore_entities_with_error_codes else ""}
                {"AND eo2.error_id IS NULL" if ignore_entities_with_error_codes else ""}
            )
            SELECT
                e1_id_normalized,
                e2_id_normalized,
                COUNT(*) as fq_document_level,
                SUM(same_sentence) as fq_sentence_level,
                COUNT(DISTINCT document_id) as uniq_documents
            FROM normalized_pairs
            GROUP BY e1_id_normalized, e2_id_normalized
        """

        self.logger.debug(f"Query for cooccurrence aggregation: {query}")

        self.cursor.execute(query)

        # Insert or replace final results using SQLite syntax
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO entity_cooccurrences_summary 
                (e1_id_normalized, e2_id_normalized, fq_document_level, 
                fq_sentence_level, uniq_documents)
            SELECT * FROM tmp_cooccurrences
        """
        )

        self.cursor.execute("DROP TABLE tmp_cooccurrences")
        self.conn.commit()

        self.logger.info("Cooccurrence aggregation completed")

    def co_aggregate_multithreaded(
        self,
        batch_size=100000,
        ignore_entities_with_error_codes: bool = True,
        num_reader_threads=16,
    ) -> None:
        """
        Aggregate entity cooccurrences based on normalized entity IDs from entity_occurrences_summary.
        Uses existing relationships through entity_occurrences.summary_id to get normalized IDs.
        Aggregate entity cooccurrences using ReaderWriterPair pattern.
        """

        self.logger.info("Starting cooccurrence aggregation using ReaderWriterPair...")

        ignore_error_code_condition_eo1 = (
            "AND eo1.error_id IS NULL" if ignore_entities_with_error_codes else ""
        )
        ignore_error_code_condition_eo2 = (
            "AND eo2.error_id IS NULL" if ignore_entities_with_error_codes else ""
        )

        def reader_query():
            """Reader query to fetch aggregated co-occurrence data."""
            return f"""
                WITH normalized_pairs AS (
                    SELECT
                        CASE WHEN eo1.summary_id <= eo2.summary_id
                            THEN eo1.summary_id
                            ELSE eo2.summary_id END AS e1_id_normalized,
                        CASE WHEN eo1.summary_id <= eo2.summary_id
                            THEN eo2.summary_id
                            ELSE eo1.summary_id END AS e2_id_normalized,
                        eo1.document_id,
                        CASE WHEN eo1.sentence_index = eo2.sentence_index THEN 1 ELSE 0 END as same_sentence
                    FROM entity_cooccurrences ec
                    JOIN entity_occurrences eo1 ON ec.e1_id = eo1.id
                    JOIN entity_occurrences eo2 ON ec.e2_id = eo2.id
                    WHERE eo1.summary_id IS NOT NULL
                    AND eo2.summary_id IS NOT NULL
                    AND eo1.document_id = eo2.document_id  -- Ensure same document
                    {ignore_error_code_condition_eo1}
                    {ignore_error_code_condition_eo2}
                )
                SELECT
                    e1_id_normalized,
                    e2_id_normalized,
                    COUNT(*) as fq_document_level,
                    SUM(same_sentence) as fq_sentence_level,
                    COUNT(DISTINCT document_id) as uniq_documents
                FROM normalized_pairs
                GROUP BY e1_id_normalized, e2_id_normalized
            """

        self.log_query_plan(reader_query())  # Log query plan for reader query

        def aggregation_process_function(batch, conn_params):
            """Processes a batch of aggregated co-occurrence data (minimal processing)."""
            return batch  # No processing needed, data is aggregated in the reader query

        def aggregation_write_function(batch, cursor, conn):
            """Writes aggregated co-occurrence data to entity_cooccurrences_summary."""
            sql = """
                INSERT OR REPLACE INTO entity_cooccurrences_summary
                    (e1_id_normalized, e2_id_normalized, fq_document_level,
                    fq_sentence_level, uniq_documents)
                VALUES (?, ?, ?, ?, ?)
            """
            try:
                cursor.executemany(sql, batch)
            except Exception as e:
                conn.rollback()
                self.logger.error(
                    f"Error in aggregation_write_function: {e}. Transaction rolled back for current batch."
                )
                return False
            return True

        reader_writer_pair = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            reader_query=reader_query(),
            process_function=aggregation_process_function,
            write_function=aggregation_write_function,
            batch_size=batch_size,
            num_reader_threads=num_reader_threads,
            profiling_reader_enabled=True,
            profiling_writer_enabled=True,
            writer_batch_chunking=1,
            total_count=self.cursor.execute(
                "SELECT COUNT(*) FROM entity_cooccurrences WHERE summary_id IS NOT NULL"
            ).fetchone()[0],
            process_title="Co-occurrence Aggregation",
            logger=self.logger,
        )

        self.logger.info(f"Starting ReaderWriterPair process.")
        reader_writer_pair.run()

        self.logger.info("Cooccurrence aggregation using ReaderWriterPair completed")

    def co_calc_pmi(self, batch_size=200000) -> None:
        """
        Calculate the Pointwise Mutual Information (PMI) for each cooccurence pairing of two unique entities and update the 'pmi' column in entity_cooccurrences_summary table.

        PMI is calculated as:
            PMI(e1, e2) = log(P(e1, e2) / (P(e1) * P(e2)))
        where:

        - P(e1, e2) is the probability of co-occurrence of entities e1 and e2
            = frequency of co-occurrence / docs_total
        - P(e1) and P(e2) are the probabilities of occurrence of entities e1 and e2 respectively
            = number of documents containing the entity / docs_total

        Uses pre-calculated entity co-occurrence frequencies and entity occurrence frequencies.

        """
        try:
            self.logger.info("Starting PMI calculation for entity co-occurrences...")
            total_docs = self.statistics.document_count

            # --- Reader Query and Process Function ---
            reader_query = f"""
                SELECT 
                    ecs.e1_id_normalized,
                    ecs.e2_id_normalized,
                    ecs.fq_document_level,
                    eos1.uniq_documents AS e1_docs,
                    eos2.uniq_documents AS e2_docs
                FROM entity_cooccurrences_summary ecs
                JOIN entity_occurrences_summary eos1 ON ecs.e1_id_normalized = eos1.id
                JOIN entity_occurrences_summary eos2 ON ecs.e2_id_normalized = eos2.id
                WHERE ecs.fq_document_level > 0
            """

            self.log_query_plan(reader_query)  # Log query plan for reader query

            def pmi_process_function(batch, conn_params):
                """Process function to calculate PMI for a batch of co-occurrences"""
                pmis = []
                for e1_id, e2_id, fq, e1_docs, e2_docs in batch:
                    if e1_docs == 0 or e2_docs == 0:
                        pmi = None  # Handle division by zero or log(0)
                    else:
                        pmi = math.log((fq * total_docs) / (e1_docs * e2_docs))
                    pmis.append((pmi, e1_id, e2_id))
                return pmis

            def pmi_write_function(batch, cursor, conn):
                """Writes PMI values back to the database in batches."""
                cursor.executemany(
                    "UPDATE entity_cooccurrences_summary SET pmi = ? WHERE e1_id_normalized = ? AND e2_id_normalized = ?",
                    batch,
                )

            # --- Instantiate and Run ReaderWriterPair ---
            rw_pair = ReaderWriterPair(
                conn_params=self.conn_params_dict,
                reader_query=reader_query,
                batch_size=batch_size,
                process_function=pmi_process_function,
                write_function=pmi_write_function,
                logger=self.logger,
                max_queue_size=10,  # Adjust as needed
                writer_batch_chunking=5,
                process_title="PMI Calculation",
                total_count=self.cursor.execute(
                    "SELECT COUNT(*) FROM entity_cooccurrences_summary WHERE pmi IS NULL"
                ).fetchone()[0],
            )
            rw_pair.run()  # Run ReaderWriterPair to calculate and write PMIs

            self.logger.info(f"PMI calculation complete")

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Sqlite error while calculating PMI {e}")
            raise
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Unexpected error calculating PMI: {e}")
            raise

    def deprecated_summarize_entity_cooccurrences(self, batch_size=50000) -> None:
        """
        Optimized implementation with:
        - Temporary staging table
        - Batch processing
        - Index optimization
        - Minimal view usage
        """
        try:
            self.logger.info("Starting optimized co-occurrence summarization...")

            # Step 0: Cleanup and preparation
            self.cursor.execute("DELETE FROM entity_cooccurrences_summary")
            self.cursor.execute(
                "CREATE TEMP TABLE temp_cooc_staging ("
                "e1_norm INT, e2_norm INT, document_id INT, fq INT, "
                "PRIMARY KEY (e1_norm, e2_norm, document_id)) WITHOUT ROWID"
            )

            # Step 1: Batch populate staging table using direct joins
            offset = 0
            while True:
                query = """
                    INSERT OR IGNORE INTO temp_cooc_staging
                    SELECT
                        MIN(eo1.summary_id, eo2.summary_id),
                        MAX(eo1.summary_id, eo2.summary_id),
                        eo1.document_id,
                        COUNT(ec.e1_id)
                    FROM entity_cooccurrences ec
                    JOIN entity_occurrences eo1 ON ec.e1_id = eo1.id
                    JOIN entity_occurrences eo2 ON ec.e2_id = eo2.id
                    WHERE eo1.document_id = eo2.document_id
                    GROUP BY MIN(eo1.summary_id, eo2.summary_id), MAX(eo1.summary_id, eo2.summary_id), eo1.document_id
                    LIMIT ? OFFSET ?
                """
                self.cursor.execute(query, (batch_size, offset))

                if self.cursor.rowcount == 0:
                    break

                offset += self.cursor.rowcount
                self.conn.commit()
                self.logger.info(f"Staging progress: {offset} rows")

            # Step 2: Bulk insert summaries using staging data
            base_query = """
                INSERT INTO entity_cooccurrences_summary (e1_id_normalized, e2_id_normalized, fq_document_level, uniq_documents)
                SELECT
                    e1_norm,
                    e2_norm,
                    SUM(fq),
                    COUNT(DISTINCT document_id)
                FROM temp_cooc_staging
                GROUP BY e1_norm, e2_norm
            """
            explain_query = "EXPLAIN QUERY PLAN " + base_query
            self.cursor.execute(explain_query)
            query_plan = self.cursor.fetchall()
            self.logger.debug(f"Query plan for step 2: {query_plan}")

            self.cursor.execute(base_query)

            self.logger.info("Summarization complete")

            # Step 3: Batch update summary_id using covering index
            self.cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cooc_pair 
                ON entity_cooccurrences(e1_id, e2_id)
            """
            )

            offset = 0
            while True:
                self.cursor.execute(
                    f"""
                    UPDATE entity_cooccurrences
                    SET summary_id = (
                        SELECT id 
                        FROM entity_cooccurrences_summary 
                        WHERE e1_id_normalized = MIN(eo1.summary_id, eo2.summary_id)
                        AND e2_id_normalized = MAX(eo1.summary_id, eo2.summary_id)
                    )
                    FROM entity_occurrences eo1
                    JOIN entity_occurrences eo2 ON eo2.id = entity_cooccurrences.e2_id
                    WHERE entity_cooccurrences.e1_id = eo1.id
                    LIMIT {batch_size} OFFSET {offset}
                """
                )

                if self.cursor.rowcount == 0:
                    break

                offset += self.cursor.rowcount
                self.conn.commit()
                self.logger.info(f"Linking progress: {offset} rows")

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Summarization failed: {str(e)}")
            raise
        finally:
            self.cursor.execute("DROP TABLE IF EXISTS temp_cooc_staging")
            self.cursor.execute("DROP INDEX IF EXISTS idx_cooc_pair")
