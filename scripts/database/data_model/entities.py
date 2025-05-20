import math
import sqlite3

import numpy as np
from tqdm import tqdm

from scripts.database.db_main import EasyNerDBHandler

from ..core.db_engine import ReaderWriterPair
import pandas as pd
from .schema import *


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
                    {TXT} TEXT,
                    {ERROR_ID} VARCHAR(20),
                    {CLASS_ID} INTEGER,
                    {DOC_ID} INTEGER,
                    {SENT_IDX} INTEGER,
                    {NE_NORM_ID} INTEGER,
                    {NE_OVERLAP} BOOLEAN,
                    {SPAN_START} INTEGER,
                    {SPAN_END} INTEGER,
                    FOREIGN KEY ({DOC_ID}) REFERENCES {TABLE_DOCS} (id),
                    FOREIGN KEY ({DOC_ID},{SENT_IDX}) REFERENCES {TABLE_SENTENCES} ({DOC_ID},{SENT_IDX}),
                    FOREIGN KEY ({CLASS_ID}) REFERENCES {TABLE_NE_CLASS} (id),
                    FOREIGN KEY ({NE_NORM_ID}) REFERENCES {TABLE_NE_AGGR} ({NE_NORM_ID}),
                    FOREIGN KEY ({ERROR_ID}) REFERENCES {TABLE_NE_ERROR} ({ERROR_ID})
                );
                """

        self.stmt_table_ne_aggregated = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_AGGR} (
                    {NE_NORM_ID} INTEGER PRIMARY KEY,
                    {CLASS_ID} INTEGER,
                    {TXT_NORM} TEXT,
                    UNIQUE ({TXT_NORM}, {CLASS_ID}) -- Probably not needed
                );
                """

        self.stmt_table_ne_lookup = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_LOOKUP} (
                    id INTEGER PRIMARY KEY, -- 1:1 relation with {TABLE_NE}
                    {CLASS_ID} INTEGER,
                    {TXT_NORM} TEXT,
                    {NE_NORM_ID} INTEGER,
                    FOREIGN KEY (id) REFERENCES {TABLE_NE} (id),
                    FOREIGN KEY ({NE_NORM_ID}) REFERENCES {TABLE_NE_AGGR} ({NE_NORM_ID})
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

    def generate_lookup_table_for_normalized_entities(self, target_table=None):
        """
        Create a temporary table with normalized entity text and entity_id.
        """

        try:
            self.logger.info(
                f"Generating {TABLE_NE} --> lookup table: {TABLE_NE_LOOKUP} for normalized entity texts"
            )

            self.cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NE_LOOKUP}")

            # Create the aggregated entities table if it doesn't exist
            self.cursor.execute(self.stmt_table_ne_aggregated)
            # Entity Linking Lookup Table
            self.cursor.execute(self.stmt_table_ne_lookup)

            self.logger.info(f"Created {TABLE_NE_LOOKUP} table")

            stmt_populate_lookup_table = f"""--sql
                INSERT INTO {TABLE_NE_LOOKUP} (id, {CLASS_ID}, {TXT_NORM})
                SELECT
                    id,
                    {CLASS_ID},
                    LOWER({TXT}) as {TXT_NORM}
                FROM {TABLE_NE}
                WHERE {ERROR_ID} IS NULL
                AND {NE_OVERLAP} IS FALSE
                AND {TXT} IS NOT NULL
                ORDER BY id
                """

            self.log_query_plan(stmt_populate_lookup_table)

            # Create temporary table with normalized entities
            self.cursor.execute(stmt_populate_lookup_table)
            self.conn.commit()

        except sqlite3.Error as e:
            self.logger.error(
                f"Error creating temporary normalized entities table: {e}"
            )

    def validate_normalized_entities(
        self, eo_table=TABLE_NE, target_table=TABLE_NE_LOOKUP
    ):
        # Use reader writer pair to validate normalized entities, i.e. all entity_ids in entiry_occurrences, filtered for error_id, and overlap, should be present in the lookup table
        self.logger.info(
            f"Validating normalized entities in {eo_table} against {target_table}"
        )

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
                self.logger.info(
                    "Batch Validation successful: All entities are normalized."
                )
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

    def _process_text_column(
        self,
        column_name: str,
        table_name: str,
        process_query: str,
        process_function=None,
        write_function=None,
    ):

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
            self.logger.error(
                f"Column {column_name} does not exist in table {table_name}"
            )
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
        table_name: str = TABLE_NE_LOOKUP,
        column_name: str = TXT_NORM,
        batch_size: int = 100000,
        offset: int = 0,
    ):
        """
        Normalizes a specified text column in a SQLite database table by:
        1. Removing leading whitespace.
        2. Removing the leading prefix '" ' (double quote followed by space).

        Args:
            database_file (str): Path to the SQLite database file.
            table_name (str): Name of the table containing the column to normalize.
            column_name (str): Name of the column to normalize (e.g., 'txt_norm').
        """

        try:
            # 2. SQL statement to remove leading whitespace
            sql_remove_leading_whitespace = f"""--sql
                UPDATE {table_name}
                SET {column_name} = LTRIM({column_name})
                WHERE {column_name} <> LTRIM({column_name})
                LIMIT ? OFFSET ?;
            """

            sql_remove_leading_whitespace = f"""--sql
                UPDATE {table_name}
                SET {column_name} = LTRIM({column_name})
                WHERE rowid IN (
                    SELECT rowid
                    FROM {table_name}
                    WHERE {column_name} <> LTRIM({column_name})
                    LIMIT ? OFFSET ?
                );
            """

            self.log_query_plan(sql_remove_leading_whitespace)

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

            sql_remove_leading_quote_space_batched = f"""--sql
                UPDATE {table_name}
                SET {column_name} =
                    CASE
                        WHEN SUBSTR({column_name}, 1, 2) = '" ' THEN SUBSTR({column_name}, 3)
                        WHEN SUBSTR({column_name}, 1, 3) = '% )' THEN SUBSTR({column_name}, 4)
                        WHEN SUBSTR({column_name}, 1, 2) = '% ' THEN SUBSTR({column_name}, 3)
                        ELSE {column_name}
                    END
                WHERE rowid IN (
                    SELECT rowid
                    FROM {table_name}
                    WHERE (SUBSTR({column_name}, 1, 2) = '" '
                        OR SUBSTR({column_name}, 1, 3) = '% )'
                        OR SUBSTR({column_name}, 1, 2) = '% ')
                    LIMIT ? OFFSET ?
                );
            """

            self.log_query_plan(sql_remove_leading_quote_space)
            # 4. Execute the SQL statements

            total_count = self.cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                """
            ).fetchone()[0]

            with tqdm(
                total=total_count,
                desc=f"Step 1: Removing leading whitespace from column '{column_name}' in table '{table_name}",
            ) as pbar:
                while True:
                    self.cursor.execute(
                        sql_remove_leading_whitespace,
                        (batch_size, offset),
                    )
                    row_count = self.cursor.rowcount
                    if (
                        row_count < batch_size or row_count == 0
                    ):  # Added condition to break if no rows are updated
                        pbar.update(row_count)
                        print("Leading whitespace removal completed.")
                        break
                    offset += batch_size
                    pbar.update(row_count)

            print(
                f"Step 2: Removing leading '\" ' from column '{column_name}' in table '{table_name}'..."
            )
            # self.cursor.execute(sql_remove_leading_quote_space)
            # print("Leading '\" ' removal completed.")

            offset = 0
            with tqdm(
                total=total_count,
                desc=f"Step 2: Removing leading '\" ' from '{column_name}' in '{table_name}'",
            ) as pbar:
                while True:
                    self.cursor.execute(
                        sql_remove_leading_quote_space_batched, (batch_size, offset)
                    )
                    row_count = self.cursor.rowcount
                    if row_count == 0:
                        pbar.update(0)  # No rows updated in this batch, we're done.
                        print("Leading quote/prefix removal completed.")
                        break
                    offset += row_count
                    self.conn.commit()
                    pbar.update(row_count)

        # # 5. Commit the changes to the database
        # self.conn.commit()
        # print("Changes committed to the database.")

        except sqlite3.Error as e:
            print(f"Database error: {e}")
        except KeyboardInterrupt:
            self.conn.rollback()
            print("Operation cancelled by user.")
            # class AggregatedData:
            """
            Contains methods for updating aggregated_entities table.
            """

    def aggregate_normalized_text__CORRECT(
        self,
        overwrite: bool = False,
        eo_aggregated_table_name=TABLE_NE_AGGR,
    ):
        """
        Aggregates distinct txt_norm values from eo_lookup into a new table eo_normalized.

        Args:
            conn_params (dict): Database connection parameters.
            eo_aggregated_table_name (str): Name of the table to store aggregated normalized texts.
            eo_lookup_table_name (str): Name of the eo_lookup table.
            logger (logging.Logger): Logger instance.
        """

        self.logger.info(f"--- Starting Aggregation to {eo_aggregated_table_name} ---")

        # 1: Create eo_normalized table (moved inside function)
        self.cursor.execute(self.stmt_table_ne_aggregated)

        # Define Reader query to get distinct normalized texts (moved inside function)
        reader_query_norm_txt = f"""--sql
            SELECT {TXT_NORM}, {CLASS_ID}
            FROM (SELECT DISTINCT {TXT_NORM}, {CLASS_ID} FROM {TABLE_NE_LOOKUP}
                {"WHERE " + NE_NORM_ID + " IS NULL" if not overwrite else ""})
            ORDER BY {TXT_NORM}
        """

        # total_count = self.cursor.execute(
        #     f"""
        #     SELECT COUNT(DISTINCT {COL_NE_TXT_NORM})
        #     FROM {TABLE_NE_LOOKUP}
        #     {"WHERE " + COL_NE_NORM_ID + " IS NULL;" if not overwrite else ""}
        #     """
        # ).fetchone()[0]

        # self.logger.info(f"Aggregating {total_count:,} distinct normalized texts")
        self.logger.info(f"Aggregating distinct normalized texts")

        # self.log_query_plan(reader_query_norm_txt)

        # create index for faster processing of entity_occurrences
        index = f"idx_{TABLE_NE}_text_entity_id"
        # Check if index with the same name exists on the table
        self.cursor.execute(
            "SELECT COUNT(*) FROM pragma_index_list(?) WHERE name = ?",
            (TABLE_NE_LOOKUP, index),
        )
        index_exists = self.cursor.fetchone()[0] > 0
        if not index_exists:
            self.logger.info(f"Creating index {index} on {TABLE_NE_LOOKUP}")
            self.cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {index} ON {TABLE_NE_LOOKUP} ({TXT_NORM}, {CLASS_ID})"""
            )
            self.logger.info(
                f"Index {index} created on {TABLE_NE_LOOKUP}, running ANALYZE"
            )
            self.cursor.execute(f"ANALYZE {TABLE_NE_LOOKUP}")
            self.log_query_plan(reader_query_norm_txt)

        def process_function_norm_txt(batch, conn_params):
            """Process batch function for normalized texts (no processing needed)."""
            return [
                (row[0], row[1]) for row in batch if row[0] is not None
            ]  # Ensure None values are skipped

        def write_function_norm_txt(batch, cursor, conn):
            """Write function to insert normalized texts into eo_normalized table."""
            insert_sql = f"""
                INSERT OR IGNORE INTO {eo_aggregated_table_name} ({TXT_NORM}, {CLASS_ID}) VALUES (?, ?)
            """
            cursor.executemany(insert_sql, batch)

        # --- Run ReaderWriterPair for aggregation (moved inside function) ---
        rw_pair_norm_agg = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            reader_query=reader_query_norm_txt,
            batch_size=1000000,
            num_reader_threads=6,
            max_queue_size=10,
            process_function=process_function_norm_txt,
            write_function=write_function_norm_txt,
            logger=self.logger,
            process_title="eo_normalized_aggregation",
            profiling_reader_enabled=False,
            profiling_writer_enabled=False,
            # total_count=total_count,
        )
        rw_pair_norm_agg.run()
        self.logger.info(f"--- Finished Aggregation: {TABLE_NE_AGGR} table ---")

    def backreference_norm_id(
        self,
        overwrite: bool = False,
        target_table=TABLE_NE,
    ):
        """
        Backreferences norm_id from eo_normalized into eo_lookup based on txt_norm and entity_class_id.

        Args:
            overwrite (bool): If True, will update all records. If False, only updates NULL norm_id records.
        """

        # Create column norm_id in eo_lookup table if it doesn't exist
        self.cursor.execute(
            f"""--sql
            PRAGMA table_info({target_table})
            """
        )
        columns = self.cursor.fetchall()
        norm_id_exists = any(column[1] == f"{NE_NORM_ID}" for column in columns)
        if not norm_id_exists:
            self.cursor.execute(
                f"""--sql
                ALTER TABLE {target_table}
                ADD COLUMN {NE_NORM_ID} INTEGER;
                """
            )

        # Create index for faster processing of entity_occurrences
        index = f"idx_{target_table}_{NE_NORM_ID}"
        self.cursor.execute(
            f"""CREATE INDEX IF NOT EXISTS {index} ON {target_table} ({NE_NORM_ID})"""
        )
        self.cursor.execute(f"ANALYZE {target_table}")

        # Check if index with the same name exists on the table
        self.logger.info(f"--- Starting Backreference: {target_table} table ---")
        try:
            # Define Reader query for eo_lookup backreference with both txt_norm and entity_class_id matching
            reader_query_eo_lookup_backref = f"""--sql
                SELECT
                    eoa.{NE_NORM_ID},
                    e.id
                FROM
                    {TABLE_NE} AS e
                JOIN
                    {TABLE_NE_LOOKUP} AS eol
                ON
                    e.id = eol.id
                JOIN
                    {TABLE_NE_AGGR} AS eoa
                ON
                    eol.{TXT_NORM} = eoa.{TXT_NORM}
                    AND eol.{CLASS_ID} = eoa.{CLASS_ID}
                WHERE
                    e.error_id IS NULL
                    AND e.overlap IS FALSE
                    {"AND e." + NE_NORM_ID + " IS NULL" if not overwrite else ""}
                ORDER BY
                    e.id
                LIMIT :limit OFFSET :offset;
            """

            self.log_query_plan(
                reader_query_eo_lookup_backref, params={"limit": 100, "offset": 0}
            )

            total_records = self.cursor.execute(
                f"""--sql
                SELECT COUNT(*) FROM {TABLE_NE}
                WHERE
                    error_id IS NULL
                    AND overlap IS FALSE
                    {"AND " + NE_NORM_ID + " IS NULL" if not overwrite else ""}

                """
            ).fetchone()[0]

            self.logger.info(
                f"Updating {NE_NORM_ID} references for {total_records} records in {target_table}"
            )

            # Process function to get the entity ID pairs
            def process_function_eo_lookup_backref(batch, conn_params):
                return [(row[0], row[1]) for row in batch]

            # Write function to update norm_id in eo_lookup
            def write_function_eo_lookup_backref(batch, cursor, conn):
                update_sql = f"""
                    UPDATE {target_table}
                    SET {NE_NORM_ID} = ?
                    WHERE id = ?
                """
                cursor.executemany(update_sql, batch)

            # Run ReaderWriterPair for backreference
            rw_pair_eo_lookup_backref = ReaderWriterPair(
                conn_params=self.conn_params_dict,
                reader_query=reader_query_eo_lookup_backref,
                batch_size=150000,
                num_reader_threads=32,
                process_function=process_function_eo_lookup_backref,
                write_function=write_function_eo_lookup_backref,
                logger=self.logger,
                process_title="eo_lookup_backreference",
                profiling_reader_enabled=True,
                profiling_writer_enabled=True,
                total_rows=total_records,
            )
            rw_pair_eo_lookup_backref.run()
            self.logger.info("--- Finished Backreference: eo_lookup table ---")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating reader query: {e}")
            raise

    def create_view_entity_occurrences(self):
        """
        Creates a view that joins entity occurrences with other tables to provide a comprehensive view of the data.
        """
        try:
            self.logger.info(f"Creating {VIEW_NE} view...")

            # Delete the view if it already exists
            self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_NE}")

            view_sql = f"""--sql
                CREATE VIEW IF NOT EXISTS {VIEW_NE} AS
                SELECT
                    eo.id,
                    eo.{TXT},
                    nea.{TXT_NORM},
                    error_code.{ERROR_ID},
                    doc.title as document_title,
                    eo.{DOC_ID},
                    nea.uniq_documents
                FROM
                    {TABLE_NE} eo
                JOIN named_entities ne ON eo.{CLASS_ID} = ne.id
                JOIN documents doc ON eo.{DOC_ID} = doc.id
                LEFT JOIN {TABLE_NE_AGGR} nea ON eo.{NE_NORM_ID} = nea.id
                LEFT JOIN {TABLE_NE_ERROR} error_code ON eo.{ERROR_ID} = {TABLE_NE_ERROR}.error_id;
            """

            self.cursor.execute(view_sql)
            self.conn.commit()
            self.logger.info(f"{VIEW_NE} view created successfully.")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating view_entity_occurrences view: {e}")
            raise

    def create_view__comp__txt(self):
        """
        Creates a view that joins entity occurrences with other tables to provide a comprehensive view of the data.
        """
        try:
            self.logger.info(f"Creating {VIEW_NE} view...")

            # Delete the view if it already exists
            self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_NE}_comp_txt")

            view_sql = f"""--sql
                CREATE VIEW IF NOT EXISTS {VIEW_NE}_comp_txt AS
                SELECT
                    eo.id as NE_ID,
                    eo.{TXT} as NE_TEXT,
                    eol.id as LOOKUP_ID,
                    eol.{TXT_NORM} as LOOKUP_TEXT,
                    eo.{NE_NORM_ID} as NORM_ID,
                    nea.{NE_NORM_ID} as AGGR_ID,
                    nea.{TXT_NORM} as AGGR_TEXT,
                    doc.title as DOC_TITLE,
                    eo.{DOC_ID} as DOC_ID
                FROM
                    {TABLE_NE} eo
                JOIN {TABLE_NE_CLASS} ne ON eo.{CLASS_ID} = ne.id
                JOIN {TABLE_DOCS} doc ON eo.{DOC_ID} = doc.id
                LEFT JOIN {TABLE_NE_LOOKUP} eol ON eo.id = eol.id
                LEFT JOIN {TABLE_NE_AGGR} nea ON eo.{NE_NORM_ID} = nea.{NE_NORM_ID}
            """

            self.cursor.execute(view_sql)
            self.conn.commit()
            self.logger.info(f"{VIEW_NE} view created successfully.")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating view_entity_occurrences view: {e}")
            raise

    def create_view_raw_entity_occurrences(self):
        """
        Creates a view that joins entity occurrences with other tables to provide a comprehensive view of the data.
        """
        try:
            self.logger.info(f"Creating {VIEW_NE_RAW} view...")

            # Delete the view if it already exists
            self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_NE_RAW}")

            view_sql = f"""--sql
                CREATE VIEW IF NOT EXISTS {VIEW_NE_RAW} AS
                SELECT
                    eo.id,
                    eo.{TXT},
                    nea.{TXT_NORM},
                    nec.{NE_CLASS},
                    error_code.{ERROR_ID},
                    doc.title as document_title,
                    eo.{DOC_ID}
                FROM
                    {TABLE_NE} eo
                JOIN {TABLE_NE_CLASS} nec ON eo.{CLASS_ID} = nec.id
                JOIN documents doc ON eo.{DOC_ID} = doc.id
                LEFT JOIN {TABLE_NE_AGGR} nea ON eo.{NE_NORM_ID} = nea.{NE_NORM_ID}
            """

            print(view_sql)

            self.cursor.execute(view_sql)
            self.conn.commit()
            self.logger.info(f"{VIEW_NE_RAW} view created successfully.")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating view_entity_occurrences view: {e}")
            raise

    def create_view_ne_compiled(self):
        """
        Create view with compiled entity information for easy access and computations
        """
        try:
            self.logger.info("Creating view_ne_compiled view...")
            self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_NE_COMP}")

            view_sql = f"""--sql
                    CREATE VIEW IF NOT EXISTS {VIEW_NE_COMP}  AS
                    SELECT
                        eo.id as NE_ID,
                        eo.{TXT} as TXT,
                        nec.{NE_CLASS} as NE_CLASS,
                        nea.{NE_NORM_ID} as AGGR_ID,
                        nea.{TXT_NORM} as TXT_NORM,
                        nea.fq as FQ,
                        nea.doc_count as DOC_COUNT,
                        doc.title as DOC_TITLE,
                        eo.{DOC_ID} as DOC_ID,
                        eo.{SENT_IDX} as SENT_IDX,
                        eo.{SPAN_START} as SPAN_START,
                        eo.{SPAN_END} as SPAN_END
                    FROM
                        {TABLE_NE} eo
                    JOIN {TABLE_NE_CLASS} nec ON eo.{CLASS_ID} = nec.id
                    JOIN {TABLE_DOCS} doc ON eo.{DOC_ID} = doc.id
                    LEFT JOIN {TABLE_NE_AGGR} nea ON eo.{NE_NORM_ID} = nea.{NE_NORM_ID}
                """

            self.cursor.execute(view_sql)
            self.conn.commit()
            self.logger.info(f"{VIEW_NE} view created successfully.")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating view_entity_occurrences view: {e}")
            raise

    def create_view_ne_stats(self):
        """
        Create view with compiled entity information for easy access and computations
        """
        try:
            self.logger.info("Creating view_ne_compiled view...")
            self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_NE_STATS}")

            view_sql = f"""--sql
                    CREATE VIEW IF NOT EXISTS {VIEW_NE_STATS}  AS
                    SELECT
                        eo.id as NE_ID,
                        eo.{CLASS_ID} as CLASS_ID,
                        eo.{NE_NORM_ID} as AGGR_ID,
                        eo.{DOC_ID} as DOC_ID,
                        eo.{SENT_IDX} as SENT_IDX,
                        nea.{DOC_COUNT} as DOC_COUNT
                    FROM
                        {TABLE_NE} eo
                    LEFT JOIN {TABLE_NE_AGGR} nea ON eo.{NE_NORM_ID} = nea.{NE_NORM_ID}
                """

            self.log_query_plan(view_sql)
            self.cursor.execute(view_sql)
            self.conn.commit()
            self.logger.info(f"{VIEW_NE} view created successfully.")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating view_entity_occurrences view: {e}")
            raise

    def create_view_error_lookup_inspection(self):
        """
        Create view showing ne with error_id and or overlap and norm_id IS NOT NULL
        This shold show no results as norm_id should be NULL for error_id OR overlap
        """
        try:
            VIEW_ERROR_LOOKUP_INSPECTION = VIEW_PREFIX + "_error_lookup_inspection"
            self.logger.info("Creating view_error_lookup_inspection view...")
            self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_ERROR_LOOKUP_INSPECTION}")

            view_sql = f"""--sql
                    CREATE VIEW IF NOT EXISTS {VIEW_ERROR_LOOKUP_INSPECTION}  AS
                    SELECT
                        eo.id as NE_ID,
                        eo.{TXT} as TXT,
                        nea.{TXT_NORM} as TXT_NORM,
                        eo.{ERROR_ID} as ERROR_ID,
                        eo.{NE_OVERLAP} as OVERLAP,
                        eo.{NE_NORM_ID} as NORM_ID
                    FROM
                        {TABLE_NE} eo
                    LEFT JOIN {TABLE_NE_AGGR} nea ON eo.{NE_NORM_ID} = nea.{NE_NORM_ID}
                    WHERE eo.{NE_NORM_ID} IS NOT NULL
                    AND (eo.{ERROR_ID} IS NOT NULL OR eo.{NE_OVERLAP} = TRUE)
                """

            self.cursor.execute(view_sql)
            self.conn.commit()
            self.logger.info(
                f"{VIEW_ERROR_LOOKUP_INSPECTION} view created successfully."
            )

        except sqlite3.Error as e:
            self.logger.error(f"Error creating view_error_lookup_inspection view: {e}")
            raise

        try:
            VIEW_UNBACKPOPULATED = VIEW_PREFIX + "_ne_without_backpopulated"
            self.logger.info("Creating view_error_lookup_inspection view...")
            self.cursor.execute(f"DROP VIEW IF EXISTS {VIEW_UNBACKPOPULATED}")

            view_sql = f"""--sql
                    CREATE VIEW IF NOT EXISTS {VIEW_UNBACKPOPULATED}  AS
                    SELECT
                        eo.id as NE_ID,
                        eo.{TXT} as TXT,
                        eol.{TXT_NORM} as TXT_LOOKUP,
                        nea.{TXT_NORM} as TXT_NORM,
                        eo.{ERROR_ID} as ERROR_ID,
                        eo.{NE_OVERLAP} as OVERLAP,
                        eo.{NE_NORM_ID} as NORM_ID
                    FROM
                        {TABLE_NE} eo
                    LEFT JOIN {TABLE_NE_LOOKUP} eol ON eo.id = eol.id
                    LEFT JOIN {TABLE_NE_AGGR} nea ON eol.{NE_NORM_ID} = nea.{NE_NORM_ID}
                    WHERE eo.{NE_NORM_ID} IS NULL AND (eo.{ERROR_ID} IS NULL AND eo.{NE_OVERLAP} = FALSE)
                """

            self.cursor.execute(view_sql)
            self.conn.commit()
            self.logger.info(f"{VIEW_UNBACKPOPULATED} view created successfully.")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating view_error_lookup_inspection view: {e}")
            raise

    def stats_aggregated(self):
        """
        Calculates statistics based on the aggregated entities in the compiled view.
        Specifically, it calculates the frequency (total count) and the document count
        (number of unique documents) for each aggregated entity.
        """
        try:
            self.logger.info("Calculating aggregated entity statistics...")

            try:
                self.logger.info("Creating indexes for optimized queries...")

                # Index on eo.document_id
                self.cursor.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_eo_doc_id ON {TABLE_NE}({DOC_ID});
                """
                )

                # Covering index for doc_id and aggrgated_id
                self.cursor.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_eo_doc_id_aggr_id ON {TABLE_NE}({DOC_ID}, {NE_NORM_ID});
                """
                )

                # Covering index on TABLE_NE_AGGR
                self.cursor.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_nea_norm_id_doc_id ON {TABLE_NE_AGGR}({NE_NORM_ID});
                """
                )

                # Covering index on TABLE_NE_AGGR for doc_count
                self.cursor.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_nea_doc_count ON {TABLE_NE_AGGR}(doc_count);
                """
                )

                self.conn.commit()
                self.logger.info("Indexes created successfully.")

            except sqlite3.Error as e:
                self.logger.error(f"Error creating indexes: {e}")
                raise

            # SQL query to calculate frequency and document count per aggregated entity
            stats_sql = f"""--sql
                SELECT
                    AGGR_ID,
                    COUNT(*) AS {FQ},
                    COUNT(DISTINCT DOC_ID) AS {DOC_COUNT}
                FROM
                    {VIEW_NE_COMP}
                WHERE
                    AGGR_ID IS NOT NULL -- This already has filtered out Errors and Overlaps for which the AGGR id is NULL
                    AND doc_count = 0
                GROUP BY
                    AGGR_ID -- this is already separated by class_id, so no need to group by class_id
                ORDER BY
                    NE_ID -- This is unique for each entity so will enshure correct ordering
                LIMIT 100 OFFSET 0;
            """

            self.log_query_plan(stats_sql)

            # Execute the query
            self.cursor.execute(stats_sql)

            # Fetch all results
            results = self.cursor.fetchall()

            # Print the results (optional - for verification)
            for row in results:
                aggr_id, frequency, doc_count = row
                self.logger.info(
                    f"AGGR_ID: {aggr_id}, Frequency: {frequency}, Document Count: {doc_count}"
                )

            self.logger.info("Aggregated entity statistics calculated and printed.")

        except sqlite3.Error as e:
            self.logger.error(f"Error calculating aggregated entity statistics: {e}")
            raise

    def stats_aggregated_threaded(self):
        """
        Calculates statistics based on the aggregated entities in the compiled view using ReaderWriterPair.
        """
        try:
            self.logger.info("Creating indexes for optimized queries...")

            # Index on eo.document_id
            self.cursor.execute(
                f"""
                    CREATE INDEX IF NOT EXISTS idx_eo_doc_id ON {TABLE_NE}({DOC_ID});
                """
            )

            # Covering index for doc_id and aggrgated_id
            self.cursor.execute(
                f"""
                    CREATE INDEX IF NOT EXISTS idx_eo_doc_id_aggr_id ON {TABLE_NE}({DOC_ID}, {NE_NORM_ID});
                """
            )

            # Covering index on TABLE_NE_AGGR
            self.cursor.execute(
                f"""
                    CREATE INDEX IF NOT EXISTS idx_nea_norm_id_doc_id ON {TABLE_NE_AGGR}({NE_NORM_ID});
                """
            )

            # Covering index on TABLE_NE_AGGR for doc_count
            self.cursor.execute(
                f"""
                    CREATE INDEX IF NOT EXISTS idx_nea_doc_count ON {TABLE_NE_AGGR}(doc_count);
                """
            )

            self.cursor.execute(
                f"""-- Create a covering index for the most used columns
                CREATE INDEX IF NOT EXISTS idx_eo_stats_covering ON {TABLE_NE} (
                    norm_id,  -- For the WHERE clause and GROUP BY
                    document_id,  -- For COUNT(DISTINCT)
                    id  -- For ORDER BY
                );
                """
            )

            self.conn.commit()

            self.cursor.execute(f"""--sql ANALYZE {TABLE_NE}""")
            self.cursor.execute(f"""--sql ANALYZE {TABLE_NE_AGGR}""")
            self.logger.info("Indexes created successfully.")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating indexes: {e}")
            raise

        total_rows = self.cursor.execute(
            f"""--sql
            SELECT COUNT(*)
            FROM {TABLE_NE_AGGR}
            WHERE DOC_COUNT IS NULL -- view joins with aggrreagated table to get doc_count
            """
        ).fetchone()[0]

        self.logger.info(
            f"Calculating aggregated entity statistics for {total_rows:,} entities..."
        )
        self.logger.info("This may take a while.")

        # Define Reader query for aggregated entity statistics

        reader_query_stats = f"""
            SELECT
                COUNT(*) AS {FQ},
                COUNT(DISTINCT DOC_ID) AS {DOC_COUNT},
                AGGR_ID
            FROM
                {VIEW_NE_STATS}
            WHERE
                DOC_COUNT IS NULL
                AND AGGR_ID IS NOT NULL
            GROUP BY
                AGGR_ID
            ORDER BY
                NE_ID
            LIMIT :limit OFFSET :offset
        """

        self.log_query_plan(reader_query_stats, params={"limit": 100, "offset": 0})

        # Process function to get the entity ID pairs
        def process_function_stats(batch, conn_params):
            return batch

        # Write function to update norm_id in eo_lookup
        def write_function_stats(batch, cursor, conn):
            update_sql = f"""
                UPDATE {TABLE_NE_AGGR}
                SET fq = ?,
                    doc_count = ?
                WHERE {NE_NORM_ID} = ?
            """
            cursor.executemany(update_sql, batch)

        # Run ReaderWriterPair for aggregated entity statistics
        rw_pair_stats = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            reader_query=reader_query_stats,
            batch_size=50000,
            num_reader_threads=32,
            max_queue_size=1000,
            process_function=process_function_stats,
            write_function=write_function_stats,
            logger=self.logger,
            process_title="aggregated_entity_statistics",
            total_rows=total_rows,
        )
        rw_pair_stats.run()

        self.logger.info("Aggregated entity statistics calculated and updated.")

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
                        {COL_NE_NORM_ID},
                        COUNT(*) as freq
                    FROM entity_occurrences
                    WHERE {COL_NE_NORM_ID} IS NOT NULL
                    GROUP BY document_id, {COL_NE_NORM_ID}
                )
                UPDATE entity_occurrences
                SET intra_doc_fq = (
                    SELECT freq
                    FROM doc_entity_counts
                    WHERE doc_entity_counts.document_id = entity_occurrences.document_id
                    AND doc_entity_counts.{COL_NE_NORM_ID} = entity_occurrences.{COL_NE_NORM_ID}
                )
                WHERE {COL_NE_NORM_ID} IS NOT NULL
            """
            )

            # Get statistics about the update
            self.cursor.execute(
                """
                SELECT
                    COUNT(*) as total_entities,
                    COUNT(DISTINCT document_id) as unique_documents,
                    COUNT(DISTINCT {COL_NE_NORM_ID}) as unique_entities,
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

    def validate_backreferences(self):
        """
        Validate that backreferences between lookup and aggregated tables are consistent.
        This checks both the existing references and potential normalization issues.
        """
        try:
            self.logger.info("Validating backreferences...")

            # First check: Find cases where same normalized text maps to different norm_ids within same class
            normalization_query = f"""
                WITH duplicates AS (
                    SELECT
                        {TXT_NORM},
                        {CLASS_ID},
                        COUNT(DISTINCT {NE_NORM_ID}) as norm_id_count
                    FROM {TABLE_NE_LOOKUP}
                    WHERE {NE_NORM_ID} IS NOT NULL
                    GROUP BY {TXT_NORM}, {CLASS_ID}
                    HAVING norm_id_count > 1
                )
                SELECT
                    l.id,
                    l.{TXT},
                    l.{TXT_NORM},
                    l.{CLASS_ID},
                    l.{NE_NORM_ID},
                    l.{DOC_ID}
                FROM {TABLE_NE_LOOKUP} l
                JOIN duplicates d ON
                    l.{TXT_NORM} = d.{TXT_NORM}
                    AND l.{CLASS_ID} = d.{CLASS_ID}
                ORDER BY l.{TXT_NORM}, l.{CLASS_ID}, l.{NE_NORM_ID}
                LIMIT 100
            """

            self.cursor.execute(normalization_query)
            normalization_issues = self.cursor.fetchall()

            if normalization_issues:
                self.logger.error(
                    f"Found {len(normalization_issues)} normalization issues:"
                )
                self.logger.error(
                    "Same normalized text mapping to different norm_ids within same class:"
                )
                current_norm = None
                for row in normalization_issues:
                    if current_norm != row[2]:  # New normalized text group
                        current_norm = row[2]
                        self.logger.error(
                            f"\nNormalized text: {row[2]}, Class: {row[3]}"
                        )
                    self.logger.error(
                        f"  ID: {row[0]}, Original: {row[1]}, norm_id: {row[4]}, Doc: {row[5]}"
                    )
                return False

            # Second check: Find cases where the references don't match the normalization
            reference_query = f"""
                SELECT
                    l.id,
                    l.{TXT},
                    l.{TXT_NORM},
                    a.{TXT_NORM} as agg_txt_norm,
                    l.{CLASS_ID},
                    a.{CLASS_ID} as agg_class_id,
                    l.{DOC_ID}
                FROM {TABLE_NE_LOOKUP} l
                LEFT JOIN {TABLE_NE_AGGR} a ON
                    l.{NE_NORM_ID} = a.{NE_NORM_ID}
                WHERE
                    l.{NE_NORM_ID} IS NOT NULL
                    AND (
                        l.{TXT_NORM} != a.{TXT_NORM}
                        OR l.{CLASS_ID} != a.{CLASS_ID}
                    )
                LIMIT 100
            """

            self.cursor.execute(reference_query)
            reference_mismatches = self.cursor.fetchall()

            if reference_mismatches:
                self.logger.error(
                    f"\nFound {len(reference_mismatches)} reference mismatches:"
                )
                for row in reference_mismatches:
                    self.logger.error(
                        f"ID: {row[0]}, Text: {row[1]}, "
                        f"Lookup(norm={row[2]}, class={row[4]}), "
                        f"Aggr(norm={row[3]}, class={row[5]}), "
                        f"Doc: {row[6]}"
                    )
                return False

            self.logger.info("All backreferences are consistent")
            return True

        except sqlite3.Error as e:
            self.logger.error(f"Error validating backreferences: {e}")
            return False


class EntityCooccurence:
    def __init__(self, conn, cursor, logger, log_query_plan, conn_params_dict):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger
        self.log_query_plan = log_query_plan
        self.conn_params_dict = conn_params_dict

    def record_entity_cooccurrences(
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

    def co_aggregate_old(
        self, batch_size=50000, ignore_entities_with_error_codes: bool = True
    ) -> None:
        """
        Aggregate entity cooccurrences based on normalized entity IDs from entity_occurrences_summary.
        Uses existing relationships through entity_occurrences.{COL_NE_NORM_ID} to get normalized IDs.
        """
        self.logger.info("Starting cooccurrence aggregation...")

        # Create temporary table for staging aggregated results
        query = f"""
            CREATE TEMPORARY TABLE tmp_cooccurrences AS
            WITH normalized_pairs AS (
                SELECT
                    CASE WHEN eo1.{NE_NORM_ID} <= eo2.{NE_NORM_ID}
                        THEN eo1.{NE_NORM_ID}
                        ELSE eo2.{NE_NORM_ID} END AS e1_id_normalized,
                    CASE WHEN eo1.{NE_NORM_ID} <= eo2.{NE_NORM_ID}
                        THEN eo2.{NE_NORM_ID}
                        ELSE eo1.{NE_NORM_ID} END AS e2_id_normalized,
                    eo1.document_id,
                    CASE WHEN eo1.sentence_index = eo2.sentence_index THEN 1 ELSE 0 END as same_sentence
                FROM entity_cooccurrences ec
                JOIN entity_occurrences eo1 ON ec.e1_id = eo1.id
                JOIN entity_occurrences eo2 ON ec.e2_id = eo2.id
                WHERE eo1.{NE_NORM_ID} IS NOT NULL
                AND eo2.{NE_NORM_ID} IS NOT NULL
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

    def populate_aggr_pairs(
        self,
        batch_size=20000,
        ignore_entities_with_error_codes: bool = True,
        num_reader_threads=16,
    ) -> None:
        """
        Aggregate entity cooccurrences based on normalized entity IDs from entity_occurrences_summary.
        Uses existing relationships through entity_occurrences.{COL_NE_NORM_ID} to get normalized IDs.
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
            return f"""--sql
                SELECT DISTINCT -- Not necessary, but reduces data size passed to reader
                        CASE WHEN eo1.{NE_NORM_ID} < eo2.{NE_NORM_ID}
                            THEN eo1.{NE_NORM_ID}
                            ELSE eo2.{NE_NORM_ID} END AS e1_id,
                        CASE WHEN eo1.{NE_NORM_ID} < eo2.{NE_NORM_ID}
                            THEN eo2.{NE_NORM_ID}
                            ELSE eo1.{NE_NORM_ID} END AS e2_id
                    FROM {TABLE_COOCCURRENCES} ec
                    JOIN {TABLE_NE} eo1 ON ec.e1_id = eo1.id
                    JOIN {TABLE_NE} eo2 ON ec.e2_id = eo2.id
                    WHERE ec.aggr_id IS NULL
                    ORDER BY ec.e1_id, ec.e2_id -- Apply ordering before aggregation
                    LIMIT :limit -- Apply limit and offset for proper batch partitioning
                    OFFSET :offset
            """

        self.log_query_plan(reader_query())  # Log query plan for reader query

        def aggregation_process_function(batch, conn_params):
            """Processes a batch of aggregated co-occurrence data (minimal processing)."""
            return batch  # No processing needed, data is aggregated in the reader query

        def aggregation_write_function(batch, cursor, conn):
            """Writes aggregated co-occurrence data to entity_cooccurrences_summary."""
            sql = f"""--sql
                INSERT OR REPLACE INTO {TABLE_CO_AGGR} -- Use REPLACE to handle duplicates
                    (e1_id, e2_id)
                VALUES (?, ?)
            """

            cursor.executemany(sql, batch)

        reader_writer_pair = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            reader_query=reader_query(),
            process_function=aggregation_process_function,
            write_function=aggregation_write_function,
            batch_size=batch_size,
            num_reader_threads=num_reader_threads,
            profiling_reader_enabled=False,
            profiling_writer_enabled=False,
            writer_batch_chunking=1,
            total_rows=self.cursor.execute(
                f"SELECT COUNT(*) FROM {TABLE_COOCCURRENCES} WHERE aggr_id IS NULL"
            ).fetchone()[0],
            process_title="Co-occurrence Aggregation",
            logger=self.logger,
        )

        self.logger.info(f"Starting ReaderWriterPair process.")
        reader_writer_pair.run()

        self.logger.info("Cooccurrence aggregation using ReaderWriterPair completed")

    def calc_fqs_uniq_doc_count(self):
        """
        Calculate frequency and unique document count for each entity co-occurrence pair.
        Gets a batch of normalized entity pairs from entity_cooccurrences_summary and
        calculates the frequency and unique document count for each pair by joining with
        TABLE cooccurences to fetch each cooccurence for the normalized entity pair and then
        counting the frequencies and unique document count.
        """

        self.logger.info(
            "Starting calculation of frequency and unique document count..."
        )

        def reader_query():
            """Reader query to fetch aggregated co-occurrence data with normalized pair batching - INLINED normalized_pairs CTE."""
            return f"""--sql
                WITH cooccurrences AS (
                    SELECT
                        ne1.{NE_NORM_ID} AS e1_norm_id,
                        ne2.{NE_NORM_ID} AS e2_norm_id,
                        ne1.document_id,
                        CASE WHEN ne1.sentence_index = ne2.sentence_index THEN 1 ELSE 0 END as same_sentence
                    FROM {TABLE_COOCCURRENCES} AS co
                    JOIN {TABLE_NE} AS ne1 ON co.e1_id = ne1.id
                    JOIN {TABLE_NE} AS ne2 ON co.e2_id = ne2.id
                    WHERE EXISTS (
                        SELECT 1
                        FROM {TABLE_CO_AGGR} AS coa  -- Inlined normalized_pairs CTE - directly querying TABLE_CO_AGGR
                        WHERE
                            CASE WHEN ne1.{NE_NORM_ID} < ne2.{NE_NORM_ID}
                                THEN ne1.{NE_NORM_ID}
                                ELSE ne2.{NE_NORM_ID} END = coa.e1_id_normalized
                        AND
                            CASE WHEN ne1.{NE_NORM_ID} < ne2.{NE_NORM_ID}
                                THEN ne2.{NE_NORM_ID}
                                ELSE ne1.{NE_NORM_ID} END = coa.e2_id_normalized
                        ORDER BY coa.e1_id_normalized, coa.e2_id_normalized  -- ORDER BY kept for consistency, though might not be strictly needed in EXISTS
                        LIMIT :limit OFFSET :offset  -- LIMIT/OFFSET kept, but now applied to subquery in EXISTS - review if this is intended
                    )
                    AND co.aggr_id IS NULL  -- **FILTER: Only get co-occurrences with aggr_id IS NULL (not yet aggregated)**
                )
                SELECT
                    co.e1_norm_id,
                    co.e2_norm_id,
                    COUNT(*) as fq_document_level,
                    SUM(co.same_sentence) as fq_sentence_level,
                    COUNT(DISTINCT co.document_id) as uniq_documents
                FROM cooccurrences co
                GROUP BY co.e1_norm_id, co.e2_norm_id -- Group by normalized entity pairs
            """

        self.log_query_plan(reader_query())

        def aggregation_write_function(batch, cursor, conn):
            """Writes aggregated co-occurrence data to entity_cooccurrences_summary."""
            sql = f"""--sql
                UPDATE {TABLE_CO_AGGR}
                SET fq_document_level = ?,
                    fq_sentence_level = ?,
                    uniq_documents = ?
                WHERE e1_id_normalized = ? AND e2_id_normalized = ?
            """
            cursor.executemany(sql, batch)

        rw_pair = ReaderWriterPair(
            conn_params=self.conn_params_dict,
            reader_query=reader_query(),
            process_function=lambda x, y: x,
            write_function=aggregation_write_function,
            batch_size=50000,
            num_reader_threads=16,
            profiling_reader_enabled=False,
            profiling_writer_enabled=False,
            writer_batch_chunking=1,
            total_rows=self.cursor.execute(
                f"SELECT COUNT(*) FROM {TABLE_COOCCURRENCES} WHERE aggr_id IS NULL"
            ).fetchone()[0],
            process_title="Co-occurrence Frequency Calculation",
            logger=self.logger,
        )

    def calc_fqs_uniq_doc_count_single_function(self):
        pass

    def co_aggregate_single_function(
        self, ignore_entities_with_error_codes: bool = True
    ):
        """
        Aggregate entity cooccurrences in a single function call (no batching).
        """

        self.logger.info("Starting single-function cooccurrence aggregation...")

        ignore_error_code_condition_eo1 = (
            "AND eo1.error_id IS NULL" if ignore_entities_with_error_codes else ""
        )
        ignore_error_code_condition_eo2 = (
            "AND eo2.error_id IS NULL" if ignore_entities_with_error_codes else ""
        )

        def reader_query():
            """Reader query to fetch aggregated co-occurrence data (no limit/offset)."""
            return f"""--sql
                WITH normalized_pairs AS (
                    SELECT
                        CASE WHEN eo1.{NE_NORM_ID} < eo2.{NE_NORM_ID}
                            THEN eo1.{NE_NORM_ID}
                            ELSE eo2.{NE_NORM_ID} END AS e1_id_normalized,
                        CASE WHEN eo1.{NE_NORM_ID} < eo2.{NE_NORM_ID}
                            THEN eo2.{NE_NORM_ID}
                            ELSE eo1.{NE_NORM_ID} END AS e2_id_normalized,
                        eo1.document_id,
                        CASE WHEN eo1.sentence_index = eo2.sentence_index THEN 1 ELSE 0 END as same_sentence
                    FROM entity_cooccurrences ec
                    JOIN entity_occurrences eo1 ON ec.e1_id = eo1.id
                    JOIN entity_occurrences eo2 ON ec.e2_id = eo2.id
                    WHERE eo1.{NE_NORM_ID} IS NOT NULL
                    AND eo2.{NE_NORM_ID} IS NOT NULL
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

        query = reader_query()
        self.log_query_plan(query)

        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(query)  # Execute the query

        aggregated_data = cursor.fetchall()  # Fetch all results into memory!

        # --- Write function (similar to batched version) ---
        def aggregation_write_function(
            batch, cursor_param, conn_param
        ):  # Simplified write function
            """Writes aggregated co-occurrence data to entity_cooccurrences_summary."""
            sql = f"""--sql
                INSERT OR REPLACE INTO {TABLE_CO_AGGR}
                    (e1_id, e2_id, fq_document_level,
                    fq_sentence_level, uniq_documents)
                VALUES (?, ?, ?, ?, ?)
            """
            cursor_param.executemany(sql, batch)

        aggregation_write_function(
            aggregated_data, cursor, conn
        )  # Write all aggregated data

        conn.commit()
        conn.close()

        self.logger.info("Single-function cooccurrence aggregation completed")

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
                total_rows=self.cursor.execute(
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
                        MIN(eo1.{COL_NE_NORM_ID}, eo2.{COL_NE_NORM_ID}),
                        MAX(eo1.{COL_NE_NORM_ID}, eo2.{COL_NE_NORM_ID}),
                        eo1.document_id,
                        COUNT(ec.e1_id)
                    FROM entity_cooccurrences ec
                    JOIN entity_occurrences eo1 ON ec.e1_id = eo1.id
                    JOIN entity_occurrences eo2 ON ec.e2_id = eo2.id
                    WHERE eo1.document_id = eo2.document_id
                    GROUP BY MIN(eo1.{COL_NE_NORM_ID}, eo2.{COL_NE_NORM_ID}), MAX(eo1.{COL_NE_NORM_ID}, eo2.{COL_NE_NORM_ID}), eo1.document_id
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

            # Step 3: Batch update {COL_NE_NORM_ID} using covering index
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
                    SET {NE_NORM_ID} = (
                        SELECT id
                        FROM entity_cooccurrences_summary
                        WHERE e1_id_normalized = MIN(eo1.{NE_NORM_ID}, eo2.{NE_NORM_ID})
                        AND e2_id_normalized = MAX(eo1.{NE_NORM_ID}, eo2.{NE_NORM_ID})
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
