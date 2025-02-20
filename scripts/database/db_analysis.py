import logging
import os
from datetime import datetime
import sqlite3
from db_statistics import DBStatistics
from db_data_exchanger import DBDataExchanger
import math
from tqdm import tqdm
import time

class DBAnalysis:

    def __init__(
        self,
        conn: sqlite3.Connection,
        cursor: sqlite3.Cursor,
        logger: logging.Logger,
        data_exchanger: DBDataExchanger,
        log_query_plan,
        execute_with_log

    ):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger
        self.data_exchanger = data_exchanger
        self.statistics = DBStatistics(
            conn, cursor, logger, data_exchanger=data_exchanger
        )  # Initialize DBStatistics
        self.log_query_plan = log_query_plan
        self.execute_with_log = execute_with_log


    def calc_document_counts(self, batch_size=100000):
        """
        Calculate word count, token count, and alphabetic character count for each document in batches.
        Updates documents where word_count or token_count are null or zero.
        Alpha count is allowed to be zero.

        Args:
            batch_size (int): The number of documents to process in each batch.
        """

        self.logger.info("Starting document counts calculation")
        start_time = time.time()

        # Check documents with null counts or zero word/token counts
        self.cursor.execute(
            """
            SELECT id
            FROM documents
            WHERE word_count IS NULL OR token_count IS NULL
                OR word_count = 0 OR token_count = 0
            """
        )
        documents_to_update = self.cursor.fetchall()
        total_to_update = len(documents_to_update)

        self.cursor.execute("SELECT COUNT(*) FROM documents")
        total_documents = self.cursor.fetchone()[0]

        self.logger.info(f"Total documents in database: {total_documents}")
        self.logger.info(f"Documents needing update: {total_to_update}")

        if total_to_update == 0:
            self.logger.info(
                "All documents have valid word and token counts. No action needed."
            )
            return

        # Create temporary indices for better performance
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS temp_sentences_doc_id ON sentences(document_id)"
        )

        # Process documents in batches
        processed = 0
        with tqdm(total=total_to_update, desc="Calculating document counts") as pbar:
            try:
                for i in range(0, total_to_update, batch_size):
                    batch_docs = documents_to_update[i : i + batch_size]
                    batch_ids = [doc[0] for doc in batch_docs]
                    batch_start = time.time()

                    self.logger.debug(
                        f"Processing batch {i//batch_size + 1}/{(total_to_update + batch_size - 1)//batch_size}"
                    )
                    self.logger.debug(f"Batch size: {len(batch_ids)} documents")

                    # Update counts for the current batch
                    query_start = time.time()
                    self.cursor.execute(
                        """
                        WITH document_stats AS (
                            SELECT 
                                document_id,
                                SUM(COALESCE(word_count, 0)) as total_words,
                                SUM(COALESCE(token_count, 0)) as total_tokens,
                                SUM(COALESCE(alpha_count, 0)) as total_alpha
                            FROM sentences
                            WHERE document_id IN (%s)
                            GROUP BY document_id
                            HAVING total_words > 0 AND total_tokens > 0
                        )
                        UPDATE documents
                        SET 
                            word_count = (
                                SELECT total_words 
                                FROM document_stats 
                                WHERE document_stats.document_id = documents.id
                            ),
                            token_count = (
                                SELECT total_tokens 
                                FROM document_stats 
                                WHERE document_stats.document_id = documents.id
                            ),
                            alpha_count = (
                                SELECT total_alpha 
                                FROM document_stats 
                                WHERE document_stats.document_id = documents.id
                            )
                        WHERE id IN (%s)
                        """
                        % (
                            ",".join("?" * len(batch_ids)),
                            ",".join("?" * len(batch_ids)),
                        ),
                        batch_ids + batch_ids,
                    )
                    query_time = time.time() - query_start

                    batch_processed = len(batch_ids)
                    commit_start = time.time()
                    self.conn.commit()
                    commit_time = time.time() - commit_start

                    processed += batch_processed
                    batch_time = time.time() - batch_start

                    self.logger.debug(
                        f"Batch timing:"
                        f"\n  - Query execution: {query_time:.2f}s"
                        f"\n  - Commit: {commit_time:.2f}s"
                        f"\n  - Total batch time: {batch_time:.2f}s"
                        f"\n  - Documents processed: {batch_processed}"
                        f"\n  - Processing rate: {batch_processed/batch_time:.1f} docs/s"
                    )

                    pbar.set_postfix(
                        {
                            "docs/s": f"{batch_processed/batch_time:.1f}",
                            "processed": processed,
                            "remaining": total_to_update - processed,
                        }
                    )
                    pbar.update(batch_processed)

            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Error during document count calculation: {str(e)}")
                raise
            finally:
                # Clean up temporary index
                self.cursor.execute("DROP INDEX IF EXISTS temp_sentences_doc_id")

        total_time = time.time() - start_time
        self.logger.info(
            f"Document count calculation completed. Processed {processed} documents in {total_time:.2f}s"
        )

        # Verify results and provide detailed statistics
        self.cursor.execute(
            """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN word_count IS NULL OR token_count IS NULL THEN 1 ELSE 0 END) as null_counts,
                SUM(CASE WHEN word_count = 0 OR token_count = 0 THEN 1 ELSE 0 END) as zero_counts,
                AVG(CASE WHEN word_count > 0 THEN word_count END) as avg_words,
                AVG(CASE WHEN token_count > 0 THEN token_count END) as avg_tokens,
                AVG(alpha_count) as avg_alpha
            FROM documents
            """
        )
        stats = self.cursor.fetchone()
        self.logger.info(f"Final statistics:")
        self.logger.info(f"- Documents with null word/token counts: {stats[1]}")
        self.logger.info(f"- Documents with zero word/token counts: {stats[2]}")
        self.logger.info(f"- Average word count: {stats[3]:.2f}")
        self.logger.info(f"- Average token count: {stats[4]:.2f}")
        self.logger.info(f"- Average alpha count: {stats[5]:.2f}")

        if stats[1] > 0 or stats[2] > 0:
            self.logger.warning(
                f"There are still {stats[1]} documents with null counts and {stats[2]} with zero word/token counts"
            )
        else:
            self.logger.info(
                "All documents have been successfully processed with valid word and token counts"
            )

    def check_sentence_counts(self):
        """
        Check and report statistics about sentence-level counts.
        """
        self.logger.info("Analyzing sentence-level counts...")

        self.cursor.execute(
            """
            SELECT 
                COUNT(*) as total_sentences,
                SUM(CASE WHEN word_count IS NULL THEN 1 ELSE 0 END) as null_word_count,
                SUM(CASE WHEN token_count IS NULL THEN 1 ELSE 0 END) as null_token_count,
                SUM(CASE WHEN word_count = 0 THEN 1 ELSE 0 END) as zero_word_count,
                SUM(CASE WHEN token_count = 0 THEN 1 ELSE 0 END) as zero_token_count,
                AVG(CASE WHEN word_count > 0 THEN word_count END) as avg_words,
                AVG(CASE WHEN token_count > 0 THEN token_count END) as avg_tokens,
                AVG(alpha_count) as avg_alpha
            FROM sentences
            """
        )
        stats = self.cursor.fetchone()

        self.logger.info("Sentence-level statistics:")
        self.logger.info(f"Total sentences: {stats[0]}")
        self.logger.info(f"Sentences with null counts:")
        self.logger.info(f"- Word count: {stats[1]}")
        self.logger.info(f"- Token count: {stats[2]}")
        self.logger.info(f"Sentences with zero counts:")
        self.logger.info(f"- Word count: {stats[3]}")
        self.logger.info(f"- Token count: {stats[4]}")
        self.logger.info(f"Average counts:")
        self.logger.info(f"- Words per sentence: {stats[5]:.2f}")
        self.logger.info(f"- Tokens per sentence: {stats[6]:.2f}")
        self.logger.info(f"- Alpha chars per sentence: {stats[7]:.2f}")

        # Sample some sentences with word/token count issues for inspection
        self.cursor.execute(
            """
            SELECT document_id, sentence_index, text, word_count, token_count, alpha_count
            FROM sentences
            WHERE word_count IS NULL OR token_count IS NULL
                OR word_count = 0 OR token_count = 0
            LIMIT 5
            """
        )
        problem_samples = self.cursor.fetchall()

        if problem_samples:
            self.logger.info("\nSample problematic sentences:")
            for sample in problem_samples:
                self.logger.info(
                    f"Doc {sample[0]}, Sentence {sample[1]}:"
                    f"\n  Text: {sample[2]}"
                    f"\n  Counts (word/token/alpha): {sample[3]}/{sample[4]}/{sample[5]}"
                )

        return stats

    def export_problematic_documents(self, output_path=None):
        """
        Export documents with null or zero word/token counts to a JSON file.
        Prompts user for confirmation and output path if not provided.

        Args:
            output_path (str, optional): Path where to save the JSON file.
                                       If not provided, will create in results directory.
        """
        # Get problematic document IDs and stats
        self.cursor.execute(
            """
            WITH problematic_docs AS (
                SELECT id
                FROM documents
                WHERE word_count IS NULL OR token_count IS NULL
                    OR word_count = 0 OR token_count = 0
            )
            SELECT 
                p.id,
                COUNT(*) OVER () as total_count,
                SUM(CASE WHEN d.word_count IS NULL THEN 1 ELSE 0 END) OVER () as null_word_count,
                SUM(CASE WHEN d.token_count IS NULL THEN 1 ELSE 0 END) OVER () as null_token_count,
                SUM(CASE WHEN d.word_count = 0 THEN 1 ELSE 0 END) OVER () as zero_word_count,
                SUM(CASE WHEN d.token_count = 0 THEN 1 ELSE 0 END) OVER () as zero_token_count
            FROM problematic_docs p
            JOIN documents d ON p.id = d.id
            """
        )
        rows = self.cursor.fetchall()
        if not rows:
            self.logger.info("No problematic documents found to export")
            return

        doc_ids = [row[0] for row in rows]
        stats = rows[0][1:]  # Stats are the same for all rows due to window functions

        stats_msg = (
            f"Found {stats[0]} problematic documents:\n"
            f"- Documents with null word count: {stats[1]}\n"
            f"- Documents with null token count: {stats[2]}\n"
            f"- Documents with zero word count: {stats[3]}\n"
            f"- Documents with zero token count: {stats[4]}"
        )
        print(stats_msg)
        user_response = input("Export these documents to JSON? (y/n): ")
        if user_response.lower() != "y":
            self.logger.info("Export cancelled by user")
            return

        if output_path is None:
            default_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "results",
                f'problematic_documents_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json',
            )
            output_path = (
                input(f"Enter output path (default: {default_path}): ").strip()
                or default_path
            )

        metadata = {
            "statistics": {
                "null_word_count": stats[1],
                "null_token_count": stats[2],
                "zero_word_count": stats[3],
                "zero_token_count": stats[4],
            }
        }

        output_path = self.export_documents_to_json(doc_ids, output_path, metadata)
        self.logger.info(
            f"Exported {len(doc_ids)} problematic documents to {output_path}"
        )
        return output_path

    def find_overlapping_entities(self, overwrite: bool = False) -> None:
        """
        Find entities in the same sentence where span_start and span_end overlap between the two entities.
        Record into new column of TABLE entity_occurrences [overlap: boolean].

        This method:
        1. Adds an 'overlap' column if it doesn't exist
        2. Sets all overlap values to FALSE initially
        3. Identifies pairs of entities that overlap within the same sentence
        4. Updates the overlap flag for all overlapping entities
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
                self.logger.info(
                    "Adding 'overlap' column to entity_occurrences table..."
                )
                self.cursor.execute(
                    """
                    ALTER TABLE entity_occurrences 
                    ADD COLUMN overlap BOOLEAN DEFAULT FALSE
                """
                )
            if overwrite:
                # Reset all overlap flags to FALSE
                self.cursor.execute(
                    """
                    UPDATE entity_occurrences 
                    SET overlap = FALSE
                """
                )

            self.cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_entity_occurrences_document_id 
                ON entity_occurrences(document_id, sentence_index, id)
                """
            ) # Create index for faster processing, This index will allow SQLite to directly locate the matching rows without scanning a range.

            self.logger.info("Finding overlapping entities...")

            # Find overlapping entities within the same sentence
            # Two entities overlap if:
            # - They are in the same document and sentence
            # - One entity's span intersects with another's span
            # - They are different entities (different IDs)
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

        try:
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
                self.logger.info(
                    "Recording new co-occurrences using batch insertions..."
                )
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

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error identifying co-occurrences: {e}")
            raise

        finally:
            self.cursor.execute("DROP TABLE IF EXISTS temp_new_cooccurrences")

    def count_named_entity_fq(self):
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

    def create_temp_normalized_entities(self):
        """
        Create a temporary table with normalized entity text and entity_id.
        """
        try:
            self.logger.info("Creating temporary normalized entities table...")

            # Drop temporary table if it exists
            self.cursor.execute("DROP TABLE IF EXISTS temp_normalized_linked_entities")

            # Entity Linking Lookup Table
            create_table = f"""
                CREATE TABLE temp_normalized_linked_entities (
                    id INTEGER,
                    entity_id INTEGER,
                    normalized_entity_text TEXT,
                    FOREIGN KEY (id) REFERENCES entity_occurrences(id)
                    )"""
            self.cursor.execute(create_table)

            self.logger.info("Temporary normalized entities table created.")

            query_string = f"""
                INSERT INTO temp_normalized_linked_entities (id, entity_id, normalized_entity_text)    
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
            self.logger.error(f"Error creating temporary normalized entities table: {e}")


    def normalize_entity_text_column(self, table_name: str = "temp_normalized_linked_entities", column_name: str = "normalized_entity_text"):
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

            print(f"Step 1: Removing leading whitespace from column '{column_name}' in table '{table_name}'...")
            self.cursor.execute(sql_remove_leading_whitespace)
            print("Leading whitespace removal completed.")

            print(f"Step 2: Removing leading '\" ' from column '{column_name}' in table '{table_name}'...")
            self.cursor.execute(sql_remove_leading_quote_space)
            print("Leading '\" ' removal completed.")

            # 5. Commit the changes to the database
            self.conn.commit()
            print("Changes committed to the database.")

        except sqlite3.Error as e:
            print(f"Database error: {e}")
    def create_aggregated_entities_from_temp(self, batch_size=
    5000, target_table="temp_entity_occurrences_summary"):
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
            "CREATE INDEX IF NOT EXISTS idx_ne_text_entity_id ON ? (normalized_entity_text, entity_id)", (target_table,)
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
                    (target_table, target_table, batch_size, offset)
                )
                self.connection.commit()

                row_count = self.cursor.rowcount
                if row_count < batch_size:
                    break
                offset += batch_size

                print(f"Batch processed, rows inserted/attempted: {row_count}, offset: {offset}") # Added feedback

            print("Batch processing complete with WHERE NOT EXISTS.")

            self.conn.commit()
            rows_before = self.cursor.execute("SELECT COUNT(*) FROM ?").fetchone()[0]
            rows_after = self.cursor.execute("SELECT COUNT(*) FROM ?", (target_table,)).fetchone()[0]
            self.logger.info(f"Aggregated entities created from temp table. Raw entity count: {rows_before}, Aggregated entity count: {rows_after}")

        except sqlite3.Error as e:
            self.logger.error(f"Error creating aggregated entities: {e}")
            self.conn.rollback()
        except KeyboardInterrupt:
            self.conn.rollback()
            self.logger.error("Aggregated entity creation cancelled by user")
    def aggregate_entity_occurrences(
        self, batch_size=10000, ignore_error_occurrences: bool = True
    ) -> None:
        """
        Aggregates the fq of unique TABLE entity_occurrences and records in entity_occurrences_summary, adds reference to summary table in entity_occurrences['summary_id']
        For each entity_occurence record a reference to the linked normalized entity in entity_occurrences_summary in column [summary_id].

        Pre-processing to link entities to the correct summary entity text
            - Normalize entity_text to lowercase
            - Remove leading and trailing whitespace
            - Remove leading punctuation
            - Remove trailing 's

        Rules for unique entity:
        - entity_text is unique

        Args:
            batch_size (int): Number of records to process in each batch for memory efficiency
        """
        try:
            self.logger.info("Starting entity occurrences summarization...")

            # Recreate entity_occurrences_summary table with entity_id
            # self.cursor.execute("DROP TABLE IF EXISTS entity_occurrences_summary")
            # self.cursor.execute(
            #     """
            #     CREATE TABLE entity_occurrences_summary (
            #         id INTEGER PRIMARY KEY NOT NULL,
            #         normalized_entity_text TEXT NOT NULL,
            #         entity_id INTEGER NOT NULL,
            #         uniq_documents INTEGER,
            #         fq INTEGER,
            #         UNIQUE(normalized_entity_text, entity_id),
            #         FOREIGN KEY (entity_id) REFERENCES named_entities (id)
            #     )
            # """
            # )

            # Drop temporary table if it exists
            self.cursor.execute("DROP TABLE IF EXISTS temp_normalized_entities")

            # Create a temporary table for normalized texts
            query_string = f"""
                CREATE TEMPORARY TABLE temp_normalized_entities AS
                SELECT 
                    id,
                    entity_id,
                    TRIM(
                        LOWER(
                            CASE 
                                WHEN entity_text LIKE '%''s' THEN SUBSTR(entity_text, 1, LENGTH(entity_text) - 2)
                                WHEN entity_text LIKE '%''s' THEN SUBSTR(entity_text, 1, LENGTH(entity_text) - 2)
                                ELSE entity_text 
                            END
                        )
                    ) as normalized_entity_text,
                    error_id
                FROM entity_occurrences
                WHERE entity_text IS NOT NULL 
                AND overlap = FALSE
                AND error_id IS NULL  -- Explicitly filter out error entities here
            """
            self.logger.debug(f"Query string for entity occurrence aggregation: {query_string}")
            self.cursor.execute(query_string)

            # Remove leading non-alphanumeric characters
            self.cursor.execute(
                """
                WITH RECURSIVE
                strip_leading(id, entity_id, txt, n) AS (
                    SELECT id, entity_id, normalized_entity_text, 1
                    FROM temp_normalized_entities
                    UNION ALL
                    SELECT id, entity_id, SUBSTR(txt, 2), n + 1
                    FROM strip_leading
                    WHERE LENGTH(txt) > 0 
                    AND SUBSTR(txt, 1, 1) NOT GLOB '[A-Za-z0-9]*'
                )
                UPDATE temp_normalized_entities
                SET normalized_entity_text = (
                    SELECT txt
                    FROM strip_leading s
                    WHERE s.id = temp_normalized_entities.id
                    AND (
                        LENGTH(s.txt) = 0 
                        OR SUBSTR(s.txt, 1, 1) GLOB '[A-Za-z0-9]*'
                    )
                    LIMIT 1
                )
            """
            )

            # Test that no entities in temp_normalized_entities have error codes
            self.cursor.execute(
                """
                SELECT COUNT(*)
                FROM temp_normalized_entities
                WHERE error_id IS NOT NULL
            """
            )
            error_count = self.cursor.fetchone()[0]
            if error_count > 0:
                self.logger.warning(
                    f"Found {error_count} entities with error codes in temp_normalized_entities"
                )
            self.logger.info(
                f"Normalized entity texts prepared, {error_count} error counts"
            )

            # Insert summaries into entity_occurrences_summary considering entity_id
            self.cursor.execute(
                """
                WITH error_entities AS (
                    -- Get all normalized forms that have error codes anywhere
                    SELECT DISTINCT TRIM(LOWER(entity_text)) as error_text
                    FROM entity_occurrences 
                    WHERE error_id IS NOT NULL
                )
                INSERT INTO entity_occurrences_summary (normalized_entity_text, entity_id, uniq_documents, fq)
                SELECT 
                    ne.normalized_entity_text,
                    ne.entity_id,
                    COUNT(DISTINCT eo.document_id) as uniq_documents,
                    COUNT(*) as fq
                FROM temp_normalized_entities ne
                JOIN entity_occurrences eo ON eo.id = ne.id 
                WHERE ne.error_id IS NULL  -- Double-check no error entities from normalized table
                AND eo.error_id IS NULL    -- Double-check no error entities from original table
                AND NOT EXISTS (          -- Exclude if any occurrence of this normalized form has an error
                    SELECT 1 
                    FROM error_entities ee
                    WHERE ee.error_text = ne.normalized_entity_text
                )
                GROUP BY ne.normalized_entity_text, ne.entity_id
                ON CONFLICT(normalized_entity_text, entity_id) DO UPDATE SET
                    uniq_documents = excluded.uniq_documents,
                    fq = excluded.fq
            """
            )

            # Test that no entities in entity_occurrences_summary have error codes
            self.cursor.execute(
                """
                SELECT COUNT(*), GROUP_CONCAT(normalized_entity_text)
                FROM entity_occurrences_summary eos
                WHERE EXISTS (
                    SELECT 1 
                    FROM entity_occurrences eo 
                    WHERE eo.entity_text = eos.normalized_entity_text 
                    AND eo.error_id IS NOT NULL
                )
            """
            )
            error_check = self.cursor.fetchone()
            if error_check[0] > 0:
                self.logger.warning(
                    f"Found {error_check[0]} entities with error codes in entity_occurrences_summary: {error_check[1]}"
                )

            # Update summary_id references
            self.cursor.execute(
                """
                SELECT eo.id, s.id as summary_id
                FROM entity_occurrences eo
                JOIN temp_normalized_entities ne ON ne.id = eo.id
                JOIN entity_occurrences_summary s 
                    ON s.normalized_entity_text = ne.normalized_entity_text 
                    AND s.entity_id = ne.entity_id
                WHERE eo.error_id IS NULL
            """
            )

            mappings = self.cursor.fetchall()
            total_records = len(mappings)
            self.logger.info(
                f"Updating summary_id references for {total_records} records in batches of {batch_size}"
            )

            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = mappings[i : i + batch_size]
                self.cursor.executemany(
                    "UPDATE entity_occurrences SET summary_id = ? WHERE id = ?",
                    [(summary_id, eo_id) for eo_id, summary_id in batch],
                )
                self.conn.commit()
                self.logger.debug(
                    f"Processed {min(i + batch_size, total_records)}/{total_records} records"
                )

            # Drop temporary table
            self.cursor.execute("DROP TABLE temp_normalized_entities")

            # Get statistics with entity type information
            self.cursor.execute(
                """
                SELECT 
                    COUNT(*) as total_summaries,
                    AVG(fq) as avg_frequency,
                    SUM(fq) as total_occurrences,
                    AVG(uniq_documents) as avg_documents,
                    COUNT(DISTINCT entity_id) as unique_entity_types
                FROM entity_occurrences_summary
            """
            )
            stats = self.cursor.fetchone()

            # Get per-entity-type statistics
            self.cursor.execute(
                """
                SELECT 
                    ne.named_entity as entity_type,
                    COUNT(*) as total_variants,
                    AVG(eos.fq) as avg_frequency,
                    SUM(eos.fq) as total_occurrences,
                    AVG(eos.uniq_documents) as avg_documents
                FROM entity_occurrences_summary eos
                JOIN named_entities ne ON ne.id = eos.entity_id
                GROUP BY eos.entity_id, ne.named_entity
                ORDER BY total_occurrences DESC
            """
            )
            type_stats = self.cursor.fetchall()

            self.conn.commit()

            self.logger.info(
                f"Entity occurrences summarization complete:\n"
                f"- Total unique normalized entities: {stats[0]}\n"
                f"- Unique entity types: {stats[4]}\n"
                f"- Average frequency per entity: {stats[1]:.2f}\n"
                f"- Total occurrences: {stats[2]}\n"
                f"- Average documents per entity: {stats[3]:.2f}\n"
                f"\nBreakdown by entity type:"
            )

            for type_stat in type_stats:
                self.logger.info(
                    f"\n{type_stat[0]}:\n"
                    f"  - Unique variants: {type_stat[1]}\n"
                    f"  - Average frequency: {type_stat[2]:.2f}\n"
                    f"  - Total occurrences: {type_stat[3]}\n"
                    f"  - Average documents: {type_stat[4]:.2f}"
                )

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error summarizing entity occurrences: {e}")
            raise

    def initialize_entity_aggregated_batched_simplified_stage1_optimized(self, batch_size=300000): # Optimized Stage 1 - 'summary' replaced with 'aggregated' for clarity, ID linking corrected, error handling added
        self.logger.info("Stage 1 (Simplified - Optimized): Initializing entity_occurrences_aggregated (batched - ID Linking) with Error Handling") # Updated log message

        # --- Get total count of records to process for progress bar estimation ---
        self.cursor.execute("SELECT COUNT(*) FROM entity_occurrences WHERE error_id IS NULL AND overlap = FALSE")
        total_records_to_process = self.cursor.fetchone()[0]
        processed_count = 0

        self.cursor.execute("DELETE FROM aggregated_eo") # Clear the table ONCE before processing
        self.conn.commit() # Commit the deletion immediately

        # Create index for faster processing of entity_occurrences
        self.cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entity_occurrences_error_overlap_id 
            ON entity_occurrences (error_id, overlap, id)
            """
        )

        try: # Start of try block for error handling
            min_id = 0  # Initialize minimum ID for keyset pagination
        with tqdm(total=total_records_to_process, desc="Entity Occurrences -> Aggregated entity occurrences") as pbar:
            while True:
                    batch_query = """
                    SELECT id, LOWER(entity_text) as normalized_text, entity_id
                    FROM entity_occurrences
                        WHERE error_id IS NULL AND overlap = FALSE AND id > ?
                        ORDER BY id
                        LIMIT ?
                """
                    self.cursor.execute(batch_query, (min_id, batch_size))
                batch_data = self.cursor.fetchall()
                if not batch_data:
                        break # No more data, exit loop

                    eo_aggregated_id_updates = []
                records_processed_in_batch = 0

                    # --- 1. & 2. INSERT into aggregated_eo (using eo_id as aggregated_eo.id) and Prepare UPDATE using List Comprehension ---
                    insert_data_for_aggregated_eo = [(eo_id, normalized_text, entity_id) for eo_id, normalized_text, entity_id in batch_data] # Include eo_id for aggregated_eo.id
                    insert_aggregated_query = """
                        INSERT INTO aggregated_eo (id, normalized_entity_text, entity_id)  -- Include 'id' in INSERT query
                        VALUES (?, ?, ?)  -- Values now include id
                    """
                    self.cursor.executemany(insert_aggregated_query, insert_data_for_aggregated_eo)

                    eo_aggregated_id_updates = [{'aggregated_id': eo_id, 'eo_id': eo_id} # aggregated_id is now simply eo_id
                                            for eo_id, _, _ in batch_data] # Iterate over batch_data to get eo_id

                    if eo_aggregated_id_updates:
                    # --- 3. Batch UPDATE entity_occurrences.summary_id ---
                    update_query = """
                        UPDATE entity_occurrences SET summary_id = :aggregated_id WHERE id = :eo_id
                    """
                        self.cursor.executemany(update_query, eo_aggregated_id_updates)
                        self.conn.commit() # Commit inside try block - only commit if batch is successful

                        records_processed_in_batch = len(batch_data)
                    processed_count += records_processed_in_batch
                    pbar.update(records_processed_in_batch)
                        self.logger.debug(f"Stage 1 (Simplified - Optimized): Processed {processed_count}/{total_records_to_process} records")

                        min_id = batch_data[-1][0]  # Update min_id to the last ID in the batch for next iteration
                else:
                    break

            self.logger.info("Stage 1 (Simplified - Optimized): Initial entity_occurrences_aggregated initialization complete (ID Linking)") # Success log message

        except KeyboardInterrupt:
            self.conn.rollback()
            self.logger.error("Stage 1 (Simplified - Optimized): KeyboardInterrupt detected. Operations rolled back.")
            raise # Re-raise KeyboardInterrupt to allow caller to handle it if needed

        except Exception as e: # Catch other exceptions
            self.conn.rollback()
            self.logger.error(f"Stage 1 (Simplified - Optimized): Exception occurred: {e}. Operations rolled back.", exc_info=True) # Log full exception info
            raise # Re-raise the exception to allow caller to handle it

    def normalize_entity_summary_text_batched_with_progressbar(self, batch_size=100000):
        self.logger.info("Stage 2: Normalizing normalized_entity_text in aggregated_eo (batched)")

        # --- Get total count of records for normalization for progress bar---
        self.cursor.execute("SELECT COUNT(*) FROM aggregated_eo")
        total_records_to_normalize = self.cursor.fetchone()[0]
        processed_count = 0

        with tqdm(total=total_records_to_normalize, desc="Stage 2 Progress") as pbar: # Initialize tqdm progress bar
            while True:
                batch_query = """
                    SELECT id, normalized_entity_text
                    FROM aggregated_eo
                    WHERE id > ? -- Simple batching based on ID, adjust as needed
                    ORDER BY id
                    LIMIT ?
                """
                self.cursor.execute(batch_query, (processed_count, batch_size))
                batch_data = self.cursor.fetchall()
                if not batch_data:
                    break

                update_records = []
                records_normalized_in_batch = 0 # Counter for records normalized in this batch
                for eos_id, current_text in batch_data:
                    normalized_text = current_text
                    if normalized_text.endswith("'s"):
                        normalized_text = normalized_text[:-2] # Remove possessive 's - Example rule
                    # Add more normalization rules here (plural removal, punctuation, etc.)
                    normalized_text = normalized_text.lower().strip() # Example: Lowercase and trim

                    if normalized_text != current_text: # Only update if normalization changed text
                        update_records.append({'id': eos_id, 'normalized_entity_text': normalized_text})
                        records_normalized_in_batch += 1 # Increment counter for normalized records

                if update_records:
                    update_query = """
                        UPDATE aggregated_eo
                        SET normalized_entity_text = :normalized_entity_text
                        WHERE id = :id
                    """
                    self.cursor.executemany(update_query, update_records)
                    self.conn.commit()
                    processed_count += len(batch_data) # Track processed based on fetched, not updated (as updates are conditional)
                    pbar.update(records_normalized_in_batch) # Update progress bar by the number of records *actually normalized*
                    self.logger.debug(f"Stage 2 (Progress Bar): Normalized text for {processed_count}/{total_records_to_normalize} records (fetched)")
                else:
                    break # Exit loop if no batch data

        self.logger.info("Stage 2 (Progress Bar): Text normalization in aggregated_eo complete")


    def merge_duplicate_aggregated_eos_batched_with_progressbar(self, batch_size=100): # Function name updated to indicate progress bar
        self.logger.info("Stage 3 (Progress Bar): Merging duplicate aggregated_eo (batched)") # Log message updated
        # Create index for faster grouping:
        # QUERY PLAN
        # |--CO-ROUTINE DuplicateGroups
        # |  `--SCAN aggregated_eo USING COVERING INDEX idx_aggregated_eo_dupe_group
        # `--SCAN DuplicateGroups

        # Previous query plan:
        # QUERY PLAN
        # |--CO-ROUTINE DuplicateGroups
        # |  |--SCAN aggregated_eo
        # |  `--USE TEMP B-TREE FOR GROUP BY
        # `--SCAN DuplicateGroups

        self.cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_aggregated_eo_dupe_group
            ON aggregated_eo (normalized_entity_text, entity_id)
            """
        )
        merge_iterations = 0
        try: # <----- TRY BLOCK START for Rollback on Error
            with tqdm(desc="Stage 3 Progress (Merging)") as pbar: # Initialize tqdm progress bar (unknown total initially)
                while True: # Iterate until no more duplicates are found in a batch
                    merge_groups_query = """
                        WITH DuplicateGroups AS (
                            SELECT
                                normalized_entity_text, entity_id,
                                GROUP_CONCAT(id) as ids_to_merge,
                                COUNT(*) as duplicate_count,
                                MIN(id) as keep_id
                            FROM aggregated_eo
                            GROUP BY normalized_entity_text, entity_id
                            HAVING COUNT(*) > 1
                            LIMIT ? -- Limit the number of duplicate groups to process in each batch
                        )
                        SELECT normalized_entity_text, entity_id, ids_to_merge, keep_id
                        FROM DuplicateGroups;
                    """
                    self.cursor.execute(merge_groups_query, (batch_size,)) # <--- Parameterized batch_size
                    duplicate_groups = self.cursor.fetchall()

                    if not duplicate_groups:
                        break # No more duplicates found in this batch, assume all merged for now

                    merge_count_batch = 0
                    update_eo_params_batch = [] # List to collect parameters for executemany (UPDATE entity_occurrences)
                    delete_eo_params_batch = [] # List to collect parameters for executemany (DELETE aggregated_eo)

                    batch_normalized_text_prefix = "N/A" # Default prefix if no groups in batch
                    first_group_processed_in_batch = False # Flag to track first group in batch

                    for normalized_text, entity_id, ids_to_merge_str, keep_id in duplicate_groups:
                        ids_to_merge = [int(x) for x in ids_to_merge_str.split(',')] # Convert IDs to integers
                        ids_to_delete = [id_val for id_val in ids_to_merge if id_val != keep_id]

                        if not ids_to_delete: # Should not happen, but safety check
                            continue

                        # Prepare parameters for executemany (UPDATE entity_occurrences)
                        for id_to_delete in ids_to_delete:
                            update_eo_params_batch.append((keep_id, id_to_delete)) # Tuple of (keep_id, id_to_delete)

                        # Prepare parameters for executemany (DELETE aggregated_eo)
                        delete_eo_params_batch.extend([(id_val,) for id_val in ids_to_delete]) # List of tuples (id_to_delete,)


                        merge_count_batch += 1 # Increment merge count for each group processed (even if actual DB operations batched later)

                        if not first_group_processed_in_batch: # Capture prefix from the first group in batch
                            batch_normalized_text_prefix = normalized_text[:3] if normalized_text else "N/A"
                            first_group_processed_in_batch = True

                    if update_eo_params_batch: # Only execute if there are updates to perform in batch
                        # --- 2. Update entity_occurrences.summary_id (Efficient batch UPDATE with executemany) ---
                        update_eo_summary_id_query = """
                            UPDATE entity_occurrences
                            SET summary_id = ?
                            WHERE summary_id = ?
                        """
                        start_time = time.time() # Profiling start for UPDATE
                        self.cursor.executemany(update_eo_summary_id_query, update_eo_params_batch) # <--- Batch UPDATE with executemany
                        update_eo_time = time.time() - start_time # Profiling end for UPDATE
                        self.logger.debug(f"  Batch UPDATE entity_occurrences with executemany, count={len(update_eo_params_batch)}, time: {update_eo_time:.4f}s, time/count = {update_eo_time/len(update_eo_params_batch):.4f}s")


                    if delete_eo_params_batch: # Only execute if there are deletes to perform in batch
                        # --- 3. DELETE merged summary rows from aggregated_eo (batch DELETE with executemany) ---
                        delete_summaries_query = """
                            DELETE FROM aggregated_eo
                            WHERE id = ?
                        """
                        start_time = time.time() # Profiling start for DELETE
                        self.cursor.executemany(delete_summaries_query, delete_eo_params_batch) # <--- Batch DELETE with executemany
                        delete_summaries_time = time.time() - start_time # Profiling end for DELETE
                        self.logger.debug(f"  Batch DELETE aggregated_eo with executemany, count={len(delete_eo_params_batch)}, time: {delete_summaries_time:.4f}s, time/counnt = {delete_summaries_time/len(delete_eo_params_batch):.4f}s")

                    start_time = time.time() # Profiling start for COMMIT
                    self.conn.commit() # Commit after each batch of merges
                    commit_time = time.time() - start_time # Profiling end for COMMIT
                    self.logger.debug(f") Progress Bar: Merged {merge_count_batch} duplicate groups (prefix: '{batch_normalized_text_prefix}...'). Iteration {merge_iterations}. Committed in {commit_time:.4f}s") # Use batch prefix in commit log too
                    merge_iterations += 1
                    pbar.update(merge_count_batch) # Update progress bar by number of groups merged in batch

                    if merge_count_batch == 0: # No merges in this batch, probably no more duplicates for now
                        break


        except KeyboardInterrupt: # <----- EXCEPT BLOCK for Keyboard Interupt
            self.conn.rollback() # Rollback transaction in case of error
            self.logger.error(f"Stage 3 (Progress Bar): KeyboardInterrupt during merge process: Rolling back transaction") # Log error
            raise # Re-raise the exception to signal failure
        except sqlite3.Error as e: # <----- EXCEPT BLOCK for Rollback on Error
            self.conn.rollback() # Rollback transaction in case of error
            self.logger.error(f"Stage 3 (Progress Bar): Error during merge process: {e}") # Log error
            raise # Re-raise the exception to signal failure
        # finally: # <---- FINALLY block to ensure stats are calculated even if errors occur in merging but transaction is still valid
            # if self.conn: # Check if connection is still valid (to prevent errors if connection itself failed earlier)
            #     self._calculate_and_insert_aggregated_eo_stats() # <---- CALL THE NEW STATS FUNCTION HERE, after all merging iterations

        self.logger.info(f"Stage 3 (Progress Bar): Duplicate aggregated_eo merging complete in {merge_iterations} iterations") # Log message updated

    def init_aggregated_entity_occurrences(self, batch_size=50000):
        self.logger.info(
            "Stage 1: Initializing aggregated table for entity occurrences (batched)"
        )
        # Get total count of entity_occurrences
        self.cursor.execute("SELECT COUNT(*) FROM entity_occurrences")
        total_records = self.cursor.fetchone()[0]

        processed_count = 0
        with tqdm(total=total_records, desc="Initializing aggregated table") as pbar:
            while True:
                batch_query = f"""
                    SELECT id, LOWER(entity_text) as normalized_text, entity_id
                    FROM entity_occurrences
                    WHERE error_id IS NULL 
                    AND overlap = FALSE 
                    AND summary_id IS NULL -- Process only un-summarized, clean records
                    LIMIT {batch_size}
                """
                self.cursor.execute(batch_query)
                batch_data = self.cursor.fetchall()
                if not batch_data:
                    break  # No more records to process

                summary_records = []
                eo_summary_id_updates = []
                for eo_id, normalized_text, entity_id in batch_data:
                    summary_records.append(
                        {
                            "id": eo_id,
                            "normalized_entity_text": normalized_text,
                            "entity_id": entity_id,
                        }
                    )
                    eo_summary_id_updates.append(
                        {"summary_id": eo_id, "eo_id": eo_id}
                    )  # Initial summary_id same as eo_id

                if summary_records:
                    # Batch INSERT into entity_occurrences_summary
                    insert_query = """
                        INSERT OR IGNORE INTO aggregated_eo (id, normalized_entity_text, entity_id)
                        VALUES (:id, :normalized_entity_text, :entity_id)
                    """
                    self.cursor.executemany(insert_query, summary_records)

                    # Batch UPDATE entity_occurrences.summary_id
                    update_query = """
                        UPDATE entity_occurrences SET summary_id = :summary_id WHERE id = :eo_id
                    """
                    self.cursor.executemany(update_query, eo_summary_id_updates)
                    self.conn.commit()  # Commit after each batch

                    processed_count += len(batch_data)
                    self.logger.debug(f"Stage 1: Processed {processed_count} records")
                    pbar.update(len(batch_data))
                else:
                    break  # Exit loop if no batch data

            self.logger.info(
                "Stage 1: Initial entity_occurrences_summary initialization complete"
            )

    def count_entity_intra_doc_fq(self) -> None:
        """
        For each entity, calculate the frequency of the entity within each document.
        This is done by using the summary_id column in entity_occurrences to group by document_id and summary_id.
        Summary_id refers to the normalized entity in entity_occurrences_summary.
        Results are stored in entity_occurrences with the column 'intra_doc_fq'.
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

            # Sample some high-frequency entities for inspection
            self.cursor.execute(
                """
                SELECT 
                    e.document_id,
                    e.entity_text,
                    n.named_entity as entity_type,
                    s.normalized_entity_text,
                    e.intra_doc_fq
                FROM entity_occurrences e
                JOIN entity_occurrences_summary s ON s.id = e.summary_id
                JOIN named_entities n ON n.id = s.entity_id
                WHERE e.intra_doc_fq > ?
                GROUP BY e.document_id, e.summary_id
                ORDER BY e.intra_doc_fq DESC
                LIMIT 5
            """,
                (stats[3] * 2,),
            )  # Show entities with frequency > 2x average

            high_freq = self.cursor.fetchall()
            if high_freq:
                self.logger.info("\nSample high-frequency entities:")
                for doc_id, text, type_, norm_text, freq in high_freq:
                    self.logger.info(
                        f"Document {doc_id}: '{text}' ({type_})\n"
                        f"  Normalized: '{norm_text}'\n"
                        f"  Frequency: {freq}"
                    )

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error calculating intra-document frequencies: {e}")
            raise

    def calculate_pmi(self, batch_size=200000) -> None:
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

            # Fetch co-occurrence pairs with document frequencies
            self.cursor.execute(
                """
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
            )
            cooccurrences = self.cursor.fetchall()

            # Calculate PMI and update in batches
            updates = []
            for e1_id, e2_id, fq, e1_docs, e2_docs in cooccurrences:
                if e1_docs == 0 or e2_docs == 0:
                    pmi = None  # Handle division by zero or log(0)
                else:
                    pmi = math.log((fq * total_docs) / (e1_docs * e2_docs))
                updates.append((pmi, e1_id, e2_id))

            # Batch update
            self.cursor.executemany(
                "UPDATE entity_cooccurrences_summary SET pmi = ? WHERE e1_id_normalized = ? AND e2_id_normalized = ?",
                updates,
            )
            self.conn.commit()

            self.logger.info(f"PMI calculation complete")

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Sqlite error while calculating PMI {e}")
            raise
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Unexpected error calculating PMI: {e}")
            raise

    def summarize_entity_cooccurrences(self, batch_size=50000) -> None:
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

    def aggregate_cooccurrences(
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

    def suite_analysis(self) -> None:
        """
        Run a suite of analysis steps in sequence:
        - Entity co-occurrence summarization
        - Entity co-occurrence aggregation
        - PMI calculation
        """

        # Baseline analysis
        self.count_named_entity_fq()

        # Entity occurrence analysis 
        self.find_overlapping_entities()
        self.aggregate_entity_occurrences()
        self.count_entity_intra_doc_fq()

        # Entity co-occurrence analysis
        self.count_entity_cooccurrences(level="document")
        self.aggregate_cooccurrences()
        self.calculate_pmi()