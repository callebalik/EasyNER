import logging
import os
from datetime import datetime
import sqlite3

class DBAnalysis:

    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor, logger: logging.Logger):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger
        
    def calc_document_counts(self, batch_size=100000):
        """
        Calculate word count, token count, and alphabetic character count for each document in batches.
        Updates documents where word_count or token_count are null or zero.
        Alpha count is allowed to be zero.

        Args:
            batch_size (int): The number of documents to process in each batch.
        """
        from tqdm import tqdm
        import time

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
            self.logger.info("All documents have valid word and token counts. No action needed.")
            return

        # Create temporary indices for better performance
        self.cursor.execute("CREATE INDEX IF NOT EXISTS temp_sentences_doc_id ON sentences(document_id)")
        
        # Process documents in batches
        processed = 0
        with tqdm(total=total_to_update, desc="Calculating document counts") as pbar:
            try:
                for i in range(0, total_to_update, batch_size):
                    batch_docs = documents_to_update[i:i + batch_size]
                    batch_ids = [doc[0] for doc in batch_docs]
                    batch_start = time.time()
                    
                    self.logger.debug(f"Processing batch {i//batch_size + 1}/{(total_to_update + batch_size - 1)//batch_size}")
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
                        """ % (','.join('?' * len(batch_ids)), ','.join('?' * len(batch_ids))),
                        batch_ids + batch_ids
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
                    
                    pbar.set_postfix({
                        'docs/s': f"{batch_processed/batch_time:.1f}",
                        'processed': processed,
                        'remaining': total_to_update - processed
                    })
                    pbar.update(batch_processed)
                        
            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Error during document count calculation: {str(e)}")
                raise
            finally:
                # Clean up temporary index
                self.cursor.execute("DROP INDEX IF EXISTS temp_sentences_doc_id")

        total_time = time.time() - start_time
        self.logger.info(f"Document count calculation completed. Processed {processed} documents in {total_time:.2f}s")
        
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
            self.logger.warning(f"There are still {stats[1]} documents with null counts and {stats[2]} with zero word/token counts")
        else:
            self.logger.info("All documents have been successfully processed with valid word and token counts")

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
        if user_response.lower() != 'y':
            self.logger.info("Export cancelled by user")
            return

        if output_path is None:
            default_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'results',
                f'problematic_documents_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
            output_path = input(f"Enter output path (default: {default_path}): ").strip() or default_path

        metadata = {
            'statistics': {
                'null_word_count': stats[1],
                'null_token_count': stats[2],
                'zero_word_count': stats[3],
                'zero_token_count': stats[4]
            }
        }

        output_path = self.export_documents_to_json(doc_ids, output_path, metadata)
        self.logger.info(f"Exported {len(doc_ids)} problematic documents to {output_path}")
        return output_path

    def find_overlapping_entities(self) -> None:
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
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM pragma_table_info('entity_occurrences') 
                WHERE name='overlap'
            """)
            if self.cursor.fetchone()[0] == 0:
                self.logger.info("Adding 'overlap' column to entity_occurrences table...")
                self.cursor.execute("""
                    ALTER TABLE entity_occurrences 
                    ADD COLUMN overlap BOOLEAN DEFAULT FALSE
                """)
            else:
                # Reset all overlap flags to FALSE
                self.cursor.execute("""
                    UPDATE entity_occurrences 
                    SET overlap = FALSE
                """)

            self.logger.info("Finding overlapping entities...")
            
            # Find overlapping entities within the same sentence
            # Two entities overlap if:
            # - They are in the same document and sentence
            # - One entity's span intersects with another's span
            # - They are different entities (different IDs)
            self.cursor.execute("""
                WITH overlapping_pairs AS (
                    SELECT DISTINCT
                        e1.id as id1,
                        e2.id as id2
                    FROM entity_occurrences e1
                    JOIN entity_occurrences e2 ON 
                        e1.document_id = e2.document_id AND
                        e1.sentence_index = e2.sentence_index AND
                        e1.id < e2.id AND
                        NOT (
                            e1.span_end <= e2.span_start OR
                            e2.span_end <= e1.span_start
                        )
                )
                UPDATE entity_occurrences
                SET overlap = TRUE
                WHERE id IN (
                    SELECT id1 FROM overlapping_pairs
                    UNION
                    SELECT id2 FROM overlapping_pairs
                )
            """)

            # Get statistics about overlapping entities
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_entities,
                    SUM(CASE WHEN overlap THEN 1 ELSE 0 END) as overlapping_entities,
                    COUNT(DISTINCT document_id) as affected_documents,
                    COUNT(DISTINCT sentence_index) as affected_sentences
                FROM entity_occurrences
                WHERE overlap = TRUE
            """)
            stats = self.cursor.fetchone()
            
            self.conn.commit()
            self.logger.info(f"Found {stats[1]} overlapping entities across {stats[2]} documents and {stats[3]} sentences")
            
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error while finding overlapping entities: {e}")
            raise

    def count_entity_cooccurrences(self, level: str = "document"):
        pass

    def count_named_entity_fq(self):
        """
        Counts the frequency of each named entity in the entity_occurrences table
        and updates the 'fq' column in the named_entities table.
        """
        try:
            self.logger.info("Counting named entity frequencies...")
            self.cursor.execute(
                """
                UPDATE named_entities
                SET fq = (
                    SELECT COUNT(*)
                    FROM entity_occurrences
                    WHERE entity_occurrences.entity_id = named_entities.id
                )
                """
            )
            self.conn.commit()
            self.logger.info("Named entity frequencies updated in named_entities table.")
        except sqlite3.Error as e:
            self.logger.error(f"Error counting named entity frequencies: {e}")

    def count_entity_occurrences(self, batch_size=10000) -> None:
        """
        Summaries the fq of unique TABLE entity_occurrences and records in entity_occurrences_summary, adds reference to summary table in entity_occurrences['summary_id']
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
            
            # Recreate entity_occurrences_summary table to ensure proper constraints
            self.cursor.execute("DROP TABLE IF EXISTS entity_occurrences_summary")
            self.cursor.execute("""
                CREATE TABLE entity_occurrences_summary (
                    id INTEGER PRIMARY KEY NOT NULL,
                    normalized_entity_text TEXT UNIQUE NOT NULL,
                    uniq_documents INTEGER,
                    fq INTEGER
                )
            """)
            
            # Drop temporary table if it exists
            self.cursor.execute("DROP TABLE IF EXISTS temp_normalized_entities")
            
            # Create a temporary table for normalized texts using SQLite string functions
            self.cursor.execute("""
                CREATE TEMPORARY TABLE temp_normalized_entities AS
                SELECT 
                    id,
                    TRIM(
                        LOWER(
                            CASE 
                                -- Remove trailing 's or 's
                                WHEN entity_text LIKE '%''s' THEN SUBSTR(entity_text, 1, LENGTH(entity_text) - 2)
                                WHEN entity_text LIKE '%''s' THEN SUBSTR(entity_text, 1, LENGTH(entity_text) - 2)
                                ELSE entity_text 
                            END
                        )
                    ) as normalized_entity_text
                FROM entity_occurrences
                WHERE entity_text IS NOT NULL
            """)

            # Remove leading non-alphanumeric characters using a separate update
            self.cursor.execute("""
                WITH RECURSIVE
                strip_leading(id, txt, n) AS (
                    SELECT id, normalized_entity_text, 1
                    FROM temp_normalized_entities
                    UNION ALL
                    SELECT id, SUBSTR(txt, 2), n + 1
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
            """)

            # Insert summaries into entity_occurrences_summary
            self.cursor.execute("""
                INSERT INTO entity_occurrences_summary (normalized_entity_text, uniq_documents, fq)
                SELECT 
                    ne.normalized_entity_text,
                    COUNT(DISTINCT eo.document_id) as uniq_documents,
                    COUNT(*) as fq
                FROM temp_normalized_entities ne
                JOIN entity_occurrences eo ON eo.id = ne.id
                GROUP BY ne.normalized_entity_text
                ON CONFLICT(normalized_entity_text) DO UPDATE SET
                    uniq_documents = excluded.uniq_documents,
                    fq = excluded.fq
            """)

            # Update summary_id references in batches using executemany
            # First, get all entity mappings
            self.cursor.execute("""
                SELECT eo.id, s.id as summary_id
                FROM entity_occurrences eo
                JOIN temp_normalized_entities ne ON ne.id = eo.id
                JOIN entity_occurrences_summary s ON s.normalized_entity_text = ne.normalized_entity_text
            """)
            
            mappings = self.cursor.fetchall()
            total_records = len(mappings)
            self.logger.info(f"Updating summary_id references for {total_records} records in batches of {batch_size}")

            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = mappings[i:i + batch_size]
                self.cursor.executemany(
                    "UPDATE entity_occurrences SET summary_id = ? WHERE id = ?",
                    [(summary_id, eo_id) for eo_id, summary_id in batch]
                )
                self.conn.commit()
                self.logger.debug(f"Processed {min(i + batch_size, total_records)}/{total_records} records")

            # Drop temporary table
            self.cursor.execute("DROP TABLE temp_normalized_entities")

            # Get statistics
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_summaries,
                    AVG(fq) as avg_frequency,
                    SUM(fq) as total_occurrences,
                    AVG(uniq_documents) as avg_documents
                FROM entity_occurrences_summary
            """)
            stats = self.cursor.fetchone()

            self.conn.commit()
            self.logger.info(
                f"Entity occurrences summarization complete:\n"
                f"- Total unique normalized entities: {stats[0]}\n"
                f"- Average frequency per entity: {stats[1]:.2f}\n"
                f"- Total occurrences: {stats[2]}\n"
                f"- Average documents per entity: {stats[3]:.2f}"
            )

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error summarizing entity occurrences: {e}")
            raise

