import logging
import os
from datetime import datetime

class DBAnalysis:
    def __init__(self, logger):
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
            SELECT 
                COUNT(*) as needs_update,
                (SELECT COUNT(*) FROM documents) as total_count,
                SUM(CASE WHEN word_count IS NULL OR token_count IS NULL THEN 1 ELSE 0 END) as null_count,
                SUM(CASE WHEN word_count = 0 OR token_count = 0 THEN 1 ELSE 0 END) as zero_count
            FROM documents
            WHERE word_count IS NULL OR token_count IS NULL
                OR word_count = 0 OR token_count = 0
            """
        )
        result = self.cursor.fetchone()
        documents_to_update, total_documents, null_count, zero_count = result

        self.logger.info(f"Total documents in database: {total_documents}")
        self.logger.info(f"Documents with null counts: {null_count}")
        self.logger.info(f"Documents with zero word/token counts: {zero_count}")
        self.logger.info(f"Total documents needing update: {documents_to_update}")

        if documents_to_update == 0:
            self.logger.info("All documents have valid word and token counts. No action needed.")
            return

        # Create temporary indices for better performance
        self.cursor.execute("CREATE INDEX IF NOT EXISTS temp_sentences_doc_id ON sentences(document_id)")
        
        # Use a more efficient single query to calculate all counts at once
        update_query = """
        WITH document_stats AS (
            SELECT 
                document_id,
                SUM(COALESCE(word_count, 0)) as total_words,
                SUM(COALESCE(token_count, 0)) as total_tokens,
                SUM(COALESCE(alpha_count, 0)) as total_alpha
            FROM sentences
            GROUP BY document_id
            HAVING total_words > 0 AND total_tokens > 0  -- Remove alpha count requirement
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
        WHERE id IN (
            SELECT id
            FROM documents
            WHERE word_count IS NULL OR token_count IS NULL
               OR word_count = 0 OR token_count = 0
            LIMIT ? OFFSET ?
        )
        """

        processed = 0
        with tqdm(total=documents_to_update, desc="Calculating document counts") as pbar:
            try:
                for offset in range(0, total_documents, batch_size):
                    batch_start = time.time()
                    
                    self.cursor.execute(update_query, (batch_size, offset))
                    rows_affected = self.cursor.rowcount
                    
                    if rows_affected == 0:
                        self.logger.debug(f"No documents updated in batch at offset {offset}")
                        continue
                        
                    self.conn.commit()
                    processed += rows_affected
                    
                    batch_time = time.time() - batch_start
                    self.logger.debug(f"Batch processed: {rows_affected} documents in {batch_time:.2f}s")
                    
                    pbar.update(rows_affected)
                    
                    # Early exit if we've processed all documents needing updates
                    if processed >= documents_to_update:
                        break
                        
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
