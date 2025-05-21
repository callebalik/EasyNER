# New class to analyze sentence-level counts
class Sentence:
    def __init__(self, conn, cursor, logger):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger

    def check_word_counts(self):
        """Check and report statistics about sentence-level counts.
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
            """,
        )
        stats = self.cursor.fetchone()

        self.logger.info("Sentence-level statistics:")
        self.logger.info(f"Total sentences: {stats[0]}")
        self.logger.info("Sentences with null counts:")
        self.logger.info(f"- Word count: {stats[1]}")
        self.logger.info(f"- Token count: {stats[2]}")
        self.logger.info("Sentences with zero counts:")
        self.logger.info(f"- Word count: {stats[3]}")
        self.logger.info(f"- Token count: {stats[4]}")
        self.logger.info("Average counts:")
        self.logger.info(f"- Words per sentence: {stats[5]:.2f}")
        self.logger.info(f"- Tokens per sentence: {stats[6]:.2f}")
        self.logger.info(f"- Alpha chars per sentence: {stats[7]:.2f}")

        # Sample some problematic sentences for inspection
        self.cursor.execute(
            """
            SELECT document_id, sentence_index, text, word_count, token_count, alpha_count
            FROM sentences
            WHERE word_count IS NULL OR token_count IS NULL
                OR word_count = 0 OR token_count = 0
            LIMIT 5
            """,
        )
        problem_samples = self.cursor.fetchall()

        if problem_samples:
            self.logger.info("\nSample problematic sentences:")
            for sample in problem_samples:
                self.logger.info(
                    f"Document ID: {sample[0]}, Sentence Index: {sample[1]}, Text: {sample[2]}, Word Count: {sample[3]}, Token Count: {sample[4]}, Alpha Count: {sample[5]}",
                )

        return stats
