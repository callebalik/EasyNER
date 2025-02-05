import sqlite3
import unittest
import os
import sys


# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.db_statistics import (
    get_entity_occurrences_with_article_id,
    calc_article_counts,
    verify_sentences_table,
    calc_entity_term_fq,
    get_number_of_articles,
    tf_idf,
    count_cooucerence_fq,
    record_sentence_cooccurences,
    record_document_cooccurences,
    count_document_cooccurence_fq,
    add_pmid_to_entity_occurrences,
    update_db_with_entity_occurrence_term_fq,
    update_tf_idf,
    calc_weighted_fqs,
    get_all_entity_occurrences
)


from scripts.db_easyner import EasyNerDB


class TestDBStatistics(unittest.TestCase):
    def setUp(self):
        self.test_db_path = "test_database.db"
        self.conn = sqlite3.connect(self.test_db_path)
        self.cursor = self.conn.cursor()
        # verify database connection
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        self.existing_tables = [table[0] for table in tables]
        # print(tables)

        # verify_sentences_table(self.conn)

    def test_count_articles_counts(self):
        calc_article_counts(self.conn)
        self.cursor.execute("SELECT * FROM articles")
        rows = self.cursor.fetchall()

    def test_get_entity_occurrences_with_article_id(self):
        df = get_entity_occurrences_with_article_id(self.conn)
        # Save as CSV for manual cinspection
        df.to_csv("entity_occurrences_with_article_id.csv", index=False)

    def test_calc_unique_doc_fq(self):
        df = get_entity_occurrences_with_article_id(self.conn)
        df = calc_entity_term_fq(df)
        df.to_csv("entity_occurrences_with_article_id.csv", index=False)

    def test_get_number_of_articles(self):
        num_articles = get_number_of_articles(self.conn)
        assert num_articles == 4

    def test_tf_idf(self):
        df = get_entity_occurrences_with_article_id(self.conn)
        df = calc_entity_term_fq(df)
        num_articles = get_number_of_articles(self.conn)
        df = tf_idf(df, num_articles)
        df.to_csv("entity_occurrences_with_article_id.csv", index=False)

    def test_count_cooucerence_fq(self):
        df = get_entity_occurrences_with_article_id(self.conn)
        df = calc_entity_term_fq(df)
        num_articles = get_number_of_articles(self.conn)
        df = tf_idf(df, num_articles)
        df = count_cooucerence_fq(df)
        df.to_csv("entity_coccurrences.csv", index=False)

    def test_count_sentence_cooccurence(self):
        df = get_entity_occurrences_with_article_id(self.conn)
        df = calc_entity_term_fq(df)
        num_articles = get_number_of_articles(self.conn)
        df = tf_idf(df, num_articles)
        # df = count_cooucerence_fq(df)
        # print(df)
        df = record_sentence_cooccurences(df)
        df.to_csv("coocurences.csv", index=False)

    def test_record_document_cooccurences(self):
        df = get_entity_occurrences_with_article_id(self.conn)
        df = calc_entity_term_fq(df)
        num_articles = get_number_of_articles(self.conn)
        df = tf_idf(df, num_articles)
        df = record_sentence_cooccurences(df)
        df = count_document_cooccurence_fq(df)
        df.to_csv("cooccurences_summary.csv", index=False)

    def test_add_pmid_to_entity_occurrences(self):
        add_pmid_to_entity_occurrences(self.conn)

    def test_update_db_with_entity_occurrence_term_fq(self):
        update_db_with_entity_occurrence_term_fq(self.conn)

    def test_update_tf_idf(self):
        update_tf_idf(self.conn)

    def test_calc_weighted_fqs(self):
        calc_weighted_fqs(self.conn)
    def test_get_all_entity_occurrences(self):
        df = get_all_entity_occurrences(self.conn)
        df.to_csv("all_entity_occurrences.csv", index=False)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDBStatistics('test_count_articles_counts'))
    # suite.addTest(TestDBStatistics('test_calc_all_article_lengths'))
    suite.addTest(TestDBStatistics("test_get_entity_occurrences_with_article_id"))
    suite.addTest(TestDBStatistics("test_calc_unique_doc_fq"))
    suite.addTest(TestDBStatistics("test_get_number_of_articles"))
    suite.addTest(TestDBStatistics("test_tf_idf"))
    suite.addTest(TestDBStatistics("test_count_cooucerence_fq"))
    suite.addTest(TestDBStatistics("test_count_sentence_cooccurence"))
    suite.addTest(TestDBStatistics("test_record_document_cooccurences"))
    suite.addTest(TestDBStatistics("test_add_pmid_to_entity_occurrences"))
    suite.addTest(TestDBStatistics("test_update_db_with_entity_occurrence_term_fq"))
    suite.addTest(TestDBStatistics("test_update_tf_idf"))
    suite.addTest(TestDBStatistics("test_calc_weighted_fqs"))
    suite.addTest(TestDBStatistics("test_get_all_entity_occurrences"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
