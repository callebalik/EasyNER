import sqlite3
import unittest
import os
import sys


# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.db_statistics import get_entity_occurrences_with_article_id, calc_article_counts, calc_all_article_lengths, verify_sentences_table, calc_unique_doc_fq


from scripts.db_easyner import EasyNerDB


class TestDBStatistics(unittest.TestCase):
    def setUp(self):
        self.test_db_path = 'test_database.db'
        self.conn = sqlite3.connect(self.test_db_path)
        self.cursor = self.conn.cursor()
        # verify database connection
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        print(tables)

        verify_sentences_table(self.conn)

    def test_count_articles_counts(self):
        calc_article_counts(self.conn)

    def test_calc_all_article_lengths(self):
        calc_all_article_lengths(self.conn)

    def test_get_entity_occurrences_with_article_id(self):
        df = get_entity_occurrences_with_article_id(self.conn)
        # Save as CSV for manual cinspection
        df.to_csv('entity_occurrences_with_article_id.csv', index=False)

    def test_calc_unique_doc_fq(self):
        df = get_entity_occurrences_with_article_id(self.conn)
        df = calc_unique_doc_fq(df)
        df.to_csv('entity_occurrences_with_article_id.csv', index=False)

def suite():
    suite = unittest.TestSuite()
    # suite.addTest(TestDBStatistics('test_count_articles_counts'))
    # suite.addTest(TestDBStatistics('test_calc_all_article_lengths'))
    suite.addTest(TestDBStatistics('test_get_entity_occurrences_with_article_id'))
    suite.addTest(TestDBStatistics('test_calc_unique_doc_fq'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
