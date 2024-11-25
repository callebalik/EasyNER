import os
import sys
import sqlite3
import json
import unittest
from glob import glob
from tqdm import tqdm

# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.db_convert_json_to_sqlite import create_database, insert_data, load_json_to_db, compare_sizes

class TestConvertJsonToSqlite(unittest.TestCase):
    def setUp(self):
        self.test_db_path = 'test_database.db'

        if os.path.exists(self.test_db_path):
            print(f"Removing existing database file: {self.test_db_path}")
            os.remove(self.test_db_path)

        self.mock_json_file = 'mockup.json'
        with open(self.mock_json_file, 'r', encoding='utf-8') as file:
            self.mock_json_data = json.load(file)

    def test_load_json_to_db(self):
        load_json_to_db([self.mock_json_file], self.test_db_path)
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM articles')
        num_articles = cursor.fetchone()[0]
        self.assertEqual(num_articles, 4)

        cursor.execute('SELECT COUNT(*) FROM sentences')
        num_sentences = cursor.fetchone()[0]
        self.assertEqual(num_sentences, 18)

        cursor.execute('SELECT COUNT(*) FROM entity_occurrences')
        num_entities = cursor.fetchone()[0]
        self.assertEqual(num_entities, 29)

        cursor.execute('SELECT COUNT(*) FROM processed_files WHERE filename = ?', (self.mock_json_file,))
        num_processed_files = cursor.fetchone()[0]
        self.assertEqual(num_processed_files, 1)

        cursor.execute('SELECT sentence_id FROM entity_occurrences LIMIT 1')
        sentence_id = cursor.fetchone()[0]
        self.assertIsNotNone(sentence_id)

        cursor.execute('SELECT COUNT(*) FROM entities')
        num_unique_entities = cursor.fetchone()[0]
        self.assertGreater(num_unique_entities, 0)

        conn.close()

    def test_insert_data(self):
        conn = create_database(self.test_db_path)
        insert_data(conn, self.mock_json_data, self.mock_json_file)
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM articles')
        num_articles = cursor.fetchone()[0]
        self.assertEqual(num_articles, 4)

        cursor.execute('SELECT COUNT(*) FROM sentences')
        num_sentences = cursor.fetchone()[0]
        self.assertEqual(num_sentences, 18)

        cursor.execute('SELECT COUNT(*) FROM entity_occurrences')
        num_entities = cursor.fetchone()[0]
        self.assertEqual(num_entities, 29)

        cursor.execute('SELECT COUNT(*) FROM processed_files WHERE filename = ?', (self.mock_json_file,))
        num_processed_files = cursor.fetchone()[0]
        self.assertEqual(num_processed_files, 1)

        cursor.execute('SELECT sentence_id FROM entity_occurrences LIMIT 1')
        sentence_id = cursor.fetchone()[0]
        self.assertIsNotNone(sentence_id)

        cursor.execute('SELECT COUNT(*) FROM entities')
        num_unique_entities = cursor.fetchone()[0]
        self.assertGreater(num_unique_entities, 0)

        conn.close()

if __name__ == "__main__":
    unittest.main()