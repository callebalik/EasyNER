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
        self.mock_json_file = 'mockup.json'
        with open(self.mock_json_file, 'r', encoding='utf-8') as file:
            self.mock_json_data = json.load(file)

    def test_load_json_to_db(self):
        load_json_to_db([self.mock_json_file], self.test_db_path)
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM articles')
        num_articles = cursor.fetchone()[0]
        self.assertEqual(num_articles, 2)

        cursor.execute('SELECT COUNT(*) FROM sentences')
        num_sentences = cursor.fetchone()[0]
        self.assertEqual(num_sentences, 15)

        cursor.execute('SELECT COUNT(*) FROM entities')
        num_entities = cursor.fetchone()[0]
        self.assertEqual(num_entities, 27)

        cursor.execute('SELECT COUNT(*) FROM processed_files WHERE filename = ?', (self.mock_json_file,))
        num_processed_files = cursor.fetchone()[0]
        self.assertEqual(num_processed_files, 1)

        conn.close()

if __name__ == "__main__":
    unittest.main()