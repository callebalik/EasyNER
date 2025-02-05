import os
import sys
import sqlite3
import unittest
import csv

# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.db_easyner import EasyNerDB
from test_db_convert_json_to_sqlite import TestConvertJsonToSqlite
from test_db_easyner import suite as easyner_suite

class TestDatabaseExport(unittest.TestCase):
    successful_tests = []

    def setUp(self):
        self.test_db_path = "test_database.db"
        print(f"Checking for {self.test_db_path}")
        if not os.path.exists(self.test_db_path):
            print(f"{self.test_db_path} not found. Running TestConvertJsonToSqlite to create it.")
            test_convert = TestConvertJsonToSqlite()
            test_convert.setUp()
            test_convert.test_load_json_to_db()

        self.db = EasyNerDB(self.test_db_path)
        self.cursor = self.db.cursor

    def test_export_entity_fq(self):
        output_file = "test_entity_fq.csv"
        self.db.export_entity_fq(output_file)

        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            self.assertEqual(header, ["entity_text", "total_count", "entity"])
            rows = list(reader)
            self.assertGreater(len(rows), 0)

        TestDatabaseExport.successful_tests.append("test_export_entity_fq")

    def test_export_entity_fq_with_filter(self):
        output_file = "test_entity_fq_filtered.csv"
        self.db.export_entity_fq(output_file, entity_filter="PNM")

        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            self.assertEqual(header, ["entity_text", "total_count", "entity"])
            rows = list(reader)
            self.assertGreater(len(rows), 0)
            for row in rows:
                self.assertEqual(row[0], "PNM")

        TestDatabaseExport.successful_tests.append("test_export_entity_fq_with_filter")

    def test_export_cooccurrences(self):
        output_file = "test_export_cooccurrences.csv"
        self.db.export_cooccurrences(output_file)

        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            self.assertEqual(header, ["entity_1", "entity_2", "fq"])
            rows = list(reader)
            self.assertGreater(len(rows), 0)

        TestDatabaseExport.successful_tests.append("test_export_cooccurrences")

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDatabaseExport('test_export_entity_fq'))
    suite.addTest(TestDatabaseExport('test_export_entity_fq_with_filter'))
    suite.addTest(TestDatabaseExport('test_export_cooccurrences'))
    return suite

if __name__ == "__main__":
    # Run the test suite from test_db_easyner.py first
    runner = unittest.TextTestRunner()
    result = runner.run(easyner_suite())
    if not result.wasSuccessful():
        print("\nFailed Tests in test_db_easyner:")
        for failed_test, traceback in result.failures:
            print(f" - {failed_test.id()}")
        print("\nErrored Tests in test_db_easyner:")
        for errored_test, traceback in result.errors:
            print(f" - {errored_test.id()}")

    # Run the test suite for export functions
    result = runner.run(suite())
    if not result.wasSuccessful():
        print("\nFailed Tests in test_database_export:")
        for failed_test, traceback in result.failures:
            print(f" - {failed_test.id()}")
        print("\nErrored Tests in test_database_export:")
        for errored_test, traceback in result.errors:
            print(f" - {errored_test.id()}")

    print("\nSuccessful Tests in test_database_export:")
    for test_name in TestDatabaseExport.successful_tests:
        print(f" - {test_name}")