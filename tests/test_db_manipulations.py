import sqlite3
import unittest
import os
import sys
import pprint
import json
import csv

# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.db_easyner import EasyNerDB


class TestDBAnalysis(unittest.TestCase):
    successful_tests = []

    def setUp(self):
        self.test_db_path = "test_database.db"
        self.db = EasyNerDB(self.test_db_path)
        self.cursor = self.db.cursor
        self.entity1 = "disease"
        self.entity2 = "phenomenon"
        self.maxDiff = None
        # Ensure the entities_copy table is dropped before each test
        self.cursor.execute("DROP TABLE IF EXISTS entities_copy")
        self.cursor.execute("DROP TABLE IF EXISTS entity_occurrences_copy")
        self.db.conn.commit()

    def test_clone_table(self):
        self.db.clone_table("entities", "entities_copy")
        # Verify that the table was created
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='entities_copy'")
        table_exists = self.cursor.fetchone()
        self.assertIsNotNone(
            table_exists,
            "The cloned table was not created successfully.",
        )
        # Compare the number of rows in the original and cloned tables
        self.cursor.execute("SELECT COUNT(*) FROM entities")
        original_count = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT COUNT(*) FROM entities_copy")
        copy_count = self.cursor.fetchone()[0]
        self.assertEqual(
            original_count,
            copy_count,
            "Number of rows in the cloned table does not match the original table.",
        )

        # Compare the contents of the original and cloned tables
        self.cursor.execute("SELECT * FROM entities")
        original_entities = self.cursor.fetchall()
        self.cursor.execute("SELECT * FROM entities_copy")
        copied_entities = self.cursor.fetchall()
        self.assertEqual(
            original_entities,
            copied_entities,
            "Contents of the cloned table do not match the original table.",
        )

        TestDBAnalysis.successful_tests.append("test_clone_table")

    def test_drop_table(self):
        self.db.drop_table("entities_copy")
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='entities_copy'")
        table_exists = self.cursor.fetchone()
        self.assertIsNone(
            table_exists,
            "The cloned table was not successfully dropped.",
        )

        TestDBAnalysis.successful_tests.append("test_drop_table")

    def test_lowercase_column(self):
        # Clone the entity_occurrences table
        self.db.clone_table("entity_occurrences", "entity_occurrences_copy") # This overwrites the table if it already exists

        # Add entries with different cases
        self.cursor.execute("INSERT INTO entity_occurrences_copy (entity_text) VALUES ('Disease')")
        self.cursor.execute("INSERT INTO entity_occurrences_copy (entity_text) VALUES ('disease')")
        self.cursor.execute("INSERT INTO entity_occurrences_copy (entity_text) VALUES ('DISEASE')")
        self.cursor.execute("INSERT INTO entity_occurrences_copy (entity_text) VALUES ('lowercase')")
        self.cursor.execute("INSERT INTO entity_occurrences_copy (entity_text) VALUES ('CamelCase')")
        self.cursor.execute("INSERT INTO entity_occurrences_copy (entity_text) VALUES ('UPPERCASE')")
        self.db.conn.commit()

        # Print the state of the table before applying the lowercase_column function
        self.cursor.execute("SELECT entity_text FROM entity_occurrences_copy")
        # print("Before lowercase_column:", self.cursor.fetchall())

        # Apply the lowercase_column function
        self.db.lowercase_column("entity_occurrences_copy", "entity_text")

        # Print the state of the table after applying the lowercase_column function
        self.cursor.execute("SELECT entity_text FROM entity_occurrences_copy")
        # print("After lowercase_column:", self.cursor.fetchall())

        # Verify that all entries are now lowercase
        self.cursor.execute("SELECT entity_text FROM entity_occurrences_copy")
        entities = self.cursor.fetchall()
        for entity in entities:
            self.assertEqual(
                entity[0],
                entity[0].lower(),
                "Not all entries were converted to lowercase."
            )

        TestDBAnalysis.successful_tests.append("test_lowercase_column")

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDBAnalysis('test_clone_table'))
    suite.addTest(TestDBAnalysis('test_drop_table'))
    suite.addTest(TestDBAnalysis('test_lowercase_column'))
    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    result = runner.run(suite())
    if not result.wasSuccessful():
        print("\nFailed Tests:")
        for failed_test, traceback in result.failures:
            print(f" - {failed_test.id()}")
        print("\nErrored Tests:")
        for errored_test, traceback in result.errors:
            print(f" - {errored_test.id()}")

        print("\nSuccessful Tests:")
        for test_name in TestDBAnalysis.successful_tests:
            print(f" - {test_name}")


