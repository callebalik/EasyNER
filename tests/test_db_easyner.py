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

    def test_get_named_entity_fqs(self):
        self.db.get_named_entity_fqs()
        self.cursor.execute("SELECT entity, fq FROM entities")
        entities = self.cursor.fetchall()
        expected_entities = [("disease", 16), ("phenomenon", 13)]
        self.assertEqual(
            entities,
            expected_entities,
            "Named entity frequencies do not match expected values.",
        )
        TestDBAnalysis.successful_tests.append("test_get_named_entity_fqs")

    def test_get_title(self):
        pmid = 1
        expected_title = "Title 1"
        title = self.db.get_title(pmid)
        self.assertEqual(
            title, expected_title, f"Title for PMID {pmid} does not match."
        )
        TestDBAnalysis.successful_tests.append("test_get_title")

    def test_get_title_not_found(self):
        pmid = 999999
        title = self.db.get_title(pmid)
        self.assertIsNone(title, f"Title for PMID {pmid} should be None.")
        TestDBAnalysis.successful_tests.append("test_get_title_not_found")

    def test_get_sentences(self):
        pmid = 3
        sentences = self.db.get_sentences(pmid)
        expected_sentences = [(0, "This only has 1 sentence without any entities.")]
        self.assertEqual(sentences, expected_sentences, "Sentences do not match.")
        TestDBAnalysis.successful_tests.append("test_get_sentences")

    def test_find_entity_cooccurrences(self, entity1="disease", entity2="phenomenon"):
        # Destroy the entity_cooccurrences table to ensure it is created from scratch
        self.cursor.execute("DROP TABLE IF EXISTS entity_cooccurrences")
        expected_result = {
            "('disease1', 'phenomenon2')": {
                "freq": 4,
                "pmid": ["9", "10", "15"],
                "sentence_ids": [9, 10, 15],
            }
        }
        # Find and record cooccurrences
        self.db.record_entity_cooccurrences(self.entity1, self.entity2)
        # Fetch cooccurrences
        cooc = self.db.get_specified_entity_cooccurrences("disease1", "phenomenon2")

        # Convert tuple keys to strings for comparison and JSON export
        cooc_str_keys = {str(k): v for k, v in cooc.items()}
        output_file = "test_find_entity_cooccurrences.json"

        # Export to JSON
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(cooc_str_keys, f, ensure_ascii=False, indent=4)

        if cooc:
            self.assertEqual(cooc_str_keys, expected_result)
            TestDBAnalysis.successful_tests.append("test_get_sentences_with_entities")
        else:
            self.fail("No cooccurrences found.")

    def test_count_cooccurence(self):
        # Assuming the test database already has the necessary data
        count = self.db.count_cooccurence("disease1", "phenomenon1")
        expected_count = 7
        self.assertEqual(
            count,
            expected_count,
            "Count of cooccurrences does not match expected value.",
        )
        TestDBAnalysis.successful_tests.append("test_count_cooccurence")

    def test_sum_cooccurences(self):
        # Destroy the table to ensure it is created from scratch
        self.cursor.execute("DROP TABLE IF EXISTS coentity_summary")

        # Assuming the test database already has the necessary data
        self.db.sum_cooccurences()
        self.cursor.execute("SELECT e1_text, e2_text, fq FROM coentity_summary")
        coentity_summary = self.cursor.fetchall()
        # print(coentity_summary)
        expected_summary = [
            # Add expected summary data here
            ("disease1", "phenomenon1", 7),
            ("disease1", "phenomenon2", 4),
            ("disease2", "phenomenon1", 1),
            ("disease2", "phenomenon2", 1),
            ("disease3", "phenomenon1", 1),
            ("disease3", "phenomenon2", 1),
        ]
        self.assertEqual(
            coentity_summary,
            expected_summary,
            "Coentity summary does not match expected values.",
        )
        TestDBAnalysis.successful_tests.append("test_sum_cooccurences")


    def test_export_cooccurrences(self):
        # Assuming the test database already has the necessary data
        output_file = "test_export_cooccurrences.csv"
        self.db.export_cooccurrences(output_file)

        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            self.assertEqual(header, ["entity_1", "entity_2", "fq"])
            rows = list(reader)
            self.assertGreater(len(rows), 0)

        TestDBAnalysis.successful_tests.append("test_export_cooccurrences")

    def test_update_entity_name(self):
        old_name = "phenomenon"
        new_name = "PNM"
        self.db.update_entity_name(old_name, new_name)
        self.cursor.execute("SELECT entity FROM entities WHERE entity = ?", (new_name,))
        updated_entity = self.cursor.fetchone()
        self.assertIsNotNone(updated_entity, f"Entity {new_name} not found.")
        self.assertEqual(updated_entity[0], new_name, f"Entity name not updated correctly.")
        TestDBAnalysis.successful_tests.append("test_update_entity_name")

        # Revert the changes
        self.db.update_entity_name(new_name, old_name)

        # Check if the entity name is reverted
        self.cursor.execute("SELECT entity FROM entities WHERE entity = ?", (old_name,))
        reverted_entity = self.cursor.fetchone()
        self.assertIsNotNone(reverted_entity, f"Entity {old_name} not found.")
        self.assertEqual(reverted_entity[0], old_name, f"Entity name not reverted correctly.")


    def test_count_entity_fq(self):
        # Destroy the entity_fq table to ensure it is created from scratch
        self.cursor.execute("DROP TABLE IF EXISTS entity_fq")

        # Assuming the test database already has the necessary data
        self.db.count_entity_fq()
        self.cursor.execute("SELECT entity_text, entity_id, fq FROM entity_fq")
        entity_frequencies = self.cursor.fetchall()
        # print(entity_frequencies)
        #  The middle values are the entity IDs, 1 for disease and 2 for phenomenon
        expected_frequencies = [
            ("disease1", 1, 9),
            ("phenomenon1", 2, 6),
            ("phenomenon2", 2, 4),
            ("disease2", 1, 3),
            ("disease3", 1, 2),
            ("disease4", 1, 1),
            ("phenomenon3", 2, 1),
            ("phenomenon4", 2, 1),
            ('typical_disease_text', 1, 1),
            ('typical_disease_text', 2, 1)
        ]

        self.assertEqual(
            entity_frequencies,
            expected_frequencies,
            "Entity frequencies do not match expected values.",
        )
        TestDBAnalysis.successful_tests.append("test_count_entity_fq")
        self.db.export_entity_fq("test_entity_fq.csv", entity_filter="PNM")

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDBAnalysis('test_get_named_entity_fqs'))
    suite.addTest(TestDBAnalysis('test_get_title'))
    suite.addTest(TestDBAnalysis('test_get_title_not_found'))
    suite.addTest(TestDBAnalysis('test_get_sentences'))
    suite.addTest(TestDBAnalysis('test_find_entity_cooccurrences'))
    suite.addTest(TestDBAnalysis('test_count_cooccurence'))
    suite.addTest(TestDBAnalysis('test_sum_cooccurences'))
    suite.addTest(TestDBAnalysis('test_export_cooccurrences'))
    suite.addTest(TestDBAnalysis('test_update_entity_name'))
    suite.addTest(TestDBAnalysis('test_count_entity_fq'))
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


