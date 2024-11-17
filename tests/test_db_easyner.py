import sqlite3
import unittest
import os
import sys
import pprint
import json

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

    def test_get_entity_fqs(self):
        self.db.get_entity_fqs()
        self.cursor.execute("SELECT entity, fq FROM entities")
        entities = self.cursor.fetchall()
        expected_entities = [("disease", 15), ("phenomenon", 12)]
        self.assertEqual(
            entities,
            expected_entities,
            "Entity frequencies do not match expected values.",
        )
        TestDBAnalysis.successful_tests.append("test_get_entity_fqs")

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
            ("disease1", "phenomenon2"): {
                "freq": 1,
                "pmid": {
                    1: {"sentence_index": [8, 8], "sentence.id": [8, 8]},
                    2: {"sentence_index": [0, 4], "sentence.id": [0, 4]},
                },
            }
        }
        result = self.db.find_sentence_cooccurence(self.entity1, self.entity2)
        output_file = "test_find_entity_cooccurrences.json"

        cooccurrences_str_keys = {str(k): v for k, v in result.items()}
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cooccurrences_str_keys, f, ensure_ascii=False, indent=4)

        if result:
            print("Actual result:")
            pprint.pprint(result)
            self.assertEqual(result, expected_result)
            TestDBAnalysis.successful_tests.append("test_get_sentences_with_entities")
        else:
            self.fail("No cooccurrences found.")

    def test_count_cooccurence(self):
        # Assuming the test database already has the necessary data
        count = self.db.count_cooccurence("disease1", "phenomenon2")
        expected_count = 7
        self.assertEqual(
            count,
            expected_count,
            "Count of cooccurrences does not match expected value.",
        )
        TestDBAnalysis.successful_tests.append("test_count_cooccurence")


if __name__ == "__main__":
    result = unittest.main(exit=False)
    print("\nSummary:")
    print(f"Ran {result.result.testsRun} tests.")
    print(f"Failures: {len(result.result.failures)}")
    print(f"Errors: {len(result.result.errors)}")
    if result.result.wasSuccessful():
        print("All tests passed successfully.")
    else:
        print("\nFailed Tests:")
        for failed_test, traceback in result.result.failures:
            print(f" - {failed_test.id()}")
        print("\nErrored Tests:")
        for errored_test, traceback in result.result.errors:
            print(f" - {errored_test.id()}")

    print("\nSuccessful Tests:")
    for test_name in TestDBAnalysis.successful_tests:
        print(f" - {test_name}")
