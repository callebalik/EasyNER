import sqlite3
import unittest
import os
import sys
import pprint

# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.db_easyner import EasyNerDB

class TestDBAnalysis(unittest.TestCase):
    def setUp(self):
        self.test_db_path = 'test_database.db'
        self.db = EasyNerDB(self.test_db_path)
        self.entity1 = 'disease'
        self.entity2 = 'phenomenon'
        self.maxDiff = None

    def test_list_fq_of_entities(self):
        entities = self.db.list_fq_of_entities()
        # print(f"Frequencies of entities: {entities}")
        self.assertEqual(entities, {'disease': 15, 'phenomenon': 12})

    def test_get_sentences_with_entities(self):
        expected_count = 2
        expected_pmids = [16798089]
        result = self.db.get_sentence_cooccurence(self.entity1, self.entity2, "disease1", "phenomenon2")
        if result:
            actual_pmids = result
            print("Actual pmids:")
            pprint.pprint(actual_pmids)
            # self.assertEqual(actual_count, expected_count, "Number of sentences with entities does not match.")
            # self.assertEqual(actual_pmids, expected_pmids)
        else:
            self.fail("No cooccurrences found.")

    def test_get_title(self):
        pmid = 1
        expected_title = "Title 1"
        title = self.db.get_title(pmid)
        self.assertEqual(title, expected_title, f"Title for PMID {pmid} does not match.")

    def test_get_title_not_found(self):
        pmid = 999999
        title = self.db.get_title(pmid)
        self.assertIsNone(title, f"Title for PMID {pmid} should be None.")

    def test_get_sentences(self):
        pmid = 3
        sentences = self.db.get_sentences(pmid)
        expected_sentences = [
            (0, "This only has 1 sentence without any entities.")
        ]
        self.assertEqual(sentences, expected_sentences, "Sentences do not match.")


if __name__ == "__main__":
    unittest.main()