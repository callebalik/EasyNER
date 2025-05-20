import unittest
import json
import os
import sys

# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.co_occurence_db import get_inter_entity_co_occurrences
from scripts.co_occurence_db import store_co_occurrences_in_db, query_co_occurrences

class TestCoOccurrenceDB(unittest.TestCase):
    def setUp(self):
        self.mockup_file_path = '/home/x_caoll/EasyNer/tests/co_occurence_mockup.json'
        with open(self.mockup_file_path, 'r') as f:
            self.mockup_data = json.load(f)  # Read mockup_data from the file

    def tearDown(self):
        pass  # No need to clean up the file after tests

    def test_get_inter_entity_co_occurrences(self):
        self.maxDiff = None  # Set maxDiff to None to see the full diff
        sorted_files = [self.mockup_file_path]  # Use the file path in the test
        entity1 = "disease"
        entity2 = "phenomenon"
        result = get_inter_entity_co_occurrences(sorted_files, entity1, entity2)

        # Convert tuple keys to strings for JSON serialization
        result_str_keys = {str(key): value for key, value in result.items()}

        # Export the actual result to a JSON file
        with open('/home/x_caoll/EasyNer/tests/actual_result.json', 'w') as f:
            json.dump(result_str_keys, f, indent=4)

        expected_result = {
            "('disease1', 'phenomenon1')": [
                {"pmid": "1", "sentences": [
                    "[1] Sentence with disease1 and phenomenon1.",
                    "[1] Another sentence with disease1 and phenomenon1.",
                    "[1] Sentence with disease1, disease2 and phenomenon1.",
                    "[1] Sentence with disease1, disease1, phenomenon1 and phenomenon2."
                ]},
                {"pmid": "2", "sentences": [
                    "[2] Yet another sentence with disease1 and phenomenon1.",
                    "[2] Sentence with disease1, disease3, phenomenon1 and phenomenon2."
                ]}
            ],
            "('disease1', 'phenomenon2')": [
                {"pmid": "1", "sentences": [
                    "[1] Sentence with disease1, disease1, phenomenon1 and phenomenon2."
                ]},
                {"pmid": "2", "sentences": [
                    "[2] Sentence with disease1 and phenomenon2.",
                    "[2] Sentence with disease1, disease3, phenomenon1 and phenomenon2."
                ]}
            ],
            "('disease2', 'phenomenon1')": [
                {"pmid": "1", "sentences": [
                    "[1] Sentence with disease1, disease2 and phenomenon1."
                ]}
            ],
            "('disease2', 'phenomenon2')": [
                {"pmid": "1", "sentences": [
                    "[1] Sentence with disease2 and phenomenon2."
                ]}
            ],
            "('disease3', 'phenomenon1')": [
                {"pmid": "2", "sentences": [
                    "[2] Sentence with disease1, disease3, phenomenon1 and phenomenon2."
                ]}
            ],
            "('disease3', 'phenomenon2')": [
                {"pmid": "2", "sentences": [
                    "[2] Sentence with disease1, disease3, phenomenon1 and phenomenon2."
                ]}
            ]
        }

        self.assertEqual(result, expected_result)

    def test_store_and_query_co_occurrences(self):
        sorted_files = [self.mockup_file_path]
        entity1 = "disease1"
        entity2 = "phenomenon1"
        co_occurrences = get_inter_entity_co_occurrences(sorted_files, "disease", "phenomenon")

        db_path = '/home/x_caoll/EasyNer/tests/co_occurrences.db'
        store_co_occurrences_in_db(co_occurrences, db_path)

        # Query the database
        result = query_co_occurrences(db_path, entity1, entity2)

        # print("Query Result:", result)  # Debug statement

        expected_result = {
            "1": [
                "[1] Sentence with disease1 and phenomenon1.",
                "[1] Another sentence with disease1 and phenomenon1.",
                "[1] Sentence with disease1, disease2 and phenomenon1.",
                "[1] Sentence with disease1, disease1, phenomenon1 and phenomenon2."
            ],
            "2": [
                "[2] Yet another sentence with disease1 and phenomenon1.",
                "[2] Sentence with disease1, disease3, phenomenon1 and phenomenon2."
            ]
        }

        self.assertEqual(result, expected_result)

        # Clean up the database file
        os.remove(db_path)

if __name__ == '__main__':
    unittest.main()
