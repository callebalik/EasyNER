import unittest
from unittest.mock import patch, mock_open
import json
import sys
import os

# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use absolute import
from scripts.co_occurence import get_pairs


class TestGetPairs(unittest.TestCase):
    @patch("builtins.open", new_callable=mock_open, read_data=json.dumps({
        "1": {
            "sentences": [
                {
                    "entities": {
                        "disease": ["disease1"],
                        "phenoma": ["phenoma1"]
                    },
                    "text": "Sentence with disease1 and phenoma1."
                },
                {
                    "entities": {
                        "disease": ["disease2"],
                        "phenoma": ["phenoma2"]
                    },
                    "text": "Sentence with disease2 and phenoma2."
                }
            ]
        },
        "2": {
            "sentences": [
                {
                    "entities": {
                        "disease": ["disease1"],
                        "phenoma": ["phenoma2"]
                    },
                    "text": "Sentence with disease1 and phenoma2."
                }
            ]
        }
    }))
    def test_get_pairs(self, mock_file):
        sorted_files = ["mock_file.json"]
        entity1 = "disease"
        entity2 = "phenoma"

        expected_pairs = {
            ("disease1", "phenoma1"): {
                "freq": 1,
                "pmid": {"1"},
                "sent": {"Sentence with disease1 and phenoma1."}
            },
            ("disease2", "phenoma2"): {
                "freq": 1,
                "pmid": {"1"},
                "sent": {"Sentence with disease2 and phenoma2."}
            },
            ("disease1", "phenoma2"): {
                "freq": 1,
                "pmid": {"2"},
                "sent": {"Sentence with disease1 and phenoma2."}
            }
        }

        pairs = get_pairs(sorted_files, entity1, entity2)
        self.assertEqual(pairs, expected_pairs)

if __name__ == "__main__":
    unittest.main()