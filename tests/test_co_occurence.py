import unittest
import json
import sys
import os

# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use absolute import
from scripts.co_occurence import get_pairs


class TestGetPairs(unittest.TestCase):
    def test_get_pairs(self):
        base_path = os.path.dirname(__file__)
        mockup_path = os.path.join(base_path, "mockup.json")
        expected_path = os.path.join(base_path, "expected_co_occurences.json")
        output_path = os.path.join(base_path, "generated_output.json")

        with open(mockup_path, "r") as f:
            test_data = json.load(f)
        with open(expected_path, "r") as f:
            expected_pairs = json.load(f)

        sorted_files = [mockup_path]
        entity1 = "disease"
        entity2 = "phenomenon"  # Note: This should match the key in the mockup.json file

        pairs = get_pairs(sorted_files, entity1, entity2)

        # Convert tuple keys to string keys and set values to sorted lists for comparison
        pairs_str_keys = {str(k): {'freq': v['freq'], 'pmid': sorted(list(v['pmid'])), 'sent': sorted(list(v['sent']))} for k, v in pairs.items()}

        # Write the generated output to a file
        with open(output_path, "w") as f:
            json.dump(pairs_str_keys, f, indent=4)

        self.assertEqual(pairs_str_keys, expected_pairs)

if __name__ == "__main__":
    unittest.main()