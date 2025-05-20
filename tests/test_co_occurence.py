import unittest
import json
import sys
import os

# TODO test for multiple occurrences of the same entity in a sentence
# TODO check when sorting is done, as it seems results must be alphabetically sorted

# Add EasyNer directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Use absolute import
from scripts.co_occurence import (
    get_inter_entity_co_occurrences,
    load_json,
    get_batch_index,
    get_intra_entity_co_occurrences,
    create_df_from_pairs,
)


class TestGetPairs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up paths for mockup and expected data files
        base_path = os.path.dirname(__file__)
        cls.mockup_path = os.path.join(base_path, "mockup.json")
        cls.expected_path = os.path.join(base_path, "expected_co_occurences.json")
        cls.output_path = os.path.join(base_path, "generated_output.json")

    def setUp(self):
        self.maxDiff = None  # Set maxDiff to None to see the full diff

    def test_get_pairs(self):
        # Test the get_pairs function
        with open(self.mockup_path, "r") as f:
            test_data = json.load(f)
        with open(self.expected_path, "r") as f:
            expected_pairs = json.load(f)

        sorted_files = [self.mockup_path]
        entity1 = "disease"
        entity2 = (
            "phenomenon"  # Note: This should match the key in the mockup.json file
        )

        pairs = get_inter_entity_co_occurrences(sorted_files, entity1, entity2)

        # Convert tuple keys to string keys and set values to sorted lists for comparison
        pairs_str_keys = {
            str(k): {
                "freq": v["freq"],
                "pmid": sorted(list(v["pmid"])),
                "sent": sorted(list(v["sent"])),
            }
            for k, v in pairs.items()
        }

        # Write the generated output to a file
        with open(self.output_path, "w") as f:
            json.dump(pairs_str_keys, f, indent=4)

        # Assert that the generated pairs match the expected pairs
        self.assertEqual(
            pairs_str_keys,
            expected_pairs,
            "Find co-occurrences between two different types of entities across a list of JSON files does not generate expected output",
        )

    def test_load_json(self):
        # Test the load_json function
        with open(self.mockup_path, "r") as f:
            expected_data = json.load(f)

        loaded_data = load_json(self.mockup_path)

        # Assert that the loaded data matches the expected data
        self.assertEqual(
            loaded_data,
            expected_data,
            "The loaded JSON data does not match the expected data.",
        )

    def test_get_batch_index(self):
        # Test the get_batch_index function
        filename = "file123.json"
        expected_index = 123
        # Assert that the extracted batch index matches the expected index
        self.assertEqual(
            get_batch_index(filename),
            expected_index,
            "The batch index extracted from the filename is incorrect.",
        )

    def test_get_inter_entity_co_occurrences(self):
        # Test the get_inter_entity_co_occurrences function
        with open(self.mockup_path, "r") as f:
            test_data = json.load(f)
        sorted_files = [self.mockup_path]
        entity1 = "disease"
        entity2 = "phenomenon"

        pairs = get_inter_entity_co_occurrences(sorted_files, entity1, entity2)

        # Convert tuple keys to string keys and set values to sorted lists for comparison
        pairs_str_keys = {
            str(k): {
                "freq": v["freq"],
                "pmid": sorted(list(v["pmid"])),
                "sent": sorted(list(v["sent"])),
            }
            for k, v in pairs.items()
        }

        # Write the generated output to a file
        with open(self.output_path, "w") as f:
            json.dump(pairs_str_keys, f, indent=4)

        with open(self.expected_path, "r") as f:
            expected_pairs = json.load(f)

        # Assert that the generated pairs match the expected pairs
        self.assertEqual(
            pairs_str_keys,
            expected_pairs,
            "Find co-occurrences within the same entity type does not generate expected output",
        )

    def test_create_df_from_pairs(self):
        # Test the create_df_from_pairs function
        with open(self.expected_path, "r") as f:
            pairs_dict = json.load(f)
        df = create_df_from_pairs(pairs_dict)
        # Assert that the DataFrame is not empty
        self.assertFalse(df.empty, "The DataFrame created from pairs_dict is empty.")

        # Check if the DataFrame has the expected columns
        expected_columns = ["entity_1", "entity_2", "frequency", "pmids", "sentences"]
        self.assertTrue(
            all(column in df.columns for column in expected_columns),
            "The DataFrame does not contain the expected columns.",
        )

        # Check if the DataFrame contains the expected data
        for key, val in pairs_dict.items():
            e1, e2 = eval(key)  # Convert string representation of tuple back to tuple
            # Assert that the pair (e1, e2) is present in the DataFrame
            self.assertTrue(
                ((df["entity_1"] == e1) & (df["entity_2"] == e2)).any(),
                f"The pair ({e1}, {e2}) is missing in the DataFrame.",
            )
            # Assert that the frequency, PMIDs, and sentences for the pair (e1, e2) are correct in the DataFrame
            self.assertTrue(
                (
                    (df["frequency"] == val["freq"])
                    & (df["pmids"] == ",".join(val["pmid"]))
                    & (df["sentences"] == " ; ".join(val["sent"]))
                ).any(),
                f"The data for the pair ({e1}, {e2}) is incorrect in the DataFrame.",
            )

        # Print the DataFrame for debugging
        print(df)


if __name__ == "__main__":
    unittest.main()
