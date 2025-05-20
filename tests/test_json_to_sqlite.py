import json
import unittest
from scripts.database.json_to_sqlite import process_chunk

class TestJsonToSqlite(unittest.TestCase):
    def setUp(self):
        with open('/home/carloa/Desktop/EasyNer/tests/mockup.json', 'r') as f:
            self.data = json.load(f)

    def test_process_chunk(self):
        self.maxDiff = None  # Set maxDiff to None to see the full diff
        chunk = {k: self.data[k] for k in list(self.data)[:2]}
        documents, sentences, named_entities, entity_occurrences, entity_cooccurrences, cooccurrence_summary, coentity_summary, source_files = process_chunk(chunk)

        expected_documents = [
            (1, 'Title 1', 0, 0, 0),
            (2, 'Title 2', 0, 0, 0)
        ]
        expected_sentences = [
            ('[1] Sentence with disease1 and phenomenon1.', 0, 1, 0, 0, 0, 0),
            ('[1] Sentence with disease2 and phenomenon2.', 1, 1, 0, 0, 0, 0),
            ('[1] Another sentence with disease1 and phenomenon1.', 2, 1, 0, 0, 0, 0),
            ('[1] Sentence without any entities.', 3, 1, 0, 0, 0, 0),
            ('[1] Sentence with only disease3.', 4, 1, 0, 0, 0, 0),
            ('[1] Sentence with only phenomenon3.', 5, 1, 0, 0, 0, 0),
            ('[1] Sentence with disease1, disease2 and phenomenon1.', 6, 1, 0, 0, 0, 0),
            ('[1] Sentence with disease1 and disease2 only.', 7, 1, 0, 0, 0, 0),
            ('[1] Sentence with disease1, disease1, phenomenon1 and phenomenon2.', 8, 1, 0, 0, 0, 0),
            ('[2] Sentence with disease1 and phenomenon2.', 0, 2, 0, 0, 0, 0),
            ('[2] Yet another sentence with disease1 and phenomenon1.', 1, 2, 0, 0, 0, 0),
            ('[2] Another sentence without any entities.', 2, 2, 0, 0, 0, 0),
            ('[2] Another sentence with only disease4.', 3, 2, 0, 0, 0, 0),
            ('[2] Another sentence with only phenomenon4.', 4, 2, 0, 0, 0, 0),
            ('[2] Sentence with disease1, disease3, phenomenon1 and phenomenon2.', 5, 2, 0, 0, 0, 0)
        ]
        expected_named_entities = {
            'disease': 1,
            'phenomenon': 2
        }
        expected_entity_occurrences = [
            (None, 'disease1', 18, 26, 1, 0, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon1', 31, 42, 2, 0, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease2', 18, 26, 3, 1, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon2', 31, 42, 4, 1, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease1', 22, 30, 1, 2, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon1', 35, 46, 2, 2, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease3', 23, 31, 5, 4, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon3', 20, 31, 6, 5, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease1', 18, 26, 1, 6, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease2', 28, 36, 3, 6, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon1', 41, 52, 2, 6, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease1', 18, 26, 1, 7, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease2', 31, 39, 3, 7, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease1', 18, 26, 1, 8, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease1', 28, 36, 1, 8, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon1', 41, 52, 2, 8, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon2', 54, 65, 4, 8, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease1', 18, 26, 1, 0, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon2', 31, 42, 4, 0, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease1', 30, 38, 1, 1, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon1', 43, 54, 2, 1, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease4', 31, 39, 7, 3, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon4', 31, 42, 8, 4, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease1', 18, 26, 1, 5, 0, 0.0, 0, 0.0, 0.0),
            (None, 'disease3', 28, 36, 5, 5, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon1', 38, 49, 2, 5, 0, 0.0, 0, 0.0, 0.0),
            (None, 'phenomenon2', 54, 65, 4, 5, 0, 0.0, 0, 0.0, 0.0)
        ]

        self.assertEqual(documents, expected_documents)
        self.assertEqual(sentences, expected_sentences)
        self.assertEqual(named_entities, expected_named_entities)
        self.assertEqual(entity_occurrences, expected_entity_occurrences)

if __name__ == '__main__':
    unittest.main()