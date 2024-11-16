import sqlite3

class EasyNerDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def list_all_entities(self):
        self.cursor.execute('SELECT DISTINCT entity FROM entities')
        entities = self.cursor.fetchall()
        return [entity[0] for entity in entities]

    def list_fq_of_entities(self, entity=None):
        if entity:
            self.cursor.execute('SELECT COUNT(entity) FROM entities WHERE entity = ?', (entity,))
            entities = self.cursor.fetchone()
            return entities[0]
        else:
            self.cursor.execute('SELECT entity, COUNT(entity) FROM entities GROUP BY entity')
            entities = self.cursor.fetchall()
            return {entity[0]: entity[1] for entity in entities}

    def get_sentence_cooccurence(self, entity1, entity2):
        """
        Get the number of cooccurrences and the details for two entities in sentences.

        Args:
            db_path (str): Path to the SQLite database file.
            entity1 (str): The first entity.
            entity2 (str): The second entity.

        Returns:
            tuple: A tuple containing the total count of cooccurrences and a list of pmids with sentence indexes.
        """

        query = '''
            SELECT s.pmid, s.sentence_index, e1.entity_text, e2.entity_text
            FROM sentences s
            JOIN entities e1 ON s.pmid = e1.pmid AND s.sentence_index = e1.sentence_index
            JOIN entities e2 ON s.pmid = e2.pmid AND s.sentence_index = e2.sentence_index
            WHERE e1.entity = ? AND e2.entity = ?
        '''
        self.cursor.execute(query, (entity1, entity2))
        results = self.cursor.fetchall()
        total_count = len(results)
        pmid_sentence_indexes = {}
        for result in results:
            pmid = result[0]
            sentence_index = result[1]
            entity_texts = (result[2], result[3])
            if pmid not in pmid_sentence_indexes:
                pmid_sentence_indexes[pmid] = []
            pmid_sentence_indexes[pmid].append((sentence_index, entity_texts))
        return total_count, pmid_sentence_indexes


if __name__ == "__main__":
    db_path = '/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/database.db'
    entity1 = 'Entity1'
    entity2 = 'Entity2'
    db = EasyNerDB(db_path)
    total_count, pmid_sentence_indexes = db.get_sentence_cooccurence(entity1, entity2)
    print(f"Total cooccurrences of '{entity1}' and '{entity2}': {total_count}")
    if pmid_sentence_indexes:
        for pmid, sentences in pmid_sentence_indexes.items():
            for sentence_index, entity_texts in sentences:
                print(f"PMID: {pmid}, Sentence Index: {sentence_index}, Entity Texts: {entity_texts}")
    else:
        print("No cooccurrences found.")
