import sqlite3
import json
import csv


class EasyNerDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()


    def __del__(self):
        self.conn.close()

    def get_entity_fqs(self):
        """
        Update the frequency of each entity in the entities table.
        """

        # Ensure the necessary column exist
        self.cursor.execute("PRAGMA table_info(entities)")
        columns = [info[1] for info in self.cursor.fetchall()]
        if "fq" not in columns:
            self.cursor.execute(
                """
                ALTER TABLE entities
                ADD COLUMN fq INTEGER DEFAULT 0
            """
            )

        self.cursor.execute(
            """
            UPDATE entities
            SET fq = (
                SELECT COUNT(*)
                FROM entity_occurrences
                WHERE entity_occurrences.entity_id = entities.id
            )
        """
        )
        self.conn.commit()

    def list_all_entities(self):
        self.cursor.execute("SELECT DISTINCT entity FROM entities")
        entities = self.cursor.fetchall()
        return [entity[0] for entity in entities]

    def list_fq_of_entities(self, entity=None):
        if entity:
            self.cursor.execute(
                "SELECT COUNT(entity) FROM entities WHERE entity = ?", (entity,)
            )
            entities = self.cursor.fetchone()
            return entities[0]
        else:
            self.cursor.execute(
                "SELECT entity, COUNT(entity) FROM entities GROUP BY entity"
            )
            entities = self.cursor.fetchall()
            return {entity[0]: entity[1] for entity in entities}

    def get_titles(self):
        """
        Get all titles from the articles table.

        Returns:
            list: A list of tuples containing pmid and title.
        """
        self.cursor.execute("SELECT pmid, title FROM articles")
        titles = self.cursor.fetchall()
        return titles

    def get_title(self, pmid):
        """
        Get the title for a specific pmid from the articles table.

        Args:
            pmid (int): The pmid to get the title for.

        Returns:
            str: The title of the article.
        """
        self.cursor.execute("SELECT title FROM articles WHERE pmid = ?", (pmid,))
        title = self.cursor.fetchone()
        return title[0] if title else None

    def get_sentences(self, pmid):
        """
        Get sentences from the sentences table.

        Args:
            pmid (int, optional): Specific pmid to get sentences for.

        Returns:
            list: A list of tuples containing sentence_index, and text.
        """
        if pmid:
            self.cursor.execute(
                "SELECT sentence_index, text FROM sentences WHERE pmid = ?", (pmid,)
            )
        sentences = self.cursor.fetchall()
        return sentences

    def record_entity_cooccurrences(self, *entities):
        # ToDo: Implement with any number of entities

        # Ensure the necessary tables exist
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_cooccurrences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity1 INTEGER NOT NULL,
                entity2 INTEGER NOT NULL,
                e1_text TEXT NOT NULL,
                e2_text TEXT NOT NULL,
                e1_id INTEGER NOT NULL,
                e2_id INTEGER NOT NULL,
                sentence_id INTEGER NOT NULL,
                FOREIGN KEY (sentence_id) REFERENCES sentences(id),
                FOREIGN KEY (entity1) REFERENCES entities(id),
                FOREIGN KEY (entity2) REFERENCES entities(id),
                FOREIGN KEY (e1_id) REFERENCES entity_occurrences(id),
                FOREIGN KEY (e2_id) REFERENCES entity_occurrences(id)
            )
        """
        )
        self.conn.commit()

        # Get entity IDs
        entity_ids = []
        for entity in entities:
            self.cursor.execute("SELECT id FROM entities WHERE entity = ?", (entity,))
            entity_id = self.cursor.fetchone()
            if not entity_id:
                return {}
            entity_ids.append(entity_id[0])

        # Fetch cooccurrences
        self.cursor.execute(
            """
            SELECT eo1.sentence_id, eo1.sentence_index, eo1.entity_text AS e1_text, eo2.entity_text AS e2_text, eo1.id AS e1_id, eo2.id AS e2_id
            FROM entity_occurrences eo1
            JOIN entity_occurrences eo2 ON eo1.sentence_id = eo2.sentence_id
            WHERE eo1.entity_id = ? AND eo2.entity_id = ?
        """,
            (entity_ids[0], entity_ids[1]),
        )
        results = self.cursor.fetchall()
        for result in results:
            sentence_id, sentence_index, e1_text, e2_text, e1_id, e2_id = result
            # key = (e1_text, e2_text)
            # if key not in cooccurrences:
            #     cooccurrences[key] = {"freq": 0, "pmid": {}}
            # cooccurrences[key]["freq"] += 1
            # if sentence_id not in cooccurrences[key]["pmid"]:
            #     cooccurrences[key]["pmid"][sentence_id] = {"sentence_index": [], "sentence.id": []}
            # # cooccurrences[key]["pmid"][sentence_id]["sentence_index"].append(sentence_index)
            # # cooccurrences[key]["pmid"][sentence_id]["sentence.id"].append(sentence_id)
            # Insert into entity_cooccurrences table
            self.cursor.execute(
                """
                INSERT OR IGNORE INTO entity_cooccurrences (entity1, entity2, e1_text, e2_text, e1_id, e2_id, sentence_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    entity_ids[0],
                    entity_ids[1],
                    e1_text,
                    e2_text,
                    e1_id,
                    e2_id,
                    sentence_id,
                ),
            )
        self.conn.commit()
        # return cooccurrences

    def get_specified_entity_cooccurrences(self, e1_text, e2_text):
        """
        Get all matching entries of entity cooccurrences in the specified format.

        Args:
            e1_text (str): The text of the first entity.
            e2_text (str): The text of the second entity.

        Returns:
            dict: A dictionary representing the cooccurrences.
        """
        self.cursor.execute(
            """
            SELECT e1_text, e2_text, sentence_id, COUNT(*)
            FROM entity_cooccurrences
            WHERE e1_text = ? AND e2_text = ?
            GROUP BY e1_text, e2_text, sentence_id
        """,
            (e1_text, e2_text),
        )
        rows = self.cursor.fetchall()
        cooccurrences = {}
        for row in rows:
            key = (row[0], row[1])
            if key not in cooccurrences:
                cooccurrences[key] = {"freq": 0, "pmid": [], "sentence_ids": []}
            cooccurrences[key]["freq"] += row[3]
            cooccurrences[key]["pmid"].append(str(row[2]))
            cooccurrences[key]["sentence_ids"].append(row[2])
        return cooccurrences

    def count_cooccurence(self, entity1_text, entity2_text):
        """
        Count the number of cooccurrences of two entities by their text.

        Args:
            entity1_text (str): The text of the first entity.
            entity2_text (str): The text of the second entity.

        Returns:
            int: The number of cooccurrences.
        """
        self.cursor.execute(
            """
            SELECT COUNT(*)
            FROM entity_cooccurrences
            WHERE e1_text = ? AND e2_text = ?
        """,
            (entity1_text, entity2_text),
        )
        count = self.cursor.fetchone()[0]
        return count

    def sum_cooccurences(self):
        """
        Summarize the cooccurrences by counting the frequency of each unique pair of entity1_text and entity2_text.
        """
        # Ensure the necessary tables exist
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS coentity_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                e1_text TEXT NOT NULL,
                e2_text TEXT NOT NULL,
                fq INTEGER NOT NULL
            )
        ''')
        self.conn.commit()

        # Get all unique pairs of entity1_text and entity2_text
        self.cursor.execute('''
            SELECT DISTINCT e1_text, e2_text
            FROM entity_cooccurrences
        ''')
        unique_pairs = self.cursor.fetchall()

        # Calculate the frequency for each unique pair and insert into coentity_summary
        for e1_text, e2_text in unique_pairs:
            frequency = self.count_cooccurence(e1_text, e2_text)
            self.cursor.execute('''
                INSERT INTO coentity_summary (e1_text, e2_text, fq)
                VALUES (?, ?, ?)
            ''', (e1_text, e2_text, frequency))
        self.conn.commit()

        # Sort the coentity_summary table by frequency, e1_text, and e2_text
        self.cursor.execute('''
            CREATE TABLE coentity_summary_sorted AS
            SELECT * FROM coentity_summary
            ORDER BY fq DESC, e1_text ASC, e2_text ASC
        ''')
        self.cursor.execute('DROP TABLE coentity_summary')
        self.cursor.execute('ALTER TABLE coentity_summary_sorted RENAME TO coentity_summary')
        self.conn.commit()

    def export_cooccurrences(self, output_file):
        """
        Export cooccurrences to a CSV file.

        Args:
            output_file (str): The path to the output CSV file.
        """
        self.cursor.execute('''
            SELECT e1_text, e2_text, fq
            FROM coentity_summary
        ''')
        coentity_summary = self.cursor.fetchall()

        with open(output_file, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["entity_1", "entity_2", "fq"])
            writer.writerows(coentity_summary)

    def update_entity_name(self, old_name, new_name):
        """
        Update the entity name in the entities table.

        Args:
            old_name (str): The current name of the entity.
            new_name (str): The new name of the entity.
        """
        self.cursor.execute(
            "UPDATE entities SET entity = ? WHERE entity = ?", (new_name, old_name)
        )
        self.conn.commit()

if __name__ == "__main__":
    db_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/database.db"
    db = EasyNerDB(db_path)


    db.record_entity_cooccurrences("disease", "phenomena")

    entity1 = "Entity1"
    entity2 = "Entity2"
    total_count, pmid_sentence_indexes = db.record_entity_cooccurrences(
        entity1, entity2
    )
    print(f"\nTotal cooccurrences of '{entity1}' and '{entity2}': {total_count}")
    if pmid_sentence_indexes:
        for pmid, sentences in pmid_sentence_indexes.items():
            for sentence_index, entity_texts in sentences:
                print(
                    f"PMID: {pmid}, Sentence Index: {sentence_index}, Entity Texts: {entity_texts}"
                )
    else:
        print("No cooccurrences found.")
