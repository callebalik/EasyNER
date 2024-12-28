import sqlite3
import json
import csv
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

# ...existing code...

def count_cooccurrence_worker(args):
    db_path, e1_text, e2_text = args
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM entity_cooccurrences
        WHERE e1_text = ? AND e2_text = ?
    """,
        (e1_text, e2_text),
    )
    count = cursor.fetchone()[0]
    conn.close()
    return (e1_text, e2_text, count)

class EasyNerDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        # self.create_indexes()

    def create_indexes(self):
        """
        Create indexes on frequently queried columns to optimize performance.
        """
        indexes = [
            ("CREATE INDEX IF NOT EXISTS idx_entities_entity ON entities(entity)", "entities(entity)"),
            ("CREATE INDEX IF NOT EXISTS idx_entity_occurrences_entity_id ON entity_occurrences(entity_id)", "entity_occurrences(entity_id)"),
            ("CREATE INDEX IF NOT EXISTS idx_entity_occurrences_sentence_id ON entity_occurrences(sentence_id)", "entity_occurrences(sentence_id)"),
            ("CREATE INDEX IF NOT EXISTS idx_sentences_pmid ON sentences(pmid)", "sentences(pmid)"),
            ("CREATE INDEX IF NOT EXISTS idx_articles_pmid ON articles(pmid)", "articles(pmid)"),
            ("CREATE INDEX IF NOT EXISTS idx_entity_cooccurrences_texts ON entity_cooccurrences (e1_text, e2_text)", "entity_cooccurrences(e1_text, e2_text)")
        ]

        with tqdm(total=len(indexes), desc="Creating indexes") as pbar:
            for index_sql, index_name in indexes:
                self.cursor.execute(index_sql)
                pbar.set_postfix(index=index_name)
                pbar.update(1)

        self.conn.commit()

    def __del__(self):
        self.conn.close()

    def get_named_entity_fqs(self):
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
         # ToDo: Filter out where spans are the same
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
                coentity_summary_id INTEGER,
                FOREIGN KEY (sentence_id) REFERENCES sentences(id),
                FOREIGN KEY (entity1) REFERENCES entities(id),
                FOREIGN KEY (entity2) REFERENCES entities(id),
                FOREIGN KEY (e1_id) REFERENCES entity_occurrences(id),
                FOREIGN KEY (e2_id) REFERENCES entity_occurrences(id),
                FOREIGN KEY (coentity_summary_id) REFERENCES coentity_summary(id)
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

        self.cursor.execute(
            """
            SELECT COUNT(*)
            FROM entity_occurrences eo1
            JOIN entity_occurrences eo2 ON eo1.sentence_id = eo2.sentence_id
            WHERE eo1.entity_id = ? AND eo2.entity_id = ?
            """,
            (entity_ids[0], entity_ids[1]),
        )
        total = self.cursor.fetchone()[0]

        with tqdm(total=total, desc="Recording entity cooccurrences") as pbar:
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
                pbar.update(1)

        self.cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_entity_cooccurrences_texts ON entity_cooccurrences (e1_text, e2_text)
        ''')
        self.cursor.execute('PRAGMA index_list("entity_cooccurrences")')

        self.conn.commit()

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

    def _count_cooccurrence_worker(self, pair):
        db_path, e1_text, e2_text = pair
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM entity_cooccurrences
            WHERE e1_text = ? AND e2_text = ?
        """,
            (e1_text, e2_text),
        )
        count = cursor.fetchone()[0]
        conn.close()
        return (e1_text, e2_text, count)

    def count_entity_fq(self):
        """
        Count the frequency of each unique entity_text and entity_id in the entity_occurrences table
        and store the results in a new table entity_fq.
        """
        # Ensure the necessary table exists
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_fq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id INTEGER NOT NULL,
                entity_text TEXT NOT NULL,
                fq INTEGER NOT NULL,
                FOREIGN KEY (entity_id) REFERENCES entities(id)
            )
        """
        )

        # Get the frequency of each unique entity_text and entity_id
        self.cursor.execute(
            """
            SELECT entity_id, entity_text, COUNT(*) as fq
            FROM entity_occurrences
            GROUP BY entity_id, entity_text
        """
        )
        entity_frequencies = self.cursor.fetchall()

        # Insert the frequencies into the entity_fq table
        with tqdm(total=len(entity_frequencies), desc="Counting entity frequencies") as pbar:
            for entity_id, entity_text, fq in entity_frequencies:
                self.cursor.execute(
                    """
                    INSERT INTO entity_fq (entity_id, entity_text, fq)
                    VALUES (?, ?, ?)
                """,
                    (entity_id, entity_text, fq),
                )
                pbar.update(1)

        self.conn.commit()

        # Sort the entity_fq table by fq and entity_text
        self.cursor.execute(
            """
            CREATE TABLE entity_fq_sorted AS
            SELECT * FROM entity_fq
            ORDER BY fq DESC, entity_text ASC
        """
        )
        self.cursor.execute('DROP TABLE entity_fq')
        self.cursor.execute('ALTER TABLE entity_fq_sorted RENAME TO entity_fq')
        self.conn.commit()

    def get_entity_fq(self, entity_filter=None):
        """
        Get the frequency of each unique entity_text and entity_id in the entity_occurrences table.

        Args:
            entity_filter (str, optional): Filter to get the frequency of a specific entity.

        Returns:
            list: A list of tuples containing entity_text, fq, and entity.
        """
        if entity_filter:
            self.cursor.execute(
                """
                SELECT ef.entity_text, ef.fq, e.entity
                FROM entity_fq ef
                JOIN entities e ON ef.entity_id = e.id
                WHERE e.entity = ?
            """,
                (entity_filter,),
            )
        else:
            self.cursor.execute(
                """
                SELECT ef.entity_text, ef.fq, e.entity
                FROM entity_fq ef
                JOIN entities e ON ef.entity_id = e.id
            """
            )
        entity_frequencies = self.cursor.fetchall()
        return entity_frequencies

    def export_entity_fq(self, output_file, entity_filter=None):
        """
        Export the entity frequencies to a CSV file.

        Args:
            output_file (str): The path to the output CSV file.
            entity_filter (str, optional): Filter to export only matching entity.
        """
        if entity_filter:
            self.cursor.execute(
                """
                SELECT ef.entity_text, ef.fq, e.entity
                FROM entity_fq ef
                JOIN entities e ON ef.entity_id = e.id
                WHERE e.entity = ?
            """, (entity_filter,)
            )
        else:
            self.cursor.execute(
                """
                SELECT ef.entity_text, ef.fq, e.entity
                FROM entity_fq ef
                JOIN entities e ON ef.entity_id = e.id
            """
            )
        entity_frequencies = self.cursor.fetchall()

        with open(output_file, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["entity_text", "total_count", "entity"])
            writer.writerows(entity_frequencies)

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

        print("Creating index")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_cooccurrences_texts ON entity_cooccurrences (e1_text, e2_text)")

        # Ensure the coentity_summary_id column exists in entity_cooccurrences
        self.cursor.execute("PRAGMA table_info(entity_cooccurrences)")
        columns = [info[1] for info in self.cursor.fetchall()]
        if "coentity_summary_id" not in columns:
            self.cursor.execute(
                """
                ALTER TABLE entity_cooccurrences
                ADD COLUMN coentity_summary_id INTEGER,
                ADD FOREIGN KEY (coentity_summary_id) REFERENCES coentity_summary(id)
            """
            )

        self.conn.commit()

        # Get all unique pairs of entity1_text and entity2_text that haven't been processed yet
        self.cursor.execute('''
            SELECT DISTINCT e1_text, e2_text
            FROM entity_cooccurrences
            WHERE coentity_summary_id IS NULL
        ''')
        unique_pairs = self.cursor.fetchall()

        db_path = self.db_path
        pairs_with_db_path = [(db_path, e1_text, e2_text) for e1_text, e2_text in unique_pairs]

        with Pool(cpu_count()) as pool:
            results = list(tqdm(pool.imap(count_cooccurrence_worker, pairs_with_db_path), total=len(unique_pairs), desc="Summarizing cooccurrences"))

        for e1_text, e2_text, frequency in results:
            self.cursor.execute('''
                INSERT INTO coentity_summary (e1_text, e2_text, fq)
                VALUES (?, ?, ?)
            ''', (e1_text, e2_text, frequency))
            coentity_summary_id = self.cursor.lastrowid

            self.cursor.execute('''
                UPDATE entity_cooccurrences
                SET coentity_summary_id = ?
                WHERE e1_text = ? AND e2_text = ?
            ''', (coentity_summary_id, e1_text, e2_text))
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

    def export_cooccurrences(self, output_file, top_n=None):
        """
        Export cooccurrences to a CSV file.

        Args:
            output_file (str): The path to the output CSV file.
            top_n (int, optional): The number of top cooccurrences to export. If None, export all.
        """
        self.cursor.execute('''
            SELECT e1_text, e2_text, fq
            FROM coentity_summary
        ''')
        coentity_summary = self.cursor.fetchall()

        with open(output_file, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["entity_1", "entity_2", "fq"])
            if top_n:
                writer.writerows(coentity_summary[:top_n])
            else:
                writer.writerows(coentity_summary)

    def update_entity_name(self, old_name, new_name):
        """
        Update the entity name in the entities table.

        Args:
            old_name (str): The current name of the entity.
            new_name (str): The new name of the entity.
        """
        self.cursor.execute("SELECT COUNT(*) FROM entities WHERE entity = ?", (old_name,))
        total = self.cursor.fetchone()[0]
        with tqdm(total=total, desc="Updating entity names") as pbar:
            self.cursor.execute(
                "UPDATE entities SET entity = ? WHERE entity = ?", (new_name, old_name)
            )
            self.conn.commit()
            pbar.update(total)

    def clone_table(self, table_name, clone_name):
        """
        Clone a table in the database.

        Args:
            table_name (str): The name of the table to clone.
            clone_name (str): The name of the new cloned table.
        """
        try:
            self.cursor.execute(f"CREATE TABLE {clone_name} AS SELECT * FROM {table_name}")
            self.conn.commit()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print(f"Error: Table '{table_name}' does not exist.")
            else:
                raise

    def drop_table(self, table_name):
        """
        Drop a table from the database.

        Args:
            table_name (str): The name of the table to drop.
        """
        try:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table '{table_name}' dropped successfully.")
            self.conn.commit()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print(f"Error: Table '{table_name}' does not exist.")
            else:
                raise
    def lowercase_column(self, table_name, column_name):
        """
        Lowercase all values in a column of a table. This merges entities with different cases.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column.
        """
        self.cursor.execute(f"UPDATE {table_name} SET {column_name} = LOWER({column_name})")
        self.conn.commit()

if __name__ == "__main__":
    db_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/database6.db"
    db = EasyNerDB(db_path)
    print("Opened database successfully")
    # db.update_entity_name("phenoma", "PNM")
    # db.update_entity_name("disease", "DIS")

    # db.record_entity_cooccurrences("DIS", "PNM")
    # db.sum_cooccurences()
    cooc_path  = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/cooc50.csv"
    # db.export_cooccurrences(cooc_path, top_n=5000)
    # db.count_entity_fq()
    entity_fq_path = "/home/x_caoll/EasyNer/results/analysis/analysis_phenoma/entity_fq.csv"
    db.export_entity_fq(entity_fq_path, "PNM")

    # db.create_indexes()
