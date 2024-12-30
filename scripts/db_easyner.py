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
        try:
            self.conn = sqlite3.connect(self.db_path)
            print("Opened database successfully")
            self.cursor = self.conn.cursor()
        except sqlite3.OperationalError as e:
            print(f"Error: {e}")
            self.conn = None

    def __del__(self):
        if self.conn:
            self.conn.close()

    def get_tables(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        return [table[0] for table in tables]

    def create_indexes(self):
        """
        Create indexes on frequently queried columns to optimize performance.

        CREATE INDEX [IF NOT EXISTS] index_name ON table_name(column1, column2, ...);

        The trailing, table_name, is used to check if the table exists before creating the index.
        """
        indexes = [
            (
                "CREATE INDEX IF NOT EXISTS idx_entities_entity ON entities(entity, fq)",
                "entities",
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_entity_occurrences ON entity_occurrences(entity_id, entity_text, sentence_id)",
                "entity_occurrences",
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_sentences ON sentences(pmid)",
                "sentences",
            ),
            # (
            #     "CREATE INDEX IF NOT EXISTS idx_entity_cooccurrences ON entity_cooccurrences (e1_text, e2_text, e1_NE_id, e2_NE_id, sentence_id, coentity_summary_id)",
            #     "entity_cooccurrences",
            # ),
        ]

        existing_tables = self.get_tables()
        with tqdm(total=len(indexes), desc="Creating indexes") as pbar:
            for index_sql, table_name in indexes:
                if table_name in existing_tables:
                    self.cursor.execute(index_sql)
                    pbar.set_postfix(index=table_name)
                    pbar.update(1)

        self.conn.commit()

    def backup_db(self):
        """
        Backup the database by copying the file to a new location using sqlite3's backup API.
        """
        backup_path = f"{self.db_path}.backup"
        with sqlite3.connect(backup_path) as backup_conn:
            self.conn.backup(backup_conn)
        print(f"Database backed up to {backup_path}")


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

    def get_named_entity_id(self, named_entity: str):
        """
        Get the ID of a named entity from the entities table.

        Args:
            named_entity (str): The named entity to get the ID for.

        Returns:
            int: The ID of the named entity.
        """
        self.cursor.execute("SELECT id FROM entities WHERE entity = ?", (named_entity,))
        named_entity_id = self.cursor.fetchone()
        return named_entity_id[0] if named_entity_id else None

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

    def get_sentences(self, pmid) -> list:
        """
        Get sentences from the sentences table.

        Args:
            pmid (int, optional): Specific pmid to get sentences for.

        Returns:
            list: A list of tuples containing sentence_index, and text, word_count, token_count, alpha_count.
        """
        if pmid:
            self.cursor.execute(
                "SELECT sentence_index, text, word_count, token_count, alpha_count FROM sentences WHERE pmid = ?",
                (pmid,),
            )
        sentences = self.cursor.fetchall()
        return sentences

    def calc_article_length(self, pmid):
        """
        Calculate the word count, token count, and alpha count for an article and adds it to the correct column in the articles table.
        """
        self.cursor.execute(
            """
            SELECT SUM(word_count), SUM(token_count), SUM(alpha_count)
            FROM sentences
            WHERE pmid = ?
            """,
            (pmid,),
        )
        counts = self.cursor.fetchone()

        self.cursor.execute(
            """
            UPDATE articles
            SET word_count = ?, token_count = ?, alpha_count = ?
            WHERE pmid = ?
            """,
            (counts[0], counts[1], counts[2], pmid),
        )

    def calc_all_article_lengths(self):
        """
        Use get_sentences to get all sentences for each article and calculate the word count, token count, and alpha count for each article.
        """
        print("Calculating article lengths...")
        self.cursor.execute("SELECT pmid, title FROM articles")
        pmids = self.cursor.fetchall()
        for pmid in pmids:
            print(f"Calculating article length for {pmid[0]}")

            sentences = self.get_sentences(pmid[0])

            for s in sentences:
                print(s)

    def calc_entity_term_frequency(self, entity_type):
        """
        Calculate the term frequency (TF) for entities of a specific type.

        Args:
            entity_type (str): The type of the entity to calculate TF for.
        """
        # Ensure the necessary column exist
        self.cursor.execute("PRAGMA table_info(entity_occurrences)")
        columns = [info[1] for info in self.cursor.fetchall()]
        if "tf" not in columns:
            self.cursor.execute(
                """
                ALTER TABLE entity_occurrences
                ADD COLUMN tf INTEGER DEFAULT 0
            """
            )

        # Calculate the term frequency (TF) for each entity by the number of times it appears in the current document / word count of the current document

    def calculate_tf_idf_entities(self, entity_type):
        """
        Calculate the term frequency-inverse document frequency (TF-IDF) for entities of a specific type.

        Args:
            entity_type (str): The type of the entity to calculate TF-IDF for.
        """
        # Ensure the necessary column exist
        self.cursor.execute("PRAGMA table_info(entity_occurrences)")
        columns = [info[1] for info in self.cursor.fetchall()]
        if "tf" not in columns:
            self.cursor.execute(
                """
                ALTER TABLE entity_occurrences
                ADD COLUMN tf INTEGER DEFAULT 0
            """
            )
        if "tf-idf" not in columns:
            self.cursor.execute(
                """
                ALTER TABLE entity_occurrences
                ADD COLUMN tf-idf INTEGER DEFAULT 0
            """
            )

        # Calculate the term frequency (TF) for each entity

        self.cursor.execute(
            """
            SELECT entity_text, COUNT(*) as fq
            FROM entity_occurrences
            JOIN entities ON entity_occurrences.entity_id = entities.id
            WHERE entities.entity = ?
            GROUP BY entity_text
            ORDER BY fq DESC
        """,
            (entity_type,),
        )
        entity_frequencies = self.cursor.fetchall()
        total_documents = self.cursor.execute(
            "SELECT COUNT(DISTINCT sentence_id) FROM entity_occurrences"
        ).fetchone()[0]
        for entity, fq in entity_frequencies:
            self.cursor.execute(
                """
                SELECT COUNT(DISTINCT sentence_id)
                FROM entity_occurrences
                WHERE entity_text = ?
            """,
                (entity,),
            )
            document_frequency = self.cursor.fetchone()[0]
            tf_idf = fq * (total_documents / document_frequency)
            print(f"{entity}: TF-IDF = {tf_idf}")

    def record_entity_cooccurrences(self, *entities, batch_size=1000):
        """
        Record co-occurrences of entities in the database.

        Args:
            entities (str): The entity types to find co-occurrences for.
            batch_size (int): The number of rows to process in each batch.

        ToDo: Implement with any number of entities
        ToDo: Filter out where spans are the same
        """

        print("-------------Recording entity cooccurrences--------------")

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_cooccurrences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                e1_text TEXT NOT NULL,
                e2_text TEXT NOT NULL,
                e1_NE_id INTEGER NOT NULL,
                e2_NE_id INTEGER NOT NULL,
                e1_id INTEGER NOT NULL,
                e2_id INTEGER NOT NULL,
                sentence_id INTEGER NOT NULL,
                coentity_summary_id INTEGER,
                FOREIGN KEY (sentence_id) REFERENCES sentences(id),
                FOREIGN KEY (e1_NE_id) REFERENCES entities(id),
                FOREIGN KEY (e2_NE_id) REFERENCES entities(id),
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

        print(f"Matching entity IDs: {entity_ids[0]} and {entity_ids[1]}")

        # Fetch the cooccurrences and count them in Python
        print(":--Fetching cooccurrences------------")
        self.cursor.execute(
            """
            SELECT eo1.sentence_id, eo1.sentence_index, eo1.entity_text AS e1_text, eo2.entity_text AS e2_text, eo1.id AS e1_id, eo2.id AS e2_id
            FROM entity_occurrences eo1
            JOIN entity_occurrences eo2 ON eo1.sentence_id = eo2.sentence_id
            WHERE eo1.entity_id = ? AND eo2.entity_id = ?
            """,
            (entity_ids[0], entity_ids[1]),
        )
        cooccurrences = self.cursor.fetchall()
        total = len(cooccurrences)

        with tqdm(total=total, desc="Recording entity cooccurrences") as pbar:
            for cooccurrence in cooccurrences:
                sentence_id, sentence_index, e1_text, e2_text, e1_id, e2_id = cooccurrence
                self.cursor.execute(
                    """
                    INSERT OR IGNORE INTO entity_cooccurrences (e1_NE_id, e2_NE_id, e1_text, e2_text, e1_id, e2_id, sentence_id)
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

        self.cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entity_cooccurrences_texts ON entity_cooccurrences (e1_text, e2_text)
        """
        )
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
        Also calculate the document frequency, i.e in how many documents the entity appears (irrespective of the number of times it appears in the document).
        """
        # Ensure the necessary table exists
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_fq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id INTEGER NOT NULL,
                entity_text TEXT NOT NULL,
                fq INTEGER NOT NULL,
                doc_fq INTEGER NOT NULL,
                FOREIGN KEY (entity_id) REFERENCES entities(id)
            )
        """
        )

        # Add column to entity_occurrences table referencing the entity_fq table
        self.cursor.execute("PRAGMA table_info(entity_occurrences)")
        columns = [info[1] for info in self.cursor.fetchall()]
        if "doc_fq" not in columns:
            self.cursor.execute(
                """
                ALTER TABLE entity_occurrences
                ADD COLUMN doc_fq INTEGER
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

        # Get the document frequency of each entity
        self.cursor.execute(
            """
            SELECT entity_id, entity_text, COUNT(DISTINCT s.pmid) as doc_fq
            FROM entity_occurrences eo
            JOIN sentences s ON eo.sentence_id = s.id
            GROUP BY entity_id, entity_text
        """
        )
        entity_doc_frequencies = self.cursor.fetchall()

        # Insert the frequencies into the entity_fq table
        with tqdm(
            total=len(entity_frequencies), desc="Counting entity frequencies"
        ) as pbar:
            for (entity_id, entity_text, fq), (_, _, doc_fq) in zip(
                entity_frequencies, entity_doc_frequencies
            ):
                self.cursor.execute(
                    """
                    INSERT INTO entity_fq (entity_id, entity_text, fq, doc_fq)
                    VALUES (?, ?, ?, ?)
                    """,
                    (entity_id, entity_text, fq, doc_fq),
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
        self.cursor.execute("DROP TABLE entity_fq")
        self.cursor.execute("ALTER TABLE entity_fq_sorted RENAME TO entity_fq")
        self.conn.commit()

    def get_entity_fq(self, entity_filter=None):
        """
        Get the frequency of each unique entity_text and entity_id in the entity_occurrences table.

        Args:
            entity_filter (str, optional): Filter to get the frequency of a specific entity.

        Returns:
            list: A list of tuples containing entity_text, fq, and entity
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

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["entity_text", "total_count", "entity"])
            writer.writerows(entity_frequencies)

    def sum_cooccurences(self):
        """
        Summarize the cooccurrences by counting the frequency of each unique pair of entity1_text and entity2_text.

        Calculate the weighted frequency of each pair of entity1_text and entity2_text by getting the tf-idf of each entity and multiplying them together.
        """
        # Ensure the necessary tables exist
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS coentity_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                e1_text TEXT NOT NULL,
                e2_text TEXT NOT NULL,
                fq INTEGER NOT NULL,
                weighted_fq REAL,
                e1_tf_idf TEXT,
                e2_tf_idf TEXT,
                avg_e1_tf_idf REAL,
                avg_e2_tf_idf REAL
            )
        """
        )


        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_entity_cooccurrences_texts ON entity_cooccurrences (e1_text, e2_text)"
        )

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

        # Get all unique pairs of entity1_text and entity2_text that haven't been processed yet and also get their weighted frequency
        self.cursor.execute(
            """
            SELECT DISTINCT e1_text, e2_text
            FROM entity_cooccurrences
            WHERE coentity_summary_id IS NULL
        """
        )
        unique_pairs = self.cursor.fetchall()

        db_path = self.db_path
        pairs_with_db_path = [
            (db_path, e1_text, e2_text) for e1_text, e2_text in unique_pairs
        ]

        with Pool(cpu_count()) as pool:
            results = list(
                tqdm(
                    pool.imap(count_cooccurrence_worker, pairs_with_db_path),
                    total=len(unique_pairs),
                    desc="Summarizing cooccurrences",
                )
            )

        for e1_text, e2_text, frequency in results:
            # Get tf-idf values for e1_text and e2_text
            self.cursor.execute(
                """
                SELECT tf_idf
                FROM entity_occurrences
                WHERE entity_text = ? AND entity_id IN (
                    SELECT entity_id
                    FROM entity_occurrences
                    WHERE entity_text = ?
                )
            """,
                (e1_text, e1_text),
            )
            e1_tf_idf_values = [row[0] for row in self.cursor.fetchall()]

            self.cursor.execute(
                """
                SELECT tf_idf
                FROM entity_occurrences
                WHERE entity_text = ? AND entity_id IN (
                    SELECT entity_id
                    FROM entity_occurrences
                    WHERE entity_text = ?
                )
            """,
                (e2_text, e2_text),
            )
            e2_tf_idf_values = [row[0] for row in self.cursor.fetchall()]

            e1_tf_idf_str = json.dumps(e1_tf_idf_values)
            e2_tf_idf_str = json.dumps(e2_tf_idf_values)

            avg_e1_tf_idf = sum(e1_tf_idf_values) / len(e1_tf_idf_values)
            avg_e2_tf_idf = sum(e2_tf_idf_values) / len(e2_tf_idf_values)
            weighted_frequency = frequency * avg_e1_tf_idf * avg_e2_tf_idf

            self.cursor.execute(
                """
                INSERT INTO coentity_summary (e1_text, e2_text, fq, weighted_fq, e1_tf_idf, e2_tf_idf, avg_e1_tf_idf, avg_e2_tf_idf)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (e1_text, e2_text, frequency, weighted_frequency, e1_tf_idf_str, e2_tf_idf_str, avg_e1_tf_idf, avg_e2_tf_idf),
            )
            coentity_summary_id = self.cursor.lastrowid

            self.cursor.execute(
                """
                UPDATE entity_cooccurrences
                SET coentity_summary_id = ?
                WHERE e1_text = ? AND e2_text = ?
            """,
                (coentity_summary_id, e1_text, e2_text),
            )
        self.conn.commit()

        # Sort the coentity_summary table by frequency, e1_text, and e2_text
        self.cursor.execute(
            """
            CREATE TABLE coentity_summary_sorted AS
            SELECT * FROM coentity_summary
            ORDER BY fq DESC, e1_text ASC, e2_text ASC
        """
        )
        self.cursor.execute("DROP TABLE coentity_summary")
        self.cursor.execute(
            "ALTER TABLE coentity_summary_sorted RENAME TO coentity_summary"
        )
        self.conn.commit()

    def export_cooccurrences(self, output_file, top_n=None):
        """
        Export cooccurrences to a CSV file.

        Args:
            output_file (str): The path to the output CSV file.
            top_n (int, optional): The number of top cooccurrences to export. If None, export all.
        """
        self.cursor.execute(
            """
            SELECT e1_text, e2_text, fq
            FROM coentity_summary
        """
        )
        coentity_summary = self.cursor.fetchall()

        with open(output_file, "w", newline="", encoding="utf-8") as f:
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
        print(f"Updating entity name from '{old_name}' to '{new_name}'")
        self.cursor.execute(
            "SELECT COUNT(*) FROM entities WHERE entity = ?", (old_name,)
        )
        total = self.cursor.fetchone()[0]
        with tqdm(total=total, desc="Updating entity names") as pbar:
            self.cursor.execute(
                "UPDATE entities SET entity = ? WHERE entity = ?", (new_name, old_name)
            )
            self.conn.commit()
            pbar.update(total)
        if total:
            print(f"Successfully updated entity name from '{old_name}' to '{new_name}'")
        else:
            print(f"Error: Entity '{old_name}' not found.")

    def clone_table(self, table_name, clone_name):
        """
        Clone a table in the database.

        Args:
            table_name (str): The name of the table to clone.
            clone_name (str): The name of the new cloned table.
        """
        try:
            self.cursor.execute(
                f"CREATE TABLE {clone_name} AS SELECT * FROM {table_name}"
            )
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
        self.cursor.execute(
            f"UPDATE {table_name} SET {column_name} = LOWER({column_name})"
        )
        self.conn.commit()

    def drop_entity_cooccurrence(
        self, entity1_text, entity1_type, entity2_text=None, entity2_type=None
    ):
        """
        Delete entity cooccurrences from the entity_cooccurrences table matching entity1_text and entity1_type.
        If entity2_text and entity2_type are provided, delete only the matching entity pairs.
        """
        try:
            # Get entity IDs for entity type 1
            entity1_id = self.get_named_entity_id(entity1_type)
            if entity1_id is None:
                print(f"Error: Entity ID for {entity1_type} not found.")
                return
            print(f"Entity 1 ID: {entity1_id}")

            if entity2_text and entity2_type:
                entity2_id = self.get_named_entity_id(entity2_type)
                if entity2_id is None:
                    print(f"Error: Entity ID for {entity2_type} not found.")
                    return

                cooccurrences = self.get_specified_entity_cooccurrences(
                    entity1_text, entity2_text
                )
                if not cooccurrences:
                    print(
                        f"Error: No cooccurrences found for {entity1_text} and {entity2_text}."
                    )
                    return

                for key in cooccurrences.keys():
                    self.cursor.execute(
                        "DELETE FROM entity_cooccurrences WHERE e1_NE_id = ? AND e1_text = ? AND e2_NE_id = ? AND e2_text = ?",
                        (entity1_id, entity1_text, entity2_id, entity2_text),
                    )
                    self.cursor.execute(
                        "UPDATE coentity_summary SET fq = fq - 1 WHERE e1_text = ? AND e2_text = ?",
                        (entity1_text, entity2_text),
                    )
            else:
                print(
                    f"Deleting all cooccurrences for {entity1_text}... of type {entity1_type} with ID {entity1_id}"
                )
                self.cursor.execute(
                    "DELETE FROM entity_cooccurrences WHERE e1_NE_id = ? AND e1_text = ?",
                    (entity1_id, entity1_text),
                )
                self.cursor.execute(
                    "UPDATE coentity_summary SET fq = fq - 1 WHERE e1_text = ?",
                    (entity1_text,),
                )

            self.conn.commit()
            print(
                f"Successfully deleted cooccurrences for {entity1_text} of type {entity1_type}. {entity2_text if entity2_text else ''}."
            )
        except Exception as e:
            print(f"Error occurred while deleting cooccurrences: {e}")

    def drop_entities_from_occurrences(
        self, entity_text, entity_type, delete_cooccurrences=False
    ):
        """
        Drop every matching entity_text and entity_type from entity_occurrences.

        Args:
            entity_text (str): The text of the entity to drop.
            entity_type (str): The type of the entity to drop.
        """
        try:
            # Get entity ID
            entity_id = self.get_named_entity_id(entity_type)
            if entity_id is None:
                print(f"Error: Entity ID for {entity_type} not found.")
                return

            # Delete from entity_occurrences table
            self.cursor.execute(
                "DELETE FROM entity_occurrences WHERE entity_id = ? AND entity_text = ?",
                (entity_id, entity_text),
            )
            self.conn.commit()
            print(
                f"Successfully deleted occurrences of entity {entity_text} of type {entity_type}."
            )

            if delete_cooccurrences:
                # Run the drop_entity_cooccurrence method to delete cooccurrences
                self.drop_entity_cooccurrence(entity_text, entity_type)

        except Exception as e:
            print(f"Error occurred while deleting entity occurrences: {e}")

    def drop_named_entities(self, named_entity: str):
        pass


if __name__ == "__main__":
    pass
