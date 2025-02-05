import sqlite3
import logging
import time
from tqdm import tqdm
import pandas as pd

logging.basicConfig(level=logging.DEBUG, filename="entity_analysis.log", format="%(asctime)s - %(levelname)s - %(message)s")


def get_number_of_sentences(conn) -> int:
    """
    Get the number of sentences in the database.
    This assumes the database is optimized and empty rows have been removed as it uses the max sentence id to determine the number of sentences.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.

    Returns:
        int: The number of sentences in the database.

    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sentences")
    return cursor.fetchone()[0]

def get_sentence_ids(conn, sentence_batch_size=100000) -> list[list[int]]:
    """
    Get all unique sentence ids from the database and split them into batches of size sentence_batch_size.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.

    Returns:
        List[List[int]]: A list of lists of sentence ids.

    """
    cursor = conn.cursor()
    # Get query plan for getting max sentence id
    cursor.execute("EXPLAIN QUERY PLAN SELECT MAX(id) FROM sentences")
    query_plan = cursor.fetchall()
    logging.info("Query Plan:")
    for row in query_plan:
        logging.info(row)

    # Get the maximum sentence id
    start_time = time.time()
    cursor.execute("SELECT MAX(id) FROM sentences")
    max_id : int = cursor.fetchone()[0]
    end_time = time.time()
    logging.info(f"Retrieved max sentence id in {end_time - start_time:.2f} seconds")

    # Generate intervals based on the batch size
    start_time = time.time()
    sentence_batches = [(i, min(i + sentence_batch_size - 1, max_id)) for i in range(1, max_id + 1, sentence_batch_size)]
    end_time = time.time()
    logging.info(f"Generated {len(sentence_batches)} intervals for sentence ids in {end_time - start_time:.2f} seconds")

    return sentence_batches

def create_sent_entity_coccurrences_table(conn):
    # Check if the sentence_cooccurrences table exists
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(sentence_cooccurrences)")
    columns = cursor.fetchall()
    if not any(column[1] == "entity_1_id" for column in columns):
        cursor.execute(
            """
            CREATE TABLE sentence_cooccurrences (
                sentence_id INTEGER,
                entity_1_id INTEGER,
                entity_2_id INTEGER,
                FOREIGN KEY (sentence_id) REFERENCES sentences(id)
                FOREIGN KEY (entity_1_id) REFERENCES entities(id)
                FOREIGN KEY (entity_2_id) REFERENCES entities(id)
            )
            """
        )

        conn.commit()

def record_sentence_entity_cooccurences(conn, sentence_batch_size=300000):
    """
    Get all combinations of 2 unique entities in same sentence.
    Records the entity_ids in a new table called sentence_cooccurrences.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.
    sentence_batch_size (int): The number of sentences to process in each batch.

    Returns:
        None
    """
    cursor = conn.cursor()

    create_sent_entity_coccurrences_table(conn) # Create the sentence_cooccurrences table if it does not exist

    logging.info("Starting entity co-occurrence analysis")

    # Get number of sentences
    start_time = time.time()
    cursor.execute("SELECT COUNT(*) FROM sentences")
    number_of_sentences = cursor.fetchone()[0]
    end_time = time.time()
    logging.info(f"Retrieved number of sentences in {end_time - start_time:.2f} seconds")

    # Split the sentences into batches using list comprehension
    start_time = time.time()
    sentence_batches : list[list[int]] = [(i, min(i + sentence_batch_size - 1, number_of_sentences)) for i in range(1, number_of_sentences + 1, sentence_batch_size)]
    end_time = time.time()

    logging.info(f"Generated {len(sentence_batches)} intervals for sentence ids in {end_time - start_time:.2f} seconds")
    logging.debug(f"Sentence batches: {sentence_batches}")

    # Select each combination of 2 entities in the same sentence and record them in the sentence_cooccurrences table
    cursor.execute(
    """
    EXPLAIN QUERY PLAN
    INSERT INTO sentence_cooccurrences (sentence_id, entity_1_id, entity_2_id)
    SELECT eo1.sentence_id, eo1.entity_id AS entity_1_id, eo2.entity_id AS entity_2_id
    FROM entity_occurrences eo1
    JOIN entity_occurrences eo2 ON eo1.sentence_id = eo2.sentence_id
    WHERE eo1.entity_id < eo2.entity_id
    AND eo1.sentence_id BETWEEN 1 AND 100000
    """
    )

    # Define the query
    query = """
        INSERT INTO sentence_cooccurrences (sentence_id, entity_1_id, entity_2_id)
        SELECT eo1.sentence_id, eo1.entity_id AS entity_1_id, eo2.entity_id AS entity_2_id
        FROM entity_occurrences eo1
        JOIN entity_occurrences eo2 ON eo1.sentence_id = eo2.sentence_id
        WHERE eo1.entity_id < eo2.entity_id
        AND eo1.sentence_id BETWEEN ? AND ?
    """

    query_plan = cursor.fetchall()
    logging.info("Query Plan:")
    for row in query_plan:
        logging.info(row)

    # Loop over each interval of sentences
    for i, (start_id, end_id) in enumerate(tqdm(sentence_batches, desc="Processing batches")):
        batch_start_time = time.time()
        cursor.execute(query, (start_id, end_id))
        conn.commit()
        batch_end_time = time.time()
        logging.info(f"Processed batch {i+1}/{len(sentence_batches)} with sentence ids {start_id} to {end_id} in {batch_end_time - batch_start_time:.2f} seconds")

def vaildate_cooccurence_entity_classes(conn, entity_id : int, entity_class : str) -> bool:
    pass

def get_sentence_entity_cooccurences(conn, named_entity_class_1 : str, named_entity_class_2 : str, number_of_entities=100,) -> pd.DataFrame:
    """
    Get n co-occurrences of named entity class 1 and named entity class 2 in the same sentence from the sentence_cooccurrences table.
    Data is reterived by using entity ids and then joining with the entity_occurences table to:
    1: Check that the entities are of the correct class. If not, the row is filtered out.
    2:
    2: Get

    Args:
    conn (sqlite3.Connection): A SQLite database connection.
    named_entity_class_1 (str): The first named entity class.
    named_entity_class_2 (str): The second named entity class.
    top_n (int): The number of top co-occurrences to return.

    Returns:
        pd.DataFrame: A dataframe containing co-occurrences of named entity class 1 and named entity class 2 in the same sentence. Columns are sentence_id, entity_1_text, entity_1_ne_class, entity_2_text, entity_2_ne_class.

    """
    cursor = conn.cursor()

    # Get the entity ids for the named entity classes
    cursor.execute(
        """
        SELECT id
        FROM entities
        WHERE entity = ?
        """,
        (named_entity_class_1,)
    )

    entity_1_id = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT id
        FROM entities
        WHERE entity = ?
        """,
        (named_entity_class_2,)
    )

    entity_2_id = cursor.fetchone()[0]

    logging.debug(f"Entity 1: ({named_entity_class_1}): id: {entity_1_id}")
    logging.debug(f"Entity 2: ({named_entity_class_2}): id: {entity_2_id}")


    # Get matching cooccurrences from the sentence_cooccurrences table
    # Join with entity_occurrences table to get the entity text and named entity class


    cursor.execute(
        """
        SELECT sc.sentence_id, e1.entity_text, e2.entity_text
        FROM sentence_cooccurrences sc
        JOIN entity_occurrences e1 ON sc.entity_1_id = e1.id
        JOIN entity_occurrences e2 ON sc.entity_2_id = e2.id
        WHERE e1.entity_id = ? AND e2.entity_id = ?
        LIMIT ?
        """,
        (named_entity_class_1, named_entity_class_2, number_of_entities)
    )



    cooccurrences = cursor.fetchall() # List of tuples


    # Convert to a DataFrame
    cooccurrences_df = pd.DataFrame(cooccurrences, columns=["sentence_id", "entity_1_text", "entity_2_text"])


    logging.debug(f"Cooccurrences: {cooccurrences_df}")

    # Add named entity class columns
    cooccurrences_df["entity_1_ne_class"] = named_entity_class_1
    cooccurrences_df["entity_2_ne_class"] = named_entity_class_2
    return cooccurrences_df








