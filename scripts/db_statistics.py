import pandas as pd
import sqlite3
import logging
from tqdm import tqdm
import numpy as np


def verify_sentences_table(conn):
    """
    Verify that the sentences table exists and contains data.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sentences LIMIT 5")
    rows = cursor.fetchall()
    if not rows:
        logging.warning("No data found in the sentences table")
    else:
        for row in rows:
            print(row)


def update_null_columns(conn):
    """
    Update null values in word_count, token_count, and alpha_count columns in the articles table.
    """
    cursor = conn.cursor()

    # Update null values with default values or calculated values
    cursor.execute("UPDATE articles SET word_count = 0 WHERE word_count IS NULL")
    cursor.execute("UPDATE articles SET token_count = 0 WHERE token_count IS NULL")
    cursor.execute("UPDATE articles SET alpha_count = 0 WHERE alpha_count IS NULL")

    conn.commit()


def calc_article_counts(conn, batch_size=1000):
    """
    Calculate word count, token count, and alphabetic character count for each article in batches if it is not already calculated.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.
    batch_size (int): The number of articles to process in each batch.
    """
    cursor = conn.cursor()

    # check if any articles have null values for word_count, token_count, or alpha_count
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM articles
        WHERE word_count IS NULL OR token_count IS NULL OR alpha_count IS NULL
        """
    )
    null_articles = cursor.fetchone()[0]

    if null_articles == 0:
        logging.info(
            "All articles have word_count, token_count, and alpha_count calculated"
        )
        return

    cursor.execute("SELECT COUNT(*) FROM articles")
    total_articles = cursor.fetchone()[0]

    with tqdm(total=total_articles, desc="Calculating article counts") as pbar:
        for offset in range(0, total_articles, batch_size):
            cursor.execute(
                """
                UPDATE articles
                SET word_count = (
                    SELECT SUM(word_count)
                    FROM sentences
                    WHERE sentences.pmid = articles.pmid
                ),
                token_count = (
                    SELECT SUM(token_count)
                    FROM sentences
                    WHERE sentences.pmid = articles.pmid
                ),
                alpha_count = (
                    SELECT SUM(alpha_count)
                    FROM sentences
                    WHERE sentences.pmid = articles.pmid
                )
                WHERE (word_count IS NULL OR token_count IS NULL OR alpha_count IS NULL)
                AND articles.rowid IN (
                    SELECT rowid
                    FROM articles
                    LIMIT ? OFFSET ?
                )
                """,
                (batch_size, offset),
            )
            conn.commit()
            pbar.update(batch_size)


def get_entity_occurrences_with_article_id(conn) -> pd.DataFrame:
    """
    Retrieve all entity occurrences and add the corresponding article ID as a column.

    Returns:
    DataFrame: A DataFrame containing entity occurrences and their corresponding article IDs.
    """
    # Retrieve entity occurrences and corresponding article IDs
    query = """
    SELECT eo.id, eo.entity_text, e.entity, eo.sentence_id, s.pmid, a.word_count, a.token_count, a.alpha_count
    FROM entity_occurrences eo
    JOIN sentences s ON eo.sentence_id = s.id
    JOIN articles a ON s.pmid = a.pmid
    JOIN entities e ON eo.entity_id = e.id
    """
    df = pd.read_sql_query(query, conn)

    return df


def get_article_entities(conn, batch=1000) -> pd.DataFrame:
    """
    Retries all entities for a given article
    """


def add_pmid_to_entity_occurrences(conn, batch=100000):
    """
    Add the article ID (pmid) to each entity occurrence in the database.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.
    batch (int): The number of entity occurrences to process in each batch.
    """
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM entity_occurrences")
    total_entity_occurrences = cursor.fetchone()[0]

    # Make sure the column exists
    cursor.execute("PRAGMA table_info(entity_occurrences)")
    columns = cursor.fetchall()
    if not any(column[1] == "pmid" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN pmid INTEGER")

    with tqdm(
        total=total_entity_occurrences, desc="Adding pmid to entity occurrences"
    ) as pbar:
        for offset in range(0, total_entity_occurrences, batch):
            cursor.execute(
                """
                UPDATE entity_occurrences
                SET pmid = (
                    SELECT s.pmid
                    FROM sentences s
                    WHERE s.id = entity_occurrences.sentence_id
                )
                WHERE entity_occurrences.pmid IS NULL
                AND entity_occurrences.rowid IN (
                    SELECT rowid
                    FROM entity_occurrences
                    LIMIT ? OFFSET ?
                )
                """,
                (batch, offset),
            )
            conn.commit()
            pbar.update(batch)


def update_db_with_entity_occurrence_term_fq(conn, batch_size=10000):
    """
    Update the database with the term frequency of each entity occurrence in the corresponding article.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.
    """
    cursor = conn.cursor()

    # Add the term frequency column to the entity occurrences table
    # Make sure the columns tf and tf-idf exist
    cursor.execute("PRAGMA table_info(entity_occurrences)")
    columns = cursor.fetchall()
    if not any(column[1] == "intra_doc_fq" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN intra_doc_fq INTEGER")
    if not any(column[1] == "tf" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN tf REAL")

    # Get all unique pmids
    cursor.execute("SELECT DISTINCT pmid FROM entity_occurrences")
    pmids = [row[0] for row in cursor.fetchall()]

    # Split the list of pmids into batches
    pmid_batches = [pmids[i : i + batch_size] for i in range(0, len(pmids), batch_size)]

    with tqdm(total=len(pmid_batches), desc="Updating term frequency") as pbar:
        for pmid_batch in pmid_batches:
            # Retrieve all entities matching the pmids in the current batch
            cursor.execute(
                """
                SELECT eo.id, eo.entity_text, eo.entity_id, eo.pmid, a.word_count
                FROM entity_occurrences eo
                JOIN articles a ON eo.pmid = a.pmid
                WHERE eo.pmid IN ({})
                """.format(
                    ",".join("?" * len(pmid_batch))
                ),
                pmid_batch,
            )
            entities = cursor.fetchall()

            # Calculate number of times the entity occurs in its document for each entity occurrence
            for entity in entities:
                cursor.execute(
                    """
                    UPDATE entity_occurrences
                    SET intra_doc_fq = (
                        SELECT COUNT(*)
                        FROM entity_occurrences eo
                        WHERE eo.entity_text = entity_occurrences.entity_text
                        AND eo.entity_id = entity_occurrences.entity_id
                        AND eo.pmid = entity_occurrences.pmid
                    ),
                    tf = (
                        SELECT intra_doc_fq * 1.0 / ?
                    )
                    WHERE id = ?
                    """,
                    (entity[4], entity[0]),
                )

            conn.commit()
            pbar.update(1)


def calc_entity_term_fq(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate if the enitity is unique in the document

    Args:
    df (pd.DataFrame): A DataFrame containing entity occurrences and their corresponding article IDs.

    Returns:
    pd.DataFrame: A DataFrame containing entity occurrences, their corresponding article IDs, and the number of times it appears in the document.
    """

    # Calculate the number of times an entity appears in the document, it's matched by both entity_text and entity_id (which NE class it belongs to)
    df["tf"] = df.groupby(["pmid", "entity_text", "entity"])["entity_text"].transform(
        "count"
    )

    return df


def update_tf_idf(conn, batch_size=10000):
    """
    Update the tf-idf values in the entity_occurrences table.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.
    batch_size (int): The number of rows to process in each batch.
    """
    cursor = conn.cursor()

    # Check if the tf_idf column exists
    cursor.execute("PRAGMA table_info(entity_occurrences)")
    columns = cursor.fetchall()
    if not any(column[1] == "inter_doc_fq" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN inter_doc_fq INTEGER")

    if not any(column[1] == "tf_idf" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN tf_idf REAL")
    if not any(column[1] == "idf" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN idf REAL")

    # Get the total number of articles
    cursor.execute("SELECT COUNT(DISTINCT pmid) FROM articles")
    nbr_of_articles = cursor.fetchone()[0]

    if not nbr_of_articles or nbr_of_articles < 1:
        raise ValueError("No articles found in the database")

    print(f"Number of articles: {nbr_of_articles}")

    # Get the total number of rows in entity_occurrences
    cursor.execute("SELECT COUNT(*) FROM entity_occurrences")
    total_rows = cursor.fetchone()[0]

    # Process in batches
    for offset in tqdm(range(0, total_rows, batch_size), desc="Updating TF-IDF"):
        # Read a batch of entity_occurrences
        cursor.execute(
            """
            SELECT eo.id, eo.entity_text, eo.entity_id, eo.pmid, eo.tf, ef.doc_fq
            FROM entity_occurrences eo
            JOIN entity_fq ef ON eo.entity_text = ef.entity_text AND eo.entity_id = ef.entity_id
            LIMIT ? OFFSET ?
        """,
            (batch_size, offset),
        )
        rows = cursor.fetchall()

        # Create a DataFrame from the batch
        df = pd.DataFrame(
            rows, columns=["id", "entity_text", "entity_id", "pmid", "tf", "doc_fq"]
        )

        # df(t) = N(t)
        # where
        # df(t) = Document frequency of a term t
        # N(t) = Number of documents containing the term t

        # Calculate the Inverse Document Frequency (idf)

        df["idf"] = np.log(nbr_of_articles / df["doc_fq"])

        # Example calculation for one row
        print(f"TF: {df['tf'].iloc[0]}")
        print(f"IDF: {df['idf'].iloc[0]}")
        print(f"TF-IDF: {df['tf'].iloc[0] * df['idf'].iloc[0]}")

        # Calculate the TF-IDF score
        df["tf_idf"] = df["tf"] * df["idf"]

        # Update the tf_idf values in the database
        for _, row in df.iterrows():
            cursor.execute(
                """
                UPDATE entity_occurrences
                SET tf_idf = ?, inter_doc_fq = ?, idf = ?
                WHERE id = ?
            """,
                (row["tf_idf"], row["doc_fq"], row["idf"], row["id"]),
            )

        conn.commit()


def get_number_of_articles(conn) -> int:
    """
    Retrieve the number of articles in the database.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.

    Returns:
    int: The number of articles in the database.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM articles")
    num_articles = cursor.fetchone()[0]
    return num_articles


def tf_idf(df: pd.DataFrame, nbr_of_articles: int) -> pd.DataFrame:
    """
    Calculate the TF-IDF score for each entity occurrence.
    Definition: TF-IDF (Term Frequency-Inverse Document Frequency) is a statistical measure that evaluates how relevant a word is to a document in a collection of documents.
    The TF-IDF score is the product of the term frequency and the inverse document frequency.

    tf-idf = tf(t, d) * idf(t, D)

    where:
    tf(t, d) = number of times term t appears in document d
    idf(t, D) = log(Number of documents in the corpus / Number of documents containing term t)

    the dataframe should contain the following columns:
    doc_fq: Number of times the entity appears in the document


    Args:
    df (pd.DataFrame): A DataFrame containing entity occurrences, their corresponding article IDs, and the number of times they appear in the document.

    Returns:
    pd.DataFrame: A DataFrame containing entity occurrences, their corresponding article IDs, the number of times they appear in the document, and the TF-IDF score.
    """

    # Calculate the IDF (Inverse Document Frequency) for each entity
    df["idf"] = df.groupby(["entity_text", "entity"])["pmid"].transform(
        lambda x: np.log(nbr_of_articles / x.nunique())
    )

    # Calculate the TF-IDF score for each entity
    df["tf_idf"] = df["tf"] * df["idf"]

    return df


def count_cooucerence_fq(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number of times an entity co-occurs with another entity in the same document.

    Args:
    df (pd.DataFrame): A DataFrame containing entity occurrences, their corresponding article IDs, and the number of times they appear in the document.

    Returns:
    pd.DataFrame: A DataFrame containing unique entity occurrences, their corresponding article IDs, the frequency of occurrence, and the list of pmids.
    """

    # Group by entity_text and entity_id, and aggregate the frequency and pmids
    result = (
        df.groupby(["entity_text", "entity"])
        .agg(fq=("entity_text", "count"), pmids=("pmid", lambda x: list(x.unique())))
        .reset_index()
    )

    return result


def record_sentence_cooccurences(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number of times two different entities co-occur in the same sentence.

    Args:
    df (pd.DataFrame): A DataFrame containing entity occurrences, their corresponding article IDs, and other related information.

    Returns:
    pd.DataFrame: A DataFrame containing sentence IDs, pmid, entity_1 data, and entity_2 data for each unique co-occurrence.
    """

    # Ensure the 'sentence_id' column exists
    if "sentence_id" not in df.columns:
        raise KeyError("sentence_id")

    # Create a DataFrame with pairs of entities in the same sentence
    cooccurrences = df.merge(df, on="sentence_id", suffixes=("_x", "_y"))
    # Filter out pairs where the entities are the same and ensure pmid is the same
    cooccurrences = cooccurrences[
        (cooccurrences["entity_x"] != cooccurrences["entity_y"])
        & (cooccurrences["pmid_x"] == cooccurrences["pmid_y"])
    ]

    # Select relevant columns and format the output
    result = cooccurrences[
        [
            "sentence_id",
            "pmid_x",
            "id_x",
            "entity_text_x",
            "entity_x",
            "tf_idf_x",
            "id_y",
            "entity_text_y",
            "entity_y",
            "tf_idf_y",
        ]
    ]
    result = result.rename(
        columns={
            "pmid_x": "pmid",
            "id_x": "entity_1_id",
            "entity_text_x": "entity_1_text",
            "entity_x": "entity_1",
            "tf_idf_x": "entity_1_tf_idf",
            "id_y": "entity_2_id",
            "entity_text_y": "entity_2_text",
            "entity_y": "entity_2",
            "tf_idf_y": "entity_2_tf_idf",
        }
    )

    return pd.DataFrame(result)


def record_document_cooccurences(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number of times two different entities co-occur in the same document.

    Args:
    df (pd.DataFrame): A DataFrame containing entity occurrences, their corresponding article IDs, and other related information.

    Returns:
    pd.DataFrame: A DataFrame containing pmid, entity_1 data, and entity_2 data for each unique co-occurrence.
    """

    # Ensure the 'sentence_id' column exists
    if "sentence_id" not in df.columns:
        raise KeyError("sentence_id")
    if "pmid" not in df.columns:
        raise KeyError("pmid")

    # Create a DataFrame with pairs of entities in the same sentence
    cooccurrences = df.merge(df, on="pmid", suffixes=("_x", "_y"))

    # Filter out pairs where the entities are the same and ensure sentence_id is the same
    cooccurrences = cooccurrences[
        (cooccurrences["entity_x"] != cooccurrences["entity_y"])
        & (cooccurrences["sentence_id_x"] == cooccurrences["sentence_id_y"])
    ]

    # Select relevant columns and format the output
    result = cooccurrences[
        [
            "pmid",
            "sentence_id_x",
            "id_x",
            "entity_text_x",
            "entity_x",
            "tf_idf_x",
            "id_y",
            "entity_text_y",
            "entity_y",
            "tf_idf_y",
        ]
    ]
    result = result.rename(
        columns={
            "pmid_x": "pmid",
            "sentence_id_x": "sentence_id",
            "id_x": "entity_1_id",
            "entity_text_x": "entity_1_text",
            "entity_x": "entity_1",
            "tf_idf_x": "entity_1_tf_idf",
            "id_y": "entity_2_id",
            "entity_text_y": "entity_2_text",
            "entity_y": "entity_2",
            "tf_idf_y": "entity_2_tf_idf",
        }
    )

    return pd.DataFrame(result)


def count_document_cooccurence_fq(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sum the total number of times two entities co-occur in the same document.
    - Record the arithmetic frequency of the co-occurrence
    - Record the weighted frequency of the co-occurrence by the tf-idf scores of the two entities. If tf-idf is high it means it's an important occurrence of the entity and thus the connection is more important to the co-occurrence frequency, and vice versa.

    Args:
    df (pd.DataFrame): A DataFrame containing entity co-occurrences, their corresponding article IDs, and other related information.

    Each row is one co-occurrence and should contain the following columns:
    sentence_id, pmid, entity_1_id, entity_1_text, entity_1, entity_1_tf_idf, entity_2_id, entity_2_text, entity_2, entity_2_tf_idf

    Returns:
    pd.DataFrame: A DataFrame containing pmid, entity_1 data, and entity_2 data as well as calculated co-occurrence frequencies.
    """
    # Initialize a new co-occurrence DataFrame
    cooccurrence_df = pd.DataFrame()

    # Calculate the frequency of co-occurrence by the number of rows matching the same entity_1_text with the same entity_2_text
    df["fq"] = df.groupby(["entity_1_text", "entity_2_text"])[
        "entity_1_text"
    ].transform("count")

    # Calculate the weighted frequency of co-occurrence by tf_idf of entity_1 * tf_idf of entity_2 * fq

    weighted_fq_multiplication = "weighted_fq_multiplication"

    df[weighted_fq_multiplication] = (
        df["entity_1_tf_idf"] * df["entity_2_tf_idf"] * df["fq"]
    )

    df["weighted_fq_additative"] = df["entity_1_tf_idf"] + df["entity_2_tf_idf"]

    # Group by pmid, entity_1_text, entity_2_text, and aggregate the frequency and weighted frequency
    cooccurrence_df = (
        df.groupby(["pmid", "entity_1_text", "entity_2_text"])
        .agg(
            fq=("fq", "sum"),
            weighted_fq=(weighted_fq_multiplication, "sum"),
            weighted_fq_additative=("weighted_fq_additative", "sum"),
        )
        .reset_index()
    )

    return cooccurrence_df


def calc_weighted_fqs(conn, batch_size=1000) -> None:
    """
    Calculate the weighted frequency of co-occurrences.

    The chosen method for this is to:
    1. Average the tf-idf scores of the two entities, this means that if it's a strong connection for one of the entities it will be weighted more, and vice versa.
    2. Multiply the average tf-idf score with the co-occurrence frequency.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.
    batch_size (int): The number of rows to process in each batch.

    Returns:
    None
    """
    cursor = conn.cursor()

    # Check if the weighted_fq column exists
    cursor.execute("PRAGMA table_info(entity_cooccurrences)")
    columns = cursor.fetchall()
    if not any(column[1] == "weighted_fq" for column in columns):
        cursor.execute("ALTER TABLE entity_cooccurrences ADD COLUMN weighted_fq REAL")

    # Get the total number of rows in entity_cooccurrences
    cursor.execute("SELECT COUNT(*) FROM entity_cooccurrences")
    total_rows = cursor.fetchone()[0]

    # Process in batches
    for offset in tqdm(
        range(0, total_rows, batch_size), desc="Calculating weighted frequencies"
    ):
        # Read a batch of entity_cooccurrences from coentity_summary table
        cursor.execute(
            """
            SELECT id, e1_text, e2_text, fq, e1_tf_idf, e2_tf_idf
            FROM coentity_summary
            LIMIT ? OFFSET ?
            """,
            (batch_size, offset),
        )
        rows = cursor.fetchall()

        # Create a DataFrame from the batch
        df = pd.DataFrame(
            rows, columns=["id", "e1_text", "e2_text", "fq", "e1_tf_idf", "e2_tf_idf"]
        )
        print(df)

        # Convert e1_tf_idf and e2_tf_idf from list format to numeric values
        df["e1_tf_idf"] = df["e1_tf_idf"].apply(lambda x: np.mean(eval(x)))
        df["e2_tf_idf"] = df["e2_tf_idf"].apply(lambda x: np.mean(eval(x)))

        # Calculate the average tf-idf score
        df["avg_tf_idf"] = (df["e1_tf_idf"] + df["e2_tf_idf"]) / 2
        print(df["avg_tf_idf"])
        print(df["fq"])
        # Calculate the weighted frequency
        df["weighted_fq"] = df["avg_tf_idf"] * df["fq"]
        print(f"Weighted FQ: {df['weighted_fq']}")
        # Update the weighted_fq values in the database
        for _, row in df.iterrows():
            cursor.execute(
                """
                UPDATE entity_cooccurrences
                SET weighted_fq = ?
                WHERE id = ?
                """,
                (row["weighted_fq"], row["id"]),
            )

        conn.commit()


def get_all_entity_occurrences(conn, batch_size=200000) -> pd.DataFrame:
    """
    Get all entity occurrences from the entity_occurrences table.

    Returns:
        A dataframe containing all entity occurrences.
    """


    cursor = conn.cursor()

    # Enshure necessary columns exist
    cursor.execute("PRAGMA table_info(entity_occurrences)")
    columns = cursor.fetchall()
    if not any(column[1] == "intra_doc_fq" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN intra_doc_fq INTEGER")
    if not any(column[1] == "tf" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN tf REAL")
    if not any(column[1] == "inter_doc_fq" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN inter_doc_fq INTEGER")
    if not any(column[1] == "tf_idf" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN tf_idf REAL")
    if not any(column[1] == "idf" for column in columns):
        cursor.execute("ALTER TABLE entity_occurrences ADD COLUMN idf REAL")

    offset = 0
    entity_occurrences_df = pd.DataFrame(
        columns=[
            "id",
            "sentence_id",
            "sentence_index",
            "entity_text",
            "entity_id",
            "intra_doc_fq",
            "tf_idf",
        ]
    )

    # Get the total number of rows in entity_occurrences
    cursor.execute("SELECT COUNT(*) FROM entity_occurrences")
    total_rows = cursor.fetchone()[0]

    with tqdm(total=total_rows, desc="Fetching entity occurrences") as pbar:
        while True:
            cursor.execute(
                """
                    SELECT eo.id, eo.sentence_id, eo.sentence_index, eo.entity_text, eo.entity_id, eo.intra_doc_fq, eo.tf_idf
                    FROM entity_occurrences eo
                    LIMIT ? OFFSET ?
                    """,
                (batch_size, offset),
            )
            entity_occurrences = cursor.fetchall()
            if not entity_occurrences:
                break

            df = pd.DataFrame(
                entity_occurrences,
                columns=[
                    "id",
                    "sentence_id",
                    "sentence_index",
                    "entity_text",
                    "entity_id",
                    "doc_fq",
                    "tf_idf",
                ],
            )
            entity_occurrences_df = pd.concat([entity_occurrences_df, df], ignore_index=True)
            offset += batch_size
            pbar.update(len(entity_occurrences))
            # Dump to CSV for inspection

    return entity_occurrences_df


