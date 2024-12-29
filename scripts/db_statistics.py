import pandas as pd
import sqlite3
import logging
from tqdm import tqdm

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

def calc_all_article_lengths(conn):
    """
    Calculate the word count, token count, and alpha count for all articles and sets the values in the articles table.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT pmid, SUM(word_count), SUM(token_count), SUM(alpha_count)
        FROM sentences
        GROUP BY pmid
        """
    )
    counts = cursor.fetchall()

    if not counts:
        logging.warning("No sentences found in the database")
        return

    for c in counts:
        print(c)


    with tqdm(total=len(counts), desc="Updating article lengths") as pbar:
        for pmid, word_count, token_count, alpha_count in counts:
            cursor.execute(
                """
                UPDATE articles
                SET word_count = ?, token_count = ?, alpha_count = ?
                WHERE pmid = ?
                """,
                (word_count, token_count, alpha_count, pmid),
            )
            pbar.update(1)


def calc_article_counts(conn):
    """
    Calculate word count, token count, and alphabetic character count for each article.

    Args:
    conn (sqlite3.Connection): A SQLite database connection.
    """
    cursor = conn.cursor()

    # Calculate word count, token count, and alphabetic character count for each article
    cursor.execute("""
    UPDATE articles
    SET word_count = (
        SELECT SUM(word_count)
        FROM sentences
        WHERE sentences.pmid = articles.pmid
    )
    """)
    cursor.execute("""
    UPDATE articles
    SET token_count = (
        SELECT SUM(token_count)
        FROM sentences
        WHERE sentences.pmid = articles.pmid
    )
    """)
    cursor.execute("""
    UPDATE articles
    SET alpha_count = (
        SELECT SUM(alpha_count)
        FROM sentences
        WHERE sentences.pmid = articles.pmid
    )
    """)

    conn.commit()



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

def get_entity_occurrences_with_article_id(conn) -> pd.DataFrame:
    """
    Retrieve all entity occurrences and add the corresponding article ID as a column.

    Returns:
    DataFrame: A DataFrame containing entity occurrences and their corresponding article IDs.
    """
    # Update article counts before retrieving entity occurrences
    calc_article_counts(conn)
    update_null_columns(conn)

    # Retrieve entity occurrences and corresponding article IDs
    query = """
    SELECT eo.id, eo.entity_text, eo.entity_id, eo.sentence_id, s.pmid, a.word_count, a.token_count, a.alpha_count
    FROM entity_occurrences eo
    JOIN sentences s ON eo.sentence_id = s.id
    JOIN articles a ON s.pmid = a.pmid
    """
    df = pd.read_sql_query(query, conn)

    return df

def calc_unique_doc_fq(df:pd.DataFrame) -> pd.DataFrame:
    """
    Calculate if the enitity is unique in the document

    Args:
    df (pd.DataFrame): A DataFrame containing entity occurrences and their corresponding article IDs.

    Returns:
    pd.DataFrame: A DataFrame containing entity occurrences, their corresponding article IDs, and the number of times it appears in the document.
    """

    # Calculate the number of times an entity appears in the document, it's matched by both entity_text and entity_id (which NE class it belongs to)
    df["doc_fq"] = df.groupby(["pmid", "entity_text", "entity_id"])["entity_text"].transform("count")



    return df