import duckdb
import pandas as pd
import warnings
from pathlib import Path
from typing import Optional, Dict, List, Any, Union

# Path to SQL files
SQL_DIR = Path(__file__).parent


def read_sql_file(file_path: Union[str, Path]) -> str:
    """Read SQL file content as a string"""
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


def initialize_db(
    database_path: Optional[str] = ":memory:",
    threads: int = 4,
    memory_limit: str = "1GB",
) -> duckdb.DuckDBPyConnection:
    """Initialize and configure a DuckDB connection"""
    con = duckdb.connect(database=database_path)
    con.execute(f"PRAGMA threads={threads}")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    return con


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create all database tables using SQL files"""
    # Execute SQL statements from files
    articles_table_sql = read_sql_file(SQL_DIR / "articles_table.sql")
    sentences_table_sql = read_sql_file(SQL_DIR / "sentences_table.sql")
    entity_sequence_sql = read_sql_file(SQL_DIR / "entity_sequence.sql")
    entities_table_sql = read_sql_file(SQL_DIR / "entities_table.sql")

    # Create tables using SQL from files
    con.execute(articles_table_sql)
    con.execute(sentences_table_sql)
    con.execute(entity_sequence_sql)
    con.execute(entities_table_sql)


def create_indices(con: duckdb.DuckDBPyConnection) -> None:
    """Create database indices for performance optimization"""
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_article_id ON articles(article_id)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sentence_article_id ON sentences(article_id)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_entity_article_sentence ON entities(article_id, sentence_id)"
    )


def insert_data(
    con: duckdb.DuckDBPyConnection,
    articles_data: Union[List[Dict[str, Any]], pd.DataFrame],
    sentences_data: Union[List[Dict[str, Any]], pd.DataFrame],
    entities_data: Union[List[Dict[str, Any]], pd.DataFrame],
) -> None:
    """Insert data into database tables using DataFrames

    Args:
        con: DuckDB connection
        articles_data: Articles data as DataFrame or list of dicts
        sentences_data: Sentences data as DataFrame or list of dicts
        entities_data: Entities data as DataFrame or list of dicts
    """
    # Convert to DataFrames if needed
    if not isinstance(articles_data, pd.DataFrame):
        articles_df = pd.DataFrame(articles_data)
    else:
        articles_df = articles_data

    if not isinstance(sentences_data, pd.DataFrame):
        sentences_df = pd.DataFrame(sentences_data)
    else:
        sentences_df = sentences_data

    if not isinstance(entities_data, pd.DataFrame):
        entities_df = pd.DataFrame(entities_data)
    else:
        entities_df = entities_data

    # Register DataFrames as views
    con.register("articles_df", articles_df)
    con.register("sentences_df", sentences_df)
    con.register("entities_df", entities_df)

    # Insert data from the registered views
    con.execute("INSERT INTO articles SELECT * FROM articles_df")
    con.execute("INSERT INTO sentences SELECT * FROM sentences_df")
    con.execute(
        """
        INSERT INTO entities (article_id, sentence_id, entity, start_pos, end_pos, inference_model, inference_model_metadata)
        SELECT article_id, sentence_id, entity, start_pos, end_pos, inference_model, inference_model_metadata FROM entities_df
        """
    )


def get_table(
    con: duckdb.DuckDBPyConnection, table_name: str, as_df: bool = True
) -> Union[pd.DataFrame, List[tuple]]:
    """Fetch data from a table, either as DataFrame or list of tuples"""
    result = con.execute(f"SELECT * FROM {table_name}")
    return result.fetchdf() if as_df else result.fetchall()


def get_table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    """Get the count of rows in a table"""
    result = con.execute(f"SELECT COUNT(*) FROM {table_name}")
    return result.fetchone()[0]


def export_to_csv(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: Union[str, Path],
) -> None:
    """Export a table to CSV file"""
    con.execute(
        f"COPY (SELECT * FROM {table_name}) TO '{output_path}' (HEADER, DELIMITER ',');"
    )


def get_entities_by_article(
    con: duckdb.DuckDBPyConnection, article_id: int
) -> pd.DataFrame:
    """Get all entities for a specific article"""
    query = """
    SELECT e.*
    FROM entities e
    WHERE e.article_id = ?
    """
    return con.execute(query, [article_id]).fetchdf()


def get_article_entity_stats(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Get statistics about entities per article"""
    query = """
    SELECT a.article_id, a.title, COUNT(DISTINCT s.sentence_id) AS sentence_count, COUNT(e.entity) AS entity_count
    FROM articles a
    LEFT JOIN sentences s ON a.article_id = s.article_id
    LEFT JOIN entities e ON s.article_id = e.article_id AND s.sentence_id = e.sentence_id
    GROUP BY a.article_id, a.title
    ORDER BY entity_count DESC
    """
    return con.execute(query).fetchdf()


def get_articles_df(connection) -> pd.DataFrame:
    """
    Get all articles from the database as a DataFrame.

    Args:
        connection: DuckDB connection object

    Returns:
        DataFrame containing article data
    """
    result = connection.execute("SELECT article_id, title FROM articles")
    return result.fetchdf()


def get_articles_as_dict_list(connection) -> List[Dict[str, Any]]:
    """
    Get all articles from the database as a list of dictionaries.

    Args:
        connection: DuckDB connection object

    Returns:
        List of dictionaries containing article data
    """
    warnings.warn(
        "The get_articles_as_dict_list method is not covered by tests and may have unexpected behavior.",
        UserWarning,
        stacklevel=2,
    )
    rows = connection.execute(
        "SELECT article_id, title FROM articles"
    ).fetchall()
    return [{"article_id": row[0], "title": row[1]} for row in rows]


def get_articles(
    connection, as_df: bool = True
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Get all articles from the database.
    This is a wrapper method for backward compatibility.

    Args:
        connection: DuckDB connection object
        as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

    Returns:
        DataFrame or list of article dictionaries
    """
    if as_df:
        return get_articles_df(connection)
    else:
        return get_articles_as_dict_list(connection)


def get_sentences_df(connection) -> pd.DataFrame:
    """
    Get all sentences from the database as a DataFrame.

    Args:
        connection: DuckDB connection object

    Returns:
        DataFrame containing sentence data
    """
    result = connection.execute(
        "SELECT article_id, sentence_id, text FROM sentences"
    )
    return result.fetchdf()


def get_sentences_as_dict_list(connection) -> List[Dict[str, Any]]:
    """
    Get all sentences from the database as a list of dictionaries.

    Args:
        connection: DuckDB connection object

    Returns:
        List of dictionaries containing sentence data
    """
    warnings.warn(
        "The get_sentences_as_dict_list method is not covered by tests and may have unexpected behavior.",
        UserWarning,
        stacklevel=2,
    )
    rows = connection.execute(
        "SELECT article_id, sentence_id, text FROM sentences"
    ).fetchall()
    return [
        {"article_id": row[0], "sentence_id": row[1], "text": row[2]}
        for row in rows
    ]


def get_sentences(
    connection, as_df: bool = True
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Get all sentences from the database.
    This is a wrapper method for backward compatibility.

    Args:
        connection: DuckDB connection object
        as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

    Returns:
        DataFrame or list of sentence dictionaries
    """
    if as_df:
        return get_sentences_df(connection)
    else:
        return get_sentences_as_dict_list(connection)


def get_entities_df(connection) -> pd.DataFrame:
    """
    Get all entities from the database as a DataFrame.

    Args:
        connection: DuckDB connection object

    Returns:
        DataFrame containing entity data
    """
    query = """
        SELECT article_id, sentence_id, entity, start_pos, end_pos,
               inference_model, inference_model_metadata
        FROM entities
    """
    result = connection.execute(query)
    return result.fetchdf()


def get_entities_as_dict_list(connection) -> List[Dict[str, Any]]:
    """
    Get all entities from the database as a list of dictionaries.

    Args:
        connection: DuckDB connection object

    Returns:
        List of dictionaries containing entity data
    """
    warnings.warn(
        "The get_entities_as_dict_list method is not covered by tests and may have unexpected behavior.",
        UserWarning,
        stacklevel=2,
    )
    query = """
        SELECT article_id, sentence_id, entity, start_pos, end_pos,
               inference_model, inference_model_metadata
        FROM entities
    """
    rows = connection.execute(query).fetchall()
    return [
        {
            "article_id": row[0],
            "sentence_id": row[1],
            "entity": row[2],
            "start_pos": row[3],
            "end_pos": row[4],
            "inference_model": row[5],
            "inference_model_metadata": row[6],
        }
        for row in rows
    ]


def get_entities(
    connection, as_df: bool = True
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Get all entities from the database.
    This is a wrapper method for backward compatibility.

    Args:
        connection: DuckDB connection object
        as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

    Returns:
        DataFrame or list of entity dictionaries
    """
    if as_df:
        return get_entities_df(connection)
    else:
        return get_entities_as_dict_list(connection)
