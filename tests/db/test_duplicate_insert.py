# Test if a error is raised if duplicate keys are attempted to be inserted
# Create articles table

from easyner.io.database import (
    DatabaseConnection,
    ArticleRepository,
    DuckDBHandler,
)
import pytest
import duckdb
import pandas as pd


@pytest.fixture(scope="class")
def db_connection() -> DatabaseConnection:
    """Fixture to set up and tear down a database connection for a test class."""
    ddb_handler = DuckDBHandler(":memory:")
    connection = ddb_handler.connection
    yield connection  # Provide the connection to the test
    if connection:
        connection.close()  # Teardown: close connection


@pytest.fixture(scope="class")
def article_repo(db_connection: DatabaseConnection) -> ArticleRepository:
    """Fixture to create an ArticleRepository instance and its table."""
    repo = ArticleRepository(db_connection)
    repo._create_table()  # Ensure table is created
    return repo


def test_insert_duplicate_key(article_repo: ArticleRepository):
    """Test that inserting a duplicate key raises a ConstraintException."""
    # Create a sample DataFrame with duplicate keys
    data = {
        "article_id": [1, 1],
        "title": ["Title 1", "Title 1"],
    }
    df = pd.DataFrame(data)

    # Attempt to insert the DataFrame into the database using the injected repository
    with pytest.raises(duckdb.ConstraintException) as excinfo:
        article_repo.insert_many_transactional(df)  # Use the injected fixture

    assert (
        "duplicate key"
        in str(excinfo.value).lower()
        # or "primary key constraint failed" in str(excinfo.value).lower()
    )
