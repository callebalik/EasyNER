import pandas as pd
import numpy as np
import pytest

from easyner.io.converters.json_to_duck_converter import JsonToDuckConverter
from easyner.io.database.duckdb_handler import DuckDBHandler
from easyner.io.database.utils.column_names import (
    ARTICLE_ID,
    SENTENCE_ID,
    TEXT,
    START_CHAR,
    END_CHAR,
    INFERENCE_MODEL,
    INFERENCE_MODEL_METADATA,
    TITLE,
    ENTITY_ID,
)
import json


def test_list_convertible_files(temp_dir, test_json_file):
    """Test listing convertible files"""
    # Create converter
    converter = JsonToDuckConverter(temp_dir, temp_dir / "output")

    # Test listing files
    files = converter.list_convertible_files()
    assert len(files) == 1
    assert files[0].name == "test_articles.json"


def test_convert_to_db(temp_dir, test_json_file):
    """Test the conversion process to a database file"""
    # Create output directory
    output_dir = temp_dir / "output"
    output_dir.mkdir(exist_ok=True)

    # Create converter with a persistent database file
    db_path = output_dir / "test.db"
    converter = JsonToDuckConverter(temp_dir, output_dir, db_file=str(db_path))

    # Run conversion
    result = converter.convert()

    # Check results
    assert result["files_processed_this_run"] == 1
    assert result["article_count"] == 2
    assert result["sentence_count"] == 3
    assert result["entity_count"] == 5

    # Check that the database file was created
    assert db_path.exists()

    # Check database content directly using DataFrame approach
    connection = converter.connection

    # Retrieve data from database as DataFrames (now default)
    articles_data = converter.db_handler.get_articles_df()
    sentences_data = converter.db_handler.get_sentences_df()
    entities_data = converter.db_handler.get_entities_df()

    # Verify database content
    assert len(articles_data) == 2
    assert len(sentences_data) == 3
    assert len(entities_data) == 5

    # Check list of converted files
    converted = converter.list_converted_files()
    assert len(converted) == 1
    assert converted[0] == test_json_file


def test_convert_with_memory_db(temp_dir, test_json_file):
    """Test conversion with in-memory database"""
    # Create converter
    converter = JsonToDuckConverter(temp_dir, temp_dir / "output")

    # Run conversion with in-memory database
    result = converter.convert(use_memory_db=True)

    # Check results
    assert result["files_processed_this_run"] == 1
    assert result["article_count"] == 2
    assert result["database_path"] == ":memory:"


def test_idempotency_no_reprocessing(temp_dir, test_json_file):
    """Test that running convert multiple times without reprocess flag doesn't reprocess files."""
    output_dir = temp_dir / "output_idempotency_no_reprocess"
    output_dir.mkdir(exist_ok=True)
    db_path = output_dir / "idem_no_reprocess.db"

    converter = JsonToDuckConverter(temp_dir, output_dir, db_file=str(db_path))

    # First run
    result1 = converter.convert()
    assert result1["files_processed_this_run"] == 1
    assert result1["article_count"] == 2  # From test_json_file
    assert result1["sentence_count"] == 3
    assert result1["entity_count"] == 5

    # Check conversion log
    log_df1 = converter.db_handler.get_conversion_log_df()
    assert len(log_df1[log_df1["status"] == "converted"]) == 1

    # Second run (should not reprocess)
    result2 = converter.convert()  # _reprocess is False by default
    assert result2["files_processed_this_run"] == 0
    assert result2["articles_added_this_run"] == 0
    assert result2["sentences_added_this_run"] == 0
    assert result2["entities_added_this_run"] == 0
    # Total counts in DB should remain the same
    assert result2["article_count"] == 2
    assert result2["sentence_count"] == 3
    assert result2["entity_count"] == 5

    # Conversion log should still have 1 converted entry
    log_df2 = converter.db_handler.get_conversion_log_df()
    assert len(log_df2[log_df2["status"] == "converted"]) == 1
    # Ensure timestamps or other details weren't unnecessarily updated for the already converted file
    # This might require comparing log_df1 and log_df2 for the specific file entry if precise.
    # For simplicity, we check the count of 'converted' status.


def test_idempotency_with_reprocess_flag(temp_dir, test_json_file):
    """Test that the reprocess flag forces reconversion."""
    output_dir = temp_dir / "output_idempotency_reprocess"
    output_dir.mkdir(exist_ok=True)
    db_path = output_dir / "idem_reprocess.db"

    converter = JsonToDuckConverter(temp_dir, output_dir, db_file=str(db_path))

    # First run
    result1 = converter.convert()
    assert result1["files_processed_this_run"] == 1
    assert result1["article_count"] == 2

    # Second run with reprocess=True
    # Note: The converter instance needs to be re-initialized or its _reprocess flag set
    # For this test, let's re-initialize to ensure clean state for the _reprocess attribute handling
    converter_reprocess = JsonToDuckConverter(
        temp_dir, output_dir, db_file=str(db_path), reprocess=True
    )
    result2 = converter_reprocess.convert()

    assert result2["files_processed_this_run"] == 1  # File is processed again
    # Counts of items added in this run should reflect the file's content
    assert result2["articles_added_this_run"] == 2
    assert result2["sentences_added_this_run"] == 3
    assert result2["entities_added_this_run"] == 5

    # Total counts in DB should remain the same (data is overwritten/re-inserted, not duplicated)
    assert result2["article_count"] == 2
    assert result2["sentence_count"] == 3
    assert result2["entity_count"] == 5

    # Conversion log should still reflect 1 file as converted, possibly with updated timestamp
    log_df2 = converter_reprocess.db_handler.get_conversion_log_df()
    assert len(log_df2[log_df2["status"] == "converted"]) == 1


def test_idempotency_adding_new_files(temp_dir, test_data):
    """Test processing new files after an initial run."""
    output_dir = temp_dir / "output_idempotency_new_files"
    output_dir.mkdir(exist_ok=True)
    db_path = output_dir / "idem_new_files.db"

    # Create initial file
    file1_path = temp_dir / "articles1.json"
    with open(file1_path, "w") as f:
        json.dump({"1": test_data["1"]}, f)  # Only first article

    converter = JsonToDuckConverter(temp_dir, output_dir, db_file=str(db_path))

    # First run (only file1.json)
    result1 = converter.convert()
    assert result1["files_processed_this_run"] == 1
    assert result1["article_count"] == 1
    # Expected sentences and entities from test_data["1"]
    # test_data["1"] has 2 sentences, 2+1=3 entities
    assert result1["sentence_count"] == 2
    assert result1["entity_count"] == 3

    # Add a new file
    file2_path = temp_dir / "articles2.json"
    with open(file2_path, "w") as f:
        json.dump({"2": test_data["2"]}, f)  # Only second article

    # Second run (should process only file2.json)
    # Create a new converter instance or ensure the existing one re-scans files correctly
    # For BaseConverter, list_unconverted_files re-scans each time.
    result2 = converter.convert()
    assert result2["files_processed_this_run"] == 1  # Only the new file
    # Articles added should be from the new file
    assert result2["articles_added_this_run"] == 1
    # Sentences and entities from test_data["2"]
    # test_data["2"] has 1 sentence, 2 entities
    assert result2["sentences_added_this_run"] == 1
    assert result2["entities_added_this_run"] == 2

    # Total counts in DB should be cumulative
    assert result2["article_count"] == 2
    assert result2["sentence_count"] == 2 + 1  # Sentences from file1 + file2
    assert result2["entity_count"] == 3 + 2  # Entities from file1 + file2

    log_df = converter.db_handler.get_conversion_log_df()
    assert len(log_df[log_df["status"] == "converted"]) == 2


class TestExampleDataStructureAndContent:
    """Test class for validating database content against CSV files.
    This class breaks down testing into separate methods for articles, sentences, and entities.
    """

    @pytest.fixture
    def test_db_setup(self, example_files_dir, tmp_path):
        """Setup a test database that can be shared across test methods."""
        # Create output directory
        output_dir = tmp_path / "example_output"
        output_dir.mkdir(exist_ok=True)

        # Create persistent database file for testing
        db_path = output_dir / "content_test.db"

        # Create converter with a persistent database file
        converter = JsonToDuckConverter(
            example_files_dir, output_dir, db_file=str(db_path)
        )
        result = converter.convert()

        # Store test data in the fixture
        test_data = {
            "converter": converter,
            "connection": converter.connection,
            "db_handler": converter.db_handler,
            "db_path": db_path,
            "result": result,
        }

        yield test_data

        # Close the connection when done
        converter.connection.close()

    @pytest.fixture
    def db_articles(self, test_db_setup):
        """Get articles data from the test database."""
        articles_data = test_db_setup["db_handler"].get_articles_df()

        # Sort by article_id for consistent ordering
        return articles_data.sort_values(ARTICLE_ID).reset_index(drop=True)

    @pytest.fixture
    def db_sentences(self, test_db_setup):
        """Get sentences data from the test database."""
        sentences_data = test_db_setup["db_handler"].get_sentences_df()

        # Sort by article_id and sentence_id for consistent ordering
        return sentences_data.sort_values(
            [ARTICLE_ID, SENTENCE_ID]
        ).reset_index(drop=True)

    @pytest.fixture
    def db_entities(self, test_db_setup):
        """Get entities data from the test database."""
        entities_data = test_db_setup["db_handler"].get_entities_df()

        # Get only the columns we want to compare
        entity_cols = [
            ARTICLE_ID,
            SENTENCE_ID,
            TEXT,
            START_CHAR,
            END_CHAR,
        ]

        # If inference columns exist, include them
        for col in [INFERENCE_MODEL, INFERENCE_MODEL_METADATA]:
            if col in entities_data.columns:
                entity_cols.append(col)

        # Convert numeric columns to ensure consistent types
        if START_CHAR in entities_data.columns:
            entities_data[START_CHAR] = pd.to_numeric(
                entities_data[START_CHAR]
            )
        if END_CHAR in entities_data.columns:
            entities_data[END_CHAR] = pd.to_numeric(entities_data[END_CHAR])

        # Sort and extract only the columns we want to compare
        return (
            entities_data[entity_cols]
            .sort_values([ARTICLE_ID, SENTENCE_ID, TEXT, START_CHAR])
            .reset_index(drop=True)
        )

    @pytest.fixture
    def exp_articles(self, expected_articles_df):
        """Prepare expected articles data for comparison."""
        df = expected_articles_df.copy()
        return df.sort_values(ARTICLE_ID).reset_index(drop=True)

    @pytest.fixture
    def exp_sentences(self, expected_sentences_df):
        """Prepare expected sentences data for comparison."""
        df = expected_sentences_df.copy()
        return df.sort_values([ARTICLE_ID, SENTENCE_ID]).reset_index(drop=True)

    @pytest.fixture
    def exp_entities(self, expected_entities_df):
        """Prepare expected entities data for comparison."""
        df = expected_entities_df.copy()

        # Get only the relevant columns
        entity_cols = [
            ARTICLE_ID,
            SENTENCE_ID,
            TEXT,
            START_CHAR,
            END_CHAR,
        ]

        # If inference columns exist, include them
        for col in [INFERENCE_MODEL, INFERENCE_MODEL_METADATA]:
            if col in df.columns:
                entity_cols.append(col)

        # Convert numeric columns
        if START_CHAR in df.columns:
            df[START_CHAR] = pd.to_numeric(df[START_CHAR])
        if END_CHAR in df.columns:
            df[END_CHAR] = pd.to_numeric(df[END_CHAR])

        return (
            df[[col for col in entity_cols if col in df.columns]]
            .sort_values([ARTICLE_ID, SENTENCE_ID, TEXT, START_CHAR])
            .reset_index(drop=True)
        )

    def test_database_setup(self, test_db_setup):
        """Test that the database was set up correctly."""
        assert test_db_setup["db_path"].exists()
        assert test_db_setup["result"]["files_processed_this_run"] == 1
        assert test_db_setup["result"]["article_count"] > 0
        assert test_db_setup["result"]["sentence_count"] > 0
        assert test_db_setup["result"]["entity_count"] > 0

    def test_articles(self, db_articles, exp_articles):
        """Test that articles in the database match the expected articles from CSV."""
        # Check that we have the same number of articles
        assert len(db_articles) == len(exp_articles)

        # For articles, verify titles match for each article_id
        for article_id in exp_articles[ARTICLE_ID]:
            expected_title = exp_articles[
                exp_articles[ARTICLE_ID] == article_id
            ][TITLE].iloc[0]
            actual_title = db_articles[db_articles[ARTICLE_ID] == article_id][
                TITLE
            ].iloc[0]
            assert (
                expected_title == actual_title
            ), f"Title mismatch for article {article_id}"

    def test_sentences(self, db_sentences, exp_sentences):
        """Test that sentences in the database match the expected sentences from CSV."""
        # Check that we have the same number of sentences
        assert len(db_sentences) == len(exp_sentences)

        # For sentences, verify text matches for each article_id and sentence_id pair
        for _, expected_row in exp_sentences.iterrows():
            article_id = expected_row[ARTICLE_ID]
            sentence_id = expected_row[SENTENCE_ID]
            expected_text = expected_row[TEXT]

            actual_sentences = db_sentences[
                (db_sentences[ARTICLE_ID] == article_id)
                & (db_sentences[SENTENCE_ID] == sentence_id)
            ]

            assert (
                len(actual_sentences) == 1
            ), f"Sentence not found: {article_id}/{sentence_id}"
            assert actual_sentences.iloc[0][TEXT] == expected_text

    def test_entities(self, db_entities, exp_entities):
        """Test that entities in the database match the expected entities from CSV."""
        # Check that we have the same number of entities
        assert len(db_entities) == len(exp_entities)

        # For entities, verify each entity is present with correct attributes
        for _, expected_row in exp_entities.iterrows():
            article_id = expected_row[ARTICLE_ID]
            sentence_id = expected_row[SENTENCE_ID]
            entity_text = expected_row[TEXT]
            start_char = expected_row[START_CHAR]
            end_char = expected_row[END_CHAR]

            matching = db_entities[
                (db_entities[ARTICLE_ID] == article_id)
                & (db_entities[SENTENCE_ID] == sentence_id)
                & (db_entities[TEXT] == entity_text)
                & (db_entities[START_CHAR] == start_char)
                & (db_entities[END_CHAR] == end_char)
            ]

            # Assert that exactly one match was found
            assert (
                len(matching) == 1
            ), f"Entity not found or duplicated: {expected_row.to_dict()}"

    def test_specific_entities(self, db_entities, exp_entities):
        """Test for specific entities of interest."""
        # Test for specific entities of interest
        entity_names = ["asthma", "eczema"]

        for entity_name in entity_names:
            db_count = len(db_entities[db_entities[TEXT] == entity_name])
            expected_count = len(
                exp_entities[exp_entities[TEXT] == entity_name]
            )
            assert (
                db_count == expected_count
            ), f"Count mismatch for '{entity_name}': got {db_count}, expected {expected_count}"

    def test_direct_article_df_comparison(
        self, test_db_setup, expected_articles_df
    ):
        """Test direct comparison of articles DataFrame from DB and CSV without type conversion."""
        # Get articles from database
        db_articles = test_db_setup["db_handler"].get_articles_df()

        # Print the types to see what they actually are
        print("\nArticle ID types:")
        print(f"Database article_id type: {db_articles[ARTICLE_ID].dtype}")
        print(f"CSV article_id type: {expected_articles_df[ARTICLE_ID].dtype}")

        # Sort both DataFrames by article_id for consistent comparison
        db_articles_sorted = db_articles.sort_values(ARTICLE_ID).reset_index(
            drop=True
        )
        expected_articles_sorted = expected_articles_df.sort_values(
            ARTICLE_ID
        ).reset_index(drop=True)

        # Direct pandas frame_equal comparison with check_dtype=False
        pd.testing.assert_frame_equal(
            db_articles_sorted[[ARTICLE_ID, TITLE]],
            expected_articles_sorted[[ARTICLE_ID, TITLE]],
            check_dtype=False,
        )
        print("Articles DataFrame comparison passed!")

    def test_direct_sentences_df_comparison(
        self, test_db_setup, expected_sentences_df
    ):
        """Test direct comparison of sentences DataFrame from DB and CSV without type conversion."""
        # Get sentences from database
        db_sentences = test_db_setup["db_handler"].get_sentences_df()

        # Print the types to see what they actually are
        print("\nSentence ID types:")
        print(f"Database article_id type: {db_sentences[ARTICLE_ID].dtype}")
        print(
            f"CSV article_id type: {expected_sentences_df[ARTICLE_ID].dtype}"
        )
        print(f"Database sentence_id type: {db_sentences[SENTENCE_ID].dtype}")
        print(
            f"CSV sentence_id type: {expected_sentences_df[SENTENCE_ID].dtype}"
        )

        # Sort both DataFrames by article_id and sentence_id for consistent comparison
        db_sentences_sorted = db_sentences.sort_values(
            [ARTICLE_ID, SENTENCE_ID]
        ).reset_index(drop=True)
        expected_sentences_sorted = expected_sentences_df.sort_values(
            [ARTICLE_ID, SENTENCE_ID]
        ).reset_index(drop=True)

        # Direct pandas frame_equal comparison with check_dtype=False
        pd.testing.assert_frame_equal(
            db_sentences_sorted[[ARTICLE_ID, SENTENCE_ID, TEXT]],
            expected_sentences_sorted[[ARTICLE_ID, SENTENCE_ID, TEXT]],
            check_dtype=False,
        )
        print("Sentences DataFrame comparison passed!")

    def test_direct_entities_df_comparison(
        self, test_db_setup, expected_entities_df
    ):
        """Test direct comparison of entities DataFrame from DB and CSV without type conversion."""
        # Get entities from database
        db_entities = test_db_setup["db_handler"].get_entities_df()

        # Print the types to see what they actually are
        print("\nEntity types:")
        print(f"Database article_id type: {db_entities[ARTICLE_ID].dtype}")
        print(f"CSV article_id type: {expected_entities_df[ARTICLE_ID].dtype}")
        print(f"Database sentence_id type: {db_entities[SENTENCE_ID].dtype}")
        print(
            f"CSV sentence_id type: {expected_entities_df[SENTENCE_ID].dtype}"
        )

        # Get relevant columns for entities comparison
        entity_cols = [
            ARTICLE_ID,
            SENTENCE_ID,
            TEXT,
            START_CHAR,
            END_CHAR,
        ]

        # Add inference columns if they exist in both DataFrames
        for col in [INFERENCE_MODEL, INFERENCE_MODEL_METADATA]:
            if (
                col in db_entities.columns
                and col in expected_entities_df.columns
            ):
                entity_cols.append(col)

        # Handle numeric columns to ensure consistent types
        db_entities_copy = db_entities.copy()
        expected_entities_copy = expected_entities_df.copy()

        # Convert numeric columns to ensure they're the same type
        for df in [db_entities_copy, expected_entities_copy]:
            if START_CHAR in df.columns:
                df[START_CHAR] = pd.to_numeric(df[START_CHAR])
            if END_CHAR in df.columns:
                df[END_CHAR] = pd.to_numeric(df[END_CHAR])

        # Drop entity_id from expected DataFrame if present (it's not in DB result)
        if ENTITY_ID in expected_entities_copy.columns:
            expected_entities_copy = expected_entities_copy.drop(
                columns=[ENTITY_ID]
            )

        # Sort both DataFrames for consistent comparison
        db_entities_sorted = db_entities_copy.sort_values(
            [ARTICLE_ID, SENTENCE_ID, TEXT, START_CHAR]
        ).reset_index(drop=True)

        expected_entities_sorted = expected_entities_copy.sort_values(
            [ARTICLE_ID, SENTENCE_ID, TEXT, START_CHAR]
        ).reset_index(drop=True)

        # Extract only columns present in both DataFrames
        common_cols = [
            col
            for col in entity_cols
            if col in db_entities_sorted.columns
            and col in expected_entities_sorted.columns
        ]

        # Handle null values to avoid warnings
        for col in common_cols:
            # For string columns, use empty string as fill value
            if (
                db_entities_sorted[col].dtype == object
                or expected_entities_sorted[col].dtype == object
            ):
                db_entities_sorted[col] = db_entities_sorted[col].fillna("")
                expected_entities_sorted[col] = expected_entities_sorted[
                    col
                ].fillna("")
            # For numeric columns, use 0 as fill value
            elif np.issubdtype(
                db_entities_sorted[col].dtype, np.number
            ) or np.issubdtype(expected_entities_sorted[col].dtype, np.number):
                db_entities_sorted[col] = db_entities_sorted[col].fillna(0)
                expected_entities_sorted[col] = expected_entities_sorted[
                    col
                ].fillna(0)

        # Direct pandas frame_equal comparison with check_dtype=False
        pd.testing.assert_frame_equal(
            db_entities_sorted[common_cols],
            expected_entities_sorted[common_cols],
            check_dtype=False,
        )
        print("Entities DataFrame comparison passed!")
