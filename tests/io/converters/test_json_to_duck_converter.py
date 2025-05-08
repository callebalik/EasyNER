import json

import numpy as np
import pandas as pd
import pytest

from easyner.io.converters.json_to_duck_converter import JsonToDuckConverter
from easyner.io.database.utils.column_names import (
    ARTICLE_ID,
    END_CHAR,
    ENTITY_ID,
    INFERENCE_MODEL,
    INFERENCE_MODEL_METADATA,
    SENTENCE_ID,
    START_CHAR,
    TEXT,
    TITLE,
)
from easyner.io.handlers.pubmed_json_handler import PubMedJsonHandler


def test_list_convertible_files(temp_dir, test_json_file_1):
    """Test listing convertible files"""
    # Create converter
    converter = JsonToDuckConverter(temp_dir, temp_dir / "output")

    # Test listing files
    files = converter.list_convertible_files()
    assert len(files) == 1
    assert files[0].name == "test_articles.json"


def test_convert_to_db(temp_dir, test_json_file_1):
    """Test the conversion process to a database file"""
    # Create output directory
    output_dir = temp_dir / "output"
    output_dir.mkdir(exist_ok=True)

    # Create converter with a persistent database file
    db_path = output_dir / "test.db"
    converter = JsonToDuckConverter(
        temp_dir,
        output_dir,
        db_file=str(db_path),
        log_duplicates=False,
        ignore_duplicates=True,
    )

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
    assert converted[0] == test_json_file_1


def test_convert_with_memory_db(temp_dir, test_json_file_1):
    """Test conversion with in-memory database"""
    # Create converter
    converter = JsonToDuckConverter(
        temp_dir,
        temp_dir / "output",
        log_duplicates=False,
        ignore_duplicates=True,
    )

    # Run conversion with in-memory database
    result = converter.convert(use_memory_db=True)

    # Check results
    assert result["files_processed_this_run"] == 1
    assert result["article_count"] == 2
    assert result["database_path"] == ":memory:"


def test_idempotency_no_reprocessing(temp_dir, test_json_file_1):
    """Test that running convert multiple times without reprocess flag doesn't reprocess files."""
    output_dir = temp_dir / "output_idempotency_no_reprocess"
    output_dir.mkdir(exist_ok=True)
    db_path = output_dir / "idem_no_reprocess.db"

    converter = JsonToDuckConverter(
        temp_dir,
        output_dir,
        db_file=str(db_path),
        log_duplicates=False,
        ignore_duplicates=True,
    )

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


def test_idempotency_with_reprocess_flag(temp_dir, test_json_file_1):
    """Test that the reprocess flag forces reconversion of all files."""
    output_dir = temp_dir / "output_idempotency_reprocess"
    output_dir.mkdir(exist_ok=True)
    db_path = output_dir / "idem_reprocess.db"

    converter = JsonToDuckConverter(
        temp_dir,
        output_dir,
        db_file=str(db_path),
        log_duplicates=False,
        ignore_duplicates=True,
    )

    # First run
    result1 = converter.convert()
    assert result1["files_processed_this_run"] == 1
    assert result1["article_count"] == 2

    # Second run with reprocess=True
    converter_reprocess = JsonToDuckConverter(
        temp_dir,
        output_dir,
        db_file=str(db_path),
        reprocess=True,
        log_duplicates=False,
        ignore_duplicates=True,
    )
    result2 = converter_reprocess.convert()

    # Key change: Files processed should be ALL files (not just the ones that weren't processed before)
    assert (
        result2["files_processed_this_run"] == 1
    )  # All files are reprocessed

    # Counts of items added in this run should reflect all data being reprocessed
    assert result2["articles_added_this_run"] == 2
    assert result2["sentences_added_this_run"] == 3
    assert result2["entities_added_this_run"] == 5

    # Total counts in DB should remain the same (data is replaced, not duplicated)
    assert result2["article_count"] == 2
    assert result2["sentence_count"] == 3
    assert result2["entity_count"] == 5

    # Conversion log should still reflect 1 file as converted (with updated timestamp)
    log_df2 = converter_reprocess.db_handler.get_conversion_log_df()
    assert len(log_df2[log_df2["status"] == "converted"]) == 1


def test_idempotency_adding_new_files(temp_dir, test_data_1):
    """Test processing new files after an initial run."""
    output_dir = temp_dir / "output_idempotency_new_files"
    output_dir.mkdir(exist_ok=True)
    db_path = output_dir / "idem_new_files.db"

    # Create initial file
    file1_path = temp_dir / "articles1.json"
    with open(file1_path, "w") as f:
        json.dump({"1": test_data_1["1"]}, f)  # Only first article

    converter = JsonToDuckConverter(
        temp_dir,
        output_dir,
        db_file=str(db_path),
        log_duplicates=False,
        ignore_duplicates=True,
    )

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
        json.dump({"2": test_data_1["2"]}, f)  # Only second article

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
            example_files_dir,
            output_dir,
            db_file=str(db_path),
            log_duplicates=False,
            ignore_duplicates=True,
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


class TestDuplicateTableLogging:
    """Test class for validating the logging of duplicates in the database.

    This is the main behavior that will be used by the converter
    It mimics the TestExampleDataStructureAndContent class but expects
    output to the duplicates table
    """

    @pytest.fixture
    def test_db_with_duplicates(
        self, temp_dir, test_json_file_1, test_json_file_combined, test_data_1
    ):
        """Setup a test database with intentional duplicate entries but without entity duplicates."""
        # Create output directory
        output_dir = temp_dir / "duplicate_test_output"
        output_dir.mkdir(exist_ok=True)

        # Create persistent database file
        db_path = output_dir / "duplicate_test.db"

        # First run - process test_json_file_1 (contains test_data_1)
        converter1 = JsonToDuckConverter(
            temp_dir,
            output_dir,
            db_file=str(db_path),
            file_pattern=test_json_file_1.name,
            log_duplicates=True,
            ignore_duplicates=False,
        )
        result1 = converter1.convert()

        # Ensure articles and sentences were inserted in the first run
        article_count = converter1.connection.execute(
            "SELECT COUNT(*) FROM articles"
        ).fetchone()[0]
        assert article_count > 0, "No articles were inserted in the first run"

        sentence_count = converter1.connection.execute(
            "SELECT COUNT(*) FROM sentences"
        ).fetchone()[0]
        assert (
            sentence_count > 0
        ), "No sentences were inserted in the first run"

        # Load the combined file JSON data
        with open(test_json_file_combined, "r") as f:
            combined_data = json.load(f)

        # Remove entities from articles that already exist in test_data_1
        # BUT keep the articles and sentences intact for duplicate detection
        for article_id in test_data_1:
            if article_id in combined_data:
                for sentence in combined_data[article_id]["sentences"]:
                    if "entities" in sentence:
                        sentence["entities"] = (
                            []
                        )  # Clear all entities from duplicated articles

        # Write the modified data to a new file
        modified_combined_file = temp_dir / "modified_combined.json"
        with open(modified_combined_file, "w") as f:
            json.dump(combined_data, f)

        # Second run - process modified combined file (no entity duplicates)
        converter2 = JsonToDuckConverter(
            temp_dir,
            output_dir,
            db_file=str(db_path),
            file_pattern=modified_combined_file.name,
            log_duplicates=True,
            ignore_duplicates=False,
        )
        result2 = converter2.convert()

        # Store test data in the fixture
        test_data = {
            "converter": converter2,
            "connection": converter2.connection,
            "db_handler": converter2.db_handler,
            "db_path": db_path,
            "first_run_result": result1,
            "second_run_result": result2,
            "test_data": test_data_1,  # The original test data for reference
        }

        yield test_data

        # Close the connection when done
        converter2.connection.close()

    def test_expected_duplicates_logged(self, test_db_with_duplicates):
        """Test that the expected duplicates were logged to duplicate tables."""
        connection = test_db_with_duplicates["connection"]
        test_data = test_db_with_duplicates["test_data"]

        # Count article duplicates - should be exactly the number of articles in test_data_1
        expected_article_count = len(test_data)
        article_dup_count = connection.execute(
            "SELECT COUNT(*) FROM articles_duplicates"
        ).fetchone()[0]
        assert (
            article_dup_count == expected_article_count
        ), f"Expected {expected_article_count} duplicate articles, got {article_dup_count}"

        # Verify the first duplicate article is article 1
        article_id = connection.execute(
            "SELECT article_id FROM articles_duplicates WHERE article_id = '1'"
        ).fetchone()
        assert (
            article_id is not None
        ), "Expected duplicate article_id '1' not found"

        # Count sentence duplicates - should match the number of sentences in all test_data_1 articles
        expected_sentence_count = sum(
            len(article["sentences"]) for article in test_data.values()
        )
        sentence_dup_count = connection.execute(
            "SELECT COUNT(*) FROM sentences_duplicates"
        ).fetchone()[0]
        assert (
            sentence_dup_count == expected_sentence_count
        ), f"Expected {expected_sentence_count} duplicate sentences, got {sentence_dup_count}"

        # We don't expect any entity duplicates since we removed them
        try:
            entity_dup_count = connection.execute(
                "SELECT COUNT(*) FROM entities_duplicates"
            ).fetchone()[0]
            assert (
                entity_dup_count == 0
            ), f"Expected 0 duplicate entities, got {entity_dup_count}"
        except Exception as e:
            # It's also acceptable if the table doesn't exist at all
            if "Table with name entities_duplicates does not exist" in str(e):
                pass
            else:
                raise

    def test_main_tables_have_correct_records(self, test_db_with_duplicates):
        """Test that main tables contain all unique records despite duplicate inserts."""
        db_handler = test_db_with_duplicates["db_handler"]
        test_data = test_db_with_duplicates["test_data"]
        second_run_result = test_db_with_duplicates["second_run_result"]

        # Get counts from main tables
        articles_count = db_handler.get_table_count("articles")
        sentences_count = db_handler.get_table_count("sentences")
        entities_count = db_handler.get_table_count("entities")

        # Expected counts from second run result (should contain everything)
        assert articles_count == second_run_result["article_count"]
        assert sentences_count == second_run_result["sentence_count"]
        assert entities_count == second_run_result["entity_count"]

        # Check that we have test_data_1 articles in the main tables
        for article_id in test_data:
            article_exists = db_handler.connection.execute(
                f"SELECT COUNT(*) FROM articles WHERE article_id = '{article_id}'"
            ).fetchone()[0]
            assert (
                article_exists == 1
            ), f"Article {article_id} not found in main articles table"

    def test_duplicate_content_matches_original(self, test_db_with_duplicates):
        """Test that content in duplicate tables matches the data in main tables."""
        connection = test_db_with_duplicates["connection"]

        # Test article duplicates
        dup_articles = connection.execute(
            """
            SELECT d.article_id, d.title, a.article_id, a.title
            FROM articles_duplicates d
            JOIN articles a ON d.article_id = a.article_id
        """
        ).fetchall()

        assert len(dup_articles) > 0, "No matching article duplicates found"
        for dup_article in dup_articles:
            assert (
                dup_article[0] == dup_article[2]
            ), f"Article IDs don't match: {dup_article[0]} vs {dup_article[2]}"
            assert (
                dup_article[1] == dup_article[3]
            ), f"Article titles don't match: {dup_article[1]} vs {dup_article[3]}"

        # Test sentence duplicates
        dup_sentences = connection.execute(
            """
            SELECT d.article_id, d.sentence_id, d.text, s.article_id, s.sentence_id, s.text
            FROM sentences_duplicates d
            JOIN sentences s ON d.article_id = s.article_id AND d.sentence_id = s.sentence_id
            LIMIT 3
        """
        ).fetchall()

        assert len(dup_sentences) > 0, "No matching sentence duplicates found"
        for dup_sentence in dup_sentences:
            assert (
                dup_sentence[0] == dup_sentence[3]
            ), f"Sentence article IDs don't match: {dup_sentence[0]} vs {dup_sentence[3]}"
            assert (
                dup_sentence[1] == dup_sentence[4]
            ), f"Sentence IDs don't match: {dup_sentence[1]} vs {dup_sentence[4]}"
            assert (
                dup_sentence[2] == dup_sentence[5]
            ), f"Sentence text doesn't match: {dup_sentence[2]} vs {dup_sentence[5]}"

    def test_duplicate_detection_timestamp_exists(
        self, test_db_with_duplicates
    ):
        """Test that each duplicate record has a timestamp field."""
        connection = test_db_with_duplicates["connection"]

        # Check for timestamp in articles_duplicates
        if (
            connection.execute(
                "SELECT COUNT(*) FROM articles_duplicates"
            ).fetchone()[0]
            > 0
        ):
            timestamp = connection.execute(
                "SELECT duplicate_detection_timestamp FROM articles_duplicates LIMIT 1"
            ).fetchone()[0]
            assert (
                timestamp is not None
            ), "Missing timestamp in articles_duplicates"

        # Check for timestamp in sentences_duplicates
        if (
            connection.execute(
                "SELECT COUNT(*) FROM sentences_duplicates"
            ).fetchone()[0]
            > 0
        ):
            timestamp = connection.execute(
                "SELECT duplicate_detection_timestamp FROM sentences_duplicates LIMIT 1"
            ).fetchone()[0]
            assert (
                timestamp is not None
            ), "Missing timestamp in sentences_duplicates"

        # Check for timestamp in entities_duplicates only if the table exists
        try:
            count_result = connection.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'entities_duplicates'"
            ).fetchone()

            # If table exists and has records, check for timestamp
            if count_result and count_result[0] > 0:
                if (
                    connection.execute(
                        "SELECT COUNT(*) FROM entities_duplicates"
                    ).fetchone()[0]
                    > 0
                ):
                    timestamp = connection.execute(
                        "SELECT duplicate_detection_timestamp FROM entities_duplicates LIMIT 1"
                    ).fetchone()[0]
                    assert (
                        timestamp is not None
                    ), "Missing timestamp in entities_duplicates"
        except:
            # Table doesn't exist - that's fine in this test scenario
            pass

    def test_duplicate_table_has_all_expected_records(
        self, test_db_with_duplicates
    ):
        """Test that every expected duplicate is logged to the duplicate tables."""
        connection = test_db_with_duplicates["connection"]
        test_data = test_db_with_duplicates["test_data"]

        # 1. Check that each article_id in test_data has a corresponding entry in articles_duplicates
        for article_id in test_data:
            duplicate_exists = connection.execute(
                f"SELECT COUNT(*) FROM articles_duplicates WHERE article_id = '{article_id}'"
            ).fetchone()[0]
            assert (
                duplicate_exists == 1
            ), f"Article ID {article_id} not found in duplicate table"

        # 2. First get the actual sentence IDs from the sentences table for each article
        for article_id, article_data in test_data.items():
            # Get all sentence IDs for this article from the main sentences table
            sentence_rows = connection.execute(
                f"SELECT sentence_id FROM sentences WHERE article_id = '{article_id}'"
            ).fetchall()
            sentence_ids = [row[0] for row in sentence_rows]

            # Make sure we have the expected number of sentences
            assert len(sentence_ids) == len(
                article_data["sentences"]
            ), f"Expected {len(article_data['sentences'])} sentences for article {article_id}, got {len(sentence_ids)}"

            # Now check each sentence ID exists in the duplicates table
            for sentence_id in sentence_ids:
                duplicate_exists = connection.execute(
                    f"SELECT COUNT(*) FROM sentences_duplicates WHERE article_id = '{article_id}' AND sentence_id = {sentence_id}"
                ).fetchone()[0]
                assert (
                    duplicate_exists == 1
                ), f"Sentence {article_id}/{sentence_id} not found in duplicate table"
