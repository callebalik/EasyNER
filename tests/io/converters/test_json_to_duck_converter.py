import os
import tempfile
import pandas as pd
from pathlib import Path
import pytest

from easyner.io.converters.json_to_duck_converter import JsonToDuckConverter
from easyner.io.database.db_utils import (
    get_articles,
    get_sentences,
    get_entities,
    initialize_db,
)


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
    assert result["files_processed"] == 1
    assert result["article_count"] == 2
    assert result["sentence_count"] == 3
    assert result["entity_count"] == 5

    # Check that the database file was created
    assert db_path.exists()

    # Check database content
    connection = converter.connection

    # Retrieve data from database
    articles_data = get_articles(connection)
    sentences_data = get_sentences(connection)
    entities_data = get_entities(connection)

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
    assert result["files_processed"] == 1
    assert result["article_count"] == 2
    assert result["database_path"] == ":memory:"


def test_with_example_files(
    example_files_dir,
    example_json_file,
    expected_articles_df,
    expected_sentences_df,
    expected_entities_df,
):
    """Test the converter using the example files in the tests directory"""
    # Setup temporary output directory
    with tempfile.TemporaryDirectory() as temp_output_dir:
        output_dir = Path(temp_output_dir)

        # Create persistent database file for testing
        db_path = output_dir / "example_test.db"

        # Create converter with a persistent database file
        converter = JsonToDuckConverter(
            example_files_dir, output_dir, db_file=str(db_path)
        )

        # Run conversion
        result = converter.convert()

        # Verify result statistics
        assert result["files_processed"] == 1
        assert result["article_count"] > 0
        assert result["sentence_count"] > 0
        assert result["entity_count"] > 0

        # Check that database file was created
        assert db_path.exists()

        # Get database connection from converter
        connection = converter.connection

        # Retrieve data from database
        articles_data = pd.DataFrame(get_articles(connection))
        sentences_data = pd.DataFrame(get_sentences(connection))
        entities_data = pd.DataFrame(get_entities(connection))

        # Convert article_id to string for comparison with CSV data
        articles_data["article_id"] = articles_data["article_id"].astype(str)
        sentences_data["article_id"] = sentences_data["article_id"].astype(str)
        entities_data["article_id"] = entities_data["article_id"].astype(str)

        # Check counts match the expected data from CSV files
        assert len(articles_data) == len(expected_articles_df)
        assert len(sentences_data) == len(expected_sentences_df)
        assert len(entities_data) == len(expected_entities_df)

        # Check for the presence of key articles
        assert "466750" in articles_data["article_id"].values
        assert "466751" in articles_data["article_id"].values

        # Check for the presence of specific entities
        filtered_entities = entities_data[entities_data["entity"] == "asthma"]
        assert (
            len(filtered_entities) >= 3
        )  # There should be at least 3 instances of "asthma"


def test_example_data_structure_and_content(
    example_files_dir,
    example_json_file,
    expected_articles_df,
    expected_sentences_df,
    expected_entities_df,
):
    """Test that the structure and content of the database matches the expected CSV files."""
    # Setup temporary output directory
    with tempfile.TemporaryDirectory() as temp_output_dir:
        output_dir = Path(temp_output_dir)

        # Create persistent database file for testing
        db_path = output_dir / "content_test.db"

        # Create converter with a persistent database file
        converter = JsonToDuckConverter(
            example_files_dir, output_dir, db_file=str(db_path)
        )
        result = converter.convert()

        # Verify that database was created
        assert db_path.exists()

        # Get database connection from converter
        connection = converter.connection

        # Retrieve data from database
        articles_data = pd.DataFrame(get_articles(connection))
        sentences_data = pd.DataFrame(get_sentences(connection))
        entities_data = pd.DataFrame(get_entities(connection))

        # Convert article_id to string for comparison with CSV data
        articles_data["article_id"] = articles_data["article_id"].astype(str)
        sentences_data["article_id"] = sentences_data["article_id"].astype(str)
        entities_data["article_id"] = entities_data["article_id"].astype(str)

        # Verify column structure (database columns may have additional fields)
        required_article_cols = ["article_id", "title"]
        required_sentence_cols = ["article_id", "sentence_id", "text"]
        required_entity_cols = [
            "article_id",
            "sentence_id",
            "entity",
            "start_pos",
            "end_pos",
        ]

        for col in required_article_cols:
            assert col in articles_data.columns
        for col in required_sentence_cols:
            assert col in sentences_data.columns
        for col in required_entity_cols:
            assert col in entities_data.columns

        # Compare the articles with the expected CSV data
        for _, expected_row in expected_articles_df.iterrows():
            article_id = str(expected_row["article_id"])
            # Find matching article in database
            matching_article = articles_data[
                articles_data["article_id"] == article_id
            ]
            if not matching_article.empty:
                assert (
                    matching_article["title"].iloc[0] == expected_row["title"]
                )

        # Compare the sentences with the expected CSV data
        for _, expected_row in expected_sentences_df.iterrows():
            article_id = str(expected_row["article_id"])
            sentence_id = expected_row["sentence_id"]

            # Find matching sentence in database
            matching_sentence = sentences_data[
                (sentences_data["article_id"] == article_id)
                & (sentences_data["sentence_id"] == sentence_id)
            ]
            if not matching_sentence.empty:
                assert (
                    matching_sentence["text"].iloc[0] == expected_row["text"]
                )

        # Compare the entities with the expected CSV data
        for _, expected_row in expected_entities_df.iterrows():
            article_id = str(expected_row["article_id"])
            sentence_id = expected_row["sentence_id"]
            entity = expected_row["entity"]
            start_pos = expected_row["start_pos"]
            end_pos = expected_row["end_pos"]

            # Find matching entity in database
            matching_entity = entities_data[
                (entities_data["article_id"] == article_id)
                & (entities_data["sentence_id"] == sentence_id)
                & (entities_data["entity"] == entity)
                & (entities_data["start_pos"] == start_pos)
                & (entities_data["end_pos"] == end_pos)
            ]
            if not matching_entity.empty:
                assert len(matching_entity) == 1
