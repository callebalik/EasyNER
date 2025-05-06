import pytest
import pandas as pd
import numpy as np  # Add explicit import for numpy
import logging
from easyner.io.handlers.pubmed_json_handler import PubMedJsonHandler


@pytest.fixture
def handler():
    """Fixture to create a PubMedJsonHandler instance"""
    return PubMedJsonHandler()


@pytest.fixture
def sample_data():
    """Fixture with sample data containing articles, sentences, and entities"""
    return {
        "1": {
            "title": "Sample Title 1",
            "abstract": "Sample Abstract 1",
            "metadata": {"author": "John Doe", "year": 2023},
            "sentences": [
                {
                    "text": "This is the first sentence.",
                    "tokens": ["This", "is", "the", "first", "sentence", "."],
                    "entities": ["entity_1"],
                    "entity_spans": [[0, 4]],
                    "names": ["Entity Name 1"],
                },
                {
                    "text": "This is the second sentence.",
                    "tokens": ["This", "is", "the", "second", "sentence", "."],
                    "entities": ["entity_2", "entity_3"],
                    "entity_spans": [[0, 4], [13, 19]],
                    "names": ["Entity Name 2", "Entity Name 3"],
                },
            ],
        },
        "2": {
            "title": "Sample Title 2",
            "abstract": "Sample Abstract 2",
            "metadata": {"author": "Jane Smith", "year": 2022},
            "sentences": [
                {
                    "text": "This is a sentence from another article.",
                    "tokens": [
                        "This",
                        "is",
                        "a",
                        "sentence",
                        "from",
                        "another",
                        "article",
                        ".",
                    ],
                    "entities": ["entity_4"],
                    "entity_spans": [[8, 16]],
                    "names": ["Entity Name 4"],
                }
            ],
        },
        "3": {
            "title": "Empty Article",
            "abstract": "",
            "metadata": {"year": 2021},
            "sentences": [],
        },
        "4": {
            "title": "Article with incomplete entities",
            "abstract": "Testing edge cases",
            "sentences": [
                {
                    "text": "This sentence has incomplete entity data.",
                    "entities": ["entity_5"],
                    "entity_spans": [],  # Empty spans
                },
                {
                    "text": "This sentence has mismatched entity data.",
                    "entities": ["entity_6", "entity_7"],
                    "entity_spans": [[0, 4]],  # Only one span
                },
            ],
        },
    }


@pytest.fixture
def empty_data():
    """Fixture with empty data"""
    return {}


# Tests for extract_articles_dataframe
def test_extract_articles_dataframe(handler, sample_data):
    """Test extracting articles from sample data"""
    df = handler.extract_articles_dataframe(sample_data)

    # Check DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 4  # Correct number of articles in sample data
    assert set(df.columns).issuperset({"article_id", "title", "abstract"})

    # Check metadata columns
    assert "author" in df.columns
    assert "year" in df.columns

    # Check specific article data
    article_1 = df[df["article_id"] == 1].iloc[0]
    assert article_1["title"] == "Sample Title 1"
    assert article_1["abstract"] == "Sample Abstract 1"
    assert article_1["author"] == "John Doe"
    assert article_1["year"] == 2023

    # Check article with missing metadata
    article_3 = df[df["article_id"] == 3].iloc[0]
    assert article_3["title"] == "Empty Article"
    assert article_3["abstract"] == ""
    assert article_3["year"] == 2021
    assert "author" not in article_3 or pd.isna(article_3["author"])

    # Verify article_id is integer
    assert all(isinstance(id_val, int) for id_val in df["article_id"])


def test_extract_articles_empty_data(handler, empty_data):
    """Test extracting articles from empty data"""
    df = handler.extract_articles_dataframe(empty_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


# Tests for extract_sentences_dataframe
def test_extract_sentences_dataframe(handler, sample_data):
    """Test extracting sentences from sample data"""
    df = handler.extract_sentences_dataframe(sample_data)

    # Check DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5  # Correct number of sentences across all articles
    assert set(df.columns).issuperset(
        {"sentence_id", "article_id", "position", "text"}
    )

    # Check specific data
    # First sentence of first article
    sent_1_0 = df[(df["article_id"] == 1) & (df["position"] == 0)].iloc[0]
    assert sent_1_0["article_id"] == 1
    assert sent_1_0["position"] == 0
    assert sent_1_0["text"] == "This is the first sentence."
    assert "tokens" in sent_1_0  # Tokens should be present

    # Second sentence of first article
    sent_1_1 = df[(df["article_id"] == 1) & (df["position"] == 1)].iloc[0]
    assert sent_1_1["position"] == 1
    assert sent_1_1["text"] == "This is the second sentence."

    # Check sentence from article with incomplete data
    sent_4_0 = df[(df["article_id"] == 4) & (df["position"] == 0)].iloc[0]
    assert sent_4_0["text"] == "This sentence has incomplete entity data."
    assert "tokens" not in sent_4_0 or pd.isna(sent_4_0["tokens"])


def test_extract_sentences_empty_data(handler, empty_data):
    """Test extracting sentences from empty data"""
    df = handler.extract_sentences_dataframe(empty_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_sentences_with_no_sentences(handler):
    """Test extracting sentences when article has no sentences field"""
    data = {"5": {"title": "No sentences", "abstract": "Abstract only"}}
    df = handler.extract_sentences_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


# Tests for extract_entities_dataframe
def test_extract_entities_dataframe_structure(handler, sample_data):
    """Test the basic structure of the entities DataFrame"""
    df = handler.extract_entities_dataframe(sample_data)

    # Check DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 4  # Correct number of entities with valid spans
    assert set(df.columns).issuperset(
        {
            "entity_id",
            "sentence_id",
            "article_id",
            "text",
            "start_char",
            "end_char",
        }
    )


def test_extract_entities_first_entity(handler, sample_data):
    """Test extraction of the first entity in the dataset"""
    df = handler.extract_entities_dataframe(sample_data)

    # First entity in article 1, sentence 0
    entity_1 = df[(df["article_id"] == 1) & (df["sentence_id"] == 0)].iloc[0]
    assert entity_1["article_id"] == 1

    # Check that sentence_id is an integer type (either Python int or numpy.integer)
    assert isinstance(
        entity_1["sentence_id"],
        (int, np.integer),  # Use np.integer instead of pd.np.integer
    ), f"Expected integer type but got {type(entity_1['sentence_id'])}"
    assert entity_1["text"] == "entity_1"
    assert entity_1["start_char"] == 0
    assert entity_1["end_char"] == 4
    assert entity_1["entity_name"] == "Entity Name 1"


def test_extract_entities_second_sentence(handler, sample_data):
    """Test extraction of entities from the second sentence"""
    df = handler.extract_entities_dataframe(sample_data)

    # Entity from second sentence of first article
    entity_2 = df[
        (df["article_id"] == 1)
        & (df["sentence_id"] == 1)
        & (df["text"] == "entity_2")
    ].iloc[0]
    # Accept both Python int and numpy integer types
    assert isinstance(
        entity_2["sentence_id"],
        (int, np.integer),  # Use np.integer instead of pd.np.integer
    ), f"Expected integer type but got {type(entity_2['sentence_id'])}"
    assert entity_2["text"] == "entity_2"


def test_extract_entities_skip_missing_spans(handler, sample_data):
    """Test that entities with missing spans are skipped"""
    df = handler.extract_entities_dataframe(sample_data)

    # Check that entity 7 with missing span is not included
    entity_7_entries = df[(df["article_id"] == 4) & (df["text"] == "entity_7")]
    assert len(entity_7_entries) == 0


def test_extract_entities_empty_data(handler, empty_data):
    """Test extracting entities from empty data"""
    df = handler.extract_entities_dataframe(empty_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_entities_with_no_entities(handler):
    """Test extracting entities when sentences have no entities field"""
    data = {
        "6": {
            "title": "No entities",
            "sentences": [
                {"text": "This sentence has no entities."},
                {"text": "This sentence also has no entities."},
            ],
        }
    }
    df = handler.extract_entities_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_entities_with_empty_entities(handler):
    """Test extracting entities when entity text is empty"""
    data = {
        "7": {
            "title": "Empty entity",
            "sentences": [
                {
                    "text": "This sentence has an empty entity.",
                    "entities": ["", "valid_entity"],
                    "entity_spans": [[0, 0], [5, 10]],
                }
            ],
        }
    }
    df = handler.extract_entities_dataframe(data)
    assert len(df) == 1  # Only the valid entity should be included
    assert df.iloc[0]["text"] == "valid_entity"


def test_extract_entities_logging_on_empty_spans(handler, caplog):
    """Test that a warning is logged for entities with empty spans"""
    data = {
        "8": {
            "title": "Empty spans warning",
            "sentences": [
                {
                    "text": "This sentence should generate a warning.",
                    "entities": ["entity_8", "entity_9"],
                    "entity_spans": [],  # Empty spans list
                }
            ],
        }
    }

    # Use caplog to capture log messages
    with caplog.at_level(logging.WARNING):
        df = handler.extract_entities_dataframe(data)
        assert len(df) == 0  # No entities should be extracted
        assert "Skipping entities in article 8" in caplog.text
        assert "Found 2 entities but no entity spans" in caplog.text


def test_extract_entities_logging_on_mismatched_spans(handler, caplog):
    """Test that a warning is logged for mismatched entity and span counts"""
    data = {
        "9": {
            "title": "Mismatched spans warning",
            "sentences": [
                {
                    "text": "This sentence should generate a mismatch warning.",  # Fixed line length
                    "entities": ["entity_10", "entity_11", "entity_12"],
                    "entity_spans": [
                        [0, 4],
                        [10, 15],
                    ],  # Fewer spans than entities
                }
            ],
        }
    }

    # Use caplog to capture log messages
    with caplog.at_level(logging.WARNING):
        df = handler.extract_entities_dataframe(data)
        assert (
            len(df) == 0
        )  # Expect no entities when there's a mismatch (skipping behavior)
        assert "Mismatched entity data in article 9" in caplog.text
        assert "Found 3 entities but only 2 spans" in caplog.text


def test_extract_entities_logging_on_empty_text(handler, caplog):
    """Test that a warning is logged for entities with empty text"""
    data = {
        "10": {
            "title": "Empty entity text warning",
            "sentences": [
                {
                    "text": "This sentence has an empty entity text.",
                    "entities": ["", "valid_entity_2"],
                    "entity_spans": [[0, 0], [5, 10]],
                }
            ],
        }
    }

    # Use caplog to capture log messages
    with caplog.at_level(logging.WARNING):
        df = handler.extract_entities_dataframe(data)
        assert len(df) == 1  # Only the valid entity should be included
        assert df.iloc[0]["text"] == "valid_entity_2"
        assert "Empty entity text at position 0 in article 10" in caplog.text
