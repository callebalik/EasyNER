import pytest
import pandas as pd
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
        "article_1": {
            "title": "Sample Title 1",
            "abstract": "Sample Abstract 1",
            "metadata": {"author": "John Doe", "year": 2023},
            "sentences": [
                {
                    "text": "This is the first sentence.",
                    "tokens": ["This", "is", "the", "first", "sentence", "."],
                    "entities": ["entity_1"],
                    "entity_spans": [[0, 4]],
                    "ids": ["id_1"],
                    "names": ["Entity Name 1"],
                },
                {
                    "text": "This is the second sentence.",
                    "tokens": ["This", "is", "the", "second", "sentence", "."],
                    "entities": ["entity_2", "entity_3"],
                    "entity_spans": [[0, 4], [13, 19]],
                    "ids": ["id_2", "id_3"],
                    "names": ["Entity Name 2", "Entity Name 3"],
                },
            ],
        },
        "article_2": {
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
                    "ids": ["id_4"],
                    "names": ["Entity Name 4"],
                }
            ],
        },
        "article_3": {
            "title": "Empty Article",
            "abstract": "",
            "metadata": {"year": 2021},
            "sentences": [],
        },
        "article_4": {
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
                    "ids": ["id_6"],  # Only one ID
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
    article_1 = df[df["article_id"] == "article_1"].iloc[0]
    assert article_1["title"] == "Sample Title 1"
    assert article_1["abstract"] == "Sample Abstract 1"
    assert article_1["author"] == "John Doe"
    assert article_1["year"] == 2023

    # Check article with missing metadata
    article_3 = df[df["article_id"] == "article_3"].iloc[0]
    assert article_3["title"] == "Empty Article"
    assert article_3["abstract"] == ""
    assert article_3["year"] == 2021
    assert "author" not in article_3 or pd.isna(article_3["author"])


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
    sent_1_0 = df[df["sentence_id"] == "article_1_0"].iloc[0]
    assert sent_1_0["article_id"] == "article_1"
    assert sent_1_0["position"] == 0
    assert sent_1_0["text"] == "This is the first sentence."
    assert "tokens" in sent_1_0  # Tokens should be present

    # Second sentence of first article
    sent_1_1 = df[df["sentence_id"] == "article_1_1"].iloc[0]
    assert sent_1_1["position"] == 1
    assert sent_1_1["text"] == "This is the second sentence."

    # Check sentence from article with incomplete data
    sent_4_0 = df[df["sentence_id"] == "article_4_0"].iloc[0]
    assert sent_4_0["text"] == "This sentence has incomplete entity data."
    assert "tokens" not in sent_4_0 or pd.isna(sent_4_0["tokens"])


def test_extract_sentences_empty_data(handler, empty_data):
    """Test extracting sentences from empty data"""
    df = handler.extract_sentences_dataframe(empty_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_sentences_with_no_sentences(handler):
    """Test extracting sentences when article has no sentences field"""
    data = {
        "article_5": {"title": "No sentences", "abstract": "Abstract only"}
    }
    df = handler.extract_sentences_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


# Tests for extract_entities_dataframe
def test_extract_entities_dataframe(handler, sample_data):
    """Test extracting entities from sample data"""
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

    # Check first entity
    entity_1 = df[df["entity_id"] == "article_1_0_0"].iloc[0]
    assert entity_1["sentence_id"] == "article_1_0"
    assert entity_1["article_id"] == "article_1"
    assert entity_1["text"] == "entity_1"
    assert entity_1["start_char"] == 0
    assert entity_1["end_char"] == 4
    assert entity_1["entity_id_value"] == "id_1"
    assert entity_1["entity_name"] == "Entity Name 1"

    # Check entity from second sentence of first article
    entity_2 = df[df["entity_id"] == "article_1_1_0"].iloc[0]
    assert entity_2["sentence_id"] == "article_1_1"
    assert entity_2["text"] == "entity_2"

    # Check handling of incomplete entity data
    entity_6 = df[df["entity_id"] == "article_4_1_0"].iloc[0]
    assert entity_6["text"] == "entity_6"
    assert entity_6["start_char"] == 0
    assert entity_6["end_char"] == 4
    assert entity_6["entity_id_value"] == "id_6"
    assert "entity_name" not in entity_6 or pd.isna(entity_6["entity_name"])

    # Check entity 7 with missing span is not included
    assert not any(df["entity_id"] == "article_4_1_1")


def test_extract_entities_empty_data(handler, empty_data):
    """Test extracting entities from empty data"""
    df = handler.extract_entities_dataframe(empty_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_entities_with_no_entities(handler):
    """Test extracting entities when sentences have no entities field"""
    data = {
        "article_6": {
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
        "article_7": {
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
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1  # Only the valid entity should be included
    assert df.iloc[0]["text"] == "valid_entity"


def test_extract_entities_logging_on_empty_spans(handler, caplog):
    """Test that a warning is logged for entities with empty spans"""
    data = {
        "article_8": {
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
        assert "Skipping entities in article article_8" in caplog.text
        assert "Found 2 entities but no entity spans" in caplog.text


def test_extract_entities_logging_on_mismatched_spans(handler, caplog):
    """Test that a warning is logged for mismatched entity and span counts"""
    data = {
        "article_9": {
            "title": "Mismatched spans warning",
            "sentences": [
                {
                    "text": "This sentence should generate a mismatch warning.",
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
        assert len(df) == 2  # Only entities with spans should be extracted
        assert any(df["text"] == "entity_10")
        assert any(df["text"] == "entity_11")
        assert not any(df["text"] == "entity_12")  # No span for this entity
        assert "Mismatched entity data in article article_9" in caplog.text
        assert "Found 3 entities but only 2 spans" in caplog.text


def test_extract_entities_logging_on_empty_text(handler, caplog):
    """Test that a warning is logged for entities with empty text"""
    data = {
        "article_10": {
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
        assert (
            "Empty entity text at position 0 in article article_10"
            in caplog.text
        )
