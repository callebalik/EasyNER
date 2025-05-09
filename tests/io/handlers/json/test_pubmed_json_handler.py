"""Unit tests for the PubMedJsonHandler class.

This module contains test cases to validate the functionality of the
PubMedJsonHandler class, including methods for extracting articles,
sentences, and entities from JSON data.
"""

import logging

import numpy as np
import pandas as pd
import pytest

from easyner.io.database.schemas.python_mappings import (
    ARTICLE_ID,
    END_CHAR,
    ENTITY_ID,
    SENTENCE_ID,
    START_CHAR,
    TEXT,
    TITLE,
)
from easyner.io.handlers.pubmed_json_handler import PubMedJsonHandler


@pytest.fixture
def handler():
    """Fixture to create a PubMedJsonHandler instance."""
    return PubMedJsonHandler()


def test_extract_articles_dataframe(handler, sample_data):
    """Test extracting articles from sample data."""
    df = handler.extract_articles_dataframe(sample_data)

    # Check DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 4  # Correct number of articles in sample data
    assert set(df.columns).issuperset({ARTICLE_ID, TITLE, "abstract"})

    # Check metadata columns
    assert "author" in df.columns
    assert "year" in df.columns

    # Check specific article data
    article_1 = df[df[ARTICLE_ID] == 1].iloc[0]
    assert article_1[TITLE] == "Sample Title 1"
    assert article_1["abstract"] == "Sample Abstract 1"
    assert article_1["author"] == "John Doe"
    assert article_1["year"] == 2023

    # Check article with missing metadata
    article_3 = df[df[ARTICLE_ID] == 3].iloc[0]
    assert article_3[TITLE] == "Empty Article"
    assert article_3["abstract"] == ""
    assert article_3["year"] == 2021
    assert "author" not in article_3 or pd.isna(article_3["author"])

    # Verify article_id is integer
    assert all(isinstance(id_val, int) for id_val in df[ARTICLE_ID])


def test_extract_articles_empty_data(handler, empty_data):
    """Test extracting articles from empty data."""
    df = handler.extract_articles_dataframe(empty_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_sentences_dataframe(handler, sample_data):
    """Test extracting sentences from sample data."""
    df = handler.extract_sentences_dataframe(sample_data)

    # Check DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5  # Correct number of sentences across all articles
    assert set(df.columns).issuperset(
        {SENTENCE_ID, ARTICLE_ID, TEXT},
    )

    # Check specific data
    # First sentence of first article
    sent_1_0 = df[(df[ARTICLE_ID] == 1) & (df[SENTENCE_ID] == 0)].iloc[0]
    assert sent_1_0[ARTICLE_ID] == 1
    assert sent_1_0[SENTENCE_ID] == 0
    assert sent_1_0[TEXT] == "This is the first sentence."
    assert "tokens" in sent_1_0  # Tokens should be present

    # Second sentence of first article
    sent_1_1 = df[(df[ARTICLE_ID] == 1) & (df[SENTENCE_ID] == 1)].iloc[0]
    assert sent_1_1[SENTENCE_ID] == 1
    assert sent_1_1[TEXT] == "This is the second sentence."

    # Check sentence from article with incomplete data
    sent_4_0 = df[(df[ARTICLE_ID] == 4) & (df[SENTENCE_ID] == 0)].iloc[0]
    assert sent_4_0[TEXT] == "This sentence has incomplete entity data."
    assert "tokens" not in sent_4_0 or pd.isna(sent_4_0["tokens"])


def test_extract_sentences_empty_data(handler, empty_data):
    """Test extracting sentences from empty data."""
    df = handler.extract_sentences_dataframe(empty_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_sentences_with_no_sentences(handler, no_sentences_data):
    """Test extracting sentences when article has no sentences field."""
    df = handler.extract_sentences_dataframe(no_sentences_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_entities_dataframe_structure(handler, sample_data):
    """Test the basic structure of the entities DataFrame."""
    df = handler.extract_entities_dataframe(sample_data)

    # Check DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 4  # Correct number of entities with valid spans
    assert set(df.columns).issuperset(
        {
            SENTENCE_ID,
            ARTICLE_ID,
            TEXT,
            START_CHAR,
            END_CHAR,
        },
    )


def test_extract_entities_first_entity(handler, sample_data):
    """Test extraction of the first entity in the dataset."""
    df = handler.extract_entities_dataframe(sample_data)

    # First entity in article 1, sentence 0
    entity_1 = df[(df[ARTICLE_ID] == 1) & (df[SENTENCE_ID] == 0)].iloc[0]
    assert entity_1[ARTICLE_ID] == 1

    # Check that sentence_id is an integer type (either Python int or numpy.integer)
    assert isinstance(
        entity_1[SENTENCE_ID],
        (int, np.integer),  # Use np.integer instead of pd.np.integer
    ), f"Expected integer type but got {type(entity_1[SENTENCE_ID])}"
    assert entity_1[TEXT] == "entity_1"
    assert entity_1[START_CHAR] == 0
    assert entity_1[END_CHAR] == 4
    assert entity_1["entity_name"] == "Entity Name 1"


def test_extract_entities_second_sentence(handler, sample_data):
    """Test extraction of entities from the second sentence."""
    df = handler.extract_entities_dataframe(sample_data)

    # Entity from second sentence of first article
    entity_2 = df[
        (df[ARTICLE_ID] == 1)
        & (df[SENTENCE_ID] == 1)
        & (df[TEXT] == "entity_2")
    ].iloc[0]
    # Accept both Python int and numpy integer types
    assert isinstance(
        entity_2[SENTENCE_ID],
        (int, np.integer),  # Use np.integer instead of pd.np.integer
    ), f"Expected integer type but got {type(entity_2[SENTENCE_ID])}"
    assert entity_2[TEXT] == "entity_2"


def test_extract_entities_skip_missing_spans(handler, sample_data):
    """Test that entities with missing spans are skipped."""
    df = handler.extract_entities_dataframe(sample_data)

    # Check that entity 7 with missing span is not included
    entity_7_entries = df[(df[ARTICLE_ID] == 4) & (df[TEXT] == "entity_7")]
    assert len(entity_7_entries) == 0


def test_extract_entities_empty_data(handler, empty_data):
    """Test extracting entities from empty data."""
    df = handler.extract_entities_dataframe(empty_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_entities_with_no_entities(handler, no_entities_data):
    """Test extracting entities when sentences have no entities field."""
    df = handler.extract_entities_dataframe(no_entities_data)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_extract_entities_with_empty_entities(handler, empty_entities_data):
    """Test extracting entities when entity text is empty."""
    df = handler.extract_entities_dataframe(empty_entities_data)
    assert len(df) == 1  # Only the valid entity should be included
    assert df.iloc[0][TEXT] == "valid_entity"


def test_extract_entities_logging_on_empty_spans(
    handler,
    empty_spans_data,
    caplog,
):
    """Test that a warning is logged for entities with empty spans."""
    with caplog.at_level(logging.WARNING):
        df = handler.extract_entities_dataframe(empty_spans_data)
        assert len(df) == 0  # No entities should be extracted
        assert "Skipping entities in article 8" in caplog.text
        assert "Found 2 entities but no entity spans" in caplog.text


def test_extract_entities_logging_on_mismatched_spans(
    handler,
    mismatched_spans_data,
    caplog,
):
    """Test that a warning is logged for mismatched entity and span counts."""
    with caplog.at_level(logging.WARNING):
        df = handler.extract_entities_dataframe(mismatched_spans_data)
        assert len(df) == 0  # Expect no entities when there's a mismatch
        assert "Mismatched entity data in article 9" in caplog.text
        assert "Found 3 entities but only 2 spans" in caplog.text


def test_extract_entities_logging_on_empty_text(
    handler,
    empty_text_data,
    caplog,
):
    """Test that a warning is logged for entities with empty text."""
    with caplog.at_level(logging.WARNING):
        df = handler.extract_entities_dataframe(empty_text_data)
        assert len(df) == 1  # Only the valid entity should be included
        assert df.iloc[0][TEXT] == "valid_entity_2"
        assert "Empty entity text at position 0 in article 10" in caplog.text
