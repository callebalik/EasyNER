import pytest


@pytest.fixture
def sample_data():
    """Fixture with sample data containing articles, sentences, and entities."""
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
                },
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
    """Fixture with empty data."""
    return {}


@pytest.fixture
def no_sentences_data():
    """Fixture with an article that has no sentences field."""
    return {"5": {"title": "No sentences", "abstract": "Abstract only"}}


@pytest.fixture
def no_entities_data():
    """Fixture with articles that have no entities field."""
    return {
        "6": {
            "title": "No entities",
            "sentences": [
                {"text": "This sentence has no entities."},
                {"text": "This sentence also has no entities."},
            ],
        },
    }


@pytest.fixture
def empty_entities_data():
    """Fixture with an article that has an empty entity text."""
    return {
        "7": {
            "title": "Empty entity",
            "sentences": [
                {
                    "text": "This sentence has an empty entity.",
                    "entities": ["", "valid_entity"],
                    "entity_spans": [[0, 0], [5, 10]],
                },
            ],
        },
    }


@pytest.fixture
def empty_spans_data():
    """Fixture with an article that has empty entity spans."""
    return {
        "8": {
            "title": "Empty spans warning",
            "sentences": [
                {
                    "text": "This sentence should generate a warning.",
                    "entities": ["entity_8", "entity_9"],
                    "entity_spans": [],  # Empty spans list
                },
            ],
        },
    }


@pytest.fixture
def mismatched_spans_data():
    """Fixture with an article that has mismatched entity and span counts."""
    return {
        "9": {
            "title": "Mismatched spans warning",
            "sentences": [
                {
                    "text": "This sentence should generate a mismatch warning.",
                    "entities": ["entity_10", "entity_11", "entity_12"],
                    "entity_spans": [
                        [0, 4],
                        [10, 15],
                    ],  # Fewer spans than entities
                },
            ],
        },
    }


@pytest.fixture
def empty_text_data():
    """Fixture with an article that has an empty entity text."""
    return {
        "10": {
            "title": "Empty entity text warning",
            "sentences": [
                {
                    "text": "This sentence has an empty entity text.",
                    "entities": ["", "valid_entity_2"],
                    "entity_spans": [[0, 0], [5, 10]],
                },
            ],
        },
    }
