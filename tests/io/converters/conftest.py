import pytest
import tempfile
import shutil
import json
import pandas as pd
from pathlib import Path
import os


@pytest.fixture
def example_files_dir():
    """Return the directory containing the example files."""
    return Path(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def example_json_file(example_files_dir):
    """Return the path to the example JSON file."""
    json_file = example_files_dir / "example_pubmed_articles.json"
    if not json_file.exists():
        pytest.skip(f"Example JSON file {json_file} not found")
    return json_file


@pytest.fixture
def example_articles_csv(example_files_dir):
    """Return the path to the expected articles output CSV."""
    csv_file = example_files_dir / "articles_output.csv"
    if not csv_file.exists():
        pytest.skip(f"Expected articles CSV file {csv_file} not found")
    return csv_file


@pytest.fixture
def example_sentences_csv(example_files_dir):
    """Return the path to the expected sentences output CSV."""
    csv_file = example_files_dir / "sentences_output.csv"
    if not csv_file.exists():
        pytest.skip(f"Expected sentences CSV file {csv_file} not found")
    return csv_file


@pytest.fixture
def example_entities_csv(example_files_dir):
    """Return the path to the expected entities output CSV."""
    csv_file = example_files_dir / "entities_output.csv"
    if not csv_file.exists():
        pytest.skip(f"Expected entities CSV file {csv_file} not found")
    return csv_file


@pytest.fixture
def expected_articles_df(example_articles_csv):
    """Return a pandas DataFrame containing the expected articles data."""
    return pd.read_csv(example_articles_csv)


@pytest.fixture
def expected_sentences_df(example_sentences_csv):
    """Return a pandas DataFrame containing the expected sentences data."""
    return pd.read_csv(example_sentences_csv)


@pytest.fixture
def expected_entities_df(example_entities_csv):
    """Return a pandas DataFrame containing the expected entities data."""
    return pd.read_csv(example_entities_csv)


@pytest.fixture
def temp_dir():
    """Create temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    # Clean up after test
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_data():
    """Create test data for the converter."""
    # Sample test data
    return {
        "1": {
            "title": "Test Article 1",
            "sentences": [
                {
                    "text": "This is a test sentence with entities.",
                    "entities": ["test", "entities"],
                    "entity_spans": [[10, 14], [25, 33]],
                },
                {
                    "text": "Another sentence for testing.",
                    "entities": ["testing"],
                    "entity_spans": [[20, 27]],
                },
            ],
        },
        "2": {
            "title": "Test Article 2",
            "sentences": [
                {
                    "text": "Medical entity recognition test.",
                    "entities": ["Medical entity", "recognition"],
                    "entity_spans": [[0, 14], [15, 26]],
                }
            ],
        },
    }


@pytest.fixture
def test_json_file(temp_dir, test_data):
    """Create a test JSON file with sample data."""
    json_file = temp_dir / "test_articles.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(test_data, f)
    return json_file
