import json
import os
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    # Clean up after test
    shutil.rmtree(temp_dir)


@pytest.fixture
def named_output_dir(temp_dir: Path, request) -> Path:
    """Create named output directory within temp_dir."""
    name = getattr(request, "param", "output")
    output_dir = temp_dir / name
    output_dir.mkdir(exist_ok=True)
    return output_dir


@pytest.fixture
def example_files_dir() -> Path:
    """Return the directory containing the example files."""
    base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    return base_dir / "example"


@pytest.fixture
def output_dir(temp_dir: Path) -> Path:
    """Create output directory within temp_dir."""
    output_dir = temp_dir / "output"
    output_dir.mkdir(exist_ok=True)
    return output_dir


@pytest.fixture
def db_path(output_dir: Path) -> Path:
    """Create a standard database path for tests."""
    return output_dir / "test.db"


@pytest.fixture
def example_json_file(example_files_dir) -> Path:
    """Return the path to the example JSON file."""
    json_file = example_files_dir / "example_pubmed_articles.json"
    if not json_file.exists():
        pytest.skip(f"Example JSON file {json_file} not found")
    return json_file


@pytest.fixture
def example_articles_csv(example_files_dir):
    """Return the path to the expected articles output CSV."""
    csv_file = example_files_dir / "pubmed_articles.csv"
    if not csv_file.exists():
        pytest.skip(f"Expected articles CSV file {csv_file} not found")
    return csv_file


@pytest.fixture
def example_sentences_csv(example_files_dir) -> Optional[Path]:
    """Return the path to the expected sentences output CSV."""
    csv_file = example_files_dir / "pubmed_sentences.csv"
    if not csv_file.exists():
        pytest.skip(f"Expected sentences CSV file {csv_file} not found")
    return csv_file


@pytest.fixture
def example_entities_csv(example_files_dir):
    """Return the path to the expected entities output CSV."""
    csv_file = example_files_dir / "pubmed_entities.csv"
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
def test_data_1():
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
def test_data_2():
    """Create additional test data for the converter."""
    # Sample test data
    return {
        "3": {
            "title": "Test Article 3",
            "sentences": [
                {
                    "text": "This is a different test sentence.",
                    "entities": ["different"],
                    "entity_spans": [[10, 19]],
                }
            ],
        },
        "4": {
            "title": "Test Article 4",
            "sentences": [
                {
                    "text": "Another test for entity recognition.",
                    "entities": ["entity recognition"],
                    "entity_spans": [[10, 30]],
                }
            ],
        },
    }


@pytest.fixture
def combined_test_data(test_data_1, test_data_2):
    """Combine two sets of test data."""
    combined_data = {**test_data_1, **test_data_2}
    return combined_data


@pytest.fixture
def test_json_file_1(temp_dir, test_data_1):
    """Create a test JSON file with sample data."""
    json_file = temp_dir / "test_articles.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(test_data_1, f)
    return json_file


@pytest.fixture
def test_json_file_2(temp_dir, test_data_2):
    """Create a second test JSON file with sample data."""
    json_file = temp_dir / "test_articles_2.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(test_data_2, f)
    return json_file


@pytest.fixture
def test_json_file_combined(temp_dir, combined_test_data):
    """Create a combined test JSON file with sample data."""
    json_file = temp_dir / "test_articles_combined.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(combined_test_data, f)
    return json_file
