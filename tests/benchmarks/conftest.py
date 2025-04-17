"""Pytest configuration for benchmarks."""

import os
import pytest
import json
from pathlib import Path


@pytest.fixture(scope="session")
def benchmark_data_dir():
    """Return the path to the benchmark data directory."""
    base_dir = Path(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    data_dir = base_dir / "tests" / "data" / "benchmarks"
    data_dir.mkdir(exist_ok=True, parents=True)
    return data_dir


@pytest.fixture(scope="session")
def benchmark_output_dir():
    """Return the path to the benchmark output directory."""
    base_dir = Path(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    output_dir = base_dir / "results" / "benchmarks"
    output_dir.mkdir(exist_ok=True, parents=True)
    return output_dir


@pytest.fixture(scope="session")
def ensure_vocab_file():
    """Ensure the test vocabulary file exists."""
    base_dir = Path(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    vocab_dir = base_dir / "tests" / "data"
    vocab_dir.mkdir(exist_ok=True, parents=True)
    vocab_path = vocab_dir / "vocab.txt"

    if not vocab_path.exists():
        with open(vocab_path, "w") as f:
            f.write(
                "COVID-19\nSARS-CoV-2\nACE2\nDMEM\nFBS\nNO\naspirin\nibuprofen\nremdesivir\n"
            )

    return str(vocab_path)
