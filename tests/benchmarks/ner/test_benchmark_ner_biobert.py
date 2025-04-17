import pytest
import os
from unittest.mock import patch, MagicMock
from easyner.pipeline.ner.ner_biobert import BioBERTNER
from easyner.pipeline.ner.exceptions import ModelLoadingError


def is_biobert_model_available(
    model_dir="models/biobert", model_name="test_model"
):
    """Check if the BioBERT model is available for benchmarking."""
    model_path = os.path.join(model_dir, model_name)
    return os.path.exists(model_path)


@pytest.fixture(scope="session")
def biobert_ner():
    """
    Create a BioBERT NER instance for benchmarking.

    Will use a real model if available, otherwise will use a mock.
    For CI/CD environments, we'll typically use mocks.
    """
    model_dir = "models/biobert"
    model_name = "test_model"

    # Check if model exists
    if is_biobert_model_available(model_dir, model_name):
        try:
            # Try to load real model
            model = BioBERTNER(
                model_dir=model_dir,
                model_name=model_name,
                model_max_length=192,
                device=-1,
            )
            return model
        except (ModelLoadingError, Exception) as e:
            pytest.skip(f"BioBERT model failed to load: {str(e)}")
    else:
        # Use a mock if model doesn't exist
        with (
            patch(
                "easyner.pipeline.ner.ner_biobert.AutoTokenizer", MagicMock()
            ),
            patch(
                "easyner.pipeline.ner.ner_biobert.AutoModelForTokenClassification",
                MagicMock(),
            ),
            patch("easyner.pipeline.ner.ner_biobert.pipeline", MagicMock()),
        ):
            model = BioBERTNER(
                model_dir=model_dir,
                model_name=model_name,
                model_max_length=192,
                device=-1,
            )
            # Mock the predict method
            model.predict = MagicMock(
                return_value=[
                    {
                        "word": "COVID-19",
                        "score": 0.99,
                        "entity": "DISEASE",
                        "start": 10,
                        "end": 18,
                    }
                ]
            )
            return model


@pytest.mark.skipif(
    not is_biobert_model_available(),
    reason="BioBERT model not available, using mock instead",
)
def test_benchmark_biobert_real(benchmark, biobert_ner):
    """Benchmark BioBERT NER with real model if available."""
    text = "Patients with COVID-19 often experience respiratory symptoms."
    benchmark(biobert_ner.predict, text)


def test_benchmark_biobert(benchmark, biobert_ner):
    """Benchmark BioBERT NER prediction on a sample text."""
    text = "Patients with COVID-19 often experience respiratory symptoms."
    benchmark(biobert_ner.predict, text)


@pytest.mark.parametrize(
    "text",
    [
        "Aspirin and ibuprofen are common painkillers.",
        "SARS-CoV-2 infection leads to COVID-19 disease.",
        "Patients treated with remdesivir showed improved outcomes.",
        "ACE2 receptor expression facilitates viral entry into cells.",
    ],
)
def test_benchmark_biobert_varied_inputs(benchmark, biobert_ner, text):
    """Benchmark BioBERT NER with different input texts to see variance."""
    benchmark(biobert_ner.predict, text)
