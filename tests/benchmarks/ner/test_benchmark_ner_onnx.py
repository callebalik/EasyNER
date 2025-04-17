import pytest
import os
from unittest.mock import patch, MagicMock
from easyner.pipeline.ner.ner_inference import BioBERTONNXInference
from easyner.pipeline.ner.exceptions import ModelLoadingError


def is_onnx_model_available(
    model_dir="models/", model_name="biobert_ner.onnx"
):
    """Check if the ONNX model is available for benchmarking."""
    model_path = os.path.join(model_dir, model_name)
    return os.path.exists(model_path)


@pytest.fixture(scope="session")
def onnx_ner():
    """
    Create an ONNX NER instance for benchmarking.

    Will use a real model if available, otherwise will use a mock.
    For CI/CD environments, we'll typically use mocks.
    """
    model_dir = "models/"
    model_name = "biobert_ner.onnx"
    model_vocab = os.path.join(model_dir, "vocab.txt")
    labels = ["[PAD]", "B", "I", "O", "X", "[CLS]", "[SEP]"]

    # Ensure the model directory exists
    os.makedirs(model_dir, exist_ok=True)

    # Create a minimal vocab file if it doesn't exist
    if not os.path.exists(model_vocab):
        with open(model_vocab, "w") as f:
            f.write("[PAD]\n[UNK]\n[CLS]\n[SEP]\n[MASK]\n")

    # Set up a more robust mock if the model doesn't exist
    if not is_onnx_model_available(model_dir, model_name):
        # Create a mock that mimics the BioBERTONNXInference class
        mock_onnx = MagicMock(spec=BioBERTONNXInference)
        mock_onnx.predict = MagicMock(
            return_value=[{"word": "COVID-19", "start": 13, "end": 21}]
        )
        mock_onnx.get_token_labels = MagicMock(
            return_value=[
                ("[CLS]", "[CLS]"),
                ("COVID", "B"),
                ("-", "I"),
                ("19", "I"),
                ("[SEP]", "[SEP]"),
            ]
        )
        mock_onnx.encode_sequence = MagicMock(
            return_value={
                "tokens": ["[CLS]", "COVID", "-", "19", "[SEP]"],
                "token_type_ids": MagicMock(),
                "attention_mask": MagicMock(),
                "input_ids": MagicMock(),
                "label_ids": MagicMock(),
            }
        )

        # Use session.run as it's called in profiling
        mock_session = MagicMock()
        mock_session.run = MagicMock(
            return_value=(None, [[0, 1, 2, 3, 0]], None)
        )
        mock_onnx._session = mock_session

        # Mock the tokenizer
        mock_tokenizer = MagicMock()
        mock_tokenizer.encode_plus = MagicMock(
            return_value={
                "input_ids": MagicMock(),
                "token_type_ids": MagicMock(),
                "attention_mask": MagicMock(),
            }
        )
        mock_onnx._tokenizer = mock_tokenizer
        mock_onnx._labels = labels

        return mock_onnx
    else:
        # Try to load real model
        try:
            return BioBERTONNXInference(
                model_dir=model_dir,
                model_name=model_name,
                model_vocab=model_vocab,
                labels=labels,
            )
        except (ModelLoadingError, Exception) as e:
            pytest.skip(f"ONNX model failed to load: {str(e)}")


@pytest.mark.skipif(
    not is_onnx_model_available(),
    reason="ONNX model not available, using mock instead",
)
def test_benchmark_onnx_real(benchmark, onnx_ner):
    """Benchmark ONNX NER with real model if available."""
    text = "Patients with COVID-19 often experience respiratory symptoms."
    benchmark(onnx_ner.predict, text)


def test_benchmark_onnx(benchmark, onnx_ner):
    """Benchmark ONNX NER prediction on a sample text."""
    text = "Patients with COVID-19 often experience respiratory symptoms."
    benchmark(onnx_ner.predict, text)


@pytest.mark.parametrize(
    "text",
    [
        "Aspirin and ibuprofen are common painkillers.",
        "SARS-CoV-2 infection leads to COVID-19 disease.",
        "Patients treated with remdesivir showed improved outcomes.",
        "ACE2 receptor expression facilitates viral entry into cells.",
    ],
)
def test_benchmark_onnx_varied_inputs(benchmark, onnx_ner, text):
    """Benchmark ONNX NER with different input texts to see variance."""
    benchmark(onnx_ner.predict, text)


@pytest.mark.parametrize(
    "text_length",
    [
        50,  # Short sentence
        200,  # Medium sentence
        500,  # Long sentence
        1000,  # Very long sentence
    ],
)
def test_benchmark_onnx_scaling(benchmark, onnx_ner, text_length):
    """Benchmark how ONNX performance scales with input length."""
    # Generate text of specified length
    text = "COVID-19 " * (text_length // 9)  # Each token ~9 chars with space
    benchmark(onnx_ner.predict, text)
