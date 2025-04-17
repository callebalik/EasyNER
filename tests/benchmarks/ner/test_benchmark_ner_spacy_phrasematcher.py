import pytest
from easyner.pipeline.ner.ner_spacy_phrasematcher import SpacyPhraseMatcher


@pytest.fixture(scope="session")
def spacy_ner():
    # Adjust these parameters as needed
    model_name = "en_core_web_sm"
    vocab_path = "tests/data/vocab.txt"
    entity_type = "CHEMICAL"
    return SpacyPhraseMatcher(model_name, vocab_path, entity_type)


def test_benchmark_spacy_phrasematcher(benchmark, spacy_ner):
    text = "Aspirin and ibuprofen are common painkillers."
    benchmark(spacy_ner.predict, text)


@pytest.mark.parametrize(
    "text",
    [
        "SARS-CoV-2 infection leads to COVID-19 disease.",
        "Patients treated with remdesivir showed improved outcomes.",
        "ACE2 receptor expression facilitates viral entry into cells.",
        "The study used DMEM supplemented with FBS for cell culture.",
    ],
)
def test_benchmark_spacy_varied_inputs(benchmark, spacy_ner, text):
    """Benchmark SpacyPhraseMatcher with different input texts."""
    benchmark(spacy_ner.predict, text)


@pytest.mark.parametrize(
    "text_length",
    [
        50,  # Short sentence
        200,  # Medium sentence
        500,  # Long sentence
        1000,  # Very long sentence
    ],
)
def test_benchmark_spacy_scaling(benchmark, spacy_ner, text_length):
    """Benchmark how SpacyPhraseMatcher performance scales with input length."""
    text = "Aspirin " * (text_length // 8)  # Each token ~8 chars with space
    benchmark(spacy_ner.predict, text)
