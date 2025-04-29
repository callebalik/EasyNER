import pytest
import torch
from unittest.mock import patch, MagicMock, PropertyMock


from easyner.pipeline.ner.transformer_based.ner_biobert import NER_biobert
from datasets import Dataset


class TestNERBiobertCaching:
    @pytest.fixture
    def mock_cuda_setup(self):
        """Setup mocks for CUDA and related functionality"""
        with (
            patch("torch.cuda.is_available", return_value=True),
            patch("torch.cuda.device_count", return_value=1),
            patch("torch.cuda.empty_cache") as mock_empty_cache,
        ):
            yield {"empty_cache": mock_empty_cache}

    @pytest.fixture
    def mock_pipeline(self):
        """Mock the HuggingFace pipeline"""
        with patch("transformers.pipeline") as mock_pipeline:
            # Configure mock pipeline to return a callable that returns empty predictions
            pipeline_instance = MagicMock()
            pipeline_instance.return_value = [{}]
            mock_pipeline.return_value = pipeline_instance

            yield mock_pipeline

    @pytest.fixture
    def mock_tokenizer_model(self):
        """Mock AutoTokenizer and AutoModelForTokenClassification"""
        with (
            patch(
                "transformers.AutoTokenizer.from_pretrained"
            ) as mock_tokenizer,
            patch(
                "transformers.AutoModelForTokenClassification.from_pretrained"
            ) as mock_model,
        ):
            # Create a more sophisticated mock that simulates being a PyTorch model
            mock_model_instance = MagicMock()
            mock_model_instance.__class__.__module__ = (
                "torch.nn.modules.module"
            )
            mock_model_instance.__class__.__name__ = "PreTrainedModel"
            mock_model_instance.device = torch.device("cuda:0")
            mock_model_instance.eval = MagicMock(
                return_value=mock_model_instance
            )

            # Make sure hf_device_map is explicitly set to None
            type(mock_model_instance).hf_device_map = PropertyMock(
                return_value=None
            )

            # Setup return values
            mock_tokenizer.return_value = MagicMock()
            mock_model.return_value = mock_model_instance

            yield {"tokenizer": mock_tokenizer, "model": mock_model}

    @pytest.fixture
    def mock_calculate_optimal_batch_size(self):
        """Mock the calculate_optimal_batch_size function"""
        # First ensure the utils module is imported
        with patch(
            "easyner.pipeline.ner.transformer_based.ner_biobert.calculate_optimal_batch_size",
            create=True,
        ) as mock_calc:
            mock_calc.return_value = 32
            yield mock_calc

    @pytest.fixture
    def mock_get_device_int(self):
        """Mock the get_device_int function to return a proper device"""
        with patch(
            "easyner.pipeline.ner.utils.get_device_int"
        ) as mock_get_device:
            # Return an integer that can be used as a device index
            mock_get_device.return_value = 0
            yield mock_get_device

    @pytest.fixture
    def sample_dataset(self):
        """Create a simple dataset for testing"""
        data = {
            "pmid": ["123", "123", "456"],
            "sent_idx": [0, 1, 0],
            "text": ["This is test 1.", "This is test 2.", "Another test."],
        }
        return Dataset.from_dict(data)

    @pytest.fixture
    def sample_dataset_similar(self):
        """Create a similar dataset with same characteristics"""
        data = {
            "pmid": ["789", "789"],
            "sent_idx": [0, 1],
            "text": ["Similar test 1.", "Similar test 2."],
        }
        return Dataset.from_dict(data)

    def test_batch_size_calculation_called_once(
        self,
        mock_cuda_setup,
        mock_pipeline,
        mock_tokenizer_model,
        mock_get_device_int,
        sample_dataset,
        monkeypatch,
    ):
        """Test that batch size calculation is only done once"""
        # Create a mock for the calculation function
        mock_calculate_optimal_batch_size = MagicMock(return_value=32)

        # Patch the import inside the method
        monkeypatch.setattr(
            "easyner.pipeline.ner.utils.calculate_optimal_batch_size",
            mock_calculate_optimal_batch_size,
        )

        # Create NER_biobert instance with integer device
        ner = NER_biobert(
            model_dir="dummy_dir", model_name="dummy_model", device=0
        )

        # Call predict_dataset multiple times
        ner.predict_dataset(sample_dataset)
        ner.predict_dataset(sample_dataset)
        ner.predict_dataset(sample_dataset)

        # Verify that calculate_optimal_batch_size was called exactly once
        assert mock_calculate_optimal_batch_size.call_count == 1

    def test_batch_size_cache_reused_between_instances(
        self,
        mock_cuda_setup,
        mock_pipeline,
        mock_tokenizer_model,
        mock_get_device_int,
        sample_dataset,
        monkeypatch,
    ):
        """Test that batch sizes are cached between NER_biobert instances"""
        # Create a mock for the calculation function
        mock_calculate_optimal_batch_size = MagicMock(return_value=32)

        # Patch the import inside the method
        monkeypatch.setattr(
            "easyner.pipeline.ner.utils.calculate_optimal_batch_size",
            mock_calculate_optimal_batch_size,
        )

        # Create two NER_biobert instances and call predict_dataset on each
        ner1 = NER_biobert(
            model_dir="dummy_dir", model_name="dummy_model", device=0
        )
        ner1.predict_dataset(sample_dataset)

        # Check the call count before second instance
        first_call_count = mock_calculate_optimal_batch_size.call_count
        assert first_call_count == 1

        # Create second instance
        ner2 = NER_biobert(
            model_dir="dummy_dir", model_name="dummy_model", device=0
        )
        ner2.predict_dataset(sample_dataset)

        # Verify calculate_optimal_batch_size was NOT called again
        assert mock_calculate_optimal_batch_size.call_count == first_call_count

    def test_different_dataset_sizes_different_cache_keys(
        self,
        mock_cuda_setup,
        mock_pipeline,
        mock_tokenizer_model,
        mock_get_device_int,
        sample_dataset,
        sample_dataset_similar,
        monkeypatch,
    ):
        """Test that different dataset sizes result in different cache entries"""
        # Create a mock for the calculation function
        mock_calculate_optimal_batch_size = MagicMock(return_value=32)

        # Patch the import inside the method
        monkeypatch.setattr(
            "easyner.pipeline.ner.utils.calculate_optimal_batch_size",
            mock_calculate_optimal_batch_size,
        )

        # Create NER_biobert instance
        ner = NER_biobert(
            model_dir="dummy_dir", model_name="dummy_model", device=0
        )

        # Process first dataset
        ner.predict_dataset(sample_dataset)

        # Process second dataset with different size
        ner.predict_dataset(sample_dataset_similar)

        # Verify calculate_optimal_batch_size was called twice (once for each dataset size)
        assert mock_calculate_optimal_batch_size.call_count == 2

    def test_full_ner_pipeline_data_flow(
        self,
        mock_cuda_setup,
        mock_tokenizer_model,
        mock_get_device_int,
        sample_dataset,
        monkeypatch,
    ):
        """Test the complete data flow through the NER pipeline with mocked results."""

        # Define mock predictions with the expected structure
        mock_predictions = [
            [  # Predictions for the first text
                {
                    "entity": "TEST",
                    "score": 0.98,
                    "word": "test",
                    "start": 8,
                    "end": 12,
                },
            ],
            [  # Predictions for the second text
                {
                    "entity": "TEST",
                    "score": 0.95,
                    "word": "test",
                    "start": 8,
                    "end": 12,
                },
                {
                    "entity": "NUM",
                    "score": 0.99,
                    "word": "2",
                    "start": 14,
                    "end": 15,
                },
            ],
            [  # Predictions for the third text
                {
                    "entity": "TEST",
                    "score": 0.97,
                    "word": "test",
                    "start": 8,
                    "end": 12,
                },
            ],
        ]

        # Create a mock pipeline function that returns our predictions
        def mock_pipeline_func(**kwargs):
            mock_pipe = MagicMock()
            mock_pipe.return_value = mock_predictions
            return mock_pipe

        # Patch the pipeline function to use our mock
        monkeypatch.setattr("transformers.pipeline", mock_pipeline_func)

        # Create a mock for the calculation function
        mock_calculate_optimal_batch_size = MagicMock(return_value=32)

        # Patch the import inside the method
        monkeypatch.setattr(
            "easyner.pipeline.ner.utils.calculate_optimal_batch_size",
            mock_calculate_optimal_batch_size,
        )

        # Create NER_biobert instance
        ner = NER_biobert(
            model_dir="dummy_dir", model_name="dummy_model", device=0
        )

        # Process the dataset
        result_dataset = ner.predict_dataset(sample_dataset)

        # Verify the dataset contains predictions column with correct structure
        assert "prediction" in result_dataset.column_names
        assert len(result_dataset["prediction"]) == len(mock_predictions)

        # Create a cache key based on device and dataset size
        cache_key = f"0_{len(sample_dataset)//1000}k"

        # Verify the cache contains our batch size
        assert cache_key in NER_biobert._optimal_batch_size_cache
        assert NER_biobert._optimal_batch_size_cache[cache_key] == 32
