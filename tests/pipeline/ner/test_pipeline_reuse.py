import pytest
from unittest.mock import patch, MagicMock
import json

from easyner.pipeline.ner.ner_main import run_ner_module, process_batch_file


class TestPipelineReuse:
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
    def mock_biobert_class(self):
        """Mock the NER_biobert class"""
        with patch(
            "easyner.pipeline.ner.transformer_based.ner_biobert.NER_biobert"
        ) as mock_class:
            # Configure the mock class
            instance = MagicMock()
            instance.predict_dataset.return_value = MagicMock()
            mock_class.return_value = instance

            yield mock_class

    @pytest.fixture
    def sample_batch_files(self, tmp_path):
        """Create sample batch files for testing"""
        # Create test directory if it doesn't exist
        test_dir = tmp_path / "test_input"
        test_dir.mkdir(exist_ok=True)
        output_dir = tmp_path / "test_output"
        output_dir.mkdir(exist_ok=True)

        # Create 3 sample batch files
        batch_files = []
        for i in range(3):
            articles = {
                f"PMC12345{i}": {
                    "sentences": [
                        {"text": f"This is a test sentence {i}-1."},
                        {"text": f"This is another test sentence {i}-2."},
                    ]
                }
            }

            filename = test_dir / f"batch-{i}.json"
            with open(filename, "w") as f:
                json.dump(articles, f)

            batch_files.append(str(filename))

        return {
            "batch_files": batch_files,
            "input_dir": str(test_dir),
            "output_dir": str(output_dir),
        }

    def test_sequential_processing_creates_one_pipeline(
        self,
        mock_cuda_setup,
        mock_biobert_class,
        sample_batch_files,
        monkeypatch,
    ):
        """Test that sequential processing creates only one NER pipeline instance"""
        # Configure ner_config
        ner_config = {
            "model_type": "biobert_finetuned",
            "model_folder": "/dummy/model/folder",
            "model_name": "dummy_model",
            "input_path": sample_batch_files["input_dir"] + "/",
            "output_path": sample_batch_files["output_dir"],
            "output_file_prefix": "test_output",
            "clear_old_results": True,
            "multiprocessing": False,  # Sequential processing
        }

        # Mock the glob function to return our sample files
        def mock_glob(pattern):
            return sample_batch_files["batch_files"]

        monkeypatch.setattr("easyner.pipeline.ner.ner_main.glob", mock_glob)

        # Mock append_to_json_file to avoid actual file writing
        monkeypatch.setattr(
            "easyner.util.append_to_json_file", lambda *args: None
        )

        # Run the function
        run_ner_module(ner_config, 1)

        # Verify that NER_biobert constructor was called exactly once
        mock_biobert_class.assert_called_once()

        # Verify that predict_dataset was called for each batch (3 times)
        assert mock_biobert_class.return_value.predict_dataset.call_count == 3

    def test_parallel_processing_creates_multiple_pipelines(
        self,
        mock_cuda_setup,
        mock_biobert_class,
        sample_batch_files,
        monkeypatch,
    ):
        """Test that parallel processing creates multiple NER pipeline instances (one per process)"""
        # Configure ner_config
        ner_config = {
            "model_type": "biobert_finetuned",
            "model_folder": "/dummy/model/folder",
            "model_name": "dummy_model",
            "input_path": sample_batch_files["input_dir"] + "/",
            "output_path": sample_batch_files["output_dir"],
            "output_file_prefix": "test_output",
            "clear_old_results": True,
            "multiprocessing": True,  # Parallel processing
        }

        # Mock the concurrent.futures.ProcessPoolExecutor
        mock_executor = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = 0
        mock_executor.__enter__.return_value.submit.return_value = mock_future

        with patch(
            "easyner.pipeline.ner.ner_main.ProcessPoolExecutor",
            return_value=mock_executor,
        ):
            # Mock the glob function to return our sample files
            def mock_glob(pattern):
                return sample_batch_files["batch_files"]

            monkeypatch.setattr(
                "easyner.pipeline.ner.ner_main.glob", mock_glob
            )
            monkeypatch.setattr(
                "easyner.pipeline.ner.ner_main.as_completed",
                lambda x: [mock_future],
            )

            # Mock append_to_json_file to avoid actual file writing
            monkeypatch.setattr(
                "easyner.util.append_to_json_file", lambda *args: None
            )

            # Run the function
            run_ner_module(ner_config, 3)

            # In parallel mode, process_batch_file is called within submit() for each batch
            # Verify that executor.submit was called for each batch
            assert mock_executor.__enter__.return_value.submit.call_count == 3

        # Now test a single process_batch_file call directly to verify it creates its own pipeline
        # Reset the mock
        mock_biobert_class.reset_mock()

        # Call process_batch_file directly
        process_batch_file(ner_config, sample_batch_files["batch_files"][0])

        # Verify that NER_biobert constructor was called exactly once for this process
        mock_biobert_class.assert_called_once()

    def test_process_batch_file_with_shared_session(
        self,
        mock_cuda_setup,
        mock_biobert_class,
        sample_batch_files,
        monkeypatch,
    ):
        """Test that process_batch_file uses a shared NER session when provided"""
        # Configure ner_config
        ner_config = {
            "model_type": "biobert_finetuned",
            "model_folder": "/dummy/model/folder",
            "model_name": "dummy_model",
            "output_path": sample_batch_files["output_dir"],
            "output_file_prefix": "test_output",
        }

        # Create a mock shared session
        shared_session = MagicMock()
        shared_session.predict_dataset.return_value = MagicMock()

        # Mock open to avoid actual file reading
        mock_open = MagicMock()
        mock_open.return_value.__enter__.return_value.read.return_value = (
            '{"PMC123": {"sentences": [{"text": "Test sentence."}]}}'
        )

        with patch("builtins.open", mock_open):
            # Mock json.loads to return a sample article
            monkeypatch.setattr(
                "json.loads",
                lambda x: {
                    "PMC123": {"sentences": [{"text": "Test sentence."}]}
                },
            )

            # Mock append_to_json_file to avoid actual file writing
            monkeypatch.setattr(
                "easyner.util.append_to_json_file", lambda *args: None
            )

            # Call process_batch_file with the shared session
            process_batch_file(
                ner_config,
                sample_batch_files["batch_files"][0],
                device="cuda:0",
                shared_ner_session=shared_session,
            )

            # Verify that NER_biobert constructor was NOT called (should use shared session)
            mock_biobert_class.assert_not_called()

            # Verify that the shared session's predict_dataset was called
            shared_session.predict_dataset.assert_called_once()
