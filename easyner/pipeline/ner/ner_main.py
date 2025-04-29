# coding=utf-8

from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import torch
from glob import glob
from typing import List, Dict, Any, Optional, Union, Tuple, Iterator
from abc import ABC, abstractmethod
from tqdm import tqdm

from easyner.io.utils import extract_batch_index
from easyner.io.handlers import JsonHandler
from easyner import util


class NERProcessor(ABC):
    """Abstract base class for NER processors."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the NER processor.

        Parameters:
        -----------
        config: Dict[str, Any]
            Configuration settings for the processor
        """
        self.config = config
        self.output_template = (
            "{output_path}/{output_file_prefix}-{batch_index}.json"
        )

    @abstractmethod
    def process_dataset(
        self, input_files: List[str], device: Any = None
    ) -> None:
        """
        Process the entire dataset of files with named entity recognition.

        Parameters:
        -----------
        input_files: List[str]
            List of input file paths to process
        device: Any, optional
            Device to use for processing
        """
        pass

    def _build_output_filepath(self, batch_index: int) -> str:
        """
        Generate the output file path based on the configuration and batch index.

        Parameters:
        -----------
        batch_index: int
            The batch index to include in the filename

        Returns:
        --------
        str: The formatted output file path
        """
        return self.output_template.format(
            output_path=self.config["output_path"],
            output_file_prefix=self.config["output_file_prefix"],
            batch_index=batch_index,
        )

    def _read_batch_file(self, batch_file: str) -> Tuple[List[Dict], int]:
        """
        Read a batch file and extract its index.

        Parameters:
        -----------
        batch_file: str
            Path to the batch file to read

        Returns:
        --------
        Tuple[List[Dict], int]: Articles and batch index
        """
        articles = JsonHandler().read(batch_file)
        batch_index = extract_batch_index(batch_file)
        return articles, batch_index

    def _save_processed_articles(
        self, articles: List[Dict], batch_index: int
    ) -> None:
        """
        Save processed articles to the appropriate output file.

        Parameters:
        -----------
        articles: List[Dict]
            Processed articles to save
        batch_index: int
            Batch index for the output filename
        """
        output_file = self._build_output_filepath(batch_index)
        util.append_to_json_file(output_file, articles)


class SpacyNERProcessor(NERProcessor):
    """NER processor using SpaCy's phrase matcher."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Load spaCy model once for all processing
        self._initialize_model()

    def _initialize_model(self) -> None:
        """Initialize the spaCy model once for all processing."""
        # Implementation-specific initialization
        pass

    def process_dataset(
        self, input_files: List[str], device: Any = None
    ) -> None:
        """
        Process all files using SpaCy's phrase matcher.

        Parameters:
        -----------
        input_files: List[str]
            List of input file paths to process
        device: Any, optional
            Device to use for processing (not used for SpaCy)
        """
        from .dictionary_based.ner_spacy import (
            run_ner_with_spacy_phrasematcher,
        )

        # Process files sequentially or in parallel based on configuration
        if self.config.get("multiprocessing", False):
            self._process_files_in_parallel(input_files)
        else:
            for batch_file in tqdm(input_files, desc="Processing with SpaCy"):
                self._process_single_file(batch_file)

    def _process_single_file(self, batch_file: str) -> int:
        """Process a single file with SpaCy NER."""
        from .dictionary_based.ner_spacy import (
            run_ner_with_spacy_phrasematcher,
        )

        articles, batch_index = self._read_batch_file(batch_file)

        if not articles:
            self._save_processed_articles(articles, batch_index)
            return batch_index

        processed_articles = run_ner_with_spacy_phrasematcher(
            articles, self.config, batch_index
        )

        self._save_processed_articles(processed_articles, batch_index)
        return batch_index

    def _process_files_in_parallel(self, input_files: List[str]) -> None:
        """Process files in parallel using multiprocessing."""
        from multiprocessing import cpu_count

        cpu_limit = self.config.get("cpu_limit", 1)

        with ProcessPoolExecutor(min(cpu_limit, cpu_count())) as executor:
            futures = [
                executor.submit(self._process_single_file, batch_file)
                for batch_file in input_files
            ]

            for i, future in enumerate(as_completed(futures)):
                batch_index = future.result()
                print(
                    f"Completed SpaCy batch {batch_index} ({i+1}/{len(futures)})"
                )


class BioBertNERProcessor(NERProcessor):
    """NER processor using BioBert fine-tuned model."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Load BioBERT model once for all processing
        self._model = None

    def _initialize_model(self, device: Any) -> None:
        """Initialize the BioBERT model once for all processing."""
        # Implementation-specific model loading
        pass

    def process_dataset(
        self, input_files: List[str], device: Any = None
    ) -> None:
        """
        Process all files using BioBERT.

        Parameters:
        -----------
        input_files: List[str]
            List of input file paths to process
        device: Any, optional
            Device to use for processing
        """
        if device is None:
            device = torch.device(0 if torch.cuda.is_available() else "cpu")

        # Initialize model once for all processing
        self._initialize_model(device)

        # BioBERT often benefits from batched processing across files
        if self.config.get("cross_file_batching", False):
            self._process_with_cross_file_batching(input_files, device)
        elif self.config.get("multiprocessing", False):
            # For multi-GPU setups
            self._process_files_in_parallel(input_files)
        else:
            # Process sequentially
            for batch_file in tqdm(
                input_files, desc="Processing with BioBERT"
            ):
                self._process_single_file(batch_file, device)

    def _process_single_file(self, batch_file: str, device: Any) -> int:
        """Process a single file with BioBERT NER."""
        from .transformer_based.ner_biobert import (
            run_ner_with_biobert_finetuned,
        )

        articles, batch_index = self._read_batch_file(batch_file)

        if not articles:
            self._save_processed_articles(articles, batch_index)
            return batch_index

        processed_articles = run_ner_with_biobert_finetuned(
            articles, self.config, batch_index, device
        )

        self._save_processed_articles(processed_articles, batch_index)
        return batch_index

    def _process_with_cross_file_batching(
        self, input_files: List[str], device: Any
    ) -> None:
        """Process with optimal batching across files."""
        # Implementation for cross-file batching strategy
        # This would combine articles from multiple files to create optimally-sized batches
        # for transformer processing
        pass

    def _process_files_in_parallel(self, input_files: List[str]) -> None:
        """Process files in parallel using multiprocessing."""
        from multiprocessing import cpu_count

        cpu_limit = self.config.get("cpu_limit", 1)

        with ProcessPoolExecutor(min(cpu_limit, cpu_count())) as executor:
            futures = []

            # Create a separate device for each worker if multiple GPUs are available
            for i, batch_file in enumerate(input_files):
                device_id = (
                    i % torch.cuda.device_count()
                    if torch.cuda.is_available()
                    else "cpu"
                )
                device = (
                    torch.device(device_id)
                    if isinstance(device_id, int)
                    else device_id
                )
                futures.append(
                    executor.submit(
                        self._process_single_file, batch_file, device
                    )
                )

            for i, future in enumerate(as_completed(futures)):
                batch_index = future.result()
                print(
                    f"Completed BioBERT batch {batch_index} ({i+1}/{len(futures)})"
                )


class NERProcessorFactory:
    """Factory class for creating appropriate NER processors."""

    @staticmethod
    def create_processor(config: Dict[str, Any]) -> NERProcessor:
        """
        Create an appropriate NER processor based on configuration.

        Parameters:
        -----------
        config: Dict[str, Any]
            Configuration for NER processing

        Returns:
        --------
        NERProcessor: The appropriate processor instance
        """
        model_type = config.get("model_type", "")

        if model_type == "spacy_phrasematcher":
            return SpacyNERProcessor(config)
        elif model_type == "biobert_finetuned":
            return BioBertNERProcessor(config)
        else:
            raise ValueError(
                f"Unknown model type: {model_type}. "
                "Supported types are 'spacy_phrasematcher' and 'biobert_finetuned'."
            )


class NERPipeline:
    """Main class for the NER pipeline that handles processing workflow."""

    def __init__(self, config: Dict[str, Any], cpu_limit: int = 1):
        """
        Initialize the NER pipeline.

        Parameters:
        -----------
        config: Dict[str, Any]
            Configuration for NER processing
        cpu_limit: int
            Maximum number of CPUs to use for multiprocessing
        """
        self.config = config
        self.config["cpu_limit"] = cpu_limit

        # Create the appropriate processor using the factory
        self.processor = NERProcessorFactory.create_processor(self.config)

    def _get_input_files_sorted(self) -> List[str]:
        """
        Find and sort input files based on configuration.

        Returns:
        --------
        List[str]: Sorted list of input files to process
        """
        input_file_list = sorted(
            glob(f'{self.config["input_path"]}*.json'),
            key=lambda x: int(
                os.path.splitext(os.path.basename(x))[0].split("-")[-1]
            ),
        )

        # Apply file range filtering if configured
        if "article_limit" in self.config and isinstance(
            self.config["article_limit"], list
        ):
            from easyner.io.utils import filter_files

            start = self.config["article_limit"][0]
            end = self.config["article_limit"][1]

            input_file_list = filter_files(input_file_list, start, end)
            print(f"Processing articles in range {start} to {end}")

        return input_file_list

    def run(self) -> None:
        """
        Main entry point for the NER pipeline.
        - Sets up output directory
        - Discovers and filters files
        - Delegates processing to the appropriate processor
        """
        print("----Starting NER pipeline----")

        # Set up output directory
        if self.config.get("clear_old_results", True):
            from easyner.io.utils import _remove_all_files_from_dir

            _remove_all_files_from_dir(self.config["output_path"])
        else:
            os.makedirs(self.config["output_path"], exist_ok=True)

        # Get sorted input files
        input_file_list = self._get_input_files_sorted()

        # Let the processor handle the dataset in the most appropriate way
        device = torch.device(0 if torch.cuda.is_available() else "cpu")
        self.processor.process_dataset(input_file_list, device)

        print("----NER pipeline processing complete----")


# For backward compatibility
def run_ner_module(ner_config: Dict[str, Any], cpu_limit: int) -> None:
    """
    Legacy entry point for the NER pipeline.

    Parameters:
    -----------
    ner_config: Dict[str, Any]
        Configuration for NER processing
    cpu_limit: int
        Maximum number of CPUs to use for multiprocessing
    """
    pipeline = NERPipeline(ner_config, cpu_limit)
    pipeline.run()


if __name__ == "__main__":
    pass
