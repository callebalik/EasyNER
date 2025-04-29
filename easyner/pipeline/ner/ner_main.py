# coding=utf-8

from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import torch
from glob import glob
from typing import List, Dict, Any, Optional, Union
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
    def process_articles(
        self, articles: List[Dict], batch_index: int, device: Any = None
    ) -> List[Dict]:
        """
        Process articles with named entity recognition.

        Parameters:
        -----------
        articles: List[Dict]
            List of article dictionaries to process
        batch_index: int
            The batch index of the current file
        device: Any, optional
            Device to use for processing

        Returns:
        --------
        List[Dict]: Processed articles with NER results
        """
        pass

    def process_batch_file(self, batch_file: str, device: Any = None) -> int:
        """
        Process a single batch file with NER.

        Parameters:
        -----------
        batch_file: str
            Path to the batch file to process
        device: Any, optional
            Device to use for processing

        Returns:
        --------
        int: The batch index of the processed file
        """
        # Read articles
        articles = JsonHandler().read(batch_file)

        # Extract batch index from filename
        batch_index = extract_batch_index(batch_file)

        # Prepare output file path
        output_file = self._build_output_filepath(batch_index)

        # Handle empty articles case
        if len(articles) == 0:
            util.append_to_json_file(output_file, articles)
            return batch_index

        # Process articles with the specific NER implementation
        processed_articles = self.process_articles(
            articles, batch_index, device
        )

        # Save results to output file
        util.append_to_json_file(output_file, processed_articles)
        return batch_index

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


class SpacyNERProcessor(NERProcessor):
    """NER processor using SpaCy's phrase matcher."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Any SpaCy-specific initialization can go here

    def process_articles(
        self, articles: List[Dict], batch_index: int, device: Any = None
    ) -> List[Dict]:
        from .dictionary_based.ner_spacy import (
            run_ner_with_spacy_phrasematcher,
        )

        return run_ner_with_spacy_phrasematcher(
            articles, self.config, batch_index
        )


class BioBertNERProcessor(NERProcessor):
    """NER processor using BioBert fine-tuned model."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Any BioBert-specific initialization can go here

    def process_articles(
        self, articles: List[Dict], batch_index: int, device: Any = None
    ) -> List[Dict]:
        from .transformer_based.ner_biobert import (
            run_ner_with_biobert_finetuned,
        )

        return run_ner_with_biobert_finetuned(
            articles, self.config, batch_index, device
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
        self.cpu_limit = cpu_limit

        # Create the appropriate processor based on model_type
        self._create_processor()

    def _create_processor(self) -> None:
        """Create the appropriate NER processor based on configuration."""
        model_type = self.config.get("model_type", "")

        if model_type == "spacy_phrasematcher":
            self.processor = SpacyNERProcessor(self.config)
        elif model_type == "biobert_finetuned":
            self.processor = BioBertNERProcessor(self.config)
        else:
            raise ValueError(
                f"Unknown model type: {model_type}. "
                "Supported types are 'spacy_phrasematcher' and 'biobert_finetuned'."
            )

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

    def _process_files_in_parallel(self, input_file_list: List[str]) -> None:
        """
        Process multiple batch files in parallel using a process pool.

        Parameters:
        -----------
        input_file_list: List[str]
            List of files to process
        """
        from multiprocessing import cpu_count

        print(
            f"Processing files in parallel with {self.config['model_type']} using {self.cpu_limit} CPUs"
        )

        with ProcessPoolExecutor(min(self.cpu_limit, cpu_count())) as executor:
            futures = [
                executor.submit(self.processor.process_batch_file, batch_file)
                for batch_file in input_file_list
            ]

            # Process results as they complete
            for i, future in enumerate(as_completed(futures)):
                batch_index = future.result()
                print(f"Completed batch {batch_index} ({i+1}/{len(futures)})")

    def run(self) -> None:
        """
        Main entry point for the NER pipeline.
        - Sets up output directory
        - Discovers and filters files
        - Processes files in parallel or sequentially
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

        # Process files (in parallel or sequentially)
        if self.config["multiprocessing"]:
            self._process_files_in_parallel(input_file_list)
        else:
            device = torch.device(0 if torch.cuda.is_available() else "cpu")
            print(
                f"Processing files sequentially with {self.config['model_type']} on device: {device}"
            )

            for batch_file in tqdm(input_file_list, desc="Processing batches"):
                self.processor.process_batch_file(batch_file, device)

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
