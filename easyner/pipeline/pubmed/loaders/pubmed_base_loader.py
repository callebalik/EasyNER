"""Base loader classes for PubMed XML processing.

This module provides an abstract base class for loading and processing PubMed XML files,
including file discovery, range filtering, and output handling.

TODO: optimize parsing. The absolute majority of time is spent in pubmed_parser
return pp.parse_medline_xml(input_file, year_info_only=False)
reports 70% of the time as python code
"""

import multiprocessing
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Tuple, Union

import pubmed_parser as pp
from tqdm import tqdm

from easyner.pipeline.pubmed.utils import _resolve_path


class BasePubMedLoader(ABC):
    """Abstract base class for PubMed XML loaders."""

    def __init__(
        self,
        input_path: str,
        output_path: str,
        baseline: str,
        file_start: Optional[int] = None,
        file_end: Optional[int] = None,
        num_workers: Optional[int] = None,
    ) -> None:
        """Initialize the base PubMed loader.

        Args:
            input_path: Directory containing input XML files
            output_path: Directory where processed files will be written
            k: Baseline identifier used in filename parsing
            file_start: Optional start index for file range processing
            file_end: Optional end index for file range processing

        """
        # Resolve paths against project root if they're relative
        self.input_path = _resolve_path(input_path)
        self.output_path = _resolve_path(output_path)
        # Ensure k is a string
        self.baseline = str(baseline)
        self.file_start = file_start
        self.file_end = file_end
        if num_workers is None:
            cpu_count = os.cpu_count()
            self.num_workers = max(
                1,
                cpu_count - 1 if cpu_count and cpu_count > 1 else 1,
            )
        elif num_workers < 1:
            msg = "num_workers must be at least 1"
            raise ValueError(msg)
        else:
            self.num_workers = num_workers

        self.current_input_file: Optional[str] = None

        # Validate file range if both are provided
        if self.file_start is not None and self.file_end is not None:
            if self.file_start > self.file_end:
                msg = (
                    f"file_start ({self.file_start}) cannot be greater than "
                    f"file_end ({self.file_end})"
                )
                raise ValueError(msg)

        # output_path could be either a database file, such as duckdb
        # or a directory for holding output json files

        # If output_path is a file, ensure the directory exists
        if not os.path.isdir(self.output_path):
            output_dir = os.path.dirname(self.output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created directory: {output_dir}")
            else:
                print(f"Output directory already exists: {output_dir}")
        else:
            # If output_path is a directory, ensure it exists
            if not os.path.exists(self.output_path):
                os.makedirs(self.output_path)
                print(f"Created directory: {self.output_path}")
            else:
                print(f"Output directory already exists: {self.output_path}")

    def _get_input_files(self, input_path: str) -> list[str]:
        """Get input files using path objects for reliable path handling.

        Args:
            input_path: Directory containing the input files

        Returns:
            List of input file paths sorted by file number

        """
        # k is used for keyword to split the filename obtained from pubmed.
        # It's different for each annual baseline
        input_path_obj = Path(input_path)

        # Use Path's glob method which handles path separators correctly
        input_files = sorted(
            [str(p) for p in input_path_obj.glob("*.gz")],
            key=lambda x: int(
                os.path.splitext(os.path.basename(x))[0].split(self.baseline + "n")[-1][
                    :-4
                ],
            ),
        )

        # Filter files by range if specified
        if input_files and (self.file_start is not None or self.file_end is not None):
            filtered_files = []
            for file_path in input_files:
                try:
                    file_num = int(
                        os.path.splitext(os.path.basename(file_path))[0].split(
                            self.baseline + "n",
                        )[-1][:-4],
                    )

                    # Apply file_start filter if specified
                    if self.file_start is not None and file_num < self.file_start:
                        continue

                    # Apply file_end filter if specified
                    if self.file_end is not None and file_num > self.file_end:
                        continue

                    filtered_files.append(file_path)
                except (ValueError, IndexError):
                    # Skip files that don't match expected naming pattern
                    continue

            input_files = filtered_files
            print(
                (
                    f"After applying range filters (start={self.file_start}, "
                    f"end={self.file_end}): {len(input_files)} files"
                ),
            )

        # Add debug output for the number of files found
        print(f"Found {len(input_files)} XML files in {input_path}")
        if len(input_files) == 0:
            print(
                f"WARNING: No XML files found in {input_path} matching pattern '*.gz'",
            )
            print("Make sure the path exists and contains gzipped XML files.")
        return input_files

    @abstractmethod
    def _write_output(self, data: Any, input_file: str) -> None:  # noqa: ANN401
        """Write processed article data to output.

        Args:
            data: The processed article data
            input_file: Original input file path

        """
        msg = "Subclasses must implement the _write_output method."
        raise NotImplementedError(
            msg,
        )

    def _load_xml(self, input_file: str) -> list[dict[str, Any]]:
        """Load XML file and parse using pubmed_parser.

        Args:
            input_file: Path to the input XML file

        Returns:
            List of article data dictionaries

        """
        return pp.parse_medline_xml(input_file, year_info_only=False)

    @staticmethod
    def _xml_parser_worker(
        input_file: str,
    ) -> tuple[str, Union[list[dict[str, Any]], Exception]]:
        """Worker function to load and parse an XML file.

        Returns a tuple (input_file, data_or_exception).
        """
        try:
            data = pp.parse_medline_xml(input_file, year_info_only=False)
            return input_file, data
        except Exception as e:
            # Print error from worker for immediate visibility,
            # main process will also log
            print(f"Error parsing {input_file} in worker: {e}")
            return input_file, e

    def run_loader_parallel(self) -> None:
        """Run the loader of PubMed files using parallel readers and a single writer."""
        print(
            f"Starting to load PubMed files from {self.input_path} using {self.num_workers} worker(s)",
        )
        input_files_list = self._get_input_files(self.input_path)

        if not input_files_list:
            print("No files to process. Please check the input path and file pattern.")
            return

        print(f"Processing {len(input_files_list)} XML files in parallel.")
        if input_files_list:  # Ensure list is not empty before accessing elements
            print(f"First file: {os.path.basename(input_files_list[0])}")
            print(f"Last file: {os.path.basename(input_files_list[-1])}")

        pool = None
        try:
            # Using a context manager for the pool is good practice if available/preferred
            # For this example, manual management:
            pool = multiprocessing.Pool(processes=self.num_workers)

            results_iterator = pool.imap_unordered(
                BasePubMedLoader._xml_parser_worker,
                input_files_list,
            )

            for input_file, result in tqdm(
                results_iterator,
                total=len(input_files_list),
                desc="Processing files",
            ):
                self.current_input_file = input_file

                if isinstance(result, Exception):
                    print(
                        f"Skipping file {os.path.basename(input_file)} due to parsing error: {result}",
                    )
                    continue

                data = result  # result is the parsed data list[dict[str, Any]]
                if data:  # Ensure data is not empty or None before writing
                    self._write_output(data, input_file)
                else:
                    print(
                        f"No data returned or empty data for {os.path.basename(input_file)}, skipping write.",
                    )

        except Exception as e:
            print(f"An error occurred during parallel processing: {e}")
        finally:
            if pool:
                pool.close()
                pool.join()

        print("Parallel PubMed loading complete.")

    def run_loader(self) -> None:
        """Run the loader of PubMed files."""
        print(f"Starting to load PubMed files from {self.input_path}")
        input_files_list = self._get_input_files(self.input_path)

        # Add more debug information about the files being processed
        if len(input_files_list) > 0:
            print(f"Processing {len(input_files_list)} XML files")
            print(f"First file: {os.path.basename(input_files_list[0])}")
            print(f"Last file: {os.path.basename(input_files_list[-1])}")
        else:
            print("No files to process. Please check the input path and file pattern.")
            return

        # Use tqdm with the list directly for proper progress tracking
        for input_file in tqdm(input_files_list, desc="Processing files"):
            self.current_input_file = input_file
            data = self._load_xml(input_file)
            self._write_output(data, input_file)

    def run_loader_mp(self) -> None:
        """Run loader of PubMed files with XML parsing in parallel."""
