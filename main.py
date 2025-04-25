# coding=utf-8

import json
import os
from glob import glob
import time
from typing import Optional
from tqdm import tqdm
import torch
from concurrent.futures import (
    ProcessPoolExecutor,
    as_completed,
)
from multiprocessing import cpu_count

from easyner.utils.timekeep import TimingManager, timed_execution
from scripts import cord_loader
from scripts import downloader
from scripts import splitter
from scripts import splitter_pubmed
from scripts import text_loader
from scripts import search
from scripts import metrics
from scripts import nel
from scripts import entity_merger
from scripts import ner_main
from scripts import analysis
from scripts import pubmed_bulk

ignored_modules = []
run_modules = []


class EasyNerModule:
    """Base class for all processing modules in the EasyNER pipeline."""

    def __init__(self, name, config, ignore_dict):
        self.name = name
        self.full_config = config
        self.config = config.get(name, {})
        self.ignored = ignore_dict.get(name, False)
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.execution_time: Optional[float] = None
        self.execution_success: Optional[bool] = False

    @timed_execution
    def run(self, **kwargs):
        """Execute this module with appropriate timing and logging."""
        if self.ignored:
            print(f"Ignoring script: {self.name}.")
            self.execution_success = None  # Indicate module ignored
            return False
        else:
            print(f"Running {self.name} script.")
            # Execute the module's specific processing
            result = self._execute()
            print(f"Finished running {self.name} script.")
            print()
            return result

    def _execute(self):
        """To be implemented by each module subclass."""
        raise NotImplementedError("Subclasses must implement _execute method")


class CordLoaderModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("cord_loader", config, ignore_dict)

    def _execute(self):
        return cord_loader.run(
            input_file=self.config["input_path"],
            output_file=self.config["output_path"],
            subset=self.config["subset"],
            subset_file=self.config["subset_file"],
        )


class DownloaderModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("downloader", config, ignore_dict)

    def _execute(self):
        return downloader.run(
            input_file=self.config["input_path"],
            output_file=self.config["output_path"],
            batch_size=self.config["batch_size"],
        )


class TextLoaderModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("text_loader", config, ignore_dict)

    def _execute(self):
        return text_loader.run(self.config)


class PubmedBulkLoaderModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("pubmed_bulk_loader", config, ignore_dict)

    def _execute(self):
        return pubmed_bulk.run_pbl(self.config)


class SplitterModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("splitter", config, ignore_dict)

    def _execute(self):
        os.makedirs(self.config["output_folder"], exist_ok=True)

        if self.config["pubmed_bulk"]:
            self._process_pubmed_bulk()
        else:
            self._process_standard()

        return {}

    def _process_pubmed_bulk(self):
        # Extract pubmed bulk processing logic
        if self.config["file_limit"] == "ALL":
            input_files_list = splitter_pubmed.load_pre_batched_files(
                self.config["input_path"]
            )
        else:
            input_files_list = splitter_pubmed.load_pre_batched_files(
                self.config["input_path"],
                limit=self.config["file_limit"],
            )

        self._process_with_tokenizer(input_files_list, is_pubmed_bulk=True)

    def _process_standard(self):
        with open(self.config["input_path"], "r", encoding="utf-8") as f:
            full_articles = json.loads(f.read())

        article_batches = splitter.make_batches(
            list(full_articles), self.config["batch_size"]
        )

        self._process_with_tokenizer(
            article_batches, full_articles=full_articles
        )

    def _process_with_tokenizer(
        self, items, is_pubmed_bulk=False, full_articles=None
    ):
        tokenizer = self.config["tokenizer"]
        print(f"Running splitter script with {tokenizer}")

        with ProcessPoolExecutor(min(CPU_LIMIT, cpu_count())) as executor:
            if is_pubmed_bulk:
                futures = [
                    executor.submit(
                        splitter_pubmed.split_prebatch,
                        self.config,
                        input_file,
                        tokenizer=tokenizer,
                    )
                    for input_file in items
                ]
            else:
                futures = [
                    executor.submit(
                        splitter.split_batch,
                        self.config,
                        idx,
                        art,
                        full_articles,
                        tokenizer=tokenizer,
                    )
                    for idx, art in enumerate(items)
                ]

            for future in as_completed(futures):
                _ = future.result()


class NERModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("ner", config, ignore_dict)

    def _execute(self):
        if self.config.get("clear_old_results", True):
            try:
                os.remove(self.config["output_path"])
            except OSError:
                pass

        os.makedirs(self.config["output_path"], exist_ok=True)

        input_file_list = sorted(
            glob(f'{self.config["input_path"]}*.json'),
            key=lambda x: int(
                os.path.splitext(os.path.basename(x))[0].split("-")[-1]
            ),
        )

        # Sort files on range
        if "article_limit" in self.config:
            if isinstance(self.config["article_limit"], list):
                start = self.config["article_limit"][0]
                end = self.config["article_limit"][1]

                input_file_list = ner_main.filter_files(
                    input_file_list, start, end
                )

                print(f"processing articles between {start} and {end} range")

        # Run prediction on each sentence in each article
        if self.config["multiprocessing"]:
            with ProcessPoolExecutor(min(CPU_LIMIT, cpu_count())) as executor:
                futures = [
                    executor.submit(
                        ner_main.run_ner_main, self.config, batch_file
                    )
                    for batch_file in input_file_list
                ]

                for future in as_completed(futures):
                    _ = future.result()
        else:
            device = torch.device(0 if torch.cuda.is_available() else "cpu")

            for batch_file in tqdm(input_file_list):
                ner_main.run_ner_main(self.config, batch_file, device)

        return True


class AnalysisModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("analysis", config, ignore_dict)

    def _execute(self):
        return analysis.run(self.config)


class MetricsModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("metrics", config, ignore_dict)

    def _execute(self):
        return metrics.get_metrics(self.full_config["metrics"])


class NelModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("nel", config, ignore_dict)

    def _execute(self):
        return nel.nel_main(self.full_config["nel"])


class MergerModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("merger", config, ignore_dict)

    def _execute(self):
        return entity_merger.run_entity_merger(self.full_config["merger"])


class SearchModule(EasyNerModule):
    def __init__(self, config, ignore_dict):
        super().__init__("result_inspection", config, ignore_dict)

    def _execute(self):
        search_config = self.full_config["result_inspection"]
        os.makedirs(
            os.path.dirname(search_config["output_file"]), exist_ok=True
        )
        searcher = search.EntitySearch(search_config)
        return searcher.run()


if __name__ == "__main__":
    print("Please see config.json for configuration!")

    with open("config.json", "r") as f:
        config = json.loads(f.read())

    print("Loaded config:")

    # Initialize timing manager
    timer = TimingManager(enabled=config["TIMEKEEP"])

    os.makedirs("data", exist_ok=True)

    ignore = config["ignore"]
    CPU_LIMIT = config["CPU_LIMIT"]  # for multiprocessing
    print(f"Limited to {CPU_LIMIT} CPUs")

    # Define modules pipeline
    modules = [
        CordLoaderModule(config, ignore),
        DownloaderModule(config, ignore),
        TextLoaderModule(config, ignore),
        PubmedBulkLoaderModule(config, ignore),
        SplitterModule(config, ignore),
        NERModule(config, ignore),
        AnalysisModule(config, ignore),
        MetricsModule(config, ignore),
        NelModule(config, ignore),
        MergerModule(config, ignore),
        SearchModule(config, ignore),
    ]

    # Execute each module
    for module in modules:
        module.run(timer=timer)

    # Finalize timing information
    timer.finalize()

    print("Program finished successfully.")
