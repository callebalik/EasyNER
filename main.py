# coding=utf-8

import json
import os
from typing import Dict, Any
import torch

from easyner.utils.timekeep import TimingManager
from easyner.core.module import registry, ModuleRegistryError

# Import all module factories to ensure they are registered
from scripts.cord_loader import create_module as _  # noqa: F401

ignored_modules = []
run_modules = []


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

    # Define modules to run (in order)
    module_names = [
        "cord_loader",
        "downloader",
        "text_loader",
        "pubmed_bulk_loader",
        "splitter",
        "ner",
        "analysis",
        "metrics",
        "nel",
        "merger",
        "result_inspection",
    ]

    # Execute each required module in the pipeline
    for module_name in module_names:
        if module_name not in ignore:
            try:
                # Get a fresh module instance from the registry
                try:
                    module = registry.get_module(module_name)
                except ModuleRegistryError:
                    print(
                        f"⚠️ Module '{module_name}' not yet migrated to new system, skipping"
                    )
                    continue

                # Get module-specific config
                module_config = config.get(module_name, {})

                # Validate configuration
                if not module.validate_config(module_config):
                    print(f"❌ Invalid configuration for {module_name}")
                    continue

                # Execute the module with timing
                print(f"▶️ Running {module_name}")
                with timer.measure(module_name):
                    result = module.execute(module_config)
                print(f"✅ Completed {module_name}")

            except Exception as e:
                print(f"❌ Error in {module_name}: {str(e)}")
        else:
            print(f"⏭️ Skipping {module_name} (ignored)")

    # Finalize timing information
    timer.finalize()

    print("Program finished successfully.")
