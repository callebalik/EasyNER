"""
Command-line tool for converting JSON files to Parquet format.
"""

import os
import sys
import argparse
import logging
from typing import Dict, Any

from easyner.io.converters.converters import convert_json_to_parquet
from easyner.io.config import (
    DEFAULT_JSON_DIR,
    DEFAULT_PARQUET_DIR,
    DEFAULT_DB_PATH,
    MAX_CONCURRENT_WORKERS,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_arguments() -> Dict[str, Any]:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert JSON files to Parquet format"
    )

    parser.add_argument(
        "--json-dir",
        default=DEFAULT_JSON_DIR,
        help=f"Directory containing JSON files (default: {DEFAULT_JSON_DIR})",
    )

    parser.add_argument(
        "--parquet-dir",
        default=DEFAULT_PARQUET_DIR,
        help=f"Output directory for Parquet files (default: {DEFAULT_PARQUET_DIR})",
    )

    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_CONCURRENT_WORKERS,
        help=f"Maximum number of concurrent workers (default: {MAX_CONCURRENT_WORKERS})",
    )

    parser.add_argument(
        "--no-recursive",
        action="store_false",
        dest="recursive",
        help="Disable recursive processing of subdirectories",
    )

    parser.add_argument(
        "--no-progress",
        action="store_false",
        dest="show_progress",
        help="Disable progress bar",
    )

    return vars(parser.parse_args())


def main() -> int:
    """Main function."""
    try:
        args = parse_arguments()

        # Validate paths
        for path_arg in ["json_dir", "parquet_dir", "db_path"]:
            path = args[path_arg]
            if path_arg == "json_dir" and not os.path.isdir(path):
                logger.error(f"JSON directory does not exist: {path}")
                return 1

            if path_arg == "db_path":
                db_dir = os.path.dirname(path)
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir, exist_ok=True)

        # Ensure output directory exists
        os.makedirs(args["parquet_dir"], exist_ok=True)

        # Run conversion
        logger.info(
            f"Starting JSON to Parquet conversion with parameters: {args}"
        )
        stats = convert_json_to_parquet(
            json_dir=args["json_dir"],
            parquet_dir=args["parquet_dir"],
            db_path=args["db_path"],
            max_workers=args["workers"],
            recursive=args["recursive"],
            show_progress=args["show_progress"],
        )

        logger.info(f"Conversion completed with stats: {stats}")

        return 0
    except KeyboardInterrupt:
        logger.info("Conversion interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Conversion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
