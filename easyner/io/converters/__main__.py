"""
Command-line runner for EasyNER converters.
Allows running the converters via 'python -m easyner.io.converters'
"""

import argparse
import sys
import importlib

# Define available converters
CONVERTERS = {
    "json_to_duck": {
        "module": "easyner.io.converters.json_to_duck_converter",
        "description": "Convert JSON files to DuckDB database",
    }
    # Add more converters here as they become available
    # "format_a_to_b": {
    #     "module": "easyner.io.converters.format_a_to_b_converter",
    #     "description": "Convert Format A to Format B"
    # }
}


def main():
    """Entry point for the converters command-line interface."""
    parser = argparse.ArgumentParser(
        description="EasyNER data format converters"
    )

    # First level argument is the converter type
    parser.add_argument(
        "converter",
        choices=list(CONVERTERS.keys()),
        help="The converter to use",
    )

    # Parse only the first argument to determine which converter to use
    args, remaining_args = parser.parse_known_args(sys.argv[1:2])

    if args.converter not in CONVERTERS:
        print(f"Error: Unknown converter '{args.converter}'")
        parser.print_help()
        return 1

    # Import the selected converter module
    try:
        converter_info = CONVERTERS[args.converter]
        module = importlib.import_module(converter_info["module"])

        # Call the module's main function with the remaining args - using sys.argv[2:] to get all args after the converter name
        return module.main(sys.argv[2:])
    except ImportError as e:
        print(f"Error importing converter module: {e}")
        return 1
    except AttributeError:
        print(f"Error: The converter module doesn't have a main() function")
        return 1
    except Exception as e:
        print(f"Error initializing converter: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
