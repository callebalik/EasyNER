#!/usr/bin/env python3
"""
Benchmark runner for EasyNER.

This script runs benchmarks and compares performance against baseline
measurements to detect regressions.
"""

import os
import sys
import json
import argparse
import subprocess
import pandas as pd
from pathlib import Path
from datetime import datetime


def run_benchmarks(args):
    """Run benchmarks and save results."""
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Construct pytest command
    benchmark_command = [
        "pytest",
        "tests/benchmarks",
        "--benchmark-only",
        f"--benchmark-autosave",
        "--benchmark-disable-gc",
        f"--benchmark-save={args.name}",
    ]

    if args.verbose:
        benchmark_command.append("-v")

    if args.filter:
        benchmark_command.append(f"tests/benchmarks/{args.filter}")

    # Run benchmarks and collect results
    print(f"Running benchmarks: {' '.join(benchmark_command)}")
    result = subprocess.run(benchmark_command)

    if result.returncode != 0:
        print(f"Benchmark run failed with exit code {result.returncode}")
        return False

    return True


def find_benchmark_files():
    """Find benchmark files and their IDs."""
    benchmark_dir = Path(".benchmarks")
    if not benchmark_dir.exists():
        return None, None

    # Find the most recent machine directory
    machine_dirs = sorted(list(benchmark_dir.glob("*")))
    if not machine_dirs:
        return None, None

    # Get the most recent machine directory
    machine_dir = machine_dirs[-1]

    # Find all JSON files in this directory
    json_files = sorted(list(machine_dir.glob("*.json")))
    if not json_files:
        return None, None

    # Extract benchmark IDs from filenames
    benchmark_ids = [f.stem for f in json_files]

    return machine_dir, benchmark_ids


def compare_benchmarks(args):
    """Compare current benchmark results with baseline to detect regressions."""
    # Find benchmark files and IDs
    machine_dir, benchmark_ids = find_benchmark_files()
    if not benchmark_ids:
        print("No benchmark files found")
        return False

    # Get the most recent benchmark ID
    latest_id = benchmark_ids[-1]

    # Find baseline ID if specified
    baseline_id = None
    if args.baseline:
        for id in benchmark_ids:
            if args.baseline in id:
                baseline_id = id
                break

    # If no baseline specified or found, use the second most recent one (if available)
    if not baseline_id and len(benchmark_ids) > 1:
        baseline_id = benchmark_ids[-2]

    if not baseline_id:
        print("No baseline found for comparison. Skipping regression check.")
        return True

    print(f"Comparing benchmark {latest_id} against baseline {baseline_id}")

    # Run pytest-benchmark compare command with correct syntax
    compare_command = [
        "pytest-benchmark",
        "compare",
        "--csv",
        f"{args.output_dir}/benchmark_comparison.csv",
        baseline_id,
        latest_id,
    ]

    print(f"Running comparison: {' '.join(compare_command)}")

    # Try the comparison with the proper syntax
    try:
        result = subprocess.run(compare_command, cwd=str(machine_dir.parent))

        # Check success and analyze CSV if available
        csv_path = Path(args.output_dir) / "benchmark_comparison.csv"
        if csv_path.exists():
            # See if there are significant regressions
            df = pd.read_csv(csv_path)

            # Calculate threshold
            threshold = (
                float(args.threshold.replace("mean+", "").replace("%", ""))
                / 100.0
                + 1.0
            )

            # Check for regressions
            regressions = []

            if (
                "name" in df.columns
                and "old" in df.columns
                and "new" in df.columns
            ):
                for _, row in df.iterrows():
                    # Skip header or invalid rows
                    if pd.isna(row["old"]) or pd.isna(row["new"]):
                        continue

                    old_time = float(row["old"])
                    new_time = float(row["new"])
                    if old_time > 0 and new_time / old_time > threshold:
                        regressions.append(
                            {
                                "name": row["name"],
                                "old": old_time,
                                "new": new_time,
                                "change": (new_time / old_time - 1)
                                * 100,  # percentage
                            }
                        )

            # Report results
            if regressions:
                print("\n🚨 PERFORMANCE REGRESSIONS DETECTED 🚨")
                print(
                    f"The following benchmarks exceed the threshold ({args.threshold}):"
                )
                for reg in regressions:
                    print(
                        f"  - {reg['name']}: {reg['old']:.6f}s -> {reg['new']:.6f}s ({reg['change']:.2f}% increase)"
                    )
                return False
            else:
                print("\n✅ No performance regressions detected")
                return True
        else:
            print(f"Warning: Comparison CSV not found at {csv_path}")
            return result.returncode == 0

    except Exception as e:
        print(f"Error comparing benchmarks: {str(e)}")

        # Fall back to our custom comparison if pytest-benchmark compare fails
        print("Attempting fallback comparison method...")
        return compare_benchmarks_fallback(
            machine_dir, benchmark_ids, latest_id, baseline_id, args
        )


def compare_benchmarks_fallback(
    machine_dir, benchmark_ids, latest_id, baseline_id, args
):
    """Fall back to custom comparison if pytest-benchmark compare fails."""
    try:
        # Load the JSON files directly
        latest_file = machine_dir / f"{latest_id}.json"
        baseline_file = machine_dir / f"{baseline_id}.json"

        with open(baseline_file, "r") as f:
            baseline_data = json.load(f)

        with open(latest_file, "r") as f:
            current_data = json.load(f)

        # Extract benchmark results
        baseline_results = {
            b["fullname"]: b["stats"]["mean"]
            for b in baseline_data["benchmarks"]
        }
        current_results = {
            b["fullname"]: b["stats"]["mean"]
            for b in current_data["benchmarks"]
        }

        # Calculate threshold
        threshold = (
            float(args.threshold.replace("mean+", "").replace("%", "")) / 100.0
            + 1.0
        )

        # Compare results and detect regressions
        regressions = []
        comparison_data = []

        for name, current_time in current_results.items():
            if name in baseline_results:
                baseline_time = baseline_results[name]
                ratio = current_time / baseline_time
                threshold_exceeded = ratio > threshold

                comparison_data.append(
                    {
                        "Name": name,
                        "Baseline(s)": baseline_time,
                        "Current(s)": current_time,
                        "Ratio": ratio,
                        "Threshold": threshold,
                        "Regression": threshold_exceeded,
                    }
                )

                if threshold_exceeded:
                    regressions.append(name)

        # Create comparison dataframe
        df = pd.DataFrame(comparison_data)

        # Save to CSV
        csv_path = Path(args.output_dir) / "benchmark_comparison.csv"
        df.to_csv(csv_path, index=False)
        print(f"Comparison saved to {csv_path}")

        # Print results
        if regressions:
            print("\n🚨 PERFORMANCE REGRESSIONS DETECTED 🚨")
            print(
                f"The following benchmarks exceed the threshold ({args.threshold}):"
            )
            for name in regressions:
                idx = next(
                    i
                    for i, item in enumerate(comparison_data)
                    if item["Name"] == name
                )
                data = comparison_data[idx]
                print(
                    f"  - {name}: {data['Baseline(s)']:.6f}s -> {data['Current(s)']:.6f}s ({(data['Ratio']-1)*100:.2f}% increase)"
                )
            return False
        else:
            print("\n✅ No performance regressions detected")
            return True

    except Exception as e:
        print(f"Error in fallback comparison: {str(e)}")
        return False


def main():
    """Main function to run benchmarks and check for regressions."""
    parser = argparse.ArgumentParser(description="Run EasyNER benchmarks")
    parser.add_argument(
        "--output-dir",
        default="results/benchmarks",
        help="Directory to store benchmark results",
    )
    parser.add_argument(
        "--baseline",
        help="Baseline to compare with (previous benchmark run ID)",
    )
    parser.add_argument(
        "--name",
        default=datetime.now().strftime("%Y%m%d_%H%M%S"),
        help="Name for this benchmark run",
    )
    parser.add_argument(
        "--threshold",
        default="mean+10%",
        help="Threshold to detect regressions (e.g., 'mean+10%')",
    )
    parser.add_argument(
        "--filter",
        help="Filter which benchmarks to run (e.g., ner/test_benchmark_ner_spacy*.py)",
    )
    parser.add_argument(
        "--skip-comparison",
        action="store_true",
        help="Skip comparison with baseline",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose output"
    )

    args = parser.parse_args()

    # Run benchmarks
    success = run_benchmarks(args)
    if not success:
        sys.exit(1)

    # Compare with baseline if requested
    if not args.skip_comparison:
        success = compare_benchmarks(args)
        if not success:
            sys.exit(1)

    print("Benchmark run completed successfully!")


if __name__ == "__main__":
    main()
