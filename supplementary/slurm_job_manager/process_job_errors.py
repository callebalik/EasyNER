import json
import os
import re
import argparse


def extract_errors_from_err_file(err_file, output_file):
    """
    Extracts the error messages (traceback and SLURM job cancellations) from a SLURM .err file
    and writes them to a separate output file, including the last batch status before the error.
    """
    if not os.path.exists(err_file):
        print(f"Error: The file {err_file} does not exist.")
        return None

    errors_present = False
    error_summary = []
    last_batch_status = None  # To keep track of the last batch status line
    capture_traceback = False
    current_traceback = []

    # Regular expressions for batch status, traceback start, and slurmstepd errors
    batch_status_pattern = re.compile(
        r"batch:(\d+):\s+(\d+)%\|"
    )  # Matches batch status lines
    traceback_start_pattern = re.compile(
        r"Traceback \(most recent call last\):"
    )  # Start of traceback
    slurm_error_pattern = re.compile(
        r"slurmstepd: error: (.*)"
    )  # Matches slurmstepd errors
    error_line_pattern = re.compile(
        r"^\s*raise\s+(\w+Error)"
    )  # Matches the error type after 'raise'

    with open(err_file, "r") as f:
        for line in f:
            # Track the last batch status line before an error
            batch_match = batch_status_pattern.search(line)
            if batch_match:
                last_batch_status = {
                    "batch_number": batch_match.group(1),
                    "completion_percentage": int(batch_match.group(2)),
                }

            # Check for the start of a traceback
            if traceback_start_pattern.search(line):
                capture_traceback = True
                current_traceback = []

            # Capture the entire traceback until it ends, then extract the error type
            if capture_traceback:
                current_traceback.append(line.strip())
                error_match = error_line_pattern.search(line)
                if error_match:
                    error_type = error_match.group(1)
                    error_summary.append(
                        {"error": error_type, "last_batch_status": last_batch_status}
                    )
                    errors_present = True
                    capture_traceback = False  # Stop capturing once we have the error

            # Check for a slurmstepd error
            slurm_error_match = slurm_error_pattern.search(line)
            if slurm_error_match:
                error_summary.append(
                    {"error": "Slurm error", "last_batch_status": last_batch_status}
                )
                errors_present = True

    # Fallback: If a traceback was detected but no specific error was found
    if capture_traceback and current_traceback:
        error_summary.append(
            {
                "error": "Traceback Error Not Parsed",
                "last_batch_status": last_batch_status,
            }
        )

    # Write errors to the output file
    if errors_present:
        with open(output_file, "w") as f_out:
            for error in error_summary:
                f_out.write(
                    f"Error: {error['error']}, Last Batch Status: {error['last_batch_status']}\n"
                )

        return {
            "errors_present": True,
            "error_types": error_summary,
            "error_log_path": f"file://{os.path.abspath(output_file)}",
        }

    return None


def process_job_errors(job_metadata_file, err_dir, output_dir):
    """
    Processes jobs from job_metadata.json and creates error logs for each job, including the last
    batch status before the error occurs. Also updates the metadata with error information.
    """
    # Read the job metadata
    with open(job_metadata_file, "r") as f:
        job_metadata = json.load(f)

    # Make sure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Process each job in the metadata
    for batch, job_info in job_metadata.items():
        job_id = job_info.get("job_id")
        job_name = job_info.get("job_name")

        if job_id:
            # Construct the .err file path (assuming a naming convention like batch_<job_id>.err)
            err_file = os.path.join(err_dir, f"{job_name}.err")
            output_file = os.path.join(output_dir, f"{job_name}_error.log")

            if not os.path.exists(err_file):
                print(f"Job {job_name} (ID: {job_id}) not started yet")
                return

            print(f"Processing error log for Job {job_name} (ID: {job_id})...")

            # Extract errors and the last batch status from the .err file
            error_summary = extract_errors_from_err_file(err_file, output_file)

            if error_summary:
                # Update the metadata with error information
                job_info["errors_present"] = error_summary["errors_present"]
                job_info["error_log_path"] = error_summary["error_log_path"]
                job_info["error_types"] = error_summary["error_types"]
            else:
                print(f"No errors found for job {job_name}.")
                job_info["errors_present"] = False
                job_info["error_log_path"] = None
                job_info["error_types"] = {}

    # Save the updated job metadata back to the file
    with open(job_metadata_file, "w") as f:
        json.dump(job_metadata, f, indent=2)

    print(f"Job metadata updated with error information.")


def run_error_logging(metadata_file, err_dir, output_dir):
    """
    Callable function to run the error logging process programmatically.
    This function can be called from other Python scripts.
    """
    process_job_errors(metadata_file, err_dir, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract errors from SLURM .err files for jobs in job_metadata.json"
    )
    parser.add_argument(
        "--metadata-file",
        type=str,
        required=True,
        help="Path to the job metadata file (job_metadata.json)",
    )
    parser.add_argument(
        "--err-dir",
        type=str,
        required=True,
        help="Directory where .err files are located",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save extracted error logs",
    )

    args = parser.parse_args()

    # Process the jobs and extract errors from their .err files
    process_job_errors(args.metadata_file, args.err_dir, args.output_dir)
