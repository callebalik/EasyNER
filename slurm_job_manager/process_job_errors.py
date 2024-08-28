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
        return False

    error_traceback = []
    slurm_errors = []
    last_batch_status = None  # To keep track of the last batch status line
    capture_error = False

    # Regular expressions for batch status and errors
    batch_status_pattern = re.compile(
        r"batch:(\d+):\s+(\d+)%\|"
    )  # Matches batch status lines
    traceback_pattern = re.compile(r"Traceback")
    slurm_error_pattern = re.compile(
        r"slurmstepd: error: \*\*\* JOB (\d+) .* CANCELLED"
    )

    with open(err_file, "r") as f:
        for line in f:
            # Track the last batch status line before an error
            batch_match = batch_status_pattern.search(line)
            if batch_match:
                last_batch_status = line.strip()  # Save the latest batch status line

            # Check for a SLURM job cancellation error
            slurm_error_match = slurm_error_pattern.search(line)
            if slurm_error_match:
                if last_batch_status and last_batch_status in line:
                    # If the error is on the same line as the batch status, log it inline
                    slurm_errors.append(line.strip())
                else:
                    # Log the last batch status before the error
                    if last_batch_status:
                        slurm_errors.append(
                            f"Last Batch Status Before Error:\n{last_batch_status}"
                        )
                    slurm_errors.append(line.strip())

            # Check for a traceback error
            elif traceback_pattern.search(line):
                capture_error = True
                if last_batch_status:
                    error_traceback.append(
                        f"Last Batch Status Before Error:\n{last_batch_status}\n"
                    )
                    last_batch_status = None  # Clear after logging
                error_traceback.append(line)
            elif capture_error and line.strip() == "":  # End of traceback
                capture_error = False

            if capture_error:
                error_traceback.append(line)

    # Prepare the final output with the last batch status before the error
    if error_traceback or slurm_errors:
        with open(output_file, "w") as f_out:
            if slurm_errors:
                f_out.write("\nSLURM Job Errors:\n")
                f_out.write("\n".join(slurm_errors) + "\n")
            if error_traceback:
                f_out.write("\nError Traceback:\n")
                f_out.write("".join(error_traceback))
        print(f"Extracted errors and last batch status saved to {output_file}")
        return True
    else:
        print(f"No errors found in {err_file}.")
        return False


def process_job_errors(job_metadata_file, err_dir, output_dir):
    """
    Processes jobs from job_metadata.json and creates error logs for each job, including the last
    batch status before the error occurs.
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

            print(f"Processing error log for Job {job_name} (ID: {job_id})...")

            # Extract errors and the last batch status from the .err file
            errors_found = extract_errors_from_err_file(err_file, output_file)

            if not errors_found:
                print(f"No errors found for job {job_name}.")
        else:
            print(f"No job ID found for batch {batch}")


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
