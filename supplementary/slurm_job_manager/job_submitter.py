import os
import json
import argparse
import subprocess


def submit_job(script_filename):
    """
    Submits a SLURM job using sbatch and returns the job ID.
    """
    # Submit the job script using sbatch and capture the output

    # Universal newlines to handle older python versions
    result = subprocess.run(
        ["sbatch", script_filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )

    # Extract the job ID from the output
    output = result.stdout.strip()
    error = result.stderr.strip()
    if result.returncode == 0:
        job_id = output.split()[-1]  # The job ID is usually the last part of the sbatch output
        print(f"Submitted {script_filename}, Job ID: {job_id}")
        return job_id, None
    else:
        print(f"Failed to submit {script_filename}, Error: {error}")
        return None, error


def update_metadata_with_job_id(metadata_file, job_ids):
    """
    Updates the job metadata file with the job IDs.
    :param metadata_file: Path to the job metadata file (JSON format).
    :param job_ids: Dictionary mapping batch numbers to SLURM job IDs.
    """
    # Load the existing metadata
    with open(metadata_file, "r") as f:
        job_metadata = json.load(f)

    # Update each job entry with its corresponding job ID
    for batch_number, job_id in job_ids.items():
        if batch_number in job_metadata:
            job_metadata[batch_number]["job_id"] = job_id

    # Save the updated metadata back to the file
    with open(metadata_file, "w") as f:
        json.dump(job_metadata, f, indent=2)

    print(f"Job metadata updated with job IDs in {metadata_file}.")


def submit_jobs(script_dir, error_log_file="job_submission_errors.json"):
    """
    Submits SLURM job scripts and returns a dictionary of job IDs.
    :param script_dir: Directory where the SLURM job scripts are located.
    :param error_log_file: File to log errors during job submission.
    :return: Dictionary mapping batch numbers to SLURM job IDs.
    """
    job_ids = {}
    errors = []

    # Iterate over the job scripts in the directory
    for script_filename in os.listdir(script_dir):
        if script_filename.endswith(".slurm"):
            batch_number = script_filename.split(".")[0]  # Assuming batch_1.slurm, batch_2.slurm format
            full_path = os.path.join(script_dir, script_filename)
            job_id, error = submit_job(full_path)
            if job_id:
                job_ids[batch_number] = job_id
            if error:
                errors.append({"batch_number": batch_number, "error": error})

    # Log errors to the error log file
    if errors:
        with open(error_log_file, "w") as f:
            json.dump(errors, f, indent=2)
        print(f"Errors logged to {error_log_file}")

    return job_ids


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Submit SLURM job scripts and optionally update job metadata with job IDs"
    )
    parser.add_argument(
        "--script-dir",
        type=str,
        required=True,
        help="Directory containing the SLURM job scripts",
    )
    parser.add_argument(
        "--metadata-file",
        type=str,
        help="Path to the job metadata file (JSON format, optional)",
    )
    parser.add_argument(
        "--error-log-file",
        type=str,
        default="job_submission_errors.json",
        help="File to log errors during job submission",
    )

    args = parser.parse_args()

    # Submit jobs and get job IDs
    job_ids = submit_jobs(args.script_dir, args.error_log_file)

    # If a metadata file is provided, update the metadata
    if args.metadata_file:
        update_metadata_with_job_id(args.metadata_file, job_ids)
    else:
        print("No metadata file provided, skipping metadata update.")

    print("All jobs submitted.")
