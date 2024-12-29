import json
import argparse
import subprocess
from prettytable import PrettyTable


def read_job_metadata(metadata_file):
    """
    Reads the job metadata from the provided JSON file.
    """
    with open(metadata_file, "r") as f:
        return json.load(f)


def get_gpu_usage(job_id):
    """
    Retrieves GPU usage details for a job using 'nvidia-smi' via 'jobsh'.
    """
    gpu_usage = None

    # Log onto the node running the job and retrieve GPU usage details
    jobsh_result = subprocess.run(
        ["jobsh", "-j", job_id, "--", "nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    if jobsh_result.stdout.strip():
        gpu_usage = jobsh_result.stdout.strip()

    return gpu_usage


def get_running_job_details(job_id):
    """
    Retrieves details for running/pending jobs using 'squeue'.
    """
    result = {"elapsed_time": None, "status": "running/pending", "gpu_usage": None}

    # Retrieve details from 'squeue'
    squeue_result = subprocess.run(
        ["squeue", "--job", job_id, "--format=%M", "--noheader"],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    if squeue_result.stdout.strip():
        # Elapsed time is given by 'squeue'
        result["elapsed_time"] = squeue_result.stdout.strip()

        # Retrieve GPU usage details
        result["gpu_usage"] = get_gpu_usage(job_id)

    return result


def get_completed_job_details(job_id):
    """
    Retrieves details for completed jobs such as elapsed time, CPU usage, and exit code using 'sacct'.
    """
    result = {
        "elapsed_time": None,
        "cpu_usage": None,
        "gpu_usage": None,  # GPU usage would require a custom tool like 'nvidia-smi'
        "exit_code": None,
        "status": "COMPLETED",
    }

    # Retrieve job details from 'sacct'
    sacct_result = subprocess.run(
        [
            "sacct",
            "--jobs",
            f"{job_id}.batch",
            "--format=JobID,Elapsed,CPUTime,State,ExitCode",
            "--parsable2",
        ],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    sacct_lines = sacct_result.stdout.strip().split("\n")

    if len(sacct_lines) > 1:
        # Extract details from the second line (skip header)
        job_data = sacct_lines[1].split("|")
        result["elapsed_time"] = job_data[1]
        result["cpu_usage"] = job_data[2]
        result["exit_code"] = job_data[4]

        # If the job failed, update the status
        if result["exit_code"] != "0:0":
            result["status"] = "FAILED"

    return result


def monitor_jobs(job_metadata, output_file, log_file):
    """
    Monitors both running and completed SLURM jobs based on job IDs in the metadata.
    Writes the monitoring results to an output file (JSON) and a text log (table format).
    """
    monitoring_results = []
    table = PrettyTable()
    table.field_names = [
        "Batch",
        "Job ID",
        "Job Name",
        "Status",
        "Elapsed Time",
        "CPU Usage",
        "GPU Usage",  # Added GPU Usage column
        "Exit Code",
    ]

    for batch, job_info in job_metadata.items():
        job_id = job_info.get("job_id")
        job_name = job_info.get("job_name")

        if job_id:
            result = {}
            result["batch"] = batch
            result["job_name"] = job_name
            result["job_id"] = job_id

            # Check if the job is still running or pending with 'squeue'
            squeue_result = subprocess.run(
                ["squeue", "--job", job_id],
                stdout=subprocess.PIPE,
                universal_newlines=True,
            )

            if squeue_result.stdout.strip():
                # Job is still running or pending
                job_details = get_running_job_details(job_id)
                result.update(job_details)
            else:
                # Job is completed, get details from 'sacct'
                job_details = get_completed_job_details(job_id)
                result.update(job_details)

            # Add results to the table for logging
            table.add_row(
                [
                    batch,
                    job_id,
                    job_name,
                    result.get("status", "UNKNOWN"),
                    result.get("elapsed_time", "N/A"),
                    result.get("cpu_usage", "N/A"),
                    result.get("gpu_usage", "N/A"),  # Added GPU Usage
                    result.get("exit_code", "N/A"),
                ]
            )

            # Append the monitoring result for this job
            monitoring_results.append(result)

        else:
            print(f"No job ID found for batch {batch}")

    # Write the monitoring results to the output JSON file
    with open(output_file, "w") as f:
        json.dump(monitoring_results, f, indent=2)

    # Write the table to the log file for easier overview
    with open(log_file, "w") as f:
        f.write(str(table))

    print(f"Monitoring results saved to {output_file} and {log_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Monitor SLURM jobs using job_metadata.json and export results to a file"
    )
    parser.add_argument(
        "--metadata-file",
        type=str,
        required=True,
        help="Path to the job metadata file (job_metadata.json)",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        required=True,
        help="Path to the output file for monitoring results (JSON)",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        required=True,
        help="Path to the log file for formatted results (Table)",
    )

    args = parser.parse_args()

    # Read job metadata from the JSON file
    job_metadata = read_job_metadata(args.metadata_file)

    # Monitor jobs based on the job IDs and export the results
    monitor_jobs(job_metadata, args.output_file, args.log_file)
