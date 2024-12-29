from process_job_errors import run_error_logging
from process_batch_completion import run_completion_logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the paths relative to the script's location
job_nbr = 7
job_dir = os.path.join(script_dir, f"results/job_batch_{job_nbr}")

metadata_file = os.path.join(job_dir, "job_metadata.json")
err_dir = os.path.join(job_dir, "error_logs")
error_log_dir = os.path.join(job_dir, "error_logs")
batch_completion_log_dir = os.path.join(job_dir, "batch_completion_logs")
batch_completion_log_file = os.path.join(job_dir, "batch_completion.log")
print(error_log_dir)

# Run the error logging process
run_error_logging(metadata_file, err_dir, error_log_dir)

# Run the batch completion logging process
run_completion_logging(metadata_file, err_dir, batch_completion_log_file)
