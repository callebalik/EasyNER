from process_job_errors import run_error_logging
from process_batch_completion import run_completion_logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

job_nbr = 6
job_dir = os.path.join(script_dir, f"results/job_batch_{job_nbr}")
job_dir = script_dir

metadata_file = os.path.join(job_dir, "job_metadata.json")
err_dir = job_dir
error_log_dir = os.path.join(job_dir, "error_logs")

batch_completion_log_file = os.path.join(job_dir, "batch_completion.log")
batch_rerun_file = os.path.join(job_dir, "rerun_batches.log")

print(error_log_dir)

# Run the error logging process
run_error_logging(metadata_file, err_dir, error_log_dir)

# Run the batch completion logging process
run_completion_logging(metadata_file, err_dir, batch_completion_log_file, rerun_file=batch_rerun_file)
