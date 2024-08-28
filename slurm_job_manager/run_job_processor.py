from process_job_errors import run_error_logging
from process_batch_completion import run_completion_logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the paths relative to the script's location
metadata_file = os.path.join(script_dir, "job_metadata.json")
err_dir = script_dir
error_log_dir = os.path.join(script_dir, "error_logs")
batch_completion_log_dir = os.path.join(script_dir, "batch_completion_logs")


# Run the error logging process
run_error_logging(metadata_file, err_dir, error_log_dir)

# Run the batch completion logging process
run_completion_logging(metadata_file, err_dir, batch_completion_log_dir)
