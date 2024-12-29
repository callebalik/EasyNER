import os
import shutil
from batch_generator import split_articles_into_batches, save_batches_to_file
from job_creator import create_jobs
from job_submitter import submit_jobs, update_metadata_with_job_id
from job_monitor import monitor_jobs, read_job_metadata
from process_job_errors import run_error_logging
from process_batch_completion import run_completion_logging

def run_batch_generator(total_articles, batch_size, output_file):
    batches = split_articles_into_batches(total_articles, batch_size)
    save_batches_to_file(batches, output_file)

def run_job_creator(batch_file, metadata_file, setup_script, job_dir):
    create_jobs(batch_file, metadata_file, setup_script, job_dir)

def run_job_submitter(script_dir, metadata_file):
    job_ids = submit_jobs(script_dir, )
    if metadata_file:
        update_metadata_with_job_id(metadata_file, job_ids)

def run_job_monitor(metadata_file, output_file, log_file):
    job_metadata = read_job_metadata(metadata_file)
    monitor_jobs(job_metadata, output_file, log_file)

def run_error_logging_process(metadata_file, err_dir, error_log_dir):
    run_error_logging(metadata_file, err_dir, error_log_dir)

def run_completion_logging_process(metadata_file, err_dir, output_file):
    run_completion_logging(metadata_file, err_dir, output_file)

if __name__ == "__main__":
    # Define paths and parameters
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Define the paths relative to the script's location
    job_nbr = 6
    job_dir = os.path.join(script_dir, f"results/job_batch_{job_nbr}")

    total_articles = 1366
    total_articles = 100
    batch_size = 50

    overwriting = False
    monitor_interval = 60  # Monitor interval in seconds

    try:
        if os.path.exists(job_dir):
            raise FileExistsError(f"Job directory {job_dir} already exists")
        else:
            os.makedirs(job_dir)
    except FileExistsError as e:
        print(e)
        # Handle the error, e.g., prompt the user to choose a different job number
        if overwriting:
            print("Overwriting the existing job directory.")
            shutil.rmtree(job_dir)  # Use shutil.rmtree to remove non-empty directory
            os.makedirs(job_dir)
        else:
            job_nbr = int(input("Please enter a different job number: "))
            job_dir = os.path.join(script_dir, f"results/job_batch_{job_nbr}")
            os.makedirs(job_dir)

    batch_file = os.path.join(job_dir, "batches.json")
    metadata_file = os.path.join(job_dir, "job_metadata.json")
    setup_script = os.path.join(script_dir, "setup_job_env.sh")
    output_file = os.path.join(job_dir, "job_status.json")
    log_file = os.path.join(job_dir, "job_log.json")
    error_log_dir = os.path.join(job_dir, "error_logs")
    err_dir = job_dir
    batch_completion_log_file = os.path.join(script_dir, "batch_completion.log")
    job_range = "1:1"  # Specify the range of jobs to submit

    # Run the batch generator
    run_batch_generator(total_articles, batch_size, batch_file)

    # Run the job creator
    run_job_creator(batch_file, metadata_file, setup_script, job_dir)

    # # Run the job submitter
    run_job_submitter(script_dir, metadata_file)

    # Periodically monitor the jobs

    # # Run the job monitor
    run_job_monitor(metadata_file, output_file, log_file)

    # # Run the error logging process
    run_error_logging_process(metadata_file, err_dir, error_log_dir)

    # # Run the batch completion logging process
    run_completion_logging_process(metadata_file, err_dir, batch_completion_log_file)
