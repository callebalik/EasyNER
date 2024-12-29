# Slurm Job Manager Scripts

## Overview
This directory contains various scripts used to manage and monitor jobs in a Slurm workload manager environment. These scripts help in generating job batches, submitting jobs, monitoring job statuses, and processing job errors.

## Project Structure
```
slurm_job_manager
├── batch_generator.py            # Script to generate batch intervals for articles
├── job_creator.py                # Script to create SLURM job scripts based on batch intervals
├── job_monitor.py                # Script to monitor SLURM jobs and export results
├── job_submitter.py              # Script to submit SLURM job scripts and update job metadata
├── process_batch_completion.py   # Script to process batch completion and update metadata
├── process_job_errors.py         # Script to process job errors and create error logs
├── run_job_processor.py          # Script to run error logging and batch completion logging
├── run_jobs.sh                   # Shell script to generate batches, create jobs, and submit them
├── setup_job_env.sh              # Shell script to set up the job environment
└── monitor_jobs.sh               # Shell script to execute the job monitoring process
```

## Scripts Description

### batch_generator.py
Generates batch intervals for articles and saves them to a JSON file.

**Usage:**
```sh
python batch_generator.py --total-articles <total_articles> --batch-size <batch_size> --output-file <output_file>
```

### job_creator.py
Creates SLURM job scripts based on batch intervals and saves job metadata.

**Usage:**
```sh
python job_creator.py --batch-file <path_to_batch_file> --metadata-file <path_to_metadata_file> --setup-script <path_to_setup_script>
```

### job_monitor.py
Monitors SLURM jobs using job metadata and exports the results to JSON and log files.

**Usage:**
```sh
python job_monitor.py --metadata-file <path_to_metadata_file> --output-file <path_to_output_file> --log-file <path_to_log_file>
```

### job_submitter.py
Submits SLURM job scripts and updates the job metadata with job IDs.

**Usage:**
```sh
python job_submitter.py --script-dir <path_to_script_directory> --metadata-file <path_to_metadata_file>
```

### process_batch_completion.py
Processes batch completion logs and updates job metadata with completion status.

**Usage:**
```sh
python process_batch_completion.py --metadata-file <path_to_metadata_file> --err-dir <path_to_error_directory> --output-file <path_to_output_file>
```

### process_job_errors.py
Processes job errors from SLURM .err files and creates error logs.

**Usage:**
```sh
python process_job_errors.py --metadata-file <path_to_metadata_file> --err-dir <path_to_error_directory> --output-file <path_to_output_file>
```

### run_job_processor.py
Runs the error logging and batch completion logging processes programmatically.

**Usage:**
```sh
python run_job_processor.py
```

### run_jobs.sh
Shell script to generate batch intervals, create SLURM jobs, and submit them.

**Usage:**
```sh
./run_jobs.sh
```

### setup_job_env.sh
Sets up the job environment by loading necessary modules and activating the conda environment.

**Usage:**
```sh
./setup_job_env.sh
```

### monitor_jobs.sh
Executes the job monitoring process by running the job_monitor.py script.

**Usage:**
```sh
./monitor_jobs.sh
```

## Usage
To use these scripts, follow the instructions provided in each script's docstring or comments. Ensure that you have the necessary dependencies and environment set up as described in the `setup_job_env.sh` script.

## Installation

3. Generate batch intervals:
   ```sh
   python batch_generator.py --total-articles <total_articles> --batch-size <batch_size> --output-file batches.json
   ```

4. Create SLURM job scripts:
   ```sh
   python job_creator.py --batch-file batches.json --metadata-file job_metadata.json --setup-script setup_job_env.sh
   ```

5. Submit the SLURM jobs:
   ```sh
   python job_submitter.py --script-dir . --metadata-file job_metadata.json
   ```

6. Monitor the jobs:
   ```sh
   ./monitor_jobs.sh
   ```

7. Process job errors and completion logs:
   ```sh
   python run_job_processor.py
   ```

## Notes
- Ensure that the paths to the input files, output files, and setup scripts are correctly specified.
- Modify the SLURM job script parameters (e.g., account, time, GPUs) as needed in the `job_creator.py` script.
- The `setup_job_env.sh` script assumes the use of Anaconda for environment management. Adjust the script if you use a different environment manager.