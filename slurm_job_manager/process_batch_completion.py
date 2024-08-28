import os
import re
import json
import argparse


def extract_highest_completion_per_batch(err_file, output_file, batch_range):
    """
    Extracts the highest completion percentage for each batch from a SLURM .err file,
    marking batches not found in the .err file as 'Not Started', and writes the results
    to a single output file for the job.
    """
    if not os.path.exists(err_file):
        print(f"Error: The file {err_file} does not exist.")
        return False

    # Initialize all batches in the range as 'Not Started'
    batch_completion = {str(batch): "Not Started" for batch in batch_range}

    # Regular expression to match lines like 'batch:1255:   14%|'
    completion_pattern = re.compile(r"batch:(\d+):\s+(\d+)%\|")

    found_any_batch = False  # Flag to check if we find any matching lines

    # Read the .err file and check for batch progress
    with open(err_file, "r") as f:
        for line in f:
            match = completion_pattern.search(line)
            if match:
                found_any_batch = True
                batch_number = match.group(1)
                percentage = int(match.group(2))
                # print(f"Found batch {batch_number} with {percentage}% progress.")  # Debugging: Show matching line details

                # Track the highest percentage for each batch
                if batch_number in batch_completion:
                    if batch_completion[batch_number] == "Not Started":
                        batch_completion[batch_number] = percentage
                    else:
                        batch_completion[batch_number] = max(
                            batch_completion[batch_number], percentage
                        )

    if not found_any_batch:
        print(f"No matching batch progress lines found in {err_file}.")
        return False

    # Write the results to the output file
    with open(output_file, "w") as f_out:
        for batch, percentage in batch_completion.items():
            if percentage == "Not Started":
                f_out.write(f"Batch {batch}: Not Started\n")
            else:
                f_out.write(
                    f"Batch {batch}: Highest completion percentage: {percentage}%\n"
                )

    print(f"Highest completion percentages for {err_file} saved to {output_file}")
    return True


def process_batch_completion(job_metadata_file, err_dir, output_dir):
    """
    Processes jobs from job_metadata.json and creates a completion log for each job summarizing
    the highest completion percentage for each batch in the job. Marks batches not started.
    """
    # Read the job metadata
    with open(job_metadata_file, "r") as f:
        job_metadata = json.load(f)

    # Make sure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Process each job in the metadata
    for job_key, job_info in job_metadata.items():
        job_id = job_info.get("job_id")
        job_name = job_info.get("job_name")

        # Infer batch numbers based on the start and end range
        start = job_info.get("start")
        end = job_info.get("end")
        batch_range = [str(i) for i in range(start, end + 1)]

        if job_id:
            # Construct the .err file path (assuming a naming convention like batch_<job_id>.err)
            err_file = os.path.join(err_dir, f"{job_name}.err")
            output_file = os.path.join(output_dir, f"{job_name}_completion.log")

            print(f"Processing completion log for Job {job_name} (ID: {job_id})...")

            # Extract highest completion percentages per batch from the .err file
            result = extract_highest_completion_per_batch(
                err_file, output_file, batch_range
            )

            if not result:
                # Remove the empty log file if no valid data was found
                if os.path.exists(output_file):
                    os.remove(output_file)

        else:
            print(f"No job ID found for batch {job_key}")


def run_completion_logging(metadata_file, err_dir, output_dir):
    """
    Callable function to run the batch completion logging process programmatically.
    This function can be called from other Python scripts.
    """
    process_batch_completion(metadata_file, err_dir, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Summarize the highest completion percentage per batch from SLURM .err files"
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
        help="Directory to save completion summary logs",
    )

    args = parser.parse_args()

    # Process the jobs and extract highest completion percentages per batch from their .err files
    process_batch_completion(args.metadata_file, args.err_dir, args.output_dir)
