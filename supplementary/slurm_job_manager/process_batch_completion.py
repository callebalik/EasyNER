import os
import re
import json
import argparse


def extract_highest_completion_per_batch(err_file, batch_range):
    """
    Extracts the highest completion percentage for each batch from a SLURM .err file,
    marking batches not found in the .err file as 'Not Started', and returns the results
    as a dictionary keyed by batch number.
    """
    if not os.path.exists(err_file):
        print(f"Error: The file {err_file} does not exist.")
        return None

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
        return None

    # Return the dictionary with batch completion percentages
    return batch_completion


def process_batch_completion(job_metadata_file, err_dir, output_file, rerun_file):
    """
    Processes jobs from job_metadata.json and writes a single completion log
    summarizing the highest completion percentage for each batch in all jobs.
    Additionally, updates the job metadata with the completion status.
    """
    # Read the job metadata
    with open(job_metadata_file, "r") as f:
        job_metadata = json.load(f)

    total_batches = 0
    total_completed_batches = 0
    incomplete_batches = []

    # Open the output file to write the combined results
    with open(output_file, "w") as f_out:
        # Process each job in the metadata
        for job_key, job_info in job_metadata.items():
            job_id = job_info.get("job_id")
            job_name = job_info.get("job_name")

            # Infer batch numbers based on the start and end range
            start = job_info.get("start")
            end = job_info.get("end")
            batch_range = [str(i) for i in range(start, end + 1)]

            if job_id:
                # Construct the .err file path (assuming a naming convention like batch_<job_name>.err)
                err_file = os.path.join(err_dir, f"{job_name}.err")

                print(f"Processing completion log for Job {job_name} (ID: {job_id})...")

                # Extract highest completion percentages per batch from the .err file
                batch_completion = extract_highest_completion_per_batch(
                    err_file, batch_range
                )

                if batch_completion:
                    completed_batches = sum(1 for p in batch_completion.values() if p == 100)
                    total_batches += len(batch_completion)
                    total_completed_batches += completed_batches
                    completion_percentage = (completed_batches / len(batch_completion)) * 100

                    # Initialize counters for unstarted and in-progress batches
                    unstarted_batches = 0
                    in_progress_batches = 0
                    all_batches_completed = True

                    # Write the results to the output file with job name
                    f_out.write(f"Job {job_name} (ID: {job_id}) - {completed_batches}/{len(batch_completion)} ({completion_percentage:.2f}%):\n")
                    for batch, percentage in batch_completion.items():
                        if percentage == "Not Started":
                            unstarted_batches += 1
                            all_batches_completed = False
                            f_out.write(f"  Batch {batch}: Not Started\n")
                        elif percentage < 100:
                            in_progress_batches += 1
                            all_batches_completed = False
                            f_out.write(
                                f"  Batch {batch}: Highest completion percentage: {percentage}%\n"
                            )
                        else:
                            f_out.write(
                                f"  Batch {batch}: Highest completion percentage: {percentage}%\n"
                            )
                        if percentage == "Not Started" or percentage < 100:
                            incomplete_batches.append((job_name, batch))
                    f_out.write("\n")

                    # Update the job metadata with completion status
                    job_info["all_batches_completed"] = all_batches_completed
                    job_info["batches_in_progress"] = in_progress_batches
                    job_info["unstarted_batches"] = unstarted_batches

                else:
                    print(f"No valid data found for job {job_name} (ID: {job_id}).")

            else:
                print(f"No job ID found for batch {job_key}")

        # Calculate and write the total completion progress bar
        total_completion_percentage = (total_completed_batches / total_batches) * 100 if total_batches > 0 else 0
        progress_bar = f"[{'#' * int(total_completion_percentage // 2)}{' ' * (50 - int(total_completion_percentage // 2))}] {total_completion_percentage:.2f}%"
        f_out.write(f"Total Completion Progress: {progress_bar}\n\n")

    # Save the list of incomplete batches to the rerun file
    with open(rerun_file, "w") as f_rerun:
        for job_name, batch in incomplete_batches:
            f_rerun.write(f"{job_name} Batch {batch}\n")

    # Save the updated job metadata back to the file
    with open(job_metadata_file, "w") as f:
        json.dump(job_metadata, f, indent=2)

    print(f"Job metadata updated with completion status.")


def run_completion_logging(metadata_file, err_dir, output_file, rerun_file):
    """
    Callable function to run the batch completion logging process programmatically.
    This function can be called from other Python scripts.
    """
    process_batch_completion(metadata_file, err_dir, output_file, rerun_file)


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
        "--output-file",
        type=str,
        required=True,
        help="File to save the combined completion summary log",
    )
    parser.add_argument(
        "--rerun-file",
        type=str,
        required=True,
        help="File to save the list of incomplete batches",
    )

    args = parser.parse_args()

    # Process the jobs and extract highest completion percentages per batch from their .err files
    process_batch_completion(args.metadata_file, args.err_dir, args.output_file, args.rerun_file)
