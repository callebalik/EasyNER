import os
import json
import argparse

def generate_slurm_script(start, end, batch_number, setup_script="setup_env.sh"):
    """
    Generates and saves a SLURM job script for processing a batch of articles.
    Embeds the setup script content directly into the SLURM job script, skipping the shebang (#!/bin/bash).
    """
    # Define the job name
    job_name = f"batch_{batch_number}"
    
    # Read the content of the setup script (setup_env.sh), skipping the first line if it's a shebang
    with open(setup_script, 'r') as f:
        setup_script_lines = f.readlines()
    
    # Skip the first line if it starts with #! (the shebang)
    if setup_script_lines[0].startswith("#!"):
        setup_script_content = ''.join(setup_script_lines[1:])
    else:
        setup_script_content = ''.join(setup_script_lines)

    # Create the SLURM job script with the embedded setup content (without duplicate shebang)
    slurm_script = f"""#!/bin/bash
#SBATCH -A berzelius-2024-146  # Your project account
#SBATCH --gpus=0                # Request 2 GPUs across the nodes
#SBATCH --mail-user=tna14col@student.lu.se
#SBATCH --mail-type=END
#SBATCH --job-name={job_name}
#SBATCH --output={job_name}.out
#SBATCH --error={job_name}.err
#SBATCH --time=07:00:00

# Set the ARTICLE_LIMIT environment variable for this batch
export ARTICLE_LIMIT="{start}:{end}"

# Begin Environment Setup (from {setup_script})
{setup_script_content}

# Run the main Python script
python main.py
"""

    script_filename = f"{job_name}.slurm"
    
    # Write the SLURM script to a file
    with open(script_filename, 'w') as script_file:
        script_file.write(slurm_script)
    
    print(f"SLURM script for {job_name} generated: {script_filename}")
    return script_filename, job_name

def load_batches_from_file(batch_file):
    """
    Loads batch intervals from a file (JSON format).
    :param batch_file: File containing batch intervals
    :return: List of batch intervals (start, end tuples)
    """
    with open(batch_file, 'r') as f:
        batches = json.load(f)
    return batches

def create_jobs(batch_file, metadata_file, setup_script="setup_env.sh"):
    """
    Loads the batch intervals from a file, generates SLURM job scripts, and saves metadata.
    """
    # Load batches from the file
    batches = load_batches_from_file(batch_file)
    job_metadata = {}

    # Generate SLURM scripts for each batch
    for i, (start, end) in enumerate(batches, 1):
        script_filename, job_name = generate_slurm_script(start, end, i, setup_script=setup_script)
        
        # Store metadata for the job, including job name
        job_metadata[f"batch_{i}"] = {
            "batch_number": i,
            "start": start,
            "end": end,
            "script_name": script_filename,
            "job_name": job_name
        }

    # Save metadata to a file
    with open(metadata_file, 'w') as f:
        json.dump(job_metadata, f, indent=2)

    print(f"Metadata saved to {metadata_file}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SLURM job scripts based on batch intervals")
    parser.add_argument('--batch-file', type=str, required=True, help="File containing batch intervals (JSON format)")
    parser.add_argument('--metadata-file', type=str, default='job_metadata.json', help="File to store job metadata")
    parser.add_argument('--setup-script', type=str, default='setup_env.sh', help="Path to the environment setup script")
    
    args = parser.parse_args()

    # Create the job scripts
    create_jobs(args.batch_file, args.metadata_file, setup_script=args.setup_script)
    
    print("Job scripts created and metadata saved.")
