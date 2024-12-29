#!/usr/bin/env bash

# Step 1: Generate the batch intervals
echo "Generating batch intervals..."
python batch_generator.py --total-articles 1366 --batch-size 5 --output-file batches.json

# Step 2: Generate SLURM jobs and submit them
echo "Generating SLURM jobs..."

python job_creator.py --batch-file batches.json --setup-script setup_job_env.sh

# Step 3: Submit the SLURM jobs and update metadata
echo "Submitting jobs and updating metadata..."
python job_submitter.py --script-dir . --metadata-file job_metadata.json

