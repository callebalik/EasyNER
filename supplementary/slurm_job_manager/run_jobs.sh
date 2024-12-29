#!/usr/bin/env bash

# This script is used to generate and submit SLURM jobs for the batch processing of articles.

# Preprocessing - Create the job_nbr directory
# promt user for the job number
echo "Enter the job number: "
read job_nbr

# create the job directory
mkdir -p results/job_$job_nbr



# Step 1: Generate the batch intervals
echo "Generating batch intervals..."
# python batch_generator.py --total-articles 1366 --batch-size 40 --output-file batches.json
python batch_generator.py --total-articles 130 --batch-size 40 --output-file batches.json

# Step 2: Generate SLURM jobs and submit them
echo "Generating SLURM jobs..."

python job_creator.py --batch-file batches.json --setup-script setup_job_env.sh

# Step 3: Submit the SLURM jobs and update metadata
echo "Submitting jobs and updating metadata..."
python job_submitter.py --script-dir . --metadata-file job_metadata.json

