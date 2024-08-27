#!/usr/bin/env bash

# Step 1: Generate the batch intervals
echo "Generating batch intervals..."
python batch_generator.py --total-articles 1366 --batch-size 114 --output-file batches.json

# Step 2: Generate SLURM jobs and submit them
echo "Generating SLURM jobs..."

python job_creator.py --batch-file batches.json --setup-script setup_job_env.sh
