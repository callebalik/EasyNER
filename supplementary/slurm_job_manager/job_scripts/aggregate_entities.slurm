#!/bin/bash
#SBATCH --job-name=aggregate_antities       # Job name
#SBATCH -o aggregate_antities_%j.out        # Standard output file
#SBATCH -e aggregate_antities_%j.err        # Standard error file
#SBATCH --time=04:00:00                     # Time limit hrs:min:sec
#SBATCH -N 1
#SBATCH --ntasks-per-node=32

# write this script to stdout-file - useful for scripting errors
cat $0

# Load required modules (adjust based on your HPC environment)
module load Anaconda3
source config_conda.sh
conda activate easyner_env


# Debug: Print the activated environment
echo "Activated conda environment: $(conda env list | grep '*')"


# Set environment variables
export PYTHONPATH=$PYTHONPATH:/home/carloa/Desktop/EasyNer
export DB_RUN=True
# export FILE_SPAN=None
echo "DB_RUN is set to: $DB_RUN"
echo "FILE_SPAN is set to: $FILE_SPAN"

# Run the conversion script (all configuration is in config.json)
python -m scripts.database.aggregate_antities
