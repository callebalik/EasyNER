#!/bin/bash
#SBATCH --job-name=analysis          # Job name
#SBATCH -o %x_%j.out                 # Standard output with job name prefix
#SBATCH -e %x_%j.err                 # Standard error with job name prefix
#SBATCH --time=09:00:00              # Time limit hrs:min:sec

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


# Import database class from python database module to the slurm script
# Run the conversion script (all configuration is in config.json)
python /home/carloa/Desktop/EasyNer/scripts/database/analysis.py




