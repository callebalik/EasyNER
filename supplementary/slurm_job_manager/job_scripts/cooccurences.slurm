#!/bin/bash
#SBATCH --job-name=cooccurences       # Job name
#SBATCH -o logs/%x/%A_%a_%j.out        # Standard output file
#SBATCH -e logs/%x/%A_%a_%j.err        # Standard error file
#SBATCH --time=02:00:00                     # Time limit hrs:min:sec
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
export DB_PATH=/lunarc/nobackup/projects/snic2020-6-41/carl/dev.db
export LOGGING_LEVEL_CONSOLE=DEBUG
echo "DB_RUN is set to: $DB_RUN"
echo "FILE_SPAN is set to: $FILE_SPAN"

# Run the conversion script (all configuration is in config.json)
python -c "
from scripts.database.db_main import EasyNerDBHandler

with EasyNerDBHandler() as db:
    db.co.analysis.record_dis_pnm()
"

# Exit the script
exit 0
