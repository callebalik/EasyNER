#!/bin/bash
#SBATCH --job-name=json_to_sql       # Job name
#SBATCH -o %j.out
#SBATCH -e %j.err
#SBATCH --time=03:00:00             # Time limit hrs:min:sec

# write this script to stdout-file - useful for scripting errors
cat $0

# Load required modules (adjust based on your HPC environment)
module load Anaconda3
source config_conda.sh
conda activate easyner_env

# Debug: Print the activated environment
echo "Activated conda environment: $(conda env list | grep '*')"

# Set environment variables
export PYTHONPATH=$PYTHONPATH:$EASYNER_DIR
export DB_RUN=True
# export FILE_SPAN=None
echo "DB_RUN is set to: $DB_RUN"
echo "FILE_SPAN is set to: $FILE_SPAN"

# Remove the existing database
rm /lunarc/nobackup/projects/snic2020-6-41/carl/envidis.db

# Run the conversion script (all configuration is in config.json)
python $EASYNER_DIR/scripts/database/json_to_sqlite.py
