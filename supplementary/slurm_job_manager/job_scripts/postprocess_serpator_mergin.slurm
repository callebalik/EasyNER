#!/bin/bash

#SBATCH -t 04:00:00
#SBATCH -J merge_separators
#SBATCH -n 1
#SBATCH --mem=16G
#SBATCH -o merge_separators_%j.out
#SBATCH -e merge_separators_%j.err

# write this script to stdout-file - useful for debugging
cat $0

# Load required modules
module load Anaconda3



# Activate the conda environment if you're using one
source config_conda.sh
conda activate easyner_env

# Set the input directory - modify this path as needed
INPUT_DIR="/lunarc/nobackup/projects/snic2020-6-41/carl/ner_merged_plurals_and_word_count/"

# Run the Python script
python /home/carloa/Desktop/EasyNer/supplementary/experiment_scripts/postprocess_separator_merging.py $INPUT_DIR