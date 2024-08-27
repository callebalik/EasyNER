#!/usr/bin/env bash

# Load the necessary modules only if not already loaded
module list 2>&1 | grep -q "Anaconda/2023.09-0-hpc1-bdist"
if [ $? -ne 0 ]; then
    echo "Loading Anaconda module..."
    module load Anaconda/2023.09-0-hpc1-bdist
else
    echo "Anaconda module already loaded"
fi

# If not a SLURM job, source conda.sh to make sure conda is available
#if [ -z "$SLURM_JOB_ID" ]; then
#    echo "Sourcing conda.sh because this is not a SLURM job"
#    source ~/anaconda3/etc/profile.d/conda.sh  # Adjust the path if necessary
#else
#    echo "SLURM job detected, skipping conda.sh sourcing"
#fi

# Check if the conda environment already exists
if conda info --envs | grep -q "^easyner_env"; then
    echo "Conda environment 'easyner_env' already exists"
else
    echo "Creating conda environment from environment.yml..."
    conda env create -f environment.yml
fi

# Activate the conda environment
conda activate easyner_env

# Install the spaCy language model if not already installed
if python -c "import spacy; spacy.load('en_core_web_sm')" 2>/dev/null; then
    echo "spaCy language model 'en_core_web_sm' is already installed"
else
    echo "Installing spaCy language model 'en_core_web_sm'..."
    python -m spacy download en_core_web_sm
fi
