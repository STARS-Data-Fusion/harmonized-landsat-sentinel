#!/bin/bash

#SBATCH --job-name=hls-kings-2022
#SBATCH --output=logs/hls_kings_2022_%j.log
#SBATCH --error=logs/hls_kings_2022_%j.err
#SBATCH --time=48:00:00
#SBATCH --mem=2GB
#SBATCH --cpus-per-task=4
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=${USER}@example.com

# Activate conda environment
source $(conda info --base)/etc/profile.d/conda.sh
conda activate harmonized-landsat-sentinel

# Navigate to project directory
cd /Users/halverso/Projects/harmonized-landsat-sentinel

# Run the dataset generation script
echo "Starting HLS dataset generation for 2022..."
python generate_upper_kings_dataset_2022.py

echo "HLS dataset generation for 2022 completed with exit code: $?"
