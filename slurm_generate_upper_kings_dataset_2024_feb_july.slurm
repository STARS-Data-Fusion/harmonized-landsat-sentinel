#!/bin/bash

#SBATCH --job-name=hls-kings-2024
#SBATCH --output=logs/hls_kings_2024_%j.log
#SBATCH --error=logs/hls_kings_2024_%j.err
#SBATCH --open-mode=append
#SBATCH --time=48:00:00
#SBATCH --mem=2GB
#SBATCH --cpus-per-task=4
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=${USER}@example.com

# Activate conda environment
source $(conda info --base)/etc/profile.d/conda.sh
conda activate harmonized-landsat-sentinel

# Navigate to project directory
cd $HOME/data/harmonized-landsat-sentinel

# Run the dataset generation script
echo "Starting HLS dataset generation for 2024 February though July..."
python -u generate_upper_kings_dataset_2024_feb_july.py

echo "HLS dataset generation for 2024 completed with exit code: $?"
