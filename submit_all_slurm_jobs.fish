#!/usr/bin/env fish

# Script to submit all SLURM jobs for HLS dataset generation
# Usage: ./submit_all_slurm_jobs.fish

set -l script_dir (dirname (status filename))
cd $script_dir

echo "Submitting SLURM jobs for HLS dataset generation..."
echo ""

# Array of SLURM script files
set -l slurm_scripts \
    slurm_generate_upper_kings_dataset_2022.slurm \
    slurm_generate_upper_kings_dataset_2023.slurm \
    slurm_generate_upper_kings_dataset_2024.slurm \
    slurm_generate_upper_kings_dataset_2025.slurm

# Submit each job and store job IDs
set -l job_ids ()

for script in $slurm_scripts
    if test -f $script
        echo "Submitting $script..."
        set -l job_id (sbatch $script | awk '{print $NF}')
        set job_ids $job_ids $job_id
        echo "  Job ID: $job_id"
    else
        echo "Warning: $script not found!"
    end
end

echo ""
echo "All jobs submitted!"
echo "Job IDs: $job_ids"
echo ""
echo "Monitor job status with: squeue -u $USER"
