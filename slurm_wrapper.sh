#!/bin/bash
#SBATCH --job-name=csdc_dl
#SBATCH --output=/work/%u/logs/csdc_dl/%x-%A_%a.log
#SBATCH --error=/work/%u/logs/csdc_dl/%x-%A_%a.error

#SBATCH --time=0-00:40:00
#SBATCH --mem-per-cpu=1G
#SBATCH -c 1


# --- Load required modules ---
module load GCCcore/13.3.0
module load Python/3.12.3

# --- Activate virtual environment in current dir ---
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
else
    echo "Virtual environment not found in .venv"
    exit 1
fi

# --- Execute Python script ---
python slurm_main.py
