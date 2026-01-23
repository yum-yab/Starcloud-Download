#!/bin/bash

#SBATCH --time=0-01:00:00
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
