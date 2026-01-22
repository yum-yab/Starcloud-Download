#!/usr/bin/env python3
import os
import json
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# --- Load .env ---
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

# --- Load S_TILES and S_YEARS ---
tiles = json.loads(os.getenv("S_TILES", "[]"))
years = json.loads(os.getenv("S_YEARS", "[]"))

if not tiles or not years:
    raise RuntimeError("S_TILES and S_YEARS must be non-empty JSON arrays")

# --- Compute array size ---
array_size = len(tiles) * len(years)
print(f"Submitting Slurm array job with {array_size} tasks")

# --- Build sbatch command ---
bash_wrapper = Path(__file__).parent / "slurm_wrapper.sh"

limit_concurrent = 50

sbatch_command = [
    "sbatch",
    f"--array=0-{array_size-1}%{limit_concurrent}",
    "--export=ALL",
    str(bash_wrapper),
]

# --- Submit job ---
print("Running:", " ".join(sbatch_command))
result = subprocess.run(sbatch_command, check=True, capture_output=True, text=True)
print(result.stdout)
print(result.stderr)