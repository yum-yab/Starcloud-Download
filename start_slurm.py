#!/usr/bin/env python3
import os
import json
import subprocess
from pathlib import Path
from dotenv import load_dotenv

if __name__ == "__main__":
# --- Load .env ---
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)

    # --- Load S_TILES and S_YEARS ---
    tiles = json.loads(os.getenv("S_TILES", "[]"))
    years = json.loads(os.getenv("S_YEARS", "[]"))

    chunks = int(os.getenv("S_SPLIT_FILES", "1"))

    current_user = os.environ["USER"]

    if not tiles or not years or not chunks:
        raise RuntimeError("S_TILES and S_YEARS must be non-empty JSON arrays.")

    
    # --- Compute array size ---
    array_size = len(tiles) * len(years) * chunks
    print(f"Submitting Slurm array job with {array_size} tasks")

    # --- Build sbatch command ---
    bash_wrapper = Path(__file__).parent / "slurm_wrapper.sh"

    limit_concurrent = int(os.getenv("S_LIMIT_CONCURRENT", 10))

    job_name = f"csdc_dl_{'_'.join(map(str, years))}" 

    log_base = f"/work/{current_user}/logs/csdc_dl/{job_name}"

    Path(log_base).mkdir(parents=True, exist_ok=True)

    sbatch_command = [
        "sbatch",
        f"--job-name={job_name}",
        f"--array=0-{array_size-1}%{limit_concurrent}",
        "--export=ALL",
        f"--output={log_base}/%x-%A_%a.log",
        f"--error={log_base}/%x-%A_%a.error",
        str(bash_wrapper),
    ]

    # --- Submit job ---
    print("Running:", " ".join(sbatch_command))
    result = subprocess.run(sbatch_command, check=True, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)