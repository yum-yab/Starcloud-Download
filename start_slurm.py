#!/usr/bin/env python3
from typing import Any


import os
import json
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import polars as pl
import argparse

from validate_starcloud_dl import incomplete_tiles, validate_year


def fetch_missing_tiles(path: Path, year: int) -> list[str]:
    df = validate_year(path=path, year=year, print_stats=True)

    incomplete_tiles: list[str] = (
        df.filter(pl.col("status") != "complete").get_column("tile").unique().to_list()
    )

    return incomplete_tiles


def parse_args() -> list[int]:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument(
        "--slurm-years",
        type=int,
        nargs="+",
        required=True,
        help="One or more years (e.g. --slurm-years 2024 2025)",
    )

    args = parser.parse_args()

    slurm_years: list[int] = args.slurm_years

    return slurm_years


if __name__ == "__main__":
    # --- Load .env ---
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)

    root_dir: Path = Path(os.environ["S_ROOT_DIR"])

    # --- Load S_TILES and S_YEARS ---

    years = parse_args()

    tiles: list[str] = []

    for y in years:
        incomplete_tiles = fetch_missing_tiles(root_dir / str(y), y)

        unique_tiles = list(set(tiles + incomplete_tiles))

        tiles = unique_tiles

    print(f"Found unique missing tiles for years: {tiles}")

    chunks = int(os.getenv("S_SPLIT_FILES", "4"))

    current_user = os.environ["USER"]

    if not tiles or not years or not chunks:
        raise RuntimeError("S_TILES and S_YEARS must be passed as arguments.")

    # --- Compute array size ---
    array_size = len(tiles) * len(years) * chunks
    print(f"Submitting Slurm array job with {array_size} tasks")

    # --- Build sbatch command ---
    bash_wrapper: Path = Path(__file__).parent / "slurm_wrapper.sh"

    limit_concurrent = int(os.getenv("S_LIMIT_CONCURRENT", 10))

    job_name = f"csdc_dl_{'_'.join(map(str, years))}"

    log_base = f"/work/{current_user}/logs/csdc_dl/{job_name}"

    Path(log_base).mkdir(parents=True, exist_ok=True)

    sbatch_command = [
        "sbatch",
        f"--job-name={job_name}",
        f"--array=0-{array_size - 1}%{limit_concurrent}",
        "--export=ALL",
        f"--output={log_base}/%x-%A_%a.log",
        f"--error={log_base}/%x-%A_%a.error",
        str(bash_wrapper),
        "--slurm-years",
        *[str(y) for y in years],
        "--slurm_tiles",
        f"'{json.dumps(tiles)}'",
    ]

    # --- Submit job ---
    print("Running:", " ".join(sbatch_command))
    result = subprocess.run(sbatch_command, check=True, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
