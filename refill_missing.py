from pathlib import Path

from requests import auth
from sc_login import AuthData, LoginCredentials, performLogin
from starcloud_dl import DEFAULT_CHUNK_SIZE, dl_file_by_id
from validate_starcloud_dl import validate_year
import argparse
import polars as pl
from dotenv import load_dotenv
import os


def fetch_missing_files(path: Path, year: int) -> pl.DataFrame:
    df = validate_year(path=path, year=year, print_stats=False)

    result = df.filter(pl.col("status") != "complete")

    if result.height > 0:
        print(f"Missing files for year {year}: {result.height}")
    return result


def parse_args() -> tuple[list[int], bool]:
    parser = argparse.ArgumentParser()

    _ = parser.add_argument(
        "--slurm-years",
        type=str,
        nargs="+",
        required=True,
        help="One or more years (e.g. --slurm-years 2024 2025 or 2024-2026)",
    )

    _ = parser.add_argument(
        "--check",
        action="store_true",
        help="Enable check mode (default: False)",
    )

    args = parser.parse_args()

    slurm_years: list[str] = args.slurm_years
    check: bool = args.check

    if len(slurm_years) == 1 and "-" in slurm_years[0]:
        start, end = map(int, slurm_years[0].split("-"))
        years = list(range(start, end))
    else:
        years = list(map(int, slurm_years))

    return years, check


if __name__ == "__main__":
    import sys

    years, just_check = parse_args()

    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)

    root_dir: Path = Path(os.environ["S_ROOT_DIR"])

    email: str = os.environ["STAR_EMAIL"]
    password: str = os.environ["STAR_PASSWORD"]
    creds = LoginCredentials(email, password)

    missing_files_df = pl.concat([fetch_missing_files(root_dir, y) for y in years])


    missing_tiles = missing_files_df.get_column('tile').unique().len()

    print(f"Missing files for {len(years)} years ({missing_tiles} diff. tiles missing): {missing_files_df.height}")


    if just_check:
        sys.exit(0)

    if missing_files_df.height == 0:
        print("No missung files, nothing to do!")
        sys.exit(0)

    try:
        authData: AuthData = performLogin(creds)
    except Exception as e:
        print(f"Error authenticating for star cloud: {str(e)}")
        sys.exit(1)

    for row in missing_files_df.iter_rows(named=True):

        year, tile_id, fname = (row['year'], row['tile'], row['filename'])
        target_dir = root_dir / str(year) / tile_id

        try:
            dl_file_by_id(
                tile_id=tile_id,
                year=year,
                auth=authData,
                filename=fname,
                target_dir=target_dir,
                show_live_progress=True,
                chunk_size=DEFAULT_CHUNK_SIZE * 4,
            )
        except Exception as e:
            print(f"Failed to download file to {target_dir / fname}! Reason: {e}")
