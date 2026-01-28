from logging import Logger


from starcloud_dl import (
    loadCredsFromEnv,
    DEFAULT_CHUNK_SIZE,
    indexAlreadyDownloadedFiles,
    ListSplitChoose,
    get_filenames_for_id,
    dl_file_list,
)
from sc_login import LoginCredentials, AuthData, performLogin
import os
import json
import itertools
from pathlib import Path
from dotenv import load_dotenv
import logging
import argparse
import sys

# --- Load .env ---
_ = load_dotenv()


LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

logger: Logger = logging.getLogger(name=__name__)



def parse_args() -> tuple[list[int], list[str]]:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument(
        "--slurm-years",
        type=int,
        nargs="+",
        required=True,
        help="One or more years (e.g. --slurm-years 2024 2025)",
    )
    _ = parser.add_argument(
        "--slurm-tiles",
        type=json.loads,
        required=True,
        help='JSON array of tiles (e.g. --slurm-tiles \'["tileA","tileB"]\')',
    )

    args = parser.parse_args()

    slurm_years: list[int] = args.slurm_years
    slurm_tiles: list[str] = args.slurm_tiles

    return slurm_years, slurm_tiles

if __name__ == "__main__":
    if (
        # "S_TILES" not in os.environ
        # or "S_YEARS" not in os.environ
        # or "S_ROOT_DIR" not in os.environ
        "S_ROOT_DIR" not in os.environ
    ):
        raise RuntimeError(
            # "If executed by slurm the tasks need to know which keys are handled! S_ROOT_DIR, S_HANDLED_TILES and S_HANDLED_YEARS need to be set need to be set!"
            "If executed by slurm the tasks need to know which keys are handled! S_ROOT_DIR need to be set need to be set!"
        )

    slurm_years, slurm_tiles = parse_args()

    chunks = int(os.getenv("S_SPLIT_FILES", "1"))

    working_dir: Path = Path(os.environ.get("SLURM_SUBMIT_DIR", "."))
    root_dir: Path = Path(os.environ["S_ROOT_DIR"])

    slurm_array_job_id: str | None = os.getenv("SLURM_ARRAY_TASK_ID")

    if slurm_array_job_id is None:
        raise RuntimeError("SLURM Starcloud download needs to be a SLURM array job!")
    else:
        job_index: int = int(slurm_array_job_id)

    


    tile_id, year, chunk_id = list[tuple[str, int, int]](
        itertools.product(slurm_tiles, slurm_years, range(chunks))
    )[job_index]

    creds: LoginCredentials = loadCredsFromEnv(envfilePath=working_dir / ".env")

    # t_before_index: float = time.perf_counter()

    # setup target directory if non-existent
    target_dir = root_dir / str(year) / tile_id

    if not target_dir.exists():
        target_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created directory {target_dir}")

    # fetch already loaded files
    file_index: dict[str, int] | None = (
        indexAlreadyDownloadedFiles(path=target_dir)
        if bool(os.getenv("S_CREATE_INDEX"))
        else None
    )

    # logger.info(msg=f'Perf loading index: {(time.perf_counter() - t_before_index):.3f}')

    # set CHunk choosing
    list_split_chooser = ListSplitChoose(i=chunk_id, n=chunks)

    try:
        file_names = get_filenames_for_id(
            tile_id=tile_id,
            year=year,
            index=file_index,
            list_split_chooser=list_split_chooser,
            write_resp_to_disk=target_dir
        )
    except Exception as e:
        logger.error(f"Error accessing file list: {str(e)}")
        sys.exit(1)

    if len(file_names) == 0:
        logger.info(
            f"No files left for array task {job_index}, {tile_id}, {year}, {list_split_chooser}: Exiting..."
        )
        sys.exit(0)
    else:
        logger.info(msg=f"Found {len(file_names)} for downloading!")

    try:
        authData: AuthData = performLogin(creds)
    except Exception as e:
        logger.error(f"Error authenticating for star cloud: {str(e)}")
        sys.exit(1)

    try:
        dl_file_list(
            tile_id=tile_id,
            year=year,
            target_dir=target_dir,
            auth=authData,
            filename_list=file_names,
            show_live_progress=False,
            chunk_size=DEFAULT_CHUNK_SIZE * 4,
            log_time=True,
        )
    except Exception as e:
        logger.error(msg=f"Error during fetching data. Reason: {str(e)}")
