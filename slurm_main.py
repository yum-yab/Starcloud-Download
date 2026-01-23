from typing import Any


from logging import Logger


from starcloud_dl import LoginCredentials, dl_years_for_tile, loadCredsFromEnv, DEFAULT_CHUNK_SIZE, indexAlreadyDownloadedFiles
import os
import json
import itertools
from pathlib import Path
from dotenv import load_dotenv
import logging
import requests
import sys
import time

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


if __name__ == "__main__":

    if "S_TILES" not in os.environ or "S_YEARS" not in os.environ or "S_ROOT_DIR" not in os.environ:
        raise RuntimeError('If executed by slurm the tasks need to know which keys are handled! S_ROOT_DIR, S_HANDLED_TILES and S_HANDLED_YEARS need to be set need to be set!')

    working_dir: Path = Path(os.environ.get("SLURM_SUBMIT_DIR", "."))
    root_dir: Path = Path(os.environ["S_ROOT_DIR"])

    slurm_array_job_id: str | None = os.getenv("SLURM_ARRAY_TASK_ID")

    if slurm_array_job_id is None:

        raise RuntimeError('SLURM Starcloud download needs to be a SLURM array job!')
    else:
        job_index: int = int(slurm_array_job_id)

    


    slurm_tiles: list[str] = json.loads(s=os.environ["S_TILES"])
    slurm_years: list[int] = json.loads(s=os.environ["S_YEARS"])

    tile_id, year = list[tuple[str, int]](itertools.product(slurm_tiles, slurm_years))[job_index]

    creds: LoginCredentials = loadCredsFromEnv(envfilePath=working_dir / ".env")

    t_before_index: float = time.perf_counter()

    file_index: dict[str, int] | None = indexAlreadyDownloadedFiles(path=root_dir / str(year) / tile_id) if bool(os.getenv("S_CREATE_INDEX")) else None

    logger.info(msg=f'Perf loading index: {(time.perf_counter() - t_before_index):.3f}')
    # if bool(os.getenv("S_CREATE_INDEX")):
    #     # create index from one dir since were not going to dl more anyway
    #     file_index: dict[str, int] = 
    # else:
    #     file_index: dict[str, int] = None

    if file_index is not None:
        logger.info(msg=f"Fetched index: {file_index}")
    
    try:
        dl_years_for_tile(
            tile_id=tile_id,
            years=[year],
            root_dir=root_dir,
            creds=creds,
            show_live_progress=False,
            dl_index=file_index,
            # greater chunks for gpfs
            chunkSize=DEFAULT_CHUNK_SIZE * 4
        )
    except RuntimeError as e:
        logger.error(msg=e)
        exit(code=1)
    except requests.exceptions.ChunkedEncodingError as e:
        logger.error(msg=f"Connection reset by server for tile: {tile_id} and year: {year}")
        exit(code=1)