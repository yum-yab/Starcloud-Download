from starcloud_dl import dl_years_for_tile, loadCredsFromEnv, DEFAULT_CHUNK_SIZE, indexAlreadyDownloadedFiles
import os
import json
import itertools
from pathlib import Path
from dotenv import load_dotenv
import logging
import requests
import sys

# --- Load .env ---
load_dotenv()


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


if __name__ == "__main__":

    working_dir = Path(os.environ.get("SLURM_SUBMIT_DIR", "."))
    root_dir = Path(os.getenv("S_ROOT_DIR"))

    slurm_array_job_id = os.getenv("SLURM_ARRAY_TASK_ID")

    if slurm_array_job_id is None:

        raise RuntimeError('SLURM Starcloud download needs to be a SLURM array job!')
    else:
        slurm_array_job_id = int(slurm_array_job_id)

    
    if "S_TILES" not in os.environ or "S_YEARS" not in os.environ:
        raise RuntimeError('If executed by slurm the tasks need to know which keys are handled! SLURM_HANDLED_TILES and SLURM_HANDLED_YEARS need to be set need to be set!')

    slurm_tiles = json.loads(os.environ["S_TILES"])
    slurm_years = json.loads(os.environ["S_YEARS"])

    tile_id, year = list(itertools.product(slurm_tiles, slurm_years))[slurm_array_job_id]

    creds = loadCredsFromEnv(working_dir / ".env")

    if bool(os.getenv("S_CREATE_INDEX")):
        # create index from one dir since were not going to dl more anyway
        index = indexAlreadyDownloadedFiles(root_dir / str(year) / tile_id)
    else:
        index = None

    if index is not None:
        logger.info(f"Fetched index: {index}")
    
    try:
        dl_years_for_tile(
            tile_id=tile_id,
            years=[year],
            root_dir=root_dir,
            creds=creds,
            show_live_progress=False,
            dl_index=index,
            # greater chunks for gpfs
            chunkSize=DEFAULT_CHUNK_SIZE * 4
        )
    except RuntimeError as e:
        logger.error(e)
        exit(1)
    except requests.exceptions.ChunkedEncodingError as e:
        logger.error(f"Connection reset by server for tile: {tile_id} and year: {year}", e)
        exit(1)