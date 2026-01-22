from .starcloud_dl import dl_years_for_tile, loadCredsFromEnv, DEFAULT_CHUNK_SIZE, indexAlreadyDownloadedFiles
import os
import json
import itertools
from pathlib import Path



if __name__ == "__main__":


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

    tile_id, year = itertools.product(slurm_tiles, slurm_years)[slurm_array_job_id]

    creds = loadCredsFromEnv(".env")

    if bool(os.getenv("S_CREATE_INDEX")):
        index = indexAlreadyDownloadedFiles(root_dir)
    else:
        index = None



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