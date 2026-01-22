from multiprocessing.sharedctypes import Value
from argparse import Namespace
import argparse
from dataclasses import dataclass, field
import requests
import os
from dotenv import load_dotenv
from typing import TypeVar, List, Dict
from pathlib import Path
import logging

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_CHUNK_SIZE: int = 1024 * 1024  # 1Mb default chunk size


session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"],
)
session.mount("https://", HTTPAdapter(max_retries=retries))


def requireEnv(value: T | None, name: str = "value") -> T:
    """Helper function to assure type-safety."""
    if value is None:
        raise ValueError(
            f"Error loading value from .env file - {name} must not be None"
        )
    return value


def indexAlreadyDownloadedFiles(path: Path) -> dict[str, int]:
    """Create index of already downloaded files.
    Also stores filesize to recognize unfinished downloads.
    """
    print(f"Creating index of already downloaded files in '{path}' ...")
    fileIndex: dict[str, int] = {}
    for file in path.rglob("*"):
        if file.is_file():
            fileSize: int = file.stat().st_size
            fileIndex[file.name] = fileSize
    return fileIndex


def _getCLIArgs() -> Namespace:
    parser = argparse.ArgumentParser(
        prog="StarCloud Downloader",
        description="Lets you download all tiles for a range of years. Login credentials need to be passed by the '.env' file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-e",
        "--env-file",
        help="Filepath of env file that is used for authentication",
        default=".env",
        type=str,
    )
    parser.add_argument(
        "tile", help="Tile that should be downloaded. i.e. '31UFS'", type=str
    )
    parser.add_argument(
        "--start-year",
        help="Tile download starting year",
        default=2000,
        type=int,
    )
    parser.add_argument(
        "--end-year",
        help="Tile download ending year",
        default=2022,
        type=int,
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        help="Directory to which the files should be written to",
        default="./",
    )
    parser.add_argument(
        "-c",
        "--chunk-size",
        help="Sets the download chunk size in bytes. 1048576 bytes (aka. 1Mb) is recommended for most use cases, but when running multiple instances of this script a smaller chunk size can be beneficial.",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
    )
    parser.add_argument(
        "--no-progress",
        help="Flag to disable progress output when downloading files. This is useful when running multiple instances of this script in parallel.",
        action="store_false",
    )
    return parser.parse_args()


@dataclass
class LoginCredentials:
    id: str = field()
    username: str
    jwt_token: str


def loadCredsFromEnv(envfilePath: str | None = None) -> LoginCredentials:
    if not load_dotenv(dotenv_path=envfilePath, override=True):
        raise RuntimeError(f".env file with path: '{envfilePath}' could not be found!")

    username: str = requireEnv(os.getenv("SC_USER"), "SC_USER")
    jwt_token: str = requireEnv(os.getenv("SC_JWT"), "SC_JWT")
    id: str = requireEnv(os.getenv("SC_ID"), "SC_ID")
    return LoginCredentials(id, username, jwt_token)


def _getFileListPage(tileName: str, year: int) -> list[str]:
    """Retrieves a list of available tile files for a given tile and year."""
    FILE_PAGE_URL = (
        "https://data-starcloud.pcl.ac.cn/aiforearth/api/data/getFileListByPage"
    )
    payload: dict[Unknown, dict[str, bool | int | str]] = {  # noqa: F821
        "params": {
            "count": 100,
            "enableSpatialQuery": False,
            "page": 1,
            "path": f"CSDC_samples/SDC_V003/{tileName}/{year}",
            "table": "rs_csdc30",
        }
    }
    response: requests.Response = requests.post(FILE_PAGE_URL, json=payload)
    if response.status_code != 200:
        raise RuntimeError(
            f"Could not fetch FileList Page! Code: {response.status_code}, Reason: {response.text}"
        )
    return [str(resp["file"]) for resp in response.json()["response"]]


def _getRandomAssSignedFileLink(
    filename: str, tileName: str, year: int, creds: LoginCredentials
) -> tuple[str, str, int]:
    """Retrieves a signed file URL and its file size based on a tileName and given filename. This URL can be used to download the file."""
    LINK_GEN_URL = (
        "https://data-starcloud.pcl.ac.cn/starcloud/api/file/downloadResource"
    )
    OBJECT_KEY: str = f"shared-dataset/CSDC_samples/CSDC_samples/SDC_V003/{tileName}/{year}/{filename}"
    auth_header: dict[str, str] = {"Authorization": f"Bearer {creds.jwt_token}"}
    payload: dict[str, int | str] = {
        "country": "Germany",
        "objectKey": OBJECT_KEY,
        "resourceId": 26,
        "resourceType": "REMOTE_SENSING",
        "userAccount": creds.username,
        "userId": creds.id,
    }
    print(auth_header)
    print(payload)
    response = requests.Response = requests.post(
        LINK_GEN_URL, headers=auth_header, json=payload
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Could not fetch signed file URL! Code: {response.status_code}, Reason: {response.text}"
        )
    responseBody = response.json()
    return (
        str(responseBody["fileName"]),
        str(responseBody["signedUrl"]),
        int(responseBody["fileSize"]),
    )


def _downloadTIFFile(
    url: str,
    outDir: Path,
    filename: str,
    i: int,
    fileCount: int,
    isProgressShown: bool = True,
    chunkSize: int = DEFAULT_CHUNK_SIZE,
) -> None:
    # response: requests.Response = requests.get(url, stream=True)
    # if response.status_code != 200:
    #     raise RuntimeError(
    #         f"Could not download .tif file! Code: {response.status_code}, Reason: {response.text}"
    #     )
    downloaded = 0
    if not isProgressShown:
        logger.debug(f"[{i}/{fileCount}] Downloading {filename}")

    with session.get(url, stream=True) as response:
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", 0))

        with open(outDir / filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunkSize):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if isProgressShown:
                        print(
                            f"\r[{i}/{fileCount}] Downloading {filename}: {round(downloaded / total * 100, 2)} %",
                            end="",
                        )
            if isProgressShown:
                print()


def dl_years_for_tile(
    tile_id: str,
    years: List[str],
    root_dir: Path,
    creds: LoginCredentials,
    dl_index: Dict[str, int] | None = None,
    show_live_progress: bool = True,
    chunkSize: int = DEFAULT_CHUNK_SIZE,
):
    for year in years:
        target_dir: Path = root_dir / str(year) / tile_id
        if not target_dir.exists():
            target_dir.mkdir(exist_ok=True, parents=True)
            logger.debug(f"Created folder: {str(target_dir)}")

        filenameList: list[str] = _getFileListPage(tile_id, year)

        logger.info(
            f"Found {len(filenameList)} files for {tile_id} in year {year}! Starting download..."
        )
        for i, f in enumerate(filenameList):
            (filename, signedURL, fileSize) = _getRandomAssSignedFileLink(
                f, tile_id, year, creds
            )

            if dl_index is not None and dl_index.get(filename, -1) == fileSize:
                logger.info(
                    f"File {filename} has already been donwloaded, going to next file... "
                )
                continue
            _downloadTIFFile(
                signedURL,
                target_dir,
                filename,
                i + 1,
                len(filenameList),
                show_live_progress,
                chunkSize,
            )


def main() -> None:
    args: Namespace = _getCLIArgs()
    tileName = args.tile
    startYear = args.start_year
    endYear = args.end_year
    envFile = args.env_file
    outputDir = args.output_dir
    isProgressShown = args.no_progress
    chunkSize = args.chunk_size

    if startYear > endYear:
        raise ValueError(
            "Argument '--start-year' must not be larger than '--end-year'!"
        )

    creds: LoginCredentials = loadCredsFromEnv(envFile)

    outDir = Path(f"{outputDir}/{tileName}")

    downloadedFileIndex: dict[str, int] = indexAlreadyDownloadedFiles(outDir)

    try:
        dl_years_for_tile(
            tileName,
            range(startYear, endYear + 1),
            outDir,
            creds,
            downloadedFileIndex,
            isProgressShown,
            chunkSize,
        )
    except RuntimeError as e:
        logger.error(e)
        exit(1)


if __name__ == "__main__":
    main()
