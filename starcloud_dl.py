from requests.sessions import Session
from sc_login import AuthData, LoginCredentials, performLogin

from logging import Logger


from argparse import ArgumentParser, Namespace
import argparse
from dataclasses import dataclass, field
import requests
import os
from dotenv import load_dotenv
from typing import Any, Generator, TypeVar
from pathlib import Path
import logging
import sys
import time
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

logger: Logger = logging.getLogger(name=__name__)

T = TypeVar(name="T")

A = TypeVar(name="A")

DEFAULT_CHUNK_SIZE: int = 1024 * 1024  # 1Mb default chunk size


def _split_into_n(seq: list[A], n_parts: int) -> list[list[A]]:
    k, m = divmod(len(seq), n_parts)
    return [
        seq[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n_parts)
    ]


@dataclass
class ListSplitChoose:
    i: int
    n: int

    def get_sublist(self, seq: list[A]) -> list[A]:
        return list[list[A]](_split_into_n(seq=seq, n_parts=self.n))[self.i]


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
    logger.debug(f"Creating index of already downloaded files in '{path}' ...")
    fileIndex: dict[str, int] = {}
    for file in path.rglob("*.tif"):
        if file.is_file():
            fileSize: int = file.stat().st_size
            fileIndex[file.name] = fileSize
    return fileIndex


def loadCredsFromEnv(envfilePath: Path | str) -> LoginCredentials:
    if not load_dotenv(dotenv_path=envfilePath):
        raise RuntimeError(f".env file with path: '{envfilePath}' could not be found!")
    email: str = requireEnv(os.getenv("STAR_EMAIL"), "STAR_EMAIL")
    password: str = requireEnv(os.getenv("STAR_PASSWORD"), "STAR_PASSWORD")
    return LoginCredentials(email, password)


def _getCLIArgs() -> Namespace:
    parser: ArgumentParser = argparse.ArgumentParser(
        prog="StarCloud Downloader",
        description="Lets you download all tiles for a range of years. Login credentials need to be passed by the '.env' file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    _ = parser.add_argument(
        "-e",
        "--env-file",
        help="Filepath of env file that is used for authentication",
        default=".env",
        type=str,
    )
    _ = parser.add_argument(
        "tile", help="Tile that should be downloaded. i.e. '31UFS'", type=str
    )
    _ = parser.add_argument(
        "--start-year",
        help="Tile download starting year",
        default=2000,
        type=int,
    )
    _ = parser.add_argument(
        "--end-year",
        help="Tile download ending year",
        default=2022,
        type=int,
    )
    _ = parser.add_argument(
        "-o",
        "--output-dir",
        help="Directory to which the files should be written to",
        default="./",
    )
    _ = parser.add_argument(
        "-c",
        "--chunk-size",
        help="Sets the download chunk size in bytes. 1048576 bytes (aka. 1Mb) is recommended for most use cases, but when running multiple instances of this script a smaller chunk size can be beneficial.",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
    )
    _ = parser.add_argument(
        "--no-progress",
        help="Flag to disable progress output when downloading files. This is useful when running multiple instances of this script in parallel.",
        action="store_false",
    )
    return parser.parse_args()


def getFileListPage(tileName: str, year: int) -> dict[str, list[dict[str, int | str]]]:
    """Retrieves a list of available tile files for a given tile and year."""
    FILE_PAGE_URL = (
        "https://data-starcloud.pcl.ac.cn/aiforearth/api/data/getFileListByPage"
    )
    payload: dict[str, dict[str, int | bool | str]] = {  # noqa: F821
        "params": {
            "count": 100,
            "enableSpatialQuery": False,
            "page": 1,
            "path": f"CSDC_samples/SDC_V003/{tileName}/{year}",
            "table": "rs_csdc30",
        }
    }
    response: requests.Response = requests.post(url=FILE_PAGE_URL, json=payload)
    if response.status_code != 200:
        raise RuntimeError(
            f"Could not fetch FileList Page! Code: {response.status_code}, Reason: {response.text}"
        )
    return response.json()  # pyright: ignore[reportAny]


def get_filenames_for_id(
    tile_id: str,
    year: int,
    index: dict[str, int] | None = None,
    list_split_chooser: ListSplitChoose | None = None,
    write_resp_to_disk: Path | None = None,
) -> list[str]:
    if write_resp_to_disk is None:
        resp_json: dict[str, list[dict[str, int | str]]] = getFileListPage(
            tileName=tile_id, year=year
        )
    else:
        resp_file_name = f"expected_files_{year}_{tile_id}.json"
        target_file = (
            write_resp_to_disk / str(year) / tile_id / resp_file_name
            if not str(write_resp_to_disk).endswith(tile_id)
            else write_resp_to_disk / resp_file_name
        )

        if target_file.exists() and target_file.is_file():
            resp_json = json.loads(target_file.read_text())  # pyright: ignore[reportAny]
        else:
            resp_json: dict[str, list[dict[str, int | str]]] = getFileListPage(
                tileName=tile_id, year=year
            )
            _ = target_file.write_text(json.dumps(resp_json))

    list_of_file_dicts = resp_json["response"]

    if list_split_chooser is not None:
        list_of_file_dicts = list_split_chooser.get_sublist(seq=list_of_file_dicts)

    if index is None:
        return [str(resp["file"]) for resp in list_of_file_dicts]
    else:
        return [
            str(resp["file"])
            for resp in list_of_file_dicts
            if index.get(str(resp["file"]), -10) != resp["size"]
        ]


def _getRandomAssSignedFileLink(
    filename: str, tileName: str, year: int, auth: AuthData
) -> tuple[str, str, int]:
    """Retrieves a signed file URL and its file size based on a tileName and given filename. This URL can be used to download the file."""
    LINK_GEN_URL = (
        "https://data-starcloud.pcl.ac.cn/starcloud/api/file/downloadResource"
    )
    OBJECT_KEY: str = f"shared-dataset/CSDC_samples/CSDC_samples/SDC_V003/{tileName}/{year}/{filename}"
    auth_header: dict[str, str] = {"Authorization": f"Bearer {auth.token}"}
    payload: dict[str, int | str] = {
        "country": "Germany",
        "objectKey": OBJECT_KEY,
        "resourceId": 26,
        "resourceType": "REMOTE_SENSING",
        "userAccount": auth.userName,
        "userId": auth.id,
    }
    response: requests.Response = requests.post(
        url=LINK_GEN_URL, headers=auth_header, json=payload
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Could not fetch signed file URL! Code: {response.status_code}, Reason: {response.text}"
        )
    responseBody = response.json()  # pyright: ignore[reportAny]
    return (
        str(responseBody["fileName"]),  # pyright: ignore[reportAny]
        str(responseBody["signedUrl"]),  # pyright: ignore[reportAny]
        int(responseBody["fileSize"]),  # pyright: ignore[reportAny]
    )


def _downloadTIFFile(
    url: str,
    outDir: Path,
    filename: str,
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
        logger.debug(f"Downloading {filename}")

    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", 0))

        with open(outDir / filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunkSize):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if isProgressShown:
                        print(
                            f"\rDownloading {filename}: {round(downloaded / total * 100, 2)} %",
                            end="",
                        )
            if isProgressShown:
                print()


def dl_file_by_id(
    tile_id: str,
    year: int,
    target_dir: Path,
    auth: AuthData,
    filename: str,
    show_live_progress: bool = True,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    log_time: bool = False,
) -> None:
    t_file_start: float = time.perf_counter()

    (filename, signedURL, _) = _getRandomAssSignedFileLink(
        filename=filename, tileName=tile_id, year=year, auth=auth
    )

    t_got_file_link: float = time.perf_counter()

    _downloadTIFFile(
        url=signedURL,
        outDir=target_dir,
        filename=filename,
        isProgressShown=show_live_progress,
        chunkSize=chunk_size,
    )

    t_downloaded: float = time.perf_counter()

    if log_time:
        logger.info(
            msg=f"Perf FileLink,Download: {(t_got_file_link - t_file_start):.2f}, {(t_downloaded - t_got_file_link):.2f} s"
        )
    logger.info(msg=f"Successfully downloaded {filename}!")


def dl_file_list(
    tile_id: str,
    year: int,
    target_dir: Path,
    auth: AuthData,
    filename_list: list[str],
    show_live_progress: bool = True,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    log_time: bool = False,
) -> None:
    for _, f in enumerate[str](filename_list):
        dl_file_by_id(
            tile_id=tile_id,
            year=year,
            target_dir=target_dir,
            auth=auth,
            filename=f,
            show_live_progress=show_live_progress,
            chunk_size=chunk_size,
            log_time=log_time,
        )


def dl_years_for_tile(
    tile_id: str,
    years: list[int],
    root_dir: Path,
    auth: AuthData,
    dl_index: dict[str, int] | None = None,
    show_live_progress: bool = True,
    log_time: bool = True,
    chunkSize: int = DEFAULT_CHUNK_SIZE,
    list_split_chooser: ListSplitChoose | None = None,
) -> None:
    for year in years:
        target_dir: Path = root_dir / str(year) / tile_id
        if not target_dir.exists():
            target_dir.mkdir(exist_ok=True, parents=True)
            logger.debug(msg=f"Created folder: {str(target_dir)}")

        start_acc: float = time.perf_counter()

        filenameList: list[str] = get_filenames_for_id(
            tile_id, year, index=dl_index, list_split_chooser=list_split_chooser
        )
        if log_time:
            logger.info(
                msg=f"Perf File List: {(time.perf_counter() - start_acc):.2f} s"
            )

        if len(filenameList) == 0:
            logger.info(
                f"No files left for {tile_id} in {year} {list_split_chooser}. Ending download...."
            )
            return
        else:
            logger.info(
                msg=f"Found {len(filenameList)} files for {tile_id} in year {year}! Starting download..."
            )

        dl_file_list(
            tile_id=tile_id,
            year=year,
            target_dir=target_dir,
            auth=auth,
            filename_list=filenameList,
            show_live_progress=show_live_progress,
            chunk_size=chunkSize,
            log_time=log_time,
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
    authData: AuthData = performLogin(creds)
    outDir = Path(f"{outputDir}/{tileName}")

    outDir: Path = Path(f"{outputDir}/{tileName}")

    downloadedFileIndex: dict[str, int] = indexAlreadyDownloadedFiles(path=outDir)

    try:
        dl_years_for_tile(
            tile_id=tileName,
            years=list[int](range(startYear, endYear + 1)),
            root_dir=outDir,
            auth=authData,
            dl_index=downloadedFileIndex,
            show_live_progress=isProgressShown,
            log_time=chunkSize,
        )
    except RuntimeError as e:
        logger.error(e)
        exit(1)
    except requests.exceptions.ChunkedEncodingError as e:
        logger.error(f"Connection reset by server for tile: {tileName}")
        exit(1)


if __name__ == "__main__":
    main()
