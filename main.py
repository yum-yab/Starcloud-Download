from multiprocessing.sharedctypes import Value
from argparse import Namespace
import argparse
from dataclasses import dataclass, field
import requests
import os
from dotenv import load_dotenv
from typing import TypeVar
from pathlib import Path

T = TypeVar("T")

DEFAULT_CHUNK_SIZE: int = 1024 * 1024  # 1Mb default chunk size


def requireEnv(value: T | None, name: str = "value") -> T:
    """Helper function to assure type-safety."""
    if value is None:
        raise ValueError(
            f"Error loading value from .env file - {name} must not be None"
        )
    return value


def _indexAlreadyDownloadedFiles(path: Path) -> dict[str, int]:
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
    id: int = field()
    username: str
    jwt_token: str


def _loadCredsFromEnv(envfilePath: str) -> LoginCredentials:
    if not load_dotenv(dotenv_path=envfilePath):
        raise RuntimeError(f".env file with path: '{envfilePath}' could not be found!")

    username: str = requireEnv(os.getenv("USERNAME"), "USERNAME")
    jwt_token: str = requireEnv(os.getenv("JWT"), "JWT")
    id: str = requireEnv(os.getenv("ID"), "ID")
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
    response: requests.Response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise RuntimeError(
            f"Could not download .tif file! Code: {response.status_code}, Reason: {response.text}"
        )
    total = int(response.headers.get("Content-Length", 0))
    downloaded = 0
    if not isProgressShown:
        print(f"[{i}/{fileCount}] Downloading {filename}")
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

    creds: LoginCredentials = _loadCredsFromEnv(envFile)

    outDir = Path(f"{outputDir}/{tileName}")

    downloadedFileIndex: dict[str, int] = _indexAlreadyDownloadedFiles(outDir)

    for year in range(startYear, endYear + 1):
        yearDir: Path = outDir / str(year)
        if not yearDir.exists():
            yearDir.mkdir(exist_ok=True, parents=True)
            print(f"Created folder: {str(yearDir)}")

        filenameList: list[str] = _getFileListPage(tileName, year)

        print(
            f"Found {len(filenameList)} files for {tileName} in year {year}! Starting download..."
        )
        for i, f in enumerate(filenameList):
            (filename, signedURL, fileSize) = _getRandomAssSignedFileLink(
                f, tileName, year, creds
            )

            if (
                filename in downloadedFileIndex
                and downloadedFileIndex[filename] == fileSize
            ):
                print(
                    f"File {filename} has already been donwloaded, going to next file... "
                )
                continue
            _downloadTIFFile(
                signedURL,
                yearDir,
                filename,
                i + 1,
                len(filenameList),
                isProgressShown,
                chunkSize,
            )


if __name__ == "__main__":
    main()
