from pandas.core.frame import DataFrame
from pandas.core.frame import DataFrame
from pandas.core.frame import DataFrame


from typing import Any
import json

from starcloud_dl import getFileListPage, indexAlreadyDownloadedFiles
import pandas as pd
from pathlib import Path

GERMAN_TILES: list[str] = [
    "31UFS",
    "31UFT",
    "31UGR",
    "31UGS",
    "31UGT",
    "31UGU",
    "31UGV",
    "32ULB",
    "32ULC",
    "32ULD",
    "32ULE",
    "32UME",
    "32UMF",
    "32TMT",
    "32TNT",
    "32TPT",
    "32TQT",
    "32UMA",
    "32UMU",
    "32UMV",
    "32UNA",
    "32UNB",
    "32UNC",
    "32UND",
    "32UNE",
    "32UNU",
    "32UNV",
    "32UPA",
    "32UPB",
    "32UPC",
    "32UPD",
    "32UPE",
    "32UPF",
    "32UPU",
    "32UPV",
    "32UQA",
    "32UQB",
    "32UQC",
    "32UQD",
    "32UQE",
    "32UQU",
    "32UQV",
    "33UUA",
    "33UUP",
    "33UUQ",
    "33UUR",
    "33UUS",
    "33UUT",
    "33UUU",
    "33UUV",
    "33UVA",
    "33UVS",
    "33UVT",
    "33UVU",
    "33UVV",
    "31UGQ",
    "32ULA",
    "32ULV",
    "32ULU",
    "32TLT",
    "32UMB",
    "32UMC",
    "32UMD",
    "32UNF",
    "33TUN",
    "33UVP",
    "33UVQ",
]

def validate_tile_year(path: Path, year: int, tile_id: str, print_stats: bool = True) -> pd.DataFrame:

    if str(path).endswith(tile_id):
        path: Path = path / tile_id

    
    res: list[dict[str, str | int]] = []

    response: dict[str, list[dict[str, int | str]]] = getFileListPage(
        tileName=tile_id, year=year
    )

    should_mapping: dict[str, int] = {
        d["file"]: int(d["size"]) for d in response["response"]
    }  # pyright: ignore[reportAssignmentType]

    year_tile_path: Path = path / tile_id

    is_mapping: dict[str, int] = indexAlreadyDownloadedFiles(path=year_tile_path)

    for filename, fsize in should_mapping.items():
        tile_response: dict[str, str | int] = {
            "tile": tile_id,
            "year": year,
            "filename": filename,
        }

        if filename in is_mapping:
            if is_mapping[filename] == fsize:
                tile_response["status"] = "complete"
            else:
                tile_response["status"] = "incomplete"

        else:
            tile_response["status"] = "missing"

        res.append(tile_response)

    df: DataFrame = pd.DataFrame(res)

    if print_stats:
        print_all_status_percentages(df)
    
    return df



def validate_year(path: Path, year: int, print_stats: bool = True) -> pd.DataFrame:
    if not str(path).endswith(str(year)):
        path: Path = path / str(year)

    res: list[pd.DataFrame] = []

    for tile_id in GERMAN_TILES:
        try:
            tile_df: DataFrame = validate_tile_year(path, year, tile_id, print_stats=False)
        except Exception as e:
            print(f'ERROR: Could not validate {year}, {tile_id}. Reason: {str(e)}')
            continue

        res.append(tile_df)

    df: pd.DataFrame = pd.concat(res)

    if print_stats:
        print_all_status_percentages(df)
    return df


def print_all_status_percentages(df: pd.DataFrame) -> None:
    stats: DataFrame = (
        df.groupby(["tile", "year", "status"])
        .size()
        .groupby(level=[0, 1])
        .apply(func=lambda s: s / s.sum() * 100)
        .rename("pct")
        .reset_index()
    )

    print(stats)


if __name__ == "__main__":
    import sys
    from datetime import datetime

    root_dir: Path = Path(sys.argv[1])
    time_str: str = datetime.now().strftime(format="%Y-%m-%d_%H:%M")


    years_to_check: list[int] = [int(y) for y in sys.argv[2:]]

    dfs: list[pd.DataFrame] = []

    for year in years_to_check:
        df: pd.DataFrame = validate_year(path=root_dir, year=year, print_stats=False)

        dfs.append(df)

    final_df: pd.DataFrame = pd.concat(dfs)


    file_name: str = f'csdc_dl_completeness_{time_str}_{"_".join(map(str, years_to_check))}.csv'

    print(f'Writing completness report to {file_name}')
    final_df.to_csv(file_name)

    if len(years_to_check) == 1:

        incomplete_tiles: list[str] = final_df[final_df["status"] != "complete"]["tile_id"].unique().tolist()

        print(f"Incomplete tiles: {json.dumps(obj=incomplete_tiles)}")
