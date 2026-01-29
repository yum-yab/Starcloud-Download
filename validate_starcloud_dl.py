import json
import select

from pandas._libs import missing

from starcloud_dl import getFileListPage, indexAlreadyDownloadedFiles
from pathlib import Path
import polars as pl

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


def validate_tile_year(
    path_year: Path, year: int, tile_id: str, print_stats: bool = True
) -> pl.DataFrame:
    
    year_tile_path: Path = path_year / tile_id


    res: list[dict[str, str | int]] = []

    expected_files_path = year_tile_path / f"expected_files_{year}_{tile_id}.json"

    if expected_files_path.exists() and expected_files_path.is_file():
        response = json.loads(expected_files_path.read_text())
    else:

        print(f"Could not find expected files {expected_files_path} {year} and {tile_id}. Downloading list...")
        response: dict[str, list[dict[str, int | str]]] = getFileListPage(
            tileName=tile_id, year=year
        )

        if not year_tile_path.exists():
            year_tile_path.mkdir(parents=True, exist_ok=True)
        _ = expected_files_path.write_text(json.dumps(response))

    should_mapping: dict[str, int] = {
        d["file"]: int(d["size"]) for d in response["response"]
    }  # pyright: ignore[reportAssignmentType]


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

    df: pl.DataFrame = pl.DataFrame(res)

    if print_stats:
        print_completeness_percentage(df)

    return df


def validate_year(path: Path, year: int, print_stats: bool = True) -> pl.DataFrame:
    index_path: Path = path if str(path).endswith(str(year)) else path / str(year)

    res: list[pl.DataFrame] = []

    for tile_id in GERMAN_TILES:
        try:
            tile_df: pl.DataFrame = validate_tile_year(
                path_year=index_path, year=year, tile_id=tile_id, print_stats=print_stats
            )
        except Exception as e:
            print(f"ERROR: Could not validate {year}, {tile_id}. Reason: {str(e)}")
            continue

        res.append(tile_df)

    df = pl.concat(res)

    if print_stats:
        print_completeness_percentage(df)
    return df


def print_completeness_percentage(df: pl.DataFrame) -> None:
    completeness_stats = (
        df.group_by(["year", "tile", "status"])
        .len()
        .with_columns(
            (pl.col("len") / pl.sum("len").over(["year", "tile"])).alias("pct")
        )
    )

    comp = completeness_stats.filter((pl.col("status") == "complete"))

    completeness = (
        comp["pct"].sum() / comp["pct"].len() * 100 if comp["pct"].len() != 0 else 0.0
    )

    tiles = df.get_column("tile").unique().cast(str).to_list()

    if len(tiles) > 5:
        tiles_str = str(len(tiles))
    else:
        tiles_str = json.dumps(tiles)

    years = df.get_column("year").unique().cast(dtype=int).to_list()

    missing_files = df.filter(pl.col("status") != "complete").get_column("status").len()

    print(
        f"Completeness for {tiles_str} tiles and {years}: {completeness:.3f} %. Missing files: {missing_files}"
    )


if __name__ == "__main__":
    import sys
    from datetime import datetime

    root_dir: Path = Path(sys.argv[1])
    time_str: str = datetime.now().strftime(format="%Y-%m-%d_%H-%M")

    years_to_check: list[int] = [int(y) for y in sys.argv[2:]]

    dfs: list[pl.DataFrame] = []

    for year in years_to_check:
        df = validate_year(path=root_dir, year=year, print_stats=True)

        dfs.append(df)

    final_df = pl.concat(dfs)

    report_dir = Path("./completeness_reports")

    if not report_dir.exists():
        report_dir.mkdir()

    file_name  = report_dir / (
        f"csdc_dl_completeness_{time_str}_{'_'.join(map(str, years_to_check))}.csv"
    )

    print(f"Writing completness report to {file_name}")
    final_df.write_csv(file_name)

    if len(years_to_check) == 1:
        incomplete_tiles: list[str] = (
            final_df.filter(pl.col("status") != "complete")
            .get_column("tile")
            .unique()
            .to_list()
        )

        print(f"Incomplete tiles: {json.dumps(obj=incomplete_tiles)}")
