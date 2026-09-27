"""Publish Datamule statistics datasets from Datamule Hub as Parquet files."""

import argparse
import datetime as dt
import shutil
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from datamulehub import databases


EASTERN = ZoneInfo("America/New_York")
DEFAULT_OUTPUT_DIR = Path("data/datamule-statistics/filing-detections-speed")


def result_files(result: dict, name: str) -> list[str]:
    files = [str(path) for path in result.get("files", [])]
    if not files:
        raise ValueError(f"The {name} query returned no Parquet files")
    for path in files:
        if not Path(path).is_file():
            raise FileNotFoundError(f"The {name} query listed a missing file: {path}")
    return files


def write_one_parquet(files: list[str], destination: Path) -> None:
    if len(files) == 1:
        shutil.copyfile(files[0], destination)
    else:
        pl.scan_parquet(files).sink_parquet(destination)


def generate(output_dir: Path = DEFAULT_OUTPUT_DIR, query=databases.query) -> None:
    output_dir = output_dir.resolve()
    filing_types_dir = output_dir.parent / "sec-filing-types"
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix=".datamule-statistics-", dir=output_dir.parent
    ) as temporary_dir:
        temporary = Path(temporary_dir)

        fastest_result = query(
            """
            SELECT accession, detected_time, detection_method
            FROM fastest_sec_filings_metadata
            """,
            output_dir=temporary / "fastest",
        )
        fastest_files = result_files(fastest_result, "fastest filings")
        earliest_ms = (
            pl.scan_parquet(fastest_files)
            .select(pl.col("detected_time").min())
            .collect()
            .item()
        )
        if earliest_ms is None:
            raise ValueError("The fastest filings query returned no detections")

        start_date = dt.datetime.fromtimestamp(earliest_ms / 1000, EASTERN).date()
        end_date = dt.datetime.now(EASTERN).date()
        start_seconds = int(dt.datetime.combine(start_date, dt.time(), EASTERN).timestamp())
        end_seconds = int(dt.datetime.combine(end_date, dt.time(), EASTERN).timestamp())

        websocket_result = query(
            f"""
            SELECT accession, source, detected_time
            FROM monitor_dumps
            WHERE detected_time >= {start_seconds}
              AND detected_time < {end_seconds}
              AND lower(source) IN ('rss', 'efts', 'anticipate')
            """,
            output_dir=temporary / "websocket",
        )
        websocket_files = result_files(websocket_result, "websocket")

        linked_result = query(
            f"""
            SELECT accessionnumber, acceptancedatetime, filingdate
            FROM submissions_metadata
            WHERE filingdate >= '{start_date}'
              AND filingdate <= '{end_date}'
            """,
            output_dir=temporary / "linked_filings",
        )
        linked_files = result_files(linked_result, "linked filings")

        filing_types_result = query(
            """
            SELECT
                submissiontype,
                CAST(date_trunc('week', CAST(filingdate AS DATE)) AS DATE) AS week_start,
                year(CAST(filingdate AS DATE)) AS calendar_year,
                COUNT(*) AS count,
                SUM(CASE WHEN containsxbrl = 1 THEN 1 ELSE 0 END) AS xbrl_count
            FROM sec_submission_details_table
            WHERE submissiontype IS NOT NULL AND filingdate IS NOT NULL
            GROUP BY 1, 2, 3
            ORDER BY calendar_year, week_start, submissiontype
            """,
            output_dir=temporary / "filing_types",
        )
        filing_types_files = result_files(filing_types_result, "filing types")

        outputs = {
            "fastest_sec_filings_websocket.parquet": fastest_files,
            "websocket.parquet": websocket_files,
            "linked_filings.parquet": linked_files,
        }
        for filename, files in outputs.items():
            write_one_parquet(files, temporary / filename)
        write_one_parquet(filing_types_files, temporary / "filing-types.parquet")

        output_dir.mkdir(parents=True, exist_ok=True)
        filing_types_dir.mkdir(parents=True, exist_ok=True)
        for filename in outputs:
            (temporary / filename).replace(output_dir / filename)
        (temporary / "filing-types.parquet").replace(filing_types_dir / "filing-types.parquet")
        print(f"Published {len(outputs) + 1} Parquet files under {output_dir.parent}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    generate(args.output_dir)
