"""Publish Datamule statistics datasets from Datamule Hub as Parquet files."""

import argparse
import csv
import datetime as dt
import gzip
import json
import shutil
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from datamulehub import databases


EASTERN = ZoneInfo("America/New_York")
DEFAULT_OUTPUT_DIR = Path("data/datamule-statistics/filing-detections-speed")
FILER_AGENT_SQL = """
SELECT
    calendar_year,
    agent_cik,
    CASE WHEN grouping(cik) = 1 THEN CAST(NULL AS bigint) ELSE cik END AS cik,
    count(DISTINCT accession) AS filing_count
FROM (
    SELECT
        d.accession,
        year(d.filing_date) AS calendar_year,
        CAST(substr(lpad(CAST(d.accession AS varchar), 18, '0'), 1, 10) AS bigint) AS agent_cik,
        TRY_CAST(regexp_replace(CAST(c.cik AS varchar), '[^0-9]', '') AS bigint) AS cik
    FROM sec_accession_cik_table c
    JOIN (
        SELECT
            TRY_CAST(replace(CAST(accessionnumber AS varchar), '-', '') AS bigint) AS accession,
            min(TRY_CAST(filingdate AS date)) AS filing_date
        FROM sec_submission_details_table
        GROUP BY 1
    ) d
      ON TRY_CAST(replace(CAST(c.accessionnumber AS varchar), '-', '') AS bigint) = d.accession
    WHERE d.filing_date <= current_date
      AND d.accession > 0 AND d.accession < 1000000000000000000
) filings
WHERE agent_cik > 0
GROUP BY GROUPING SETS (
    (calendar_year, agent_cik),
    (calendar_year, agent_cik, cik)
)
HAVING grouping(cik) = 1 OR (cik > 0 AND cik < 10000000000)
"""
BENFORD_SQL = """
SELECT
    acc AS accessionnumber,
    c.cik,
    d.submissiontype,
    taxonomy,
    digit,
    n
FROM (
    SELECT
        accessionnumber AS acc,
        taxonomy,
        CAST(regexp_extract(value, '[1-9]') AS integer) AS digit,
        count(*) AS n
    FROM simple_xbrl
    WHERE regexp_like(value, '^-?[0-9]+([.][0-9]+)?$')
      AND regexp_like(value, '[1-9]')
    GROUP BY 1, 2, 3
)
JOIN sec_accession_cik_table c
  ON acc = c.accessionnumber
JOIN sec_submission_details_table d
  ON acc = d.accessionnumber
ORDER BY 1, 2, 3, 4, 5
"""
SIGNATURE_SQL = """
SELECT accessionnumber, filingdate, name
FROM (
    SELECT accessionnumber, filingdate, signature AS name
    FROM "schedule_13d_signature_person"
    WHERE signature IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturename
    FROM "345_owner_signature"
    WHERE signaturename IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturereportingpersonname
    FROM "schedule_13g_signature_information"
    WHERE signaturereportingpersonname IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, personsignature
    FROM "c_signature_person"
    WHERE personsignature IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, personsignature
    FROM "c_tr_signature_person"
    WHERE personsignature IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturename
    FROM "13fhr"
    WHERE signaturename IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturename
    FROM "d"
    WHERE signaturename IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturename
    FROM "25_nse"
    WHERE signaturename IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturename
    FROM "ncr"
    WHERE signaturename IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signername
    FROM "ma"
    WHERE signername IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signername
    FROM "ma_w"
    WHERE signername IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signername
    FROM "sbse_c"
    WHERE signername IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signpersonname
    FROM "x_17a_5"
    WHERE signpersonname IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, nameofsigningofficer
    FROM "n_mfp12"
    WHERE nameofsigningofficer IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, nameofsigningofficer
    FROM "n_mfp3"
    WHERE nameofsigningofficer IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, registrantsignedname
    FROM "n_cen"
    WHERE registrantsignedname IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signername
    FROM "nport_p"
    WHERE signername IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturename
    FROM "ta_1"
    WHERE signaturename IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturename
    FROM "ta_2"
    WHERE signaturename IS NOT NULL

    UNION ALL
    SELECT accessionnumber, filingdate, signaturename
    FROM "ta_w"
    WHERE signaturename IS NOT NULL
) signers
WHERE filingdate >= CAST(date_add('year', -5, current_date) AS VARCHAR)
"""
COMMISSIONS_SQL = """
SELECT
    accessionnumber,
    filingdate,
    'd' AS submission_type,
    CAST(findersfeedollaramount AS varchar) AS findersfee,
    CAST(salescommissionsdollaramount AS varchar) AS commissions
FROM "d"
WHERE findersfeedollaramount IS NOT NULL
   OR salescommissionsdollaramount IS NOT NULL

UNION ALL
SELECT
    accessionnumber,
    filingdate,
    '1-A',
    CAST(finderfeesfee AS varchar),
    CAST(salescommissionsserviceproviderfees AS varchar)
FROM "1a"
WHERE finderfeesfee IS NOT NULL
   OR salescommissionsserviceproviderfees IS NOT NULL

UNION ALL
SELECT
    accessionnumber,
    filingdate,
    '1-K',
    CAST(findersfees AS varchar),
    CAST(salescommissionsfee AS varchar)
FROM "1k_summary_info"
WHERE findersfees IS NOT NULL
   OR salescommissionsfee IS NOT NULL

UNION ALL
SELECT
    accessionnumber,
    filingdate,
    '1-Z',
    CAST(findersfees AS varchar),
    CAST(salescommissionsfee AS varchar)
FROM "1_z"
WHERE findersfees IS NOT NULL
   OR salescommissionsfee IS NOT NULL
"""
MERGERS_SQL = """
SELECT
    accessionnumber,
    filingdate,
    acquiringcik,
    acquiringentityname,
    acquiringseriesid,
    acquiringseriesname,
    acquiringclasscontractid,
    acquiringclasscontractname,
    targetcik,
    targetentityname,
    targetseriesid,
    targetseriesname,
    targetclasscontractid,
    targetclasscontractname
FROM "effect_merger"
WHERE acquiringseriesid IS NOT NULL
  AND acquiringseriesname IS NOT NULL
  AND targetseriesid IS NOT NULL
  AND targetseriesname IS NOT NULL
"""


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


def write_merger_parquet(files: list[str], destination: Path) -> None:
    mergers = (
        pl.scan_parquet(files)
        .with_columns(
            pl.col("accessionnumber").cast(pl.Utf8).str.strip_chars().str.replace_all("-", "").str.zfill(18),
            pl.col("filingdate").cast(pl.Utf8).str.strip_chars().str.slice(0, 10),
            pl.col("acquiringseriesid", "acquiringseriesname", "targetseriesid", "targetseriesname")
            .cast(pl.Utf8).str.strip_chars(),
        )
        .filter(
            pl.col("accessionnumber").str.contains(r"^\d{18}$"),
            pl.col("accessionnumber") != "0" * 18,
            pl.col("filingdate").str.contains(r"^\d{4}-\d{2}-\d{2}$"),
            pl.col("acquiringseriesid") != "",
            pl.col("acquiringseriesname") != "",
            pl.col("targetseriesid") != "",
            pl.col("targetseriesname") != "",
        )
        .unique()
        .collect()
    )
    if mergers.is_empty():
        raise ValueError("The merger query returned no usable series relationships")
    mergers.write_parquet(destination)


def write_signature_parts(files: list[str], destination: Path) -> list[Path]:
    signatures = (
        pl.scan_parquet(files)
        .select(
            pl.col("accessionnumber").cast(pl.Utf8).str.strip_chars().str.replace_all("-", "").str.zfill(18),
            pl.col("filingdate").cast(pl.Utf8).str.strip_chars().str.slice(0, 10),
            pl.col("name").cast(pl.Utf8).str.strip_chars(),
        )
        .filter(
            pl.col("accessionnumber").str.contains(r"^\d{18}$"),
            pl.col("accessionnumber") != "0" * 18,
            pl.col("filingdate").str.contains(r"^\d{4}-\d{2}-\d{2}$"),
            pl.col("name") != "",
        )
        .unique()
        .sort("name", "filingdate", "accessionnumber")
        .collect()
    )
    if signatures.is_empty():
        raise ValueError("The signature query returned no usable rows")
    destination.mkdir()
    parts = []
    for index in range(4):
        start = index * signatures.height // 4
        end = (index + 1) * signatures.height // 4
        part = destination / f"part-{index:05d}.parquet"
        signatures.slice(start, end - start).write_parquet(part)
        parts.append(part)
    return parts


def write_filer_agent_parquets(files: list[str], destination: Path, metadata_dir: Path) -> list[Path]:
    counts = (
        pl.scan_parquet(files)
        .select(
            pl.col("calendar_year").cast(pl.Int32),
            pl.col("agent_cik", "cik", "filing_count").cast(pl.Int64),
        )
        .unique()
        .collect()
    )
    if counts.is_empty() or counts.filter(
        pl.col("calendar_year").is_null() | pl.col("agent_cik").is_null()
        | pl.col("filing_count").is_null() | (pl.col("filing_count") <= 0)
        | (pl.col("agent_cik") <= 0) | (pl.col("agent_cik") >= 10_000_000_000)
        | (pl.col("cik").is_not_null() & ((pl.col("cik") <= 0) | (pl.col("cik") >= 10_000_000_000)))
    ).height:
        raise ValueError("The filer agent query returned invalid counts")
    if counts.select("calendar_year", "agent_cik", "cik").is_duplicated().any():
        raise ValueError("The filer agent query returned conflicting counts")

    agents = counts.filter(pl.col("cik").is_null()).drop("cik")
    entities = counts.filter(pl.col("cik").is_not_null())
    if agents.is_empty() or entities.is_empty():
        raise ValueError("The filer agent query returned no agent totals or entity relationships")
    totals = agents.select("calendar_year", "agent_cik", pl.col("filing_count").alias("agent_total"))
    checked = entities.join(totals, on=["calendar_year", "agent_cik"], how="left")
    if checked.filter(pl.col("agent_total").is_null() | (pl.col("filing_count") > pl.col("agent_total"))).height:
        raise ValueError("Filer agent entity counts exceed their agent totals")

    needed = set(agents["agent_cik"].to_list()) | set(entities["cik"].to_list())
    names = {}
    for filename in ("listed_filer_metadata.csv.gz", "unlisted_filer_metadata.csv.gz"):
        with gzip.open(metadata_dir / filename, "rt", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if not {"cik", "name"}.issubset(reader.fieldnames or []):
                raise ValueError(f"Missing cik or name column in {filename}")
            for row in reader:
                cik = str(row["cik"]).strip()
                if cik.isascii() and cik.isdigit() and int(cik) in needed:
                    name = str(row["name"] or "").strip()
                    if name:
                        names.setdefault(int(cik), name)
    name_table = pl.DataFrame({"cik": list(names), "name": list(names.values())}, schema={"cik": pl.Int64, "name": pl.String})
    agents = (
        agents.join(name_table.rename({"cik": "agent_cik", "name": "agent_name"}), on="agent_cik", how="left")
        .with_columns(pl.col("agent_name").fill_null(""))
        .sort(["calendar_year", "filing_count", "agent_cik"], descending=[False, True, False])
    )
    entities = entities.join(name_table, on="cik", how="left").with_columns(pl.col("name").fill_null(""))
    destination.mkdir()
    outputs = [destination / "agents.parquet"]
    agents.write_parquet(outputs[0])
    years = sorted(agents["calendar_year"].unique().to_list())
    for year in years:
        path = destination / f"entities-{year}.parquet"
        (
            entities.filter(pl.col("calendar_year") == year).drop("calendar_year")
            .sort(["agent_cik", "filing_count", "cik"], descending=[False, True, False])
            .write_parquet(path)
        )
        outputs.append(path)
    outputs.append(destination / "manifest.json")
    now = dt.datetime.now(dt.timezone.utc)
    outputs[-1].write_text(json.dumps({
        "generated_at": now.isoformat(),
        "through_date": now.astimezone(EASTERN).date().isoformat(),
        "years": years,
    }, indent=2) + "\n", encoding="utf-8")
    return outputs


def generate(output_dir: Path = DEFAULT_OUTPUT_DIR, query=databases.query, metadata_dir: Path | None = None) -> None:
    output_dir = output_dir.resolve()
    filing_types_dir = output_dir.parent / "sec-filing-types"
    signature_dir = output_dir.parent / "sec-signature-search"
    commissions_dir = output_dir.parent / "sec-commissions"
    mergers_dir = output_dir.parent / "sec-mergers"
    benford_dir = output_dir.parent / "xbrl-benford"
    filer_agent_dir = output_dir.parent / "sec-filer-agents"
    metadata_dir = metadata_dir or output_dir.parent.parent / "filer_metadata"
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
                CAST(date_trunc('month', CAST(filingdate AS DATE)) AS DATE) AS month_start,
                year(CAST(filingdate AS DATE)) AS calendar_year,
                COUNT(*) AS count,
                SUM(CASE WHEN containsxbrl = 1 THEN 1 ELSE 0 END) AS xbrl_count
            FROM sec_submission_details_table
            WHERE submissiontype IS NOT NULL AND filingdate IS NOT NULL
            GROUP BY 1, 2, 3, 4
            ORDER BY calendar_year, month_start, week_start, submissiontype
            """,
            output_dir=temporary / "filing_types",
        )
        filing_types_files = result_files(filing_types_result, "filing types")

        signature_result = query(SIGNATURE_SQL, output_dir=temporary / "all_signers")
        signature_files = result_files(signature_result, "signatures")

        commissions_result = query(
            COMMISSIONS_SQL, output_dir=temporary / "finders_and_commissions"
        )
        commissions_files = result_files(commissions_result, "commissions")

        mergers_result = query(MERGERS_SQL, output_dir=temporary / "effect_merger")
        mergers_files = result_files(mergers_result, "mergers")

        benford_result = query(
            BENFORD_SQL, output_dir=temporary / "benford_by_accession_cik_taxonomy_type"
        )
        benford_files = result_files(benford_result, "XBRL Benford")

        filer_agent_result = query(FILER_AGENT_SQL, output_dir=temporary / "filer_agent_counts")
        filer_agent_files = result_files(filer_agent_result, "filer agents")
        filer_agent_outputs = write_filer_agent_parquets(filer_agent_files, temporary / "filer_agents", metadata_dir)

        outputs = {
            "fastest_sec_filings_websocket.parquet": fastest_files,
            "websocket.parquet": websocket_files,
            "linked_filings.parquet": linked_files,
        }
        for filename, files in outputs.items():
            write_one_parquet(files, temporary / filename)
        write_one_parquet(filing_types_files, temporary / "filing-types.parquet")
        signature_parts = write_signature_parts(signature_files, temporary / "signature_parts")
        write_one_parquet(commissions_files, temporary / "commissions.parquet")
        write_merger_parquet(mergers_files, temporary / "mergers.parquet")
        write_one_parquet(benford_files, temporary / "benford.parquet")

        output_dir.mkdir(parents=True, exist_ok=True)
        filing_types_dir.mkdir(parents=True, exist_ok=True)
        signature_dir.mkdir(parents=True, exist_ok=True)
        commissions_dir.mkdir(parents=True, exist_ok=True)
        mergers_dir.mkdir(parents=True, exist_ok=True)
        benford_dir.mkdir(parents=True, exist_ok=True)
        filer_agent_dir.mkdir(parents=True, exist_ok=True)
        for filename in outputs:
            (temporary / filename).replace(output_dir / filename)
        (temporary / "filing-types.parquet").replace(filing_types_dir / "filing-types.parquet")
        for part in signature_parts:
            part.replace(signature_dir / part.name)
        (temporary / "commissions.parquet").replace(commissions_dir / "commissions.parquet")
        (temporary / "mergers.parquet").replace(mergers_dir / "mergers.parquet")
        (temporary / "benford.parquet").replace(benford_dir / "benford.parquet")
        for path in filer_agent_outputs:
            path.replace(filer_agent_dir / path.name)
        print(f"Published {len(outputs) + 4 + len(signature_parts) + len(filer_agent_outputs)} files under {output_dir.parent}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--filer-metadata-dir", type=Path, help="Directory containing listed and unlisted filer metadata CSV.gz files")
    args = parser.parse_args()
    generate(args.output_dir, metadata_dir=args.filer_metadata_dir)
