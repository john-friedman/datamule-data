"""Offline checks for filer agent counts, names, and annual partitions."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import polars as pl

from test_datamule_statistics import filer_agent_counts, statistics, write_filer_metadata


class FilerAgentTests(unittest.TestCase):
    def test_partitions_and_missing_names(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_filer_metadata(root / "metadata")
            source = root / "counts.parquet"
            counts = filer_agent_counts()
            counts = pl.concat([counts, counts.head(1)])
            counts.write_parquet(source)
            statistics.write_filer_agent_parquets([str(source)], root / "published", root / "metadata")
            agents = pl.read_parquet(root / "published" / "agents.parquet")
            self.assertEqual(agents.height, 2)
            self.assertEqual(agents["agent_name"].to_list(), ["Test Agent", ""])
            self.assertEqual(json.loads((root / "published" / "manifest.json").read_text())["years"], [2025, 2026])
            entities = pl.read_parquet(root / "published" / "entities-2025.parquet")
            self.assertEqual(entities["filing_count"].sum(), 4)
            self.assertEqual(agents.filter(pl.col("calendar_year") == 2025)["filing_count"].item(), 3)
            self.assertEqual(set(entities["name"]), {"Listed Company", "Unlisted Entity"})

    def test_invalid_totals_do_not_publish(self):
        for problem in ("too_many", "conflict", "missing_total"):
            with self.subTest(problem=problem), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                write_filer_metadata(root / "metadata")
                counts = filer_agent_counts()
                if problem == "too_many":
                    counts = counts.with_columns(pl.when(pl.col("cik") == 100).then(99).otherwise(pl.col("filing_count")).alias("filing_count"))
                elif problem == "conflict":
                    counts = pl.concat([counts, counts.head(1).with_columns(pl.lit(99).cast(pl.Int64).alias("filing_count"))])
                else:
                    counts = counts.filter(pl.col("cik").is_not_null())
                source = root / "counts.parquet"
                counts.write_parquet(source)
                with self.assertRaises(ValueError):
                    statistics.write_filer_agent_parquets([str(source)], root / "published", root / "metadata")
                self.assertFalse((root / "published").exists())

    def test_missing_metadata_does_not_publish(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "counts.parquet"
            filer_agent_counts().write_parquet(source)
            with self.assertRaises(FileNotFoundError):
                statistics.write_filer_agent_parquets([str(source)], root / "published", root / "metadata")
            self.assertFalse((root / "published").exists())

    @unittest.skipUnless(importlib.util.find_spec("duckdb"), "Install duckdb to check SQL joins offline")
    def test_sql_deduplicates_filings_and_uses_joined_year(self):
        import duckdb

        with duckdb.connect() as database:
            database.execute("CREATE TABLE sec_accession_cik_table (accessionnumber VARCHAR, cik VARCHAR)")
            database.execute("CREATE TABLE sec_submission_details_table (accessionnumber VARCHAR, filingdate VARCHAR)")
            database.executemany("INSERT INTO sec_accession_cik_table VALUES (?, ?)", [
                ("110465923015159", "100"), ("110465923015159", "100"),
                ("110465923015159", "200"), ("0001104659-23-015160", "100"),
                ("95017023000001", "200"), ("110465923999999", "100"),
                ("0", "100"), ("invalid", "100"),
            ])
            database.executemany("INSERT INTO sec_submission_details_table VALUES (?, ?)", [
                ("110465923015159", "2025-01-02"), ("110465923015159", "2025-01-02"),
                ("110465923015160", "2025-02-03"), ("95017023000001", "2026-01-01"),
                ("0", "2025-01-02"), ("invalid", "2025-01-02"),
            ])
            rows = set(database.execute(statistics.FILER_AGENT_SQL).fetchall())
        self.assertEqual(rows, {
            (2025, 1104659, None, 2), (2025, 1104659, 100, 2),
            (2025, 1104659, 200, 1), (2026, 950170, None, 1), (2026, 950170, 200, 1),
        })


if __name__ == "__main__":
    unittest.main()
