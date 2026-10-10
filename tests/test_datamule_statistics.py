"""Offline checks for the Datamule statistics publisher."""

import datetime as dt
import csv
import gzip
import importlib.util
import tempfile
import unittest
from pathlib import Path

import polars as pl


SCRIPT = Path(__file__).resolve().parents[1] / "code" / "datamule_statistics.py"
SPEC = importlib.util.spec_from_file_location("datamule_statistics", SCRIPT)
statistics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(statistics)


def write_filer_metadata(destination):
    destination.mkdir(parents=True, exist_ok=True)
    for filename, rows in (
        ("listed_filer_metadata.csv.gz", [(100, "Listed Company")]),
        ("unlisted_filer_metadata.csv.gz", [(200, "Unlisted Entity"), (1104659, "Test Agent")]),
    ):
        with gzip.open(destination / filename, "wt", encoding="utf-8", newline="") as source:
            writer = csv.writer(source)
            writer.writerow(["cik", "name"])
            writer.writerows(rows)


def filer_agent_counts():
    return pl.DataFrame({
        "calendar_year": [2025, 2025, 2025, 2026, 2026],
        "agent_cik": [1104659, 1104659, 1104659, 950170, 950170],
        "cik": [None, 100, 200, None, 100],
        "filing_count": [3, 2, 2, 1, 1],
    })


class StatisticsTests(unittest.TestCase):
    def test_publishes_statistics_and_four_signature_parts(self):
        earliest_ms = int(dt.datetime(2026, 9, 21, tzinfo=dt.timezone.utc).timestamp() * 1000)
        queries = []

        def fake_query(sql, output_dir):
            queries.append(sql)
            output_dir.mkdir(parents=True)
            if sql == statistics.FILER_AGENT_SQL:
                tables = [filer_agent_counts()]
            elif "FROM fastest_sec_filings_metadata" in sql:
                tables = [pl.DataFrame({
                    "accession": ["1", "2"],
                    "detected_time": [earliest_ms, earliest_ms + 1000],
                    "detection_method": ["rss", "index_url"],
                })]
            elif "FROM monitor_dumps" in sql:
                tables = [pl.DataFrame({
                    "accession": ["1"], "source": ["rss"], "detected_time": [1_790_000_000],
                })]
            elif "FROM submissions_metadata" in sql:
                tables = [
                    pl.DataFrame({"accessionnumber": ["1"], "acceptancedatetime": ["2026-09-21T12:00:00"], "filingdate": ["2026-09-21"]}),
                    pl.DataFrame({"accessionnumber": ["2"], "acceptancedatetime": ["2026-09-21T13:00:00"], "filingdate": ["2026-09-21"]}),
                ]
            elif "FROM sec_submission_details_table" in sql:
                tables = [
                    pl.DataFrame({"submissiontype": ["10-K"], "week_start": [dt.date(2026, 9, 21)], "month_start": [dt.date(2026, 9, 1)], "calendar_year": [2026], "count": [3], "xbrl_count": [2]}),
                    pl.DataFrame({"submissiontype": ["10-Q"], "week_start": [dt.date(2026, 9, 21)], "month_start": [dt.date(2026, 9, 1)], "calendar_year": [2026], "count": [5], "xbrl_count": [4]}),
                ]
            elif 'FROM "schedule_13d_signature_person"' in sql:
                tables = [
                    pl.DataFrame({
                        "accessionnumber": [110465923015159, 110465923015160, 110465923015161],
                        "filingdate": ["2026-09-21T08:00:00"] * 3,
                        "name": ["Alice", "Bob", "Cara"],
                    }),
                    pl.DataFrame({
                        "accessionnumber": [110465923015159, 110465923015162, 0],
                        "filingdate": ["2026-09-21"] * 3,
                        "name": ["Alice", "Dan", "Nobody"],
                    }),
                ]
            elif "findersfeedollaramount" in sql:
                tables = [
                    pl.DataFrame({
                        "accessionnumber": [149315221009727], "filingdate": ["2021-04-27"],
                        "submission_type": ["1-K"], "findersfee": ["0.00"], "commissions": ["8193.00"],
                    }),
                    pl.DataFrame({
                        "accessionnumber": [149315221009728], "filingdate": ["2021-04-27"],
                        "submission_type": ["1-K"], "findersfee": ["0.00"], "commissions": ["30703.00"],
                    }),
                ]
            elif 'FROM "effect_merger"' in sql:
                merger = {
                    "accessionnumber": 110465923015159,
                    "filingdate": "2021-10-07",
                    "acquiringcik": "0001352280",
                    "acquiringentityname": "Columbia Funds Series Trust II",
                    "acquiringseriesid": "S000074121",
                    "acquiringseriesname": "Columbia Integrated Large Cap Value Fund",
                    "acquiringclasscontractid": "C000231669",
                    "acquiringclasscontractname": "Advisor Class",
                    "targetcik": "0000889366",
                    "targetentityname": "BMO FUNDS, INC.",
                    "targetseriesid": "S000038425",
                    "targetseriesname": "BMO Low Volatility Equity Fund",
                    "targetclasscontractid": "C000118493",
                    "targetclasscontractname": "Class I",
                }
                another_class = {**merger, "acquiringclasscontractid": "C000231672"}
                incomplete = {**merger, "acquiringseriesid": None}
                tables = [
                    pl.DataFrame([merger, another_class]),
                    pl.DataFrame([merger, incomplete]),
                ]
            elif "FROM simple_xbrl" in sql:
                tables = [
                    pl.DataFrame({
                        "accessionnumber": [6771624000017, 6771624000017, 6771624000017],
                        "cik": ['\\"67716\\"'] * 3,
                        "submissiontype": ["10-Q"] * 3,
                        "taxonomy": ["us-gaap", "us-gaap", "dei"],
                        "digit": [1, 2, 7],
                        "n": [550, 379, 1],
                    }),
                    pl.DataFrame({
                        "accessionnumber": [6771624000022],
                        "cik": ['\\"67716\\"'],
                        "submissiontype": ["10-K"],
                        "taxonomy": ["dei"],
                        "digit": [6],
                        "n": [1],
                    }),
                ]
            else:
                raise AssertionError(f"Unexpected query: {sql}")
            files = []
            for index, table in enumerate(tables):
                file = output_dir / f"part-{index:05d}.parquet"
                table.write_parquet(file)
                files.append(str(file))
            return {"files": files}

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output_dir = root / "data" / "datamule-statistics" / "filing-detections-speed"
            sibling = root / "data" / "keep.txt"
            sibling.parent.mkdir()
            sibling.write_text("keep", encoding="utf-8")
            write_filer_metadata(root / "data" / "filer_metadata")

            statistics.generate(output_dir, query=fake_query)

            self.assertEqual(len(queries), 9)
            self.assertIn("detected_time >=", queries[1])
            self.assertIn("lower(source) IN ('rss', 'efts', 'anticipate')", queries[1])
            self.assertIn("date_trunc('week'", queries[3])
            self.assertIn("date_trunc('month'", queries[3])
            self.assertIn("GROUP BY 1, 2, 3, 4", queries[3])
            self.assertIn("year(CAST(filingdate AS DATE))", queries[3])
            self.assertIn('FROM "schedule_13d_signature_person"', queries[4])
            self.assertIn("date_add('year', -5, current_date)", queries[4])
            self.assertIn("'d' AS submission_type", queries[5])
            self.assertIn('FROM "1_z"', queries[5])
            self.assertIn('FROM "effect_merger"', queries[6])
            self.assertIn("acquiringseriesid IS NOT NULL", queries[6])
            self.assertIn("targetseriesname IS NOT NULL", queries[6])
            self.assertIn("regexp_extract(value, '[1-9]')", queries[7])
            self.assertIn("JOIN sec_submission_details_table d", queries[7])
            self.assertIn("d.submissiontype", queries[7])
            self.assertEqual(sibling.read_text(encoding="utf-8"), "keep")
            self.assertEqual(
                {path.name for path in output_dir.iterdir()},
                {"fastest_sec_filings_websocket.parquet", "websocket.parquet", "linked_filings.parquet"},
            )
            self.assertEqual(pl.read_parquet(output_dir / "fastest_sec_filings_websocket.parquet").height, 2)
            self.assertEqual(pl.read_parquet(output_dir / "websocket.parquet").height, 1)
            self.assertEqual(pl.read_parquet(output_dir / "linked_filings.parquet").height, 2)
            filing_types = pl.read_parquet(output_dir.parent / "sec-filing-types" / "filing-types.parquet")
            self.assertEqual(filing_types.height, 2)
            self.assertEqual(filing_types["count"].sum(), 8)
            self.assertEqual(set(filing_types["month_start"]), {dt.date(2026, 9, 1)})
            signature_parts = sorted((output_dir.parent / "sec-signature-search").glob("part-*.parquet"))
            self.assertEqual(len(signature_parts), 4)
            self.assertEqual([pl.read_parquet(part).height for part in signature_parts], [1, 1, 1, 1])
            signatures = pl.read_parquet(signature_parts)
            self.assertEqual(set(signatures["name"]), {"Alice", "Bob", "Cara", "Dan"})
            self.assertEqual(signatures.filter(pl.col("name") == "Alice").height, 1)
            self.assertEqual(signatures.filter(pl.col("name") == "Alice")["accessionnumber"][0], "000110465923015159")
            self.assertEqual(set(signatures["filingdate"]), {"2026-09-21"})
            commissions = pl.read_parquet(output_dir.parent / "sec-commissions" / "commissions.parquet")
            self.assertEqual(commissions.height, 2)
            self.assertEqual(commissions.columns, ["accessionnumber", "filingdate", "submission_type", "findersfee", "commissions"])
            self.assertEqual(commissions["commissions"].to_list(), ["8193.00", "30703.00"])
            mergers = pl.read_parquet(output_dir.parent / "sec-mergers" / "mergers.parquet")
            self.assertEqual(mergers.height, 2)
            self.assertEqual(set(mergers["accessionnumber"]), {"000110465923015159"})
            self.assertEqual(set(mergers["acquiringclasscontractid"]), {"C000231669", "C000231672"})
            benford_dir = output_dir.parent / "xbrl-benford"
            self.assertEqual([path.name for path in benford_dir.iterdir()], ["benford.parquet"])
            digits = pl.read_parquet(benford_dir / "benford.parquet")
            self.assertEqual(digits.height, 4)
            self.assertEqual(digits.filter(pl.col("digit") == 1)["n"].item(), 550)
            self.assertEqual(set(digits["taxonomy"]), {"dei", "us-gaap"})
            self.assertEqual(set(digits["cik"]), {'\\"67716\\"'})
            self.assertEqual(set(digits["submissiontype"]), {"10-Q", "10-K"})
            filer_agents_dir = output_dir.parent / "sec-filer-agents"
            agents = pl.read_parquet(filer_agents_dir / "agents.parquet")
            self.assertEqual(agents["filing_count"].to_list(), [3, 1])
            self.assertEqual(agents["agent_name"].to_list(), ["Test Agent", ""])
            entities = pl.read_parquet(filer_agents_dir / "entities-2025.parquet")
            self.assertEqual(entities["name"].to_list(), ["Listed Company", "Unlisted Entity"])
            self.assertEqual(entities["filing_count"].sum(), 4)

    def test_query_failure_preserves_published_files(self):
        earliest_ms = int(dt.datetime(2026, 9, 21, tzinfo=dt.timezone.utc).timestamp() * 1000)

        def fake_query(sql, output_dir):
            if 'FROM "schedule_13d_signature_person"' in sql:
                raise RuntimeError("Query failed")
            output_dir.mkdir(parents=True)
            file = output_dir / "part-00000.parquet"
            if "FROM fastest_sec_filings_metadata" in sql:
                table = pl.DataFrame({"accession": ["1"], "detected_time": [earliest_ms], "detection_method": ["rss"]})
            else:
                table = pl.DataFrame({"accession": ["1"], "source": ["rss"], "detected_time": [1_790_000_000]})
            table.write_parquet(file)
            return {"files": [str(file)]}

        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary) / "data" / "datamule-statistics" / "filing-detections-speed"
            output_dir.mkdir(parents=True)
            old_file = output_dir / "websocket.parquet"
            old_file.write_bytes(b"previous published file")
            filing_types_dir = output_dir.parent / "sec-filing-types"
            filing_types_dir.mkdir()
            old_types = filing_types_dir / "filing-types.parquet"
            old_types.write_bytes(b"previous filing types")
            signature_dir = output_dir.parent / "sec-signature-search"
            signature_dir.mkdir()
            old_signatures = signature_dir / "part-00000.parquet"
            old_signatures.write_bytes(b"previous signatures")
            commissions_dir = output_dir.parent / "sec-commissions"
            commissions_dir.mkdir()
            old_commissions = commissions_dir / "commissions.parquet"
            old_commissions.write_bytes(b"previous commissions")
            mergers_dir = output_dir.parent / "sec-mergers"
            mergers_dir.mkdir()
            old_mergers = mergers_dir / "mergers.parquet"
            old_mergers.write_bytes(b"previous mergers")
            benford_dir = output_dir.parent / "xbrl-benford"
            benford_dir.mkdir()
            old_benford = benford_dir / "benford.parquet"
            old_benford.write_bytes(b"previous Benford data")

            with self.assertRaisesRegex(RuntimeError, "Query failed"):
                statistics.generate(output_dir, query=fake_query)

            self.assertEqual(old_file.read_bytes(), b"previous published file")
            self.assertEqual(old_types.read_bytes(), b"previous filing types")
            self.assertEqual(old_signatures.read_bytes(), b"previous signatures")
            self.assertEqual(old_commissions.read_bytes(), b"previous commissions")
            self.assertEqual(old_mergers.read_bytes(), b"previous mergers")
            self.assertEqual(old_benford.read_bytes(), b"previous Benford data")
            self.assertEqual([path.name for path in output_dir.iterdir()], ["websocket.parquet"])


if __name__ == "__main__":
    unittest.main()
