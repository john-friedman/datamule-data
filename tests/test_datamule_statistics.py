"""Offline checks for the Datamule statistics publisher."""

import datetime as dt
import importlib.util
import tempfile
import unittest
from pathlib import Path

import polars as pl


SCRIPT = Path(__file__).resolve().parents[1] / "code" / "datamule_statistics.py"
SPEC = importlib.util.spec_from_file_location("datamule_statistics", SCRIPT)
statistics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(statistics)


class StatisticsTests(unittest.TestCase):
    def test_publishes_statistics_and_four_signature_parts(self):
        earliest_ms = int(dt.datetime(2026, 9, 21, tzinfo=dt.timezone.utc).timestamp() * 1000)
        queries = []

        def fake_query(sql, output_dir):
            queries.append(sql)
            output_dir.mkdir(parents=True)
            if "FROM fastest_sec_filings_metadata" in sql:
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
                    pl.DataFrame({"submissiontype": ["10-K"], "week_start": [dt.date(2026, 9, 21)], "calendar_year": [2026], "count": [3], "xbrl_count": [2]}),
                    pl.DataFrame({"submissiontype": ["10-Q"], "week_start": [dt.date(2026, 9, 21)], "calendar_year": [2026], "count": [5], "xbrl_count": [4]}),
                ]
            elif 'FROM "schedule_13d_signature_person"' in sql:
                tables = [
                    pl.DataFrame({
                        "accessionnumber": ["0001104659-23-015159", "0001104659-23-015160", "0001104659-23-015161"],
                        "filingdate": ["2026-09-21"] * 3,
                        "name": ["Alice", "Bob", "Cara"],
                    }),
                    pl.DataFrame({
                        "accessionnumber": ["0001104659-23-015159", "0001104659-23-015162", "invalid"],
                        "filingdate": ["2026-09-21"] * 3,
                        "name": ["Alice", "Dan", "Nobody"],
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

            statistics.generate(output_dir, query=fake_query)

            self.assertEqual(len(queries), 5)
            self.assertIn("detected_time >=", queries[1])
            self.assertIn("lower(source) IN ('rss', 'efts', 'anticipate')", queries[1])
            self.assertIn("date_trunc('week'", queries[3])
            self.assertIn("year(CAST(filingdate AS DATE))", queries[3])
            self.assertIn('FROM "schedule_13d_signature_person"', queries[4])
            self.assertIn("date_add('year', -5, current_date)", queries[4])
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
            signature_parts = sorted((output_dir.parent / "sec-signature-search").glob("part-*.parquet"))
            self.assertEqual(len(signature_parts), 4)
            self.assertEqual([pl.read_parquet(part).height for part in signature_parts], [1, 1, 1, 1])
            signatures = pl.read_parquet(signature_parts)
            self.assertEqual(set(signatures["name"]), {"Alice", "Bob", "Cara", "Dan"})
            self.assertEqual(signatures.filter(pl.col("name") == "Alice").height, 1)

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

            with self.assertRaisesRegex(RuntimeError, "Query failed"):
                statistics.generate(output_dir, query=fake_query)

            self.assertEqual(old_file.read_bytes(), b"previous published file")
            self.assertEqual(old_types.read_bytes(), b"previous filing types")
            self.assertEqual(old_signatures.read_bytes(), b"previous signatures")
            self.assertEqual([path.name for path in output_dir.iterdir()], ["websocket.parquet"])


if __name__ == "__main__":
    unittest.main()
