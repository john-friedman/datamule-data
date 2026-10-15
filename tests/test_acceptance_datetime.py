"""Offline checks for comparison signs, daylight saving time, and exclusions."""

import importlib.util
import re
import unittest

from test_datamule_statistics import statistics


class AcceptanceDatetimeTests(unittest.TestCase):
    def test_query_references_only_physical_tables(self):
        # Datamule's API resolves FROM/JOIN names against its table catalog.
        masked = re.sub(r"'([^']|'')*'", "''", statistics.ACCEPTANCE_DATETIME_SQL)
        references = set(re.findall(r'\b(?:from|join)\s+([A-Za-z_][\w$]*)', masked, re.IGNORECASE))
        self.assertEqual(references, {"submissions_metadata", "filing_metadata_filings"})

    @unittest.skipUnless(importlib.util.find_spec("duckdb"), "Install duckdb to check SQL offline")
    def test_offsets_dst_and_missing_invalid_timestamps(self):
        import duckdb

        with duckdb.connect() as database:
            # Adapt the three Trino timestamp functions to DuckDB's equivalents.
            database.execute("CREATE MACRO date_parse(value, format) AS strptime(value, '%Y%m%d%H%M%S')")
            database.execute("CREATE MACRO with_timezone(value, zone) AS timezone(zone, value)")
            database.execute("CREATE MACRO from_iso8601_timestamp(value) AS CAST(value AS TIMESTAMPTZ)")
            database.execute('CREATE TABLE submissions_metadata (accessionnumber VARCHAR, "form" VARCHAR, filingdate VARCHAR, acceptancedatetime VARCHAR)')
            database.execute('CREATE TABLE filing_metadata_filings (accessionnumber VARCHAR, acceptance_datetime VARCHAR)')
            submissions = []
            headers = []
            for index, (date, endpoint) in enumerate([
                ("2025-01-06", "2025-01-06T12:00:00Z"),  # -5h in winter
                ("2025-01-06", "2025-01-06T17:00:00Z"),  # exact
                ("2025-01-06", "2025-01-06T22:00:00Z"),  # +5h
                ("2025-01-06", "2025-01-06T18:00:00Z"),  # other (+1h)
                ("2025-07-07", "2025-07-07T12:00:00Z"),  # -4h in summer
                ("2025-07-07", "2025-07-07T16:00:00Z"),  # exact
                ("2025-07-07", "2025-07-07T20:00:00Z"),  # +4h
                ("2025-07-07", "2025-07-07T12:00:00-04:00"),  # explicit offset, exact
                ("2025-07-07", None),
                ("2025-07-07", "invalid"),
                ("2024-01-06", "2024-01-06T17:00:00Z"),  # outside date range
            ], start=1):
                submissions.append((f"0000000001-25-{index:06d}", "10-K", date, endpoint))
                headers.append((str(int(f"000000000125{index:06d}")), date.replace('-', '') + '120000'))
            submissions.extend([
                ("0000000001-25-000020", "10-K", "2025-07-07", "2025-07-07T16:00:00Z"),
                ("0000000001-25-000021", "10-K", "2025-07-07", "2025-07-07T16:00:00Z"),
                ("0000000001-25-000022", "10-K", "2025-07-07", "2025-07-07T16:00:00Z"),
                ("0000000001-25-000023", "8-K", "2025-07-07", None),
                ("invalid", "10-K", "not a date", "2025-07-07T16:00:00Z"),
            ])
            headers.extend([
                ("125000021", "invalid"),
                ("125000022", None),
            ])
            database.executemany('INSERT INTO submissions_metadata VALUES (?, ?, ?, ?)', submissions)
            database.executemany('INSERT INTO filing_metadata_filings VALUES (?, ?)', headers)
            # DuckDB's count_if returns NULL for all-NULL inputs, whereas Trino returns zero.
            rows = [tuple(0 if value is None else value for value in row)
                    for row in database.execute(statistics.ACCEPTANCE_DATETIME_SQL).fetchall()]

        self.assertEqual(rows, [
            ("10-K", 2025, 1, 1, 0, 1, 0, 1, 1, 4, 0),
            ("10-K", 2025, 7, 0, 1, 2, 1, 0, 0, 4, 5),
            ("8-K", 2025, 7, 0, 0, 0, 0, 0, 0, 0, 1),
        ])


if __name__ == "__main__":
    unittest.main()
