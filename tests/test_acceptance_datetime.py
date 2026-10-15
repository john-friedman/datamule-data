"""Offline checks for comparison signs, daylight saving time, and exclusions."""

import importlib.util
import re
import unittest

from test_datamule_statistics import statistics


def comparison_database():
    import duckdb

    database = duckdb.connect()
    # Adapt the three Trino timestamp functions to DuckDB's equivalents.
    database.execute("CREATE MACRO date_parse(value, format) AS strptime(value, '%Y%m%d%H%M%S')")
    database.execute("CREATE MACRO with_timezone(value, zone) AS timezone(zone, value)")
    database.execute("CREATE MACRO from_iso8601_timestamp(value) AS CAST(value AS TIMESTAMPTZ)")
    database.execute('CREATE TABLE submissions_metadata (accessionnumber VARCHAR, "form" VARCHAR, filingdate VARCHAR, acceptancedatetime VARCHAR)')
    database.execute('CREATE TABLE filing_metadata_filings (accessionnumber VARCHAR, acceptance_datetime VARCHAR)')
    return database


def compare(submissions, headers):
    with comparison_database() as database:
        database.executemany('INSERT INTO submissions_metadata VALUES (?, ?, ?, ?)', submissions)
        database.executemany('INSERT INTO filing_metadata_filings VALUES (?, ?)', headers)
        # DuckDB's count_if returns NULL for all-NULL inputs, whereas Trino returns zero.
        return [tuple(0 if value is None else value for value in row)
                for row in database.execute(statistics.ACCEPTANCE_DATETIME_SQL).fetchall()]


class AcceptanceDatetimeTests(unittest.TestCase):
    def test_query_references_only_physical_tables(self):
        # Datamule's API resolves FROM/JOIN names against its table catalog.
        masked = re.sub(r"'([^']|'')*'", "''", statistics.ACCEPTANCE_DATETIME_SQL)
        references = set(re.findall(r'\b(?:from|join)\s+([A-Za-z_][\w$]*)', masked, re.IGNORECASE))
        self.assertEqual(references, {"submissions_metadata", "filing_metadata_filings"})

    @unittest.skipUnless(importlib.util.find_spec("duckdb"), "Install duckdb to check SQL offline")
    def test_offsets_dst_and_missing_invalid_timestamps(self):
        with comparison_database() as database:
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

    @unittest.skipUnless(importlib.util.find_spec("duckdb"), "Install duckdb to check SQL offline")
    def test_deduplicates_both_sources_before_join_and_compares_instants(self):
        submissions = [
            ('0000000001-25-000001', '10-K', '2025-01-06', '2025-01-06T17:00:00Z'),
            ('000000000125000001', '10-K', '2025-01-06', '2025-01-06T12:00:00-05:00'),
            (' 125000001 ', '10-K', '2025-01-06', '2025-01-06T17:00:00+00:00'),
            ('0000000001-25-000002', '8-K', '2025-07-07', '2025-07-07T20:00:00Z'),
        ]
        headers = [
            ('125000001', '20250106120000'),
            ('0000000001-25-000001', '20250106120000'),
            (' 000000000125000001 ', '20250106120000'),
            ('125000002', '20250707120000'),
        ]
        expected = [
            ('10-K', 2025, 1, 0, 0, 1, 0, 0, 0, 1, 0),
            ('8-K', 2025, 7, 0, 0, 0, 1, 0, 0, 1, 0),
        ]
        self.assertEqual(compare(submissions, headers), expected)
        self.assertEqual(compare(submissions * 3, headers * 4), expected)

    @unittest.skipUnless(importlib.util.find_spec("duckdb"), "Install duckdb to check SQL offline")
    def test_conflicting_timestamps_excluded_once_and_sparse_copies_use_valid_time(self):
        submissions = []
        headers = []
        for accession in range(1, 6):
            submissions.append((str(accession), '10-K', '2025-01-06', '2025-01-06T17:00:00Z'))
            headers.append((str(accession), '20250106120000'))
        # Conflict on the endpoint side, then on the header side.
        submissions.append(('1', '10-K', '2025-01-06', '2025-01-06T18:00:00Z'))
        headers.append(('2', '20250106130000'))
        # Sources with no valid timestamp remain excluded.
        submissions[2] = ('3', '10-K', '2025-01-06', None)
        submissions.append(('3', '10-K', '2025-01-06', 'invalid'))
        headers[3] = ('4', None)
        headers.append(('4', 'invalid'))
        # Missing/invalid duplicate copies do not override a unique valid time.
        submissions += [('5', '10-K', '2025-01-06', value) for value in [None, 'invalid']]
        headers += [('5', value) for value in [None, 'invalid']]
        # An accession present only in filing headers is not an endpoint submission.
        headers.append(('999', '20250106120000'))
        expected = [('10-K', 2025, 1, 0, 0, 1, 0, 0, 0, 1, 4)]
        self.assertEqual(compare(submissions, headers), expected)
        self.assertEqual(compare(list(reversed(submissions)) * 2, list(reversed(headers)) * 3), expected)

    @unittest.skipUnless(importlib.util.find_spec("duckdb"), "Install duckdb to check SQL offline")
    def test_conflicting_metadata_and_invalid_accessions(self):
        submissions = [
            ('1', '10-K', '2025-01-06', '2025-01-06T17:00:00Z'),
            ('1', '10-Q', '2025-01-06', '2025-01-06T17:00:00Z'),
            ('2', '10-K', '2025-02-06', '2025-01-06T17:00:00Z'),
            ('2', '10-K', '2025-01-06', '2025-01-06T17:00:00Z'),
            ('3', '10-K', 'invalid', '2025-01-06T17:00:00Z'),
            ('4', '10-K', '2024-12-31', '2025-01-06T17:00:00Z'),
            ('4', '10-K', '2025-01-06', '2025-01-06T17:00:00Z'),
        ]
        headers = [(str(accession), '20250106120000') for accession in range(1, 5)]
        for accession in [None, 'invalid', '0', '-1', '1000000000000000000']:
            submissions.append((accession, '10-K', '2025-01-06', '2025-01-06T17:00:00Z'))
            headers.append((accession, '20250106120000'))
        expected = [
            ('10-K', 2025, 1, 0, 0, 0, 0, 0, 0, 0, 1),
            ('Unknown', 2025, 1, 0, 0, 0, 0, 0, 0, 0, 1),
        ]
        self.assertEqual(compare(submissions, headers), expected)
        self.assertEqual(compare(list(reversed(submissions)), list(reversed(headers))), expected)


if __name__ == "__main__":
    unittest.main()
