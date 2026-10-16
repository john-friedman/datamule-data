"""Checks for the filing-header acceptance times used by detection-speed charts."""

import contextlib
import importlib.util
import io
from pathlib import Path
import re
import tempfile
import unittest

import polars as pl

from test_datamule_statistics import statistics


class LinkedFilingsTests(unittest.TestCase):
    def test_query_uses_only_header_table(self):
        sql = statistics.LINKED_FILINGS_SQL.format(start_date='2026-01-01', end_date='2026-12-31')
        masked = re.sub(r"'([^']|'')*'", "''", sql)
        references = set(re.findall(r'\b(?:from|join)\s+([A-Za-z_][\w$]*)', masked, re.IGNORECASE))
        self.assertEqual(references, {'filing_metadata_filings'})
        self.assertIn("filingdate >= '2026-01-01'", sql)
        self.assertIn("filingdate <= '2026-12-31'", sql)

    @unittest.skipUnless(importlib.util.find_spec('duckdb'), 'Install duckdb to check SQL offline')
    def test_sql_converts_winter_and_summer_to_explicit_utc(self):
        import duckdb

        with duckdb.connect() as database:
            database.execute("CREATE MACRO date_parse(value, format) AS strptime(value, '%Y%m%d%H%M%S')")
            database.execute('CREATE MACRO with_timezone(value, zone) AS timezone(zone, value)')
            database.execute('CREATE MACRO at_timezone(value, zone) AS timezone(zone, value)')
            database.execute("CREATE MACRO to_iso8601(value) AS strftime(value, '%Y-%m-%dT%H:%M:%S.000Z')")
            database.execute('CREATE TABLE filing_metadata_filings (accessionnumber VARCHAR, filingdate VARCHAR, acceptance_datetime VARCHAR)')
            database.executemany('INSERT INTO filing_metadata_filings VALUES (?, ?, ?)', [
                ('1', '2026-01-06', '20260106120000'),
                ('2', '2026-07-06', '20260706120000'),
                ('3', '2026-10-02', '20261002082659'),
                ('4', '2026-10-02', 'invalid'),
                ('5', '2026-10-02', None),
                ('6', '2025-12-31', '20251231120000'),
            ])
            sql = statistics.LINKED_FILINGS_SQL.format(start_date='2026-01-01', end_date='2026-12-31')
            rows = database.execute(sql).fetchall()
        self.assertEqual(rows, [
            ('1', '2026-01-06', '20260106120000', '2026-01-06T17:00:00.000Z'),
            ('2', '2026-07-06', '20260706120000', '2026-07-06T16:00:00.000Z'),
            ('3', '2026-10-02', '20261002082659', '2026-10-02T12:26:59.000Z'),
            ('4', '2026-10-02', 'invalid', None),
            ('5', '2026-10-02', None, None),
        ])

    def test_deduplicates_parts_and_omits_missing_conflicting_invalid_headers(self):
        rows = [
            ('0000000001-26-000001', '2026-07-06', '2026-07-06T16:00:00.000Z'),
            ('126000001', '2026-07-06', '2026-07-06T12:00:00-04:00'),
            ('000000000126000001', '2026-07-06', None),
            ('126000001', '2026-07-06', 'invalid'),
            ('2', '2026-01-06', '2026-01-06T17:00:00.000Z'),
            ('3', '2026-01-06', None),
            ('4', '2026-01-06', '2026-01-06T17:00:00.000Z'),
            ('4', '2026-01-06', '2026-01-06T18:00:00.000Z'),
            ('5', '2026-01-06', '2026-01-06T17:00:00.000Z'),
            ('5', '2026-01-07', '2026-01-06T17:00:00.000Z'),
            ('0', '2026-01-06', '2026-01-06T17:00:00.000Z'),
            ('invalid', '2026-01-06', '2026-01-06T17:00:00.000Z'),
            ('6', 'invalid', '2026-01-06T17:00:00.000Z'),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = []
            for index, part in enumerate([rows[:6], rows[6:]]):
                path = root / f'part-{index:05d}.parquet'
                pl.DataFrame(part, schema=['accessionnumber', 'filingdate', 'acceptancedatetime'], orient='row').write_parquet(path)
                paths.append(str(path))
            output = root / 'linked_filings.parquet'
            log = io.StringIO()
            with contextlib.redirect_stdout(log):
                statistics.write_linked_filings_parquet(paths, output)
            linked = pl.read_parquet(output)
        self.assertEqual(linked.columns, ['accessionnumber', 'acceptancedatetime', 'filingdate'])
        self.assertEqual(linked.to_dicts(), [
            {'accessionnumber': '000000000000000002', 'acceptancedatetime': '2026-01-06T17:00:00.000Z', 'filingdate': '2026-01-06'},
            {'accessionnumber': '000000000126000001', 'acceptancedatetime': '2026-07-06T16:00:00.000Z', 'filingdate': '2026-07-06'},
        ])
        self.assertIn('2 usable accessions', log.getvalue())
        self.assertIn('1 without a valid timestamp', log.getvalue())
        self.assertIn('2 with conflicting timestamps or dates', log.getvalue())
        self.assertIn('3 rows with invalid accessions or dates', log.getvalue())

    def test_no_usable_headers_preserves_previous_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'headers.parquet'
            pl.DataFrame({'accessionnumber': ['1'], 'filingdate': ['2026-10-02'], 'acceptancedatetime': [None]}).write_parquet(source)
            destination = root / 'linked_filings.parquet'
            destination.write_bytes(b'previous header timestamps')
            with self.assertRaisesRegex(ValueError, 'no usable acceptance timestamps'):
                statistics.write_linked_filings_parquet([str(source)], destination)
            self.assertEqual(destination.read_bytes(), b'previous header timestamps')


if __name__ == '__main__':
    unittest.main()
