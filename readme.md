# datamule data generators

This `scripts` branch contains the code and configuration used to generate the data published on the `master` branch.

Clone only this branch to work on the generators without downloading the data history:

```sh
git clone --branch scripts --single-branch https://github.com/john-friedman/datamule-data.git
```

The scheduled workflows live on `master`. Each workflow checks out this branch in `.scripts/`, runs its generator from the `master` working directory, then commits the updated files to `master`. The daily generator reads `data.json` from this branch and reads or writes `data/` and `updates.json` in the working directory.

## Acceptance datetime comparison

`code/datamule_statistics.py` also publishes
`data/datamule-statistics/sec-acceptance-datetime/acceptance-datetime.parquet` on
`master`. The existing `datamule-statistics` workflow generates it nightly, or on
manual dispatch; no workflow change is needed.

The dataset compares the SEC submissions endpoint's acceptance timestamp against
the timestamp extracted from the filing header, interpreted in
`America/New_York` with daylight saving time. Differences are endpoint minus
header, in seconds. Counts are grouped by submission type and filing year/month
from January 2025. The five named buckets are -5h, -4h, exact, +4h, and +5h;
`num_other` covers every other difference, and `total_compared` includes all six
buckets. `num_excluded` counts submissions without a matching header or with a
missing/unparseable timestamp or conflicting duplicate records; these are excluded
from percentage denominators.

Both sources are reduced to one row per normalized accession before joining, so
each accession contributes once. Accession normalization strips hyphens and
surrounding whitespace and converts to an integer. Zero, invalid, or out-of-range
accessions are omitted, as are submissions without a usable filing date.

Duplicate timestamps are compared as parsed instants: different ISO timezone
representations of the same instant are equivalent. Missing or unparseable copies
do not override a unique valid timestamp. If either source has more than one
distinct valid timestamp for an accession, that accession is counted once in
`num_excluded`, never in an offset bucket. Conflicting submission types or filing
dates also cause exclusion. The earliest valid filing date supplies the reporting
month (and the January 2025 cutoff); conflicting types are reported as `Unknown`.

Work only on this generator branch when publishing code. Do not pull or merge
`master`, `data/`, or `updates.json`; the workflow handles generated data on
`master` separately. Run offline checks with
`python -m unittest discover -s tests -v` before pushing to `scripts`.

## Filing detection statistics

`code/datamule_statistics.py` uses three Datamule Hub queries to publish
`fastest_sec_filings_websocket.parquet`, `websocket.parquet`, and
`linked_filings.parquet` under
`master:data/datamule-statistics/filing-detections-speed/`. The workflow reads
`DATAMULE_API_KEY` from a repository Actions secret.

`linked_filings.parquet` now uses `filing_metadata_filings.acceptance_datetime`,
extracted from filing headers, instead of the SEC submissions endpoint's
acceptance time. Compact header timestamps are interpreted in `America/New_York`
(including daylight saving time), then converted to explicit UTC ISO strings.
There is no additional fixed-hour correction.

The published columns remain `accessionnumber`, `acceptancedatetime`, and
`filingdate`, so the website's existing loader can read the new data. Accessions
are normalized to 18 digits and deduplicated across query parts. Missing or invalid
duplicate copies do not override a unique valid time. Accessions with multiple
distinct valid times or filing dates are omitted; missing timestamps and invalid
accession/date rows are also omitted. The workflow logs these exclusion counts.
If no usable timestamps remain, generation fails before replacing published
files. No fallback to endpoint timestamps is used.

The query keeps the existing filing-date window, from the earliest Fastest
detection's Eastern date through the generation date. Detection datasets and
detection-minus-detection totals are unchanged. The separate acceptance datetime
comparison still queries both sources to measure their disagreement.

## Filer Agent

The same generator publishes `data/datamule-statistics/sec-filer-agents/` on `master`:

- `agents.parquet`: `calendar_year`, `agent_cik`, `filing_count`, `agent_name`.
- `entities-YYYY.parquet`: `agent_cik`, `cik`, `filing_count`, `name` for one calendar year.
- `manifest.json`: available years, generation timestamp, and the date through which filings are included.

`FILER_AGENT_SQL` joins `sec_accession_cik_table` to `sec_submission_details_table` on accession. Filing years come from `filingdate`, not the two-digit accession year. Accessions are padded to 18 digits before extracting the first ten digits as the submitting account's CIK. That account can belong to an agent or an entity filing for itself.

Annual agent totals count distinct accessions. Entity counts count distinct accessions per associated CIK, so their sum can exceed the agent total. Duplicate mapping and date rows do not inflate counts. Rows without a matching usable filing date are excluded.

Current names come from `data/filer_metadata/listed_filer_metadata.csv.gz` and `unlisted_filer_metadata.csv.gz`, produced by the daily metadata workflow. Both files must exist; missing individual CIK names remain blank for the page to display a CIK fallback. Names are included in the output so the page loads only the annual summary and selected year's entities. The optional `--filer-metadata-dir` argument overrides the input directory.

The website ranks submitting CIKs separately each year. Top 5 and Top 10 show their combined share versus the remaining CIKs; rank groups use 1–10, 11–100, and 101+. The year slider filters the selected agent's entities, sorted by filing count descending. The current year is labeled year to date.

Run offline checks with `python -m unittest discover -s tests`. Install `duckdb` to include the SQL fixture check; no API credentials or live queries are needed.
